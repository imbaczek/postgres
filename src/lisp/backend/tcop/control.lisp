;;;; control.lisp -- out-of-band control plane: hot reload and a Lisp REPL.
;;;;
;;;; The backend normally speaks the PostgreSQL wire protocol.  This file adds
;;;; a second, non-SQL interface for developers and operators:
;;;;
;;;;   * SIGHUP recompiles and reloads the "pg" system in the running image,
;;;;     the same way PostgreSQL reloads configuration.
;;;;   * An optional REPL is exposed on a loopback TCP port and/or a Unix
;;;;     domain socket, enabled with --control-port / --control-socket.
;;;;
;;;; The REPL evaluates arbitrary Lisp, so it is off unless explicitly asked
;;;; for; the TCP variant is always bound to 127.0.0.1 and the Unix socket is
;;;; protected by filesystem permissions.

(in-package #:pg)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (require :sb-posix))

;;; ---- in-image reload ----

;; The process's real stdout/stderr, captured at startup so that a reload
;; triggered from a REPL connection can send ASDF's chatter to the server log
;; instead of the client.
(defvar *server-output* nil)
(defvar *server-error* nil)

(defun capture-server-streams ()
  (setf *server-output* *standard-output*
        *server-error* *error-output*))

;; Serialize SIGHUP reloads and explicit (reload) calls so they cannot run
;; concurrently and interleave a half-compiled image.
(defvar *reload-run-lock* (sb-thread:make-mutex :name "pg reload run lock"))

(defun reload ()
  "Recompile and reload the pg system in this image.  Returns T on success."
  (sb-thread:with-mutex (*reload-run-lock*)
    (format t "~&pg-lisp: reloading code...~%")
    (finish-output)
    (let ((ok (handler-case
                  (let ((*standard-output* (or *server-output* *standard-output*))
                        (*error-output* (or *server-error* *error-output*))
                        (*compile-verbose* nil)
                        (*load-verbose* nil))
                    (asdf:load-system "pg" :force t)
                    t)
                (error (e)
                  (format *error-output* "~&pg-lisp: reload failed: ~a~%" e)
                  (finish-output *error-output*)
                  nil))))
      (format t "~&pg-lisp: reload ~:[failed (see server log)~;complete~]~%" ok)
      (finish-output)
      ok)))

(defvar *reload-requested* nil)
(defvar *reload-mutex* (sb-thread:make-mutex :name "pg reload mutex"))
(defvar *reload-cv* (sb-thread:make-waitqueue :name "pg reload waitqueue"))

(defun request-reload (&rest signal-info)
  "SIGHUP handler: wake the reloader thread and return immediately.

Signal handlers must be quick, and ASDF must not run inside one, so the
actual work happens on a helper thread."
  (declare (ignore signal-info))
  (sb-thread:with-mutex (*reload-mutex*)
    (setf *reload-requested* t)
    (sb-thread:condition-broadcast *reload-cv*)))

(defun reload-loop ()
  (loop
    (sb-thread:with-mutex (*reload-mutex*)
      (loop until *reload-requested*
            do (sb-thread:condition-wait *reload-cv* *reload-mutex*))
      (setf *reload-requested* nil))
    (reload)))

(defun enable-reload ()
  "Install the SIGHUP handler and start the reloader thread."
  (sb-sys:enable-interrupt sb-unix:sighup #'request-reload)
  (sb-thread:make-thread #'reload-loop :name "pg reloader")
  (values))

;;; ---- control REPL ----

;; A symbol, not a gensym, so the tag survives a reload: a connection that is
;; already blocked on CATCH keeps seeing the QUIT that the redefined function
;; throws.
(defvar *control-quit-tag* 'control-repl-quit)

(defun quit ()
  "Close the current control REPL connection."
  (throw *control-quit-tag* nil))

(defvar *control-socket-path* nil)

(defun cleanup-control-socket ()
  "Remove the Unix control socket, if one was created."
  (when *control-socket-path*
    (ignore-errors (delete-file *control-socket-path*))
    (setf *control-socket-path* nil)))

(defun enable-cleanup ()
  "Arrange for the Unix control socket to be removed on exit.

SIGTERM/SIGINT/SIGQUIT are handled by the shutdown machinery in
postmaster.lisp; a clean exit runs this hook, an immediate shutdown skips it."
  (pushnew 'cleanup-control-socket sb-ext:*exit-hooks*)
  (values))

(defun make-local-listener (path)
  "Return a listening AF_UNIX stream socket bound to PATH.

The socket node is chmod'ed to 0700: it evaluates arbitrary Lisp, and it is
the node's mode (not the containing directory) that decides who may connect."
  (ignore-errors (delete-file path))
  (let ((s (make-instance 'sb-bsd-sockets:local-socket
                          :type :stream :protocol 0)))
    (sb-bsd-sockets:socket-bind s (namestring path))
    (sb-bsd-sockets:socket-listen s 16)
    (handler-case (sb-posix:chmod (namestring path) #o700)
      (error (e)
        (format *error-output* "~&pg-lisp: warning: cannot chmod ~a: ~a~%"
                path e)))
    s))

(defun control-accept-loop (listener name)
  (loop
    (let ((sock (accept-connection-or-nil listener name)))
      (when sock
        (sb-thread:make-thread
         (lambda () (control-serve sock))
         :name "pg control connection")))))

(defun control-eval (stream form)
  "Evaluate FORM, printing its values (or an error) to STREAM."
  (handler-case
      (format stream "~&~{~s~^~%~}~%" (multiple-value-list (eval form)))
    (error (e)
      (format stream "~&;; error: ~a~%" e))))

(defun read-control-form (stream)
  "Read one form, with reader evaluation disabled.

The binding must cover READ only: EVAL may run code that reads source (a
reload, say), which legitimately uses #. for compile-time constants."
  (let ((*read-eval* nil))
    (read stream nil :eof)))

(defun control-read-eval-loop (stream)
  "Read forms from STREAM until EOF, printing each result."
  (loop
    (format stream "~&pg> ")
    (finish-output stream)
    (let ((form (handler-case (read-control-form stream)
                  (error (e)
                    (format stream "~&;; read error: ~a~%" e)
                    :skip))))
      (cond
        ((eq form :eof) (return))
        ((eq form :skip))
        (t (control-eval stream form))))))

(defun control-serve (sock)
  "Read-eval-print loop for one control connection."
  (let ((stream nil))
    (unwind-protect
         (handler-case
             (progn
               (setf stream (sb-bsd-sockets:socket-make-stream
                             sock :input t :output t
                             :element-type 'character :buffering :line))
               (let ((*standard-output* stream)
                     (*error-output* stream)
                     (*trace-output* stream)
                     (*query-io* stream)
                     (*package* (find-package '#:pg)))
                 (format stream "~&pg-lisp control REPL~%server ~a, package ~a~%~
                                 (reload) reloads code, (quit) disconnects.~%"
                         *server-version* (package-name *package*))
                 (finish-output stream)
                 (catch *control-quit-tag*
                   (control-read-eval-loop stream))
                 (format stream "~&bye~%")))
           ;; A client that vanished shows up as a stream error; anything else
           ;; is a bug worth logging.
           (stream-error () nil)
           (error (e)
             (format *error-output* "~&pg-lisp: control connection error: ~a~%"
                     e)
             nil))
      (when stream
        (ignore-errors (finish-output stream))
        (ignore-errors (close stream)))
      (ignore-errors (sb-bsd-sockets:socket-close sock)))))

(defun start-control-plane (&key port socket-path)
  "Start the control REPL on loopback TCP PORT and/or Unix SOCKET-PATH.

Both are optional; with neither, no control plane is started."
  (when (or port socket-path)
    (enable-cleanup))
  (when port
    (let ((listener (make-listener "127.0.0.1" port)))
      (format t "~&pg-lisp: control REPL on 127.0.0.1:~a~%" port)
      (finish-output)
      (sb-thread:make-thread
       (lambda () (control-accept-loop listener "control tcp"))
       :name "pg control tcp")))
  (when socket-path
    (let* ((pn (pathname socket-path))
           (listener (make-local-listener pn)))
      (setf *control-socket-path* (namestring pn))
      (format t "~&pg-lisp: control REPL on ~a~%" (namestring pn))
      (finish-output)
      (sb-thread:make-thread
       (lambda () (control-accept-loop listener "control unix"))
       :name "pg control unix")))
  (values))
