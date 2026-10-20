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

;; A reload closes admission to SQL operations and waits for current queries to
;; leave before redefining functions used by backend threads.
(defvar *live-operation-lock*
  (sb-thread:make-mutex :name "pg live operation lock"))
(defvar *live-operation-cv*
  (sb-thread:make-waitqueue :name "pg live operation condition"))
(defvar *live-operation-admission-open* t)
(defvar *live-operation-drain-open* nil)
(defvar *live-operation-count* 0)
(defvar *live-operation-depth* 0)

(defun live-operation-admitted-p (drain-p)
  (or *live-operation-admission-open*
      (and drain-p *live-operation-drain-open*)))

(defun wait-for-live-operation-admission (drain-p)
  (sb-thread:with-mutex (*live-operation-lock*)
    (loop until (live-operation-admitted-p drain-p)
          do (sb-thread:condition-wait *live-operation-cv*
                                       *live-operation-lock*)))
  (values))

(defun try-enter-live-operation (drain-p)
  "Count an operation if its admission lane remains open; never wait here."
  (sb-thread:with-mutex (*live-operation-lock*)
    (when (live-operation-admitted-p drain-p)
      (incf *live-operation-count*)
      t)))

(defun leave-live-operation ()
  ;; Keep termination responsive while waiting for the lock. Once acquired,
  ;; finish the count/broadcast transition without allowing an interrupt to
  ;; strand reload waiters. A caught termination is re-signaled afterward.
  (let ((finished-p nil)
        (lock-acquired-p nil)
        (pending-interrupt nil))
    (loop until finished-p
          do (progn
               (handler-case
                   (sb-sys:without-interrupts
                     (when (sb-thread:grab-mutex *live-operation-lock* :waitp nil)
                       (setf lock-acquired-p t)
                       (unwind-protect
                            (progn
                              (unless (plusp *live-operation-count*)
                                (error "live operation count underflow"))
                              (decf *live-operation-count*)
                              (when (zerop *live-operation-count*)
                                (sb-thread:condition-broadcast *live-operation-cv*))
                              (setf finished-p t))
                         (sb-thread:release-mutex *live-operation-lock*))))
                 (error (condition)
                   (if lock-acquired-p
                       (error condition)
                       (setf pending-interrupt condition))))
               (unless finished-p
                 (handler-case
                     (sb-thread:thread-yield)
                   (error (condition)
                     (setf pending-interrupt condition))))))
    (when pending-interrupt
      (error pending-interrupt)))
  (values))

(defun close-live-operation-admission ()
  (when (plusp *live-operation-depth*)
    (error "cannot reload from inside a live operation"))
  (sb-thread:with-mutex (*live-operation-lock*)
    (setf *live-operation-admission-open* nil)
    (setf *live-operation-drain-open* t)
    (loop while (plusp *live-operation-count*)
          do (sb-thread:condition-wait *live-operation-cv*
                                       *live-operation-lock*))
    ;; Close the drain lane atomically with observing an empty operation set.
    ;; Otherwise a new COMMIT could enter after this waiter decides reload is
    ;; safe and overlap ASDF's function redefinitions.
    (setf *live-operation-drain-open* nil))
  (values))

(defun open-live-operation-admission ()
  (sb-thread:with-mutex (*live-operation-lock*)
    (setf *live-operation-drain-open* nil
          *live-operation-admission-open* t)
    (sb-thread:condition-broadcast *live-operation-cv*))
  (values))

(defmacro with-live-operation-internal ((drain-p) &body body)
  "Run BODY under the reload gate, optionally while active work drains."
  (let ((outermost (gensym "OUTERMOST-"))
        (admitted (gensym "ADMITTED-"))
        (operation-block (gensym "LIVE-OPERATION-")))
    `(let ((,outermost (zerop *live-operation-depth*)))
       (if ,outermost
           (block ,operation-block
             (loop
               (wait-for-live-operation-admission ,drain-p)
               (let ((,admitted nil))
                 ;; Keep asynchronous termination from landing after the count
                 ;; increments but before its cleanup has been installed.
                 (sb-sys:without-interrupts
                   (setf ,admitted (try-enter-live-operation ,drain-p))
                   (when ,admitted
                     (return-from ,operation-block
                       (unwind-protect
                            (sb-sys:allow-with-interrupts
                              (let ((*live-operation-depth*
                                      (1+ *live-operation-depth*)))
                                (progn ,@body)))
                         (leave-live-operation))))))))
           (let ((*live-operation-depth* (1+ *live-operation-depth*)))
             (progn ,@body))))))

(defmacro with-live-operation (&body body)
  "Run BODY while protecting its function definitions from a live reload."
  `(with-live-operation-internal (nil) ,@body))

(defmacro with-live-operation-drain (&body body)
  "Run a BODY that may help already-active operations drain."
  `(with-live-operation-internal (t) ,@body))

(defmacro with-live-operation-quiescence (&body body)
  "Run BODY after existing live operations drain and before new ones enter."
  `(unwind-protect
        (progn
          (close-live-operation-admission)
          ,@body)
     (open-live-operation-admission)))

(defun reload ()
  "Recompile and reload the pg system in this image.  Returns T on success."
  (sb-thread:with-mutex (*reload-run-lock*)
    (format t "~&pg-lisp: reloading code...~%")
    (finish-output)
    (let ((ok (with-live-operation-quiescence
                (handler-case
                    (let ((*standard-output* (or *server-output* *standard-output*))
                          (*error-output* (or *server-error* *error-output*))
                          (*compile-verbose* nil)
                          (*load-verbose* nil))
                      (asdf:load-system "pg" :force t)
                      t)
                  (error (e)
                    (format *error-output* "~&pg-lisp: reload failed: ~a~%" e)
                    (finish-output *error-output*)
                    nil)))))
      (format t "~&pg-lisp: reload ~:[failed (see server log)~;complete~]~%" ok)
      (finish-output)
      ok)))

(defvar *reload-requested* nil)

(defun request-reload (&rest signal-info)
  "SIGHUP handler: record the request and return immediately.

Signal handlers run in whichever thread the OS delivered the signal to, so
this must not take a lock (the signal may land in the reloader itself).  ASDF
must not run inside a handler either; the reloader thread polls the flag."
  (declare (ignore signal-info))
  (setf *reload-requested* t))

(defun reload-loop ()
  (loop
    (if (sb-ext:compare-and-swap *reload-requested* t nil)
        (reload)
        (sleep 0.05))))

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
postmaster.lisp; each shutdown mode exits through SB-EXT:EXIT and runs this
hook."
  (pushnew 'cleanup-control-socket sb-ext:*exit-hooks*)
  (values))

(defun private-control-socket-path (path)
  "Resolve PATH inside a directory owned by this user with mode 0700 or stricter."
  (let* ((path (pathname path))
         (directory (truename
                     (merge-pathnames
                      (make-pathname :name nil :type nil :defaults path)
                      *default-pathname-defaults*)))
         (status (sb-posix:stat (namestring directory)))
         (mode (sb-posix:stat-mode status)))
    (unless (and (= (logand mode sb-posix:s-ifmt) sb-posix:s-ifdir)
                 (= (sb-posix:stat-uid status) (sb-posix:getuid))
                 (= (logand mode #o700) #o700)
                 (zerop (logand mode #o077)))
      (error "control socket directory must be owned by this user and mode 0700: ~a"
             directory))
    (make-pathname :directory (pathname-directory directory)
                   :name (pathname-name path)
                   :type (pathname-type path)
                   :version nil)))

(defun make-local-listener (path)
  "Return a listening AF_UNIX stream socket bound to PATH.

The socket node and its owner-only parent directory are both restricted:
this listener evaluates arbitrary Lisp."
  (let ((s (make-instance 'sb-bsd-sockets:local-socket
                          :type :stream :protocol 0))
        (bound nil))
    (handler-case
        (progn
          (sb-bsd-sockets:socket-bind s (namestring path))
          (setf bound t)
          (sb-posix:chmod (namestring path) #o700)
          (sb-bsd-sockets:socket-listen s 16)
          s)
      (error (e)
        (ignore-errors (sb-bsd-sockets:socket-close s))
        (when bound
          (ignore-errors (delete-file path)))
        (error "cannot create protected control socket ~a: ~a" path e)))))

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
      (format stream "~&~{~s~^~%~}~%"
              (multiple-value-list
               (if (and (consp form)
                        (eq (first form) 'reload)
                        (null (rest form)))
                   ;; Keep the documented REPL reload entry outside the live
                   ;; operation gate; RELOAD coordinates quiescence itself.
                   (reload)
                   (with-live-operation
                     (eval form)))))
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
  (when socket-path
    (setf socket-path (private-control-socket-path socket-path)))
  (let ((local-listener nil)
        (tcp-listener nil)
        (local-path (and socket-path (pathname socket-path)))
        (started nil))
    (unwind-protect
         (progn
           ;; Bind and secure every requested endpoint before accepting on any
           ;; of them. A local-socket permission failure must not leave TCP REPL
           ;; threads serving while startup unwinds.
           (when local-path
             (setf local-listener (make-local-listener local-path)))
           (when port
             (setf tcp-listener (make-listener "127.0.0.1" port)))
           (when local-listener
             (setf *control-socket-path* (namestring local-path)))
           (when (or port local-path)
             (enable-cleanup))
           (when local-listener
             (sb-thread:make-thread
              (lambda () (control-accept-loop local-listener "control unix"))
              :name "pg control unix")
             (format t "~&pg-lisp: control REPL on ~a~%"
                     (namestring local-path)))
           (when tcp-listener
             (sb-thread:make-thread
              (lambda () (control-accept-loop tcp-listener "control tcp"))
              :name "pg control tcp")
             (format t "~&pg-lisp: control REPL on 127.0.0.1:~a~%" port))
           (when (or port local-path)
             (finish-output))
           (setf started t)
           (values))
      (unless started
        (setf *control-socket-path* nil)
        (when local-listener
          (ignore-errors (sb-bsd-sockets:socket-close local-listener)))
        (when local-listener
          (ignore-errors (delete-file local-path)))
        (when tcp-listener
          (ignore-errors (sb-bsd-sockets:socket-close tcp-listener)))))))

(defun start-swank-listener (port bind-address)
  "Start the optional Swank listener on BIND-ADDRESS and PORT."
  (when port
    (let* ((package (find-package "SWANK"))
           (create-server (and package (find-symbol "CREATE-SERVER" package)))
           (global-debugger (and package (find-symbol "*GLOBAL-DEBUGGER*" package))))
      (unless (and create-server (fboundp create-server))
        (error "pinned Swank system is not loaded"))
      ;; A Swank connection can still debug its own requests, but must not
      ;; redirect unhandled errors from SQL or control threads to Emacs.
      (when global-debugger
        (set global-debugger nil))
      (handler-case
          (funcall create-server :port port
                                 :interface bind-address
                                 :style :spawn
                                 :dont-close t)
        (error (e)
          (error "cannot start Swank on ~a:~a: ~a" bind-address port e)))
      (format t "~&pg-lisp: Swank listening on ~a:~a~%" bind-address port)
      (finish-output)))
  (values))
