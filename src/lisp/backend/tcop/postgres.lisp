;;;; postgres.lisp -- backend main loop and connection lifecycle.

(in-package #:pg)

;; DEFVAR so that hot reload does not reset them to zero.
(defvar *pid-counter* 0)
(defvar *secret-counter* 0)

;;; ---- startup ----

(defun next-cstring (bytes start)
  (let ((end (position 0 bytes :start start)))
    (unless end
      (return-from next-cstring (values nil nil)))
    (values (bytes->string bytes :start start :end end) (1+ end))))

(defun parse-startup-params (body)
  (let ((params '())
        (i 4))
    (loop
      (when (or (>= i (length body)) (zerop (aref body i)))
        (return))
      (multiple-value-bind (key next) (next-cstring body i)
        (unless key (return))
        (multiple-value-bind (value next2) (next-cstring body next)
          (unless value (return))
          (push (cons key value) params)
          (setf i next2))))
    (nreverse params)))

(defun read-startup-until-ready (stream)
  "Consume SSL/GSS negotiation requests; return the StartupMessage body or NIL."
  (loop
    (let ((body (read-startup-packet stream)))
      (when (null body)
        (return-from read-startup-until-ready nil))
      (when (< (length body) 4)
        (return-from read-startup-until-ready nil))
      (let ((code (bytes-to-u32 body 0)))
        (cond
          ((= code +ssl-request-code+)
           (write-byte (char-code #\N) stream)
           (force-output stream))
          ((= code +gssenc-request-code+)
           (write-byte (char-code #\N) stream)
           (force-output stream))
          ((= code +cancel-request-code+)
           (return-from read-startup-until-ready nil))
          (t
           (return-from read-startup-until-ready body)))))))

(defun process-startup (stream body)
  (let* ((code (bytes-to-u32 body 0))
         (major (ash code -16)))
    (unless (= major 3)
      (ereport "0A000"
               (format nil "unsupported frontend protocol ~a.~a"
                       major (logand code #xFFFF))))
    (let* ((params (parse-startup-params body))
           (app (or (cdr (assoc "application_name" params :test #'string=))
                    ""))
           (dbname (or (cdr (assoc "database" params :test #'string=))
                       "postgres")))
      (setf (backend-database *backend*) dbname)
      (with-catalog-lock
        (unless (gethash dbname (catalog-databases *catalog*))
          (get-database dbname)
          (save-catalog)))
      (send-auth-ok stream)
      (send-parameter-status stream "server_version" *server-version*)
      (send-parameter-status stream "server_version_num"
                             (princ-to-string *server-version-num*))
      (send-parameter-status stream "server_encoding" "UTF8")
      (send-parameter-status stream "client_encoding" "UTF8")
      (send-parameter-status stream "DateStyle" "ISO, MDY")
      (send-parameter-status stream "TimeZone" "UTC")
      (send-parameter-status stream "integer_datetimes" "on")
      (send-parameter-status stream "standard_conforming_strings" "on")
      (send-parameter-status stream "IntervalStyle" "postgres")
      (send-parameter-status stream "is_superuser" "on")
      (send-parameter-status stream "session_authorization" "postgres")
      (send-parameter-status stream "application_name" app)
      (send-parameter-status stream "default_transaction_read_only" "off")
      (send-parameter-status stream "in_hot_standby" "off")
      (send-backend-key-data stream
                             (incf *pid-counter*)
                             (incf *secret-counter*))
      (send-ready-for-query stream #\I))))

;;; ---- query dispatch ----

(defun query-string (message)
  (let* ((payload (frontend-message-payload message))
         (end (or (position 0 payload) (length payload))))
    (bytes->string payload :start 0 :end end)))

(defun empty-query-p (query)
  (every (lambda (ch)
           (member ch '(#\Space #\Tab #\Newline #\Return #\;)))
         query))

(defun handle-simple-query (stream message)
  (let ((query (query-string message)))
    (if (empty-query-p query)
        (send-empty-query-response stream)
        (exec-simple-query stream query))
    (send-ready-for-query stream (transaction-status-char))))

(defun handle-unsupported-message (stream message)
  (declare (ignore message))
  (send-error-response
   stream
   (make-condition 'pg-error
                   :sqlstate "0A000"
                   :message "extended query protocol is not supported by this server"))
  (send-ready-for-query stream (transaction-status-char)))

;;; ---- backend loop ----

(defun backend-loop (stream)
  (loop
    (let ((message (read-message stream)))
      (when (null message)
        (return))
      (case (frontend-message-type message)
        (#\X (return))
        (#\Q (handle-simple-query stream message))
        (otherwise (handle-unsupported-message stream message))))))

(defun handle-connection (sock)
  (let ((stream nil))
    (unwind-protect
         (handler-case
             (progn
               (setf stream (socket->stream sock))
               (let ((*backend* (make-backend :stream stream
                                              :database "postgres")))
                 ;; A dropped connection must not strand an open transaction.
                 (unwind-protect
                      (let ((body (read-startup-until-ready stream)))
                        (when body
                          (process-startup stream body)
                          (backend-loop stream)))
                   (end-backend-session))))
           (pg-error (e)
             (when stream (ignore-errors (send-error-response stream e))))
           (error (e)
             (when stream
               (ignore-errors
                (send-error-response
                 stream
                 (make-condition 'pg-error :sqlstate "XX000"
                                 :message (princ-to-string e)))))))
      (close-connection sock stream))))

;;; ---- server main ----

(defun parse-args (argv)
  (let ((port 5433)
        (host "127.0.0.1")
        (pgdata nil)
        (control-port nil)
        (control-socket nil))
    (loop for rest on argv
          for arg = (car rest)
          do (cond
               ((member arg '("-p" "--port") :test #'string=)
                (setf port (parse-integer (cadr rest))))
               ((and (> (length arg) 7) (string= arg "--port=" :end1 7))
                (setf port (parse-integer arg :start 7)))
               ((member arg '("-h" "--host") :test #'string=)
                (setf host (cadr rest)))
               ((member arg '("-D" "--pgdata") :test #'string=)
                (setf pgdata (cadr rest)))
               ((and (> (length arg) 9) (string= arg "--pgdata=" :end1 9))
                (setf pgdata (subseq arg 9)))
               ((member arg '("-C" "--control-port") :test #'string=)
                (setf control-port (parse-integer (cadr rest))))
               ((and (> (length arg) 15) (string= arg "--control-port=" :end1 15))
                (setf control-port (parse-integer arg :start 15)))
               ((string= arg "--control-socket")
                (setf control-socket (cadr rest)))
               ((and (> (length arg) 17) (string= arg "--control-socket=" :end1 17))
                (setf control-socket (subseq arg 17)))))
    (values host port (or pgdata (sb-ext:posix-getenv "PGDATA") "./pgdata")
            control-port control-socket)))

(defun directory-pathname (path)
  "Return PATH as an absolute directory pathname (trailing slash).

Relative data directories are resolved against the current working
directory, matching PostgreSQL.  Making them absolute matters because
CL's RENAME-FILE merges a relative target with the source's directory."
  (let* ((s (string path))
         (with-slash (if (and (> (length s) 0)
                              (char= (char s (1- (length s))) #\/))
                         s
                         (concatenate 'string s "/")))
         (pn (pathname with-slash)))
    (if (eq (first (pathname-directory pn)) :absolute)
        pn
        (merge-pathnames pn (uiop:getcwd)))))

(defun serve (host port)
  (let ((listener (make-listener host port)))
    (loop
      (let ((sock (handler-case (accept-connection listener)
                    (error (e)
                      (format *error-output* "~&pg-lisp: accept: ~a~%" e)
                      nil))))
        (when sock
          (sb-thread:make-thread
           (lambda () (handle-connection sock))
           :name "pg backend"))))))

;;; ---- hot reload (SIGHUP) and control REPL live in tcop/control.lisp ----

(defun main ()
  (multiple-value-bind (host port pgdata control-port control-socket)
      (parse-args (cdr sb-ext:*posix-argv*))
    (setf *pgdata* (directory-pathname pgdata))
    (capture-server-streams)
    (xact-init)
    (catalog-init)
    (enable-reload)
    (start-control-plane :port control-port :socket-path control-socket)
    (format t "~&pg-lisp: minimal PostgreSQL-compatible server~%")
    (format t "pg-lisp: data directory ~a~%" *pgdata*)
    (format t "pg-lisp: listening on ~a:~a~%" host port)
    (finish-output)
    (serve host port)))
