;;;; postgres.lisp -- backend main loop and connection lifecycle.

(in-package #:pg)

(defun call-with-live-operation (function)
  "Run one post-read validation phase under ordinary reload admission."
  (with-live-operation (funcall function)))

;;; ---- startup ----

(defun random-cancel-key ()
  "Return a 32-bit cancel key from the operating system's entropy source."
  (with-open-file (stream #p"/dev/urandom"
                          :direction :input
                          :element-type '(unsigned-byte 8))
    (let ((bytes (make-array 4 :element-type '(unsigned-byte 8))))
      (unless (= (read-sequence bytes stream) (length bytes))
        (error "could not read a cancel key from /dev/urandom"))
      (bytes-to-u32 bytes 0))))

(defun cancel-key-matches-p (received expected)
  ;; Compare every byte before testing the accumulated difference. PostgreSQL's
  ;; procsignal.c uses timingsafe_bcmp for the same credential check.
  (let ((difference 0))
    (dotimes (i 4)
      (setf difference
            (logior difference
                    (logxor (ldb (byte 8 (* i 8)) received)
                            (ldb (byte 8 (* i 8)) expected)))))
    (zerop difference)))

(defun process-cancel-request (body)
  "Cancel the named backend only when its PID and secret key both match."
  (let* ((pid (bytes-to-u32 body 4))
         (key (bytes-to-u32 body 8))
         (backend (lookup-backend pid)))
    (when (and backend
               (integerp (backend-cancel-key backend))
               (cancel-key-matches-p key (backend-cancel-key backend)))
      (cancel-backend backend)))
  (values))

(defun next-cstring (bytes start)
  (let ((end (position 0 bytes :start start)))
    (unless end
      (return-from next-cstring (values nil nil)))
    (values (bytes->string bytes :start start :end end) (1+ end))))

(defun parse-startup-params (body)
  (let ((params '())
        (i 4))
    (loop
      (when (>= i (length body))
        (ereport-fatal "08P01" "startup packet is missing its parameter terminator"))
      (when (zerop (aref body i))
        (unless (= i (1- (length body)))
          (ereport-fatal "08P01" "startup packet has bytes after its terminator"))
        (return))
      (multiple-value-bind (key next) (next-cstring body i)
        (unless key
          (ereport-fatal "08P01" "startup packet contains an unterminated parameter name"))
        (multiple-value-bind (value next2) (next-cstring body next)
          (unless value
            (ereport-fatal "08P01" "startup packet contains an unterminated parameter value"))
          (push (cons key value) params)
          (setf i next2))))
    (nreverse params)))

(defun read-startup-until-ready (stream &optional before-startup)
  "Consume SSL/GSS requests; return StartupMessage or NIL for a CancelRequest.

BEFORE-STARTUP runs after the request code is known to be something other than
CancelRequest, before the rest of that packet is read."
  (loop
    (multiple-value-bind (body cancel-request-p)
        (read-startup-packet stream #'call-with-live-operation
                             :before-body before-startup
                             :bypass-code +cancel-request-code+)
      (when (null body)
        (return-from read-startup-until-ready nil))
      ;; CancelRequest has no session state to register or modify. Its narrow
      ;; handler can run during transaction drain, and ASDF deferral captures
      ;; only a query that was already active when the request arrived.
      (when cancel-request-p
        (unless (= (length body) 12)
          (with-live-operation
            (ereport-fatal "08P01" "cancel request packet has an invalid length")))
        ;; This handler calls reloadable backend lookup and cancellation code.
        ;; Count it during drain; during ASDF, retain only a matching active
        ;; query token so an idle request cannot affect the backend's next query.
        (with-live-operation-cancel-request (body)
          (when *backend*
            (unregister-backend *backend*))
          (process-cancel-request body))
        (return-from read-startup-until-ready nil))
      ;; Packet reads stay outside the gate; validation and ordinary startup
      ;; processing are short and must not overlap live function redefinition.
      (with-live-operation
        (when (< (length body) 4)
          (ereport-fatal "08P01" "startup packet is missing its request code"))
        (let ((code (bytes-to-u32 body 0)))
          (cond
            ((= code +ssl-request-code+)
             (unless (= (length body) 4)
               (ereport-fatal "08P01" "SSL request packet has an invalid length"))
             (write-byte (char-code #\N) stream)
             (force-output stream))
            ((= code +gssenc-request-code+)
             (unless (= (length body) 4)
               (ereport-fatal "08P01" "GSS encryption request packet has an invalid length"))
             (write-byte (char-code #\N) stream)
             (force-output stream))
            (t
             (return-from read-startup-until-ready body))))))))

(defun process-startup (stream body)
  (with-live-operation
    (let* ((code (bytes-to-u32 body 0))
           (major (ash code -16)))
      (unless (= major 3)
        (ereport "0A000"
                 (format nil "unsupported frontend protocol ~a.~a"
                         major (logand code #xFFFF))))
      ;; Reject new clients once a shutdown is underway, before authentication,
      ;; exactly as PostgreSQL's canAcceptConnections() does.
      (unless (conns-allowed-p)
        (ereport-fatal "57P03" "the database system is shutting down"))
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
        (let ((key (random-cancel-key)))
          (setf (backend-cancel-key *backend*) key)
          (send-backend-key-data stream (backend-pid *backend*) key))
        (send-ready-for-query stream #\I)))))

;;; ---- query dispatch ----

(defun query-string (message)
  (let* ((payload (frontend-message-payload message))
         (end (or (position 0 payload) (length payload))))
    (bytes->string payload :start 0 :end end)))

(defun transaction-ending-statement-p (statement)
  (member (getf statement :type) '(:commit :rollback)))

(defun frontend-frame-envelope-valid-p (length-bytes payload)
  "Check only bounded frame shape needed to choose a reload-drain lane."
  (and length-bytes
       (= (length length-bytes) 4)
       (let ((length (bytes-to-u32 length-bytes 0)))
         (and (<= 4 length +max-query-message-size+)
              payload
              (= (length payload) (- length 4))))))

(defun simple-query-payload-starts-with-transaction-end-p
    (payload &optional (end (and payload
                                 (or (position 0 payload) (length payload)))))
  "Recognize a transaction-ending first command in raw query bytes.

This bounded byte-only check selects drain admission; full validation and
frontend-message construction happen after admission."
  (unless payload
    (return-from simple-query-payload-starts-with-transaction-end-p nil))
  (let ((start 0))
    (labels ((whitespace-byte-p (byte)
               (member byte '(9 10 11 12 13 32)))
             (word-byte-p (byte)
               (or (<= 65 byte 90) (<= 97 byte 122)
                   (<= 48 byte 57) (= byte 95) (= byte 36)
                   (>= byte 128)))
             (skip-line-comment ()
               (loop while (and (< start end) (/= (aref payload start) 10))
                     do (incf start)))
             (skip-block-comment ()
               (incf start 2)
               (loop while (< start end)
                     do (if (and (< (1+ start) end)
                                 (= (aref payload start) 42)
                                 (= (aref payload (1+ start)) 47))
                            (progn (incf start 2) (return))
                            (incf start))))
             (skip-leading-trivia ()
               (loop
                 (cond
                   ((and (< start end) (whitespace-byte-p (aref payload start)))
                    (incf start))
                   ((and (< (1+ start) end)
                         (= (aref payload start) 45)
                         (= (aref payload (1+ start)) 45))
                    (skip-line-comment))
                   ((and (< (1+ start) end)
                         (= (aref payload start) 47)
                         (= (aref payload (1+ start)) 42))
                    (skip-block-comment))
                   ((and (< start end) (= (aref payload start) 59))
                    (incf start))
                   (t (return)))))
             (matches-word-p (word from to)
               (and (= (- to from) (length word))
                    (loop for index from from below to
                          for expected across word
                          for byte = (aref payload index)
                          for upper = (if (<= 97 byte 122) (- byte 32) byte)
                          always (= upper (char-code expected))))))
      (skip-leading-trivia)
      (let ((word-start start))
        (loop while (and (< start end) (word-byte-p (aref payload start)))
              do (incf start))
        (let ((word-end start))
          ;; This byte-level check only decides whether the first command could
          ;; be a transaction end. The AST parser remains authoritative, and a
          ;; real statement before that command never enters the drain lane.
          (or (matches-word-p "COMMIT" word-start word-end)
              (matches-word-p "ROLLBACK" word-start word-end)
              (matches-word-p "END" word-start word-end)
              (matches-word-p "ABORT" word-start word-end)))))))

(defun simple-query-frame-needs-drain-p (length-bytes payload)
  "Route malformed query frames through drain so their fatal error can clean up.

The admitted decoder repeats these checks authoritatively."
  (or (not (frontend-frame-envelope-valid-p length-bytes payload))
      (let ((terminator (position 0 payload)))
        (or (not (and terminator
                      (= terminator (1- (length payload)))))
            (simple-query-payload-starts-with-transaction-end-p
             payload terminator)))))

(defun handle-simple-query-frame (stream type-byte length-bytes payload)
  (let* ((backend *backend*)
         (drain-command-p
           (simple-query-frame-needs-drain-p length-bytes payload))
         (query nil)
         (suffix-start nil)
         (prefix-before-reload nil))
    (unwind-protect
         (progn
           ;; Install cleanup before publishing the query token: a fatal
           ;; interrupt at any later point must not retain this backend.
           (begin-backend-query-execution backend)
           (handler-case
               (progn
                 ;; Only transactions and already-admitted sessions captured at
                 ;; gate closure can continue through the drain lane.
                 (with-live-operation-query-dispatch drain-command-p
                   (setf query
                         (query-string
                          (decode-frontend-message
                           type-byte length-bytes payload)))
                   (if (eq *live-operation-current-lane* :drain)
                       (multiple-value-bind (statements parsed-p)
                           ;; Parse the entire message before executing anything so
                           ;; syntax errors after COMMIT are found before commit.
                           (exec-simple-query
                            stream query :prepare-only-p t
                            :start-execution-p nil :finish-execution-p nil)
                         (cond
                           ((not parsed-p)
                            (send-ready-for-query
                             stream (transaction-status-char)))
                           (t
                            (let ((ending-position
                                    (position-if
                                     #'transaction-ending-statement-p statements)))
                              (if ending-position
                                  (let* ((prefix-end (1+ ending-position))
                                         (prefix (subseq statements 0 prefix-end)))
                                    (multiple-value-bind (result completed-p)
                                        (exec-simple-query
                                         stream query :statements prefix
                                         :start-execution-p nil
                                         :finish-execution-p nil)
                                      (declare (ignore result))
                                      (if (and completed-p
                                               (< prefix-end (length statements)))
                                          ;; Once COMMIT/ROLLBACK ends the captured
                                          ;; transaction, unrelated suffix work must
                                          ;; wait for normal admission.
                                          (setf suffix-start prefix-end
                                                prefix-before-reload prefix)
                                          (send-ready-for-query
                                           stream (transaction-status-char)))))
                                  ;; A captured transaction may need another query
                                  ;; to release an XID held by counted work.
                                  (progn
                                    (exec-simple-query
                                     stream query :statements statements
                                     :start-execution-p nil
                                     :finish-execution-p nil)
                                    (send-ready-for-query
                                     stream (transaction-status-char))))))))
                       (progn
                         (exec-simple-query
                          stream query
                          :start-execution-p nil :finish-execution-p nil)
                         (send-ready-for-query stream
                                               (transaction-status-char)))))
                 (when suffix-start
                   (with-live-operation
                     (multiple-value-bind (reparsed reparsed-p)
                         (exec-simple-query
                          stream query :prepare-only-p t
                          :start-execution-p nil :finish-execution-p nil)
                       (if reparsed-p
                           (let* ((ending-position
                                    (position-if
                                     #'transaction-ending-statement-p reparsed))
                                  (reparsed-prefix-end
                                    (and ending-position (1+ ending-position)))
                                  (reparsed-prefix
                                    (and reparsed-prefix-end
                                         (subseq reparsed 0 reparsed-prefix-end))))
                             (if (and reparsed-prefix-end
                                      (= reparsed-prefix-end suffix-start)
                                      (equal reparsed-prefix prefix-before-reload)
                                      (< reparsed-prefix-end (length reparsed)))
                                 ;; Use ASTs from the newly loaded parser. The
                                 ;; equality guard prevents skipping/replaying SQL.
                                 (exec-simple-query
                                  stream query :statements
                                  (nthcdr reparsed-prefix-end reparsed)
                                               :start-execution-p nil :finish-execution-p nil)
                                 (send-error-response
                                  stream
                                  (make-condition
                                   'pg-error :sqlstate "0A000"
                                             :message
                                             "query suffix changed during reload; retry it separately"))))
                           ;; PREPARE-ONLY already sent the parse error.
                           (values))
                       (send-ready-for-query stream (transaction-status-char))))))
             (pg-error (condition)
               (if (fatal-error-p condition)
                   (error condition)
                   (with-live-operation-captured-backend-cleanup
                     (note-query-error)
                     (send-error-response stream condition)
                     (send-ready-for-query stream (transaction-status-char)))))))
      (with-live-operation-query-state-cleanup
        (end-backend-query-execution backend)))))

(defun handle-simple-query (stream message)
  "Dispatch an already-constructed simple-query message.

This entry point is useful to in-image callers; the wire loop uses
HANDLE-SIMPLE-QUERY-FRAME so decoding occurs after admission."
  (let ((payload (frontend-message-payload message)))
    (handle-simple-query-frame
     stream (char-code (frontend-message-type message))
     (u32-to-bytes (+ 4 (length payload))) payload)))

(defun handle-unsupported-message (stream message)
  (declare (ignore message))
  (send-error-response
   stream
   (make-condition 'pg-error
                   :sqlstate "0A000"
                   :message "extended query protocol is not supported by this server"))
  (send-ready-for-query stream (transaction-status-char)))

(defun dispatch-frontend-frame (stream type-byte length-bytes payload)
  "Admit a raw frame before protocol validation and message construction."
  (cond
    ;; Query dispatch needs its byte-only classifier first so an already-active
    ;; transaction can still send COMMIT/ROLLBACK while reload drains.
    ((= type-byte (char-code #\Q))
     (handle-simple-query-frame stream type-byte length-bytes payload))
    ;; A captured active session may terminate during drain so its transaction
    ;; cleanup can complete. Idle sessions wait for ordinary admission.
    ((= type-byte (char-code #\X))
     (with-live-operation-query-dispatch t
       (decode-frontend-message type-byte length-bytes payload)
       :terminate))
    ((not (frontend-frame-envelope-valid-p length-bytes payload))
     ;; Bad frame shape must reach fatal validation during drain so it can
     ;; release any transaction holding up the reload.
     (with-live-operation-query-dispatch t
       (decode-frontend-message type-byte length-bytes payload)
       :continue))
    (t
     (with-live-operation
       (handle-unsupported-message
        stream (decode-frontend-message type-byte length-bytes payload))
       :continue))))

;;; ---- backend loop ----

(defun backend-loop (stream)
  (loop
    (multiple-value-bind (type-byte length-bytes payload)
        (read-message-frame stream)
      (when (null type-byte)
        (return))
      ;; Reading socket bytes and selecting the query path stay outside
      ;; admission. Validation, object construction and other dispatch enter it.
      (when (eq (dispatch-frontend-frame
                 stream type-byte length-bytes payload)
                :terminate)
        (return)))))

(defun finish-backend-connection (backend sock stream)
  "Send final connection cleanup through the backend's captured cleanup lane."
  (with-live-operation-backend-shutdown-cleanup (backend)
    (unwind-protect
         (close-connection sock stream)
      ;; Install accounting before unregistering: a concurrent administrator may
      ;; have retained this backend from a registry lookup and interrupt it
      ;; immediately after removal. Keep both transitions indivisible to that
      ;; asynchronous termination.
      (sb-sys:without-interrupts
        (unwind-protect
             (when backend
               (unregister-backend backend))
          (connection-finished))))))

(defun handle-connection (sock)
  (let ((stream nil)
        (backend nil))
    (unwind-protect
         (handler-case
             (progn
               (setf stream (socket->stream sock))
               (let ((*backend* (make-backend :stream stream
                                              :database "postgres"
                                              :thread sb-thread:*current-thread*
                                              :pid (sb-thread:thread-os-tid
                                                    sb-thread:*current-thread*)))
                     (*output-stream* stream))
                 (setf backend *backend*)
                 ;; Track the connection before any blocking startup read so
                 ;; fast shutdown can interrupt a client stalled in its first
                 ;; header. CancelRequest removes this provisional backend once
                 ;; its request code is known.
                 (register-backend backend)
                 ;; A dropped connection must not strand an open transaction.
                 (unwind-protect
                      (let ((body (read-startup-until-ready stream)))
                        (when body
                          (process-startup stream body)
                          (backend-loop stream)))
                   ;; Fast shutdown terminates every backend and exits the
                   ;; process, so startup recovery can classify in-progress
                   ;; CLOG entries as aborted. Smart shutdown still permits
                   ;; sessions to continue and must release disconnected XIDs.
                   (when (and (< (shutdown-mode) +fast-shutdown+)
                              *backend*
                              (or (backend-xid *backend*)
                                  (not (eq (backend-state *backend*) :idle))))
                     (with-live-operation-transaction-drain
                       (end-backend-session))))))
           (pg-error (e)
             (with-live-operation-backend-shutdown-cleanup (backend)
               (when stream (ignore-errors (send-error-response stream e)))))
           (error (e)
             (with-live-operation-backend-shutdown-cleanup (backend)
               (when stream
                 (ignore-errors
                  (send-error-response
                   stream
                   (make-condition 'pg-error :sqlstate "XX000"
                                             :message (princ-to-string e))))))))
      ;; Send the final error and close the socket *before* unregistering:
      ;; during a fast shutdown the last connection's CONNECTION-FINISHED exits
      ;; the process, and the client must still receive its FATAL.
      (finish-backend-connection backend sock stream))))

;;; ---- server main ----

(defun arg-value (rest arg)
  "Return the value following ARG in REST, or fail with a message."
  (or (cadr rest)
      (error "option ~a requires a value" arg)))

(defun parse-port-arg (arg value)
  "Parse VALUE as a TCP port for option ARG, enforcing 1..65535."
  (let ((n (ignore-errors (parse-integer value))))
    (unless (and n (<= 1 n 65535))
      (error "option ~a expects a port in 1..65535, got ~s" arg value))
    n))

(defun parse-query-temp-file-limit-bytes (value)
  "Parse a nonnegative byte cap, or -1 for PostgreSQL-style unlimited space."
  (let ((limit (ignore-errors (parse-integer value))))
    (unless (and limit (>= limit -1))
      (error "--temp-file-limit-bytes expects a nonnegative integer or -1, got ~s"
             value))
    (unless (= limit -1)
      limit)))

(defun parse-query-sort-arena-limit-bytes (value)
  "Parse an optional experimental sort arena capacity (-1 disables it)."
  (let ((limit (ignore-errors (parse-integer value))))
    (unless (and limit
                 (or (= limit -1) (<= 65536 limit most-positive-fixnum)))
      (error "--sort-arena-limit-bytes expects -1 or an integer in 65536..~d, got ~s"
             most-positive-fixnum value))
    (unless (= limit -1) limit)))

(defun parse-bind-address-arg (arg value)
  "Parse a non-empty listener address for option ARG."
  (unless (and (stringp value) (plusp (length value)))
    (error "option ~a expects a non-empty bind address" arg))
  value)

(defun parse-args (argv)
  (let ((port 5433)
        (host "127.0.0.1")
        (pgdata nil)
        (control-port nil)
        (control-socket nil)
        (swank-port nil)
        (swank-bind-address "127.0.0.1")
        (temp-file-limit-argument nil)
        (sort-arena-limit-argument nil))
    (loop for rest on argv
          for arg = (car rest)
          do (cond
               ((member arg '("-p" "--port") :test #'string=)
                (setf port (parse-port-arg arg (arg-value rest arg))))
               ((and (> (length arg) 7) (string= arg "--port=" :end1 7))
                (setf port (parse-port-arg "--port" (subseq arg 7))))
               ((member arg '("-h" "--host") :test #'string=)
                (setf host (arg-value rest arg)))
               ((member arg '("-D" "--pgdata") :test #'string=)
                (setf pgdata (arg-value rest arg)))
               ((and (> (length arg) 9) (string= arg "--pgdata=" :end1 9))
                (setf pgdata (subseq arg 9)))
               ((member arg '("-C" "--control-port") :test #'string=)
                (setf control-port (parse-port-arg arg (arg-value rest arg))))
               ((and (> (length arg) 15) (string= arg "--control-port=" :end1 15))
                (setf control-port (parse-port-arg "--control-port" (subseq arg 15))))
               ((string= arg "--control-socket")
                (setf control-socket (arg-value rest arg)))
               ((and (> (length arg) 17) (string= arg "--control-socket=" :end1 17))
                (setf control-socket (subseq arg 17)))
               ((string= arg "--swank-port")
                (setf swank-port (parse-port-arg arg (arg-value rest arg))))
               ((and (>= (length arg) 13) (string= arg "--swank-port=" :end1 13))
                (setf swank-port (parse-port-arg "--swank-port" (subseq arg 13))))
               ((string= arg "--swank-bind-address")
                (setf swank-bind-address
                      (parse-bind-address-arg
                       arg (arg-value rest arg))))
               ((and (>= (length arg) 21)
                     (string= arg "--swank-bind-address=" :end1 21))
                (setf swank-bind-address
                      (parse-bind-address-arg
                       "--swank-bind-address" (subseq arg 21))))
               ((string= arg "--temp-file-limit-bytes")
                (setf temp-file-limit-argument (arg-value rest arg)))
               ((and (>= (length arg)
                         (length "--temp-file-limit-bytes="))
                     (string= "--temp-file-limit-bytes=" arg
                              :end2 (length "--temp-file-limit-bytes=")))
                (setf temp-file-limit-argument
                      (subseq arg (length "--temp-file-limit-bytes="))))
               ((string= arg "--sort-arena-limit-bytes")
                (setf sort-arena-limit-argument (arg-value rest arg)))
               ((and (>= (length arg) (length "--sort-arena-limit-bytes="))
                     (string= "--sort-arena-limit-bytes=" arg
                              :end2 (length "--sort-arena-limit-bytes=")))
                (setf sort-arena-limit-argument
                      (subseq arg (length "--sort-arena-limit-bytes="))))))
    (values host port (or pgdata (sb-ext:posix-getenv "PGDATA") "./pgdata")
            control-port control-socket swank-port swank-bind-address
            (let ((value (or temp-file-limit-argument
                             (sb-ext:posix-getenv
                              "PG_QUERY_TEMP_FILE_LIMIT_BYTES"))))
              (if value
                  (parse-query-temp-file-limit-bytes value)
                  *query-temp-file-limit-bytes*))
            (let ((value (or sort-arena-limit-argument
                             (sb-ext:posix-getenv
                              "PG_QUERY_SORT_ARENA_LIMIT_BYTES"))))
              (if value
                  (parse-query-sort-arena-limit-bytes value)
                  *query-sort-arena-limit-bytes*)))))

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
    (sb-thread:with-mutex (*accept-lock*)
      (setf *accept-stop-requested* nil))
    (loop
      (let ((ready (sb-sys:wait-until-fd-usable
                    (sb-bsd-sockets:socket-file-descriptor listener)
                    :input 0.1 nil))
            (sock nil)
            (stop nil)
            (accept-error nil))
        ;; ACCEPT is called only after readiness and while holding the same
        ;; lock that shutdown uses to stop the listener.  Counting the socket
        ;; in this critical section closes the accept/accounting race.
        (sb-thread:with-mutex (*accept-lock*)
          (setf stop *accept-stop-requested*)
          (when (and ready (not stop))
            (handler-case
                (setf sock (accept-connection listener))
              (error (e)
                (setf accept-error e)))
            (when sock
              (connection-started))))
        (when stop
          (return))
        (when accept-error
          (format *error-output* "~&pg-lisp: backend accept: ~a~%" accept-error)
          (sleep 0.1))
        (when sock
          (handler-case
              (sb-thread:make-thread
               (lambda ()
                 (let ((*query-temp-session-id*
                         (next-query-temp-session-id)))
                   (handle-connection sock)))
               :name "pg backend")
            (error (e)
              (format *error-output* "~&pg-lisp: backend: ~a~%" e)
              (connection-finished)
              (ignore-errors (close-connection sock nil)))))))))

;;; ---- hot reload (SIGHUP) and control REPL live in tcop/control.lisp ----

(defun main ()
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port swank-bind-address temp-file-limit-bytes
                        sort-arena-limit-bytes)
      (parse-args (cdr sb-ext:*posix-argv*))
    (setf *pgdata* (directory-pathname pgdata)
          *query-temp-file-limit-bytes* temp-file-limit-bytes
          *query-sort-arena-limit-bytes* sort-arena-limit-bytes)
    ;; Reject an unsupported SBCL before opening storage or accepting clients.
    (call-with-query-sort-arena (lambda (arena) (declare (ignore arena))))
    (initialize-query-temp-storage)
    (capture-server-streams)
    (wal-init)
    (xact-init)
    (wal-recover)
    (wal-load-replication-slots)
    (catalog-init)
    (wal-remove-orphan-relations)
    (complete-xid-wraparound)
    (enable-reload)
    (enable-shutdown-handlers)
    (start-swank-listener swank-port swank-bind-address)
    (start-control-plane :port control-port :socket-path control-socket)
    (format t "~&pg-lisp: minimal PostgreSQL-compatible server~%")
    (format t "pg-lisp: data directory ~a~%" *pgdata*)
    (format t "pg-lisp: listening on ~a:~a~%" host port)
    (finish-output)
    (serve host port)))
