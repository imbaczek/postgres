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

(defun startup-replication-mode (value)
  (cond
    ((null value) nil)
    ((string= value "database") :database)
    (t
     (handler-case
         (when (startup-parse-boolean-setting value) :physical)
       (error ()
         (ereport-fatal "22023" "invalid value for parameter \"replication\": \"~a\"" value
                        :hint "Valid values are: \"false\", 0, \"true\", 1, \"database\"."))))))

(defun startup-replication-parameters-mode (params)
  "Resolve repeated replication parameters using their startup packet order.
An enabled database mode stays selected if a later enabled value is physical."
  (let ((enabled nil) (database-p nil))
    (dolist (pair params)
      (when (string= "replication" (car pair))
        (let ((mode (startup-replication-mode (cdr pair))))
          (setf enabled (not (null mode)))
          (when (eq mode :database) (setf database-p t)))))
    (when enabled (if database-p :database :physical))))

(defvar *hot-standby-reported-backends* (make-hash-table :test #'eq :weakness :key))

(defvar *hot-standby-report-lock* (sb-thread:make-mutex :name "hot standby status reports"))

(defun report-hot-standby-status (stream &optional initial)
  "Report startup recovery status and its later change to off on this connection.
Call under admission, before query execution. Remember only connections that
were told on, using weak keys so disconnected backends are not retained. Send
outside the report lock; a blocked client must not hold up other reports.
Like C ReportChangedGUCOptions, recovery can only change from on to off."
  (let* ((standby (recovery-in-progress-p))
         (report (sb-thread:with-mutex (*hot-standby-report-lock*)
                   (prog1 (or initial
                              (and (not standby)
                                   (gethash *backend* *hot-standby-reported-backends*)))
                     (if standby
                         (when initial (setf (gethash *backend* *hot-standby-reported-backends*) t))
                         (remhash *backend* *hot-standby-reported-backends*))))))
    (when report (send-parameter-status stream "in_hot_standby" (if standby "on" "off")))))

(defun admit-startup-backend-with-configuration (backend configuration peer)
  (postmaster-admit-startup-backend
   backend (startup-configuration-max-connections configuration)
   (startup-configuration-max-senders configuration) peer))

(defun authorize-and-admit-startup-backend (backend configuration user database peer)
  "Admit a trusted startup or return :SCRAM before claiming any quota."
  (if (eq :scram (authentication-authorize (startup-configuration-authentication configuration)
                                           user database peer (backend-replication-mode backend)))
      :scram
      (progn
        (authentication-check-role-admission user (backend-replication-mode backend))
        (admit-startup-backend-with-configuration backend configuration peer))))

(defun finish-startup (stream params dbname)
  "Send startup parameters for an admitted backend."
  (send-auth-ok stream)
  (session-apply-startup-options params)
  (with-catalog-lock
    (unless (or (eq (backend-replication-mode *backend*) :physical)
                (gethash dbname (catalog-databases *catalog*)))
      (when (or (recovery-in-progress-p) (rewind-restricted-sql-session-p))
        (ereport-fatal "3D000" "database ~s does not exist" dbname))
      (get-database dbname)
      (save-catalog)))
  (send-parameter-status stream "server_version" *server-version*)
  (session-report-parameter stream "search_path" (session-search-path))
  (send-parameter-status stream "server_version_num" (princ-to-string *server-version-num*))
  (send-parameter-status stream "server_encoding" "UTF8")
  (send-parameter-status stream "client_encoding" "UTF8")
  (session-report-parameter stream "DateStyle" (session-extra-setting-value "datestyle"))
  (session-report-parameter stream "TimeZone" (or (gethash "timezone" (backend-settings *backend*)) "UTC"))
  (send-parameter-status stream "integer_datetimes" "on")
  (send-parameter-status stream "standard_conforming_strings" "on")
  (send-parameter-status stream "IntervalStyle" "postgres")
  (let ((user (backend-startup-user *backend*)))
    (let ((role (authentication-committed-role user)))
      (send-parameter-status stream "is_superuser" (if (or (null role) (pg-role-superuser-p role)) "on" "off"))
      (send-parameter-status stream "session_authorization" (if role user (current-user-name)))))
  (session-report-parameter stream "application_name" (sql-setting-value "application_name"))
  (send-parameter-status stream "default_transaction_read_only" "off")
  (report-hot-standby-status stream t)
  (let ((key (random-cancel-key)))
    (setf (backend-cancel-key *backend*) key)
    (send-backend-key-data stream (backend-pid *backend*) key))
  (send-ready-for-query stream #\I))

(defun process-startup (stream body)
  "Check startup and select HBA under admission.
Trust reserves quota immediately. SCRAM releases admission for the network
exchange, then rechecks the configuration before reserving quota.
Capture the replication source identity for later sender registration."
  (multiple-value-bind (configuration params user dbname peer)
      (with-live-operation
        (let* ((configuration (startup-require-configuration))
               (code (bytes-to-u32 body 0))
               (major (ash code -16)))
          (unless (= major 3)
            (ereport "0A000"
                     (format nil "unsupported frontend protocol ~a.~a" major (logand code #xFFFF))))
          (unless (conns-allowed-p)
            (ereport-fatal "57P03" "the database system is shutting down"))
          (let* ((params (parse-startup-params body))
                 (replication (startup-replication-parameters-mode params))
                 (app (or (startup-last-parameter params "application_name") "")))
            (multiple-value-bind (user dbname) (authentication-startup-identities params replication)
              (setf (backend-startup-user *backend*) user
                    (backend-database *backend*) dbname
                    (backend-application-name *backend*) app
                    (backend-replication-mode *backend*) replication
                    (backend-replication-source-id *backend*)
                    (when replication (nth-value 0 (sync-replication-configuration-state))))
              (when (and (recovery-in-progress-p) (not (startup-configuration-hot-standby configuration)))
                (ereport-fatal "57P03" "the database system is not accepting connections"
                               :detail "Hot standby mode is disabled."))
              (let ((peer (sb-bsd-sockets:socket-peername (backend-socket *backend*))))
                (unless (eq :scram (authorize-and-admit-startup-backend
                                    *backend* configuration user dbname peer))
                  (return-from process-startup (finish-startup stream params dbname)))
                (values configuration params user dbname peer))))))
    (handler-case
        (authenticate-role-scram stream user)
      (sb-ext:timeout ()
        (ereport-fatal "57014" "canceling authentication due to timeout")))
    (with-live-operation
      (unless (and (conns-allowed-p) (eq configuration (startup-require-configuration)))
        (ereport-fatal "57P03" "startup configuration changed; retry the connection"))
      (authentication-check-role-admission user (backend-replication-mode *backend*))
      (admit-startup-backend-with-configuration *backend* configuration peer)
      (finish-startup stream params dbname))))

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
              (= (if (extended-bind-spool-p payload)
                     (extended-bind-spool-size payload)
                     (length payload))
                 (- length 4))))))

(defun simple-query-payload-starts-with-word-p
    (payload words &optional (end (and payload
                                       (or (position 0 payload) (length payload)))) (sql-trivia-p t))
  "Classify a raw query's first word before reload admission and full decoding."
  (unless payload
    (return-from simple-query-payload-starts-with-word-p nil))
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
                         (and sql-trivia-p (= (aref payload start) 45))
                         (= (aref payload (1+ start)) 45))
                    (skip-line-comment))
                   ((and (< (1+ start) end)
                         (and sql-trivia-p (= (aref payload start) 47))
                         (= (aref payload (1+ start)) 42))
                    (skip-block-comment))
                   ((and sql-trivia-p (< start end) (= (aref payload start) 59))
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
        (some (lambda (word) (matches-word-p word word-start start)) words)))))

(defun simple-query-payload-starts-with-transaction-end-p
    (payload &optional (end (and payload
                                 (or (position 0 payload) (length payload)))))
  "Recognize a possible transaction end; the SQL parser remains authoritative."
  (simple-query-payload-starts-with-word-p
   payload '("COMMIT" "ROLLBACK" "END" "ABORT") end))

(defun replication-query-payload-p (payload)
  "Match repl_scanner's first-token dispatch: skip whitespace, not SQL comments.
Database walsenders fall back to SQL for a leading comment or empty statement."
  ;; Keep lease verbs in this early dispatch list as well as the protocol
  ;; parser table so database walsenders route them to the replication parser.
  ;; This includes selected cleanup before full command parsing.
  (simple-query-payload-starts-with-word-p
   payload '("IDENTIFY_SYSTEM" "CREATE_REPLICATION_SLOT" "DROP_REPLICATION_SLOT"
             "START_REPLICATION" "READ_REPLICATION_SLOT" "TIMELINE_HISTORY" "PREPARE_FOLLOW" "PROVE_RETAINED" "RELEASE_FOLLOW" "RELEASE_BOOTSTRAP"
             "BASE_BACKUP" "BASE_BACKUP_NATIVE" "CAPTURE_BACKUP_NATIVE" "BACKUP_LEASE_STATUS" "BACKUP_LEASE_ABORT" "BACKUP_LEASE_HANDOFF" "BACKUP_LEASE_CLEANUP_SELECTED" "BACKUP_LEASE_PREPARE_FOLLOW" "BACKUP_LEASE_CANCEL_FOLLOW"
             "BACKUP_LEASE_PROVE_REWIND" "CAPTURE_REWIND_NATIVE" "REPLAY_REWIND_NATIVE" "IDENTIFY_REWIND_ORIGIN"
             "UPLOAD_MANIFEST" "ALTER_REPLICATION_SLOT" "SHOW")
   (and payload (or (position 0 payload) (length payload))) nil))

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
    (with-backend-query-execution (:backend backend :cleanup :query-state)
      (progn
        (handler-case
            (progn
              ;; Only transactions and already-admitted sessions captured at
              ;; gate closure can continue through the drain lane.
              (with-live-operation-query-dispatch drain-command-p
                (report-hot-standby-status stream)
                (setf query
                      (query-string
                       (decode-frontend-message
                        type-byte length-bytes payload)))
                (when *extended-query-state*
                  (extended-drop-portal *extended-query-state* "")
                  (remhash "" (extended-query-state-statements *extended-query-state*)))
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
      )))

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

(defun handle-extended-query-frame (stream type-byte length-bytes payload)
  ;; Keep the cycle deadline across a suspended Execute; Sync or a completed
  ;; Execute ends it. Errors discard execution state before waiting for Sync.
  (let ((state *extended-query-state*) (*extended-execute-suspended-p* nil))
    (with-backend-query-execution (:cleanup :query-state)
      (with-live-operation-query-dispatch t
        (handler-case
            (progn
              (when (extended-query-state-statement-deadline state)
                (setf (backend-statement-deadline *backend*)
                      (extended-query-state-statement-deadline state)))
              (if (extended-bind-spool-p payload)
                  (unless (and (= type-byte (char-code #\B))
                               (frontend-frame-envelope-valid-p length-bytes payload))
                    (malformed-protocol-message "invalid spooled Bind frame"))
                  (decode-frontend-message type-byte length-bytes payload))
              (when (and (eq *live-operation-current-lane* :drain)
                         (/= type-byte (char-code #\S)))
                (ereport "0A000" "extended query cannot start work during reload drain"))
              (arm-statement-timeout)
              (check-for-interrupts)
              (unless (= type-byte (char-code #\S))
                (setf (extended-query-state-in-cycle-p state) t))
              (unless (and (extended-query-state-ignore-until-sync state)
                           (/= type-byte (char-code #\S)))
                (extended-apply-operation stream
                                          (if (extended-bind-spool-p payload)
                                              (progn
                                                (extended-verify-bind-spool payload)
                                                (extended-parse-bind-spool payload))
                                              (extended-parse-frame (code-char type-byte) payload))
                                          payload))
              (setf (extended-query-state-statement-deadline state)
                    (unless (or (= type-byte (char-code #\S))
                                (and (= type-byte (char-code #\E))
                                     (not *extended-execute-suspended-p*)))
                      (backend-statement-deadline *backend*))))
          (error (condition)
            (when (fatal-error-p condition) (error condition))
            (let ((implicit (extended-query-state-implicit-p state)))
              (note-query-error)
              (when *physical-slot-cold-cleanup-owner*
                (send-error-response stream condition)
                (finish-output stream)
                (return-from handle-extended-query-frame :terminate))
              (setf (extended-query-state-statement-deadline state) nil
                    (extended-query-state-executed-in-cycle-p state) nil)
              (let ((*preserve-failed-extended-portals* (eq (backend-state *backend*) :aborted)))
                (extended-clear-portals-for-backend *backend* :discard-observations-p t))
              (when implicit
                (unless (eq (backend-state *backend*) :idle) (abort-transaction))
                (restore-session-settings stream))
              (setf (extended-query-state-implicit-p state) nil
                    (extended-query-state-ignore-until-sync state) t)
              (send-error-response stream condition)
              (when (= type-byte (char-code #\S))
                (setf (extended-query-state-ignore-until-sync state) nil
                      (extended-query-state-in-cycle-p state) nil)
                (send-ready-for-query stream (transaction-status-char)))))))))
  :continue)

(defun handle-extended-implicit-simple-query (stream type-byte length-bytes payload)
  "Run a Simple Query while an extended implicit transaction is still open."
  (let ((state *extended-query-state*) (backend *backend*) (completed nil) (cleaned nil))
    (with-backend-query-execution (:backend backend :cleanup :query-state)
      (with-live-operation-query-dispatch t
        (let* ((message (decode-frontend-message type-byte length-bytes payload))
               (query (query-string message)))
          (extended-drop-portal state "")
          (remhash "" (extended-query-state-statements state))
          (setf (extended-query-state-statement-deadline state) nil)
          (multiple-value-bind (statements parsed-p)
              (exec-simple-query stream query :prepare-only-p t
                                              :start-execution-p nil :finish-execution-p nil)
            (when (and parsed-p
                       (not (every (lambda (statement)
                                     (member (getf statement :type)
                                             '(:select :insert :update :delete :set :reset :show :do
                                               :begin :commit :rollback)))
                                   statements)))
              (setf parsed-p nil)
              (send-error-response stream
                                   (make-condition 'pg-error :sqlstate "0A000"
                                                             :message "utility command in an extended implicit transaction is not supported")))
            (when (and parsed-p (eq *live-operation-current-lane* :drain))
              (let ((ending (position-if #'transaction-ending-statement-p statements)))
                (when (and ending (< (1+ ending) (length statements)))
                  (setf parsed-p nil)
                  (send-error-response stream
                                       (make-condition 'pg-error :sqlstate "0A000"
                                                                 :message "mixed query suffix cannot run during reload drain")))))
            (when parsed-p
              (let ((*extended-simple-multi-p* (not (null (rest statements))))
                    (*deferred-command-completes* nil))
                (multiple-value-bind (result ok)
                    (exec-simple-query stream query :statements statements
                                                    :start-execution-p nil :finish-execution-p nil)
                  (declare (ignore result))
                  (setf completed ok))
                (when completed
                  (handler-case
                      (progn
                        (when (and *extended-simple-multi-p*
                                   (eq (backend-state backend) :in-progress)
                                   (not (backend-explicit backend)))
                          (commit-transaction))
                        (dolist (response (nreverse *deferred-command-completes*))
                          (apply #'send-command-complete response)))
                    (error (condition)
                      (when (fatal-error-p condition) (error condition))
                      (setf completed nil)
                      (unless (backend-explicit backend)
                        (unless (eq (backend-state backend) :idle) (abort-transaction))
                        (restore-session-settings stream))
                      (let ((*preserve-failed-extended-portals* (eq (backend-state *backend*) :aborted)))
                        (extended-clear-portals-for-backend *backend*))
                      (setf (extended-query-state-implicit-p state) nil cleaned t)
                      (send-error-response stream
                                           (if (typep condition 'pg-error) condition
                                               (make-condition 'pg-error :sqlstate "XX000"
                                                                         :message (princ-to-string condition)))))))))
            (unless (or completed cleaned)
              (unless (backend-explicit backend)
                (unless (eq (backend-state backend) :idle) (abort-transaction))
                (restore-session-settings stream)))
            (when (or (not completed) (eq (backend-state backend) :idle)
                      (backend-explicit backend))
              (setf (extended-query-state-implicit-p state) nil
                    (extended-query-state-executed-in-cycle-p state) nil)
              (when (or (not completed) (eq (backend-state backend) :idle))
                (let ((*preserve-failed-extended-portals* (eq (backend-state *backend*) :aborted)))
                  (extended-clear-portals-for-backend *backend*))))
            (send-ready-for-query stream (transaction-status-char)))))))
  :continue)

(defun %dispatch-frontend-frame (stream type-byte length-bytes payload)
  "Admit a raw frame before protocol validation and message construction."
  (when (and (eq (backend-replication-mode *backend*) :physical)
             (not (string= (backend-startup-user *backend*) "postgres"))
             (not (member (code-char type-byte) '(#\Q #\X))))
    (ereport-fatal "08P01" "physical replication requires simple-query protocol"))
  (cond
    ((and *extended-query-state*
          (extended-query-state-ignore-until-sync *extended-query-state*)
          (not (member (code-char type-byte) '(#\S #\X))))
     (unless (frontend-frame-envelope-valid-p length-bytes payload)
       (with-live-operation-query-dispatch t
         (decode-frontend-message type-byte length-bytes payload)))
     :continue)
    ;; Query dispatch needs its byte-only classifier first so an already-active
    ;; transaction can still send COMMIT/ROLLBACK while reload drains.
    ((and (= type-byte (char-code #\Q))
          (member (backend-replication-mode *backend*) '(:database :physical))
          (or (eq (backend-replication-mode *backend*) :physical)
              (replication-query-payload-p payload)))
     (handle-replication-query-frame stream type-byte length-bytes payload)
     :continue)
    ((= type-byte (char-code #\Q))
     (when (and *extended-query-state* (extended-query-state-implicit-p *extended-query-state*)
                (not (extended-query-state-executed-in-cycle-p *extended-query-state*)))
       (with-live-operation-query-dispatch t
         (decode-frontend-message type-byte length-bytes payload)
         (when (eq (backend-state *backend*) :in-progress) (commit-transaction))
         (setf (extended-query-state-implicit-p *extended-query-state*) nil
               (extended-query-state-executed-in-cycle-p *extended-query-state*) nil)
         (let ((*preserve-failed-extended-portals* (eq (backend-state *backend*) :aborted)))
           (extended-clear-portals-for-backend *backend*))))
     (if (and *extended-query-state* (extended-query-state-implicit-p *extended-query-state*))
         (handle-extended-implicit-simple-query stream type-byte length-bytes payload)
         (prog1 (handle-simple-query-frame stream type-byte length-bytes payload)
           (when *extended-query-state*
             (setf (extended-query-state-statement-deadline *extended-query-state*) nil))
           (when (and *extended-query-state*
                      (member (backend-state *backend*) '(:idle :aborted)))
             (when (eq (backend-state *backend*) :idle)
               (setf (extended-query-state-implicit-p *extended-query-state*) nil))
             (let ((*preserve-failed-extended-portals* (eq (backend-state *backend*) :aborted)))
               (extended-clear-portals-for-backend *backend*))))))
    ((find (code-char type-byte) "PBESCHD")
     (handle-extended-query-frame stream type-byte length-bytes payload))
    ;; A captured active session may terminate during drain so its transaction
    ;; cleanup can complete. Idle sessions wait for ordinary admission.
    ((= type-byte (char-code #\X))
     (with-live-operation-query-dispatch t
       (decode-frontend-message type-byte length-bytes payload)
       :terminate))
    ((and *extended-query-state* (extended-query-state-in-cycle-p *extended-query-state*))
     (handle-extended-query-frame stream type-byte length-bytes payload))
    ((not (frontend-frame-envelope-valid-p length-bytes payload))
     ;; Bad frame shape must reach fatal validation during drain so it can
     ;; release any transaction holding up the reload.
     (with-live-operation-query-dispatch t
       (decode-frontend-message type-byte length-bytes payload)
       :continue))
    (t
     (with-live-operation
       (decode-frontend-message type-byte length-bytes payload)
       (malformed-protocol-message "unsupported frontend message type")))))

(defun dispatch-frontend-frame (stream type-byte length-bytes payload)
  "Dispatch a frame, then drain recycling without waiting for admission or owner exit.
Preserve all dispatch values, including secondary values, for the caller."
  (let* ((*extended-query-state*
           (or *extended-query-state* (extended-query-state-for-backend *backend*)))
         (results (multiple-value-list
                   (%dispatch-frontend-frame stream type-byte length-bytes payload))))
    (unless (eq (first results) :terminate) (wal-finish-pending-slot-recycling))
    (values-list results)))

;;; ---- backend loop ----

(defun backend-loop (stream)
  "Process frontend messages until termination or suppressed output closes the session."
  (loop
    (when *physical-slot-cold-cleanup-owner* (return))
    (when (wal-sender-backend-stopping-p *backend*)
      (ereport-fatal "57P01" "WAL sender is stopping"))
    (check-backend-frontend-output)
    (let ((type-byte (with-backend-client-read-wait
                       (read-byte stream nil nil))))
      (unless type-byte (return))
      (multiple-value-bind (length-bytes got)
          (with-backend-client-read-wait
            (read-exact-bytes stream 4))
        (declare (ignore got))
        (let ((length (and length-bytes (bytes-to-u32 length-bytes 0))))
          (if (and (= type-byte (char-code #\B)) length
                   (<= 4 length +max-query-message-size+)
                   (> length (* 1024 1024)))
              (let ((complete nil))
                (unwind-protect
                     (call-with-extended-bind-spool
                      stream (- length 4)
                      (lambda (spool crc)
                        (setf complete t)
                        (when (eq (dispatch-frontend-frame
                                   stream type-byte length-bytes
                                   (make-extended-bind-spool :stream spool :size (- length 4) :crc crc))
                                  :terminate)
                          (return))))
                  (unless complete (close stream :abort t))))
              (let ((payload (when (and length (<= 4 length +max-query-message-size+))
                               (with-backend-client-read-wait
                                 (read-exact-bytes stream (- length 4))))))
                (when (eq (dispatch-frontend-frame stream type-byte length-bytes payload) :terminate)
                  (return)))))))))

(defun finish-backend-connection (backend sock stream)
  "Finish through the backend's captured cleanup lane.
Always unregister after backend cleanup, even if cleanup exits abnormally, and
finish outer connection accounting after unregistering."
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
               (unwind-protect
                    (progn
                      (wal-sender-unregister backend)
                      (forget-extended-query-state backend)
                      (wal-cleanup-backend-physical-slots backend)
                      (logical-release-subscription-owner backend))
                 (unregister-backend backend)))
          (connection-finished))))))

(defun handle-connection (sock)
  (let ((stream nil)
        (backend nil))
    (unwind-protect
         (handler-case
             (progn
               (setf stream (socket->stream sock))
               (let ((*backend* (make-backend :stream stream
                                              :socket sock
                                              :database "postgres"
                                              :thread sb-thread:*current-thread*
                                              :pid (sb-thread:thread-os-tid
                                                    sb-thread:*current-thread*)))
                     (*output-stream* stream)
                     (*sql-prng-state* nil))
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
                          ;; Use the startup identity even if the source changed before registration.
                          (when (backend-replication-mode *backend*)
                            (wal-sender-register *backend* (backend-replication-source-id *backend*)))
                          (backend-loop stream)))
                   ;; Fast teardown cannot wait behind a closed reload gate.
                   ;; MAIN retires its captured transaction after all clients exit.
                   (when (and *backend*
                              (or (backend-xid *backend*)
                                  (not (eq (backend-state *backend*) :idle))))
                     (cond
                       (*physical-slot-cold-cleanup-owner* nil)
                       ((>= (shutdown-mode) +fast-shutdown+)
                        (postmaster-remember-abandoned-backend *backend*))
                       (t
                        (with-live-operation-cold-cleanup-fallback ()
                          (with-live-operation-transaction-drain
                            (end-backend-session)))))))))
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
      (unwind-protect
           (finish-backend-connection backend sock stream)
        (request-cold-slot-reconciliation-exit backend)))))

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

(defun parse-slot-wal-keep-bytes (value)
  "Parse the per-slot WAL retention cap; -1 explicitly disables it."
  (let ((limit (ignore-errors (parse-integer value))))
    (unless (and limit (>= limit -1))
      (error "--max-slot-wal-keep-bytes expects a nonnegative integer or -1, got ~s"
             value))
    (unless (= limit -1) limit)))

(defun parse-bind-address-arg (arg value)
  "Parse a non-empty listener address for option ARG."
  (unless (and (stringp value) (plusp (length value)))
    (error "option ~a expects a non-empty bind address" arg))
  value)

(defun parse-args (argv)
  (startup-argument-values argv))

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
    (postmaster-note-listener listener)
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
          (setf stop (or *accept-stop-requested* *physical-slot-cold-cleanup-owner*))
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

(defun startup-check-retired-storage ()
  ;; Require the primary writer to be retired and any page, CLOG, or WAL
  ;; temporary cleanup work to be finished before startup.
  (require-primary-wal-writer-retired)
  (wal-require-no-pending-storage)
  (sb-thread:with-mutex (*wal-lock*)
    (wal-require-no-temp-cleanup-under-lock))
  (values))

(defun check-primary-storage-startup-context (gate-owner ownership)
  ;; The drained gate, postmaster ownership, absence of retained readers, and
  ;; retired storage are separate requirements.  Ownership of PGDATA alone
  ;; does not establish source, history, or lifetime eligibility.
  (check-live-operation-startup-quiescence gate-owner)
  (postmaster-check-ownership ownership)
  (when (physical-recovery-reader-conflict-p)
    (error "primary storage startup requires no retained readers or idle transactions"))
  (startup-check-retired-storage)
  (values))

(defun check-primary-feedback-after-replay (reset-boundary)
  "Reject retained feedback when startup cannot account for its transaction status."
  (let* ((next (next-full-xid))
         (epoch-start (xid-epoch-start (min next +max-xid+)))
         (boundary (max (or reset-boundary 0)
                        (if (> next epoch-start)
                            epoch-start
                            (- epoch-start +xid-cycle+))))
         (oldest (wal-limit-xmin-by-physical-feedback (max boundary epoch-start) :include-catalog t)))
    (when (< oldest boundary)
      (error "retained physical feedback precedes the recovered transaction status boundary"))
    ;; At the first XID of an epoch, a checkpoint can hide a completed reset.
    ;; A checkpoint with an old active XID predates the reset, which waits
    ;; for all old transactions to end.
    (when (and (< oldest epoch-start)
               (= next epoch-start)
               (plusp *wal-checkpoint-lsn*)
               (>= (or (getf *wal-checkpoint-snapshot* :next-xid) epoch-start)
                   epoch-start)
               (not (< 0 (or (getf *wal-checkpoint-snapshot* :oldest-active-xid) 0)
                       epoch-start)))
      (error "cannot verify retained physical feedback across the checkpoint's XID epoch boundary"))))

(defun primary-storage-bootstrap-p ()
  "Recognize a new cluster before startup creates any storage files."
  (not (or (some #'probe-file (list (pgdata-path "PG_VERSION") (control-path)
                                    (wal-control-path) (catalog-path) (clog-path)))
           (directory-entry-names (pgdata-path "base/") :follow-symlinks t)
           (wal-segment-entry-names))))

(defun initialize-primary-storage (&key imported-startup)
  "Initialize a cold or promoted primary from its durable WAL/control checkpoint.
Hold the drained admission gate and borrow the real postmaster ownership."
  (call-with-live-operation-startup-quiescence
   (lambda (gate-owner)
     (call-with-postmaster-ownership
      (lambda (ownership)
        ;; Recheck imported evidence under ownership immediately before WAL
        ;; mutation.  The normal typed slot loader runs before catalog startup.
        (check-primary-storage-startup-context gate-owner ownership)
        (when imported-startup
          (logical-slot-imported-startup-check-owned imported-startup))
        (let* ((initialize (primary-storage-bootstrap-p))
               (catalog (unless initialize (load-catalog))))
          (unless initialize
            (unless (probe-file (clog-path))
              (error "transaction status file ~a is missing" (clog-path)))
            (check-primary-catalog-identity catalog (nth-value 6 (wal-read-control-fields))))
          (wal-mark-primary-startup)
          (wal-init)
          (xact-init :initialize initialize)
          (let* ((*smgr-redo-existing-main-forks*
                   (when catalog
                     (let ((*catalog* catalog)
                           (forks (make-hash-table :test #'eql)))
                       (dolist (node (catalog-physical-relfilenodes) forks)
                         (setf (gethash node forks) t)))))
                 (reset-boundary (wal-recover)))
            (when (startup-primary-wal-archive-enabled-p)
              (with-wal-critical (wal-archive-try-reconcile-ready-under-lock)))
            (note-primary-startup-replay)
            ;; Replay can change the state checked above. Check again before
            ;; restoring slots, which can also remove temporary slot files.
            (check-primary-storage-startup-context gate-owner ownership)
            (let ((selections (when imported-startup
                                (logical-slot-imported-restore-startup-retained
                                 gate-owner ownership (getf (getf imported-startup :inputs) :proof)))))
              (wal-load-replication-slots :gate-owner gate-owner :ownership ownership)
              (check-primary-feedback-after-replay reset-boundary)
              (when imported-startup
                (logical-slot-imported-check-startup-retained
                 gate-owner ownership (getf (getf imported-startup :inputs) :proof)
                 selections :collect))))
          (if initialize (catalog-init :initialize t) (catalog-init))
          (wal-ensure-directory-durable (merge-pathnames "base/" *pgdata*))
          (physical-backup-lease-load-records)
          (physical-source-pin-load)
          (catalog-remove-orphan-relations)
          (complete-xid-wraparound)))))))

(defun startup-check-cold-installation-state ()
  "Require no page, CLOG or WAL temporary cleanup debt before a cold stage.
Then check the cached handles and workers under startup exclusion."
  (startup-check-retired-storage)
  (unless (and (not *wal-ready*) (not *wal-output-stream*) (not *catalog*)
               (not *clog-stream*) (zerop (hash-table-count *relation-streams*))
               (not *physical-recovery-state*) (not *physical-receive-store*)
               (not *physical-follow-runtime*) (not *physical-follow-operation*)
               (not *physical-follow-desired-worker*) (not *physical-follow-controller-worker*)
               (not *physical-restartpoint-worker*) (not *swank-listener-enabled-p*)
               (not (shutdown-in-progress-p)))
    (error "startup configuration requires storage and workers to remain uninitialized"))
  (values))

(defun offline-primary-recovery-options (arguments)
  "Parse the supported single-user recovery arguments and settings.
Require -D DATADIR and template1. Pass supported -c NAME=VALUE settings through.
Consume the archive_mode=on and archive_command=false overrides."
  (unless (and (cddr arguments)
               (equal (first arguments) "-D")
               (plusp (length (second arguments)))
               (equal (car (last arguments)) "template1"))
    (error "offline recovery expects --single -D DATADIR [OPTIONS] template1"))
  (let ((options (list "-D" (second arguments))))
    (loop for (flag setting) on (butlast (cddr arguments)) by #'cddr
          do (unless (and (equal flag "-c") setting)
               (error "offline recovery options must use -c NAME=VALUE"))
             (cond
               ((uiop:string-prefix-p "archive_mode=" setting)
                (unless (equal setting "archive_mode=on")
                  (error "offline recovery requires archive_mode=on")))
               ((uiop:string-prefix-p "archive_command=" setting)
                (unless (equal setting "archive_command=false")
                  (error "offline recovery requires archive_command=false")))
               (t (setf options (nconc options (list flag setting))))))
    options))

(defun offline-primary-recovery-check-before-pid ()
  "Under the directory lock, admit only an initialized ordinary primary.
Check recovery control, recovery signals and startup fences before changing PID metadata."
  (wal-check-serving-namespace-before-pid)
  (when (or (physical-recovery-read-signal-mode)
            (physical-adoption-path-present-p (physical-recovery-control-path)))
    (error "offline primary recovery requires no standby or promotion recovery state"))
  (logical-slot-copy-preflight-startup)
  (physical-reseed-follow-check-ordinary-startup)
  (let ((state (getf (native-control-read *pgdata*) :db-state)))
    (unless (member state (list +db-shutdowned+ +db-shutdowning+
                                +db-in-crash-recovery+ +db-in-production+))
      (error "offline recovery requires an initialized ordinary primary"))))

(defun run-offline-primary-recovery (arguments)
  "Recover an existing primary offline with empty standard input.
Own PGDATA before storage startup and redo, then load the export origin and
publish a durable clean shutdown. Suppress checkpoint-driven WAL recycling
throughout replay and shutdown."
  (unless (eq (read-char *standard-input* nil :eof) :eof)
    (error "offline primary recovery requires empty input"))
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port swank-bind-address temp-file-limit-bytes
                        sort-arena-limit-bytes max-slot-wal-keep-bytes startup-settings startup-native startup-overrides)
      (parse-args (offline-primary-recovery-options arguments))
    (declare (ignore control-port control-socket swank-port swank-bind-address
                     temp-file-limit-bytes sort-arena-limit-bytes))
    (let ((*max-slot-wal-keep-bytes* max-slot-wal-keep-bytes)
          (*wal-shutdown-defer-recycling* t)
          (*shutdown-finalizing-p* t))
      (startup-install-initial-settings startup-settings startup-native startup-overrides)
      (startup-install-resource-capacity startup-settings)
      (call-with-physical-reseed-offline-startup
       (directory-pathname pgdata)
       (lambda ()
         (postmaster-own-data-directory
          host port :existing-only t :startup-before-pid #'offline-primary-recovery-check-before-pid)
         (initialize-primary-storage)
         (physical-export-origin-load)
         (prepare-primary-shutdown)))))
  (sb-ext:exit :code 0))

(defun native-initdb-check-stage (directory allowed)
  "Reject any unexpected entry, including hidden files and dangling symlinks."
  (unless (and (probe-file directory)
               (equal (directory-entry-names directory :maximum-entries (length allowed)) allowed))
    (error "native initialization requires an empty private staging directory")))

(defun run-native-initdb (arguments)
  "Build a clean primary in a fresh staging directory without starting listeners."
  (unless (and (= (length arguments) 2) (equal (first arguments) "-D"))
    (error "native initialization requires -D STAGE"))
  (let ((directory (directory-pathname (second arguments))))
    (native-initdb-check-stage directory nil)
    (multiple-value-bind (host port pgdata control-port control-socket swank-port swank-bind-address
                          temp-file-limit sort-arena-limit max-slot-wal-keep-bytes settings native overrides)
        (parse-args arguments)
      (declare (ignore pgdata control-port control-socket swank-port swank-bind-address
                       temp-file-limit sort-arena-limit max-slot-wal-keep-bytes))
      (let ((*shutdown-finalizing-p* t))
        (call-with-physical-reseed-offline-startup
         directory
         (lambda ()
           (authentication-preflight-startup settings)
           (startup-install-initial-settings settings native overrides)
           (startup-install-resource-capacity settings)
           (postmaster-own-data-directory host port :startup-before-pid
                                          (lambda () (native-initdb-check-stage directory '(".pg_lisp.lock"))))
           (startup-complete-authentication)
           (initialize-primary-storage)
           (prepare-primary-shutdown)
           (with-open-file (stream (merge-pathnames "postgresql.conf" directory)
                                   :direction :output :if-exists :error)
             (write-line "# Native pg-lisp configuration." stream)
             (fsync-stream stream))
           (fsync-directory directory))))))
  (sb-ext:exit :code 0))

(defun initialize-statement-statistics (settings)
  "Install the process-local statistics store before admitting connections."
  (setf *statement-statistics-store*
        (when (startup-setting-value settings "pg_stat_statements.enabled")
          (make-statement-statistics-store
           (startup-setting-value settings "pg_stat_statements.max")
           (startup-setting-value settings "pg_stat_statements.max_text_bytes")))))

(defun main ()
  ;; Preflight policy before ownership, then initialize it after owning PGDATA.
  ;; Install resource limits before recovery, and check or report them before listen.
  (let ((arguments (cdr sb-ext:*posix-argv*)))
    (when (equal (first arguments) "--single")
      (run-offline-primary-recovery (rest arguments)))
    (when (equal (first arguments) "--initdb-native")
      (run-native-initdb (rest arguments)))
    (startup-handle-meta-request arguments))
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port swank-bind-address temp-file-limit-bytes
                        sort-arena-limit-bytes max-slot-wal-keep-bytes startup-settings startup-native startup-overrides)
      (parse-args (cdr sb-ext:*posix-argv*))
    (setf *pgdata* (directory-pathname pgdata)
          *query-temp-file-limit-bytes* temp-file-limit-bytes
          *query-sort-arena-limit-bytes* sort-arena-limit-bytes
          *max-slot-wal-keep-bytes* max-slot-wal-keep-bytes)
    (let ((imported (logical-slot-copy-imported-startup-candidate-p))
          (imported-startup nil))
      (unless imported (logical-slot-copy-preflight-startup))
      (authentication-preflight-startup startup-settings)
      (startup-install-initial-settings startup-settings startup-native startup-overrides)
      (startup-install-resource-capacity startup-settings)
      (initialize-statement-statistics startup-settings)
      (sb-sys:enable-interrupt sb-unix:sigusr1 #'request-physical-promotion)
      ;; Observe before PID repair and consume terminal retirement under ownership
      ;; before mutable bootstrap or storage initialization begins.
      (if imported
          (setf imported-startup (logical-slot-imported-own-startup host port))
          (physical-reseed-follow-own-startup host port))
      (unless imported
        (logical-slot-copy-owned-startup)
        (physical-backup-cleanup-stages))
      ;; Complete authentication using the captured inputs after PGDATA ownership.
      ;; Do not reread configuration files or reinstall startup resource capacity.
      (startup-complete-authentication)
      (if imported
          (logical-slot-imported-startup-check-policy)
          (physical-promotion-initialize-requests))
      (physical-promotion-test-point :after-promotion-request-initialization)
      ;; Reject an unsupported SBCL before pending reconstruction can open storage.
      (call-with-query-sort-arena (lambda (arena) (declare (ignore arena))))
      ;; Check the selected startup policy again before bootstrap and storage.
      ;; Imported startup uses its local evidence; ordinary startup checks retirement.
      (if imported
          (logical-slot-imported-startup-check-policy)
          (physical-reseed-follow-check-ordinary-startup))
      (let ((primary-ready (and (not imported)
                                (eq :primary-ready (physical-bootstrap-check-startup)))))
        (initialize-query-temp-storage)
        (capture-server-streams)
        (unless primary-ready
          (if imported
              (logical-slot-imported-startup-check-policy)
              (physical-reseed-follow-check-ordinary-startup))
          (if imported
              (progn
                (initialize-primary-storage :imported-startup imported-startup)
                (physical-backup-cleanup-stages))
              (if (recovery-in-progress-p)
                  (physical-recovery-open-storage)
                  (initialize-primary-storage))))
        (when (recovery-in-progress-p)
          (physical-backup-lease-load-records)
          (physical-backup-lease-load-standby-pins))
        (physical-export-origin-load)
        (physical-backup-lease-reconcile-startup)
        (physical-rewind-cleanup-stages-at-startup *physical-backup-lease-records*)
        (physical-restore-cleanup-owned-stages-at-startup)
        (if imported
            (logical-slot-imported-startup-check-policy)
            (physical-follow-prepare-managed-startup))
        (if (recovery-in-progress-p)
            (physical-recovery-check-resource-readiness)
            (progn
              (wal-report-installed-resource-capacity)
              (wal-note-primary-ready)))
        (if primary-ready
            ;; Cold promotion already installed the source identity.
            (assert (nth-value 0 (sync-replication-configuration-state)))
            (with-live-operation-quiescence
              (sync-replication-new-source-incarnation)))
        (unless (recovery-in-progress-p) (start-primary-wal-writer))
        (enable-reload)
        (enable-shutdown-handlers)
        (start-wal-archiver)
        (start-swank-listener swank-port swank-bind-address)
        (start-control-plane :port control-port :socket-path control-socket)
        (if imported
            (start-postmaster-recovery-workers :imported-primary t)
            (start-postmaster-recovery-workers))
        (format t "~&pg-lisp: minimal PostgreSQL-compatible server~%")
        (format t "pg-lisp: data directory ~a~%" *pgdata*)
        (format t "pg-lisp: listening on ~a:~a~%" host port)
        (finish-output)
        (serve host port)
        (unless *physical-slot-cold-cleanup-owner*
          (handler-case (prepare-postmaster-shutdown)
            (primary-clean-publication-fenced ()
              (assert *physical-slot-cold-cleanup-owner*))))
        (when *physical-slot-cold-cleanup-owner*
          (loop (sleep 1)))
        (admin-log "database system is shut down")
        (sb-ext:exit :code 0)))))
