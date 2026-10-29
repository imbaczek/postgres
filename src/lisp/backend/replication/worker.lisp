;;;; worker.lisp -- snapshot copier and durable pgoutput subscriber.

(in-package #:pg)

(defconstant +logical-copy-chunk-bytes+ (* 8 1024 1024))
(defparameter +subscription-progress-table+ "pg_lisp_subscription_progress")
(defparameter +subscription-config-table+ "pg_lisp_subscription_config")

(defstruct logical-subscription-options
  (source-host "127.0.0.1") (source-port 5433) (source-database "postgres")
  source-identity (mode :extension) publication actual-slot
  (target-host "127.0.0.1") (target-port 5433) (target-database "postgres")
  slot-name (tables '()))

(defstruct pgoutput-column name type-oid key-p (typmod -1))
(defstruct pgoutput-relation oid namespace name columns key-columns)

(defstruct logical-subscription-runtime
  options target (relations (make-hash-table :test #'eql))
  (validated-relations (make-hash-table :test #'equal))
  source-client current-lsn transaction-open-p source-xid)

(defun logical-simple-identifier-p (value)
  (and (stringp value) (plusp (length value))
       (or (alpha-char-p (char value 0)) (char= (char value 0) #\_))
       (every (lambda (char)
                (or (alphanumericp char) (char= char #\_)))
              value)
       (string= value (string-downcase value))))

(defun logical-parse-port (option value)
  (let ((port (ignore-errors (parse-integer value :junk-allowed nil))))
    (unless (and port (<= 1 port 65535))
      (error "option ~a expects a port in 1..65535" option))
    port))

(defun parse-logical-subscription-args (args)
  (let ((remaining args)
        (options (make-logical-subscription-options))
        (slot-seen nil))
    (labels ((next-value (option)
               (or (pop remaining)
                   (error "option ~a requires a value" option))))
      (loop while remaining
            for arg = (pop remaining)
            do (cond
                 ((string= arg "--mode")
                  (setf (logical-subscription-options-mode options)
                        (cond ((string= (first remaining) "standard") :standard)
                              ((string= (first remaining) "extension") :extension)
                              (t (error "--mode expects standard or extension"))))
                  (next-value arg))
                 ((string= arg "--publication")
                  (setf (logical-subscription-options-publication options) (next-value arg)))
                 ((string= arg "--source-host")
                  (setf (logical-subscription-options-source-host options)
                        (next-value arg)))
                 ((string= arg "--source-port")
                  (setf (logical-subscription-options-source-port options)
                        (logical-parse-port arg (next-value arg))))
                 ((string= arg "--source-database")
                  (setf (logical-subscription-options-source-database options)
                        (next-value arg)))
                 ((string= arg "--target-host")
                  (setf (logical-subscription-options-target-host options)
                        (next-value arg)))
                 ((string= arg "--target-port")
                  (setf (logical-subscription-options-target-port options)
                        (logical-parse-port arg (next-value arg))))
                 ((string= arg "--target-database")
                  (setf (logical-subscription-options-target-database options)
                        (next-value arg)))
                 ((string= arg "--slot")
                  (setf (logical-subscription-options-slot-name options)
                        (next-value arg)
                        slot-seen t))
                 ((string= arg "--table")
                  (push (next-value arg)
                        (logical-subscription-options-tables options)))
                 ((member arg '("-h" "--help") :test #'string=)
                  (return-from parse-logical-subscription-args :help))
                 (t (error "unknown pg-lisp-subscribe option ~a" arg)))))
    (setf (logical-subscription-options-tables options)
          (nreverse (logical-subscription-options-tables options)))
    (unless (and slot-seen
                 (wal-valid-replication-slot-name-p
                  (logical-subscription-options-slot-name options)))
      (error "--slot requires a valid lowercase replication slot name"))
    (if (eq (logical-subscription-options-mode options) :standard)
        (unless (logical-simple-identifier-p (logical-subscription-options-publication options))
          (error "standard mode requires --publication with a simple identifier"))
        (when (logical-subscription-options-publication options)
          (error "--publication requires standard mode")))
    (unless (logical-subscription-options-tables options)
      (error "at least one --table allowlist entry is required"))
    (dolist (table (logical-subscription-options-tables options))
      (unless (and (logical-simple-identifier-p table)
                   (not (logical-private-metadata-table-name-p table)))
        (error "unsupported allowlist table identifier ~s" table)))
    (setf (logical-subscription-options-tables options)
          (remove-duplicates (logical-subscription-options-tables options)
                             :test #'string= :from-end t))
    options))

(defun logical-sql-literal (value)
  (if (null value)
      "NULL"
      (with-output-to-string (stream)
        (write-char #\' stream)
        (loop for char across value
              do (when (char= char #\') (write-char #\' stream))
                 (write-char char stream))
        (write-char #\' stream))))

(defun logical-progress-table-ddl ()
  (format nil "CREATE TABLE IF NOT EXISTS ~a (source_slot varchar PRIMARY KEY, source_lsn varchar, state varchar NOT NULL, snapshot_token varchar)"
          +subscription-progress-table+))

(defun logical-progress-insert-query (slot lsn state token)
  (format nil "INSERT INTO ~a (source_slot, source_lsn, state, snapshot_token) VALUES (~a, ~a, ~a, ~a)"
          +subscription-progress-table+
          (logical-sql-literal slot) (logical-sql-literal lsn)
          (logical-sql-literal state) (logical-sql-literal token)))

(defun logical-progress-delete-query (slot)
  (format nil "DELETE FROM ~a WHERE source_slot = ~a"
          +subscription-progress-table+ (logical-sql-literal slot)))

(defun logical-progress-select-query (slot)
  (format nil "SELECT source_lsn, state, snapshot_token FROM ~a WHERE source_slot = ~a"
          +subscription-progress-table+ (logical-sql-literal slot)))

(defun logical-subscription-config-ddl ()
  (format nil "CREATE TABLE IF NOT EXISTS ~a (source_slot varchar PRIMARY KEY, source_host varchar NOT NULL, source_port integer NOT NULL, source_database varchar NOT NULL, table_allowlist varchar NOT NULL)"
          +subscription-config-table+))

(defun logical-subscription-config-values (options)
  (list (logical-subscription-options-source-host options)
        (logical-subscription-options-source-port options)
        (logical-subscription-options-source-database options)
        (format nil "~{~a~^,~}"
                (sort (copy-list
                       (logical-subscription-options-tables options))
                      #'string<))))

(defun logical-subscription-config-insert-query (slot options)
  (destructuring-bind (host port database allowlist)
      (logical-subscription-config-values options)
    (unless (logical-subscription-options-source-identity options)
      (error "publisher cluster identity was not loaded before saving subscription config"))
    (format nil "INSERT INTO ~a (source_slot, source_host, source_port, source_database, table_allowlist) VALUES (~a, ~a, ~a, ~a, ~a)"
            +subscription-config-table+
            (logical-sql-literal slot)
            (logical-sql-literal
             (format nil "~a|~a" host
                     (logical-subscription-options-source-identity options))) port
            (logical-sql-literal database) (logical-sql-literal allowlist))))

(defun logical-subscription-config-select-query (slot)
  (format nil "SELECT source_host, source_port, source_database, table_allowlist FROM ~a WHERE source_slot = ~a"
          +subscription-config-table+ (logical-sql-literal slot)))

(defun logical-target-query (client query)
  (pg-client-query client query))

(defun logical-target-transaction
    (client function &key before-commit after-commit)
  (logical-target-query client "BEGIN")
  (handler-case
      (multiple-value-prog1
          (funcall function)
        (when before-commit (funcall before-commit))
        (logical-target-query client "COMMIT")
        (when after-commit (funcall after-commit)))
    (error (condition)
      (ignore-errors (logical-target-query client "ROLLBACK"))
      (error condition))))

(defun logical-replace-progress-row (client slot lsn state token)
  (logical-target-query client (logical-progress-delete-query slot))
  (logical-target-query client (logical-progress-insert-query slot lsn state token)))

(defun logical-persist-progress
    (client slot lsn state token &key before-commit after-commit)
  (logical-target-transaction
   client
   (lambda () (logical-replace-progress-row client slot lsn state token))
   :before-commit before-commit
   :after-commit after-commit))

(defun logical-persist-initial-progress (client options slot lsn state token)
  (logical-target-transaction
   client
   (lambda ()
     (logical-target-query
      client (logical-progress-delete-query slot))
     (logical-target-query client (logical-progress-insert-query slot lsn state token))
     (logical-target-query
      client (format nil "DELETE FROM ~a WHERE source_slot = ~a"
                     +subscription-config-table+ (logical-sql-literal slot)))
     (logical-target-query
      client (logical-subscription-config-insert-query slot options)))))

(defun logical-read-progress (client slot)
  (let ((rows (logical-target-query client (logical-progress-select-query slot))))
    (when rows
      (let ((row (first rows)))
        (list :lsn (aref row 0) :state (aref row 1) :token (aref row 2))))))

(defun logical-read-subscription-config (client slot)
  (let ((rows
          (logical-target-query
           client (logical-subscription-config-select-query slot))))
    (when rows
      (let ((row (first rows)))
        (let* ((stored-host (aref row 0))
               (separator (position #\| stored-host)))
          (list :source-host (if separator
                                 (subseq stored-host 0 separator)
                                 stored-host)
                :source-identity (and separator
                                      (subseq stored-host (1+ separator)))
                :source-port (parse-integer (aref row 1))
                :source-database (aref row 2)
                :table-allowlist (aref row 3)))))))

(defun logical-validate-resume-config
    (options progress config &optional (check-source-identity-p t))
  (when progress
    (unless config
      (error "saved subscriber source and table configuration is missing; recreate its target progress"))
    (destructuring-bind (host port database allowlist)
        (logical-subscription-config-values options)
      (unless (and (string= host (getf config :source-host))
                   (= port (getf config :source-port))
                   (string= database (getf config :source-database))
                   (string= allowlist (getf config :table-allowlist))
                   (or (not check-source-identity-p)
                       (equal
                        (logical-subscription-options-source-identity options)
                        (getf config :source-identity))))
        (error "subscriber source or table allowlist does not match saved progress"))))
  (values))

(defun logical-ensure-progress-table (client)
  (logical-target-query client (logical-progress-table-ddl))
  (logical-target-query client (logical-subscription-config-ddl)))

(defparameter +logical-replication-format-options+
  "-c DateStyle=ISO,MDY -c IntervalStyle=postgres -c extra_float_digits=3 -c TimeZone=UTC")

(defun logical-startup-replication-client (options)
  (pg-client-connect
   (logical-subscription-options-source-host options)
   (logical-subscription-options-source-port options)
   (logical-subscription-options-source-database options)
   :replication-p t :application-name "pg-lisp-subscribe"
   :options +logical-replication-format-options+))

(defun logical-startup-source-client (options)
  (pg-client-connect
   (logical-subscription-options-source-host options)
   (logical-subscription-options-source-port options)
   (logical-subscription-options-source-database options)
   :application-name "pg-lisp-subscribe-copy"
   :options +logical-replication-format-options+))

(defun logical-read-source-identity (options)
  (when (eq (logical-subscription-options-mode options) :standard)
    (return-from logical-read-source-identity (logical-standard-source-identity options)))
  (let ((source (logical-startup-source-client options)))
    (unwind-protect
         (let* ((rows (pg-client-query source "SELECT pg_lisp_cluster_identity()"))
                (row (first rows)))
           (unless (and row (stringp (aref row 0)) (plusp (length (aref row 0))))
             (error "publisher returned no durable cluster identity"))
           (aref row 0))
      (pg-client-close source))))

(defun logical-startup-target-client (options)
  (pg-client-connect
   (logical-subscription-options-target-host options)
   (logical-subscription-options-target-port options)
   (logical-subscription-options-target-database options)
   :application-name "pg-lisp-subscribe-apply"
   :options +logical-replication-format-options+))

(defun logical-drop-source-slot-if-present (options)
  (let ((client (logical-startup-replication-client options)))
    (unwind-protect
         (handler-case
             (pg-client-query
              client (format nil "DROP_REPLICATION_SLOT ~a"
                             (logical-subscription-options-slot-name options)))
           (pg-client-error (condition)
             (unless (string= (pg-client-error-sqlstate condition) "42704")
               (error condition))))
      (pg-client-close client))))

(defun logical-create-source-slot (options)
  (logical-drop-source-slot-if-present options)
  (let ((client (logical-startup-replication-client options)))
    (unwind-protect
         (let* ((rows
                  (pg-client-query
                   client
                   (format nil "CREATE_REPLICATION_SLOT ~a LOGICAL pg_lisp EXPORT_SNAPSHOT"
                           (logical-subscription-options-slot-name options))))
                (row (first rows)))
           (unless (and row (= (length row) 4) (equal (aref row 3) "pg_lisp")
                        (string= (aref row 0)
                                 (logical-subscription-options-slot-name options)))
             (error "source returned an invalid replication slot result"))
           (values (aref row 1) (aref row 2)))
      (pg-client-close client))))

(defun logical-copy-delay-seconds ()
  (let ((milliseconds (sb-ext:posix-getenv "PG_LISP_SUBSCRIBE_COPY_DELAY_MS")))
    (when milliseconds
      (let ((value (ignore-errors (parse-integer milliseconds))))
        (unless (and value (<= 0 value 60000))
          (error "PG_LISP_SUBSCRIBE_COPY_DELAY_MS must be 0..60000"))
        (/ value 1000.0)))))

(defun logical-type-supported-p (type-oid)
  (member type-oid '(17 20 21 23 25 114 3802 700 701 1043 1082 1083 1114 1184 1186 1266 1700)))

(defun logical-validate-client-schema (table source-columns target-columns)
  (unless (= (length source-columns) (length target-columns))
    (error "source and target schemas differ for table ~a" table))
  (loop for source in source-columns
        for target in target-columns
        do (unless (and (string= (pg-client-column-name source)
                                 (pg-client-column-name target))
                        (= (pg-client-column-type-oid source)
                           (pg-client-column-type-oid target))
                        (= (pg-client-column-typmod source)
                           (pg-client-column-typmod target))
                        (logical-type-supported-p
                         (pg-client-column-type-oid source)))
             (error "source and target schemas differ at table ~a column ~a"
                    table (pg-client-column-name source)))))

(defun logical-table-schema (client table &optional public-p)
  (multiple-value-bind (rows columns tag)
      (pg-client-query client (format nil "SELECT * FROM ~a~a LIMIT 0" (if public-p "public." "") table))
    (declare (ignore rows tag))
    columns))

(defun logical-table-identity-signature (client table)
  ;; PostgreSQL pgoutput.c marks replica-identity columns in Relation messages.
  ;; This catalog probe lets the copier reject a subscriber whose own identity
  ;; would re-encode an apply transaction with a larger key list.
  (let* ((rows
           (pg-client-query
            client
            (format nil "SELECT pg_lisp_replica_identity_columns(~a)"
                    (logical-sql-literal table))))
         (row (first rows)))
    (unless row
      (error "could not inspect replica identity for table ~a" table))
    (aref row 0)))

(defun logical-validate-copy-schema (source target table)
  (let ((source-columns (logical-table-schema source table))
        (target-columns (logical-table-schema target table))
        (source-identity (logical-table-identity-signature source table))
        (target-identity (logical-table-identity-signature target table)))
    (unless source-columns
      (error "source table ~a has no supported columns" table))
    (unless (plusp (length source-identity))
      (error "source table ~a has no usable replica identity" table))
    (logical-validate-client-schema table source-columns target-columns)
    (unless (string= source-identity target-identity)
      (error "source and target replica identities differ for table ~a" table))
    source-columns))

(defun logical-read-replication-bootstrap (client)
  (let* ((rows (logical-target-query
                client "SELECT pg_lisp_replication_bootstrap()"))
         (row (first rows)))
    (unless (and row (stringp (aref row 0)))
      (error "replication bootstrap query returned no schema description"))
    ;; The projection validates all raw entries and owners before filtering,
    ;; so malformed metadata cannot reach target schema creation.
    (logical-replication-bootstrap-view
     (logical-read-data-string (aref row 0)))))

(defun logical-find-bootstrap-object (name objects key-function)
  (find name objects :key key-function :test #'string=))

(defun logical-bootstrap-create-schema (target bootstrap-view &optional existing-view)
  "Apply missing table definitions from the validated bootstrap.
Existing tables were validated for compatibility; skip their full definitions,
including appended NOT VALID ALTERs, rather than reinstalling constraints."
  (dolist (entry (getf bootstrap-view :sequences))
    (logical-target-query target (getf entry :definition)))
  (dolist (entry (getf bootstrap-view :tables))
    (unless (find (getf entry :name) (getf existing-view :tables)
                  :key (lambda (table) (getf table :name)) :test #'string=)
      (logical-target-query target (getf entry :sql))))
  (dolist (entry (getf bootstrap-view :indexes))
    (logical-target-query target (getf entry :sql))))

(defun logical-bootstrap-set-sequence-state (target bootstrap)
  (dolist (entry (getf bootstrap :sequences))
    (let* ((state (getf entry :state))
           (name (getf state :name)))
      (logical-target-query
       target
       (format nil "SELECT setval(~a, ~d, ~a)"
               (logical-sql-literal name) (getf state :last)
               (if (getf state :called) "true" "false"))))))

(defun logical-validate-existing-replication-bootstrap
    (source-client target-client source target options)
  "Reject target drift before bootstrap can create or reset anything."
  (unless (logical-schema-contract-subset-compatible-p
           (logical-bootstrap-schema-contract source)
           (logical-bootstrap-schema-contract target))
    (error "target has existing schema definitions that differ from source"))
  (let ((source-names (mapcar (lambda (entry) (getf entry :name))
                              (getf source :tables))))
    (dolist (table (logical-subscription-options-tables options))
      (unless (member table source-names :test #'string=)
        (error "allowlisted source table ~a does not exist" table)))
    (dolist (source-table (getf source :tables))
      (let* ((name (getf source-table :name))
             (target-table
               (logical-find-bootstrap-object
                name (getf target :tables) (lambda (entry) (getf entry :name)))))
        (when target-table
          (logical-validate-client-schema
           name (logical-table-schema source-client name)
           (logical-table-schema target-client name))
          (when (member name (logical-subscription-options-tables options)
                        :test #'string=)
            (logical-validate-copy-schema source-client target-client name))))))
  (values))

(defun logical-validate-replication-bootstrap
    (source-client target-client source target options)
  ;; Compare the full implemented object set after creating source objects that
  ;; were absent on an empty or compatible target. Sequence values are excluded.
  (unless (equal (logical-bootstrap-schema-contract source)
                 (logical-bootstrap-schema-contract target))
    (error "source and target supported schema definitions differ"))
  (let ((source-names (mapcar (lambda (entry) (getf entry :name))
                              (getf source :tables))))
    (dolist (table (logical-subscription-options-tables options))
      (unless (member table source-names :test #'string=)
        (error "allowlisted source table ~a does not exist" table))))
  (dolist (table (getf source :tables))
    (let* ((name (getf table :name))
           (source-columns (logical-table-schema source-client name))
           (target-columns (logical-table-schema target-client name)))
      (logical-validate-client-schema name source-columns target-columns)))
  (values))

(defun logical-reset-target-tables (target tables)
  (logical-target-transaction
   target
   (lambda ()
     (dolist (table tables)
       (logical-target-query target (format nil "TRUNCATE TABLE ~a" table)))))
  (values))

(defun logical-copy-row-size (row)
  (+ 32 (* 16 (length row))
     (loop for value across row when value sum (length (string->bytes value)))))

(defun logical-copy-chunk-limit (test-limit failpoint action)
  (if (null test-limit)
      +logical-copy-chunk-bytes+
      (progn
        (unless (and (string-equal (or failpoint "")
                                   "after-copy-chunk-commit")
                     (string-equal (or action "") "crash"))
          (error "test copy chunk limit requires the copy-chunk crash failpoint"))
        (let ((limit (ignore-errors
                      (parse-integer test-limit :junk-allowed nil))))
          (unless (and limit (<= 1 limit +logical-copy-chunk-bytes+))
            (error "PG_LISP_TEST_COPY_CHUNK_BYTES must be between 1 and ~d"
                   +logical-copy-chunk-bytes+))
          limit))))

(defun logical-copy-chunk-limit-bytes ()
  (logical-copy-chunk-limit
   (sb-ext:posix-getenv "PG_LISP_TEST_COPY_CHUNK_BYTES")
   (sb-ext:posix-getenv "PG_LISP_TEST_FAILPOINT")
   (sb-ext:posix-getenv "PG_LISP_TEST_FAILPOINT_ACTION")))

(defun logical-copy-insert-query (table columns row)
  (format nil "INSERT INTO ~a (~{~a~^, ~}) VALUES (~{~a~^, ~})"
          table (mapcar #'pg-client-column-name columns)
          (loop for column in columns
                for value across row
                collect (logical-source-value-literal
                         (pg-client-column-type-oid column) value :copy))))

(defun logical-source-value-literal (type-oid value context)
  (unless (member context '(:copy :pgoutput))
    (error "unknown logical value conversion context ~s" context))
  (cond
    ((eq value :unchanged-toast) (error "unchanged TOAST value cannot be used here"))
    ((null value) "NULL")
    ((member type-oid '(20 21 23))
     (unless (ignore-errors (parse-integer value :junk-allowed nil))
       (if (eq context :copy)
           (error "source returned a malformed integer value")
           (error "pgoutput integer value is malformed")))
     value)
    ((member type-oid '(17 25 114 3802 700 701 1043 1082 1083 1114 1184 1186 1266 1700)) (logical-sql-literal value))
    (t (error "unsupported ~a type OID ~a"
              (if (eq context :copy) "logical copy" "pgoutput") type-oid))))

(defun logical-copy-check-query-size (query)
  "Reject a Simple Query frame that the target cannot accept."
  (when (> (+ 5 (length (string->bytes query))) +max-query-message-size+)
    (error "initial-copy row exceeds the target query frame limit (~d bytes)"
           +max-query-message-size+))
  query)

(defun logical-copy-target-chunk (target table columns rows)
  (when rows
    (logical-target-transaction
     target
     (lambda ()
       (dolist (row (nreverse rows))
         (logical-test-failpoint :before-copy-row-change table)
         (logical-target-query
          target (logical-copy-check-query-size (logical-copy-insert-query table columns row)))
         (logical-test-failpoint :after-copy-row-change table)))
     :before-commit
     (lambda ()
       (logical-test-failpoint :before-copy-chunk-commit table))
     :after-commit
     (lambda ()
       (logical-test-failpoint :after-copy-chunk-commit table)))))

(defun logical-copy-source-table (source target table &optional (validate-schema #'logical-validate-copy-schema) (source-table table))
  (let ((columns (funcall validate-schema source target table))
        (rows '())
        (chunk-bytes 0)
        (chunk-limit (logical-copy-chunk-limit-bytes)))
    (labels ((flush-chunk ()
               (when rows
                 (logical-copy-target-chunk target table columns rows)
                 (setf rows '() chunk-bytes 0)))
             (consume-row (row row-columns)
               (declare (ignore row-columns))
               (let ((row-bytes (logical-copy-row-size row)))
                 (when (and rows
                            (> (+ chunk-bytes row-bytes) chunk-limit))
                   (flush-chunk))
                 (push row rows)
                 (incf chunk-bytes row-bytes)
                 ;; A row larger than the batching target is a singleton, not
                 ;; an unbounded batch waiting for the next source frame.
                 (when (> row-bytes chunk-limit)
                   (flush-chunk)))))
      ;; Executor SELECT streams DataRow frames; the callback commits each
      ;; bounded chunk while this source Repeatable Read snapshot remains open.
      (pg-client-query source (format nil "SELECT * FROM ~a" source-table)
                       :on-row #'consume-row)
      (flush-chunk))))

(defun logical-copy-delay-if-configured ()
  (let ((delay (logical-copy-delay-seconds)))
    (when (and delay (plusp delay))
      (format *error-output* "pg-lisp-subscribe: snapshot imported; copy pause ~a seconds~%"
              delay)
      (finish-output *error-output*)
      (sleep delay))))

(defun logical-import-copy-snapshot (source token)
  (pg-client-query source "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
  (handler-case
      (progn
        (pg-client-query
         source (format nil "SET TRANSACTION SNAPSHOT ~a"
                        (logical-sql-literal token)))
        :imported)
    (pg-client-error (condition)
      (if (string= (pg-client-error-sqlstate condition) "22023")
          :expired
          (error condition)))))

(defun logical-persist-copy-ready (options target boundary)
  (let* ((slot (logical-subscription-options-slot-name options))
         (before (lambda () (logical-test-failpoint :before-copy-ready-commit slot boundary)))
         (after (lambda () (logical-test-failpoint :after-copy-ready-commit slot boundary))))
    (if (eq (logical-subscription-options-mode options) :standard)
        (logical-standard-persist-state target options "ready" boundary
                                        :before-commit before :after-commit after)
        (logical-persist-progress target slot boundary "ready" nil
                                  :before-commit before :after-commit after))))

(defun logical-copy-data-and-finish (options target source boundary
                                     &optional (validate-schema #'logical-validate-copy-schema))
  "Keep reset/copy/snapshot commit/durable READY ordering shared across modes."
  (let ((slot (logical-subscription-options-slot-name options)))
    (logical-test-failpoint :before-copy-reset slot boundary)
    (logical-reset-target-tables target (logical-subscription-options-tables options))
    (logical-test-failpoint :after-copy-reset slot boundary)
    (dolist (table (logical-subscription-options-tables options))
      (logical-copy-source-table source target table validate-schema
                                 (if (eq (logical-subscription-options-mode options) :standard)
                                     (format nil "public.~a" table) table)))
    (pg-client-query source "COMMIT")
    (logical-test-failpoint :after-copy-snapshot-transaction-commit slot boundary)
    (logical-persist-copy-ready options target boundary))
  :ready)

(defun logical-copy-from-exported-snapshot (options target boundary token)
  (let ((source (logical-startup-source-client options)))
    (unwind-protect
         (if (eq (logical-import-copy-snapshot source token) :expired)
             :snapshot-expired
             (progn
               (format *error-output*
                       "pg-lisp-subscribe: snapshot imported at ~a~%" boundary)
               (finish-output *error-output*)
               (logical-copy-delay-if-configured)
               (let* ((source-bootstrap
                        (logical-read-replication-bootstrap source))
                      (target-bootstrap
                        (logical-read-replication-bootstrap target)))
                 ;; Validate existing target definitions before pg_dump-style
                 ;; schema creation or any sequence/data mutation.
                 (logical-validate-existing-replication-bootstrap
                  source target source-bootstrap target-bootstrap options)
                 (logical-bootstrap-create-schema target source-bootstrap target-bootstrap)
                 (logical-validate-replication-bootstrap
                  source target source-bootstrap
                  (logical-read-replication-bootstrap target) options))
               (dolist (table (logical-subscription-options-tables options))
                 (logical-validate-copy-schema source target table))
               ;; Sequence values are non-MVCC; apply the boundary capture only
               ;; after every schema and replica-identity check has succeeded.
               (logical-bootstrap-set-sequence-state
                target (logical-read-replication-bootstrap source))
               (logical-copy-data-and-finish options target source boundary)
               :ready))
      (when source (pg-client-close source)))))

(defun logical-copy-or-reseed (options target progress)
  (if (and progress
           (string= (getf progress :state) "copying")
           (getf progress :token)
           (getf progress :lsn))
      (when (eq (logical-copy-from-exported-snapshot
                 options target (getf progress :lsn) (getf progress :token))
                :snapshot-expired)
        (format *error-output*
                "pg-lisp-subscribe: exported snapshot expired; reseeding~%")
        (logical-create-copy-state options target))
      (logical-create-copy-state options target)))

(defun logical-create-copy-state (options target)
  (multiple-value-bind (boundary token) (logical-create-source-slot options)
    (let ((slot (logical-subscription-options-slot-name options)))
      (logical-test-failpoint
       :after-source-slot-created-before-copying-progress slot boundary)
      (logical-persist-initial-progress
       target options slot boundary "copying" token)
      (logical-test-failpoint :after-copying-progress-commit slot boundary)
      (logical-copy-from-exported-snapshot options target boundary token))))

(defun logical-relation-key-signature (relation)
  (format nil "~{~d~^,~}"
          (loop for column in (pgoutput-relation-columns relation)
                for attnum from 1
                when (pgoutput-column-key-p column)
                  collect attnum)))

(defun logical-validate-stream-relation (runtime relation)
  (let* ((options (logical-subscription-runtime-options runtime))
         (name (pgoutput-relation-name relation)))
    (when (member name (logical-subscription-options-tables options)
                  :test #'string=)
      (unless (string= (pgoutput-relation-namespace relation) "public")
        (error "allowlisted relation ~a is outside the supported public schema" name))
      (unless (pgoutput-relation-key-columns relation)
        (error "allowlisted table ~a has no usable source replica identity" name))
      (multiple-value-bind (rows target-columns tag)
          (pg-client-query
           (logical-subscription-runtime-target runtime)
           (format nil "SELECT * FROM ~a LIMIT 0" name))
        (declare (ignore rows tag))
        (logical-validate-client-schema
         name
         (mapcar (lambda (column)
                   (make-pg-client-column :name (pgoutput-column-name column)
                                          :type-oid (pgoutput-column-type-oid column)
                                          :typmod (pgoutput-column-typmod column)))
                 (pgoutput-relation-columns relation))
         target-columns)
        (unless (string= (logical-relation-key-signature relation)
                         (funcall (if (eq (logical-subscription-options-mode options) :standard)
                                      #'logical-standard-key-signature
                                      #'logical-table-identity-signature)
                                  (logical-subscription-runtime-target runtime) name))
          (error "source and target replica identities differ for table ~a" name))
        (setf (gethash name
                       (logical-subscription-runtime-validated-relations runtime)) t)))))

(defun logical-relation-selected-p (runtime relation)
  (and (member (pgoutput-relation-name relation)
               (logical-subscription-options-tables
                (logical-subscription-runtime-options runtime))
               :test #'string=)
       (gethash (pgoutput-relation-name relation)
                (logical-subscription-runtime-validated-relations runtime))))

(defun logical-pgoutput-read-relation (payload &optional standard-p)
  (let* ((position 1)
         (oid (bytes-to-u32 payload position))
         (namespace nil)
         (columns '())
         (key-columns '()))
    (incf position 4)
    (multiple-value-setq (namespace position)
      (pg-client-cstring payload position))
    (multiple-value-bind (name next) (pg-client-cstring payload position)
      (setf position next)
      (when (> position (1- (length payload)))
        (error "truncated pgoutput Relation message"))
      (when (and standard-p (/= (aref payload position) (char-code #\d)))
        (error "standard replication requires default replica identity"))
      (incf position) ; replica identity mode
      (let ((count (bytes-to-u16 payload position)))
        (incf position 2)
        (dotimes (i count)
          (when (> (+ position 1) (length payload))
            (error "truncated pgoutput Relation column flags"))
          (let ((key-p (= (aref payload position) 1)))
            (unless (member (aref payload position) '(0 1))
              (error "unsupported pgoutput Relation column flags"))
            (incf position)
            (multiple-value-bind (column-name after-name)
                (pg-client-cstring payload position)
              (setf position after-name)
              (when (> (+ position 8) (length payload))
                (error "truncated pgoutput Relation column metadata"))
              (let ((column
                      (make-pgoutput-column
                       :name column-name :key-p key-p
                       :type-oid (bytes-to-u32 payload position)
                       :typmod (logical-u32-signed
                                (bytes-to-u32 payload (+ position 4))))))
                (incf position 8) ; type OID and type modifier
                (push column columns)
                (when key-p (push column key-columns)))))))
      (unless (= position (length payload))
        (error "pgoutput Relation message has trailing bytes"))
      (make-pgoutput-relation :oid oid :namespace namespace :name name
                              :columns (nreverse columns)
                              :key-columns (nreverse key-columns)))))

(defun logical-pgoutput-read-tuple (payload position)
  (when (> (+ position 2) (length payload))
    (error "truncated pgoutput tuple header"))
  (let ((count (bytes-to-u16 payload position))
        (values '()))
    (incf position 2)
    (dotimes (i count)
      (when (>= position (length payload))
        (error "truncated pgoutput tuple value"))
      (let ((kind (code-char (aref payload position))))
        (incf position)
        (case kind
          (#\n (push nil values))
          (#\u (push :unchanged-toast values))
          (#\t
           (when (> (+ position 4) (length payload))
             (error "truncated pgoutput text value length"))
           (let ((size (bytes-to-u32 payload position)))
             (incf position 4)
             (unless (<= (+ position size) (length payload))
               (error "truncated pgoutput text value"))
             (push (bytes->string payload :start position :end (+ position size))
                   values)
             (incf position size)))
          (otherwise (error "unsupported pgoutput tuple value kind ~s" kind)))))
    (values (coerce (nreverse values) 'vector) position)))

(defun logical-row-values-for-columns (tuple columns)
  (unless (= (length tuple) (length columns))
    (error "pgoutput tuple width does not match relation metadata"))
  (loop for column in columns
        for value across tuple
        collect (cons (pgoutput-column-name column)
                      (cons (pgoutput-column-type-oid column) value))))

(defun logical-row-insert-sql (relation tuple)
  (let* ((columns (pgoutput-relation-columns relation))
         (values (logical-row-values-for-columns tuple columns)))
    (format nil "INSERT INTO ~a (~{~a~^, ~}) VALUES (~{~a~^, ~})"
            (pgoutput-relation-name relation)
            (mapcar #'car values)
            (mapcar (lambda (entry)
                      (logical-source-value-literal
                       (cadr entry) (cddr entry) :pgoutput))
                    values))))

(defun logical-row-key-predicate (tuple columns)
  (let ((entries (logical-row-values-for-columns tuple columns)))
    (format nil "~{~a~^ AND ~}"
            (mapcar (lambda (entry)
                      (if (null (cddr entry))
                          (format nil "~a IS NULL" (car entry))
                          (format nil "~a = ~a" (car entry)
                                  (logical-source-value-literal
                                   (cadr entry) (cddr entry) :pgoutput))))
                    entries))))

(defun logical-assert-one-row-affected (tag operation)
  (let* ((separator (and tag (position #\Space tag :from-end t)))
         (count (and separator (ignore-errors
                                (parse-integer tag :start (1+ separator))))))
    (unless (eql count 1)
      (error "logical ~a expected one target row, got ~a" operation (or tag "no command tag")))))

(defun logical-apply-insert (runtime relation payload position)
  (unless (and (< position (length payload))
               (= (aref payload position) (char-code #\N)))
    (error "pgoutput INSERT lacks a new tuple"))
  (multiple-value-bind (tuple end)
      (logical-pgoutput-read-tuple payload (1+ position))
    (unless (= end (length payload)) (error "pgoutput INSERT has trailing bytes"))
    (when (logical-relation-selected-p runtime relation)
      (unless (= (length tuple) (length (pgoutput-relation-columns relation)))
        (error "pgoutput INSERT tuple is not a full row"))
      (logical-target-query
       (logical-subscription-runtime-target runtime)
       (logical-row-insert-sql relation tuple)))))

(defun logical-pgoutput-read-update (payload position)
  "Read nonstreamed UPDATE tuples in C logicalrep_read_update field order."
  (when (>= position (length payload)) (error "truncated pgoutput UPDATE"))
  (let ((kind (code-char (aref payload position)))
        (initial-kind (code-char (aref payload position)))
        (old-tuple nil)
        (new-tuple nil))
    (when (member kind '(#\K #\O))
      (multiple-value-setq (old-tuple position)
        (logical-pgoutput-read-tuple payload (1+ position)))
      (when (>= position (length payload)) (error "truncated pgoutput UPDATE"))
      (setf kind (code-char (aref payload position))))
    (unless (char= kind #\N)
      (error "pgoutput UPDATE lacks a new tuple"))
    (multiple-value-setq (new-tuple position)
      (logical-pgoutput-read-tuple payload (1+ position)))
    (unless (= position (length payload))
      (error "pgoutput UPDATE has trailing bytes"))
    (values old-tuple new-tuple initial-kind)))

(defun logical-pgoutput-key-values (relation tuple)
  "Project a relation-width tuple onto its replica identity columns."
  (unless (= (length tuple) (length (pgoutput-relation-columns relation)))
    (error "pgoutput identity tuple width does not match relation metadata"))
  (unless (pgoutput-relation-key-columns relation)
    (error "pgoutput relation has no replica identity"))
  (coerce (loop for column in (pgoutput-relation-columns relation)
                for value across tuple
                when (pgoutput-column-key-p column) collect value)
          'vector))

(defun logical-update-set-clause (relation new-tuple)
  "Render changed fields, retaining the target's unchanged external values."
  (let ((assignments
          (loop for column in (pgoutput-relation-columns relation)
                for value across new-tuple
                unless (eq value :unchanged-toast)
                  collect (format nil "~a = ~a"
                                  (pgoutput-column-name column)
                                  (logical-source-value-literal
                                   (pgoutput-column-type-oid column) value :pgoutput)))))
    (unless assignments
      (let ((key (first (pgoutput-relation-key-columns relation))))
        (unless key (error "pgoutput relation has no replica identity"))
        (push (format nil "~a = ~a" (pgoutput-column-name key)
                      (pgoutput-column-name key)) assignments)))
    (format nil "~{~a~^, ~}" assignments)))

(defun logical-apply-update (runtime relation payload position)
  (multiple-value-bind (old-tuple new-tuple kind)
      (logical-pgoutput-read-update payload position)
    (when (logical-relation-selected-p runtime relation)
      (when (char= kind #\O)
        (error "REPLICA IDENTITY FULL is not supported"))
      (unless (= (length new-tuple) (length (pgoutput-relation-columns relation)))
        (error "pgoutput UPDATE tuple is not a full row"))
      (let* ((set-clause (logical-update-set-clause relation new-tuple))
             (where (logical-row-key-predicate
                     (logical-pgoutput-key-values relation (or old-tuple new-tuple))
                     (pgoutput-relation-key-columns relation)))
             (tag (nth-value
                   2 (logical-target-query
                      (logical-subscription-runtime-target runtime)
                      (format nil "UPDATE ~a SET ~a WHERE ~a"
                              (pgoutput-relation-name relation) set-clause where)))))
        (logical-assert-one-row-affected tag "UPDATE")))))

(defun logical-apply-delete (runtime relation payload position)
  (when (>= position (length payload)) (error "truncated pgoutput DELETE"))
  (let ((kind (code-char (aref payload position))))
    (multiple-value-bind (tuple end)
        (logical-pgoutput-read-tuple payload (1+ position))
      (unless (= end (length payload)) (error "pgoutput DELETE has trailing bytes"))
      (when (logical-relation-selected-p runtime relation)
        (unless (char= kind #\K)
          (error "allowlisted table ~a requires a pgoutput K identity tuple"
                 (pgoutput-relation-name relation)))
        (let* ((where (logical-row-key-predicate
                       (logical-pgoutput-key-values relation tuple)
                       (pgoutput-relation-key-columns relation)))
               (tag (nth-value
                     2 (logical-target-query
                        (logical-subscription-runtime-target runtime)
                        (format nil "DELETE FROM ~a WHERE ~a"
                                (pgoutput-relation-name relation) where)))))
          (logical-assert-one-row-affected tag "DELETE"))))))

(defun logical-progress-transaction-commit (runtime commit-lsn)
  (let* ((options (logical-subscription-runtime-options runtime))
         (slot (logical-subscription-options-slot-name options))
         (target (logical-subscription-runtime-target runtime))
         (lsn-text (wal-format-lsn commit-lsn)))
    ;; Match apply_handle_commit_internal(): the source transaction and its
    ;; applied-origin marker commit atomically on the subscriber.
    (if (eq (logical-subscription-options-mode options) :standard)
        (logical-standard-save-lsn target options lsn-text)
        (logical-replace-progress-row target slot lsn-text "ready" nil))
    (logical-test-failpoint :before-target-commit slot commit-lsn)
    (logical-target-query target "COMMIT")
    (setf (logical-subscription-runtime-current-lsn runtime) commit-lsn
          (logical-subscription-runtime-transaction-open-p runtime) nil
          (logical-subscription-runtime-source-xid runtime) nil)
    ;; walsender.c only learns this position from the reply sent after local
    ;; COMMIT; a retry starts from the durable target marker if this point dies.
    (logical-test-failpoint
     :after-target-commit-before-feedback slot commit-lsn)
    (pg-client-send-standby-status
     (logical-subscription-runtime-source-client runtime) commit-lsn)))

(defun logical-progress-keepalive (runtime wal-end)
  "Reply to a keepalive without treating WAL-END as an applied position."
  ;; WalSndKeepalive() in walsender.c reports the publisher's end, which may
  ;; include events the subscriber has not applied. logical/worker.c advances
  ;; confirmation only through LogicalConfirmReceivedLocation() after commit.
  (declare (ignore wal-end))
  (pg-client-send-standby-status
   (logical-subscription-runtime-source-client runtime)
   (logical-subscription-runtime-current-lsn runtime)))

(defun logical-advance-progress-from-scan (runtime lsn)
  "Commit and acknowledge a safe frontier returned by the WAL reader."
  (let* ((current (logical-subscription-runtime-current-lsn runtime))
         (options (logical-subscription-runtime-options runtime))
         (slot (logical-subscription-options-slot-name options)))
    (unless (and (integerp current) (integerp lsn) (>= lsn current))
      (error "safe logical scan progress moved backwards"))
    (when (> lsn current)
      ;; Commit the marker before feedback, as in logical/worker.c's
      ;; LogicalConfirmReceivedLocation() ordering.
      (logical-persist-progress
       (logical-subscription-runtime-target runtime)
       slot (wal-format-lsn lsn) "ready" nil)
      (setf (logical-subscription-runtime-current-lsn runtime) lsn))
    (logical-test-failpoint :after-scan-progress-before-feedback slot lsn)
    (pg-client-send-standby-status
     (logical-subscription-runtime-source-client runtime)
     (logical-subscription-runtime-current-lsn runtime))))

(defun logical-pgoutput-read-message (runtime payload)
  "Decode a nonstreamed Message and return prefix, LSN, content, transactional-p."
  (when (< (length payload) 10)
    (error "truncated pgoutput Message header"))
  (let ((flags (aref payload 1))
        (position 10)
        (lsn (pg-client-u64 payload 2))
        (prefix nil))
    (unless (member flags '(0 1))
      (error "unsupported pgoutput Message flags"))
    (when (and (= flags 1)
               (not (logical-subscription-runtime-transaction-open-p runtime)))
      (error "transactional pgoutput Message outside a transaction"))
    (multiple-value-setq (prefix position) (pg-client-cstring payload position))
    (when (> (+ position 4) (length payload))
      (error "truncated pgoutput Message content length"))
    (let ((size (bytes-to-u32 payload position)))
      (incf position 4)
      (unless (and (<= size +max-logical-transaction-bytes+)
                   (= (+ position size) (length payload)))
        (error "invalid pgoutput Message content length"))
      (values prefix lsn
              (bytes->string payload :start position :end (length payload))
              (= flags 1)))))

(defun logical-standalone-event-key (options database-oid)
  (format nil "~a:~d:~a:~a:~d:~a"
          (logical-subscription-options-source-host options)
          (logical-subscription-options-source-port options)
          (logical-subscription-options-source-database options)
          (logical-subscription-options-source-identity options)
          database-oid
          (logical-subscription-options-slot-name options)))

(defun logical-apply-standalone-event (runtime lsn payload)
  (let* ((data (logical-read-data-string payload))
         (database-oid (getf data :database-oid))
         (options (logical-subscription-runtime-options runtime))
         (target (logical-subscription-runtime-target runtime))
         (slot (logical-subscription-options-slot-name options)))
    (unless (and (integerp database-oid) (<= 1 database-oid #xFFFFFFFF))
      (error "logical event has an invalid source database OID"))
    (logical-test-failpoint :before-standalone-event-apply slot lsn)
    (logical-target-query
     target
     (format nil "APPLY LOGICAL EVENT ~a ~d ~a"
             (logical-sql-literal
              (logical-standalone-event-key options database-oid))
             lsn (logical-sql-literal payload)))
    ;; APPLY commits the catalog state and event high-water before progress.
    ;; Retries after this point deduplicate in save-catalog.
    (logical-test-failpoint
     :after-standalone-target-apply-before-progress slot lsn)
    (logical-persist-progress target slot (wal-format-lsn lsn) "ready" nil)
    (setf (logical-subscription-runtime-current-lsn runtime) lsn)
    (logical-test-failpoint
     :after-standalone-progress-before-feedback slot lsn)
    (pg-client-send-standby-status
     (logical-subscription-runtime-source-client runtime) lsn)))

(defun logical-pgoutput-read-truncate (payload)
  "Decode native TRUNCATE; reject flags whose effects we cannot preserve."
  (unless (and (>= (length payload) 6)
               (= (aref payload 0) (char-code #\T)))
    (error "invalid pgoutput TRUNCATE header"))
  (let ((count (bytes-to-u32 payload 1))
        (flags (aref payload 5)))
    (unless (zerop flags)
      (error "unsupported pgoutput TRUNCATE flags ~d" flags))
    (unless (= (length payload) (+ 6 (* count 4)))
      (error "invalid pgoutput TRUNCATE relation count"))
    (loop for offset from 6 below (length payload) by 4
          collect (bytes-to-u32 payload offset))))

(defun logical-truncate-target-tables (runtime names)
  "Apply one TRUNCATE operation with shared fault boundaries for all its tables."
  (when names
    (dolist (name names)
      (logical-test-failpoint :before-row-change :truncate name))
    (logical-target-query
     (logical-subscription-runtime-target runtime)
     (format nil "TRUNCATE TABLE ~{~a~^, ~}" (mapcar #'catalog-sql-quote-ident names)))
    (dolist (name names)
      (logical-test-failpoint :after-row-change :truncate name))))

(defun logical-apply-truncate (runtime payload)
  "Apply native TRUNCATE inside the current durable apply transaction."
  (unless (logical-subscription-runtime-transaction-open-p runtime)
    (error "pgoutput TRUNCATE appeared outside a transaction"))
  (let ((relations
          (loop for oid in (logical-pgoutput-read-truncate payload)
                collect (or (gethash oid (logical-subscription-runtime-relations runtime))
                            (error "pgoutput TRUNCATE has no Relation metadata")))))
    ;; C apply_handle_truncate collects the relations for one ExecuteTruncateGuts.
    (logical-truncate-target-tables
     runtime (loop for relation in relations
                   when (logical-relation-selected-p runtime relation)
                     collect (pgoutput-relation-name relation)))))

(defun logical-apply-truncate-message (runtime payload)
  (let* ((data (logical-read-data-string payload))
         (tables (getf data :tables))
         (options (logical-subscription-runtime-options runtime)))
    (unless (and (listp tables) (every #'stringp tables))
      (error "invalid pgoutput TRUNCATE message payload"))
    (logical-truncate-target-tables
     runtime (remove-if-not
              (lambda (name)
                (member name (logical-subscription-options-tables options) :test #'string=))
              tables))))

(defun logical-pgoutput-dispatch (runtime payload)
  (when (zerop (length payload)) (error "empty pgoutput message"))
  (let ((kind (code-char (aref payload 0))))
    (case kind
      (#\B
       (when (logical-subscription-runtime-transaction-open-p runtime)
         (error "nested pgoutput transaction"))
       (unless (= (length payload) 21)
         (error "invalid pgoutput Begin message"))
       (logical-target-query (logical-subscription-runtime-target runtime) "BEGIN")
       (setf (logical-subscription-runtime-transaction-open-p runtime) t
             (logical-subscription-runtime-source-xid runtime)
             (bytes-to-u32 payload 17)))
      (#\R
       (let ((relation (logical-pgoutput-read-relation
                        payload (eq (logical-subscription-options-mode
                                     (logical-subscription-runtime-options runtime)) :standard))))
         (setf (gethash (pgoutput-relation-oid relation)
                        (logical-subscription-runtime-relations runtime)) relation)
         (logical-validate-stream-relation runtime relation)))
      ((#\I #\U #\D)
       (unless (logical-subscription-runtime-transaction-open-p runtime)
         (error "pgoutput row change appeared outside a transaction"))
       (when (< (length payload) 5) (error "truncated pgoutput row message"))
       (let* ((oid (bytes-to-u32 payload 1))
              (relation (gethash oid
                                 (logical-subscription-runtime-relations runtime))))
         (unless relation (error "pgoutput row message has no Relation metadata"))
         (logical-test-failpoint
          :before-row-change kind (pgoutput-relation-name relation))
         (ecase kind
           (#\I (logical-apply-insert runtime relation payload 5))
           (#\U (logical-apply-update runtime relation payload 5))
           (#\D (logical-apply-delete runtime relation payload 5)))
         (logical-test-failpoint
          :after-row-change kind (pgoutput-relation-name relation))))
      (#\T (logical-apply-truncate runtime payload))
      (#\M
       (when (eq (logical-subscription-options-mode
                  (logical-subscription-runtime-options runtime)) :standard)
         (error "standard subscriber does not accept extension Message events"))
       (multiple-value-bind (prefix lsn content transactional-p)
           (logical-pgoutput-read-message runtime payload)
         (cond
           ((string= prefix "pg_lisp/truncate")
            (unless (logical-subscription-runtime-transaction-open-p runtime)
              (error "pgoutput TRUNCATE message is outside a transaction"))
            (progn
              (unless transactional-p
                (error "pgoutput TRUNCATE message is not transactional"))
              (logical-apply-truncate-message runtime content)))
           ((member prefix '("pg_lisp/ddl" "pg_lisp/sequence-state")
                    :test #'string=)
            (when (logical-subscription-runtime-transaction-open-p runtime)
              (error "catalog event unexpectedly appears inside a transaction"))
            (logical-apply-standalone-event runtime lsn content))
           ((string= prefix "pg_lisp/progress")
            (when (logical-subscription-runtime-transaction-open-p runtime)
              (error "safe progress Message appeared inside a transaction"))
            (unless (string= content "")
              (error "safe progress Message must have empty content"))
            (logical-advance-progress-from-scan runtime lsn))
           (t (error "unsupported pgoutput Message prefix ~s" prefix)))))
      (#\C
       (unless (and (logical-subscription-runtime-transaction-open-p runtime)
                    (= (length payload) 26)
                    (zerop (aref payload 1)))
         (error "invalid pgoutput Commit message"))
       ;; logicalrep_write_commit() carries flags, commit LSN, end LSN, and time.
       (logical-progress-transaction-commit
        runtime (pg-client-u64 payload 10)))
      (otherwise (error "unsupported pgoutput message ~s" kind)))))

(defun logical-stream-frame (runtime payload)
  (when (zerop (length payload)) (error "empty replication CopyData payload"))
  (case (code-char (aref payload 0))
    (#\w
     (when (< (length payload) 26) (error "truncated logical XLogData frame"))
     (logical-pgoutput-dispatch runtime (subseq payload 25)))
    (#\k
     (unless (= (length payload) 18) (error "invalid logical keepalive frame"))
     (logical-progress-keepalive runtime (pg-client-u64 payload 1)))
    (otherwise (error "unexpected replication CopyData message ~s"
                      (code-char (aref payload 0))))))

(defun logical-run-stream (options target marker)
  (let* ((source (logical-startup-replication-client options))
         (runtime
           (make-logical-subscription-runtime
            :options options :target target :current-lsn marker)))
    (unwind-protect
         (progn
           (setf (logical-subscription-runtime-source-client runtime) source)
           (when (eq (logical-subscription-options-mode options) :standard)
             (logical-standard-validate-resume-slot options marker))
           (pg-client-start-replication
            source
            (if (eq (logical-subscription-options-mode options) :standard)
                (logical-standard-start-command options marker)
                (format nil "START_REPLICATION SLOT ~a LOGICAL ~a (proto_version '2')"
                        (logical-subscription-options-slot-name options)
                        (wal-format-lsn marker))))
           ;; The first feedback is the copy boundary or the target's durable
           ;; restart marker; walsender.c validates it before entering its loop.
           (pg-client-send-standby-status source marker)
           (format *error-output* "pg-lisp-subscribe: streaming from ~a~%"
                   (wal-format-lsn marker))
           (finish-output *error-output*)
           (loop
             (multiple-value-bind (type payload)
                 (pg-client-replication-frame source)
               (unless (char= type #\d)
                 (error "unexpected CopyBoth frontend response ~s" type))
               (logical-stream-frame runtime payload)))
           (values))
      (pg-client-close source))))

(defun logical-subscription-run (options)
  (when (eq (logical-subscription-options-mode options) :standard)
    (return-from logical-subscription-run (logical-standard-subscription-run options)))
  (let ((target (logical-startup-target-client options)))
    (unwind-protect
         (progn
           (logical-ensure-progress-table target)
           (let* ((slot (logical-subscription-options-slot-name options))
                  (progress (logical-read-progress target slot))
                  (config (logical-read-subscription-config target slot)))
             ;; Reject endpoint and allowlist changes before opening a publisher
             ;; connection; then pin resumes to the publisher incarnation too.
             (logical-validate-resume-config options progress config nil)
             (setf (logical-subscription-options-source-identity options)
                   (logical-read-source-identity options))
             (logical-validate-resume-config options progress config)
             (cond
               ((null progress)
                (logical-create-copy-state options target))
               ((string= (or (getf progress :state) "") "copying")
                (logical-copy-or-reseed options target progress))
               ((string= (or (getf progress :state) "") "ready") nil)
               (t (error "unknown subscriber progress state ~s"
                         (getf progress :state))))
             (let* ((refreshed
                      (logical-read-progress
                       target (logical-subscription-options-slot-name options)))
                    (marker-text (getf refreshed :lsn)))
               (unless (and marker-text
                            (string= (getf refreshed :state) "ready"))
                 (error "subscriber initial copy did not reach ready state"))
               (logical-run-stream
                options target (parse-replication-lsn marker-text)))))
      (pg-client-close target))))

(defun logical-subscription-main ()
  (handler-case
      (let ((options (parse-logical-subscription-args
                      (cdr sb-ext:*posix-argv*))))
        (if (eq options :help)
            (format t "usage: pg-lisp-subscribe --slot NAME --table TABLE [--table TABLE] [--mode extension|standard] [--publication NAME] [--source-host HOST] [--source-port PORT] [--source-database DB] [--target-host HOST] [--target-port PORT] [--target-database DB]~%")
            (logical-subscription-run options)))
    (error (condition)
      (format *error-output* "pg-lisp-subscribe: ~a~%" condition)
      (finish-output *error-output*)
      (sb-ext:exit :code 1))))
