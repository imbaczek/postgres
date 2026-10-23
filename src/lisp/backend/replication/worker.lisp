;;;; worker.lisp -- snapshot copier and durable pgoutput subscriber.

(in-package #:pg)

(defconstant +logical-copy-chunk-bytes+ (* 8 1024 1024))
(defparameter +subscription-progress-table+ "pg_lisp_subscription_progress")
(defparameter +subscription-config-table+ "pg_lisp_subscription_config")

(defstruct logical-subscription-options
  (source-host "127.0.0.1") (source-port 5433) (source-database "postgres")
  source-identity
  (target-host "127.0.0.1") (target-port 5433) (target-database "postgres")
  slot-name (tables '()))

(defstruct pgoutput-column name type-oid key-p (typmod -1))
(defstruct pgoutput-relation oid namespace name columns key-columns)
(defstruct logical-subscription-runtime
  options target (relations (make-hash-table :test #'eql))
  (validated-relations (make-hash-table :test #'equal))
  source-client current-lsn transaction-open-p source-xid
  (drop-feedback-p nil) (drop-feedback-ready-p nil))

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
    (unless (logical-subscription-options-tables options)
      (error "at least one --table allowlist entry is required"))
    (dolist (table (logical-subscription-options-tables options))
      (unless (and (logical-simple-identifier-p table)
                   (not (member table
                                (list +subscription-progress-table+
                                      +subscription-config-table+)
                                :test #'string=)))
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

(defun logical-target-transaction (client function)
  (logical-target-query client "BEGIN")
  (handler-case
      (multiple-value-prog1
          (funcall function)
        (logical-target-query client "COMMIT"))
    (error (condition)
      (ignore-errors (logical-target-query client "ROLLBACK"))
      (error condition))))

(defun logical-replace-progress-row (client slot lsn state token)
  (logical-target-query client (logical-progress-delete-query slot))
  (logical-target-query client (logical-progress-insert-query slot lsn state token)))

(defun logical-persist-progress (client slot lsn state token)
  (logical-target-transaction
   client (lambda () (logical-replace-progress-row client slot lsn state token))))

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

(defun logical-startup-replication-client (options)
  (pg-client-connect
   (logical-subscription-options-source-host options)
   (logical-subscription-options-source-port options)
   (logical-subscription-options-source-database options)
   :replication-p t :application-name "pg-lisp-subscribe"))

(defun logical-startup-source-client (options)
  (pg-client-connect
   (logical-subscription-options-source-host options)
   (logical-subscription-options-source-port options)
   (logical-subscription-options-source-database options)
   :application-name "pg-lisp-subscribe-copy"))

(defun logical-read-source-identity (options)
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
   :application-name "pg-lisp-subscribe-apply"))

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
                   (format nil "CREATE_REPLICATION_SLOT ~a LOGICAL pgoutput EXPORT_SNAPSHOT"
                           (logical-subscription-options-slot-name options))))
                (row (first rows)))
           (unless (and row (= (length row) 4)
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
  (member type-oid '(20 23 1043)))

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

(defun logical-table-schema (client table)
  (multiple-value-bind (rows columns tag)
      (pg-client-query client (format nil "SELECT * FROM ~a LIMIT 0" table))
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
    (let ((data (logical-read-data-string (aref row 0))))
      (unless (and (integerp (getf data :database-oid))
                   (listp (getf data :tables))
                   (listp (getf data :indexes))
                   (listp (getf data :sequences)))
        (error "replication bootstrap schema description is malformed"))
      data)))

(defun logical-bootstrap-sequence-name (entry)
  (getf (getf entry :state) :name))

(defun logical-find-bootstrap-object (name objects key-function)
  (find name objects :key key-function :test #'string=))

(defun logical-bootstrap-create-schema (target bootstrap)
  ;; Sequences precede tables because supported DEFAULT expressions call nextval.
  (dolist (entry (getf bootstrap :sequences))
    (logical-target-query target (getf entry :definition)))
  (dolist (entry (getf bootstrap :tables))
    (logical-target-query target (getf entry :sql)))
  (dolist (entry (getf bootstrap :indexes))
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

(defun logical-bootstrap-schema-matches-p (source target)
  "Check every source bootstrap object's definition after idempotent creation."
  (loop for source-table in (getf source :tables)
        for name = (getf source-table :name)
        for target-table =
                         (logical-find-bootstrap-object name (getf target :tables)
                                                        (lambda (entry) (getf entry :name)))
        always (and target-table
                    (string= (getf source-table :sql)
                             (getf target-table :sql)))))

(defun logical-bootstrap-indexes-match-p (source target)
  (loop for source-index in (getf source :indexes)
        for name = (getf source-index :name)
        for target-index =
                         (logical-find-bootstrap-object name (getf target :indexes)
                                                        (lambda (entry) (getf entry :name)))
        always (and target-index
                    (string= (getf source-index :sql)
                             (getf target-index :sql)))))

(defun logical-bootstrap-sequences-match-p (source target)
  (loop for source-sequence in (getf source :sequences)
        for name = (logical-bootstrap-sequence-name source-sequence)
        for target-sequence =
                            (logical-find-bootstrap-object
                             name (getf target :sequences) #'logical-bootstrap-sequence-name)
        always (and target-sequence
                    (string= (getf source-sequence :definition)
                             (getf target-sequence :definition)))))

(defun logical-validate-replication-bootstrap
    (source-client target-client source target options)
  (unless (and (logical-bootstrap-schema-matches-p source target)
               (logical-bootstrap-indexes-match-p source target)
               (logical-bootstrap-sequences-match-p source target))
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
    ((null value) "NULL")
    ((member type-oid '(20 23))
     (unless (ignore-errors (parse-integer value :junk-allowed nil))
       (if (eq context :copy)
           (error "source returned a malformed integer value")
           (error "pgoutput integer value is malformed")))
     value)
    ((= type-oid 1043) (logical-sql-literal value))
    (t (error "unsupported ~a type OID ~a"
              (if (eq context :copy) "logical copy" "pgoutput") type-oid))))

(defun logical-copy-target-chunk (target table columns rows)
  (when rows
    (logical-target-transaction
     target
     (lambda ()
       (dolist (row (nreverse rows))
         (logical-target-query
          target (logical-copy-insert-query table columns row)))))))

(defun logical-copy-source-table (source target table)
  (let ((columns (logical-validate-copy-schema source target table))
        (rows '())
        (chunk-bytes 0))
    (labels ((flush-chunk ()
               (logical-copy-target-chunk target table columns rows)
               (setf rows '() chunk-bytes 0))
             (consume-row (row row-columns)
               (declare (ignore row-columns))
               (let ((row-bytes (logical-copy-row-size row)))
                 (when (> row-bytes +logical-copy-chunk-bytes+)
                   (error "one source row exceeds the 8 MiB initial-copy chunk limit"))
                 (when (and rows
                            (> (+ chunk-bytes row-bytes)
                               +logical-copy-chunk-bytes+))
                   (flush-chunk))
                 (push row rows)
                 (incf chunk-bytes row-bytes))))
      ;; Executor SELECT streams DataRow frames; the callback commits each
      ;; bounded chunk while this source Repeatable Read snapshot remains open.
      (pg-client-query source (format nil "SELECT * FROM ~a" table)
                       :on-row #'consume-row)
      (flush-chunk))))

(defun logical-copy-delay-if-configured ()
  (let ((delay (logical-copy-delay-seconds)))
    (when (and delay (plusp delay))
      (format *error-output* "pg-lisp-subscribe: snapshot imported; copy pause ~a seconds~%"
              delay)
      (finish-output *error-output*)
      (sleep delay))))

(defun logical-copy-from-exported-snapshot (options target boundary token)
  (let ((source (logical-startup-source-client options))
        (slot (logical-subscription-options-slot-name options)))
    (unwind-protect
         (progn
           (pg-client-query source
                            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
           (pg-client-query
            source (format nil "SET TRANSACTION SNAPSHOT ~a"
                           (logical-sql-literal token)))
           (format *error-output* "pg-lisp-subscribe: snapshot imported at ~a~%" boundary)
           (finish-output *error-output*)
           (logical-copy-delay-if-configured)
           (let ((source-bootstrap (logical-read-replication-bootstrap source)))
             ;; pg_dump separates schema and data phases; create only the
             ;; implemented sequence/table/btree definitions before COPY.
             (logical-bootstrap-create-schema target source-bootstrap)
             (logical-validate-replication-bootstrap
              source target source-bootstrap
              (logical-read-replication-bootstrap target) options)
             ;; Sequence values are non-MVCC in PostgreSQL too. Capture their
             ;; state at the imported snapshot before copying table rows.
             (logical-bootstrap-set-sequence-state target source-bootstrap))
           (dolist (table (logical-subscription-options-tables options))
             (logical-validate-copy-schema source target table))
           (logical-reset-target-tables
            target (logical-subscription-options-tables options))
           (dolist (table (logical-subscription-options-tables options))
             (logical-copy-source-table source target table))
           (pg-client-query source "COMMIT")
           (logical-persist-progress target slot boundary "ready" nil)
           (pg-client-close source)
           (setf source nil))
      (when source (pg-client-close source)))))

(defun logical-copy-or-reseed (options target progress)
  (if (and progress (string= (getf progress :state) "copying")
           (getf progress :token) (getf progress :lsn))
      (handler-case
          (logical-copy-from-exported-snapshot
           options target (getf progress :lsn) (getf progress :token))
        (pg-client-error (condition)
          (unless (string= (pg-client-error-sqlstate condition) "22023")
            (error condition))
          (format *error-output* "pg-lisp-subscribe: exported snapshot expired; reseeding~%")
          (logical-create-copy-state options target)))
      (logical-create-copy-state options target)))

(defun logical-create-copy-state (options target)
  (multiple-value-bind (boundary token) (logical-create-source-slot options)
    (logical-persist-initial-progress
     target options (logical-subscription-options-slot-name options)
     boundary "copying" token)
    (logical-copy-from-exported-snapshot options target boundary token)))

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
                         (logical-table-identity-signature
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

(defun logical-pgoutput-read-relation (payload)
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
      (incf position) ; replica identity mode
      (let ((count (bytes-to-u16 payload position)))
        (incf position 2)
        (dotimes (i count)
          (when (> (+ position 1) (length payload))
            (error "truncated pgoutput Relation column flags"))
          (let ((key-p (= (aref payload position) 1)))
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

(defun logical-apply-update (runtime relation payload position)
  (when (>= position (length payload)) (error "truncated pgoutput UPDATE"))
  (let ((old-kind (code-char (aref payload position)))
        (position (1+ position))
        (old-tuple nil)
        (new-tuple nil))
    (when (member old-kind '(#\K #\O))
      (multiple-value-setq (old-tuple position)
        (logical-pgoutput-read-tuple payload position)))
    (unless (and (< position (length payload))
                 (= (aref payload position) (char-code #\N)))
      (error "pgoutput UPDATE lacks a new tuple"))
    (multiple-value-setq (new-tuple position)
      (logical-pgoutput-read-tuple payload (1+ position)))
    (unless (= position (length payload))
      (error "pgoutput UPDATE has trailing bytes"))
    (when (logical-relation-selected-p runtime relation)
      (unless (and (char= old-kind #\K)
                   (= (length old-tuple)
                      (length (pgoutput-relation-key-columns relation))))
        (error "allowlisted table ~a requires a pgoutput K identity tuple"
               (pgoutput-relation-name relation)))
      (unless (= (length new-tuple) (length (pgoutput-relation-columns relation)))
        (error "pgoutput UPDATE tuple is not a full row"))
      (let* ((all-columns (pgoutput-relation-columns relation))
             (set-clause
               (format nil "~{~a~^, ~}"
                       (loop for column in all-columns
                             for value across new-tuple
                             collect (format nil "~a = ~a"
                                             (pgoutput-column-name column)
                                             (logical-source-value-literal
                                              (pgoutput-column-type-oid column)
                                              value :pgoutput)))))
             (where (logical-row-key-predicate
                     old-tuple
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
        (unless (and (char= kind #\K)
                     (= (length tuple)
                        (length (pgoutput-relation-key-columns relation))))
          (error "allowlisted table ~a requires a pgoutput K identity tuple"
                 (pgoutput-relation-name relation)))
        (let* ((where (logical-row-key-predicate
                       tuple (pgoutput-relation-key-columns relation)))
               (tag (nth-value
                     2 (logical-target-query
                        (logical-subscription-runtime-target runtime)
                        (format nil "DELETE FROM ~a WHERE ~a"
                                (pgoutput-relation-name relation) where)))))
          (logical-assert-one-row-affected tag "DELETE"))))))

(defun logical-progress-transaction-commit (runtime commit-lsn)
  (let* ((options (logical-subscription-runtime-options runtime))
         (slot (logical-subscription-options-slot-name options))
         (lsn-text (wal-format-lsn commit-lsn)))
    (logical-replace-progress-row
     (logical-subscription-runtime-target runtime) slot lsn-text "ready" nil)
    (logical-target-query (logical-subscription-runtime-target runtime) "COMMIT")
    (setf (logical-subscription-runtime-current-lsn runtime) commit-lsn
          (logical-subscription-runtime-transaction-open-p runtime) nil
          (logical-subscription-runtime-source-xid runtime) nil
          (logical-subscription-runtime-drop-feedback-ready-p runtime)
          (logical-subscription-runtime-drop-feedback-p runtime))
    (unless (logical-subscription-runtime-drop-feedback-ready-p runtime)
      (pg-client-send-standby-status
       (logical-subscription-runtime-source-client runtime) commit-lsn))))

(defun logical-progress-keepalive (runtime lsn)
  (let ((current (logical-subscription-runtime-current-lsn runtime)))
    (when (> lsn current)
      (logical-persist-progress
       (logical-subscription-runtime-target runtime)
       (logical-subscription-options-slot-name
        (logical-subscription-runtime-options runtime))
       (wal-format-lsn lsn) "ready" nil)
      (setf (logical-subscription-runtime-current-lsn runtime) lsn))
    (pg-client-send-standby-status
     (logical-subscription-runtime-source-client runtime)
     (logical-subscription-runtime-current-lsn runtime))))

(defun logical-pgoutput-read-message (runtime payload)
  "Decode pgoutput v2 M, whose transactional XID precedes its flags."
  (let* ((transactional-p
           (logical-subscription-runtime-transaction-open-p runtime))
         (position 1))
    (when transactional-p
      (when (> (+ position 4) (length payload))
        (error "truncated transactional pgoutput Message xid"))
      (unless (= (bytes-to-u32 payload position)
                 (logical-subscription-runtime-source-xid runtime))
        (error "pgoutput Message xid does not match its transaction"))
      (incf position 4))
    (when (>= position (length payload))
      (error "truncated pgoutput Message flags"))
    (let ((flags (aref payload position)))
      (incf position)
      (unless (= flags (if transactional-p 1 0))
        (error "pgoutput Message transactional flag does not match its context")))
    (when (> (+ position 8) (length payload))
      (error "truncated pgoutput Message LSN"))
    (let ((lsn (pg-client-u64 payload position))
          (prefix nil)
          (content nil))
      (incf position 8)
      (multiple-value-setq (prefix position)
        (pg-client-cstring payload position))
      (when (> (+ position 4) (length payload))
        (error "truncated pgoutput Message content length"))
      (let ((size (bytes-to-u32 payload position)))
        (incf position 4)
        (unless (and (<= size +max-logical-transaction-bytes+)
                     (= (+ position size) (length payload)))
          (error "invalid pgoutput Message content length"))
        (setf content (bytes->string payload :start position
                                             :end (length payload))))
      (values prefix lsn content))))

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
    (logical-target-query
     target
     (format nil "APPLY LOGICAL EVENT ~a ~d ~a"
             (logical-sql-literal
              (logical-standalone-event-key options database-oid))
             lsn (logical-sql-literal payload)))
    ;; APPLY commits both catalog state and the source LSN high-water mark before
    ;; this progress row moves. A retry after a crash is therefore idempotent.
    (logical-persist-progress target slot (wal-format-lsn lsn) "ready" nil)
    (setf (logical-subscription-runtime-current-lsn runtime) lsn)
    (pg-client-send-standby-status
     (logical-subscription-runtime-source-client runtime) lsn)))

(defun logical-apply-truncate-message (runtime payload)
  (let* ((data (logical-read-data-string payload))
         (tables (getf data :tables))
         (options (logical-subscription-runtime-options runtime)))
    (unless (and (listp tables) (every #'stringp tables))
      (error "invalid pgoutput TRUNCATE message payload"))
    ;; TRUNCATE follows the same publication filter as row changes. pgoutput.c
    ;; emits one transactional Truncate message rather than per-row deletes.
    (dolist (name tables)
      (when (member name (logical-subscription-options-tables options)
                    :test #'string=)
        (logical-target-query
         (logical-subscription-runtime-target runtime)
         (format nil "TRUNCATE TABLE ~a" (catalog-sql-quote-ident name)))))))

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
       (let ((relation (logical-pgoutput-read-relation payload)))
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
         (ecase kind
           (#\I (logical-apply-insert runtime relation payload 5))
           (#\U (logical-apply-update runtime relation payload 5))
           (#\D (logical-apply-delete runtime relation payload 5)))))
      (#\M
       (multiple-value-bind (prefix lsn content)
           (logical-pgoutput-read-message runtime payload)
         (cond
           ((string= prefix "pg_lisp/truncate")
            (unless (logical-subscription-runtime-transaction-open-p runtime)
              (error "pgoutput TRUNCATE message is outside a transaction"))
            (logical-apply-truncate-message runtime content))
           ((member prefix '("pg_lisp/ddl" "pg_lisp/sequence-state")
                    :test #'string=)
            (when (logical-subscription-runtime-transaction-open-p runtime)
              (error "catalog event unexpectedly appears inside a transaction"))
            (logical-apply-standalone-event runtime lsn content))
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
            :options options :target target :current-lsn marker
            :drop-feedback-p
            (equal (sb-ext:posix-getenv "PG_LISP_SUBSCRIBE_DROP_FEEDBACK_ONCE") "1"))))
    (unwind-protect
         (progn
           (setf (logical-subscription-runtime-source-client runtime) source)
           (pg-client-start-replication
            source
            (format nil "START_REPLICATION SLOT ~a LOGICAL ~a (proto_version '2')"
                    (logical-subscription-options-slot-name options)
                    (wal-format-lsn marker)))
           ;; START_REPLICATION's first feedback is the copy boundary on first
           ;; use, or a durable target commit after reconnect. walsender.c
           ;; validates these startup handshakes before entering its send loop.
           (pg-client-send-standby-status source marker)
           (format *error-output* "pg-lisp-subscribe: streaming from ~a~%"
                   (wal-format-lsn marker))
           (finish-output *error-output*)
           (loop
             (multiple-value-bind (type payload)
                 (pg-client-replication-frame source)
               (unless (char= type #\d)
                 (error "unexpected CopyBoth frontend response ~s" type))
               (logical-stream-frame runtime payload)
               (when (and (logical-subscription-runtime-drop-feedback-ready-p runtime)
                          (not (logical-subscription-runtime-transaction-open-p runtime))
                          (> (logical-subscription-runtime-current-lsn runtime) marker))
                 ;; Regression hook for the crash window after target COMMIT but
                 ;; before the publisher receives feedback.
                 (format *error-output* "pg-lisp-subscribe: dropping feedback after durable target commit~%")
                 (finish-output *error-output*)
                 (return))))
           (values))
      (pg-client-close source))))

(defun logical-subscription-run (options)
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
            (format t "usage: pg-lisp-subscribe --slot NAME --table TABLE [--table TABLE] [--source-host HOST] [--source-port PORT] [--source-database DB] [--target-host HOST] [--target-port PORT] [--target-database DB]~%")
            (logical-subscription-run options)))
    (error (condition)
      (format *error-output* "pg-lisp-subscribe: ~a~%" condition)
      (finish-output *error-output*)
      (sb-ext:exit :code 1))))
