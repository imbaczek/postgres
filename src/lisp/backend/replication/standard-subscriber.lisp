(in-package #:pg)

(defparameter +standard-subscription-table+ "pg_lisp_standard_subscriptions")

(defvar *logical-subscription-owners* (make-hash-table :test #'equal))

(defvar *logical-subscription-owner-lock*
  (sb-thread:make-mutex :name "subscriber session ownership"))

(defun logical-acquire-subscription-owner (slot)
  "Own one target subscription until this backend connection closes."
  (unless (and *backend* (wal-valid-replication-slot-name-p slot))
    (ereport "22023" "invalid subscription key"))
  (let ((key (list (backend-database *backend*) slot)))
    (sb-thread:with-mutex (*logical-subscription-owner-lock*)
      (let ((owner (gethash key *logical-subscription-owners*)))
        (when (and owner (not (eq owner *backend*)))
          (ereport "55006" "subscription ~s already has an active worker" slot))
        (setf (gethash key *logical-subscription-owners*) *backend*))))
  t)

(defun logical-release-subscription-owner (backend)
  (sb-thread:with-mutex (*logical-subscription-owner-lock*)
    (loop for key being the hash-keys of *logical-subscription-owners*
            using (hash-value owner)
          when (eq owner backend) collect key into keys
            finally (dolist (key keys) (remhash key *logical-subscription-owners*))))
  (values))

(defun logical-standard-ensure-state-table (target)
  (pg-client-query target
                   (format nil "CREATE TABLE IF NOT EXISTS ~a (subscription_key varchar PRIMARY KEY, source_host varchar NOT NULL, source_port integer NOT NULL, source_database varchar NOT NULL, source_identity varchar NOT NULL, publication_name varchar NOT NULL, table_allowlist varchar NOT NULL, output_plugin varchar NOT NULL, actual_slot varchar NOT NULL, state varchar NOT NULL, source_lsn varchar)"
                           +standard-subscription-table+)))

(defun logical-standard-config-values (options)
  (list (logical-subscription-options-source-host options)
        (write-to-string (logical-subscription-options-source-port options))
        (logical-subscription-options-source-database options)
        (logical-subscription-options-source-identity options)
        (logical-subscription-options-publication options)
        (format nil "~{~a~^,~}" (sort (copy-list (logical-subscription-options-tables options)) #'string<))
        "pgoutput"))

(defun logical-standard-read-state (target options)
  (let ((rows (pg-client-query
               target
               (format nil "SELECT source_host,source_port,source_database,source_identity,publication_name,table_allowlist,output_plugin,actual_slot,state,source_lsn FROM ~a WHERE subscription_key=~a"
                       +standard-subscription-table+
                       (logical-sql-literal (logical-subscription-options-slot-name options))))))
    (when rows
      (let ((row (first rows)))
        (list :config (subseq (coerce row 'list) 0 7)
              :actual-slot (aref row 7) :state (aref row 8) :lsn (aref row 9))))))

(defun logical-standard-validate-state (options state &optional (identity-p t))
  (when state
    (let ((wanted (logical-standard-config-values options))
          (saved (getf state :config)))
      (unless (and (= 7 (length saved))
                   (loop for value in wanted for old in saved for index from 0
                         always (or (and (not identity-p) (= index 3)) (equal value old))))
        (error "standard subscriber source/publication/table configuration changed"))
      (unless (logical-standard-attempt-slot-p (getf state :actual-slot))
        (error "standard subscriber has an invalid saved attempt slot")))))

(defun logical-standard-write-state (target options state lsn)
  (let ((slot (logical-subscription-options-slot-name options)))
    (pg-client-query target
                     (format nil "DELETE FROM ~a WHERE subscription_key=~a"
                             +standard-subscription-table+ (logical-sql-literal slot)))
    (pg-client-query target
                     (format nil "INSERT INTO ~a VALUES (~a)"
                             +standard-subscription-table+
                             (format nil "~{~a~^,~}"
                                     (mapcar #'logical-sql-literal
                                             (append (list slot) (logical-standard-config-values options)
                                                     (list (logical-subscription-options-actual-slot options) state lsn))))))))

(defun logical-standard-persist-state (target options state lsn &key before-commit after-commit)
  (logical-target-transaction
   target (lambda () (logical-standard-write-state target options state lsn))
   :before-commit before-commit :after-commit after-commit))

(defun logical-standard-save-lsn (target options lsn)
  ;; This UPDATE runs in the same target transaction as the applied row changes.
  (multiple-value-bind (rows columns tag)
      (pg-client-query target
                       (format nil "UPDATE ~a SET source_lsn=~a WHERE subscription_key=~a AND actual_slot=~a AND state='ready'"
                               +standard-subscription-table+ (logical-sql-literal lsn)
                               (logical-sql-literal (logical-subscription-options-slot-name options))
                               (logical-sql-literal (logical-subscription-options-actual-slot options))))
    (declare (ignore rows columns))
    (logical-assert-one-row-affected tag "standard progress update")))

(defun logical-standard-attempt-slot-p (name)
  (and (stringp name) (= (length name) 35) (string= name "pl_" :end1 3)
       (every (lambda (char) (find char "0123456789abcdef")) (subseq name 3))))

(defun logical-standard-new-attempt-slot ()
  (let ((bytes (make-array 16 :element-type '(unsigned-byte 8))))
    (with-open-file (input #p"/dev/urandom" :element-type '(unsigned-byte 8))
      (unless (= 16 (read-sequence bytes input)) (error "could not read slot-name entropy")))
    (string-downcase (format nil "pl_~{~2,'0x~}" (coerce bytes 'list)))))

(defun logical-standard-remote-slot (source options)
  (let ((rows (pg-client-query
               source
               (format nil "SELECT plugin,slot_type,datoid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database()),temporary,active,confirmed_flush_lsn,restart_lsn,invalidation_reason,two_phase,failover FROM pg_catalog.pg_replication_slots WHERE slot_name=~a"
                       (logical-sql-literal (logical-subscription-options-actual-slot options))))))
    (when (rest rows) (error "ambiguous source slot metadata"))
    (first rows)))

(defun logical-standard-retire-attempt (options)
  ;; A durable creating/copying intent plus an unguessable name is our ownership
  ;; evidence. PostgreSQL has no slot-owner tag against deliberate replacement.
  (let ((source (logical-startup-source-client options))
        (replication nil))
    (unwind-protect
         (let ((row (logical-standard-remote-slot source options)))
           (when row
             (unless (and (equal "pgoutput" (aref row 0)) (equal "logical" (aref row 1))
                          (equal "t" (aref row 2)) (equal "f" (aref row 3))
                          (equal "f" (aref row 4)) (equal "f" (aref row 8))
                          (equal "f" (aref row 9)))
               (error "recorded standard attempt slot is active or incompatible"))
             (setf replication (logical-startup-replication-client options))
             (pg-client-query replication
                              (format nil "DROP_REPLICATION_SLOT ~a" (logical-subscription-options-actual-slot options)))))
      (when replication (pg-client-close replication))
      (pg-client-close source))))

(defun logical-standard-legacy-progress-p (target slot)
  (handler-case (not (null (logical-read-progress target slot)))
    (pg-client-error (condition)
      (if (string= "42P01" (pg-client-error-sqlstate condition)) nil (error condition)))))

(defun logical-standard-subscription-run (options)
  (let ((target (logical-startup-target-client options)))
    (unwind-protect
         (progn
           (pg-client-query target
                            (format nil "SELECT pg_lisp_acquire_subscription(~a)"
                                    (logical-sql-literal (logical-subscription-options-slot-name options))))
           (logical-standard-ensure-state-table target)
           (let ((state (logical-standard-read-state target options)))
             (when (and (null state)
                        (logical-standard-legacy-progress-p
                         target (logical-subscription-options-slot-name options)))
               (error "legacy subscription progress exists; standard mode cannot reset it"))
             (logical-standard-validate-state options state nil)
             (setf (logical-subscription-options-source-identity options)
                   (logical-standard-source-identity options))
             (logical-standard-validate-state options state)
             (when state
               (setf (logical-subscription-options-actual-slot options) (getf state :actual-slot)))
             (cond ((null state) (logical-standard-create-copy-state options target))
                   ((member (getf state :state) '("creating" "copying") :test #'equal)
                    (logical-standard-resume-copy options target state))
                   ((equal "ready" (getf state :state)) nil)
                   (t (error "unknown standard subscription state")))
             (let ((ready (logical-standard-read-state target options)))
               (unless (and (equal "ready" (getf ready :state)) (getf ready :lsn))
                 (error "standard initial copy did not reach READY"))
               (setf (logical-subscription-options-actual-slot options) (getf ready :actual-slot))
               (logical-run-stream options target (parse-replication-lsn (getf ready :lsn))))))
      (pg-client-close target))))

(defun logical-standard-source-identity (options)
  "Bind standard progress to the publisher cluster, database OID, and publication."
  (let ((replication (logical-startup-replication-client options))
        (source nil))
    (unwind-protect
         (let ((identity (first (pg-client-query replication "IDENTIFY_SYSTEM"))))
           (unless (and identity (= (length identity) 4) (aref identity 0))
             (error "invalid IDENTIFY_SYSTEM result"))
           (setf source (logical-startup-source-client options))
           (let ((database (first (pg-client-query source
                                                   "SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database()"))))
             (unless database (error "source database has no durable OID"))
             (format nil "standard|~a|~a|~a" (aref identity 0) (aref database 0)
                     (logical-subscription-options-publication options))))
      (when source (pg-client-close source))
      (pg-client-close replication))))

(defun logical-standard-key-attnums (text count)
  "Use only indnkeyatts entries; indkey's trailing INCLUDE attributes are not keys."
  (let ((start 0) (values nil))
    (loop for end = (position #\Space text :start start)
          do (push (parse-integer text :start start :end end :junk-allowed nil) values)
          while end do (setf start (1+ end)))
    (setf values (nreverse values))
    (unless (and (plusp count) (<= count (length values)) (every #'plusp values))
      (error "invalid primary index key metadata"))
    (sort (subseq values 0 count) #'<)))

(defun logical-standard-key-signature (client table)
  (let* ((relations (pg-client-query
                     client
                     (format nil "SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname=~a"
                             (logical-sql-literal table))))
         (oid (and (= 1 (length relations)) (parse-integer (aref (first relations) 0))))
         (index (and oid (first (pg-client-query client
                                                 (format nil "SELECT i.indkey,i.indnkeyatts FROM pg_catalog.pg_index i WHERE i.indexrelid=pg_catalog.pg_get_replica_identity_index(~d) AND i.indisprimary AND i.indisvalid" oid)))))
         (keys (and index (logical-standard-key-attnums (aref index 0) (parse-integer (aref index 1)))))
         (attributes (and keys (pg-client-query client
                                                (format nil "SELECT attnum,attnotnull FROM pg_catalog.pg_attribute WHERE attrelid=~d AND attnum>0 ORDER BY attnum" oid)))))
    (unless (and keys
                 (every (lambda (key)
                          (let ((row (find key attributes :key (lambda (row) (parse-integer (aref row 0))))))
                            (and row (equal "t" (aref row 1)))))
                        keys))
      (error "standard table ~a requires a valid non-null primary key" table))
    (format nil "~{~d~^,~}" keys)))

(defun logical-standard-copy-schema (source target table)
  (let ((columns (logical-table-schema source table t))
        (target-columns (logical-table-schema target table t)))
    (logical-validate-client-schema table columns target-columns)
    (unless (and columns
                 (equal (logical-standard-key-signature source table)
                        (logical-standard-key-signature target table)))
      (error "source and target primary keys differ for table ~a" table))
    columns))

(defun logical-standard-publication-row-supported-p (row)
  (and (= 8 (length row))
       (equal "public" (aref row 0)) (equal "r" (aref row 2))
       (equal "d" (aref row 3)) (equal "t" (aref row 5))
       (equal "f" (aref row 7))
       (equal (aref row 4)
              (format nil "~{~d~^ ~}"
                      (loop for i from 1 to (parse-integer (aref row 6)) collect i)))))

(defun logical-standard-validate-publication (source options)
  "Validate the complete selected publication before mutating the target."
  (let* ((publication (logical-subscription-options-publication options))
         (actions (pg-client-query source
                                   (format nil "SELECT puballtables,pubinsert,pubupdate,pubdelete,pubtruncate,pubviaroot FROM pg_catalog.pg_publication WHERE pubname=~a"
                                           (logical-sql-literal publication))))
         (rows (pg-client-query
                source
                (format nil "SELECT n.nspname,c.relname,c.relkind,c.relreplident,gpt.attrs::text,gpt.qual IS NULL,c.relnatts,c.relispartition FROM pg_catalog.pg_get_publication_tables(~a) gpt JOIN pg_catalog.pg_class c ON c.oid=gpt.relid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace"
                        (logical-sql-literal publication))))
         (names nil))
    (unless (equalp actions '(#("f" "t" "t" "t" "t" "f")))
      (error "standard mode requires an explicit-table publication with all four publish actions"))
    (dolist (row rows)
      (unless (logical-standard-publication-row-supported-p row)
        (error "unsupported standard publication relation ~a.~a" (aref row 0) (aref row 1)))
      (pushnew (aref row 1) names :test #'equal))
    (setf names (sort names #'string<))
    (unless (equal names
                   (sort (copy-list (logical-subscription-options-tables options)) #'string<))
      (error "standard publication tables must match the configured table allowlist"))
    ;; Reject generated/dropped columns before SELECT * can silently change its
    ;; projection relative to Relation messages.
    (dolist (table names)
      (when (pg-client-query
             source
             (format nil "SELECT attnum FROM pg_catalog.pg_attribute WHERE attrelid=~a::regclass AND attnum>0 AND (attisdropped OR attgenerated!='')"
                     (logical-sql-literal (format nil "public.~a" table))))
        (error "standard source table ~a has unsupported dropped/generated columns" table)))))

(defun logical-standard-resume-slot-safe-p (row marker)
  (and row (= 10 (length row))
       (equal "pgoutput" (aref row 0)) (equal "logical" (aref row 1))
       (equal "t" (aref row 2)) (equal "f" (aref row 3))
       (equal "f" (aref row 4)) (aref row 5) (aref row 6)
       (null (aref row 7)) (equal "f" (aref row 8)) (equal "f" (aref row 9))
       (<= (parse-replication-lsn (aref row 5)) marker)
       (<= (parse-replication-lsn (aref row 6)) marker)))

(defun logical-standard-validate-resume-slot (options marker)
  ;; logical.c starts at max(requested, confirmed_flush). Never let a replaced
  ;; or advanced source slot move past target durability.
  (let ((source (logical-startup-source-client options)))
    (unwind-protect
         (progn
           (logical-standard-validate-publication source options)
           (unless (logical-standard-resume-slot-safe-p
                    (logical-standard-remote-slot source options) marker)
             (error "standard source slot cannot safely resume the durable target marker")))
      (pg-client-close source))))

(defun logical-standard-start-command (options marker)
  (format nil "START_REPLICATION SLOT ~a LOGICAL ~a (proto_version '1', publication_names ~a, binary 'false', streaming 'off', messages 'false', origin 'any')"
          (or (logical-subscription-options-actual-slot options)
              (error "standard actual slot was not loaded")) (wal-format-lsn marker)
          (logical-sql-literal (logical-subscription-options-publication options))))

(defun logical-standard-copy-snapshot (options target source boundary)
  ;; SOURCE already owns the snapshot installed by CREATE ... SNAPSHOT 'use'.
  (logical-standard-validate-publication source options)
  (dolist (table (logical-subscription-options-tables options))
    (logical-standard-copy-schema source target table))
  (logical-copy-delay-if-configured)
  (logical-copy-data-and-finish options target source boundary #'logical-standard-copy-schema))

(defun logical-standard-create-copy-state (options target)
  ;; Persist intent BEFORE CREATE, including the random actual slot. A lost
  ;; CREATE response is then distinguishable from an unrelated existing slot.
  (setf (logical-subscription-options-actual-slot options) (logical-standard-new-attempt-slot))
  (logical-standard-persist-state target options "creating" nil)
  (let ((source (logical-startup-replication-client options))
        (slot (logical-subscription-options-slot-name options)))
    (unwind-protect
         (progn
           (logical-test-failpoint :after-standard-slot-intent slot)
           ;; C tablesync.c uses one RR transaction and SNAPSHOT 'use'. The
           ;; permanent slot and all SELECT copies share that exact snapshot.
           (pg-client-query source "BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")
           (let* ((row (first (pg-client-query source
                                               (format nil "CREATE_REPLICATION_SLOT ~a LOGICAL pgoutput (SNAPSHOT 'use')"
                                                       (logical-subscription-options-actual-slot options)))))
                  (boundary (and row (aref row 1))))
             (unless (and row (= (length row) 4)
                          (equal (aref row 0) (logical-subscription-options-actual-slot options))
                          (equal (aref row 3) "pgoutput") boundary)
               (error "invalid standard replication slot result"))
             (logical-test-failpoint :after-source-slot-created-before-copying-progress slot boundary)
             (logical-standard-persist-state target options "copying" boundary)
             (logical-test-failpoint :after-copying-progress-commit slot boundary)
             (if (logical-subscription-options-binary-copy-p options)
                 (let* ((snapshot-rows (pg-client-query source "SELECT pg_export_snapshot()"))
                        (snapshot-row (first snapshot-rows))
                        (token (and snapshot-row (= (length snapshot-row) 1) (aref snapshot-row 0)))
                        (copy-source nil))
                   (unless (and (stringp token) (plusp (length token)))
                     (error "source did not export its copy snapshot"))
                   (unwind-protect
                        (progn
                          (sb-sys:without-interrupts
                            (setf copy-source (sb-sys:allow-with-interrupts
                                                (logical-startup-source-client options))))
                          (unless (eq (logical-import-copy-snapshot copy-source token) :imported)
                            (error "exported copy snapshot expired"))
                          (let ((*logical-copy-exporter* source))
                            (logical-standard-copy-snapshot options target copy-source boundary)))
                     (when copy-source (pg-client-close copy-source))))
                 (logical-standard-copy-snapshot options target source boundary))))
      (pg-client-close source))))

(defun logical-standard-resume-copy (options target progress)
  (unless (and (member (getf progress :state) '("creating" "copying") :test #'equal)
               (logical-standard-attempt-slot-p (getf progress :actual-slot)))
    (error "only an owned incomplete standard copy can be reseeded"))
  (setf (logical-subscription-options-actual-slot options) (getf progress :actual-slot))
  ;; Keep old intent until DROP is acknowledged or absence is observed. On a
  ;; lost DROP response the next retry inspects that same name again.
  (logical-standard-retire-attempt options)
  (logical-standard-create-copy-state options target))
