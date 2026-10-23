;;;; logical-tests.lisp -- logical row-change codec and WAL stream tests.

(in-package #:pg)

(defun logical-test-columns ()
  (list (list :attnum 1 :name "id" :type :int4 :typmod -1 :notnull t)
        (list :attnum 2 :name "amount" :type :int8 :typmod -1 :notnull nil)
        (list :attnum 3 :name "label" :type :varchar :typmod 64 :notnull nil)))

(defun logical-test-change (kind database-oid relation-oid
                            &key old-values new-values (key-attnums '(1)))
  (make-logical-change :kind kind :database-oid database-oid
                       :relation-oid relation-oid :relation-name "logical_rows"
                       :columns (logical-test-columns)
                       :key-attnums key-attnums
                       :old-values old-values :new-values new-values))

(defun test-logical-row-change-codec ()
  ;; These are the executor-boundary equivalents of DecodeInsert,
  ;; DecodeUpdate, and DecodeDelete in src/backend/replication/logical/decode.c.
  (let* ((old-row (vector 10 -90 "old"))
         (new-row (vector 10 120 +null+))
         (events
           (list (logical-test-change :insert 7 701 :new-values new-row)
                 (logical-test-change :update 7 701 :old-values old-row
                                                    :new-values new-row)
                 (logical-test-change :delete 7 701 :old-values old-row))))
    (dolist (event events)
      (let* ((encoded (logical-encode-change event))
             (decoded (logical-decode-change encoded)))
        (check (and (eq (logical-change-kind decoded)
                        (logical-change-kind event))
                    (= (logical-change-database-oid decoded) 7)
                    (= (logical-change-relation-oid decoded) 701)
                    (equal (logical-change-key-attnums decoded)
                           (logical-change-key-attnums event))
                    (equal (logical-change-columns decoded)
                           (logical-change-columns event))
                    (equalp (logical-change-old-values decoded)
                            (logical-change-old-values event))
                    (equalp (logical-change-new-values decoded)
                            (logical-change-new-values event)))
               "logical row image and schema round-trip")))
    (let* ((encoded (logical-encode-change (first events)))
           (with-trailing-byte
               (concatenate '(vector (unsigned-byte 8)) encoded #(0))))
      (check (handler-case
                 (progn (logical-decode-change with-trailing-byte) nil)
               (error () t))
             "logical row payload rejects trailing bytes"))
    (let* ((long-name (make-string 70000 :initial-element #\r))
           (event (logical-test-change :insert 7 701
                                       :new-values #(11 12 "long name"))))
      (setf (logical-change-relation-name event) long-name)
      (check (string= long-name
                      (logical-change-relation-name
                       (logical-decode-change (logical-encode-change event))))
             "logical schema names use lengths large enough for valid identifiers"))))

(defun test-logical-pgoutput-v2-codec ()
  ;; Compare the v2 message shapes with pgoutput.c's Relation/Begin/Commit and
  ;; logicalrep_write_insert/update/delete in src/backend/replication/.
  (let* ((lsn #x0102030405060708)
         (insert (logical-test-change :insert 7 701 :new-values #(10 20 "oak")))
         (update (logical-test-change :update 7 701
                                      :old-values #(10 20 "oak")
                                      :new-values #(10 30 "elm")))
         (delete (logical-test-change :delete 7 701
                                      :old-values #(10 30 "elm")))
         (transaction (make-logical-transaction
                       :xid 91 :commit-lsn lsn
                       :changes (list insert update delete)))
         (relation (pgoutput-build-relation insert))
         (insert-payload (pgoutput-build-row-change insert))
         (update-payload (pgoutput-build-row-change update))
         (delete-payload (pgoutput-build-row-change delete))
         (begin (pgoutput-build-begin transaction))
         (commit (pgoutput-build-commit transaction)))
    (check (and (= (aref relation 0) (char-code #\R))
                (= (bytes-to-u32 relation 1) 701)
                (= (aref insert-payload 0) (char-code #\I))
                (= (bytes-to-u32 insert-payload 1) 701))
           "pgoutput Relation and INSERT carry the source relation OID")
    (multiple-value-bind (tuple end)
        (logical-pgoutput-read-tuple insert-payload 6)
      (check (and (= (aref insert-payload 5) (char-code #\N))
                  (= end (length insert-payload))
                  (equalp tuple #("10" "20" "oak")))
             "pgoutput INSERT encodes a complete text-format tuple"))
    (multiple-value-bind (key end)
        (logical-pgoutput-read-tuple update-payload 6)
      (check (and (= (aref update-payload 5) (char-code #\K))
                  (= (aref update-payload end) (char-code #\N))
                  (equalp key #("10")))
             "pgoutput UPDATE sends the replica identity before the new tuple"))
    (multiple-value-bind (key end)
        (logical-pgoutput-read-tuple delete-payload 6)
      (check (and (= (aref delete-payload 5) (char-code #\K))
                  (= end (length delete-payload))
                  (equalp key #("10")))
             "pgoutput DELETE sends only its key tuple"))
    (check (string= (logical-row-key-predicate
                     #(nil) (list (make-pgoutput-column :name "id" :type-oid 23)))
                    "id IS NULL")
           "nullable NULLS NOT DISTINCT replica keys generate NULL-safe predicates")
    (check (and (= (aref begin 0) (char-code #\B))
                (= (pg-client-u64 begin 1) lsn)
                (= (bytes-to-u32 begin 17) 91)
                (= (length commit) 26)
                (= (aref commit 0) (char-code #\C))
                (= (pg-client-u64 commit 2) lsn)
                (= (pg-client-u64 commit 10) lsn))
           "pgoutput Begin and Commit retain their C wire field order and LSNs")
    (let* ((catalog-change
             (make-logical-change :kind :ddl :database-oid 7
                                  :event-payload "(:database-oid 7 :ast (:type :create-table))"))
           (standalone
             (pgoutput-build-message
              catalog-change
              (make-logical-transaction :xid 0 :commit-lsn lsn
                                        :changes (list catalog-change))))
           (truncate
             (make-logical-change :kind :truncate :database-oid 7
                                  :event-payload "(:tables (\"logical_rows\"))"))
           (transactional
             (pgoutput-build-message truncate transaction)))
      (check (and (= (aref standalone 0) (char-code #\M))
                  (= (aref standalone 1) 0)
                  (= (pg-client-u64 standalone 2) lsn))
             "pgoutput v2 emits standalone DDL Message without an XID")
      (check (and (= (aref transactional 0) (char-code #\M))
                  (= (bytes-to-u32 transactional 1) 91)
                  (= (aref transactional 5) 1)
                  (= (pg-client-u64 transactional 6) lsn))
             "pgoutput v2 emits transactional TRUNCATE Message with its XID")
      (multiple-value-bind (prefix event-lsn content)
          (logical-pgoutput-read-message
           (make-logical-subscription-runtime) standalone)
        (check (and (string= prefix "pg_lisp/ddl") (= event-lsn lsn)
                    (string= content (logical-change-event-payload catalog-change)))
               "subscriber decodes standalone v2 Message fields"))
      (multiple-value-bind (prefix event-lsn content)
          (logical-pgoutput-read-message
           (make-logical-subscription-runtime
            :transaction-open-p t :source-xid 91)
           transactional)
        (check (and (string= prefix "pg_lisp/truncate") (= event-lsn lsn)
                    (string= content (logical-change-event-payload truncate)))
               "subscriber validates transactional v2 Message XID and content")))
    (check (eq :create
               (logical-replication-command-type
                (parse-replication-command
                 "CREATE_REPLICATION_SLOT unit_slot LOGICAL pgoutput EXPORT_SNAPSHOT")))
           "logical slot command parser accepts the supported pgoutput syntax")
    (check (eq :start
               (logical-replication-command-type
                (parse-replication-command
                 "START_REPLICATION SLOT unit_slot LOGICAL 0/0 (proto_version '2')")))
           "logical slot command parser accepts the worker's pgoutput v2 start command")
    (check (handler-case
               (progn
                 (parse-replication-command
                  "START_REPLICATION SLOT unit_slot LOGICAL 0/0 (proto_version '1')")
                 nil)
             (error () t))
           "logical slot command parser rejects pgoutput v1 for v2 Message support")))

(defun test-logical-catalog-event-codec-and-parser ()
  (let* ((ast (list :type :alter-sequence :name "counter" :restart 19))
         (bytes (logical-encode-catalog-event 7 ast))
         (change (logical-decode-catalog-event bytes))
         (envelope (logical-read-data-string
                    (logical-change-event-payload change)))
         (sql (format nil "APPLY LOGICAL EVENT ~a 12345 ~a"
                      (logical-sql-literal "source:7:slot")
                      (logical-sql-literal
                       (catalog-readable-data-string envelope))))
         (apply-ast (first (parse-statements sql))))
    (check (and (eq (logical-change-kind change) :ddl)
                (= (logical-change-database-oid change) 7)
                (= (getf envelope :database-oid) 7)
                (equal (getf envelope :ast) ast))
           "catalog event preserves its source database and supported DDL AST")
    (check (and (eq (getf apply-ast :type) :apply-logical-event)
                (string= (getf apply-ast :event-key) "source:7:slot")
                (= (getf apply-ast :event-lsn) 12345)
                (string= (getf apply-ast :payload)
                         (catalog-readable-data-string envelope)))
           "private APPLY LOGICAL EVENT SQL parses the idempotent event envelope")
    (check (handler-case
               (progn (logical-read-data-string "(:type :ddl) (:type :drop-table)") nil)
             (error () t))
           "logical event parser rejects trailing S-expression payloads")
    (check (handler-case
               (progn (logical-read-data-string "#.(error \"unsafe\")") nil)
             (error () t))
           "logical event parser disables reader evaluation")))

(defun test-logical-catalog-terminal-validation ()
  (let ((catalog-event (make-logical-change :kind :ddl :database-oid 7))
        (row-change (logical-test-change :insert 7 701 :new-values #(1 2 "a")))
        (commit (make-wal-record :type +wal-record-commit+ :xid 9 :end 100))
        (catalog-terminal
          (make-wal-record :type +wal-record-catalog+ :xid 0 :end 100)))
    (check (handler-case
               (progn
                 (wal-logical-reader-finish-batch
                  (make-logical-reader-state :database-oid 7 :batch-open-p t
                                             :changes (list catalog-event))
                  commit)
                 nil)
             (error () t))
           "catalog events cannot become visible at a row commit terminal")
    (check (handler-case
               (progn
                 (wal-logical-reader-finish-batch
                  (make-logical-reader-state :database-oid 7 :batch-open-p t
                                             :changes (list row-change))
                  catalog-terminal)
                 nil)
             (error () t))
           "row changes cannot become visible at a catalog terminal")
    (check (handler-case
               (progn
                 (let ((*wal-recovery-logical-catalog-event-p* t)
                       (*wal-recovery-logical-chunk-count* nil))
                   (wal-recovery-validate-logical-terminal :commit 9))
                 nil)
             (error () t))
           "WAL recovery rejects a catalog event followed by a row commit")
    (check (handler-case
               (progn
                 (let ((*wal-recovery-logical-catalog-event-p* nil)
                       (*wal-recovery-logical-chunk-count* 1))
                   (wal-recovery-validate-logical-terminal :catalog))
                 nil)
             (error () t))
           "WAL recovery rejects row changes followed by a catalog terminal")))

(defun test-logical-replication-bootstrap-definitions ()
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend :database "postgres"))
         (db (make-database :name "postgres" :oid 7))
         (table
           (make-pg-class
            :oid 301 :name "bootstrap_rows" :relfilenode 1301
            :columns
            (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                    :attnum 1 :notnull t)
                    (make-pg-column :name "amount" :type :int8 :typmod -1
                                    :attnum 2 :notnull nil
                                    :default-sequence-name "bootstrap_seq"))
            :indexes '(401 402)))
         (primary
           (make-pg-index :oid 401 :name "bootstrap_rows_pkey"
                          :relfilenode 1401 :table-oid 301 :columns '(1)
                          :unique-p t :primary-p t :constraint-p t :valid-p t))
         (standalone
           (make-pg-index :oid 402 :name "bootstrap_rows_amount_idx"
                          :relfilenode 1402 :table-oid 301 :columns '(2)
                          :unique-p t :nulls-not-distinct-p t :valid-p t))
         (sequence
           (make-pg-sequence :oid 501 :name "bootstrap_seq" :increment -2
                             :start-value -1 :min-value +int8-min+
                             :max-value -1 :last-value -9 :called-p t)))
    (setf (gethash "postgres" (catalog-databases *catalog*)) db
          (gethash "bootstrap_rows" (database-tables db)) table
          (gethash "bootstrap_rows_pkey" (database-indexes db)) primary
          (gethash "bootstrap_rows_amount_idx" (database-indexes db)) standalone
          (gethash "bootstrap_seq" (database-sequences db)) sequence)
    (let* ((bootstrap (catalog-replication-bootstrap-data db))
           (sequence-snapshot
             (make-snapshot-struct
              :sequence-database-oid 7 :sequence-state-captured-p t
              :sequence-states (getf bootstrap :sequences)))
           (table-sql (getf (first (getf bootstrap :tables)) :sql))
           (index-entry (first (getf bootstrap :indexes)))
           (sequence-entry (first (getf bootstrap :sequences))))
      (check (and (search "CREATE TABLE IF NOT EXISTS" table-sql)
                  (search "DEFAULT nextval('bootstrap_seq')" table-sql)
                  (search "CONSTRAINT \"bootstrap_rows_pkey\" PRIMARY KEY" table-sql)
                  (not (search "bootstrap_rows_amount_idx" table-sql)))
             "bootstrap table definition retains defaults and only constraint indexes")
      (check (and (string= (getf index-entry :name) "bootstrap_rows_amount_idx")
                  (search "CREATE UNIQUE INDEX IF NOT EXISTS" (getf index-entry :sql))
                  (search "NULLS NOT DISTINCT" (getf index-entry :sql)))
             "bootstrap emits standalone btree indexes as separate SQL")
      (check (and (search (format nil "INCREMENT BY -2 MINVALUE ~d MAXVALUE -1 START WITH -1"
                                  +int8-min+)
                          (getf sequence-entry :definition))
                  (= (getf (getf sequence-entry :state) :last) -9)
                  (getf (getf sequence-entry :state) :called))
             "bootstrap preserves sequence increment, bounds, start, and state")
      ;; The exported snapshot keeps its sequence state from the slot boundary,
      ;; even though PostgreSQL sequences are not MVCC-visible.
      (setf (pg-sequence-last-value sequence) -11)
      (let* ((captured-bootstrap
               (catalog-replication-bootstrap-data db sequence-snapshot))
             (captured-sequence
               (first (getf captured-bootstrap :sequences))))
        (check (= (getf (getf captured-sequence :state) :last) -9)
               "bootstrap uses the sequence state captured with the slot boundary")))))

(defun test-wal-logical-catalog-event-slot-gating ()
  (let* ((*wal-replication-slots* (make-hash-table :test #'equal))
         (*wal-logical-catalog-event-interests* (make-hash-table :test #'eq))
         (slot (make-wal-replication-slot :name "catalog_event_slot"
                                          :database-oid 7)))
    (sb-thread:with-mutex (*wal-lock*)
      (check (not (wal-logical-catalog-event-needed-under-lock-p 7))
             "catalog state before logical slot creation needs no stream event")
      (setf (gethash "catalog_event_slot" *wal-replication-slots*) slot)
      (check (wal-logical-catalog-event-needed-under-lock-p 7)
             "catalog changes are recorded for a live database slot")
      (check (not (wal-logical-catalog-event-needed-under-lock-p 8))
             "catalog events are scoped to the slot database")
      (setf (wal-replication-slot-wal-status slot) :lost)
      (check (not (wal-logical-catalog-event-needed-under-lock-p 7))
             "a lost slot no longer requires catalog stream events"))))

(declaim (ftype (function (function) *) logical-test-with-wal-storage))

(defun test-logical-snapshot-capture-to-slot-publication-interest ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*catalog* (make-catalog))
            (*backend* (make-backend :database "postgres"))
            (*active-xids* nil)
            (*next-xid* 3)
            (*active-snapshots* 0)
            (*active-snapshot-set* (make-hash-table :test #'eq))
            (*wal-xid-first-lsn* (make-hash-table :test #'eql))
            (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
            (*wal-page-cache* (make-hash-table :test #'equal))
            (*wal-page-cache-xids* (make-hash-table :test #'equal))
            (database (make-database :name "postgres" :oid 7))
            (sequence
              (make-pg-sequence :oid 701 :name "gap_counter" :start-value 10
                                :increment 1 :min-value 1 :max-value +int8-max+
                                :last-value 10 :called-p t))
            (boundary-lsn nil)
            (token nil)
            (event-p nil)
            (*logical-test-after-snapshot-capture-hook*
              (lambda (slot-name captured-lsn)
                (declare (ignore slot-name))
                (setf boundary-lsn captured-lsn)
                (sb-thread:with-mutex (*wal-lock*)
                  (check (wal-logical-catalog-event-needed-under-lock-p 7)
                         "captured snapshot reserves catalog events before slot publish"))
                ;; This is the concurrent sequence.c operation that can run
                ;; after snapshot capture releases *WRITE-LOCK* but before slot
                ;; publication makes the durable slot visible.
                (sequence-nextval "gap_counter"))))
       (setf (gethash "postgres" (catalog-databases *catalog*)) database
             (gethash "gap_counter" (database-sequences database)) sequence)
       (unwind-protect
            (progn
              (multiple-value-setq (boundary-lsn token)
                (create-exported-logical-snapshot-slot "gap_slot"))
              (let* ((exported (gethash token *exported-logical-snapshots*))
                     (captured-state
                       (getf (first
                              (snapshot-sequence-states
                               (exported-logical-snapshot-snapshot exported)))
                             :state)))
                (check (and (= (getf captured-state :last) 10)
                            (getf captured-state :called)
                            (= (pg-sequence-last-value sequence) 11))
                       "copy retains boundary sequence state before an interleaved nextval")
                (wal-scan
                 (lambda (record)
                   (when (= (wal-record-type record)
                            +wal-record-logical-catalog-event+)
                     (let ((change
                             (logical-decode-catalog-event
                              (wal-record-payload record))))
                       (when (and (> (wal-record-end record) boundary-lsn)
                                  (eq (logical-change-kind change) :sequence-state))
                         (setf event-p t)))))
                 boundary-lsn)
                (check event-p
                       "sequence advance between capture and publication is streamable")))
         (when token
           (remove-exported-logical-snapshot-token token))
         (ignore-errors (wal-drop-replication-slot "gap_slot")))))))

(defun test-create-sequence-parser-bounds ()
  (let* ((explicit
           (first (parse-statements
                   "CREATE SEQUENCE bounded_counter INCREMENT BY -2 MINVALUE -90 MAXVALUE 5 START WITH 4")))
         (default-descending
           (first (parse-statements
                   "CREATE SEQUENCE descending_counter INCREMENT BY -2"))))
    (check (and (= (getf explicit :increment) -2)
                (= (getf explicit :min) -90)
                (= (getf explicit :max) 5)
                (= (getf explicit :start) 4))
           "CREATE SEQUENCE parses explicit int8 minimum and maximum bounds")
    (check (and (= (getf default-descending :min) +int8-min+)
                (= (getf default-descending :max) -1)
                (= (getf default-descending :start) -1))
           "descending sequence defaults follow sequence.c init_params")
    (check (handler-case
               (progn
                 (parse-statements
                  "CREATE SEQUENCE invalid_counter MINVALUE 20 MAXVALUE 10")
                 nil)
             (error () t))
           "CREATE SEQUENCE rejects bounds that do not contain its start")))

(defun test-logical-subscription-argument-parser ()
  (let ((options
          (parse-logical-subscription-args
           '("--slot" "test_slot" "--source-port" "5434"
             "--target-port" "5435" "--table" "first_rows"
             "--table" "second_rows"))))
    (check (and (= (logical-subscription-options-source-port options) 5434)
                (= (logical-subscription-options-target-port options) 5435)
                (equal (logical-subscription-options-tables options)
                       '("first_rows" "second_rows")))
           "subscriber CLI consumes option values and retains repeated tables")
    (check (handler-case
               (progn
                 (parse-logical-subscription-args
                  '("--slot" "test_slot" "--table"))
                 nil)
             (error () t))
           "subscriber CLI rejects an option with no value")))

(defun test-logical-subscription-resume-config ()
  (let* ((options (make-logical-subscription-options
                   :source-host "publisher.example" :source-port 5440
                   :source-database "publisher_db"
                   :source-identity "cluster-a"
                   :tables '("z_rows" "a_rows")))
         (config (list :source-host "publisher.example"
                       :source-identity "cluster-a"
                       :source-port 5440
                       :source-database "publisher_db"
                       :table-allowlist "a_rows,z_rows")))
    (check (string= (fourth (logical-subscription-config-values options))
                    "a_rows,z_rows")
           "subscriber config canonicalizes allowlist ordering")
    (check (null (logical-validate-resume-config options '(:state "ready") config))
           "matching source and allowlist permit a safe resume")
    (check (handler-case
               (progn
                 (logical-validate-resume-config
                  (make-logical-subscription-options
                   :source-host "other.example" :source-port 5440
                   :source-database "publisher_db" :tables '("z_rows" "a_rows"))
                  '(:state "ready") config)
                 nil)
             (error () t))
           "subscriber config rejects changing the source endpoint")
    (check (handler-case
               (progn
                 (logical-validate-resume-config
                  (make-logical-subscription-options
                   :source-host "publisher.example" :source-port 5440
                   :source-database "publisher_db" :tables '("a_rows"))
                  '(:state "ready") config)
                 nil)
             (error () t))
           "subscriber config rejects changing the table allowlist")
    (check (handler-case
               (progn
                 (logical-validate-resume-config options '(:state "ready") nil)
                 nil)
             (error () t))
           "legacy progress without a saved config cannot resume unsafely")
    (check (handler-case
               (progn
                 (logical-validate-resume-config
                  (make-logical-subscription-options
                   :source-host "publisher.example" :source-port 5440
                   :source-database "publisher_db"
                   :source-identity "replacement-cluster"
                   :tables '("z_rows" "a_rows"))
                  '(:state "ready") config)
                 nil)
             (error () t))
           "a replacement publisher incarnation cannot reuse the old event marker")))

(defun logical-test-append-batch (xid changes &optional commit-p)
  ;; Like reorderbuffer.c, the reader must not expose queued changes until a
  ;; transaction's commit record closes the WAL batch.
  (wal-append-batch-start)
  (when changes
    (wal-append-logical-changes xid (mapcar #'logical-encode-change changes)))
  (when commit-p
    (wal-append-record +wal-record-commit+ xid #())))

(defun logical-test-chunk-record (xid index count)
  (let ((event
          (logical-encode-change
           (logical-test-change :insert 7 701 :new-values #(1 2 "chunk")))))
    (make-wal-record :type +wal-record-logical-changes+ :xid xid
                     :payload (logical-encode-chunk index count (list event)))))

(defun test-logical-change-chunk-validation ()
  (let* ((event
           (logical-encode-change
            (logical-test-change :insert 7 701 :new-values #(1 2 "chunk"))))
         (change-bytes (length event))
         (first-chunk
           (make-wal-record :type +wal-record-logical-changes+ :xid 88
                            :payload (logical-encode-chunk 0 2 (list event)))))
    (labels ((rejects (record xid count index bytes)
               (handler-case
                   (progn
                     (wal-advance-logical-change-chunk
                      record xid count index bytes)
                     nil)
                 (error () t))))
      (multiple-value-bind (xid count next-index bytes changes)
          (wal-advance-logical-change-chunk first-chunk nil nil 0 0)
        (check (and (= xid 88) (= count 2) (= next-index 1)
                    (= bytes change-bytes) (= (length changes) 1))
               "logical chunk progress records the first complete chunk"))
      (check (rejects (logical-test-chunk-record 88 1 2) nil nil 0 0)
             "logical chunks must start at index zero")
      (check (rejects (logical-test-chunk-record 0 0 2) nil nil 0 0)
             "logical chunks require a positive transaction ID")
      (check (rejects (logical-test-chunk-record 89 1 2) 88 2 1 change-bytes)
             "logical chunks cannot change transaction ID")
      (check (rejects (logical-test-chunk-record 88 0 2) 88 2 1 change-bytes)
             "logical chunks must be sequential")
      (check (rejects (logical-test-chunk-record 88 1 3) 88 2 1 change-bytes)
             "logical chunk count cannot change within a transaction")
      (check (rejects (logical-test-chunk-record 88 1 2) 88 2 1
                      +max-logical-wal-transaction-bytes+)
             "logical chunk progress enforces the 65 MiB transaction cap")
      (let ((state (make-logical-reader-state :database-oid 7 :batch-open-p t
                                              :next-chunk 0))
            (commit (make-wal-record :type +wal-record-commit+ :xid 88 :end 100)))
        (wal-logical-reader-add-chunk state first-chunk)
        (check (handler-case
                   (progn (wal-logical-reader-finish-batch state commit) nil)
                 (error () t))
               "logical commit terminals require every declared chunk")))))

(defun logical-test-with-wal-storage (function)
  (let* ((test-pgdata
           (merge-pathnames (format nil "logical-stream-~a/" (random 100000000))
                            *pgdata*))
         (*pgdata* test-pgdata)
         (*backend* nil)
         (*wal-ready* t)
         (*wal-end-lsn* 0)
         (*wal-checkpoint-lsn* 0)
         (*wal-durable-lsn* 0)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (*wal-new-segments* '())
         (*wal-poisoned* nil)
         (*clog-stream* nil)
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (*wal-logical-catalog-event-interests* (make-hash-table :test #'eq))
         (*max-slot-wal-keep-bytes* nil))
    (ignore-errors
     (uiop:delete-directory-tree test-pgdata :validate t
                                             :if-does-not-exist :ignore))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    (wal-ensure-directory-durable (wal-replication-slot-directory))
    (unwind-protect
         (funcall function)
      (when *wal-output-stream*
        (ignore-errors (close *wal-output-stream*)))
      (when *clog-stream*
        (ignore-errors (close *clog-stream*)))
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun logical-test-assert-owned-reader-progress (slot first-end later-end)
  (call-with-logical-replication-slot
   "logical_slot"
   (lambda (owned-slot)
     (declare (ignore owned-slot))
     (let ((bounded-transactions
             (wal-logical-slot-read-transactions
              "logical_slot" :max-transactions 1)))
       (check (equal (mapcar #'logical-transaction-xid bounded-transactions)
                     '(901))
              "logical reader stops at its transaction-count boundary"))
     (let ((bounded-transactions
             (wal-logical-slot-read-transactions
              "logical_slot" :max-transactions 8 :max-bytes 1)))
       (check (equal (mapcar #'logical-transaction-xid bounded-transactions)
                     '(901))
              "byte budget returns one complete oversized transaction"))
     (check (handler-case
                (progn
                  (wal-logical-slot-acknowledge "logical_slot" later-end)
                  nil)
              (error () t))
            "slot cannot acknowledge a transaction beyond read progress")
     (check (handler-case
                (progn
                  (wal-logical-slot-acknowledge "logical_slot" "invalid")
                  nil)
              (error () t))
            "logical acknowledgment rejects non-integer LSNs")
     (let ((transactions (wal-logical-slot-read-transactions "logical_slot")))
       (check (equal (mapcar #'logical-transaction-xid transactions) '(901 903))
              "reader drops crash tails and filters other databases")
       (check (< (logical-transaction-commit-lsn (first transactions))
                 (logical-transaction-commit-lsn (second transactions)))
              "logical transactions are emitted in durable commit order")
       (check (equal (mapcar #'logical-change-kind
                             (logical-transaction-changes (first transactions)))
                     '(:insert :update))
              "row changes retain executor order within a transaction")
       (check (handler-case
                  (progn
                    (wal-logical-slot-acknowledge
                     "other_slot"
                     (logical-transaction-commit-lsn (first transactions)))
                    nil)
                (error () t))
              "a reader cannot acknowledge a different slot")
       (wal-logical-slot-acknowledge
        "logical_slot"
        (logical-transaction-commit-lsn (first transactions)))
       (check (= (wal-replication-slot-confirmed-flush-lsn slot) first-end)
              "slot acknowledgment persists the applied commit boundary")
       (multiple-value-bind (transactions progress-lsn)
           (wal-logical-slot-read-transactions
            "logical_slot" :max-scan-bytes 1)
         (check (null transactions)
                "bounded scan can make progress without row transactions")
         (check (> progress-lsn first-end)
                "empty read returns its last complete terminal LSN")
         (wal-logical-slot-acknowledge "logical_slot" progress-lsn))
       (check (equal (mapcar #'logical-transaction-xid
                             (wal-logical-slot-read-transactions "logical_slot"))
                     '(903))
              "a second read resumes after the durable acknowledgment")))))

(defun test-logical-wal-reader-commit-order-and-slot-ack ()
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((slot (make-wal-replication-slot
                   :name "logical_slot" :database-oid 7 :restart-lsn 0
                   :consistent-lsn 0 :confirmed-flush-lsn 0))
            (other-slot (make-wal-replication-slot
                         :name "other_slot" :database-oid 7 :restart-lsn 0
                         :consistent-lsn 0 :confirmed-flush-lsn 0))
            (lost-slot (make-wal-replication-slot
                        :name "lost_slot" :database-oid 7 :restart-lsn 0
                        :consistent-lsn 0 :confirmed-flush-lsn 0
                        :wal-status :lost))
            (first-end nil)
            (later-end nil))
       (setf (gethash "logical_slot" *wal-replication-slots*) slot
             (gethash "other_slot" *wal-replication-slots*) other-slot
             (gethash "lost_slot" *wal-replication-slots*) lost-slot)
       ;; An unterminated batch before a later batch mirrors a crash tail that
       ;; wal-replay-record discards when it sees the next batch-start record.
       (logical-test-append-batch
        900 (list (logical-test-change :insert 7 701
                                       :new-values #(90 0 "aborted"))))
       (logical-test-append-batch
        901 (list (logical-test-change :insert 7 701
                                       :new-values #(10 1 "first"))
                  (logical-test-change :update 7 701
                                       :old-values #(10 1 "first")
                                       :new-values #(10 2 "second")))
        t)
       (setf first-end *wal-end-lsn*)
       ;; A slot is database-scoped even though the physical WAL is shared.
       (logical-test-append-batch
        902 (list (logical-test-change :insert 8 801
                                       :new-values #(20 2 "other db")))
        t)
       (logical-test-append-batch
        903 (list (logical-test-change :delete 7 701
                                       :old-values #(30 3 "third")))
        t)
       (setf later-end *wal-end-lsn*)
       ;; The scan's durable horizon may also end in an incomplete crash tail.
       (logical-test-append-batch
        904 (list (logical-test-change :insert 7 701
                                       :new-values #(40 4 "uncommitted"))))
       (wal-flush)
       (wal-recover)
       (check (handler-case
                  (progn
                    (call-with-logical-replication-slot "lost_slot"
                                                        (lambda (owned-slot) (declare (ignore owned-slot))))
                    nil)
                (error () t))
              "logical readers cannot acquire a slot whose WAL was lost")
       (check (handler-case
                  (progn (wal-logical-slot-read-transactions "logical_slot") nil)
                (error () t))
              "logical slot reads require an explicit active owner")
       (check (handler-case
                  (progn (wal-logical-slot-acknowledge "logical_slot" first-end)
                         nil)
                (error () t))
              "logical acknowledgment requires ownership of its slot")
       (logical-test-assert-owned-reader-progress slot first-end later-end)))))

(defun logical-test-large-change (id payload)
  (make-logical-change
   :kind :insert :database-oid 7 :relation-oid 702
   :relation-name "wide_logical_rows"
   :columns (list (list :attnum 1 :name "id" :type :int4
                        :typmod -1 :notnull t)
                  (list :attnum 2 :name "payload" :type :varchar
                        :typmod -1 :notnull nil))
   :key-attnums '(1) :new-values (vector id payload)))

(defun logical-test-capture-row-change (catalog table application-name byte-count)
  (let* ((*catalog* catalog)
         (*backend* (make-backend :xid 9902
                                  :application-name application-name
                                  :database "postgres")))
    (setf (backend-logical-change-bytes *backend*) byte-count)
    (handler-case
        (progn
          (logical-note-row-change :insert table :new-values #(1 "small"))
          (values t (backend-logical-change-bytes *backend*)))
      (error () (values nil (backend-logical-change-bytes *backend*))))))

(defun test-logical-capture-application-headroom ()
  (let* ((catalog (make-catalog))
         (database (make-database :name "postgres" :oid 7))
         (table
           (make-pg-class
            :oid 702 :name "logical_rows"
            :columns (vector (make-pg-column :name "id" :type :int4
                                             :typmod -1 :attnum 1 :notnull t)
                             (make-pg-column :name "label" :type :varchar
                                             :typmod -1 :attnum 2 :notnull nil)))))
    (setf (gethash "postgres" (catalog-databases catalog)) database
          (gethash "logical_rows" (database-tables database)) table)
    (multiple-value-bind (accepted bytes)
        (logical-test-capture-row-change
         catalog table "postgres" +max-logical-transaction-bytes+)
      (declare (ignore bytes))
      (check (not accepted)
             "ordinary source transactions remain capped at 64 MiB"))
    (multiple-value-bind (accepted bytes)
        (logical-test-capture-row-change
         catalog table "pg-lisp-subscribe-apply"
         +max-logical-transaction-bytes+)
      (check (and accepted (> bytes +max-logical-transaction-bytes+)
                  (<= bytes +max-logical-wal-transaction-bytes+))
             "subscriber apply capture may use only its reserved headroom"))
    (multiple-value-bind (accepted bytes)
        (logical-test-capture-row-change
         catalog table "pg-lisp-subscribe-apply"
         +max-logical-wal-transaction-bytes+)
      (declare (ignore bytes))
      (check (not accepted)
             "subscriber apply capture still rejects changes beyond 65 MiB"))))

(defun test-logical-wal-apply-headroom-recovery ()
  ;; The subscriber's own progress row is committed alongside each source
  ;; transaction. Leave 1 MiB of headroom above the source's 64 MiB event cap.
  ;; Keep individual records below the WAL segment record-size limit.
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((payload-size (+ (* 10 1024 1024) (* 768 1024)))
            (payload (make-string payload-size :initial-element #\x))
            (changes (loop for id from 1 to 6
                           collect (logical-test-large-change id payload)))
            (encoded (mapcar #'logical-encode-change changes))
            (change-bytes (reduce #'+ encoded :key #'length :initial-value 0))
            (slot (make-wal-replication-slot
                   :name "headroom_slot" :database-oid 7 :restart-lsn 0
                   :consistent-lsn 0 :confirmed-flush-lsn 0)))
       (check (and (> change-bytes +max-logical-transaction-bytes+)
                   (<= change-bytes +max-logical-wal-transaction-bytes+))
              "target apply metadata fits the WAL transaction headroom")
       (setf (gethash "headroom_slot" *wal-replication-slots*) slot)
       (logical-test-append-batch 9901 changes t)
       (wal-flush)
       (wal-recover)
       (call-with-logical-replication-slot
        "headroom_slot"
        (lambda (owned-slot)
          (declare (ignore owned-slot))
          (let* ((transactions
                   (wal-logical-slot-read-transactions
                    "headroom_slot" :max-bytes (* 8 1024 1024)))
                 (transaction (first transactions)))
            (check (and (= (length transactions) 1)
                        (= (length (logical-transaction-changes transaction)) 6)
                        (= (length (aref
                                    (logical-change-new-values
                                     (first (logical-transaction-changes transaction)))
                                    1))
                           payload-size))
                   "near-limit logical transaction recovers and reads as one commit"))))))))

(defun logical-test-commit-boundary-batch (xid)
  (with-wal-critical
    (let ((batch-start (wal-append-batch-start)))
      ;; Model an earlier page batch from XID before a logical slot captures its
      ;; snapshot; the slot must retain this active XID through the later commit.
      (wal-record-page-xids-at-batch-start
       (list (make-wal-page-image :xids (list xid))) batch-start)
      (wal-append-record +wal-record-maintenance+ 0 #())
      (wal-flush)
      batch-start)))

(defun logical-test-commit-insert (xid table values)
  (let* ((*backend* (make-backend :xid xid))
         (*active-xids* (cons xid *active-xids*)))
    (setf (backend-database *backend*) "postgres"
          (backend-state *backend*) :in-progress)
    (logical-note-row-change :insert table :new-values values)
    (commit-transaction)))

(defun logical-test-abort-insert (xid table)
  (let* ((*backend* (make-backend :xid xid))
         (*active-xids* (cons xid *active-xids*))
         (wal-end-before *wal-end-lsn*))
    (setf (backend-database *backend*) "postgres"
          (backend-state *backend*) :in-progress)
    (logical-note-row-change :insert table :new-values #(58 7 "aborted"))
    (abort-transaction)
    (check (null (backend-logical-changes *backend*))
           "rollback discards its transaction-local logical queue")
    (check (= *wal-end-lsn* wal-end-before)
           "rollback emits no logical WAL records")))

(defun test-logical-snapshot-handoff-active-xid ()
  ;; SnapBuildProcessRunningXacts and CreateInitDecodingContext establish a
  ;; consistent point while concurrent transactions remain in progress. This
  ;; test captures a row before that point, commits its XID afterward, and reads
  ;; it only from the shared snapshot/stream boundary.
  (logical-test-with-wal-storage
   (lambda ()
     (let* ((*catalog* (make-catalog))
            (database (make-database :name "postgres" :oid 7))
            (table
              (make-pg-class
               :oid 701 :name "logical_rows"
               :columns
               (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                       :attnum 1 :notnull t)
                       (make-pg-column :name "amount" :type :int8 :typmod -1
                                       :attnum 2 :notnull nil)
                       (make-pg-column :name "label" :type :varchar :typmod 64
                                       :attnum 3 :notnull nil))))
            (xid 9501)
            (*backend* (make-backend :xid xid))
            (*active-xids* (list xid))
            (*next-xid* (1+ xid))
            (*active-snapshots* 0)
            (*active-snapshot-set* (make-hash-table :test #'eq))
            (*wal-xid-first-lsn* (make-hash-table :test #'eql))
            (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
            (*wal-page-cache* (make-hash-table :test #'equal))
            (*wal-page-cache-xids* (make-hash-table :test #'equal))
            (registration (make-snapshot-registration))
            (owner (gensym "SNAPSHOT-HANDOFF-"))
            (during-copy-xid (1+ xid))
            (boundary-lsn nil)
            (restart-lsn nil)
            (snapshot nil)
            (slot nil)
            (ack-lsn nil))
       (setf (backend-database *backend*) "postgres"
             (backend-state *backend*) :in-progress
             (gethash "postgres" (catalog-databases *catalog*)) database
             (gethash "logical_rows" (database-tables database)) table)
       (logical-note-row-change :insert table :new-values #(55 4 "across boundary"))
       (logical-test-commit-boundary-batch xid)
       (multiple-value-setq (snapshot boundary-lsn restart-lsn slot)
         (with-write-lock
           (wal-create-replication-slot-under-writer-lock
            "handoff_slot" 0 registration owner 7)))
       (check (member xid (snapshot-xip snapshot) :test #'=)
              "snapshot contains the active writer")
       (check (< restart-lsn boundary-lsn)
              (format nil "slot retains active writer WAL (restart=~a boundary=~a xid-first=~a)"
                      restart-lsn boundary-lsn (gethash xid *wal-xid-first-lsn*)))
       (check (null (wal-replication-slot-confirmed-flush-lsn slot))
              "new slot awaits initial-copy acknowledgment")
       (check (not (member during-copy-xid (snapshot-xip snapshot) :test #'=))
              "transaction started after the snapshot is outside the copy")
       (logical-test-commit-insert
        during-copy-xid table #(56 5 "committed during copy"))
       ;; A second statement in the original transaction remains attached to
       ;; its XID even though it runs after the snapshot boundary.
       (logical-note-row-change :insert table :new-values #(57 6 "second statement"))
       (release-snapshot-registration registration)
       (wal-finalize-replication-slot-copy "handoff_slot" boundary-lsn owner)
       (wal-release-replication-slot-owner "handoff_slot" owner)
       (commit-transaction)
       (check (and (null (backend-logical-changes *backend*))
                   (not (member xid *active-xids* :test #'=)))
              "real transaction commit publishes and retires its queued row change")
       (logical-test-abort-insert (1+ during-copy-xid) table)
       ;; Startup recovery validates complete v5 row records before the slot
       ;; reader resumes from its durable acknowledgment.
       (wal-recover)
       (call-with-logical-replication-slot
        "handoff_slot"
        (lambda (owned-slot)
          (declare (ignore owned-slot))
          (let* ((transactions
                   (wal-logical-slot-read-transactions "handoff_slot"))
                 (transaction (second transactions))
                 (change (first (logical-transaction-changes transaction))))
            (check (and (= (length transactions) 2)
                        (equal (mapcar #'logical-transaction-xid transactions)
                               (list during-copy-xid xid))
                        (> (logical-transaction-commit-lsn transaction)
                           boundary-lsn))
                   "copy-time and active-at-snapshot transactions stream in commit order")
            (check (and (eq (logical-change-kind change) :insert)
                        (equalp (logical-change-new-values change)
                                #(55 4 "across boundary")))
                   "executor-captured row reaches the reader through real commit WAL")
            (check (equalp (mapcar #'logical-change-new-values
                                   (logical-transaction-changes transaction))
                           '(#(55 4 "across boundary")
                             #(57 6 "second statement")))
                   "multi-statement row changes retain their transaction order")
            (setf ack-lsn
                  (logical-transaction-commit-lsn (second transactions)))
            (wal-logical-slot-acknowledge "handoff_slot" ack-lsn))))
       ;; Slot files persist subscriber progress across restart. The resumed
       ;; reader must start after the last durable acknowledgment.
       (wal-load-replication-slots)
       (setf slot (gethash "handoff_slot" *wal-replication-slots*))
       (check (and (= (wal-replication-slot-database-oid slot) 7)
                   (= (wal-replication-slot-confirmed-flush-lsn slot) ack-lsn))
              "restart reloads the database binding and logical acknowledgment")
       (call-with-logical-replication-slot
        "handoff_slot"
        (lambda (owned-slot)
          (declare (ignore owned-slot))
          (check (null (wal-logical-slot-read-transactions "handoff_slot"))
                 "restart does not replay acknowledged transactions")))))))

(defun test-logical-snapshot-import-locks-token-through-pin ()
  (let* ((catalog (make-catalog))
         (database (make-database :name "postgres" :oid 7))
         (token (new-exported-logical-snapshot-token))
         (source-snapshot
           (make-snapshot-struct :xmin 10 :xmax 20 :xip '() :curcid 1))
         (source-registration (make-snapshot-registration))
         (state (make-exported-logical-snapshot
                 :slot-name "snapshot_race" :token token :database-oid 7
                 :snapshot source-snapshot :registration source-registration))
         (import-backend
           (make-backend :database "postgres" :state :in-progress :explicit t
                         :isolation-level :repeatable-read :read-only t))
         (gate-lock (sb-thread:make-mutex :name "logical snapshot import test"))
         (gate-cv (sb-thread:make-waitqueue :name "logical snapshot import test"))
         (lookup-seen nil)
         (finalizer-started nil)
         (finalizer-done nil)
         (allow-import nil)
         (import-error nil)
         (finalizer-error nil)
         (importer nil)
         (finalizer nil)
         (baseline-snapshots *active-snapshots*))
    (setf (gethash "postgres" (catalog-databases catalog)) database)
    (sb-thread:with-mutex (*xid-lock*)
      (register-active-snapshot source-snapshot source-registration))
    (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
      (setf (gethash token *exported-logical-snapshots*) state))
    (unwind-protect
         (progn
           (setf importer
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*catalog* catalog)
                          (*backend* import-backend)
                          (*logical-test-after-snapshot-token-lookup-hook*
                            (lambda (ignored-state)
                              (declare (ignore ignored-state))
                              (sb-thread:with-mutex (gate-lock)
                                (setf lookup-seen t)
                                (sb-thread:condition-broadcast gate-cv)
                                (loop until allow-import
                                      do (sb-thread:condition-wait
                                          gate-cv gate-lock))))))
                      (handler-case
                          (import-exported-logical-snapshot token)
                        (error (condition)
                          (setf import-error condition)))))))
           (sb-thread:with-mutex (gate-lock)
             (loop until lookup-seen
                   do (sb-thread:condition-wait gate-cv gate-lock)))
           (setf finalizer
                 (sb-thread:make-thread
                  (lambda ()
                    (sb-thread:with-mutex (gate-lock)
                      (setf finalizer-started t)
                      (sb-thread:condition-broadcast gate-cv))
                    (handler-case
                        (remove-exported-logical-snapshot-token token)
                      (error (condition)
                        (setf finalizer-error condition)))
                    (sb-thread:with-mutex (gate-lock)
                      (setf finalizer-done t)
                      (sb-thread:condition-broadcast gate-cv)))))
           (sb-thread:with-mutex (gate-lock)
             (loop until finalizer-started
                   do (sb-thread:condition-wait gate-cv gate-lock)))
           (sleep 0.02)
           (sb-thread:with-mutex (gate-lock)
             (check (not finalizer-done)
                    "snapshot finalizer waits while import publishes its pin")
             (setf allow-import t)
             (sb-thread:condition-broadcast gate-cv))
           (sb-thread:join-thread importer)
           (sb-thread:join-thread finalizer)
           (check (null import-error) "snapshot import completes during finalization")
           (check (null finalizer-error) "snapshot finalization completes after import")
           (check (null (gethash token *exported-logical-snapshots*))
                  "copy completion removes the exact imported snapshot token")
           (check (snapshot-registration-active-p
                   (backend-transaction-snapshot-registration import-backend))
                  "imported transaction retains an independent snapshot pin")
           (check (= *active-snapshots* (1+ baseline-snapshots))
                  "finalization releases only the exported snapshot registration"))
      (sb-thread:with-mutex (gate-lock)
        (setf allow-import t)
        (sb-thread:condition-broadcast gate-cv))
      (when (and importer (sb-thread:thread-alive-p importer))
        (sb-thread:join-thread importer))
      (when (and finalizer (sb-thread:thread-alive-p finalizer))
        (sb-thread:join-thread finalizer))
      (when (gethash token *exported-logical-snapshots*)
        (ignore-errors (remove-exported-logical-snapshot-token token)))
      (when (snapshot-registration-active-p source-registration)
        (release-snapshot-registration source-registration))
      (let ((import-registration
              (backend-transaction-snapshot-registration import-backend)))
        (when (and import-registration
                   (snapshot-registration-active-p import-registration))
          (release-snapshot-registration import-registration))))))

(defun test-wal-pgslot3-database-field-validation ()
  (let* ((test-directory
           (merge-pathnames (format nil "logical-slot-file-~a/" (random 100000000))
                            *pgdata*))
         (valid-path (merge-pathnames "valid_slot.slot" test-directory))
         (invalid-path (merge-pathnames "invalid_slot.slot" test-directory))
         (unbound (make-wal-replication-slot
                   :name "valid_slot" :restart-lsn 0 :consistent-lsn 0
                   :wal-status :reserved))
         (invalid-prefix
           (format nil "PGSLOT3 invalid_slot not-an-oid 0 0 - reserved")))
    (wal-ensure-directory-durable test-directory)
    (unwind-protect
         (progn
           (with-open-file (stream valid-path :direction :output
                                              :element-type '(unsigned-byte 8)
                                              :if-exists :supersede
                                              :if-does-not-exist :create)
             (write-sequence (wal-encode-replication-slot unbound) stream))
           (check (null (wal-replication-slot-database-oid
                         (wal-read-replication-slot valid-path)))
                  "PGSLOT3 preserves a valid legacy database-unbound slot")
           (with-open-file (stream invalid-path :direction :output
                                                :element-type '(unsigned-byte 8)
                                                :if-exists :supersede
                                                :if-does-not-exist :create)
             (write-sequence
              (string->bytes
               (format nil "~a ~8,'0X~%" invalid-prefix
                       (wal-crc32 (string->bytes invalid-prefix))))
              stream))
           (check (handler-case
                      (progn (wal-read-replication-slot invalid-path) nil)
                    (error () t))
                  "PGSLOT3 rejects malformed database OIDs with a valid CRC"))
      (ignore-errors
       (uiop:delete-directory-tree test-directory :validate t
                                                  :if-does-not-exist :ignore)))))
