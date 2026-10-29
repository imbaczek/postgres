;;;; logical-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-logical-row-change-codec
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
          (rove:ok (and (eq (logical-change-kind decoded)
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
        (rove:ok (handler-case
                     (progn (logical-decode-change with-trailing-byte) nil)
                   (error () t))
                 "logical row payload rejects trailing bytes"))
      (let* ((long-name (make-string 70000 :initial-element #\r))
             (event (logical-test-change :insert 7 701
                                         :new-values #(11 12 "long name"))))
        (setf (logical-change-relation-name event) long-name)
        (rove:ok (string= long-name
                          (logical-change-relation-name
                           (logical-decode-change (logical-encode-change event))))
                 "logical schema names use lengths large enough for valid identifiers"))))

(rove:deftest test-logical-copy-target-query-frame-boundary
    (let* ((within (make-string (- +max-query-message-size+ 5) :initial-element #\a))
           (over (concatenate 'string within "a")))
      (rove:ok (eq within (logical-copy-check-query-size within)))
      (rove:ok (handler-case (progn (logical-copy-check-query-size over) nil)
                 (error () t)))
      (setf (char within 0) #\ż)
      (rove:ok (handler-case (progn (logical-copy-check-query-size within) nil)
                 (error () t)))))

(rove:deftest test-logical-copy-frame-counts-sql-escaping
    (let* ((column (make-pg-client-column :name "payload" :type-oid 25))
           (row (vector (make-string (floor +max-query-message-size+ 2) :initial-element #\')))
           (query (logical-copy-insert-query "t" (list column) row)))
      (rove:ok (handler-case (progn (logical-copy-check-query-size query) nil)
                 (error () t))
               "doubling quotes cannot bypass the actual target-frame limit")))

(rove:deftest test-logical-copy-oversize-query-rolls-back
    (let ((original (symbol-function 'logical-target-query))
          (queries nil)
          (column (make-pg-client-column :name "payload" :type-oid 25)))
      (unwind-protect
           (progn
             (setf (symbol-function 'logical-target-query)
                   (lambda (target query)
                     (declare (ignore target))
                     (push query queries)))
             (rove:ok
              (handler-case
                  (progn
                    (logical-copy-target-chunk
                     :target "t" (list column)
                     (list (vector (make-string (floor +max-query-message-size+ 2)
                                                :initial-element #\'))))
                    nil)
                (error () t)))
             (rove:ok (equal (nreverse queries) '("BEGIN" "ROLLBACK"))
                      "frame rejection rolls back before sending the oversized INSERT"))
        (setf (symbol-function 'logical-target-query) original))))

(rove:deftest test-logical-text-row-change
    (let* ((value (format nil "~a~a" "Zażółć " (make-string 3000 :initial-element #\x)))
           (event (logical-test-change :insert 7 701 :new-values (vector 10 120 value))))
      (setf (getf (third (logical-change-columns event)) :type) :text
            (getf (third (logical-change-columns event)) :typmod) -1)
      (let ((decoded (logical-decode-change (logical-encode-change event))))
        (rove:ok (eq :text (getf (third (logical-change-columns decoded)) :type))
                 "logical WAL retains the distinct text type")
        (rove:ok (equal value (aref (logical-change-new-values decoded) 2))
                 "logical WAL retains long UTF-8 text"))
      (rove:ok (= 25 (pgoutput-type-oid :text)) "pgoutput advertises text OID 25")
      (rove:ok (logical-type-supported-p 25) "subscriber accepts text OID 25")))

(rove:deftest test-logical-bytea-row-change
    (let* ((value (make-array 256 :element-type '(unsigned-byte 8)
                                  :initial-contents (loop for i below 256 collect i)))
           (event (logical-test-change :insert 7 701 :new-values (vector 10 120 value))))
      (setf (getf (third (logical-change-columns event)) :type) :bytea
            (getf (third (logical-change-columns event)) :typmod) -1)
      (let ((decoded (logical-decode-change (logical-encode-change event))))
        (rove:ok (eq :bytea (getf (third (logical-change-columns decoded)) :type))
                 "logical WAL preserves bytea type")
        (rove:ok (equalp value (aref (logical-change-new-values decoded) 2))
                 "logical WAL preserves arbitrary octets"))
      (rove:ok (= 17 (pgoutput-type-oid :bytea)) "pgoutput advertises bytea OID 17")
      (rove:ok (logical-type-supported-p 17) "subscriber accepts bytea OID 17")
      (rove:ok (equal (sql-bytea-output value) (pgoutput-value-text value :bytea))
               "pgoutput uses text-format bytea output")))

(rove:deftest test-logical-json-row-change
    (let* ((value "{ \"a\": 1, \"a\": [null, true] }")
           (event (logical-test-change :insert 7 701 :new-values (vector 10 120 value))))
      (setf (getf (third (logical-change-columns event)) :type) :json
            (getf (third (logical-change-columns event)) :typmod) -1)
      (let ((decoded (logical-decode-change (logical-encode-change event))))
        (rove:ok (eq :json (getf (third (logical-change-columns decoded)) :type))
                 "logical WAL preserves JSON type")
        (rove:ok (string= value (aref (logical-change-new-values decoded) 2))
                 "logical WAL preserves JSON source text"))
      (rove:ok (= 114 (pgoutput-type-oid :json)) "pgoutput advertises JSON OID 114")
      (rove:ok (logical-type-supported-p 114) "subscriber accepts JSON OID 114")))

(rove:deftest test-logical-jsonb-row-change
    (let* ((value (sql-jsonb-input "{\"b\":[null,1.00], \"a\":true}"))
           (event (logical-test-change :insert 7 701 :new-values (vector 10 120 value))))
      (setf (getf (third (logical-change-columns event)) :type) :jsonb
            (getf (third (logical-change-columns event)) :typmod) -1)
      (let* ((decoded (logical-decode-change (logical-encode-change event)))
             (restored (aref (logical-change-new-values decoded) 2)))
        (rove:ok (eq :jsonb (getf (third (logical-change-columns decoded)) :type))
                 "logical WAL preserves JSONB type")
        (rove:ok (zerop (sql-jsonb-compare value restored)) "logical WAL preserves JSONB value"))
      (rove:ok (= 3802 (pgoutput-type-oid :jsonb)) "pgoutput advertises JSONB OID 3802")
      (rove:ok (logical-type-supported-p 3802) "subscriber accepts JSONB OID 3802")))

(rove:deftest test-logical-pgoutput-v2-codec
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
      (rove:ok (and (= (aref relation 0) (char-code #\R))
                    (= (bytes-to-u32 relation 1) 701)
                    (= (aref insert-payload 0) (char-code #\I))
                    (= (bytes-to-u32 insert-payload 1) 701))
               "pgoutput Relation and INSERT carry the source relation OID")
      (multiple-value-bind (tuple end)
          (logical-pgoutput-read-tuple insert-payload 6)
        (rove:ok (and (= (aref insert-payload 5) (char-code #\N))
                      (= end (length insert-payload))
                      (equalp tuple #("10" "20" "oak")))
                 "pgoutput INSERT encodes a complete text-format tuple"))
      (multiple-value-bind (key end)
          (logical-pgoutput-read-tuple update-payload 6)
        (rove:ok (and (= (aref update-payload 5) (char-code #\K))
                      (= (aref update-payload end) (char-code #\N))
                      (equalp key #("10" nil nil)))
                 "pgoutput UPDATE retains relation width in its old key tuple"))
      (multiple-value-bind (key end)
          (logical-pgoutput-read-tuple delete-payload 6)
        (rove:ok (and (= (aref delete-payload 5) (char-code #\K))
                      (= end (length delete-payload))
                      (equalp key #("10" nil nil)))
                 "pgoutput DELETE retains relation width in its old key tuple"))
      (rove:ok (string= (logical-row-key-predicate
                         #(nil) (list (make-pgoutput-column :name "id" :type-oid 23)))
                        "id IS NULL")
               "nullable NULLS NOT DISTINCT replica keys generate NULL-safe predicates")
      (rove:ok (and (= (aref begin 0) (char-code #\B))
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
        (rove:ok (and (= (aref standalone 0) (char-code #\M))
                      (= (aref standalone 1) 0)
                      (= (pg-client-u64 standalone 2) lsn))
                 "pgoutput v2 emits standalone DDL Message without an XID")
        (rove:ok (and (= (aref transactional 0) (char-code #\M))
                      (= (aref transactional 1) 1)
                      (= (pg-client-u64 transactional 2) lsn))
                 "ordinary transactional Message has flags and LSN without an XID")
        (multiple-value-bind (prefix event-lsn content)
            (logical-pgoutput-read-message
             (make-logical-subscription-runtime) standalone)
          (rove:ok (and (string= prefix "pg_lisp/ddl") (= event-lsn lsn)
                        (string= content (logical-change-event-payload catalog-change)))
                   "subscriber decodes standalone v2 Message fields"))
        (multiple-value-bind (prefix event-lsn content)
            (logical-pgoutput-read-message
             (make-logical-subscription-runtime
              :transaction-open-p t :source-xid 91)
             transactional)
          (rove:ok (and (string= prefix "pg_lisp/truncate") (= event-lsn lsn)
                        (string= content (logical-change-event-payload truncate)))
                   "subscriber validates ordinary transactional Message flags and content")))
      (rove:ok (eq :create
                   (logical-replication-command-type
                    (parse-replication-command
                     "CREATE_REPLICATION_SLOT unit_slot LOGICAL pgoutput EXPORT_SNAPSHOT")))
               "logical slot command parser accepts the supported pgoutput syntax")
      (rove:ok (eq :start
                   (logical-replication-command-type
                    (parse-replication-command
                     "START_REPLICATION SLOT unit_slot LOGICAL 0/0 (proto_version '2')")))
               "logical slot command parser accepts the worker's pgoutput v2 start command")
      (rove:ok (handler-case
                   (progn
                     (parse-replication-command
                      "START_REPLICATION SLOT unit_slot LOGICAL 0/0 (proto_version '1')")
                     nil)
                 (error () t))
               "logical slot command parser rejects pgoutput v1 for v2 Message support")))

(rove:deftest test-logical-catalog-event-codec-and-parser
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
      (rove:ok (and (eq (logical-change-kind change) :ddl)
                    (= (logical-change-database-oid change) 7)
                    (= (getf envelope :database-oid) 7)
                    (equal (getf envelope :ast) ast))
               "catalog event preserves its source database and supported DDL AST")
      (rove:ok (and (eq (getf apply-ast :type) :apply-logical-event)
                    (string= (getf apply-ast :event-key) "source:7:slot")
                    (= (getf apply-ast :event-lsn) 12345)
                    (string= (getf apply-ast :payload)
                             (catalog-readable-data-string envelope)))
               "private APPLY LOGICAL EVENT SQL parses the idempotent event envelope")
      (rove:ok (handler-case
                   (progn (logical-read-data-string "(:type :ddl) (:type :drop-table)") nil)
                 (error () t))
               "logical event parser rejects trailing S-expression payloads")
      (rove:ok (handler-case
                   (progn (logical-read-data-string "#.(error \"unsafe\")") nil)
                 (error () t))
               "logical event parser disables reader evaluation")))

(rove:deftest test-logical-bootstrap-private-index-projection
    (let* ((progress-name "pg_lisp_subscription_progress")
           (config-name "pg_lisp_subscription_config")
           (ordinary-name "logical_guard_rows")
           (progress-source-index "publisher_progress_source_lsn_idx")
           (config-source-index "publisher_config_source_port_idx")
           (ordinary-index "logical_guard_rows_label_idx")
           (ordinary-lookalike-index
             "pg_lisp_subscription_progress_label_idx")
           (source
             (list :database-oid 7
                   :tables
                   (list (list :name progress-name
                               :sql "CREATE TABLE pg_lisp_subscription_progress (source_lsn varchar)")
                         (list :name config-name
                               :sql "CREATE TABLE pg_lisp_subscription_config (source_port integer)")
                         (list :name ordinary-name
                               :sql "CREATE TABLE logical_guard_rows (label varchar)"))
                   :indexes
                   (list (list :table-name progress-name
                               :name progress-source-index
                               :sql "CREATE INDEX publisher_progress_source_lsn_idx ON pg_lisp_subscription_progress (source_lsn)")
                         (list :table-name config-name
                               :name config-source-index
                               :sql "CREATE INDEX publisher_config_source_port_idx ON pg_lisp_subscription_config (source_port)")
                         (list :table-name ordinary-name
                               :name ordinary-index
                               :sql "CREATE INDEX logical_guard_rows_label_idx ON logical_guard_rows (label)")
                         (list :table-name ordinary-name
                               :name ordinary-lookalike-index
                               :sql "CREATE INDEX pg_lisp_subscription_progress_label_idx ON logical_guard_rows (label)"))
                   :sequences nil))
           (target
             (list :database-oid 7
                   :tables
                   (list (list :name progress-name
                               :sql "CREATE TABLE pg_lisp_subscription_progress (source_slot varchar PRIMARY KEY, source_lsn varchar, state varchar NOT NULL, snapshot_token varchar)")
                         (list :name config-name
                               :sql "CREATE TABLE pg_lisp_subscription_config (source_slot varchar PRIMARY KEY, source_host varchar NOT NULL, source_port integer NOT NULL, source_database varchar NOT NULL, table_allowlist varchar NOT NULL)")
                         (list :name ordinary-name
                               :sql "CREATE TABLE logical_guard_rows (label varchar)"))
                   :indexes
                   (list (list :table-name progress-name
                               :name "pg_lisp_subscription_progress_pkey"
                               :sql "CREATE UNIQUE INDEX pg_lisp_subscription_progress_pkey ON pg_lisp_subscription_progress (source_slot)")
                         (list :table-name config-name
                               :name "pg_lisp_subscription_config_pkey"
                               :sql "CREATE UNIQUE INDEX pg_lisp_subscription_config_pkey ON pg_lisp_subscription_config (source_slot)")
                         (list :table-name ordinary-name
                               :name ordinary-index
                               :sql "CREATE INDEX logical_guard_rows_label_idx ON logical_guard_rows (label)")
                         (list :table-name ordinary-name
                               :name ordinary-lookalike-index
                               :sql "CREATE INDEX pg_lisp_subscription_progress_label_idx ON logical_guard_rows (label)"))
                   :sequences nil))
           (source-view (logical-replication-bootstrap-view source))
           (target-view (logical-replication-bootstrap-view target))
           (source-contract (logical-bootstrap-schema-contract source-view))
           (target-contract (logical-bootstrap-schema-contract target-view)))
      (rove:ok (equal (mapcar (lambda (entry) (getf entry :name))
                              (getf source-view :tables))
                      (list ordinary-name))
               "bootstrap projection omits both subscriber-private tables")
      (rove:ok (equal (mapcar (lambda (entry) (getf entry :name))
                              (getf source-view :indexes))
                      (list ordinary-index ordinary-lookalike-index))
               "bootstrap projection filters private indexes by owning table")
      (rove:ok (and (equal source-contract target-contract)
                    (logical-schema-contract-subset-compatible-p
                     source-contract target-contract))
               "private table and index differences are outside the schema contract")
      (let ((private-source (copy-tree source))
            (private-target (copy-tree target)))
        (setf (getf private-source :tables)
              (subseq (getf private-source :tables) 0 2)
              (getf private-source :indexes)
              (subseq (getf private-source :indexes) 0 2)
              (getf private-target :tables)
              (subseq (getf private-target :tables) 0 2)
              (getf private-target :indexes)
              (subseq (getf private-target :indexes) 0 2))
        (rove:ok (handler-case
                     (progn
                       (logical-validate-existing-replication-bootstrap
                        nil nil
                        (logical-replication-bootstrap-view private-source)
                        (logical-replication-bootstrap-view private-target)
                        (make-logical-subscription-options))
                       t)
                   (error () nil))
                 "private table schema differences bypass copy-schema validation"))
      (dolist (malformed-index
               (list '(:name "missing_owner_idx" :sql "CREATE INDEX missing_owner_idx")
                     '(:table-name 7 :name "non_string_owner_idx"
                       :sql "CREATE INDEX non_string_owner_idx")
                     '(:table-name "missing_table" :name "unknown_owner_idx"
                       :sql "CREATE INDEX unknown_owner_idx")
                     '(:table-name "logical_guard_rows" :name "duplicate_owner_idx"
                       :sql "CREATE INDEX duplicate_owner_idx"
                       :table-name "pg_lisp_subscription_progress")))
        (let ((malformed (copy-tree source)))
          (setf (getf malformed :indexes) (list malformed-index))
          (rove:ok (handler-case
                       (progn (logical-replication-bootstrap-view malformed) nil)
                     (error () t))
                   "malformed or unknown bootstrap index owners are rejected")))))

(rove:deftest test-logical-private-metadata-ddl-targets
    (let* ((database (make-database :name "postgres" :oid 7))
           (progress-name (first +logical-private-schema-table-names+))
           (config-name (second +logical-private-schema-table-names+))
           (ordinary-name "logical_guard_rows")
           (progress-index-name "pg_lisp_subscription_progress_pkey")
           (config-index-name "pg_lisp_subscription_config_pkey")
           (ordinary-index-name "logical_guard_rows_pkey")
           (qualified-progress-name (concatenate 'string "public." progress-name))
           (qualified-config-name (concatenate 'string "public." config-name))
           (qualified-progress-index-name
             (concatenate 'string "public." progress-index-name))
           (progress-table
             (make-pg-class :oid 701 :name progress-name :relfilenode 1701))
           (config-table
             (make-pg-class :oid 702 :name config-name :relfilenode 1702))
           (ordinary-table
             (make-pg-class :oid 703 :name ordinary-name :relfilenode 1703))
           (cases nil))
      (setf (gethash progress-name (database-tables database)) progress-table
            (gethash config-name (database-tables database)) config-table
            (gethash ordinary-name (database-tables database)) ordinary-table
            (gethash progress-index-name (database-indexes database))
            (make-pg-index :oid 801 :name progress-index-name
                           :relfilenode 1801 :table-oid 701)
            (gethash config-index-name (database-indexes database))
            (make-pg-index :oid 802 :name config-index-name
                           :relfilenode 1802 :table-oid 702)
            (gethash ordinary-index-name (database-indexes database))
            (make-pg-index :oid 803 :name ordinary-index-name
                           :relfilenode 1803 :table-oid 703))
      (setf cases
            (list
             (list (list :type :create-table :name progress-name :if-not-exists t)
                   progress-name)
             ;; The parser has already stripped one public prefix here; the
             ;; executor's lookup-table strips another before resolving the name.
             (list (list :type :create-table :name qualified-progress-name
                         :if-not-exists t)
                   progress-name)
             (list (list :type :drop-table
                         :names (list ordinary-name config-name) :if-exists t)
                   config-name)
             (list (list :type :drop-table
                         :names (list qualified-progress-name) :if-exists t)
                   nil)
             (list (list :type :alter-table :table progress-name :actions nil)
                   progress-name)
             (list (list :type :alter-table :table qualified-progress-name
                         :actions nil)
                   progress-name)
             (list (list :type :create-index :table config-name
                         :name "config_extra_idx")
                   config-name)
             (list (list :type :create-index :table qualified-config-name
                         :name "qualified_config_idx")
                   config-name)
             (list (list :type :create-index :table ordinary-name
                         :name progress-index-name)
                   progress-name)
             (list (list :type :create-index :table ordinary-name
                         :name qualified-progress-index-name)
                   progress-name)
             (list (list :type :drop-index
                         :names (list ordinary-index-name config-index-name))
                   config-name)
             (list (list :type :drop-index
                         :names (list qualified-progress-index-name))
                   nil)
             (list (list :type :alter-index :name progress-index-name
                         :new-name "renamed_progress_idx")
                   progress-name)
             (list (list :type :alter-index :name qualified-progress-index-name
                         :new-name "renamed_progress_idx")
                   progress-name)
             (list (list :type :create-table :name ordinary-name) nil)
             (list (list :type :drop-table :names (list ordinary-name)) nil)
             (list (list :type :alter-table :table ordinary-name :actions nil) nil)
             (list (list :type :create-index :table ordinary-name
                         :name "ordinary_extra_idx")
                   nil)
             (list (list :type :drop-index :names (list ordinary-index-name)) nil)
             (list (list :type :alter-sequence :name "ordinary_seq" :increment 2)
                   nil)))
      (dolist (case cases)
        (rove:ok (equal (logical-private-metadata-ddl-target-name
                         database (first case))
                        (second case))
                 "replicated DDL guard classifies protected table and index targets"))))

(rove:deftest test-logical-scan-budget-override
    (rove:ok (= (logical-scan-budget-limit nil nil nil)
                +max-logical-read-scan-bytes+)
             "logical reader uses the production scan budget by default")
  (rove:ok (= (logical-scan-budget-limit
               "1" "after-scan-progress-before-feedback" "crash")
              1)
           "safe-progress crash tests can narrow the scan budget")
  (dolist (value '("0" "not-a-number"))
    (rove:ok (handler-case
                 (progn
                   (logical-scan-budget-limit
                    value "after-scan-progress-before-feedback" "crash")
                   nil)
               (error () t))
             "logical scan budget rejects invalid values"))
  (rove:ok (handler-case
               (progn
                 (logical-scan-budget-limit
                  (write-to-string (1+ +max-logical-read-scan-bytes+))
                  "after-scan-progress-before-feedback" "crash")
                 nil)
             (error () t))
           "logical scan budget cannot exceed the production limit")
  (dolist (gate '(("after-target-commit-before-feedback" "crash")
                  ("after-scan-progress-before-feedback" "error")))
    (rove:ok (handler-case
                 (progn (apply #'logical-scan-budget-limit "1" gate) nil)
               (error () t))
             "logical scan budget requires its dedicated crash failpoint")))

(rove:deftest test-create-sequence-parser-bounds
    (let* ((explicit
             (first (parse-statements
                     "CREATE SEQUENCE bounded_counter INCREMENT BY -2 MINVALUE -90 MAXVALUE 5 START WITH 4")))
           (default-descending
             (first (parse-statements
                     "CREATE SEQUENCE descending_counter INCREMENT BY -2"))))
      (rove:ok (and (= (getf explicit :increment) -2)
                    (= (getf explicit :min) -90)
                    (= (getf explicit :max) 5)
                    (= (getf explicit :start) 4))
               "CREATE SEQUENCE parses explicit int8 minimum and maximum bounds")
      (rove:ok (and (= (getf default-descending :min) +int8-min+)
                    (= (getf default-descending :max) -1)
                    (= (getf default-descending :start) -1))
               "descending sequence defaults follow sequence.c init_params")
      (rove:ok (handler-case
                   (progn
                     (parse-statements
                      "CREATE SEQUENCE invalid_counter MINVALUE 20 MAXVALUE 10")
                     nil)
                 (error () t))
               "CREATE SEQUENCE rejects bounds that do not contain its start")))

(rove:deftest test-logical-subscription-argument-parser
    (let ((options
            (parse-logical-subscription-args
             '("--slot" "test_slot" "--source-port" "5434"
               "--target-port" "5435" "--table" "first_rows"
               "--table" "second_rows"))))
      (rove:ok (and (= (logical-subscription-options-source-port options) 5434)
                    (= (logical-subscription-options-target-port options) 5435)
                    (equal (logical-subscription-options-tables options)
                           '("first_rows" "second_rows")))
               "subscriber CLI consumes option values and retains repeated tables")
      (rove:ok (handler-case
                   (progn
                     (parse-logical-subscription-args
                      '("--slot" "test_slot" "--table"))
                     nil)
                 (error () t))
               "subscriber CLI rejects an option with no value")
      (dolist (table +logical-private-schema-table-names+)
        (rove:ok (handler-case
                     (progn
                       (parse-logical-subscription-args
                        (list "--slot" "test_slot" "--table" table))
                       nil)
                   (error () t))
                 "subscriber CLI rejects private metadata tables in --table"))))

(rove:deftest test-logical-subscription-resume-config
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
      (rove:ok (string= (fourth (logical-subscription-config-values options))
                        "a_rows,z_rows")
               "subscriber config canonicalizes allowlist ordering")
      (rove:ok (null (logical-validate-resume-config options '(:state "ready") config))
               "matching source and allowlist permit a safe resume")
      (rove:ok (handler-case
                   (progn
                     (logical-validate-resume-config
                      (make-logical-subscription-options
                       :source-host "other.example" :source-port 5440
                       :source-database "publisher_db" :tables '("z_rows" "a_rows"))
                      '(:state "ready") config)
                     nil)
                 (error () t))
               "subscriber config rejects changing the source endpoint")
      (rove:ok (handler-case
                   (progn
                     (logical-validate-resume-config
                      (make-logical-subscription-options
                       :source-host "publisher.example" :source-port 5440
                       :source-database "publisher_db" :tables '("a_rows"))
                      '(:state "ready") config)
                     nil)
                 (error () t))
               "subscriber config rejects changing the table allowlist")
      (rove:ok (handler-case
                   (progn
                     (logical-validate-resume-config options '(:state "ready") nil)
                     nil)
                 (error () t))
               "legacy progress without a saved config cannot resume unsafely")
      (rove:ok (handler-case
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

(rove:deftest test-logical-fragment-assembly
    (let* ((first (make-array 10 :element-type '(unsigned-byte 8)
                                 :initial-contents '(0 1 2 3 4 5 6 7 8 9)))
           (second (make-array 2 :element-type '(unsigned-byte 8)
                                 :initial-contents '(10 11)))
           (plan (logical-plan-event-fragments (list first second) 4 12))
           (state (make-logical-fragment-assembler 12))
           (complete '()))
      (rove:ok (= (length plan) 4))
      (dolist (fragment plan)
        (let* ((event (logical-fragment-event fragment))
               (start (logical-fragment-start fragment))
               (end (logical-fragment-end fragment))
               (result (logical-assemble-event-fragment state (length event) start
                                                        (subseq event start end))))
          (when result (push result complete))))
      (logical-finish-fragment-assembly state)
      (rove:ok (equalp (nreverse complete) (list first second)))
      (rove:ok (= (logical-fragment-assembler-bytes state) 12))
      (rove:ok (handler-case (progn (logical-plan-event-fragments (list first second) 4 11) nil)
                 (error () t))))
  (flet ((bad (total offset octets)
           (handler-case
               (progn (logical-assemble-event-fragment
                       (make-logical-fragment-assembler 10) total offset
                       (coerce octets '(vector (unsigned-byte 8))))
                      nil)
             (error () t))))
    (rove:ok (bad 4 1 #(1)))
    (rove:ok (bad 11 0 #(1)))
    (rove:ok (bad 2 0 #()))
    (rove:ok (bad 2 1 #(1 2))))
  (let ((state (make-logical-fragment-assembler 10)))
    (logical-assemble-event-fragment state 4 0 (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(1 2)))
    (rove:ok (handler-case (progn (logical-assemble-event-fragment state 4 3 (make-array 1 :element-type '(unsigned-byte 8) :initial-element 3)) nil) (error () t)))
    (rove:ok (handler-case (progn (logical-assemble-event-fragment state 5 2 (make-array 1 :element-type '(unsigned-byte 8) :initial-element 3)) nil) (error () t)))
    (rove:ok (handler-case (progn (logical-finish-fragment-assembly state) nil) (error () t)))))

(rove:deftest test-logical-fragment-limits-and-failure-state
    (let* ((bytes (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(7 8)))
           (state (make-logical-fragment-assembler 3)))
      (rove:ok (equalp (logical-assemble-event-fragment state 2 0 bytes) bytes))
      (rove:ok (= (logical-fragment-assembler-bytes state) 2))
      (rove:ok (handler-case
                   (progn (logical-assemble-event-fragment state 2 0 bytes) nil)
                 (error () t)))
      (rove:ok (= (logical-fragment-assembler-bytes state) 2))
      (rove:ok (null (logical-fragment-assembler-buffer state)))
      (rove:ok (handler-case
                   (progn (logical-assemble-event-fragment state 1 0 #(999)) nil)
                 (error () t)))
      (rove:ok (= (logical-fragment-assembler-bytes state) 2)))
  (dolist (limits '((0 10) (-1 10) (1 0) (1 -1)))
    (rove:ok (handler-case
                 (progn (logical-plan-event-fragments '() (first limits) (second limits)) nil)
               (error () t)))))

(rove:deftest test-logical-fragment-payload-roundtrip
    (let* ((event (make-array 9 :element-type '(unsigned-byte 8)
                                :initial-contents '(0 1 2 3 4 5 6 7 8)))
           (plan (logical-plan-event-fragments (list event) 4 9))
           (state (make-logical-fragment-assembler 9))
           (completed nil))
      (loop for fragment in plan for index from 0
            for payload = (logical-encode-fragment index (length plan) fragment)
            do (multiple-value-bind (actual-index count total offset bytes)
                   (logical-decode-fragment payload)
                 (rove:ok (= actual-index index))
                 (rove:ok (= count (length plan)))
                 (setf completed (or (logical-assemble-event-fragment state total offset bytes)
                                     completed))))
      (logical-finish-fragment-assembly state)
      (rove:ok (equalp event completed))
      (let* ((payload (logical-encode-fragment 0 3 (first plan)))
             (trailing (concatenate '(vector (unsigned-byte 8)) payload #(0))))
        (rove:ok (handler-case (progn (logical-decode-fragment trailing) nil)
                   (error () t)))
        (setf (aref payload 0) 0)
        (rove:ok (handler-case (progn (logical-decode-fragment payload) nil)
                   (error () t))))))

(rove:deftest test-logical-fragment-rejects-truncated-payloads
    (let* ((event (make-array 5 :element-type '(unsigned-byte 8) :initial-element 42))
           (fragment (first (logical-plan-event-fragments (list event) 4 5)))
           (payload (logical-encode-fragment 0 2 fragment)))
      (loop for length below (length payload)
            do (rove:ok (handler-case
                            (progn (logical-decode-fragment (subseq payload 0 length)) nil)
                          (error () t)))))
  (let* ((event (make-array 5 :element-type '(unsigned-byte 8) :initial-element 42))
         (fragment (first (logical-plan-event-fragments (list event) 4 5))))
    (dolist (field-offset '(8 12 20))
      (let ((payload (logical-encode-fragment 0 2 fragment)))
        (fill payload 0 :start field-offset :end (+ field-offset 4))
        (rove:ok (handler-case (progn (logical-decode-fragment payload) nil)
                   (error () t)))))))

(rove:deftest test-logical-fragment-batch-validation
    (let* ((event (logical-encode-change
                   (logical-test-change :insert 7 701 :new-values #(1 2 "fragment"))))
           (plan (logical-plan-event-fragments (list event) 9 10000))
           (payloads (loop for fragment in plan for index from 0
                           collect (logical-encode-fragment index (length plan) fragment)))
           (batch (make-logical-fragment-batch 10000)))
      (rove:ok (handler-case
                   (progn (logical-fragment-batch-accept batch 5 (second payloads)) nil)
                 (error () t)))
      (logical-fragment-batch-accept batch 5 (first payloads))
      (rove:ok (handler-case (progn (logical-fragment-batch-finish batch 5) nil)
                 (error () t)))
      (rove:ok (handler-case
                   (progn (logical-fragment-batch-accept batch 6 (second payloads)) nil)
                 (error () t)))
      (let ((change nil))
        (dolist (payload (rest payloads))
          (let ((result (logical-fragment-batch-accept batch 5 payload)))
            (when result (setf change result))))
        (rove:ok (eq (logical-change-kind change) :insert))
        (rove:ok (= (logical-change-relation-oid change) 701)))
      (rove:ok (handler-case (progn (logical-fragment-batch-finish batch 6) nil)
                 (error () t)))
      (logical-fragment-batch-finish batch 5)))

(rove:deftest test-logical-fragments-recovery-and-slot-reader
    (let* ((event (logical-encode-change
                   (logical-test-change :insert 7 701 :new-values #(1 2 "fragment"))))
           (plan (logical-plan-event-fragments (list event) 9 10000))
           (records (loop for fragment in plan for index from 0
                          collect (make-wal-record :type +wal-record-logical-changes+ :xid 88
                                                   :payload (logical-encode-fragment index (length plan) fragment))))
           (state (make-logical-reader-state :database-oid 7))
           (*wal-recovery-group-active* t)
           (*wal-recovery-logical-xid* nil)
           (*wal-recovery-logical-chunk-count* nil)
           (*wal-recovery-logical-next-chunk* 0)
           (*wal-recovery-logical-bytes* 0))
      (wal-logical-reader-start-batch state)
      (wal-recovery-accept-logical-change-chunk (first records))
      (wal-logical-reader-add-chunk state (first records))
      (rove:ok (handler-case (progn (wal-recovery-finish-logical-change-batch 88) nil)
                 (error () t)))
      (dolist (record (rest records))
        (wal-recovery-accept-logical-change-chunk record)
        (wal-logical-reader-add-chunk state record))
      (wal-recovery-finish-logical-change-batch 88)
      (rove:ok (= (length (logical-reader-state-changes state)) 1))
      (rove:ok (= (logical-reader-state-change-bytes state) (length event)))
      (logical-fragment-batch-finish (logical-reader-state-fragment-batch state) 88)
      (wal-logical-reader-reset-batch state nil)
      (rove:ok (null (logical-reader-state-fragment-batch state)))))

(rove:deftest test-logical-change-chunk-validation
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
          (rove:ok (and (= xid 88) (= count 2) (= next-index 1)
                        (= bytes change-bytes) (= (length changes) 1))
                   "logical chunk progress records the first complete chunk"))
        (rove:ok (rejects (logical-test-chunk-record 88 1 2) nil nil 0 0)
                 "logical chunks must start at index zero")
        (rove:ok (rejects (logical-test-chunk-record 0 0 2) nil nil 0 0)
                 "logical chunks require a positive transaction ID")
        (rove:ok (rejects (logical-test-chunk-record 89 1 2) 88 2 1 change-bytes)
                 "logical chunks cannot change transaction ID")
        (rove:ok (rejects (logical-test-chunk-record 88 0 2) 88 2 1 change-bytes)
                 "logical chunks must be sequential")
        (rove:ok (rejects (logical-test-chunk-record 88 1 3) 88 2 1 change-bytes)
                 "logical chunk count cannot change within a transaction")
        (rove:ok (rejects (logical-test-chunk-record 88 1 2) 88 2 1
                          +max-logical-wal-transaction-bytes+)
                 "logical chunk progress enforces the 65 MiB transaction cap")
        (let ((state (make-logical-reader-state :database-oid 7 :batch-open-p t
                                                :next-chunk 0))
              (commit (make-wal-record :type +wal-record-commit+ :xid 88 :end 100)))
          (wal-logical-reader-add-chunk state first-chunk)
          (rove:ok (handler-case
                       (progn (wal-logical-reader-finish-batch state commit) nil)
                     (error () t))
                   "logical commit terminals require every declared chunk")))))

(rove:deftest test-logical-copy-chunk-limit-override
    (rove:ok (= (logical-copy-chunk-limit nil nil nil)
                +logical-copy-chunk-bytes+)
             "initial-copy chunks use the production limit by default")
  (rove:ok (= (logical-copy-chunk-limit
               "300" "after-copy-chunk-commit" "crash")
              300)
           "copy-chunk crash tests can use a smaller chunk limit")
  (dolist (arguments '(("0" "after-copy-chunk-commit" "crash")
                       ("not-a-number" "after-copy-chunk-commit" "crash")
                       ("300" "after-target-commit-before-feedback" "crash")
                       ("300" "after-copy-chunk-commit" "error")))
    (rove:ok (handler-case
                 (progn (apply #'logical-copy-chunk-limit arguments) nil)
               (error () t))
             "initial-copy chunk override rejects invalid value or failpoint"))
  (rove:ok (handler-case
               (progn
                 (logical-copy-chunk-limit
                  (write-to-string (1+ +logical-copy-chunk-bytes+))
                  "after-copy-chunk-commit" "crash")
                 nil)
             (error () t))
           "initial-copy chunk override cannot exceed production limit"))

(rove:deftest test-logical-replication-bootstrap-definitions
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
        (rove:ok (and (search "CREATE TABLE IF NOT EXISTS" table-sql)
                      (search "DEFAULT nextval('bootstrap_seq')" table-sql)
                      (search "CONSTRAINT \"bootstrap_rows_pkey\" PRIMARY KEY" table-sql)
                      (not (search "bootstrap_rows_amount_idx" table-sql)))
                 "bootstrap table definition retains defaults and only constraint indexes")
        (rove:ok (and (string= (getf index-entry :name) "bootstrap_rows_amount_idx")
                      (string= (getf index-entry :table-name) "bootstrap_rows")
                      (search "CREATE UNIQUE INDEX IF NOT EXISTS" (getf index-entry :sql))
                      (search "NULLS NOT DISTINCT" (getf index-entry :sql)))
                 "bootstrap emits standalone btree indexes as separate SQL")
        (rove:ok (and (search (format nil "INCREMENT BY -2 MINVALUE ~d MAXVALUE -1 START WITH -1"
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
          (rove:ok (= (getf (getf captured-sequence :state) :last) -9)
                   "bootstrap uses the sequence state captured with the slot boundary")))))

(rove:deftest test-logical-bootstrap-constraint-order-stable
    (let* ((*catalog* (make-catalog))
           (*backend* (make-backend :database "postgres"))
           (database (make-database :name "postgres" :oid 7))
           (table
             (make-pg-class
              :oid 801 :name "orders" :relfilenode 1801
              :columns
              (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                      :attnum 1 :notnull t)
                      (make-pg-column :name "account_id" :type :int4 :typmod -1
                                      :attnum 2 :notnull t))
              :indexes '(901 902)))
           (primary
             (make-pg-index :oid 901 :name "orders_pkey"
                            :relfilenode 1901 :table-oid 801 :columns '(1)
                            :unique-p t :primary-p t :constraint-p t :valid-p t))
           (unique
             (make-pg-index :oid 902 :name "orders_account_key"
                            :relfilenode 1902 :table-oid 801 :columns '(2)
                            :unique-p t :constraint-p t :valid-p t)))
      (setf (gethash "postgres" (catalog-databases *catalog*)) database
            (gethash "orders" (database-tables database)) table
            (gethash "orders_pkey" (database-indexes database)) primary
            (gethash "orders_account_key" (database-indexes database)) unique)
      (let* ((primary-first (catalog-table-definition-sql table))
             (account-position
               (search "CONSTRAINT \"orders_account_key\" UNIQUE" primary-first))
             (primary-position
               (search "CONSTRAINT \"orders_pkey\" PRIMARY KEY" primary-first)))
        (setf (pg-class-indexes table) '(902 901))
        (rove:ok (string= primary-first (catalog-table-definition-sql table))
                 "schema contracts serialize multiple constraints independently of catalog order")
        (rove:ok (and account-position primary-position
                      (< account-position primary-position))
                 "table constraints serialize in stable name order"))))

(rove:deftest test-logical-schema-contract-strictness
    (let* ((*catalog* (make-catalog))
           (*backend* (make-backend :database "postgres"))
           (db (make-database :name "postgres" :oid 7))
           (table
             (make-pg-class
              :oid 701 :name "contract_rows" :relfilenode 1701
              :columns
              (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                      :attnum 1 :notnull t))))
           (sequence
             (make-pg-sequence :oid 702 :name "contract_seq" :start-value 1
                               :increment 1 :min-value 1 :max-value +int8-max+
                               :last-value 4 :called-p t)))
      (setf (gethash "postgres" (catalog-databases *catalog*)) db
            (gethash "contract_rows" (database-tables db)) table
            (gethash "contract_seq" (database-sequences db)) sequence)
      (let* ((before (logical-current-schema-contract))
             (empty '(:tables () :indexes () :sequences ()))
             (event-bytes
               (let ((*logical-source-schema-precondition* before))
                 (logical-catalog-event-for-ast
                  '(:type :alter-table :name "contract_rows" :actions ()))))
             (event (logical-decode-catalog-event event-bytes))
             (envelope
               (logical-validate-ddl-event-envelope
                (logical-read-data-string
                 (logical-change-event-payload event)))))
        (rove:ok (logical-schema-contract-valid-p before)
                 "supported catalog objects produce a valid schema contract")
        (rove:ok (logical-schema-contract-subset-compatible-p before empty)
                 "an empty subscriber schema can be bootstrapped")
        (rove:ok (and (equal (getf envelope :schema-before) before)
                      (equal (getf envelope :schema-after) before)
                      (eql (getf envelope :schema-contract-version) 1))
                 "DDL events carry versioned before and after schema contracts")
        (logical-validate-schema-contract before :before)
        ;; Sequence values are not schema, as nextval() is non-MVCC state.
        (setf (pg-sequence-last-value sequence) 99)
        (rove:ok (equal before (logical-current-schema-contract))
                 "sequence advancement does not invalidate schema compatibility")
        (let ((target-with-extra
                (list :tables (append (copy-tree (getf before :tables))
                                      '((:name "local_only" :sql "CREATE TABLE local_only ()")))
                      :indexes (copy-tree (getf before :indexes))
                      :sequences (copy-tree (getf before :sequences)))))
          (rove:ok (not (logical-schema-contract-subset-compatible-p
                         before target-with-extra))
                   "an extra target table is treated as schema drift"))
        (rove:ok (not (logical-schema-contract-valid-p
                       (append (copy-tree before) '(:unexpected t))))
                 "schema contracts reject unknown or duplicate top-level keys")
        (let ((duplicate-objects (copy-tree before)))
          (setf (getf duplicate-objects :tables)
                (append (copy-tree (getf duplicate-objects :tables))
                        (copy-tree (getf duplicate-objects :tables))))
          (rove:ok (not (logical-schema-contract-valid-p duplicate-objects))
                   "schema contracts reject duplicate object names"))
        (let ((sequence-drift (copy-tree before)))
          (setf (getf (first (getf sequence-drift :sequences)) :definition)
                "CREATE SEQUENCE contract_seq INCREMENT BY 2")
          (rove:ok (not (logical-schema-contract-subset-compatible-p
                         before sequence-drift))
                   "sequence definition drift is rejected"))
        (let ((source-with-index (copy-tree before))
              (target-with-index-drift (copy-tree before)))
          (setf (getf source-with-index :indexes)
                '((:name "contract_idx" :sql "CREATE INDEX contract_idx"))
                (getf target-with-index-drift :indexes)
                '((:name "contract_idx" :sql "CREATE UNIQUE INDEX contract_idx")))
          (rove:ok (not (logical-schema-contract-subset-compatible-p
                         source-with-index target-with-index-drift))
                   "index definition drift is rejected"))
        (let ((malformed-envelope (copy-tree envelope)))
          (setf (getf malformed-envelope :ast)
                '(:type :drop-table :names ("one")
                  :type :drop-sequence :names ("two")))
          (rove:ok (handler-case
                       (progn
                         (logical-validate-ddl-event-envelope malformed-envelope)
                         nil)
                     (error () t))
                   "DDL envelopes reject malformed AST plists"))
        (setf (pg-column-notnull (aref (pg-class-columns table) 0)) nil)
        (rove:ok (not (equal before (logical-current-schema-contract)))
                 "column nullability changes are part of the schema contract")
        (rove:ok (handler-case
                     (progn (logical-validate-schema-contract before :before) nil)
                   (error () t))
                 "strict schema validation rejects target drift"))))

(rove:deftest test-logical-schema-contract-column-shape-drift
    (let* ((*catalog* (make-catalog))
           (*backend* (make-backend :database "postgres"))
           (database (make-database :name "postgres" :oid 7))
           (columns
             (vector (make-pg-column :name "id" :type :int4 :typmod -1
                                     :attnum 1 :notnull t)
                     (make-pg-column :name "label" :type :varchar :typmod 20
                                     :attnum 2 :notnull nil)
                     (make-pg-column :name "amount" :type :int8 :typmod -1
                                     :attnum 3 :notnull nil)))
           (table (make-pg-class :oid 703 :name "contract_rows"
                                 :relfilenode 1703 :columns columns)))
      (setf (gethash "postgres" (catalog-databases *catalog*)) database
            (gethash "contract_rows" (database-tables database)) table)
      (let ((source-contract (logical-current-schema-contract)))
        (setf (pg-column-type (aref columns 1)) :int4)
        (rove:ok (not (logical-schema-contract-subset-compatible-p
                       source-contract (logical-current-schema-contract)))
                 "schema contract rejects column type drift")
        (setf (pg-column-type (aref columns 1)) :varchar
              (pg-column-typmod (aref columns 1)) 36)
        (rove:ok (not (logical-schema-contract-subset-compatible-p
                       source-contract (logical-current-schema-contract)))
                 "schema contract rejects varchar typmod drift")
        (setf (pg-column-typmod (aref columns 1)) 20
              (pg-class-columns table)
              (vector (aref columns 1) (aref columns 0) (aref columns 2)))
        (rove:ok (not (logical-schema-contract-subset-compatible-p
                       source-contract (logical-current-schema-contract)))
                 "schema contract rejects column order drift"))))

(rove:deftest test-logical-capture-application-headroom
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
      (multiple-value-bind (sample-accepted event-bytes)
          (logical-test-capture-row-change catalog table "postgres" 0)
        (rove:ok sample-accepted
                 "a small source change fits the ordinary transaction budget")
        (labels ((check-boundary (application limit label)
                   (dolist (delta '(-1 0 1))
                     (let* ((expected (+ limit delta))
                            (initial (- expected event-bytes)))
                       (multiple-value-bind (accepted total)
                           (logical-test-capture-row-change
                            catalog table application initial)
                         (rove:ok (= (if accepted total (+ initial event-bytes)) expected)
                                  (format nil "~a accounts for encoded event bytes" label))
                         (rove:ok (eq accepted (<= expected limit))
                                  (format nil "~a accepts through its inclusive cap" label)))))))
          (check-boundary "postgres" +max-logical-transaction-bytes+
                          "ordinary source transaction")
          (check-boundary "pg-lisp-subscribe-apply"
                          +max-logical-wal-transaction-bytes+
                          "subscriber apply transaction")))))

(rove:deftest test-logical-truncate-capture-headroom
    (let* ((catalog (make-catalog))
           (database (make-database :name "postgres" :oid 7)))
      (setf (gethash "postgres" (catalog-databases catalog)) database)
      (multiple-value-bind (sample-accepted event-bytes)
          (logical-test-capture-truncate-event catalog 0)
        (rove:ok sample-accepted
                 "a small TRUNCATE utility event fits the source transaction budget")
        (dolist (delta '(-1 0 1))
          (let* ((expected (+ +max-logical-transaction-bytes+ delta))
                 (initial (- expected event-bytes)))
            (multiple-value-bind (accepted total)
                (logical-test-capture-truncate-event catalog initial)
              (rove:ok (= (if accepted total (+ initial event-bytes)) expected)
                       "TRUNCATE capture accounts for encoded utility-event bytes")
              (rove:ok (eq accepted
                           (<= expected +max-logical-transaction-bytes+))
                       "TRUNCATE accepts through the inclusive 64 MiB cap")))))))

(rove:deftest test-logical-catalog-terminal-validation
    (let ((catalog-event (make-logical-change :kind :ddl :database-oid 7))
          (row-change (logical-test-change :insert 7 701 :new-values #(1 2 "a")))
          (commit (make-wal-record :type +wal-record-commit+ :xid 9 :end 100))
          (catalog-terminal
            (make-wal-record :type +wal-record-catalog+ :xid 0 :end 100)))
      (rove:ok (handler-case
                   (progn
                     (wal-logical-reader-finish-batch
                      (make-logical-reader-state :database-oid 7 :batch-open-p t
                                                 :changes (list catalog-event))
                      commit)
                     nil)
                 (error () t))
               "catalog events cannot become visible at a row commit terminal")
      (rove:ok (handler-case
                   (progn
                     (wal-logical-reader-finish-batch
                      (make-logical-reader-state :database-oid 7 :batch-open-p t
                                                 :changes (list row-change))
                      catalog-terminal)
                     nil)
                 (error () t))
               "row changes cannot become visible at a catalog terminal")
      (rove:ok (handler-case
                   (progn
                     (let ((*wal-recovery-logical-catalog-event-p* t)
                           (*wal-recovery-logical-chunk-count* nil))
                       (wal-recovery-validate-logical-terminal :commit 9))
                     nil)
                 (error () t))
               "WAL recovery rejects a catalog event followed by a row commit")
      (rove:ok (handler-case
                   (progn
                     (let ((*wal-recovery-logical-catalog-event-p* nil)
                           (*wal-recovery-logical-chunk-count* 1))
                       (wal-recovery-validate-logical-terminal :catalog))
                     nil)
                 (error () t))
               "WAL recovery rejects row changes followed by a catalog terminal")))

(rove:deftest test-wal-logical-catalog-event-slot-gating
    (let* ((*wal-replication-slots* (make-hash-table :test #'equal))
           (*wal-logical-catalog-event-interests* (make-hash-table :test #'eq))
           (slot (make-wal-replication-slot :name "catalog_event_slot"
                                            :database-oid 7)))
      (sb-thread:with-mutex (*wal-lock*)
        (rove:ok (not (wal-logical-catalog-event-needed-under-lock-p 7))
                 "catalog state before logical slot creation needs no stream event")
        (setf (gethash "catalog_event_slot" *wal-replication-slots*) slot)
        (rove:ok (wal-logical-catalog-event-needed-under-lock-p 7)
                 "catalog changes are recorded for a live database slot")
        (rove:ok (not (wal-logical-catalog-event-needed-under-lock-p 8))
                 "catalog events are scoped to the slot database")
        (setf (wal-replication-slot-wal-status slot) :lost)
        (rove:ok (not (wal-logical-catalog-event-needed-under-lock-p 7))
                 "a lost slot no longer requires catalog stream events"))))

(rove:deftest test-logical-wal-recovery-byte-boundaries
    (let* ((record (logical-test-chunk-record 9902 1 2))
           (change-bytes
             (nth-value 3
                        (wal-decode-logical-change-chunk
                         (wal-record-payload record))))
           (limit +max-logical-wal-transaction-bytes+)
           (expected-xid 9902)
           (expected-count 2)
           (expected-index 1))
      (labels ((accepted-total-p (prior-bytes expected-total)
                 (multiple-value-bind (xid count next-index total)
                     (wal-advance-logical-change-chunk
                      record expected-xid expected-count expected-index prior-bytes)
                   (and (= xid expected-xid)
                        (= count expected-count)
                        (= next-index (1+ expected-index))
                        (= total expected-total))))
               (rejected-p (prior-bytes)
                 (handler-case
                     (progn
                       (wal-advance-logical-change-chunk
                        record expected-xid expected-count expected-index prior-bytes)
                       nil)
                   (error () t))))
        (rove:ok (accepted-total-p (- limit change-bytes 1) (1- limit))
                 "WAL recovery accepts 65 MiB minus one byte")
        (rove:ok (accepted-total-p (- limit change-bytes) limit)
                 "WAL recovery accepts exactly 65 MiB")
        (rove:ok (rejected-p (1+ (- limit change-bytes)))
                 "WAL recovery rejects one byte above 65 MiB"))))

(rove:deftest test-unpinned-snapshot-release-preserves-wal-pin-map
    ;; Snapshot release follows snapmgr.c:AtEOXact_Snapshot for the MVCC
    ;; registration; only a published WAL pin may be removed under *wal-lock*.
    ;; This impossible registration/map mismatch is an adversarial sentinel that
    ;; catches accidental pin-map mutation in the unpinned path.
    (let* ((*active-snapshots* 0)
           (*active-snapshot-set* (make-hash-table :test #'eq))
           (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
           (registration (make-snapshot-registration))
           (snapshot (make-snapshot-struct :xmin 10 :xmax 20 :xip '() :curcid 1))
           (sentinel :test-only-unpublished-pin))
      (unwind-protect
           (progn
             (sb-thread:with-mutex (*xid-lock*)
               (register-active-snapshot snapshot registration))
             (sb-thread:with-mutex (*wal-lock*)
               (setf (gethash registration *wal-snapshot-retention-pins*) sentinel))
             (rove:ok (null (snapshot-registration-wal-retention-lsn registration))
                      "test pin-map sentinel has no published registration LSN")
             (release-snapshot-registration registration)
             (rove:ok (and (zerop *active-snapshots*)
                           (not (snapshot-registration-releasing-p registration)))
                      "unpinned release retires the active snapshot registration")
             (rove:ok (eq sentinel
                          (sb-thread:with-mutex (*wal-lock*)
                            (gethash registration *wal-snapshot-retention-pins*)))
                      "unpinned release does not mutate WAL pin state without *wal-lock*"))
        (release-snapshot-registration registration)
        (sb-thread:with-mutex (*wal-lock*)
          (remhash registration *wal-snapshot-retention-pins*)))))
