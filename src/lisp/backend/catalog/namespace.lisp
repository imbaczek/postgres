;;;; namespace.lisp -- name resolution helpers.

(in-package #:pg)

(defstruct sql-relation-name schema name)

(defstruct sql-derived-relation query)

(defstruct sql-record fields values)

(defparameter +physical-slot-create-result-fields+
  '(("slot_name" :name) ("lsn" :pg-lsn)))

(defparameter +physical-slot-advance-result-fields+
  '(("slot_name" :name) ("end_lsn" :pg-lsn)))

(defparameter +rewind-stat-file-result-fields+
  '(("size" :int8) ("access" :timestamptz) ("modification" :timestamptz)
    ("change" :timestamptz) ("creation" :timestamptz) ("isdir" :bool)))

(defparameter +control-checkpoint-result-fields+
  '(("checkpoint_lsn" :pg-lsn) ("redo_lsn" :pg-lsn) ("redo_wal_file" :text)
    ("timeline_id" :int4) ("prev_timeline_id" :int4)
    ("full_page_writes" :bool) ("logical_decoding" :bool) ("next_xid" :text)
    ("next_oid" :oid8) ("next_multixact_id" :xid) ("next_multi_offset" :int8)
    ("oldest_xid" :xid) ("oldest_xid_dbid" :oid) ("oldest_active_xid" :xid)
    ("oldest_multi_xid" :xid) ("oldest_multi_dbid" :oid)
    ("oldest_commit_ts_xid" :xid) ("newest_commit_ts_xid" :xid)
    ("data_page_checksum_version" :int4) ("checkpoint_time" :timestamptz))
  "Fixed 20-column SQL descriptor for pg_control_checkpoint().")

(defstruct sql-array
  ;; Elements are flat in row-major order and may contain +null+. Dimensions
  ;; and lower bounds have equal lengths; their dimension product is the element
  ;; count. Empty arrays use an empty vector and NIL for both lists.
  element-type-id
  elements
  dimensions
  lower-bounds)

(defparameter +publication-table-result-fields+
  '(("pubid" :oid) ("relid" :oid) ("attrs" :int2vector) ("qual" :pg-node-tree)))

(defparameter +wal-sender-result-fields+
  '(("pid" :int4) ("state" :text) ("sent_lsn" :pg-lsn) ("write_lsn" :pg-lsn)
    ("flush_lsn" :pg-lsn) ("replay_lsn" :pg-lsn) ("write_lag" :interval)
    ("flush_lag" :interval) ("replay_lag" :interval) ("sync_priority" :int4)
    ("sync_state" :text) ("reply_time" :timestamptz)))

(defconstant +bootstrap-role-oid+ 10)

(defun sql-visible-roles ()
  (let ((roles (and *catalog* (catalog-roles *catalog*)))
        (changes (and *backend* (backend-role-changes *backend*))))
    (append (mapcar #'third changes)
            (when roles
              (loop for role being the hash-values of roles
                    unless (assoc (pg-role-name role) changes :test #'string=)
                      collect role))
            (unless (or (and roles (gethash "postgres" roles))
                        (assoc "postgres" changes :test #'string=))
              (list (catalog-bootstrap-role))))))

(defun sql-roles-rows ()
  (loop for role in (sql-visible-roles)
        collect (vector (pg-role-name role) (pg-role-superuser-p role)
                        (pg-role-login-p role) (pg-role-replication-p role)
                        (pg-role-oid role))))

(defun sql-authid-rows ()
  "Return role identities without exposing password verifiers."
  (loop for role in (sql-visible-roles)
        collect (vector (pg-role-oid role) (pg-role-name role))))

(defparameter +activity-result-fields+
  '(("pid" :int4) ("usesysid" :oid) ("application_name" :text)
    ("client_addr" :inet)))

(defun sql-wal-sender-row (snapshot)
  "Convert a detached sender snapshot to C's twelve-column status record.
Report its captured priority and synchronous state; unknown positions and lag are NULL."
  (flet ((nullable-position (key)
           (let ((value (getf snapshot key)))
             (if (or (null value) (zerop value)) +null+ value))))
    (vector (getf snapshot :pid) (getf snapshot :state)
            (nullable-position :sent-lsn) (nullable-position :write-lsn)
            (nullable-position :flush-lsn) (nullable-position :replay-lsn)
            +null+ +null+ +null+ (getf snapshot :sync-priority)
            (getf snapshot :sync-state) (nullable-position :reply-time))))

(defun make-status-record-iterator (fields rows)
  "Return records from the captured rows using the supplied field descriptors."
  (lambda ()
    (if rows
        (values (make-sql-record :fields fields :values (pop rows)) t)
        (values nil nil))))

(defun make-wal-sender-result-iterator ()
  (make-status-record-iterator +wal-sender-result-fields+
                               (mapcar #'sql-wal-sender-row (wal-sender-snapshots))))

(defun make-activity-result-iterator (pid)
  "Snapshot activity for the selected backend, or all for SQL NULL.
A provisional connection has no SQL role until startup admission succeeds."
  (make-status-record-iterator
   +activity-result-fields+
   (sb-thread:with-mutex (*backend-registry-lock*)
     (loop for backend being the hash-values of *backend-registry*
           when (or (eq pid +null+) (= pid (backend-pid backend)))
             collect (vector (backend-pid backend)
                             (or (when (gethash backend *backend-admission-classes*)
                                   (let* ((user (backend-startup-user backend))
                                          (role (and *catalog* (gethash user (catalog-roles *catalog*)))))
                                     (cond (role (pg-role-oid role))
                                           ((string= user "postgres") +bootstrap-role-oid+))))
                                 +null+)
                             (copy-seq (or (backend-application-name backend) ""))
                             (or (gethash backend *backend-client-addresses*) +null+))))))

(defparameter +wal-receiver-result-fields+
  '(("pid" :int4) ("status" :text) ("receive_start_lsn" :pg-lsn)
    ("receive_start_tli" :int4) ("written_lsn" :pg-lsn) ("flushed_lsn" :pg-lsn)
    ("received_tli" :int4) ("last_msg_send_time" :timestamptz)
    ("last_msg_receipt_time" :timestamptz) ("latest_end_lsn" :pg-lsn)
    ("latest_end_time" :timestamptz) ("slot_name" :text) ("sender_host" :text)
    ("sender_port" :int4) ("conninfo" :text)))

(defun wal-receiver-result ()
  "Return C's nullable scalar record; the native pin is not a physical slot."
  (let ((statistics (physical-follow-statistics)))
    (when statistics
      (flet ((value (key &optional zero-is-null)
               (let ((value (getf statistics key)))
                 (if (or (null value) (and zero-is-null (zerop value))) +null+ value))))
        (return-from wal-receiver-result
          (make-sql-record
           :fields +wal-receiver-result-fields+
           :values (vector (value :pid) (value :status)
                           (value :receive-start-lsn t) (value :receive-start-tli)
                           (value :written-lsn t) (value :flushed-lsn t) (value :received-tli)
                           (value :last-send-time t) (value :last-receipt-time t)
                           (value :latest-end-lsn t) (value :latest-end-time t)
                           (value :public-slot) (value :host) (value :port) (value :conninfo))))))
    +null+))

(defun publication-table-result-rows (names &optional (relation-id nil relation-p))
  "Match the variadic SRF and C20's missing-publication-tolerant relation overload."
  (when (or (eq names +null+) (and relation-p (eq relation-id +null+)))
    (return-from publication-table-result-rows nil))
  (when (sql-array-p names)
    (unless (eq (sql-array-element-type-id names) :text)
      (ereport "42804" "publication names must be a text array"))
    (setf names (sql-array-elements names)))
  (unless (and (vectorp names) (not (stringp names)))
    (ereport "42804" "publication names must be a text array"))
  (with-write-lock
    (let ((database (current-database)) (rows nil))
      (when (and relation-p (not (find-table-by-oid relation-id)))
        (return-from publication-table-result-rows nil))
      (when (find +null+ names :test #'eq)
        (ereport "22004" "null array element not allowed"))
      (loop for name across names
            for publication = (gethash name (database-publications database))
            do (unless (or publication relation-p)
                 (ereport "42704" "publication ~s does not exist" name))
               (when publication
                 (unless (pg-publication-oid publication)
                   (error "Publication has no durable OID"))
                 (dolist (table
                          (sort (loop for table-name in
                                                     (publication-selected-table-names publication database)
                                      for table = (gethash table-name (database-tables database))
                                      when (or (not relation-p) (= relation-id (pg-class-oid table)))
                                        collect table)
                                #'< :key #'pg-class-oid))
                   (let ((entry (publication-table-entry publication (pg-class-name table))))
                     (when (getf entry :filter)
                       (ereport "0A000" "publication row-filter pg_node_tree output is not supported"))
                     (push (make-sql-record
                            :fields +publication-table-result-fields+
                            :values (vector
                                     (pg-publication-oid publication) (pg-class-oid table)
                                     (if (plusp (length (pg-class-columns table)))
                                         (map 'vector #'pg-column-attnum (pg-class-columns table))
                                         +null+)
                                     +null+))
                           rows)))))
      (nreverse rows))))

(defun make-publication-table-result-iterator (names &optional (relation-id nil relation-p))
  (let ((rows (if relation-p (publication-table-result-rows names relation-id)
                  (publication-table-result-rows names))))
    (lambda ()
      (if rows (values (pop rows) t) (values nil nil)))))

(defun sql-relation-base-name (reference)
  "Return a relation identifier without interpreting dots inside a quoted name."
  (if (sql-relation-name-p reference)
      (sql-relation-name-name reference)
      reference))

(defun session-search-path ()
  "Return the raw per-session search_path, including the PostgreSQL default."
  (if *backend*
      (gethash "search_path" (backend-settings *backend*) "\"$user\", public")
      "\"$user\", public"))

(defun search-path-identifiers (text &optional (parameter "search_path"))
  "Follow C SplitIdentifierString: whitespace/comma delimiters and doubled quotes."
  (let ((position 0)
        (end (length text))
        (names nil))
    (labels ((space-p (character) (member (char-code character) '(9 10 11 12 13 32)))
             (skip-space ()
               (loop while (and (< position end) (space-p (char text position)))
                     do (incf position)))
             (invalid ()
               (ereport "22023" "invalid value for parameter ~s: ~s" parameter text))
             (read-name ()
               (when (= position end) (invalid))
               (if (= (char-code (char text position)) 34)
                   (progn
                     (incf position)
                     (with-output-to-string (output)
                       (loop
                         (when (= position end) (invalid))
                         (let ((character (char text position)))
                           (incf position)
                           (if (= (char-code character) 34)
                               (if (and (< position end)
                                        (= (char-code (char text position)) 34))
                                   (progn (write-char character output) (incf position))
                                   (return))
                               (write-char character output))))))
                   (let ((start position))
                     (loop while (and (< position end)
                                      (/= (char-code (char text position)) 44)
                                      (not (space-p (char text position))))
                           do (incf position))
                     (when (= start position) (invalid))
                     (map 'string
                          (lambda (character)
                            (if (<= 65 (char-code character) 90)
                                (code-char (+ (char-code character) 32))
                                character))
                          (subseq text start position))))))
      (skip-space)
      (when (= position end) (return-from search-path-identifiers nil))
      (loop
        (push (read-name) names)
        (skip-space)
        (when (= position end) (return (nreverse names)))
        (unless (= (char-code (char text position)) 44) (invalid))
        (incf position)
        (skip-space)))))

(defun parse-sql-search-path (text)
  "Resolve existing schemas from C-style search_path identifiers."
  (remove-duplicates
   (remove-if-not (lambda (name) (member name '("public" "pg_catalog") :test #'string=))
                  (search-path-identifiers text))
   :test #'string= :from-end t))

(defun explicit-sql-search-path ()
  "Resolve existing explicit schemas without implicitly adding pg_catalog."
  (parse-sql-search-path (session-search-path)))

(defvar *session-setting-reports-suppressed-p* nil)

(defun set-session-search-path (value)
  "Validate and save the session search path, retaining transaction undo state.
A changed value is reported at the next reporting boundary."
  (let ((text (if (eq value +null+) "\"$user\", public" value)))
    (parse-sql-search-path text)
    (session-remember-settings)
    (unless (string= text (session-search-path))
      (setf (gethash "search_path" (backend-settings *backend*)) text))
    text))

(defun set-config-value (name value local-p)
  "Set a supported session value, including LOCAL application_name and
synchronous_commit. Reject unsupported parameters and other local settings."
  (when (eq name +null+)
    (ereport "22004" "SET requires parameter name"))
  (startup-check-session-setting (string-downcase name))
  (when (string-equal name "application_name")
    (return-from set-config-value
      (session-set-application-name value (and local-p (not (eq local-p +null+))))))
  (when (string-equal name "synchronous_commit")
    (session-set-synchronous-commit value (and local-p (not (eq local-p +null+))))
    (return-from set-config-value (session-synchronous-commit-value)))
  (when (session-extra-setting-p (string-downcase name))
    (when (and local-p (not (eq local-p +null+)))
      (ereport "0A000" "transaction-local settings are not supported"))
    (return-from set-config-value (session-set-extra-setting (string-downcase name) value)))
  (unless (string-equal name "search_path")
    (ereport "42704" "unrecognized configuration parameter \"~a\"" name))
  (when (and local-p (not (eq local-p +null+)))
    (ereport "0A000" "transaction-local settings are not supported"))
  (set-session-search-path value))

(defun ensure-sql-creation-schema ()
  "Refuse unqualified creation when no supported creation schema exists."
  (let ((schema (first (explicit-sql-search-path))))
    (unless schema (ereport "3F000" "no schema has been selected to create in"))
    (unless (string= schema "public")
      (ereport "0A000" "creation in schema \"~a\" is not supported" schema))))

(defun effective-sql-search-path ()
  "Include pg_catalog implicitly unless its explicit position already exists."
  (let ((schemas (explicit-sql-search-path)))
    (if (member "pg_catalog" schemas :test #'string=)
        schemas
        (cons "pg_catalog" schemas))))

(defun sql-system-relation-p (table)
  "Identify executor-backed catalogs by identity, not a user table name."
  (not (null (sql-system-relation-definition (pg-class-oid table)))))

(defparameter +sql-publication-fields+
  ;; Name, exposed type, not-null flag, original Anum_pg_publication_*.
  '(("oid" :oid t 1) ("pubname" :name t 2) ("pubowner" :oid t 3)
    ("puballtables" :bool t 4) ("puballsequences" :bool t 5)
    ("pubinsert" :bool t 6) ("pubupdate" :bool t 7) ("pubdelete" :bool t 8)
    ("pubtruncate" :bool t 9) ("pubviaroot" :bool t 10) ("pubgencols" :char t 11)))

(defparameter +sql-settings-fields+
  '(("name" :text nil 1) ("setting" :text nil 2) ("unit" :text nil 3)
    ("context" :text nil 7) ("vartype" :text nil 8) ("source" :text nil 9)
    ("enumvals" :text-array nil 12)
    ("sourcefile" :text nil 15) ("sourceline" :int4 nil 16)
    ("pending_restart" :bool nil 17)))

(defparameter +sql-replication-slot-fields+
  '(("slot_name" :name nil 1) ("plugin" :name nil 2) ("slot_type" :text nil 3)
    ("datoid" :oid nil 4) ("database" :name nil 5) ("temporary" :bool nil 6)
    ("active" :bool nil 7) ("active_pid" :int4 nil 8) ("xmin" :xid nil 9)
    ("catalog_xmin" :xid nil 10) ("restart_lsn" :pg-lsn nil 11)
    ("confirmed_flush_lsn" :pg-lsn nil 12) ("wal_status" :text nil 13)
    ("safe_wal_size" :int8 nil 14) ("two_phase" :bool nil 15)
    ("two_phase_at" :pg-lsn nil 16) ("inactive_since" :timestamptz nil 17)
    ("conflicting" :bool nil 18) ("invalidation_reason" :text nil 19)
    ("failover" :bool nil 20) ("synced" :bool nil 21)
    ("slotsync_skip_reason" :text nil 22) ("retained_wal_bytes" :int8 t 23)))

(defparameter +replication-slot-result-fields+
  (loop for (name type) in +sql-replication-slot-fields+
        unless (member name '("database" "retained_wal_bytes") :test #'string=)
          collect (list name type)))

(defstruct (sql-system-relation-descriptor
            (:constructor %make-sql-system-relation-descriptor
                (oid name kind rows fields attribute-count)))
  (oid nil :read-only t)
  (name nil :read-only t)
  (kind nil :read-only t)
  (rows nil :read-only t)
  (fields nil :read-only t)
  (attribute-count nil :read-only t))

(defun make-sql-system-relation-descriptor (oid name kind rows fields)
  (%make-sql-system-relation-descriptor
   oid name kind rows fields (reduce #'max fields :key #'fourth)))

(defparameter +sql-statement-statistics-fields+
  '(("userid" :oid t 1) ("dbid" :oid t 2) ("toplevel" :bool t 3)
    ("queryid" :int8 nil 4) ("query" :text nil 5) ("plans" :int8 t 6)
    ("total_plan_time" :float8 t 7) ("min_plan_time" :float8 t 8)
    ("max_plan_time" :float8 t 9) ("mean_plan_time" :float8 t 10)
    ("stddev_plan_time" :float8 t 11) ("calls" :int8 t 12)
    ("total_exec_time" :float8 t 13) ("min_exec_time" :float8 t 14)
    ("max_exec_time" :float8 t 15) ("mean_exec_time" :float8 t 16)
    ("stddev_exec_time" :float8 t 17) ("rows" :int8 t 18)
    ("shared_blks_hit" :int8 nil 19) ("shared_blks_read" :int8 nil 20)
    ("shared_blks_dirtied" :int8 nil 21) ("shared_blks_written" :int8 nil 22)
    ("local_blks_hit" :int8 nil 23) ("local_blks_read" :int8 nil 24)
    ("local_blks_dirtied" :int8 nil 25) ("local_blks_written" :int8 nil 26)
    ("temp_blks_read" :int8 nil 27) ("temp_blks_written" :int8 nil 28)
    ("shared_blk_read_time" :float8 nil 29) ("shared_blk_write_time" :float8 nil 30)
    ("local_blk_read_time" :float8 nil 31) ("local_blk_write_time" :float8 nil 32)
    ("temp_blk_read_time" :float8 nil 33) ("temp_blk_write_time" :float8 nil 34)
    ("wal_records" :int8 nil 35) ("wal_fpi" :int8 nil 36) ("wal_bytes" :numeric nil 37)
    ("wal_buffers_full" :int8 nil 38)
    ("jit_functions" :int8 nil 39) ("jit_generation_time" :float8 nil 40)
    ("jit_inlining_count" :int8 nil 41) ("jit_inlining_time" :float8 nil 42)
    ("jit_optimization_count" :int8 nil 43) ("jit_optimization_time" :float8 nil 44)
    ("jit_emission_count" :int8 nil 45) ("jit_emission_time" :float8 nil 46)
    ("jit_deform_count" :int8 nil 47) ("jit_deform_time" :float8 nil 48)
    ("parallel_workers_to_launch" :int8 nil 49) ("parallel_workers_launched" :int8 nil 50)
    ("generic_plan_calls" :int8 nil 51) ("custom_plan_calls" :int8 nil 52)
    ("stats_since" :timestamptz t 53) ("minmax_stats_since" :timestamptz t 54)))

(defparameter +statement-statistics-result-fields+
  (mapcar (lambda (field) (list (first field) (second field)))
          +sql-statement-statistics-fields+))

(defparameter +statement-statistics-info-fields+
  '(("dealloc" :int8) ("stats_reset" :timestamptz)))

(defparameter +sql-activity-view-fields+
  '(("datname" :name nil 2) ("pid" :int4 t 3)
    ("usesysid" :oid nil 5) ("usename" :name nil 6)
    ("application_name" :text t 7)
    ("xact_start" :timestamptz nil 12) ("query_start" :timestamptz nil 13)
    ("state_change" :timestamptz nil 14)
    ("wait_event_type" :text nil 15) ("wait_event" :text nil 16) ("state" :text nil 17)
    ("query_id" :int8 nil 20) ("query" :text nil 21) ("backend_type" :text t 22)))

(defparameter +sql-system-relations+
  ;; Catalog OIDs and original attribute numbers follow the pinned C tree.
  ;; The sequence view OID is from that tree's unmodified initdb catalog.
  (list
   (make-sql-system-relation-descriptor 15990 "pg_stat_statements" #\v
                                        'sql-statement-statistics-rows
                                        +sql-statement-statistics-fields+)
   (make-sql-system-relation-descriptor
    15991 "pg_stat_statements_info" #\v 'sql-statement-statistics-info-rows
    '(("dealloc" :int8 t 1) ("stats_reset" :timestamptz nil 2)))
   (make-sql-system-relation-descriptor 15992 "pg_stat_activity" #\v
                                        'sql-activity-view-rows +sql-activity-view-fields+)
   (make-sql-system-relation-descriptor
    15993 "pg_stat_statements_resources" #\v 'sql-statement-statistics-resource-rows
    '(("userid" :oid t 1) ("dbid" :oid t 2) ("toplevel" :bool t 3)
      ("queryid" :int8 nil 4) ("relation_reads" :int8 t 5)
      ("clean_cache_hits" :int8 t 6) ("relation_writes" :int8 t 7)
      ("spill_read_bytes" :int8 t 8) ("spill_written_bytes" :int8 t 9)
      ("wal_records" :int8 t 10) ("wal_bytes" :int8 t 11)
      ("execution_errors" :int8 t 12) ("execution_cancellations" :int8 t 13)))
   (make-sql-system-relation-descriptor
    15994 "pg_lisp_runtime_stats" #\v 'sql-lisp-runtime-stats-rows
    '(("gc_cpu_time_ms" :float8 t 1) ("allocated_bytes" :int8 t 2)
      ("dynamic_heap_bytes" :int8 t 3) ("build_revision" :text nil 4)))
   (make-sql-system-relation-descriptor 12109 "pg_settings" #\v
                                        'startup-settings-rows +sql-settings-fields+)
   (make-sql-system-relation-descriptor 1260 "pg_authid" #\r 'sql-authid-rows
                                        '(("oid" :oid t 1) ("rolname" :name t 2)))
   (make-sql-system-relation-descriptor 12110 "pg_roles" #\v 'sql-roles-rows
                                        '(("rolname" :name t 1) ("rolsuper" :bool t 2)
                                          ("rolcanlogin" :bool t 6) ("rolreplication" :bool t 7) ("oid" :oid t 13)))
   (make-sql-system-relation-descriptor 1262 "pg_database" #\r 'sql-database-rows
                                        '(("oid" :oid t 1) ("datname" :name t 2)))
   (make-sql-system-relation-descriptor 1259 "pg_class" #\r 'sql-class-rows
                                        '(("oid" :oid t 1) ("relname" :name t 2)
                                          ("relnamespace" :oid t 3) ("relkind" :char t 18)
                                          ("relnatts" :int2 t 19) ("relreplident" :char t 27)
                                          ("reltoastrelid" :oid t 14) ("relispartition" :bool t 28)))
   (make-sql-system-relation-descriptor 1249 "pg_attribute" #\r 'sql-attribute-rows
                                        '(("attrelid" :oid t 1) ("attname" :name t 2)
                                          ("atttypid" :oid t 3) ("attnum" :int2 t 5)
                                          ("atttypmod" :int4 t 6) ("attnotnull" :bool t 12)
                                          ("attgenerated" :char t 16) ("attisdropped" :bool t 17)
                                          ("attstorage" :char t 10)))
   (make-sql-system-relation-descriptor 1247 "pg_type" #\r 'sql-type-rows
                                        '(("oid" :oid t 1) ("typname" :name t 2)
                                          ("typnamespace" :oid t 3) ("typlen" :int2 t 5)
                                          ("typalign" :char t 23) ("typstorage" :char t 24)))
   (make-sql-system-relation-descriptor 2610 "pg_index" #\r 'sql-index-rows
                                        '(("indexrelid" :oid t 1) ("indrelid" :oid t 2)
                                          ("indisunique" :bool t 5) ("indisprimary" :bool t 7)
                                          ("indisvalid" :bool t 11) ("indkey" :int2vector t 16)
                                          ("indnkeyatts" :int2 t 4)
                                          ("indisready" :bool t 13) ("indislive" :bool t 14)))
   (make-sql-system-relation-descriptor 2615 "pg_namespace" #\r 'sql-namespace-rows
                                        '(("oid" :oid t 1) ("nspname" :name t 2)))
   (make-sql-system-relation-descriptor 6104 "pg_publication" #\r 'sql-publication-rows
                                        +sql-publication-fields+)
   (make-sql-system-relation-descriptor 12073 "pg_publication_sequences" #\v
                                        'sql-publication-sequence-rows
                                        '(("pubname" :name nil 1) ("schemaname" :name nil 2)
                                          ("sequencename" :name nil 3)))
   (make-sql-system-relation-descriptor 12283 "pg_replication_slots" #\v
                                        'sql-replication-slot-rows
                                        +sql-replication-slot-fields+)))

(defun sql-system-relation-definition (identity)
  (if (stringp identity)
      (find identity +sql-system-relations+ :key #'sql-system-relation-descriptor-name :test #'string=)
      (find identity +sql-system-relations+ :key #'sql-system-relation-descriptor-oid :test #'eql)))

(defun sql-system-row-source (table)
  (let ((descriptor (sql-system-relation-definition (pg-class-oid table))))
    (when descriptor (sql-system-relation-descriptor-rows descriptor))))

(defun write-sql-statement-phase (row offset phase)
  (let ((count (statement-phase-statistics-count phase)))
    (setf (aref row offset) count
          (aref row (+ offset 1)) (statement-phase-statistics-total phase)
          (aref row (+ offset 2)) (statement-phase-statistics-minimum phase)
          (aref row (+ offset 3)) (statement-phase-statistics-maximum phase)
          (aref row (+ offset 4)) (statement-phase-statistics-mean phase)
          (aref row (+ offset 5))
          (if (> count 1)
              (sqrt (/ (max 0.0d0 (statement-phase-statistics-m2 phase)) count))
              0.0d0))))

(defun sql-lisp-runtime-stats-rows ()
  "Sample process-wide SBCL counters without running a collection."
  (let ((revision (startup-setting-show-value "pg_lisp.build_revision")))
    (list (vector (internal-time-ticks->milliseconds sb-ext:*gc-run-time*)
                  (sb-ext:get-bytes-consed)
                  (sb-kernel:dynamic-usage)
                  (if (and revision (plusp (length revision))) revision +null+)))))

(defun sql-statement-statistics-info-rows ()
  (let ((store *statement-statistics-store*))
    (when store
      (multiple-value-bind (evictions reset-time) (statement-statistics-info store)
        (list (vector evictions (or reset-time +null+)))))))

(defun make-statement-statistics-info-iterator ()
  (make-status-record-iterator +statement-statistics-info-fields+
                               (sql-statement-statistics-info-rows)))

(defun sql-statement-statistics-reset (&optional (userid 0) (dbid 0) (queryid 0) minmax-only)
  "Clear cumulative statistics and return the PostgreSQL timestamp of the reset."
  (let* ((name (and *backend* (backend-startup-user *backend*)))
         (role (and name *catalog* (gethash name (catalog-roles *catalog*))))
         (store *statement-statistics-store*))
    (unless (if role (pg-role-superuser-p role) (equal name "postgres"))
      (ereport "42501" "permission denied for function pg_stat_statements_reset"))
    (if store
        (reset-statement-statistics store (lambda () (sql-current-local-microseconds nil nil))
                                    userid dbid queryid minmax-only)
        +null+)))

(defun sql-statement-statistics-reader-identity ()
  (let* ((name (and *backend* (backend-startup-user *backend*)))
         (role (and name (authentication-committed-role name))))
    (values (cond (role (pg-role-oid role))
                  ((equal name "postgres") +bootstrap-role-oid+))
            (if role (pg-role-superuser-p role) (equal name "postgres")))))

(defun sql-statement-statistics-resource-rows ()
  "Report completed execution resources in native units."
  (let ((store *statement-statistics-store*))
    (when store
      (multiple-value-bind (userid superuser-p) (sql-statement-statistics-reader-identity)
        (loop with empty-usage = (make-statement-resource-usage)
              for entry in (snapshot-statement-statistics store nil)
              for resources = (statement-statistics-entry-resources entry)
              for usage = (or resources empty-usage)
              for errors = (statement-statistics-entry-execution-errors entry)
              for cancellations = (statement-statistics-entry-execution-cancellations entry)
              when (or resources (plusp errors) (plusp cancellations))
                collect
                (destructuring-bind (owner database toplevel queryid)
                    (statement-statistics-entry-key entry)
                  (vector owner database toplevel
                          (if (or superuser-p (eql userid owner)) queryid +null+)
                          (statement-resource-usage-relation-reads usage)
                          (statement-resource-usage-clean-cache-hits usage)
                          (statement-resource-usage-relation-writes usage)
                          (statement-resource-usage-spill-read-bytes usage)
                          (statement-resource-usage-spill-written-bytes usage)
                          (statement-resource-usage-wal-records usage)
                          (statement-resource-usage-wal-bytes usage)
                          errors cancellations)))))))

(defun sql-statement-statistics-rows (&optional (showtext t))
  "Adapt detached cumulative entries to the supported pg_stat_statements columns."
  (let ((store *statement-statistics-store*))
    (when store
      (multiple-value-bind (userid superuser-p) (sql-statement-statistics-reader-identity)
        (loop for entry in (snapshot-statement-statistics store showtext)
              when (or (plusp (statement-phase-statistics-count
                               (statement-statistics-entry-planning entry)))
                       (plusp (statement-phase-statistics-count
                               (statement-statistics-entry-execution entry))))
                collect
                (destructuring-bind (owner database toplevel queryid)
                    (statement-statistics-entry-key entry)
                  (let* ((visible-p (or superuser-p (eql userid owner)))
                         (row (make-array (length +sql-statement-statistics-fields+)
                                          :initial-element +null+)))
                    (setf (aref row 0) owner
                          (aref row 1) database
                          (aref row 2) toplevel
                          (aref row 3) (if visible-p queryid +null+)
                          (aref row 4) (cond ((not showtext) +null+)
                                             (visible-p
                                              (or (statement-statistics-entry-text entry) +null+))
                                             (t "<insufficient privilege>"))
                          (aref row 17) (statement-statistics-entry-rows entry)
                          (aref row 52) (statement-statistics-entry-stats-since entry)
                          (aref row 53) (statement-statistics-entry-minmax-stats-since entry))
                    (write-sql-statement-phase row 5 (statement-statistics-entry-planning entry))
                    (write-sql-statement-phase row 11 (statement-statistics-entry-execution entry))
                    row)))))))

(defun make-statement-statistics-result-iterator (showtext)
  (make-status-record-iterator +statement-statistics-result-fields+
                               (unless (eq showtext +null+)
                                 (sql-statement-statistics-rows showtext))))

(defun backend-wait-event-labels (event)
  "Return the SQL wait class and name for a published backend wait."
  (case event
    (:relation (values "Lock" "relation"))
    (:transactionid (values "Lock" "transactionid"))
    (:btree (values "LWLock" "BtreeRelation"))
    (:client-read (values "Client" "ClientRead"))
    (:client-write (values "Client" "ClientWrite"))
    (:data-file-read (values "IO" "DataFileRead"))
    (:data-file-write (values "IO" "DataFileWrite"))
    (:wal-write (values "IO" "WALWrite"))
    (:wal-sync (values "IO" "WALSync"))
    (:sync-rep (values "IPC" "SyncRep"))
    (otherwise (values +null+ +null+))))

(defun sql-activity-view-row (backend viewer privileged-p)
  "Capture a coherent activity snapshot before copying and formatting its fields."
  (let* ((name (backend-startup-user backend))
         (role (authentication-committed-role name))
         (userid (cond (role (pg-role-oid role))
                       ((equal name "postgres") +bootstrap-role-oid+)
                       (t +null+)))
         (visible-p (or privileged-p (equal viewer name))))
    (multiple-value-bind (application started query-start state-change wait state queryid query replication)
        (sb-thread:with-mutex (*live-operation-lock*)
          ;; These strings are replaced on publication, so the captured
          ;; references remain valid after releasing the activity mutex.
          (values (backend-application-name backend)
                  (and visible-p (backend-transaction-start-unix-microseconds backend))
                  (and visible-p (backend-activity-query-start backend))
                  (and visible-p (backend-activity-state-change backend))
                  (and visible-p (backend-activity-wait-event backend))
                  (when visible-p
                    (cond ((backend-executing backend) "active")
                          ((eq (backend-state backend) :aborted) "idle in transaction (aborted)")
                          ((eq (backend-state backend) :in-progress) "idle in transaction")
                          (t "idle")))
                  (and visible-p (backend-activity-queryid backend))
                  (and visible-p (backend-activity-query backend))
                  (backend-replication-mode backend)))
      (multiple-value-bind (wait-type wait-name) (backend-wait-event-labels wait)
        (flet ((timestamp (stamp)
                 (if stamp (sql-current-local-microseconds nil stamp) +null+)))
          (vector (backend-database backend) (backend-pid backend) userid name
                  (copy-seq (or application ""))
                  (timestamp started) (timestamp query-start) (timestamp state-change)
                  wait-type wait-name (or state +null+) (or queryid +null+)
                  (cond (query (copy-seq query))
                        (visible-p +null+)
                        (t "<insufficient privilege>"))
                  (if replication "walsender" "client backend")))))))

(defun sql-activity-view-rows ()
  "Copy admitted backend membership before taking any activity snapshots."
  (let* ((viewer (and *backend* (backend-startup-user *backend*)))
         (role (and viewer (authentication-committed-role viewer)))
         (privileged-p (if role (pg-role-superuser-p role) (equal viewer "postgres")))
         (backends
           (sb-thread:with-mutex (*backend-registry-lock*)
             (loop for backend being the hash-values of *backend-registry*
                   when (gethash backend *backend-admission-classes*)
                     collect backend))))
    (mapcar (lambda (backend) (sql-activity-view-row backend viewer privileged-p)) backends)))

(defun sql-database-rows ()
  "Expose durable database identities to standard replication discovery."
  (with-write-lock
    (let ((rows nil))
      (maphash (lambda (name database)
                 (push (vector (database-oid database) name) rows))
               (catalog-databases *catalog*))
      rows)))

(defun sql-namespace-rows ()
  "Return public, catalog, and internal TOAST namespace metadata."
  (list (vector 11 "pg_catalog") (vector 99 "pg_toast") (vector 2200 "public")))

(defun sql-replication-slot-row (row)
  "Adapt the private logical WAL snapshot to typed SQL values and column order."
  (flet ((lsn (slot)
           (let ((value (aref row slot)))
             (if (eq value +null+) value (sql-pg-lsn-input value)))))
    (vector (aref row 0) (aref row 1) (lsn 2) (lsn 3) (aref row 5) (aref row 4)
            (aref row 6) "logical" (aref row 7) nil
            (if (string= (aref row 5) "lost") "wal_removed" +null+) nil nil)))

(defun sql-public-replication-slot-row (slot policy keep-mb)
  "Build a public view row while the caller holds write and WAL locks.
Lost slots expose NULL restart while retaining their saved floor internally.
The retained-byte extension measures from the durable or flushed endpoint."
  (let* ((physical (wal-physical-slot-p slot))
         (restart (wal-replication-slot-restart-lsn slot))
         (lost (eq :lost (wal-replication-slot-wal-status slot)))
         (horizons (when (and physical (not lost)) (physical-slot-horizons slot)))
         (xmin (first horizons))
         (catalog-xmin (unless lost
                         (if physical (second horizons) (wal-replication-slot-catalog-xmin slot))))
         (database-oid (wal-replication-slot-database-oid slot))
         (database (when database-oid
                     (loop for database being the hash-values of (catalog-databases *catalog*)
                           when (eql database-oid (database-oid database))
                             return (database-name database))))
         (backend (wal-replication-slot-active-owner-backend slot))
         (inactive (wal-replication-slot-inactive-since slot))
         (durable (if (recovery-in-progress-p) (physical-slot-recovery-durable-lsn) *wal-durable-lsn*)))
    (flet ((nullable (value) (if (null value) +null+ value)))
      (vector (wal-replication-slot-name slot) (nullable (wal-replication-slot-plugin slot))
              (if physical "physical" "logical") (nullable database-oid) (nullable database)
              (wal-replication-slot-temporary-p slot)
              (not (null (wal-replication-slot-active-owner slot)))
              (if backend (backend-pid backend) +null+)
              (if xmin (xid32-of xmin) +null+)
              (if catalog-xmin (xid32-of catalog-xmin) +null+)
              (if lost +null+ (nullable restart))
              (nullable (wal-replication-slot-confirmed-flush-lsn slot))
              (wal-slot-catalog-status slot policy keep-mb)
              (wal-slot-safe-wal-bytes slot policy keep-mb)
              nil +null+ (if (zerop inactive) +null+ inactive)
              (if physical +null+ nil) (if lost "wal_removed" +null+)
              nil nil +null+ (if (or lost (null restart)) 0 (max 0 (- durable restart)))))))

(declaim (ftype function startup-retention-inputs))

(defun sql-replication-slot-rows ()
  "Snapshot both slot kinds under write-before-WAL locking.
Capture policy and keep size once for every row in the snapshot."
  (with-write-lock
    (sb-thread:with-mutex (*wal-lock*)
      (multiple-value-bind (policy keep-mb) (startup-retention-inputs)
        (loop for slot being the hash-values of *wal-replication-slots*
              collect (sql-public-replication-slot-row slot policy keep-mb))))))

(defun make-replication-slot-result-iterator ()
  "Project the public snapshot to the 21 columns of pg_get_replication_slots."
  (let ((rows (sql-replication-slot-rows))
        (positions (mapcar (lambda (field)
                             (position (first field) +sql-replication-slot-fields+
                                       :key #'first :test #'string=))
                           +replication-slot-result-fields+)))
    (lambda ()
      (if rows
          (let ((row (pop rows)))
            (values (make-sql-record
                     :fields +replication-slot-result-fields+
                     :values (map 'vector (lambda (position) (aref row position)) positions))
                    t))
          (values nil nil)))))

(defun sql-class-rows ()
  "Snapshot implemented catalog relations and persisted user objects."
  (with-write-lock
    (let ((database (current-database))
          (rows (loop for entry in +sql-system-relations+
                      collect (vector (sql-system-relation-descriptor-oid entry) (sql-system-relation-descriptor-name entry) 11
                                      (char-code (sql-system-relation-descriptor-kind entry))
                                      (sql-system-relation-descriptor-attribute-count entry)
                                      (char-code #\d) 0 nil))))
      (labels ((add-object (oid name kind columns &optional (toast-oid 0) (namespace 2200) (identity #\d))
                 (push (vector oid name namespace (char-code kind) columns
                               (char-code identity) toast-oid nil) rows)))
        (catalog-map-visible-relations (lambda (name table)
                                         (let* ((toast (pg-class-toast table))
                                                (parent-oid (pg-class-oid table)))
                                           (add-object parent-oid name #\r (length (pg-class-columns table))
                                                       (if toast (pg-toast-oid toast) 0))
                                           (when toast
                                             (let ((toast-name (format nil "pg_toast_~d" parent-oid)))
                                               (add-object (pg-toast-oid toast) toast-name #\t 3 0 99 #\n)
                                               (add-object (pg-toast-index-oid toast)
                                                           (concatenate 'string toast-name "_index") #\i 2 0 99 #\n)))))
                                       database :table)
        (catalog-map-visible-relations (lambda (name index)
                                         (add-object (pg-index-oid index) name #\i (length (index-key-specifications index))))
                                       database :index)
        (maphash (lambda (name sequence)
                   (add-object (pg-sequence-oid sequence) name #\S 3))
                 (database-sequences database)))
      (sort rows #'< :key (lambda (row) (aref row 0))))))

(defun sql-type-rows ()
  "Expose metadata for the type identifiers recognized by SQL expression analysis."
  (loop for (type oid length) in +sql-expression-types+
        collect (vector oid (sql-type-catalog-name type) 11 length
                        (char-code (sql-type-alignment-code type))
                        (char-code (sql-type-storage-code type)))))

(defun sql-attribute-row (oid name type number typmod notnull &optional storage)
  (vector oid name (type-oid type) number (or typmod -1) notnull 0 nil
          (char-code (or storage (sql-type-storage-code type)))))

(defun sql-attribute-rows ()
  "Expose original attribute numbers; virtual executor slots remain dense internally."
  (with-write-lock
    (let ((database (current-database)) (rows nil))
      (dolist (entry +sql-system-relations+)
        (loop for field in (sql-system-relation-descriptor-fields entry)
              do (push (sql-attribute-row (sql-system-relation-descriptor-oid entry) (first field) (second field)
                                          (fourth field) -1 (third field)) rows)))
      (catalog-map-visible-relations
       (lambda (name table)
         (declare (ignore name))
         (loop for column across (pg-class-columns table)
               do (push (sql-attribute-row
                         (pg-class-oid table) (pg-column-name column) (pg-column-type column)
                         (pg-column-attnum column) (pg-column-typmod column)
                         (pg-column-notnull column) (pg-column-effective-storage column)) rows))
         (let ((toast (pg-class-toast table)))
           (when toast
             (dolist (relation (list (cons (pg-toast-oid toast) t)
                                     (cons (pg-toast-index-oid toast) nil)))
               (loop for (column type) in '(("chunk_id" :oid) ("chunk_seq" :int4) ("chunk_data" :bytea))
                     for number from 1
                     when (or (cdr relation) (< number 3))
                       do (push (sql-attribute-row (car relation) column type number -1 nil #\p) rows))))))
       database :table)
      (catalog-map-visible-relations
       (lambda (name index)
         (declare (ignore name))
         (let ((table (find-table-by-oid (pg-index-table-oid index))))
           (loop for key in (index-key-specifications index) for number from 1
                 for column = (when (integerp key) (aref (pg-class-columns table) (1- key)))
                 do (push (if column
                              (sql-attribute-row (pg-index-oid index) (pg-column-name column)
                                                 (pg-column-type column) number (pg-column-typmod column)
                                                 nil (pg-column-effective-storage column))
                              (sql-attribute-row (pg-index-oid index) "expr" (expr-type key)
                                                 number (expr-typmod key) nil))
                          rows))))
       database :index)
      (maphash
       (lambda (name sequence)
         (declare (ignore name))
         (loop for (column type) in '(("last_value" :int8) ("log_cnt" :int8) ("is_called" :bool))
               for number from 1
               do (push (sql-attribute-row (pg-sequence-oid sequence) column type number -1 t)
                        rows)))
       (database-sequences database))
      (sort rows (lambda (left right)
                   (or (< (aref left 0) (aref right 0))
                       (and (= (aref left 0) (aref right 0))
                            (< (aref left 3) (aref right 3)))))))))

(defun sql-index-rows ()
  (with-write-lock
    (let ((database (current-database)) (rows nil))
      (catalog-map-visible-relations (lambda (name index)
                                       (declare (ignore name))
                                       (push (vector (pg-index-oid index) (pg-index-table-oid index)
                                                     (pg-index-unique-p index) (pg-index-primary-p index)
                                                     (pg-index-valid-p index) (map 'vector (lambda (key) (if (integerp key) key 0))
                                                                                   (index-key-specifications index))
                                                     (length (index-key-specifications index))
                                                     (pg-index-ready-p index) (pg-index-live-p index)) rows))
                                     database :index)
      (catalog-map-visible-relations (lambda (name table)
                                       (declare (ignore name))
                                       (let ((toast (pg-class-toast table)))
                                         (when toast
                                           (push (vector (pg-toast-index-oid toast) (pg-toast-oid toast)
                                                         t t t #(1 2) 2 t t) rows))))
                                     database :table)
      rows)))

(defun sql-replica-identity-index (oid)
  (with-write-lock
    (let ((table (find-table-by-oid oid)))
      (unless table
        (if (sql-system-relation-definition oid)
            (return-from sql-replica-identity-index +null+)
            (ereport "42P01" "relation with OID ~a does not exist" oid)))
      (let ((index (find-if (lambda (index)
                              (and (pg-index-primary-p index) (pg-index-valid-p index)))
                            (table-indexes table))))
        (if index (pg-index-oid index) +null+)))))

(defun sql-publication-sequence-rows ()
  "Expose selected sequence objects; owned-column bootstrap is a separate extension."
  (with-write-lock
    (let ((database (current-database)))
      (loop for publication in
                            (sort (loop for pub being the hash-values of (database-publications database)
                                        collect pub)
                                  #'string< :key #'pg-publication-name)
            append
            (let ((sequences
                    (if (eq (pg-publication-sequences publication) :all)
                        (loop for sequence being the hash-values of (database-sequences database)
                              collect sequence)
                        (remove nil (mapcar (lambda (name) (gethash name (database-sequences database)))
                                            (pg-publication-sequences publication))))))
              (loop for sequence in (sort sequences #'string< :key #'pg-sequence-name)
                    collect (vector (pg-publication-name publication) "public"
                                    (pg-sequence-name sequence))))))))

(defun sql-result-column-attnum (table-oid slot)
  "Separate dense virtual-row slots from original catalog attribute numbers."
  (let* ((descriptor (sql-system-relation-definition table-oid))
         (fields (and descriptor (sql-system-relation-descriptor-fields descriptor))))
    (if (and fields slot (<= 1 slot (length fields)))
        (fourth (nth (1- slot) fields))
        slot)))

(defun make-sql-system-relation (descriptor)
  "Build fresh executor metadata with dense, one-based column slots."
  (let ((oid (sql-system-relation-descriptor-oid descriptor)))
    (make-pg-class
     :oid oid :name (sql-system-relation-descriptor-name descriptor) :relfilenode (1- oid)
     :columns
     (coerce (loop for (column-name type notnull) in (sql-system-relation-descriptor-fields descriptor)
                   for attnum from 1
                   collect (make-pg-column :name column-name :type type :typmod -1
                                           :attnum attnum :notnull notnull))
             'vector))))

(defun sql-publication-rows ()
  "Snapshot the publication catalog in C column order."
  (with-write-lock
    (let* ((database (current-database))
           (publications (loop for pub being the hash-values of (database-publications database)
                               collect pub)))
      (loop for publication in (sort publications #'string< :key #'pg-publication-name)
            for actions = (pg-publication-publish-actions publication)
            collect (vector
                     (or (pg-publication-oid publication) +null+)
                     (pg-publication-name publication)
                     +bootstrap-role-oid+
                     (not (null (pg-publication-all-tables-p publication)))
                     (eq (pg-publication-sequences publication) :all)
                     (not (null (member :insert actions)))
                     (not (null (member :update actions)))
                     (not (null (member :delete actions)))
                     (not (null (member :truncate actions)))
                     ;; Unsupported partition/generated-column options cannot be set.
                     nil (char-code #\n))))))

(defun lookup-sql-system-relation (name)
  "Return read-only supported catalog columns from their shared descriptor."
  (let ((entry (sql-system-relation-definition name)))
    (when entry
      (make-sql-system-relation entry))))

(defun lookup-sql-relation (reference)
  "Resolve SQL schema qualification and search order without changing raw lookup."
  (let* ((name (sql-relation-base-name reference))
         (schemas (if (sql-relation-name-p reference)
                      (list (sql-relation-name-schema reference))
                      (effective-sql-search-path))))
    (loop for schema in schemas
          for table = (cond
                        ((string= schema "pg_catalog")
                         (lookup-sql-system-relation name))
                        ((string= schema "public")
                         (lookup-table-by-name name)))
          when table return table)))

(defun sql-relation-or-error (reference)
  "Resolve an existing SQL relation, reporting its original identifier."
  (or (lookup-sql-relation reference)
      (ereport "42P01" "relation \"~a\" does not exist"
               (if (sql-relation-name-p reference)
                   (format nil "~a.~a" (sql-relation-name-schema reference)
                           (sql-relation-name-name reference))
                   reference))))

(defun lookup-sql-user-relation (name)
  "Resolve a writable relation, preserving missing-object handling for IF EXISTS."
  (let ((table (lookup-sql-relation name)))
    (when (and table (sql-system-relation-p table))
      (ereport "0A000" "operation on read-only catalog relation is not supported"))
    table))

(defun resolve-table-or-error (name)
  (or (lookup-sql-user-relation name)
      (ereport "42P01" "relation \"~a\" does not exist" name)))

(defun resolve-column-or-error (table name)
  (let ((col (find-column table name)))
    (unless col
      (ereport "42703" "column \"~a\" does not exist" name))
    col))

(defun current-schema-name ()
  (or (first (explicit-sql-search-path)) +null+))

(defun current-user-name ()
  (if *backend* (backend-startup-user *backend*) "postgres"))
