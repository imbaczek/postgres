;;;; namespace.lisp -- name resolution helpers.

(in-package #:pg)

(defstruct sql-relation-name schema name)

(defstruct sql-derived-relation query)

(defstruct sql-record fields values)

(defparameter +physical-slot-create-result-fields+
  '(("slot_name" :name) ("lsn" :pg-lsn)))

(defparameter +physical-slot-advance-result-fields+
  '(("slot_name" :name) ("end_lsn" :pg-lsn)))

(defstruct sql-array
  ;; Elements are flat in row-major order and may contain +null+. Dimensions
  ;; and lower bounds have equal lengths; their dimension product is the element
  ;; count. Empty arrays use an empty vector and NIL for both lists.
  element-type-id
  elements
  dimensions
  lower-bounds)

(defparameter +sql-array-types+
  '((:text . :text-array) (:bytea . :bytea-array) (:json . :json-array) (:jsonb . :jsonb-array)
    (:int2 . :int2-array) (:int4 . :int4-array)
    (:int8 . :int8-array) (:numeric . :numeric-array) (:float4 . :float4-array)
    (:float8 . :float8-array) (:bool . :bool-array) (:char . :char-array)
    (:name . :name-array) (:varchar . :varchar-array) (:oid . :oid-array)
    (:date . :date-array) (:time . :time-array) (:timetz . :timetz-array)
    (:timestamp . :timestamp-array) (:timestamptz . :timestamptz-array)
    (:interval . :interval-array) (:pg-lsn . :pg-lsn-array) (:bit . :bit-array)
    (:int2vector . :int2vector-array)))

(defun sql-array-type (element-type)
  (cdr (assoc element-type +sql-array-types+)))

(defun sql-array-element-type (array-type)
  (car (rassoc array-type +sql-array-types+)))

(defconstant +max-varlena-array-elements+ (floor +max-query-message-size+ 8)
  "Limit the resident element vector for these arrays to the query frame budget.")

(defvar *sql-varlena-array-element-reserver* nil
  "Optional aggregate element-budget callback, called before array allocations.")

(defun sql-varlena-array-element-oid (type)
  "Return the PostgreSQL element OID for supported variable-width arrays."
  (ecase type (:bytea 17) (:json 114) (:jsonb 3802)))

(defun sql-varlena-array-element-octets (value type)
  (ecase type
    (:bytea (check-type value (vector (unsigned-byte 8))) value)
    (:json (sql-json-input value) (string->bytes value))
    (:jsonb (sql-jsonb-to-octets value))))

(defun sql-varlena-array-encode (array element-type)
  "Encode an ArrayType payload, excluding its outer four-byte varlena header."
  (unless (and (sql-array-p array) (eq (sql-array-element-type-id array) element-type))
    (error "invalid array element type"))
  (let* ((dimensions (sql-array-dimensions array))
         (bounds (sql-array-lower-bounds array))
         (items (sql-array-elements array))
         (ndim (length dimensions))
         (count (length items))
         (has-null nil)
         (data-start 0)
         (cursor 0)
         (encoded nil))
    (when (> count +max-varlena-array-elements+)
      (ereport "54000" "array element count exceeds supported maximum"))
    (unless (and (<= 0 ndim 6) (= ndim (length bounds))
                 (if (zerop ndim) (zerop count)
                     (and (every (lambda (dimension) (typep dimension '(integer 1 #x7fffffff))) dimensions)
                          (= count (reduce #'* dimensions :initial-value 1))))
                 (loop for lower in bounds for dimension in dimensions
                       always (and (typep lower '(signed-byte 32))
                                   (<= (+ lower dimension -1) #x7fffffff))))
      (error "invalid array dimensions"))
    (when *sql-varlena-array-element-reserver*
      (funcall *sql-varlena-array-element-reserver* count))
    (setf has-null (loop for item across items for index from 0
                         do (when (zerop (mod index 4096)) (check-for-interrupts))
                           thereis (eq item +null+))
          data-start (maxalign (+ 16 (* 8 ndim) (if has-null (ceiling count 8) 0)))
          cursor (- data-start 4)
          encoded (make-array count :initial-element nil))
    (loop for item across items for index from 0
          do (when (zerop (mod index 4096)) (check-for-interrupts))
          unless (eq item +null+) do
            (let ((bytes (sql-varlena-array-element-octets item element-type)))
              (setf (aref encoded index) bytes
                    cursor (+ cursor (align-up (+ 4 (length bytes)) 4)))
              (when (> (+ cursor 4) #x3fffffff)
                (ereport "54000" "array value exceeds maximum size"))))
    (let ((result (make-array cursor :element-type '(unsigned-byte 8) :initial-element 0)))
      (page-set-u32 result 0 ndim)
      (page-set-u32 result 4 (if has-null data-start 0))
      (page-set-u32 result 8 (sql-varlena-array-element-oid element-type))
      (loop for dimension in dimensions for i from 0
            do (page-set-u32 result (+ 12 (* i 4)) dimension))
      (loop for lower in bounds for i from 0
            do (page-set-u32 result (+ 12 (* ndim 4) (* i 4)) (logand lower #xffffffff)))
      (let ((position (- data-start 4)) (bitmap (+ 12 (* 8 ndim))))
        (loop for item across items for bytes across encoded for index from 0
              do (when (zerop (mod index 4096)) (check-for-interrupts))
              unless (eq item +null+) do
                (when has-null
                  (setf (aref result (+ bitmap (floor index 8)))
                        (logior (aref result (+ bitmap (floor index 8))) (ash 1 (mod index 8)))))
                (setf position (align-up position 4))
                (page-set-u32 result position (ash (+ 4 (length bytes)) 2))
                (replace result bytes :start1 (+ position 4))
                (incf position (align-up (+ 4 (length bytes)) 4))))
      result)))

(defun sql-varlena-array-decode (bytes element-type)
  "Decode a PostgreSQL ArrayType payload, validating all bounds and elements."
  (labels ((bad () (ereport "XX001" "invalid stored array")))
    (unless (and (typep bytes '(vector (unsigned-byte 8))) (>= (length bytes) 12))
      (bad))
    (let* ((length (length bytes))
           (ndim (page-u32 bytes 0))
           (offset (page-u32 bytes 4))
           (oid (page-u32 bytes 8)))
      (unless (and (<= ndim 6) (= oid (sql-varlena-array-element-oid element-type))
                   (<= (+ 12 (* ndim 8)) length))
        (bad))
      (let ((dimensions nil) (bounds nil) (count (if (zerop ndim) 0 1)))
        (dotimes (i ndim)
          (let* ((dimension (page-u32 bytes (+ 12 (* i 4))))
                 (raw-bound (page-u32 bytes (+ 12 (* ndim 4) (* i 4))))
                 (lower (if (logbitp 31 raw-bound) (- raw-bound #x100000000) raw-bound)))
            (unless (and (<= 1 dimension #x7fffffff)
                         (<= (+ lower dimension -1) #x7fffffff))
              (bad))
            (setf count (* count dimension))
            (push dimension dimensions)
            (push lower bounds)))
        (let* ((has-null (plusp offset))
               (header (+ 16 (* ndim 8)))
               (minimum (maxalign (+ header (if has-null (ceiling count 8) 0))))
               (start (if has-null offset (maxalign header)))
               (bitmap (+ 12 (* ndim 8))))
          (unless (and (>= start minimum) (zerop (mod start 8)) (<= start (+ length 4)))
            (bad))
          (when (and (not has-null) (> count (floor (- length (- start 4)) 4)))
            (bad))
          (when (> count +max-varlena-array-elements+)
            (ereport "54000" "array element count exceeds supported maximum"))
          ;; Each non-null varlena consumes at least four aligned bytes.
          ;; Reject impossible counts before allocating the element vector.
          (let ((non-null-count
                  (if has-null
                      (loop for index below (ceiling count 8)
                            for remaining = (- count (* index 8))
                            for byte = (aref bytes (+ bitmap index))
                            do (when (zerop (mod index 4096)) (check-for-interrupts))
                            sum (logcount (if (< remaining 8)
                                              (logand byte (1- (ash 1 remaining))) byte)))
                      count)))
            (when (> non-null-count (floor (- length (- start 4)) 4)) (bad)))
          (when *sql-varlena-array-element-reserver*
            (funcall *sql-varlena-array-element-reserver* count))
          (let ((values (make-array count :initial-element +null+))
                (position (- start 4)))
            (dotimes (i count)
              (when (zerop (mod i 4096)) (check-for-interrupts))
              (when (or (not has-null)
                        (logbitp (mod i 8) (aref bytes (+ bitmap (floor i 8)))))
                (when (>= position length) (bad))
                (let* ((first (aref bytes position))
                       (short-p (logbitp 0 first))
                       (size (if short-p (ash first -1)
                                 (progn
                                   (when (> (+ position 4) length) (bad))
                                   (let ((word (page-u32 bytes position)))
                                     (unless (zerop (logand word 3)) (bad))
                                     (ash word -2)))))
                       (header-size (if short-p 1 4)))
                  (when (or (< size header-size) (= first 1)
                            (> (+ position size) length))
                    (bad))
                  (let ((payload (subseq bytes (+ position header-size) (+ position size))))
                    (setf (aref values i)
                          (ecase element-type
                            (:bytea payload)
                            (:json (handler-case
                                       (let ((text (bytes->string payload)))
                                         (sql-json-input text) text)
                                     (sb-int:character-decoding-error () (bad))
                                     (pg-error (condition)
                                       (if (or (and (>= (length (pg-error-sqlstate condition)) 2)
                                                    (string= "22" (pg-error-sqlstate condition) :end2 2))
                                               (string= "54001" (pg-error-sqlstate condition)))
                                           (bad) (error condition)))))
                            (:jsonb (sql-jsonb-from-octets payload)))))
                  (incf position (align-up size 4))
                  (when (> position length) (bad)))))
            (unless (= position length) (bad))
            (make-sql-array :element-type-id element-type :elements values
                            :dimensions (nreverse dimensions) :lower-bounds (nreverse bounds))))))))

(defparameter +publication-table-result-fields+
  '(("pubid" :oid) ("relid" :oid) ("attrs" :int2vector) ("qual" :pg-node-tree)))

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
  "Set a supported session value, including transaction-local application_name.
Reject unsupported parameters and unsupported local settings."
  (when (eq name +null+)
    (ereport "22004" "SET requires parameter name"))
  (startup-check-session-setting (string-downcase name))
  (when (string-equal name "application_name")
    (return-from set-config-value
      (session-set-application-name value (and local-p (not (eq local-p +null+))))))
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

(defparameter +sql-system-relations+
  ;; Catalog OIDs and original attribute numbers follow the pinned C tree.
  ;; The sequence view OID is from that tree's unmodified initdb catalog.
  (list
   (list :oid 12109 :name "pg_settings" :kind #\v
         :rows 'startup-settings-rows :fields +sql-settings-fields+)
   (list :oid 1262 :name "pg_database" :kind #\r :rows 'sql-database-rows
         :fields '(("oid" :oid t 1) ("datname" :name t 2)))
   (list :oid 1259 :name "pg_class" :kind #\r :rows 'sql-class-rows
         :fields '(("oid" :oid t 1) ("relname" :name t 2)
                   ("relnamespace" :oid t 3) ("relkind" :char t 18)
                   ("relnatts" :int2 t 19) ("relreplident" :char t 27)
                   ("reltoastrelid" :oid t 14) ("relispartition" :bool t 28)))
   (list :oid 1249 :name "pg_attribute" :kind #\r :rows 'sql-attribute-rows
         :fields '(("attrelid" :oid t 1) ("attname" :name t 2)
                   ("atttypid" :oid t 3) ("attnum" :int2 t 5)
                   ("atttypmod" :int4 t 6) ("attnotnull" :bool t 12)
                   ("attgenerated" :char t 16) ("attisdropped" :bool t 17)
                   ("attstorage" :char t 10)))
   (list :oid 1247 :name "pg_type" :kind #\r :rows 'sql-type-rows
         :fields '(("oid" :oid t 1) ("typname" :name t 2)
                   ("typnamespace" :oid t 3) ("typlen" :int2 t 5)
                   ("typalign" :char t 23) ("typstorage" :char t 24)))
   (list :oid 2610 :name "pg_index" :kind #\r :rows 'sql-index-rows
         :fields '(("indexrelid" :oid t 1) ("indrelid" :oid t 2)
                   ("indisunique" :bool t 5) ("indisprimary" :bool t 7)
                   ("indisvalid" :bool t 11) ("indkey" :int2vector t 16)
                   ("indnkeyatts" :int2 t 4)))
   (list :oid 2615 :name "pg_namespace" :kind #\r :rows 'sql-namespace-rows
         :fields '(("oid" :oid t 1) ("nspname" :name t 2)))
   (list :oid 6104 :name "pg_publication" :kind #\r :rows 'sql-publication-rows
         :fields +sql-publication-fields+)
   (list :oid 12073 :name "pg_publication_sequences" :kind #\v
         :rows 'sql-publication-sequence-rows
         :fields '(("pubname" :name nil 1) ("schemaname" :name nil 2)
                   ("sequencename" :name nil 3)))
   (list :oid 12283 :name "pg_replication_slots" :kind #\v
         :rows 'sql-replication-slot-rows
         :fields +sql-replication-slot-fields+)))

(defun sql-system-relation-definition (identity)
  (find identity +sql-system-relations+
        :key (if (stringp identity) (lambda (entry) (getf entry :name))
                 (lambda (entry) (getf entry :oid)))
        :test (if (stringp identity) #'string= #'eql)))

(defun sql-system-row-source (table)
  (getf (sql-system-relation-definition (pg-class-oid table)) :rows))

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

(defun sql-public-replication-slot-row (slot)
  "Build a public view row while the caller holds write and WAL locks.
Lost slots expose NULL restart while retaining their saved floor internally.
The retained-byte extension measures from the durable or flushed endpoint."
  (let* ((physical (wal-physical-slot-p slot))
         (restart (wal-replication-slot-restart-lsn slot))
         (lost (eq :lost (wal-replication-slot-wal-status slot)))
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
              +null+ +null+ (if lost +null+ (nullable restart))
              (nullable (wal-replication-slot-confirmed-flush-lsn slot))
              (wal-slot-catalog-status slot)
              (wal-slot-safe-wal-bytes slot)
              nil +null+ (if (zerop inactive) +null+ inactive)
              (if physical +null+ nil) (if lost "wal_removed" +null+)
              nil nil +null+ (if (or lost (null restart)) 0 (max 0 (- durable restart)))))))

(defun sql-replication-slot-rows ()
  "Snapshot both slot kinds under write-before-WAL locking."
  (with-write-lock
    (sb-thread:with-mutex (*wal-lock*)
      (loop for slot being the hash-values of *wal-replication-slots*
            collect (sql-public-replication-slot-row slot)))))

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
                      collect (vector (getf entry :oid) (getf entry :name) 11
                                      (char-code (getf entry :kind))
                                      (reduce #'max (getf entry :fields) :key #'fourth)
                                      (char-code #\d) 0 nil))))
      (labels ((add-object (oid name kind columns &optional (toast-oid 0) (namespace 2200) (identity #\d))
                 (push (vector oid name namespace (char-code kind) columns
                               (char-code identity) toast-oid nil) rows)))
        (maphash (lambda (name table)
                   (let* ((toast (pg-class-toast table))
                          (parent-oid (pg-class-oid table)))
                     (add-object parent-oid name #\r (length (pg-class-columns table))
                                 (if toast (pg-toast-oid toast) 0))
                     (when toast
                       (let ((toast-name (format nil "pg_toast_~d" parent-oid)))
                         (add-object (pg-toast-oid toast) toast-name #\t 3 0 99 #\n)
                         (add-object (pg-toast-index-oid toast)
                                     (concatenate 'string toast-name "_index") #\i 2 0 99 #\n)))))
                 (database-tables database))
        (maphash (lambda (name index)
                   (add-object (pg-index-oid index) name #\i (length (pg-index-columns index))))
                 (database-indexes database))
        (maphash (lambda (name sequence)
                   (add-object (pg-sequence-oid sequence) name #\S 3))
                 (database-sequences database)))
      (sort rows #'< :key (lambda (row) (aref row 0))))))

(defun sql-type-storage-code (type)
  "Return PostgreSQL's default typstorage code for a supported TYPE."
  (cond ((eq type :numeric) #\m)
        ((or (sql-array-element-type type)
             (member type '(:text :varchar :bytea :json :jsonb :bit :pg-node-tree :record
                            :bytea-array :json-array :jsonb-array)))
         #\x)
        (t #\p)))

(defun sql-type-catalog-name (type)
  "Return PostgreSQL's internal typname for a supported Lisp type identifier."
  (let ((element (sql-array-element-type type)))
    (if element
        (concatenate 'string "_" (sql-type-catalog-name element))
        (substitute #\_ #\- (string-downcase (symbol-name type))))))

(defun sql-type-alignment-code (type)
  "Return the native typalign code for a supported type."
  (let ((element (sql-array-element-type type)))
    (cond
      (element (if (member element '(:int8 :float8 :time :timetz :timestamp
                                     :timestamptz :interval :pg-lsn))
                   #\d #\i))
      ((member type '(:bool :char :name)) #\c)
      ((eq type :int2) #\s)
      ((member type '(:int8 :float8 :time :timetz :timestamp :timestamptz
                      :interval :pg-lsn :record)) #\d)
      (t #\i))))

(defun sql-type-rows ()
  "Expose metadata for the type identifiers recognized by SQL expression analysis."
  (loop for (type oid length) in (symbol-value '+sql-expression-types+)
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
        (loop for field in (getf entry :fields)
              do (push (sql-attribute-row (getf entry :oid) (first field) (second field)
                                          (fourth field) -1 (third field)) rows)))
      (maphash
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
       (database-tables database))
      (maphash
       (lambda (name index)
         (declare (ignore name))
         (let ((table (find-table-by-oid (pg-index-table-oid index))))
           (loop for attnum in (pg-index-columns index) for number from 1
                 for column = (aref (pg-class-columns table) (1- attnum))
                 do (push (sql-attribute-row
                           (pg-index-oid index) (pg-column-name column) (pg-column-type column)
                           number (pg-column-typmod column) nil (pg-column-effective-storage column)) rows))))
       (database-indexes database))
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
      (maphash (lambda (name index)
                 (declare (ignore name))
                 (push (vector (pg-index-oid index) (pg-index-table-oid index)
                               (pg-index-unique-p index) (pg-index-primary-p index)
                               (pg-index-valid-p index) (coerce (pg-index-columns index) 'vector)
                               (length (pg-index-columns index))) rows))
               (database-indexes database))
      (maphash (lambda (name table)
                 (declare (ignore name))
                 (let ((toast (pg-class-toast table)))
                   (when toast
                     (push (vector (pg-toast-index-oid toast) (pg-toast-oid toast)
                                   t t t #(1 2) 2) rows))))
               (database-tables database))
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
  (let ((fields (getf (sql-system-relation-definition table-oid) :fields)))
    (if (and fields slot (<= 1 slot (length fields)))
        (fourth (nth (1- slot) fields))
        slot)))

(defun make-sql-system-relation (oid name fields)
  "Construct read-only catalog metadata with consistent column numbering."
  (make-pg-class
   :oid oid :name name :relfilenode (1- oid)
   :columns
   (coerce (loop for (column-name type notnull) in fields
                 for attnum from 1
                 collect (make-pg-column :name column-name :type type :typmod -1
                                         :attnum attnum :notnull notnull))
           'vector)))

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
                     ;; The implemented role model has one owner, bootstrap superuser 10.
                     10
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
      (make-sql-system-relation (getf entry :oid) (getf entry :name) (getf entry :fields)))))

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
                         (gethash name (database-tables (current-database)))))
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

(defun current-user-name () "postgres")
