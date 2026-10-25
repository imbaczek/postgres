;;;; namespace.lisp -- name resolution helpers.

(in-package #:pg)

(defstruct sql-relation-name schema name)

(defstruct sql-derived-relation query)

(defstruct sql-record fields values)

(defparameter +publication-table-result-fields+
  '(("pubid" :oid) ("relid" :oid) ("attrs" :int2vector) ("qual" :pg-node-tree)))

(defun publication-table-result-rows (names &optional (relation-id nil relation-p))
  "Match the variadic SRF and C20's missing-publication-tolerant relation overload."
  (when (or (eq names +null+) (and relation-p (eq relation-id +null+)))
    (return-from publication-table-result-rows nil))
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

(defun search-path-identifiers (text)
  "Follow C SplitIdentifierString: whitespace/comma delimiters and doubled quotes."
  (let ((position 0)
        (end (length text))
        (names nil))
    (labels ((space-p (character) (member (char-code character) '(9 10 11 12 13 32)))
             (skip-space ()
               (loop while (and (< position end) (space-p (char text position)))
                     do (incf position)))
             (invalid ()
               (ereport "22023" "invalid value for parameter \"search_path\": \"~a\"" text))
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

(defun set-session-search-path (value)
  "Validate, save transaction undo state, and report the new session search path."
  (let ((text (if (eq value +null+) "\"$user\", public" value)))
    (parse-sql-search-path text)
    (when (and (eq (backend-state *backend*) :in-progress)
               (null (backend-settings-transaction-start *backend*)))
      (setf (backend-settings-transaction-start *backend*)
            (copy-session-settings (backend-settings *backend*))))
    (unless (string= text (session-search-path))
      (setf (gethash "search_path" (backend-settings *backend*)) text)
      (when (backend-stream *backend*)
        (send-parameter-status (backend-stream *backend*) "search_path" text)))
    text))

(defun set-config-value (name value local-p)
  "Implement supported set_config semantics; reject unsupported GUCs and LOCAL."
  (when (eq name +null+)
    (ereport "22004" "SET requires parameter name"))
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

(defparameter +sql-system-relations+
  ;; Catalog OIDs and original attribute numbers follow the pinned C tree.
  ;; The sequence view OID is from that tree's unmodified initdb catalog.
  (list
   (list :oid 1259 :name "pg_class" :kind #\r :rows 'sql-class-rows
         :fields '(("oid" :oid t 1) ("relname" :name t 2)
                   ("relnamespace" :oid t 3) ("relkind" :char t 18)
                   ("relnatts" :int2 t 19) ("relreplident" :char t 27)))
   (list :oid 1249 :name "pg_attribute" :kind #\r :rows 'sql-attribute-rows
         :fields '(("attrelid" :oid t 1) ("attname" :name t 2)
                   ("atttypid" :oid t 3) ("attnum" :int2 t 5)
                   ("atttypmod" :int4 t 6) ("attnotnull" :bool t 12)
                   ("attgenerated" :char t 16) ("attisdropped" :bool t 17)))
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
         :fields '(("slot_name" :name nil 1) ("active" :bool nil 7)
                   ("restart_lsn" :pg-lsn nil 11) ("confirmed_flush_lsn" :pg-lsn nil 12)
                   ("wal_status" :text nil 13) ("retained_wal_bytes" :int8 t 23)))))

(defun sql-system-relation-definition (identity)
  (find identity +sql-system-relations+
        :key (if (stringp identity) (lambda (entry) (getf entry :name))
                 (lambda (entry) (getf entry :oid)))
        :test (if (stringp identity) #'string= #'eql)))

(defun sql-system-row-source (table)
  (getf (sql-system-relation-definition (pg-class-oid table)) :rows))

(defun sql-namespace-rows ()
  ;; These are the two namespaces implemented by SQL relation resolution.
  (list (vector 11 "pg_catalog") (vector 2200 "public")))

(defun sql-replication-slot-row (row)
  "Adapt the extension WAL snapshot to typed SQL values and catalog column order."
  (flet ((lsn (slot)
           (let ((value (aref row slot)))
             (if (eq value +null+) value (sql-pg-lsn-input value)))))
    (vector (aref row 0) (aref row 1) (lsn 2) (lsn 3) (aref row 5) (aref row 4))))

(defun sql-replication-slot-rows ()
  (mapcar #'sql-replication-slot-row (wal-replication-slot-rows)))

(defun sql-class-rows ()
  "Snapshot implemented catalog relations and persisted user objects."
  (with-write-lock
    (let ((database (current-database))
          (rows (loop for entry in +sql-system-relations+
                      collect (vector (getf entry :oid) (getf entry :name) 11
                                      (char-code (getf entry :kind))
                                      (length (getf entry :fields)) (char-code #\d)))))
      (labels ((add-object (oid name kind columns)
                 (push (vector oid name 2200 (char-code kind) columns (char-code #\d)) rows)))
        (maphash (lambda (name table)
                   (add-object (pg-class-oid table) name #\r (length (pg-class-columns table))))
                 (database-tables database))
        (maphash (lambda (name index)
                   (add-object (pg-index-oid index) name #\i (length (pg-index-columns index))))
                 (database-indexes database))
        (maphash (lambda (name sequence)
                   (add-object (pg-sequence-oid sequence) name #\S 3))
                 (database-sequences database)))
      (sort rows #'< :key (lambda (row) (aref row 0))))))

(defun sql-attribute-row (oid name type number typmod notnull)
  (vector oid name (type-oid type) number (or typmod -1) notnull 0 nil))

(defun sql-attribute-rows ()
  "Use dense local virtual slots; outward origins retain their separate C Anums."
  (with-write-lock
    (let ((database (current-database)) (rows nil))
      (dolist (entry +sql-system-relations+)
        (loop for field in (getf entry :fields) for number from 1
              do (push (sql-attribute-row (getf entry :oid) (first field) (second field)
                                          number -1 (third field)) rows)))
      (maphash
       (lambda (name table)
         (declare (ignore name))
         (loop for column across (pg-class-columns table)
               do (push (sql-attribute-row
                         (pg-class-oid table) (pg-column-name column) (pg-column-type column)
                         (pg-column-attnum column) (pg-column-typmod column)
                         (pg-column-notnull column)) rows)))
       (database-tables database))
      (maphash
       (lambda (name index)
         (declare (ignore name))
         (let ((table (find-table-by-oid (pg-index-table-oid index))))
           (loop for attnum in (pg-index-columns index) for number from 1
                 for column = (aref (pg-class-columns table) (1- attnum))
                 do (push (sql-attribute-row
                           (pg-index-oid index) (pg-column-name column) (pg-column-type column)
                           number (pg-column-typmod column) nil) rows))))
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
    (loop for index being the hash-values of (database-indexes (current-database))
          collect (vector (pg-index-oid index) (pg-index-table-oid index)
                          (pg-index-unique-p index) (pg-index-primary-p index)
                          (pg-index-valid-p index) (coerce (pg-index-columns index) 'vector)
                          (length (pg-index-columns index))))))

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
