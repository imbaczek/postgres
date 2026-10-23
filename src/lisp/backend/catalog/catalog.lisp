;;;; catalog.lisp -- pg_class / pg_attribute / pg_index as defstructs,
;;;; persisted to $PGDATA/global/pg_catalog.dat.

(in-package #:pg)

(defstruct pg-column
  name
  type              ; :int4 | :int8 | :varchar
  typmod            ; n+4 for varchar(n), else -1
  attnum
  notnull)

(defstruct pg-class
  oid
  name
  relfilenode
  (relfrozenxid +first-normal-xid+)
  columns           ; simple-vector of pg-column, attnum order
  indexes           ; list of index OIDs
  statistics)       ; derived ANALYZE estimates; raw samples remain transient

(defstruct pg-index
  oid
  name
  relfilenode
  table-oid
  columns           ; list of attnums
  unique-p
  primary-p
  constraint-p      ; a table constraint depends on this index
  nulls-not-distinct-p
  valid-p)           ; false for legacy indexes that may be incomplete

(defstruct pg-column-statistics
  null-fraction
  distinct-count
  most-common-values)

(defstruct pg-table-statistics
  row-count
  sample-count
  columns)

(defun pg-index-constraint-owned-p (index)
  "Return whether a table constraint depends on INDEX.

Older catalogs have no CONSTRAINT field, but did persist PRIMARY."
  (or (pg-index-constraint-p index)
      (pg-index-primary-p index)))

(defstruct database
  name
  oid
  (tables (make-hash-table :test #'equal))
  (indexes (make-hash-table :test #'equal)))

(defstruct catalog
  (next-oid +first-normal-object-id+)
  (databases (make-hash-table :test #'equal)))

(defvar *catalog* nil)
(defvar *catalog-mutex* (sb-thread:make-mutex :name "pg catalog mutex"))
(defvar *catalog-oldest-relfrozenxid* nil)
;; Incremented only after a catalog image has been durably committed.
(defvar *catalog-durable-generation* 0)

;;; ---- serialization ----

(defmacro define-catalog-codec (name constructor &body fields)
  "Define NAME-TO-SEXP and NAME-FROM-SEXP from explicit field mappings.

Each FIELD is (SEXP-KEY ACCESSOR CONSTRUCTOR-KEY &key ENCODE DECODE)."
  (let ((to-sexp (intern (format nil "~a-TO-SEXP" name) *package*))
        (from-sexp (intern (format nil "~a-FROM-SEXP" name) *package*)))
    `(progn
       (defun ,to-sexp (object)
         (list
          ,@(loop for field in fields
                  append
                  (destructuring-bind (key accessor constructor-key
                                       &key encode decode)
                      field
                    (declare (ignore constructor-key decode))
                    `(,key
                      ,(if encode
                           `(funcall ,encode (,accessor object))
                           `(,accessor object)))))))
       (defun ,from-sexp (sexp)
         (,constructor
          ,@(loop for field in fields
                  append
                  (destructuring-bind (key accessor constructor-key
                                       &key encode decode)
                      field
                    (declare (ignore accessor encode))
                    `(,constructor-key
                      ,(if decode
                           `(funcall ,decode (getf sexp ,key))
                           `(getf sexp ,key))))))))))

(define-catalog-codec column make-pg-column
  (:name pg-column-name :name)
  (:type pg-column-type :type)
  (:typmod pg-column-typmod :typmod)
  (:attnum pg-column-attnum :attnum)
  (:notnull pg-column-notnull :notnull))

(define-catalog-codec column-statistics make-pg-column-statistics
  (:null-fraction pg-column-statistics-null-fraction :null-fraction)
  (:distinct-count pg-column-statistics-distinct-count :distinct-count)
  (:most-common-values pg-column-statistics-most-common-values
   :most-common-values))

(define-catalog-codec table-statistics make-pg-table-statistics
  (:row-count pg-table-statistics-row-count :row-count)
  (:sample-count pg-table-statistics-sample-count :sample-count)
  (:columns pg-table-statistics-columns :columns
            :encode (lambda (columns)
                      (map 'list #'column-statistics-to-sexp columns))
            :decode (lambda (columns)
                      (map 'vector #'column-statistics-from-sexp columns))))

(define-catalog-codec class make-pg-class
  (:oid pg-class-oid :oid)
  (:name pg-class-name :name)
  (:relfilenode pg-class-relfilenode :relfilenode)
  (:relfrozenxid pg-class-relfrozenxid :relfrozenxid
                 :decode (lambda (xid)
                           (or xid (xid-epoch-start *next-xid*))))
  (:columns pg-class-columns :columns
            :encode (lambda (columns)
                      (loop for col across columns collect (column-to-sexp col)))
            :decode (lambda (columns)
                      (coerce (mapcar #'column-from-sexp columns) 'vector)))
  (:indexes pg-class-indexes :indexes)
  (:statistics pg-class-statistics :statistics
               :encode (lambda (statistics)
                         (and statistics (table-statistics-to-sexp statistics)))
               :decode (lambda (statistics)
                         (and statistics
                              (table-statistics-from-sexp statistics)))))

(define-catalog-codec index make-pg-index
  (:oid pg-index-oid :oid)
  (:name pg-index-name :name)
  (:relfilenode pg-index-relfilenode :relfilenode)
  (:table-oid pg-index-table-oid :table-oid)
  (:columns pg-index-columns :columns)
  (:unique pg-index-unique-p :unique-p)
  (:primary pg-index-primary-p :primary-p)
  (:constraint pg-index-constraint-p :constraint-p)
  (:nulls-not-distinct pg-index-nulls-not-distinct-p :nulls-not-distinct-p)
  (:valid pg-index-valid-p :valid-p))

(defun database-to-sexp (db)
  `(:name ,(database-name db)
    :oid ,(database-oid db)
    :tables ,(loop for c being the hash-values of (database-tables db)
                   collect (class-to-sexp c))
    :indexes ,(loop for i being the hash-values of (database-indexes db)
                    collect (index-to-sexp i))))

(defun database-from-sexp (s)
  (let ((db (make-database :name (getf s :name) :oid (getf s :oid))))
    (dolist (cs (getf s :tables))
      (let ((c (class-from-sexp cs)))
        (setf (gethash (pg-class-name c) (database-tables db)) c)))
    (dolist (is (getf s :indexes))
      (let ((i (index-from-sexp is)))
        (setf (gethash (pg-index-name i) (database-indexes db)) i)))
    db))

(defun catalog-to-sexp (cat)
  `(:next-oid ,(catalog-next-oid cat)
    :databases ,(loop for db being the hash-values of (catalog-databases cat)
                      collect (database-to-sexp db))))

(defun catalog-from-sexp (s)
  (let ((cat (make-catalog :next-oid (getf s :next-oid))))
    (dolist (ds (getf s :databases))
      (let ((db (database-from-sexp ds)))
        (setf (gethash (database-name db) (catalog-databases cat)) db)))
    cat))

(defun catalog-path ()
  (pgdata-path "global/pg_catalog.dat"))

(defun catalog-image-bytes ()
  (string->bytes
   (with-output-to-string (out)
     (with-standard-io-syntax
       (let ((*print-readably* t)
             (*print-pretty* t))
         (prin1 (catalog-to-sexp *catalog*) out)
         (terpri out))))))

(defvar *catalog-rename-function* nil)

(defun write-catalog-image (bytes)
  (with-atomic-output (stream
                       (catalog-path)
                       (pgdata-path "global/pg_catalog.tmp")
                       (pgdata-path "global/")
                       :element-type '(unsigned-byte 8)
                       :rename-function (or *catalog-rename-function*
                                            #'sb-posix:rename))
    ;; Atomic rename keeps the old catalog present until the new image is
    ;; durable, so a crash cannot leave the catalog name missing.
    (write-sequence bytes stream)))

(defun catalog-compute-oldest-relfrozenxid ()
  (let ((oldest nil))
    (when *catalog*
      (maphash
       (lambda (database-name database)
         (declare (ignore database-name))
         (maphash
          (lambda (table-name table)
            (declare (ignore table-name))
            (let ((relfrozenxid (pg-class-relfrozenxid table)))
              (when (and relfrozenxid
                         (or (null oldest) (< relfrozenxid oldest)))
                (setf oldest relfrozenxid))))
          (database-tables database)))
       (catalog-databases *catalog*)))
    oldest))

(defun catalog-refresh-oldest-relfrozenxid ()
  (setf *catalog-oldest-relfrozenxid*
        (catalog-compute-oldest-relfrozenxid)))

(defun catalog-oldest-relfrozenxid ()
  (or *catalog-oldest-relfrozenxid*
      (catalog-compute-oldest-relfrozenxid)))

(defun save-catalog (&key deferred-relations on-record-appended)
  ;; Do fallible derived-state work before publication. Once WAL commits, the
  ;; generation update and cache assignment below must not signal: callers use
  ;; the generation to distinguish rollback-safe failures from durable ones.
  (let ((oldest (catalog-compute-oldest-relfrozenxid)))
    (wal-commit-catalog (catalog-image-bytes) #'write-catalog-image
                        :relation-deletes deferred-relations
                        :on-record-appended on-record-appended)
    (incf *catalog-durable-generation*)
    (setf *catalog-oldest-relfrozenxid* oldest)))

(defun load-catalog ()
  (let ((path (catalog-path)))
    (if (probe-file path)
        (with-open-file (s path :direction :input :if-does-not-exist nil)
          (if s
              (with-standard-io-syntax
                (let ((*read-eval* nil))
                  (catalog-from-sexp (read s nil nil))))
              (make-catalog)))
        (make-catalog))))

(defun allocate-oid ()
  (prog1 (catalog-next-oid *catalog*)
    (incf (catalog-next-oid *catalog*))))

(defun catalog-init ()
  (setf *catalog* (or (load-catalog) (make-catalog)))
  (unless (gethash "postgres" (catalog-databases *catalog*))
    (let ((db (make-database :name "postgres" :oid (allocate-oid))))
      (setf (gethash "postgres" (catalog-databases *catalog*)) db)
      (save-catalog)))
  (catalog-refresh-oldest-relfrozenxid)
  (values))

(defun freeze-all-heap-xids (old-epoch
                             &key (fsync-function #'smgr-fsync)
                               after-page-function)
  "Incrementally freeze catalog relations before reusing 32-bit XIDs.

The all-frozen visibility-map bit lets prior VACUUM work skip pages that no
longer contain transaction-status references."
  (let ((boundary (+ (* (1+ old-epoch) +xid-cycle+)
                     +first-normal-xid+)))
    (vacuum-all-relations-for-xid-wraparound
     boundary :fsync-function fsync-function
     :after-page-function after-page-function)))

(defun get-database (name)
  (or (gethash name (catalog-databases *catalog*))
      (let ((db (make-database :name name :oid (allocate-oid))))
        (setf (gethash name (catalog-databases *catalog*)) db)
        db)))

(defun current-database ()
  (get-database (backend-database *backend*)))

(defun strip-schema (name)
  (if (and (> (length name) 7)
           (string= name "public." :end1 7))
      (subseq name 7)
      name))

(defun lookup-table (name)
  (gethash (strip-schema name) (database-tables (current-database))))

(defun ensure-current-table (class)
  "Raise 42P01 when CLASS was dropped or replaced after it was resolved."
  (unless (eq class (lookup-table (pg-class-name class)))
    (ereport "42P01" "relation \"~a\" does not exist" (pg-class-name class)))
  class)

(defun lookup-index (name)
  (gethash (strip-schema name) (database-indexes (current-database))))

(defun find-column (class name)
  (loop for col across (pg-class-columns class)
        when (string= (pg-column-name col) name)
          return col))

(defun find-index-by-oid (oid)
  (loop for idx being the hash-values of (database-indexes (current-database))
        when (= (pg-index-oid idx) oid)
          return idx))

(defun find-table-by-oid (oid)
  (loop for table being the hash-values of (database-tables (current-database))
        when (= (pg-class-oid table) oid)
          return table))

(defun table-indexes (class)
  (loop for oid in (pg-class-indexes class)
        for idx = (find-index-by-oid oid)
        when idx collect idx))

(defun unique-indexes (class)
  (remove-if-not #'pg-index-unique-p (table-indexes class)))

(defun index-types (idx class)
  (loop for attnum in (pg-index-columns idx)
        collect (pg-column-type (aref (pg-class-columns class) (1- attnum)))))

(defun index-values (idx values)
  (loop for attnum in (pg-index-columns idx)
        collect (aref values (1- attnum))))

;;; ---- DDL helpers (caller holds the write lock) ----

(defun check-new-relation-name (db name)
  (when (or (gethash name (database-tables db))
            (gethash name (database-indexes db)))
    (ereport "42P07" "relation \"~a\" already exists" name)))

(defun generate-index-name (db table-name suffix)
  (let ((base (format nil "~a_~a" table-name suffix)))
    (if (or (gethash base (database-tables db))
            (gethash base (database-indexes db)))
        (loop for n from 1
              for candidate = (format nil "~a~a" base n)
              unless (or (gethash candidate (database-tables db))
                         (gethash candidate (database-indexes db)))
                return candidate)
        base)))

(defun generate-unique-index-name (db table-name table attnums)
  "Generate PostgreSQL's default unique-constraint name for ATTNUMS."
  (generate-index-name
   db table-name
   (format nil "~{~a~^_~}_key"
           (mapcar (lambda (attnum)
                     (pg-column-name
                      (aref (pg-class-columns table) (1- attnum))))
                   attnums))))

(defun make-index-object (oid name table cols
                          &key unique primary constraint nulls-not-distinct)
  "Create detached INDEX metadata with a previously reserved object ID."
  (make-pg-index :oid oid
                 :name name
                 :relfilenode oid
                 :table-oid (pg-class-oid table)
                 :columns cols
                 :unique-p unique
                 :primary-p primary
                 :constraint-p constraint
                 :nulls-not-distinct-p nulls-not-distinct))

(defun publish-index* (db index table)
  "Install a fully built INDEX in the database and its owning table."
  (check-new-relation-name db (pg-index-name index))
  (unless (= (pg-index-table-oid index) (pg-class-oid table))
    (ereport "XX000" "index \"~a\" belongs to another table"
             (pg-index-name index)))
  (setf (gethash (pg-index-name index) (database-indexes db)) index)
  (pushnew (pg-index-oid index) (pg-class-indexes table) :test #'=)
  index)

(defun create-index* (db name table cols
                      &key unique primary constraint nulls-not-distinct)
  (check-new-relation-name db name)
  (let* ((oid (allocate-oid))
         (idx (make-index-object oid name table cols
                                 :unique unique
                                 :primary primary
                                 :constraint constraint
                                 :nulls-not-distinct nulls-not-distinct)))
    (btree-init oid)
    (publish-index* db idx table)))

(defun rename-index* (db old-name new-name)
  "Rename an index relation, retaining its OID and physical index file."
  (let ((idx (gethash old-name (database-indexes db))))
    (unless idx
      (ereport "42704" "index \"~a\" does not exist" old-name))
    (unless (string= old-name new-name)
      (check-new-relation-name db new-name)
      (remhash old-name (database-indexes db))
      (setf (pg-index-name idx) new-name
            (gethash new-name (database-indexes db)) idx))
    idx))

(defun create-table* (db name columns)
  "COLUMNS is a list of plists with :name :type :typmod :notnull :primary,
:unique, and :nulls-not-distinct. Returns the new pg-class."
  (check-new-relation-name db name)
  (let* ((oid (allocate-oid))
         (cols (coerce
                (loop for def in columns
                      for i from 1
                      collect (make-pg-column
                               :name (getf def :name)
                               :type (getf def :type)
                               :typmod (getf def :typmod)
                               :attnum i
                               :notnull (getf def :notnull)))
                'vector))
         (class (make-pg-class :oid oid
                               :name name
                               :relfilenode oid
                               :relfrozenxid (next-full-xid)
                               :columns cols
                               :indexes '())))
    (setf (gethash name (database-tables db)) class)
    (handler-case
        (progn
          ;; Per-column PRIMARY KEY / UNIQUE.
          (loop for def in columns
                for col across cols
                do (cond
                     ((getf def :primary)
                      (setf (pg-column-notnull col) t)
                      (setf (pg-index-valid-p
                             (create-index*
                              db
                              (generate-index-name db name "pkey")
                              class (list (pg-column-attnum col))
                              :unique t :primary t :constraint t))
                            t))
                     ((getf def :unique)
                      (setf (pg-index-valid-p
                             (create-index*
                              db
                              (generate-index-name
                               db name
                               (format nil "~a_key"
                                       (pg-column-name col)))
                              class (list (pg-column-attnum col))
                              :unique t
                              :constraint t
                              :nulls-not-distinct
                              (getf def :nulls-not-distinct)))
                            t))))
          class)
      (error (e)
        ;; A column constraint can fail after the table and earlier indexes
        ;; have been installed.  Roll all of them back as one CREATE TABLE.
        (when (eq class (gethash name (database-tables db)))
          (drop-table* db name))
        (error e)))))

(defun drop-table* (db name)
  (let ((class (gethash name (database-tables db))))
    (unless class
      (ereport "42P01" "table \"~a\" does not exist" name))
    (dolist (oid (pg-class-indexes class))
      (let ((idx (find-index-by-oid oid)))
        (when idx
          (remhash (pg-index-name idx) (database-indexes db))
          (wal-defer-relation-delete (pg-index-relfilenode idx)))))
    (remhash name (database-tables db))
    (wal-defer-relation-delete (pg-class-relfilenode class))
    (values)))

(defun drop-index* (db name)
  (let ((idx (gethash name (database-indexes db))))
    (unless idx
      (ereport "42704" "index \"~a\" does not exist" name))
    (remhash name (database-indexes db))
    (let ((class (loop for c being the hash-values of (database-tables db)
                       when (= (pg-class-oid c) (pg-index-table-oid idx))
                         return c)))
      (when class
        (setf (pg-class-indexes class)
              (delete (pg-index-oid idx) (pg-class-indexes class)))))
    (wal-defer-relation-delete (pg-index-relfilenode idx))
    (values)))
