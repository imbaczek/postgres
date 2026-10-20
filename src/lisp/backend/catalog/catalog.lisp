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
  columns           ; simple-vector of pg-column, attnum order
  indexes)          ; list of index OIDs

(defstruct pg-index
  oid
  name
  relfilenode
  table-oid
  columns           ; list of attnums
  unique-p
  primary-p
  constraint-p      ; a table constraint depends on this index
  nulls-not-distinct-p)

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

(define-catalog-codec class make-pg-class
  (:oid pg-class-oid :oid)
  (:name pg-class-name :name)
  (:relfilenode pg-class-relfilenode :relfilenode)
  (:columns pg-class-columns :columns
            :encode (lambda (columns)
                      (loop for col across columns collect (column-to-sexp col)))
            :decode (lambda (columns)
                      (coerce (mapcar #'column-from-sexp columns) 'vector)))
  (:indexes pg-class-indexes :indexes))

(define-catalog-codec index make-pg-index
  (:oid pg-index-oid :oid)
  (:name pg-index-name :name)
  (:relfilenode pg-index-relfilenode :relfilenode)
  (:table-oid pg-index-table-oid :table-oid)
  (:columns pg-index-columns :columns)
  (:unique pg-index-unique-p :unique-p)
  (:primary pg-index-primary-p :primary-p)
  (:constraint pg-index-constraint-p :constraint-p)
  (:nulls-not-distinct pg-index-nulls-not-distinct-p :nulls-not-distinct-p))

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

(defun save-catalog ()
  (ensure-directories-exist (catalog-path))
  (let ((tmp (pgdata-path "global/pg_catalog.tmp")))
    (with-open-file (s tmp
                       :direction :output
                       :if-exists :supersede
                       :if-does-not-exist :create
                       :element-type 'character)
      (with-standard-io-syntax
        (let ((*print-readably* t)
              (*print-pretty* t))
          (prin1 (catalog-to-sexp *catalog*) s)
          (terpri s)))
      (finish-output s)
      (sb-posix:fsync (sb-sys:fd-stream-fd s)))
    ;; RENAME(2) atomically replaces the destination.  Deleting the old
    ;; catalog first would open a window in which neither name exists, so a
    ;; crash there would lose the whole catalog.
    (sb-posix:rename (namestring tmp) (namestring (catalog-path)))
    (fsync-directory (pgdata-path "global/")))
  (values))

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
  (values))

(defun freeze-all-heap-xids (old-epoch
                             &key (fsync-function #'smgr-fsync)
                               after-page-function)
  "Freeze tuple XIDs from OLD-EPOCH in every catalogued heap relation."
  (maphash
   (lambda (db-name db)
     (declare (ignore db-name))
     (maphash
      (lambda (table-name table)
        (declare (ignore table-name))
        (heapam-freeze-relation-xids (pg-class-relfilenode table)
                                     (pg-class-columns table)
                                     old-epoch
                                     :fsync-function fsync-function
                                     :after-page-function
                                     after-page-function))
      (database-tables db)))
   (catalog-databases *catalog*))
  (values))

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

(defun create-index* (db name table cols
                      &key unique primary constraint nulls-not-distinct)
  (check-new-relation-name db name)
  (let* ((oid (allocate-oid))
         (idx (make-pg-index :oid oid
                             :name name
                             :relfilenode oid
                             :table-oid (pg-class-oid table)
                             :columns cols
                             :unique-p unique
                             :primary-p primary
                             :constraint-p constraint
                             :nulls-not-distinct-p nulls-not-distinct)))
    (btree-init oid)
    (setf (gethash name (database-indexes db)) idx)
    (push oid (pg-class-indexes table))
    idx))

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
                      (create-index* db
                                     (generate-index-name db name "pkey")
                                     class (list (pg-column-attnum col))
                                     :unique t :primary t :constraint t))
                     ((getf def :unique)
                      (create-index* db
                                     (generate-index-name
                                      db name
                                      (format nil "~a_key"
                                              (pg-column-name col)))
                                     class (list (pg-column-attnum col))
                                     :unique t
                                     :constraint t
                                     :nulls-not-distinct
                                     (getf def :nulls-not-distinct)))))
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
          (smgr-forget (pg-index-relfilenode idx) :delete t))))
    (remhash name (database-tables db))
    (smgr-forget (pg-class-relfilenode class) :delete t)
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
    (smgr-forget (pg-index-relfilenode idx) :delete t)
    (values)))
