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
  primary-p)

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

(defun column-to-sexp (c)
  `(:name ,(pg-column-name c)
    :type ,(pg-column-type c)
    :typmod ,(pg-column-typmod c)
    :attnum ,(pg-column-attnum c)
    :notnull ,(pg-column-notnull c)))

(defun column-from-sexp (s)
  (make-pg-column :name (getf s :name)
                  :type (getf s :type)
                  :typmod (getf s :typmod)
                  :attnum (getf s :attnum)
                  :notnull (getf s :notnull)))

(defun class-to-sexp (c)
  `(:oid ,(pg-class-oid c)
    :name ,(pg-class-name c)
    :relfilenode ,(pg-class-relfilenode c)
    :columns ,(loop for col across (pg-class-columns c)
                    collect (column-to-sexp col))
    :indexes ,(pg-class-indexes c)))

(defun class-from-sexp (s)
  (make-pg-class :oid (getf s :oid)
                 :name (getf s :name)
                 :relfilenode (getf s :relfilenode)
                 :columns (coerce (mapcar #'column-from-sexp
                                          (getf s :columns))
                                  'vector)
                 :indexes (getf s :indexes)))

(defun index-to-sexp (i)
  `(:oid ,(pg-index-oid i)
    :name ,(pg-index-name i)
    :relfilenode ,(pg-index-relfilenode i)
    :table-oid ,(pg-index-table-oid i)
    :columns ,(pg-index-columns i)
    :unique ,(pg-index-unique-p i)
    :primary ,(pg-index-primary-p i)))

(defun index-from-sexp (s)
  (make-pg-index :oid (getf s :oid)
                 :name (getf s :name)
                 :relfilenode (getf s :relfilenode)
                 :table-oid (getf s :table-oid)
                 :columns (getf s :columns)
                 :unique-p (getf s :unique)
                 :primary-p (getf s :primary)))

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
    (ignore-errors (delete-file (catalog-path)))
    (rename-file tmp (catalog-path)))
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

(defun table-indexes (class)
  (loop for oid in (pg-class-indexes class)
        for idx = (find-index-by-oid oid)
        when idx collect idx))

(defun unique-indexes (class)
  (remove-if-not #'pg-index-unique-p (table-indexes class)))

(defun index-types (idx class)
  (loop for attnum in (pg-index-columns idx)
        collect (pg-column-type (aref (pg-class-columns class) (1- attnum)))))

(defun index-values (idx class values)
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
        (loop for n from 2
              for candidate = (format nil "~a~a" base n)
              unless (or (gethash candidate (database-tables db))
                         (gethash candidate (database-indexes db)))
                return candidate)
        base)))

(defun create-index* (db name table cols &key unique primary)
  (check-new-relation-name db name)
  (let* ((oid (allocate-oid))
         (idx (make-pg-index :oid oid
                             :name name
                             :relfilenode oid
                             :table-oid (pg-class-oid table)
                             :columns cols
                             :unique-p unique
                             :primary-p primary)))
    (btree-init oid)
    (setf (gethash name (database-indexes db)) idx)
    (push oid (pg-class-indexes table))
    idx))

(defun create-table* (db name columns)
  "COLUMNS is a list of plists with :name :type :typmod :notnull :primary
and :unique. Returns the new pg-class."
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
    ;; Per-column PRIMARY KEY / UNIQUE.
    (loop for def in columns
          for col across cols
          do (cond
               ((getf def :primary)
                (setf (pg-column-notnull col) t)
                (create-index* db
                               (generate-index-name db name "pkey")
                               class (list (pg-column-attnum col))
                               :unique t :primary t))
               ((getf def :unique)
                (create-index* db
                               (generate-index-name db
                                                    name
                                                    (format nil "~a_key"
                                                            (pg-column-name col)))
                               class (list (pg-column-attnum col))
                               :unique t))))
    class))

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
