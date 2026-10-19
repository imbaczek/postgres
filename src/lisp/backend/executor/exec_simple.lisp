;;;; exec_simple.lisp -- simple-query dispatch, DDL, transactions, SET/SHOW.

(in-package #:pg)

(defun ddl-p (ast)
  (member (getf ast :type)
          '(:create-table :drop-table :create-index :drop-index :alter-table)))

(defun note-query-error ()
  "Record that the current statement failed, matching PostgreSQL.

An error inside an explicit transaction block moves it to the aborted
state even when it happens while parsing, before EXECUTE-STATEMENT has
run, so the next command reports 25P02.  An implicit transaction is
rolled back immediately."
  (when *backend*
    (when (eq (backend-state *backend*) :in-progress)
      (if (backend-explicit *backend*)
          (setf (backend-state *backend*) :aborted)
          (abort-transaction)))))

;;; ---- transaction commands ----

(defun command-begin (stream)
  (if (eq (backend-state *backend*) :idle)
      (setf (backend-state *backend*) :in-progress
            (backend-explicit *backend*) t
            (backend-command-id *backend*) 0
            (backend-xid *backend*) nil
            (backend-snapshot *backend*) nil
            (backend-dirty-relations *backend*) '())
      (send-notice stream "there is already a transaction in progress"))
  (send-command-complete stream "BEGIN"))

(defun command-commit (stream)
  (case (backend-state *backend*)
    (:aborted
     (abort-transaction)
     (send-command-complete stream "ROLLBACK"))
    (:in-progress
     (commit-transaction)
     (send-command-complete stream "COMMIT"))
    (t
     (send-notice stream "there is no transaction in progress")
     (send-command-complete stream "COMMIT"))))

(defun command-rollback (stream)
  (if (eq (backend-state *backend*) :idle)
      (progn
        (send-notice stream "there is no transaction in progress")
        (send-command-complete stream "ROLLBACK"))
      (progn
        (abort-transaction)
        (send-command-complete stream "ROLLBACK"))))

;;; ---- SET / SHOW / RESET ----

(defun exec-show (stream ast)
  (let* ((name (getf ast :name))
         (value (cond
                  ((string= name "transaction_isolation") "read committed")
                  ((string= name "server_version") *server-version*)
                  ((string= name "server_version_num")
                   (princ-to-string *server-version-num*))
                  ((string= name "client_encoding") "UTF8")
                  ((string= name "server_encoding") "UTF8")
                  ((string= name "timezone") "UTC")
                  ((string= name "datestyle") "ISO, MDY")
                  ((string= name "standard_conforming_strings") "on")
                  ((string= name "is_superuser") "on")
                  (t
                   (ereport "42704"
                            "unrecognized configuration parameter \"~a\""
                            name)))))
    (send-row-description
     stream
     (list (make-result-column :name name
                               :type-oid +textoid+
                               :typlen -1
                               :typmod -1)))
    (send-data-row-text stream (list value))
    (send-command-complete stream "SHOW")))

;;; ---- DDL ----

(defun resolve-attnums (table names)
  (mapcar (lambda (n)
            (pg-column-attnum (resolve-column-or-error table n)))
          names))

(defun fsync-index (idx)
  (smgr-fsync (pg-index-relfilenode idx)))

(defun build-unique-index (idx table name)
  (heapam-scan
   (pg-class-relfilenode table)
   (pg-class-columns table)
   *current-snapshot*
   (lambda (tuple)
     (let* ((types (index-types idx table))
            (vals (index-values idx table (htup-values tuple)))
            (key (encode-index-key vals types)))
       ;; NULLs are distinct, so they cannot violate uniqueness.
       (unless (key-has-null-p vals)
         (when (btree-search (pg-index-relfilenode idx) key)
           (ereport "23505"
                    "could not create unique index \"~a\"" name)))
       (btree-insert (pg-index-relfilenode idx)
                     key (htup-block tuple) (htup-offnum tuple))))))

(defun check-not-null-existing (table attnums constraint-name)
  (heapam-scan
   (pg-class-relfilenode table)
   (pg-class-columns table)
   *current-snapshot*
   (lambda (tuple)
     (let ((values (htup-values tuple)))
       (dolist (attnum attnums)
         (when (eq (aref values (1- attnum)) +null+)
           (let ((col (aref (pg-class-columns table) (1- attnum))))
             (ereport "23502"
                      "column \"~a\" of relation \"~a\" contains null values"
                      (pg-column-name col)
                      (or constraint-name (pg-class-name table))))))))))

(defun exec-create-table (stream ast)
  (let ((db (current-database))
        (name (getf ast :name)))
    (when (lookup-table name)
      (if (getf ast :if-not-exists)
          (progn
            (send-notice stream
                         (format nil "relation \"~a\" already exists, skipping" name))
            (send-command-complete stream "CREATE TABLE")
            (return-from exec-create-table nil))
          (ereport "42P07" "relation \"~a\" already exists" name)))
    (let ((cols (getf ast :columns))
          (constraints (getf ast :constraints)))
      (let ((names (mapcar (lambda (c) (getf c :name)) cols)))
        (when (/= (length names)
                  (length (remove-duplicates names :test #'string=)))
          (ereport "42701" "column specified more than once")))
      (let ((pk-count (+ (count-if (lambda (c) (getf c :primary)) cols)
                         (count-if (lambda (c) (eq (getf c :kind) :primary))
                                   constraints))))
        (when (> pk-count 1)
          (ereport "42P16"
                   "multiple primary keys for table \"~a\" are not allowed"
                   name)))
      (with-write-lock
        (with-catalog-lock
          (let ((class (create-table* db name cols)))
            (handler-case
                (progn
                  (dolist (c constraints)
                    (let ((attnums (resolve-attnums class (getf c :columns))))
                      (case (getf c :kind)
                        (:primary
                         (dolist (attnum attnums)
                           (setf (pg-column-notnull
                                  (aref (pg-class-columns class) (1- attnum)))
                                 t))
                         (let ((idx (create-index*
                                     db
                                     (or (getf c :name)
                                         (generate-index-name db name "pkey"))
                                     class attnums :unique t :primary t)))
                           (build-unique-index idx class (pg-index-name idx))
                           (fsync-index idx)))
                        (:unique
                         (let ((idx (create-index*
                                     db
                                     (or (getf c :name)
                                         (generate-index-name db name "key"))
                                     class attnums :unique t)))
                           (build-unique-index idx class (pg-index-name idx))
                           (fsync-index idx))))))
                  (dolist (idx (table-indexes class))
                    (fsync-index idx))
                  (save-catalog))
              (error (e)
                (save-catalog)
                (error e))))))
      (send-command-complete stream "CREATE TABLE"))))

(defun exec-drop-table (stream ast)
  (let ((db (current-database))
        (names (getf ast :names))
        (if-exists (getf ast :if-exists)))
    (dolist (name names)
      (unless (or (gethash name (database-tables db)) if-exists)
        (ereport "42P01" "table \"~a\" does not exist" name)))
    (with-write-lock
      (with-catalog-lock
        (dolist (name names)
          (if (gethash name (database-tables db))
              (drop-table* db name)
              (send-notice stream
                           (format nil "table \"~a\" does not exist, skipping"
                                   name))))
        (save-catalog)))
    (send-command-complete stream "DROP TABLE")))

(defun exec-create-index (stream ast)
  (let* ((db (current-database))
         (table (resolve-table-or-error (getf ast :table)))
         (name (getf ast :name))
         (unique (getf ast :unique))
         (if-not-exists (getf ast :if-not-exists)))
    (when (lookup-index name)
      (if if-not-exists
          (progn
            (send-notice stream
                         (format nil "relation \"~a\" already exists, skipping" name))
            (send-command-complete stream "CREATE INDEX")
            (return-from exec-create-index nil))
          (ereport "42P07" "relation \"~a\" already exists" name)))
    (let ((attnums (resolve-attnums table (getf ast :columns))))
      (with-write-lock
        (with-catalog-lock
          (let ((idx (create-index* db name table attnums :unique unique)))
            (handler-case
                (progn
                  (when unique
                    (build-unique-index idx table name))
                  (fsync-index idx)
                  (save-catalog))
              (error (e)
                (drop-index* db name)
                (error e)))))))
    (send-command-complete stream "CREATE INDEX")))

(defun exec-drop-index (stream ast)
  (let ((db (current-database))
        (names (getf ast :names))
        (if-exists (getf ast :if-exists)))
    (dolist (name names)
      (unless (or (gethash name (database-indexes db)) if-exists)
        (ereport "42704" "index \"~a\" does not exist" name)))
    (with-write-lock
      (with-catalog-lock
        (dolist (name names)
          (if (gethash name (database-indexes db))
              (drop-index* db name)
              (send-notice stream
                           (format nil "index \"~a\" does not exist, skipping"
                                   name))))
        (save-catalog)))
    (send-command-complete stream "DROP INDEX")))

(defun exec-alter-table (stream ast)
  (let ((db (current-database))
        (name (getf ast :table))
        (table (lookup-table (getf ast :table)))
        (primary (eq (getf ast :action) :add-primary-key)))
    (unless table
      (if (getf ast :if-exists)
          (progn
            (send-notice stream
                         (format nil "relation \"~a\" does not exist, skipping"
                                 name))
            (send-command-complete stream "ALTER TABLE")
            (return-from exec-alter-table nil))
          (ereport "42P01" "relation \"~a\" does not exist" name)))
    (let ((attnums (resolve-attnums table (getf ast :columns))))
      (with-write-lock
        (with-catalog-lock
          (when (and primary
                     (find-if #'pg-index-primary-p (table-indexes table)))
            (ereport "42P16"
                     "multiple primary keys for table \"~a\" are not allowed"
                     name))
          (when primary
            (check-not-null-existing table attnums (getf ast :constraint-name))
            (dolist (attnum attnums)
              (setf (pg-column-notnull
                     (aref (pg-class-columns table) (1- attnum)))
                    t)))
          (let ((idx (create-index*
                      db
                      (or (getf ast :constraint-name)
                          (generate-index-name db name
                                               (if primary "pkey" "key")))
                      table attnums :unique t :primary primary)))
            (handler-case
                (progn
                  (build-unique-index idx table (pg-index-name idx))
                  (fsync-index idx)
                  (save-catalog))
              (error (e)
                (drop-index* db (pg-index-name idx))
                (error e)))))))
    (send-command-complete stream "ALTER TABLE")))

;;; ---- simple-query execution ----

(defun execute-in-transaction (stream ast)
  (let ((implicit (not (backend-explicit *backend*))))
    (when (and (ddl-p ast) (backend-explicit *backend*))
      (ereport "0A000"
               "DDL is not supported inside an explicit transaction block"))
    (let ((snap (statement-start)))
      (with-active-snapshot
        (let ((*current-snapshot* snap))
          (handler-case
              (case (getf ast :type)
                (:select (exec-select stream ast))
                (:insert (exec-insert stream ast))
                (:update (exec-update stream ast))
                (:delete (exec-delete stream ast))
                (:create-table (exec-create-table stream ast))
                (:drop-table (exec-drop-table stream ast))
                (:create-index (exec-create-index stream ast))
                (:drop-index (exec-drop-index stream ast))
                (:alter-table (exec-alter-table stream ast))
                (t (ereport "0A000" "unsupported statement")))
            (error (e)
              (if implicit
                  (abort-transaction)
                  (setf (backend-state *backend*) :aborted))
              (error e)))))
      (when implicit
        (commit-transaction)))))

(defun execute-statement (stream ast)
  (let ((type (getf ast :type)))
    (when (and (eq (backend-state *backend*) :aborted)
               (not (member type '(:commit :rollback))))
      (ereport "25P02"
               "current transaction is aborted, commands ignored until end of transaction block"))
    (case type
      (:begin (command-begin stream))
      (:commit (command-commit stream))
      (:rollback (command-rollback stream))
      (:set (send-command-complete stream "SET"))
      (:reset (send-command-complete stream "RESET"))
      (:show (exec-show stream ast))
      (otherwise (execute-in-transaction stream ast)))))

(defun exec-simple-query (stream query)
  (handler-case
      (let ((stmts (parse-statements query)))
        (if (null stmts)
            (send-empty-query-response stream)
            (dolist (ast stmts)
              (execute-statement stream ast))))
    (pg-error (e)
      (note-query-error)
      (send-error-response stream e))
    (error (e)
      (note-query-error)
      (send-error-response
       stream
       (make-condition 'pg-error
                       :sqlstate "XX000"
                       :message (princ-to-string e))))))
