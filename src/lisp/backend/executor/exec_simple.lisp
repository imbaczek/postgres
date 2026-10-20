;;;; exec_simple.lisp -- simple-query dispatch, DDL, transactions, SET/SHOW.

(in-package #:pg)

(defmacro with-table-ddl ((table &key refresh-snapshot) &body body)
  "Lock and revalidate TABLE before running its catalog change BODY."
  (let ((table-value (gensym "TABLE-")))
    `(let ((,table-value ,table))
       (with-relation-build-lock ((pg-class-relfilenode ,table-value))
         ,(if refresh-snapshot
              `(let ((*current-snapshot* (refresh-statement-snapshot)))
                 (with-write-catalog-locks
                   (ensure-current-table ,table-value)
                   ,@body))
              `(with-write-catalog-locks
                 (ensure-current-table ,table-value)
                 ,@body))))))

(defmacro with-error-cleanup ((condition cleanup) &body body)
  "Run CLEANUP and re-signal ERROR from BODY."
  `(handler-case
       (progn ,@body)
     (error (,condition)
       ,cleanup
       (error ,condition))))

(defun ddl-p (ast)
  (member (getf ast :type)
          '(:create-table :drop-table :create-index :drop-index
            :alter-index :alter-table)))

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

(defun copy-session-settings (settings)
  (let ((copy (make-hash-table :test (hash-table-test settings))))
    (maphash (lambda (key value) (setf (gethash key copy) value)) settings)
    copy))

(defun restore-session-settings (stream)
  (let ((settings (backend-settings-transaction-start *backend*)))
    (when settings
      (setf (backend-settings *backend*) settings
            (backend-settings-transaction-start *backend*) nil)
      (send-parameter-status stream "TimeZone"
                             (or (gethash "timezone" settings) "UTC")))))

(defun command-begin (stream)
  (if (eq (backend-state *backend*) :idle)
      (setf (backend-state *backend*) :in-progress
            (backend-explicit *backend*) t
            (backend-command-id *backend*) 0
            (backend-xid *backend*) nil
            (backend-snapshot *backend*) nil
            (backend-dirty-relations *backend*) '()
            (backend-settings-transaction-start *backend*)
            (copy-session-settings (backend-settings *backend*)))
      (send-notice stream "there is already a transaction in progress"))
  (send-command-complete stream "BEGIN"))

(defun command-commit (stream)
  (case (backend-state *backend*)
    (:aborted
     (abort-transaction)
     (restore-session-settings stream)
     (send-command-complete stream "ROLLBACK"))
    (:in-progress
     (commit-transaction)
     (setf (backend-settings-transaction-start *backend*) nil)
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
        (restore-session-settings stream)
        (send-command-complete stream "ROLLBACK"))))

(defun command-checkpoint (stream)
  (unless (eq (backend-state *backend*) :idle)
    (ereport "25001" "CHECKPOINT cannot run inside a transaction block"))
  (wal-checkpoint)
  (send-command-complete stream "CHECKPOINT"))

;;; ---- SET / SHOW / RESET ----

(defun known-show-setting-p (name)
  (member name '("transaction_isolation" "server_version"
                 "server_version_num" "client_encoding" "server_encoding"
                 "timezone" "datestyle" "standard_conforming_strings"
                 "is_superuser")
          :test #'string=))

(defun valid-timezone-value-p (value)
  (and (stringp value)
       (plusp (length value))
       (not (char= (char value 0) #\/))
       (every (lambda (char)
                (or (and (char>= char #\A) (char<= char #\Z))
                    (and (char>= char #\a) (char<= char #\z))
                    (and (char>= char #\0) (char<= char #\9))
                    (find char "/_+-")))
              value)
       (handler-case
           (with-open-file (stream (merge-pathnames
                                    value #p"/usr/share/zoneinfo/")
                                   :direction :input
                                   :element-type '(unsigned-byte 8))
             (and (loop for expected across #(84 90 105 102)
                        always (= (read-byte stream) expected))))
         (error () nil))))

(defun exec-set (stream ast)
  (let* ((name (string-downcase (getf ast :name)))
         (raw-value (getf ast :value))
         (value (if (eq raw-value :default)
                    (cond
                      ((string= name "timezone") "UTC")
                      ((string= name "transaction_isolation") "read committed")
                      ((string= name "client_encoding") "UTF8")
                      ((string= name "standard_conforming_strings") "on")
                      (t :default))
                    raw-value)))
    (cond
      ((string= name "timezone")
       (if (eq raw-value :default)
           (remhash "timezone" (backend-settings *backend*))
           (progn
             (when (string-equal value "local")
               (ereport "0A000" "this TIME ZONE value is not supported"))
             (unless (valid-timezone-value-p value)
               (ereport "22023"
                        "invalid value for parameter \"TimeZone\": \"~a\""
                        value))
             (setf (gethash "timezone" (backend-settings *backend*)) value)))
       (send-parameter-status
        stream "TimeZone"
        (or (gethash "timezone" (backend-settings *backend*)) "UTC")))
      ((string= name "transaction_isolation")
       (unless (string-equal value "read committed")
         (ereport "0A000" "only READ COMMITTED isolation is supported")))
      ((string= name "client_encoding")
       (unless (string-equal value "UTF8")
         (ereport "0A000" "only UTF8 client encoding is supported")))
      ((string= name "standard_conforming_strings")
       (unless (string-equal value "on")
         (ereport "0A000" "standard_conforming_strings can only be on")))
      ((known-show-setting-p name)
       (ereport "0A000" "configuration parameter \"~a\" is read-only"
                name))
      (t
       (ereport "42704" "unrecognized configuration parameter \"~a\""
                name)))
    (send-command-complete stream "SET")))

(defun exec-reset (stream ast)
  (let ((name (string-downcase (getf ast :name))))
    (cond
      ((string= name "all")
       (clrhash (backend-settings *backend*))
       (send-parameter-status stream "TimeZone" "UTC"))
      ((string= name "timezone")
       (remhash "timezone" (backend-settings *backend*))
       (send-parameter-status stream "TimeZone" "UTC"))
      ((member name '("transaction_isolation" "client_encoding"
                      "standard_conforming_strings")
               :test #'string=)
       ;; These settings have a fixed supported value in this server.
       (values))
      ((known-show-setting-p name)
       (ereport "0A000" "configuration parameter \"~a\" cannot be reset"
                name))
      (t
       (ereport "42704" "unrecognized configuration parameter \"~a\""
                name)))
    (send-command-complete stream "RESET")))

(defun exec-show (stream ast)
  (let* ((name (string-downcase (getf ast :name)))
         (value (cond
                  ((string= name "transaction_isolation") "read committed")
                  ((string= name "server_version") *server-version*)
                  ((string= name "server_version_num")
                   (princ-to-string *server-version-num*))
                  ((string= name "client_encoding") "UTF8")
                  ((string= name "server_encoding") "UTF8")
                  ((string= name "timezone")
                   (or (gethash "timezone" (backend-settings *backend*))
                       "UTC"))
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
  (do-table-tuples (tuple table)
    (let* ((types (index-types idx table))
           (vals (index-values idx (htup-values tuple)))
           (key (encode-index-key vals types)))
      ;; By default NULLs are distinct.  A NULLS NOT DISTINCT index compares
      ;; null-containing keys like any other key.
      (unless (and (key-has-null-p vals)
                   (not (pg-index-nulls-not-distinct-p idx)))
        (when (btree-search (pg-index-relfilenode idx) key)
          (ereport "23505"
                   "could not create unique index \"~a\"" name)))
      (btree-insert (pg-index-relfilenode idx)
                    key (htup-block tuple) (htup-offnum tuple)))))

(defun check-not-null-existing (table attnums constraint-name)
  (do-table-tuples (tuple table)
    (let ((values (htup-values tuple)))
      (dolist (attnum attnums)
        (when (eq (aref values (1- attnum)) +null+)
          (let ((col (aref (pg-class-columns table) (1- attnum))))
            (ereport "23502"
                     "column \"~a\" of relation \"~a\" contains null values"
                     (pg-column-name col)
                     (or constraint-name (pg-class-name table)))))))))

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
      (with-write-catalog-locks
        (let ((class (create-table* db name cols)))
          (with-error-cleanup
              (e (progn
                   ;; CREATE TABLE is one statement.  A failed constraint
                   ;; build must not leave earlier indexes published.
                   (when (eq class (lookup-table name))
                     (drop-table* db name))
                   (save-catalog)))
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
                               class attnums :unique t :primary t
                               :constraint t)))
                     (build-unique-index idx class (pg-index-name idx))
                     (fsync-index idx)))
                  (:unique
                   (let ((idx (create-index*
                               db
                               (or (getf c :name)
                                   (generate-unique-index-name
                                    db name class attnums))
                               class attnums :unique t
                               :constraint t
                               :nulls-not-distinct
                               (getf c :nulls-not-distinct))))
                     (build-unique-index idx class (pg-index-name idx))
                     (fsync-index idx))))))
            (dolist (idx (table-indexes class))
              (fsync-index idx))
            (save-catalog))))
      (send-command-complete stream "CREATE TABLE"))))

(defun exec-drop-table (stream ast)
  (let* ((db (current-database))
         (names (getf ast :names))
         (if-exists (getf ast :if-exists))
         (relations
           (mapcar (lambda (name)
                     (let ((class (gethash name (database-tables db))))
                       (unless (or class if-exists)
                         (ereport "42P01" "table \"~a\" does not exist" name))
                       (cons name class)))
                   names))
         (relfilenodes
           (loop for entry in relations
                 for class = (cdr entry)
                 when class collect (pg-class-relfilenode class))))
    (with-relation-build-locks (relfilenodes t)
      (with-write-catalog-locks
        ;; A concurrent DROP/CREATE may have replaced a name while we waited.
        ;; Never delete a relation whose gate was not part of this operation.
        (dolist (entry relations)
          (let* ((name (car entry))
                 (original (cdr entry))
                 (current (gethash name (database-tables db))))
            (when (and original
                       (not (eq original current))
                       (not if-exists))
              (if current
                  (ereport "55000" "table \"~a\" changed while waiting for lock"
                           name)
                  (ereport "42P01" "table \"~a\" does not exist" name)))))
        (dolist (entry relations)
          (let* ((name (car entry))
                 (original (cdr entry))
                 (current (gethash name (database-tables db))))
            (if (and original (eq original current))
                (drop-table* db name)
                (send-notice stream
                             (format nil "table \"~a\" does not exist, skipping"
                                     name)))))
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
      (with-table-ddl (table :refresh-snapshot t)
        (let ((idx (create-index* db name table attnums
                                  :unique unique
                                  :nulls-not-distinct
                                  (getf ast :nulls-not-distinct))))
          (with-error-cleanup (e (drop-index* db name))
            (when unique
              (build-unique-index idx table name))
            (fsync-index idx)
            (save-catalog)))))
    (send-command-complete stream "CREATE INDEX")))

(defun exec-drop-index (stream ast)
  (let* ((db (current-database))
         (names (getf ast :names))
         (if-exists (getf ast :if-exists))
         (entries (mapcar (lambda (name)
                            (cons name (gethash name (database-indexes db))))
                          names))
         (relfilenodes
           (loop for entry in entries
                 for idx = (cdr entry)
                 for table = (when idx
                               (find-table-by-oid (pg-index-table-oid idx)))
                 when table collect (pg-class-relfilenode table))))
    (dolist (entry entries)
      (unless (or (cdr entry) if-exists)
        (ereport "42704" "index \"~a\" does not exist" (car entry))))
    (with-relation-build-locks (relfilenodes)
      (with-write-catalog-locks
        ;; Revalidate every captured index before making any change.  This
        ;; keeps a concurrent drop/recreate from redirecting this statement.
        (dolist (entry entries)
          (let* ((name (car entry))
                 (original (cdr entry))
                 (current (gethash name (database-indexes db))))
            (when (and original (not (eq original current)))
              (cond
                (current
                 (ereport "55000" "index \"~a\" changed while waiting for lock"
                          name))
                ((not if-exists)
                 (ereport "42704" "index \"~a\" does not exist" name))))))
        ;; Check dependencies as a preflight, so a multi-index statement
        ;; cannot drop an earlier index and then fail on a later one.
        (dolist (entry entries)
          (let* ((name (car entry))
                 (idx (cdr entry)))
            (when (and idx (eq idx (gethash name (database-indexes db)))
                       (pg-index-constraint-owned-p idx))
              (let ((table (find-table-by-oid (pg-index-table-oid idx))))
                (ereport "2BP01"
                         "cannot drop index \"~a\" because constraint \"~a\" on table \"~a\" requires it"
                         name name (if table (pg-class-name table) "?"))))))
        (dolist (entry entries)
          (let* ((name (car entry))
                 (original (cdr entry)))
            (if (and original
                     (eq original (gethash name (database-indexes db))))
                (drop-index* db name)
                (send-notice stream
                             (format nil "index \"~a\" does not exist, skipping"
                                     name)))))
        (save-catalog)))
    (send-command-complete stream "DROP INDEX")))

(defun exec-alter-index (stream ast)
  (let* ((db (current-database))
         (name (getf ast :name))
         (new-name (getf ast :new-name))
         (idx (lookup-index name))
         (table (when idx (find-table-by-oid (pg-index-table-oid idx)))))
    (unless idx
      (ereport "42704" "index \"~a\" does not exist" name))
    (unless table
      (ereport "42P01" "table for index \"~a\" does not exist" name))
    (with-table-ddl (table)
      (let ((current (lookup-index name)))
        (unless (eq idx current)
          (if current
              (ereport "55000" "index \"~a\" changed while waiting for lock"
                       name)
              (ereport "42704" "index \"~a\" does not exist" name)))
        (rename-index* db name new-name)
        (save-catalog)))
    (send-command-complete stream "ALTER INDEX")))

(defun exec-alter-table-using-index (db table ast primary)
  (let* ((index-name (getf ast :using-index))
         (idx (lookup-index index-name)))
    (unless idx
      (ereport "42704" "index \"~a\" does not exist" index-name))
    (with-table-ddl (table :refresh-snapshot t)
      ;; Recheck after waiting for the relation gate.
      (setf idx (lookup-index index-name))
      (unless idx
        (ereport "42704" "index \"~a\" does not exist" index-name))
      (when (pg-index-constraint-owned-p idx)
        (ereport "55000"
                 "index \"~a\" is already associated with a constraint"
                 index-name))
      (unless (= (pg-index-table-oid idx) (pg-class-oid table))
        (ereport "55000" "index \"~a\" does not belong to table \"~a\""
                 index-name (pg-class-name table)))
      (unless (pg-index-unique-p idx)
        (ereport "42809" "\"~a\" is not a unique index" index-name))
      (when primary
        (when (find-if #'pg-index-primary-p (table-indexes table))
          (ereport "42P16"
                   "multiple primary keys for table \"~a\" are not allowed"
                   (pg-class-name table)))
        (when (pg-index-nulls-not-distinct-p idx)
          (ereport "42P16"
                   "primary keys cannot use NULLS NOT DISTINCT indexes"))
        (check-not-null-existing table (pg-index-columns idx)
                                 (getf ast :constraint-name)))
      (let* ((new-name (or (getf ast :constraint-name)
                           (pg-index-name idx))))
        (unless (string= new-name (pg-index-name idx))
          (check-new-relation-name db new-name))
        (when primary
          (dolist (attnum (pg-index-columns idx))
            (setf (pg-column-notnull
                   (aref (pg-class-columns table) (1- attnum)))
                  t))
          (setf (pg-index-primary-p idx) t))
        (setf (pg-index-constraint-p idx) t)
        (unless (string= new-name (pg-index-name idx))
          (rename-index* db (pg-index-name idx) new-name))
        (save-catalog)))))

(defun exec-alter-table-constraint (stream db table ast)
  (let* ((name (getf ast :constraint-name))
         (action (getf ast :action))
         (if-exists (getf ast :constraint-if-exists)))
    (with-table-ddl (table)
      (let* ((idx (gethash name (database-indexes db)))
             (belongs-to-table
               (and idx
                    (= (pg-index-table-oid idx) (pg-class-oid table))
                    (pg-index-constraint-owned-p idx))))
        (unless belongs-to-table
          (if (and (eq action :drop-constraint) if-exists)
              (progn
                (send-notice stream
                             (format nil "constraint \"~a\" of relation \"~a\" does not exist, skipping"
                                     name (pg-class-name table)))
                (return-from exec-alter-table-constraint nil))
              (ereport "42704"
                       "constraint \"~a\" of relation \"~a\" does not exist"
                       name (pg-class-name table))))
        (case action
          (:rename-constraint
           (rename-index* db name (getf ast :new-name)))
          (:drop-constraint
           (drop-index* db name)))
        (save-catalog)))
    t))

(defun exec-alter-table-replace-primary (stream db table actions)
  (unless (and (= (length actions) 2)
               (eq (getf (first actions) :action) :drop-constraint)
               (eq (getf (second actions) :action) :add-primary-key)
               (getf (second actions) :using-index))
    (ereport "0A000"
             "only DROP CONSTRAINT followed by ADD PRIMARY KEY USING INDEX is supported in multi-action ALTER TABLE"))
  (let* ((drop-action (first actions))
         (add-action (second actions))
         (old-name (getf drop-action :constraint-name))
         (old-if-exists (getf drop-action :constraint-if-exists))
         (index-name (getf add-action :using-index)))
    (with-table-ddl (table :refresh-snapshot t)
      (let* ((old-idx (gethash old-name (database-indexes db)))
             (old-constraint
               (and old-idx
                    (= (pg-index-table-oid old-idx) (pg-class-oid table))
                    (pg-index-constraint-owned-p old-idx)
                    old-idx))
             (new-idx (gethash index-name (database-indexes db)))
             (new-name (or (getf add-action :constraint-name)
                           index-name)))
        (unless old-constraint
          (if old-if-exists
              (send-notice stream
                           (format nil "constraint \"~a\" of relation \"~a\" does not exist, skipping"
                                   old-name (pg-class-name table)))
              (ereport "42704" "constraint \"~a\" of relation \"~a\" does not exist"
                       old-name (pg-class-name table))))
        (unless new-idx
          (ereport "42704" "index \"~a\" does not exist" index-name))
        (unless (= (pg-index-table-oid new-idx) (pg-class-oid table))
          (ereport "55000" "index \"~a\" does not belong to table \"~a\""
                   index-name (pg-class-name table)))
        (unless (pg-index-unique-p new-idx)
          (ereport "42809" "\"~a\" is not a unique index" index-name))
        (when (pg-index-constraint-owned-p new-idx)
          (ereport "55000"
                   "index \"~a\" is already associated with a constraint"
                   index-name))
        (when (pg-index-nulls-not-distinct-p new-idx)
          (ereport "42P16"
                   "primary keys cannot use NULLS NOT DISTINCT indexes"))
        (when (find-if (lambda (idx)
                         (and (pg-index-primary-p idx)
                              (not (eq idx old-constraint))))
                       (table-indexes table))
          (ereport "42P16"
                   "multiple primary keys for table \"~a\" are not allowed"
                   (pg-class-name table)))
        (check-not-null-existing table (pg-index-columns new-idx) new-name)
        (when (or (gethash new-name (database-tables db))
                  (let ((named (gethash new-name (database-indexes db))))
                    (and named
                         (not (eq named old-constraint))
                         (not (eq named new-idx)))))
          (ereport "42P07" "relation \"~a\" already exists" new-name))
        (when old-constraint
          (drop-index* db old-name))
        (progn
          (setf (pg-index-primary-p new-idx) t
                (pg-index-constraint-p new-idx) t)
          (dolist (attnum (pg-index-columns new-idx))
            (setf (pg-column-notnull
                   (aref (pg-class-columns table) (1- attnum)))
                  t))
          (unless (string= new-name (pg-index-name new-idx))
            (rename-index* db (pg-index-name new-idx) new-name))
          (save-catalog))))))

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
    (cond
      ((getf ast :actions)
       (exec-alter-table-replace-primary stream db table (getf ast :actions)))
      ((getf ast :using-index)
       (exec-alter-table-using-index db table ast primary))
      ((member (getf ast :action) '(:rename-constraint :drop-constraint))
       (exec-alter-table-constraint stream db table ast))
      (t
       (let ((attnums (resolve-attnums table (getf ast :columns))))
         (with-table-ddl (table :refresh-snapshot t)
           (when (and primary
                      (find-if #'pg-index-primary-p (table-indexes table)))
             (ereport "42P16"
                      "multiple primary keys for table \"~a\" are not allowed"
                      name))
           (when primary
             (check-not-null-existing table attnums (getf ast :constraint-name)))
           (let* ((previous-notnull
                    (when primary
                      (mapcar (lambda (attnum)
                                (cons attnum
                                      (pg-column-notnull
                                       (aref (pg-class-columns table)
                                             (1- attnum)))))
                              attnums)))
                  (idx (create-index*
                        db
                        (or (getf ast :constraint-name)
                            (if primary
                                (generate-index-name db name "pkey")
                                (generate-unique-index-name
                                 db name table attnums)))
                        table attnums :unique t :primary primary
                        :constraint t
                        :nulls-not-distinct
                        (getf ast :nulls-not-distinct))))
             (with-error-cleanup
                 (e (progn
                      (drop-index* db (pg-index-name idx))
                      (dolist (entry previous-notnull)
                        (setf (pg-column-notnull
                               (aref (pg-class-columns table)
                                     (1- (car entry))))
                              (cdr entry)))))
               (build-unique-index idx table (pg-index-name idx))
               (when primary
                 (dolist (attnum attnums)
                   (setf (pg-column-notnull
                          (aref (pg-class-columns table)
                                (1- attnum)))
                         t)))
               (fsync-index idx)
               (save-catalog)))))))
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
                (:alter-index (exec-alter-index stream ast))
                (:alter-table (exec-alter-table stream ast))
                (t (ereport "0A000" "unsupported statement")))
            (error (e)
              (if (fatal-error-p e)
                  (error e)
                  (progn
                    (if implicit
                        (abort-transaction)
                        (setf (backend-state *backend*) :aborted))
                    (error e)))))))
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
      (:checkpoint (command-checkpoint stream))
      (:set (exec-set stream ast))
      (:reset (exec-reset stream ast))
      (:show (exec-show stream ast))
      (otherwise (execute-in-transaction stream ast)))))

(defun exec-simple-query (stream query &key (start-execution-p t)
                                         (finish-execution-p t)
                                         (statements nil statements-p)
                                         prepare-only-p)
  (let ((backend *backend*))
    ;; Mark the backend as executing a command, so pg_cancel_backend()
    ;; knows a pending cancel is meaningful (an idle cancel is ignored).
    (when (and backend start-execution-p)
      ;; Clear a stale idle cancel before publishing EXECUTING. If these stores
      ;; are reversed, a concurrent CancelRequest can set the flag after
      ;; EXECUTING becomes true and have this query-start path erase it.
      (setf (backend-cancel-pending backend) nil
            (backend-executing backend) t))
    (unwind-protect
         (let ((completed-p nil)
               (result nil))
           (setf result
                 (handler-case
                     (let ((stmts (if statements-p
                                      statements
                                      (parse-statements query))))
                       (prog1
                           (if prepare-only-p
                               stmts
                               (if (null stmts)
                                   (send-empty-query-response stream)
                                   (dolist (ast stmts)
                                     (check-for-interrupts)
                                     (execute-statement stream ast))))
                         (setf completed-p t)))
                   (pg-error (e)
                     ;; A FATAL must end the connection; let it reach handle-connection.
                     (if (fatal-error-p e)
                         (error e)
                         (progn
                           (note-query-error)
                           (send-error-response stream e))))
                   (error (e)
                     (note-query-error)
                     (send-error-response
                      stream
                      (make-condition 'pg-error
                                      :sqlstate "XX000"
                                      :message (princ-to-string e))))))
           (values result completed-p))
      (when (and backend finish-execution-p)
        (setf (backend-executing backend) nil
              (backend-cancel-pending backend) nil)))))
