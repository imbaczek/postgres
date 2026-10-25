(in-package #:pg)

;; Subscriber-owned metadata: one registry for allowlists, bootstrap/schema
;; projection, and replicated utility guards so new state tables cannot diverge.
(defparameter +logical-private-schema-table-names+
  '("pg_lisp_subscription_progress" "pg_lisp_subscription_config"
    "pg_lisp_standard_subscriptions"))

(defun logical-private-metadata-table-name-p (name)
  (and (stringp name)
       (member name +logical-private-schema-table-names+ :test #'string=)))

(defun logical-plist-has-exact-keys-p (plist keys)
  (and (listp plist)
       (integerp (list-length plist))
       (evenp (length plist))
       (= (length plist) (* 2 (length keys)))
       (let ((found (loop for (key value) on plist by #'cddr collect key)))
         (and (= (length found) (length (remove-duplicates found :test #'eq)))
              (every (lambda (key) (member key keys :test #'eq)) found)
              (every (lambda (key) (member key found :test #'eq)) keys)))))

(defun logical-bootstrap-table-entry-valid-p (entry)
  (and (logical-plist-has-exact-keys-p entry '(:name :sql))
       (stringp (getf entry :name))
       (stringp (getf entry :sql))))

(defun logical-bootstrap-index-entry-valid-p (entry table-names)
  (and (logical-plist-has-exact-keys-p
        entry '(:table-name :name :sql))
       (stringp (getf entry :table-name))
       (stringp (getf entry :name))
       (stringp (getf entry :sql))
       (member (getf entry :table-name) table-names :test #'string=)))

(defun logical-bootstrap-object-names-unique-p (objects)
  (let ((names (mapcar (lambda (object) (getf object :name)) objects)))
    (= (length names) (length (remove-duplicates names :test #'string=)))))

(defun logical-replication-bootstrap-view (bootstrap)
  "Validate publisher metadata, then omit subscriber-private tables and indexes.
The trusted catalog emitter derives each index owner and SQL from the same table
object, matching PostgreSQL's pg_index.indrelid ownership field. Older
bootstrap payloads without index owners are rejected before schema changes."
  (let ((tables (getf bootstrap :tables))
        (indexes (getf bootstrap :indexes))
        (sequences (getf bootstrap :sequences)))
    (unless (and (integerp (getf bootstrap :database-oid))
                 (listp tables) (integerp (list-length tables))
                 (listp indexes) (integerp (list-length indexes))
                 (listp sequences) (integerp (list-length sequences))
                 (every #'logical-bootstrap-table-entry-valid-p tables)
                 (logical-bootstrap-object-names-unique-p tables))
      (error "replication bootstrap schema description is malformed"))
    (let ((table-names (mapcar (lambda (entry) (getf entry :name)) tables)))
      (unless (and (every (lambda (entry)
                            (logical-bootstrap-index-entry-valid-p
                             entry table-names))
                          indexes)
                   (logical-bootstrap-object-names-unique-p indexes))
        (error "replication bootstrap index ownership metadata is malformed"))
      (list :database-oid (getf bootstrap :database-oid)
            :tables (remove-if
                     (lambda (entry)
                       (logical-private-metadata-table-name-p
                        (getf entry :name)))
                     tables)
            :indexes (remove-if
                      (lambda (entry)
                        (logical-private-metadata-table-name-p
                         (getf entry :table-name)))
                      indexes)
            :sequences sequences))))

(defun logical-schema-contract-object-list (bootstrap-view key)
  (mapcar
   (lambda (object)
     (if (eq key :sequences)
         (list :name (getf (getf object :state) :name)
               :definition (getf object :definition))
         (list :name (getf object :name) :sql (getf object :sql))))
   (getf bootstrap-view key)))

(defun logical-bootstrap-schema-contract (bootstrap-view)
  "Return definitions only; sequence values are deliberately not schema."
  (list :tables (logical-schema-contract-object-list bootstrap-view :tables)
        :indexes (logical-schema-contract-object-list bootstrap-view :indexes)
        :sequences (logical-schema-contract-object-list bootstrap-view :sequences)))

;; The private event envelope lets DDL use the normal executor, unlike PostgreSQL's
;; pgoutput tuple stream. Protect subscriber-owned relations before the custom
;; event reaches utility.c:ExecDropStmt's equivalent apply path.

(defun logical-private-metadata-resolved-table-name (database name)
  ;; PostgreSQL resolves RangeVar names in catalog/namespace.c before
  ;; tcop/utility.c:ExecDropStmt dispatches destructive DDL. This parser and
  ;; lookup-table each strip the one supported public prefix, so mirror both.
  (let* ((table (and (stringp name)
                     (gethash (strip-schema name)
                              (database-tables database))))
         (resolved-name (and table (pg-class-name table))))
    (and (logical-private-metadata-table-name-p resolved-name)
         resolved-name)))

(defun logical-private-metadata-index-owner-name (database index-name)
  (let* ((index (gethash index-name (database-indexes database)))
         (table
           (and index
                (loop for candidate being the hash-values of (database-tables database)
                      when (= (pg-class-oid candidate)
                              (pg-index-table-oid index))
                        return candidate)))
         (table-name (and table (pg-class-name table))))
    (and (logical-private-metadata-table-name-p table-name)
         table-name)))

(defun logical-private-metadata-ddl-target-name (database ast)
  "Return the subscriber-owned table name targeted by replicated DDL AST."
  (with-write-lock
    (case (getf ast :type)
      (:create-table
       (let ((name (getf ast :name)))
         (or (and (logical-private-metadata-table-name-p name) name)
             (logical-private-metadata-resolved-table-name database name))))
      ;; DROP TABLE uses the parser's already-normalized catalog key directly.
      (:drop-table
       (find-if #'logical-private-metadata-table-name-p (getf ast :names)))
      ;; ALTER TABLE resolves the AST name through lookup-table.
      (:alter-table
       (let ((name (getf ast :table)))
         (or (and (logical-private-metadata-table-name-p name) name)
             (logical-private-metadata-resolved-table-name database name))))
      (:create-index
       (let ((table-name (getf ast :table))
             (index-name (getf ast :name)))
         (or (and (logical-private-metadata-table-name-p table-name)
                  table-name)
             (logical-private-metadata-resolved-table-name database table-name)
             (logical-private-metadata-index-owner-name database index-name)
             (and (stringp index-name)
                  (logical-private-metadata-index-owner-name
                   database (strip-schema index-name))))))
      ;; DROP INDEX uses the parser's already-normalized catalog key directly.
      (:drop-index
       (loop for name in (getf ast :names)
             for owner = (logical-private-metadata-index-owner-name
                          database name)
             when owner return owner))
      ;; ALTER INDEX resolves the AST name through lookup-index.
      (:alter-index
       (let ((name (getf ast :name)))
         (or (logical-private-metadata-index-owner-name database name)
             (and (stringp name)
                  (logical-private-metadata-index-owner-name
                   database (strip-schema name))))))
      (otherwise nil))))

(defun logical-current-schema-contract-under-write-lock ()
  (logical-bootstrap-schema-contract
   (logical-replication-bootstrap-view
    (catalog-replication-bootstrap-data (current-database)))))

(defun logical-current-schema-contract ()
  "Capture the supported catalog shape atomically, excluding sequence state."
  (with-write-lock
    (logical-current-schema-contract-under-write-lock)))

(defun logical-schema-object-list-valid-p (objects definition-key)
  (and (listp objects)
       (integerp (list-length objects))
       (every
        (lambda (object)
          (and (logical-plist-has-exact-keys-p object (list :name definition-key))
               (stringp (getf object :name))
               (stringp (getf object definition-key))))
        objects)
       (let ((names (mapcar (lambda (object) (getf object :name)) objects)))
         (= (length names) (length (remove-duplicates names :test #'string=))))))

(defun logical-schema-contract-valid-p (contract)
  (ignore-errors
   (and (logical-plist-has-exact-keys-p contract '(:tables :indexes :sequences))
        (logical-schema-object-list-valid-p (getf contract :tables) :sql)
        (logical-schema-object-list-valid-p (getf contract :indexes) :sql)
        (logical-schema-object-list-valid-p
         (getf contract :sequences) :definition))))

(defun logical-schema-object-compatible-p (source-object target-object key)
  (and source-object
       (string= (getf source-object :name) (getf target-object :name))
       (string= (getf source-object key) (getf target-object key))))

(defun logical-schema-contract-subset-compatible-p (source target)
  "True when every existing target object has the publisher's same definition."
  (and (logical-schema-contract-valid-p source)
       (logical-schema-contract-valid-p target)
       (loop for (objects-key definition-key) in
             '((:tables :sql) (:indexes :sql) (:sequences :definition))
             always
             (loop for target-object in (getf target objects-key)
                   for source-object =
                                     (find (getf target-object :name)
                                           (getf source objects-key)
                                           :key (lambda (object) (getf object :name))
                                           :test #'string=)
                   always
                   (logical-schema-object-compatible-p
                    source-object target-object definition-key)))))

(defun logical-validate-schema-contract-under-write-lock (expected phase)
  (unless (logical-schema-contract-valid-p expected)
    (error "logical DDL event has a malformed schema contract"))
  (let ((actual (logical-current-schema-contract-under-write-lock)))
    (unless (equal expected actual)
      (error "subscriber schema differs from publisher schema ~a: expected ~s, got ~s"
             phase expected actual)))
  t)

(defun logical-validate-schema-contract (expected phase)
  (with-write-lock
    (logical-validate-schema-contract-under-write-lock expected phase)))

(defun logical-supported-ddl-ast-p (ast)
  (and (listp ast)
       (integerp (list-length ast))
       (evenp (length ast))
       (let ((keys (loop for (key value) on ast by #'cddr collect key)))
         (and (member :type keys :test #'eq)
              (= (count :type keys :test #'eq) 1)
              (= (length keys) (length (remove-duplicates keys :test #'eq)))
              (member (getf ast :type)
                      '(:create-table :drop-table :create-index :drop-index
                        :alter-index :alter-table :create-sequence
                        :alter-sequence :drop-sequence)
                      :test #'eq)))))

(defun logical-validate-ddl-event-envelope (envelope)
  (let ((database-oid (getf envelope :database-oid))
        (ast (getf envelope :ast))
        (schema-before (getf envelope :schema-before))
        (schema-after (getf envelope :schema-after)))
    (unless (and (integerp database-oid)
                 (<= 1 database-oid #xFFFFFFFF)
                 (logical-supported-ddl-ast-p ast)
                 (eql (getf envelope :schema-contract-version) 1)
                 (logical-schema-contract-valid-p schema-before)
                 (logical-schema-contract-valid-p schema-after))
      (ereport "08P01" "invalid or unsupported replicated DDL event"))
    envelope))

(defun logical-catalog-event-for-ast (ast)
  ;; PostgreSQL's pgoutput Relation messages describe row shape; PostgreSQL does
  ;; not stream DDL. This extension carries strict before/after catalog shapes
  ;; alongside supported DDL so a subscriber can reject drift before applying.
  (unless (member (getf ast :type)
                  '(:create-publication :alter-publication :drop-publication))
    (let* ((database (current-database))
           (database-oid (database-oid database))
           (schema-before
             (or *logical-source-schema-precondition*
                 (logical-current-schema-contract-under-write-lock)))
           (schema-after (logical-current-schema-contract-under-write-lock))
           (envelope
             (list :schema-contract-version 1
                   :database-oid database-oid
                   :ast ast
                   :schema-before schema-before
                   :schema-after schema-after))
           (change
             (make-logical-change
              :kind :ddl :database-oid database-oid
              :event-payload (catalog-readable-data-string envelope))))
      (logical-wrap-catalog-event
       database-oid (logical-encode-change change)))))
