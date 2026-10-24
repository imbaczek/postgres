(in-package #:pg)

;; These relations belong to the subscriber's progress machinery, not the
;; publisher's replicated schema. Keep them out of the strict schema contract.
(defparameter +logical-private-schema-table-names+
  '("pg_lisp_subscription_progress" "pg_lisp_subscription_config"))

(defun logical-schema-contract-object-list (bootstrap key)
  (remove-if
   (lambda (object)
     (and (eq key :tables)
          (member (getf object :name) +logical-private-schema-table-names+
                  :test #'string=)))
   (mapcar
    (lambda (object)
      (if (eq key :sequences)
          (list :name (getf (getf object :state) :name)
                :definition (getf object :definition))
          (list :name (getf object :name) :sql (getf object :sql))))
    (getf bootstrap key))))

(defun logical-bootstrap-schema-contract (bootstrap)
  "Return definitions only; sequence values are deliberately not schema."
  (list :tables (logical-schema-contract-object-list bootstrap :tables)
        :indexes (logical-schema-contract-object-list bootstrap :indexes)
        :sequences (logical-schema-contract-object-list bootstrap :sequences)))

(defun logical-current-schema-contract-under-write-lock ()
  (logical-bootstrap-schema-contract
   (catalog-replication-bootstrap-data (current-database))))

(defun logical-current-schema-contract ()
  "Capture the supported catalog shape atomically, excluding sequence state."
  (with-write-lock
    (logical-current-schema-contract-under-write-lock)))

(defun logical-plist-has-exact-keys-p (plist keys)
  (and (listp plist)
       (integerp (list-length plist))
       (evenp (length plist))
       (= (length plist) (* 2 (length keys)))
       (let ((found (loop for (key value) on plist by #'cddr collect key)))
         (and (= (length found) (length (remove-duplicates found :test #'eq)))
              (every (lambda (key) (member key keys :test #'eq)) found)
              (every (lambda (key) (member key found :test #'eq)) keys)))))

(defun logical-schema-object-list-valid-p (objects definition-key)
  (and (listp objects)
       (integerp (list-length objects))
       (every
        (lambda (object)
          (and (listp object)
               (integerp (list-length object))
               (evenp (length object))
               (= (length object) 4)
               (let ((keys (loop for (key value) on object by #'cddr
                                 collect key)))
                 (and (= (length keys) 2)
                      (= (length (remove-duplicates keys :test #'eq)) 2)
                      (member :name keys :test #'eq)
                      (member definition-key keys :test #'eq)))
               (stringp (getf object :name))
               (stringp (getf object definition-key))))
        objects)
       (let ((names (mapcar (lambda (object) (getf object :name)) objects)))
         (= (length names) (length (remove-duplicates names :test #'string=))))))

(defun logical-schema-contract-valid-p (contract)
  (ignore-errors
   (and (listp contract)
        (integerp (list-length contract))
        (evenp (length contract))
        (= (length contract) 6)
        (let ((keys (loop for (key value) on contract by #'cddr collect key)))
          (and (= (length keys) 3)
               (= (length (remove-duplicates keys :test #'eq)) 3)
               (every (lambda (key)
                        (member key '(:tables :indexes :sequences) :test #'eq))
                      keys)))
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
