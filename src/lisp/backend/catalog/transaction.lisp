;;;; Transaction-private catalog names and publication conflict checks.

(in-package #:pg)

(defstruct catalog-transaction-entry
  base
  base-version
  object
  sequence-dependencies)

(defstruct (catalog-transaction (:constructor make-catalog-transaction (database)))
  (database nil :read-only t)
  (names (make-hash-table :test #'equal) :read-only t)
  (dependencies (make-hash-table :test #'equalp) :read-only t)
  (publication-edits (make-hash-table :test #'equal) :read-only t)
  (provisional-nodes (make-hash-table :test #'eql) :read-only t)
  staged-ddl-p
  capture-ddl-p
  logical-ddl-p
  logical-apply-state
  logical-sequence-dependencies)

(defun catalog-transaction-has-changes-p (transaction)
  (and transaction
       (or (plusp (hash-table-count (catalog-transaction-names transaction)))
           (plusp (hash-table-count (catalog-transaction-provisional-nodes transaction))))))

(defun catalog-transaction-captured-ddl-p (transaction)
  "Whether TRANSACTION has ordered logical DDL, even if its final overlay is empty."
  (and transaction (catalog-transaction-logical-ddl-p transaction)))

(defun catalog-transaction-name-map (database kind)
  (ecase kind
    (:table (database-tables database))
    (:index (database-indexes database))))

(defun catalog-relation-definition-version (object)
  "Snapshot structural fields that legacy DDL can change in place.
Statistics and freeze horizons can advance without changing the definition."
  (etypecase object
    (null nil)
    (pg-index (copy-tree (index-to-sexp object)))
    (pg-class
     (list (pg-class-relfilenode object)
           (map 'list #'column-to-sexp (pg-class-columns object))
           (copy-list (pg-class-indexes object))
           (catalog-expression-to-sexp (pg-class-checks object))
           (let ((toast (pg-class-toast object)))
             (when toast
               (list (pg-toast-oid toast) (pg-toast-relfilenode toast)
                     (pg-toast-index-oid toast) (pg-toast-index-relfilenode toast))))))))

(defun catalog-transaction-lookup (transaction kind name)
  "Resolve NAME against the transaction's own changes and the live catalog."
  (let ((entry (gethash (cons kind name) (catalog-transaction-names transaction))))
    (if entry
        (catalog-transaction-entry-object entry)
        (gethash name (catalog-transaction-name-map (catalog-transaction-database transaction)
                                                    kind)))))

(defun catalog-transaction-find-by-oid (transaction kind oid)
  (let ((oid-of (ecase kind (:table #'pg-class-oid) (:index #'pg-index-oid))))
    (or (loop for key being the hash-keys of (catalog-transaction-names transaction)
                using (hash-value entry)
              for object = (catalog-transaction-entry-object entry)
              when (and object (eq kind (car key)) (= oid (funcall oid-of object)))
                return object)
        (loop for name being the hash-keys of (catalog-transaction-name-map
                                               (catalog-transaction-database transaction) kind)
                using (hash-value object)
              when (and (= oid (funcall oid-of object))
                        (null (gethash (cons kind name) (catalog-transaction-names transaction))))
                return object))))

(defun catalog-transaction-new-table-indexes (transaction table predicate)
  "Enumerate staged indexes whose OIDs are not in the committed table object."
  (loop for entry being the hash-values of (catalog-transaction-names transaction)
        for object = (catalog-transaction-entry-object entry)
        when (and (pg-index-p object)
                  (= (pg-index-table-oid object) (pg-class-oid table))
                  (not (member (pg-index-oid object) (pg-class-indexes table) :test #'eql))
                  (or (null predicate) (funcall predicate object)))
          collect object))

(defun catalog-transaction-table-indexes (transaction table predicate)
  (nconc (loop for oid in (pg-class-indexes table)
               for index = (catalog-transaction-find-by-oid transaction :index oid)
               when (and index (or (null predicate) (funcall predicate index)))
                 collect index)
         (catalog-transaction-new-table-indexes transaction table predicate)))

(defun catalog-map-visible-relations (function database kind)
  "Visit committed objects and this backend's staged additions and replacements."
  (let* ((transaction (and *backend* (backend-catalog-transaction *backend*)))
         (live (catalog-transaction-name-map database kind)))
    (if (and transaction (eq database (catalog-transaction-database transaction)))
        (let ((names (catalog-transaction-names transaction)))
          (maphash (lambda (name object)
                     (unless (gethash (cons kind name) names)
                       (funcall function name object)))
                   live)
          (maphash (lambda (key entry)
                     (let ((object (catalog-transaction-entry-object entry)))
                       (when (and object (eq (car key) kind))
                         (funcall function (cdr key) object))))
                   names))
        (maphash function live))))

(defun catalog-transaction-observe-table (transaction table)
  "Retain a committed table definition that a staged index depends on."
  (let* ((database (catalog-transaction-database transaction))
         (name (pg-class-name table))
         (committed (gethash name (database-tables database))))
    (when (eq table committed)
      (let ((address (catalog-relation-address database table))
            (dependencies (catalog-transaction-dependencies transaction)))
        (unless (gethash address dependencies)
          (setf (gethash address dependencies)
                (list name table (catalog-relation-definition-version table))))))))

(defun catalog-relation-storage-nodes (object)
  (etypecase object
    (null nil)
    (pg-index (list (pg-index-relfilenode object)))
    (pg-class
     (let ((toast (pg-class-toast object)))
       (if toast
           (list (pg-class-relfilenode object) (pg-toast-relfilenode toast)
                 (pg-toast-index-relfilenode toast))
           (list (pg-class-relfilenode object)))))))

(defun catalog-transaction-displace-relation (transaction previous replacement)
  "Remove owned indexes on table replacement and release abandoned private forks."
  (when (and (pg-class-p previous)
             (or (null replacement)
                 (/= (pg-class-oid previous) (pg-class-oid replacement))))
    ;; The SQL drop path checks constraints and other dependencies before
    ;; staging. Indexes and TOAST storage are internal to their owning table.
    (dolist (index (catalog-transaction-table-indexes transaction previous nil))
      (catalog-transaction-stage transaction :index (pg-index-name index) nil)))
  (when (and previous (plusp (hash-table-count (catalog-transaction-provisional-nodes transaction))))
    (let ((kept (catalog-relation-storage-nodes replacement)))
      (dolist (node (catalog-relation-storage-nodes previous))
        (when (and (not (member node kept :test #'eql))
                   (gethash node (catalog-transaction-provisional-nodes transaction)))
          (unless (and *backend* (eq transaction (backend-catalog-transaction *backend*)))
            (error "cannot discard provisional storage owned by another catalog transaction"))
          (catalog-transaction-drop-provisional-storage node))))))

(defun catalog-table-sequence-dependencies (database table)
  "Record the sequence identities seen by a staged table definition."
  (loop for name in (catalog-table-sequence-names table)
        for sequence = (gethash name (database-sequences database))
        unless sequence do (ereport "42P01" "sequence ~s does not exist" name)
          collect (cons name (pg-sequence-oid sequence))))

(defun catalog-transaction-stage (transaction kind name object)
  "Stage OBJECT, or NIL for a drop, retaining the first base definition.
Return OBJECT and its sequence dependency snapshot for logical capture."
  (when (and (eq kind :index) object)
    (let ((table (catalog-transaction-find-by-oid transaction :table (pg-index-table-oid object))))
      (unless table (ereport "42P01" "index ~a has no owning table" name))
      (unless (catalog-index-owner-compatible-p object table)
        (ereport "42703" "index ~a references a missing column" name))
      (catalog-transaction-observe-table transaction table)))
  (let* ((names (catalog-transaction-names transaction))
         (key (cons kind name))
         (entry (gethash key names))
         (sequence-dependencies (when (and object (eq kind :table))
                                  (catalog-table-sequence-dependencies
                                   (catalog-transaction-database transaction) object)))
         (previous (if entry (catalog-transaction-entry-object entry)
                       (gethash name (catalog-transaction-name-map
                                      (catalog-transaction-database transaction) kind)))))
    (unless entry
      (setf entry (make-catalog-transaction-entry
                   :base previous :base-version (catalog-relation-definition-version previous))))
    (catalog-transaction-displace-relation transaction previous object)
    (cond ((or object (catalog-transaction-entry-base entry))
           (setf (catalog-transaction-entry-object entry) object
                 (catalog-transaction-entry-sequence-dependencies entry) sequence-dependencies
                 (gethash key names) entry))
          (t (remhash key names)))
    (when (or previous object)
      (setf (catalog-transaction-staged-ddl-p transaction) t))
    (values object sequence-dependencies)))

(defun catalog-explicit-publication-table-p (database table)
  "Whether an explicit publication retains this table identity."
  (loop for publication being the hash-values of (database-publications database)
          thereis (and (not (pg-publication-all-tables-p publication))
                       (loop for entry in (pg-publication-tables publication)
                               thereis (and (string= (getf entry :name) (pg-class-name table))
                                            (let ((oid (getf entry :table-oid)))
                                              (or (null oid) (= oid (pg-class-oid table)))))))))

(defun retain-logical-table-sequence-dependencies (transaction name dependencies)
  "Keep a captured CREATE's dependencies even if that table is later dropped."
  (when dependencies
    (push (cons name dependencies) (catalog-transaction-logical-sequence-dependencies transaction))))

(defun validate-catalog-sequence-dependencies (database name dependencies)
  (dolist (dependency dependencies)
    (let ((current (gethash (car dependency) (database-sequences database))))
      (unless (and current (= (pg-sequence-oid current) (cdr dependency)))
        (ereport "40001" "sequence ~a referenced by table ~a changed during transaction"
                 (car dependency) name)))))

(defun catalog-transaction-remove-publication-member (transaction publication table-name)
  "Stage removal of a publication relation dependent on a dropped column."
  (let* ((edits (catalog-transaction-publication-edits transaction))
         (name (pg-publication-name publication))
         (edit (gethash name edits))
         (replacement (copy-pg-publication publication)))
    (setf (pg-publication-tables replacement)
          (remove table-name (pg-publication-tables replacement)
                  :key (lambda (entry) (getf entry :name)) :test #'string=))
    (incf (pg-publication-generation replacement))
    (if edit
        (setf (third edit) replacement)
        (setf (gethash name edits)
              (list publication (pg-publication-generation publication) replacement)))))

(defun catalog-transaction-publications (transaction database)
  "Return the live publication map or a detached view with staged removals."
  (let ((live (database-publications database))
        (edits (and transaction (catalog-transaction-publication-edits transaction))))
    (if (and edits (plusp (hash-table-count edits)))
        (let ((view (make-hash-table :test (hash-table-test live) :size (hash-table-count live))))
          (maphash (lambda (name publication) (setf (gethash name view) publication)) live)
          (maphash (lambda (name edit) (setf (gethash name view) (third edit))) edits)
          view)
        live)))

(defun catalog-publication-filter-column-conflict-p (publications base replacement)
  "Detect a filter installed while a same-OID descriptor change was private."
  (loop for publication being the hash-values of publications
        for entry = (publication-table-entry publication (pg-class-name base))
        for filter = (getf entry :filter)
          thereis
          (when filter
            (let ((expression (analyze-expr filter base))
                  (new-columns (pg-class-columns replacement)))
              (loop for old across (pg-class-columns base)
                    for slot from 0
                    for new = (and (< slot (length new-columns)) (aref new-columns slot))
                      thereis
                      (and (not (pg-column-dropped-p old))
                           (or (null new) (pg-column-dropped-p new)
                               (not (string= (pg-column-name old) (pg-column-name new)))
                               (not (eq (pg-column-type old) (pg-column-type new)))
                               (not (eql (pg-column-typmod old) (pg-column-typmod new))))
                           (stored-expression-references-attnum-p expression (pg-column-attnum old))))))))

(defun validate-catalog-table-dependencies (database publications entry name)
  "Recheck dependencies whose DDL can commit independently of relation admission."
  (let ((base (catalog-transaction-entry-base entry))
        (object (catalog-transaction-entry-object entry)))
    (when (and base (or (null object) (/= (pg-class-oid base) (pg-class-oid object)))
               (catalog-explicit-publication-table-p database base))
      (ereport "40001" "publication membership of table ~a changed during transaction" name))
    (when (and base object (= (pg-class-oid base) (pg-class-oid object))
               (catalog-publication-filter-column-conflict-p publications base object))
      (ereport "40001" "publication filter of table ~a changed during transaction" name))
    (validate-catalog-sequence-dependencies
     database name (catalog-transaction-entry-sequence-dependencies entry))))

(defun catalog-transaction-validate (transaction database)
  "Reject conflicts under the catalog lock before publishing any WAL."
  (unless (eq database (catalog-transaction-database transaction))
    (ereport "40001" "database catalog changed during transaction"))
  (maphash (lambda (name edit)
             (destructuring-bind (base generation replacement) edit
               (declare (ignore replacement))
               (unless (and (eq (gethash name (database-publications database)) base)
                            (= (pg-publication-generation base) generation))
                 (ereport "40001" "publication ~a changed during transaction" name))))
           (catalog-transaction-publication-edits transaction))
  (maphash (lambda (address observation)
             (declare (ignore address))
             (destructuring-bind (name base version) observation
               (let ((current (gethash name (database-tables database))))
                 (unless (and (eq current base)
                              (equal version (catalog-relation-definition-version current)))
                   (ereport "40001" "catalog definition of index owner ~a changed during transaction" name)))))
           (catalog-transaction-dependencies transaction))
  (maphash (lambda (key entry)
             (let* ((name (cdr key))
                    (current (gethash name (catalog-transaction-name-map database (car key)))))
               (unless (and (eq current (catalog-transaction-entry-base entry))
                            (equal (catalog-relation-definition-version current)
                                   (catalog-transaction-entry-base-version entry)))
                 (ereport "40001" "catalog definition of relation ~a changed during transaction" name))
               (when (and (catalog-transaction-entry-object entry)
                          (or (gethash name (database-sequences database))
                              (catalog-transaction-lookup
                               transaction (ecase (car key) (:table :index) (:index :table)) name)))
                 (ereport "40001" "catalog name ~a conflicts with another relation" name))
               (when (eq (car key) :table)
                 (validate-catalog-table-dependencies
                  database (catalog-transaction-publications transaction database) entry name))
               (let ((object (catalog-transaction-entry-object entry)))
                 (when (and (eq (car key) :index) object
                            (not (catalog-index-owner-compatible-p
                                  object (catalog-transaction-find-by-oid transaction :table
                                                                          (pg-index-table-oid object)))))
                   (ereport "40001" "catalog index ~a has an incompatible owning table" name)))))
           (catalog-transaction-names transaction))
  (loop for (name . dependencies) in (catalog-transaction-logical-sequence-dependencies transaction)
        do (validate-catalog-sequence-dependencies database name dependencies))
  (values))

(defun catalog-transaction-merged-name-map (transaction kind)
  "Build a detached map for the commit image without replacing live objects."
  (let* ((database (catalog-transaction-database transaction))
         (live (catalog-transaction-name-map database kind))
         (merged (make-hash-table :test (hash-table-test live) :size (hash-table-count live))))
    (maphash (lambda (name object) (setf (gethash name merged) object)) live)
    (maphash (lambda (key entry)
               (when (eq (car key) kind)
                 (let ((object (catalog-transaction-entry-object entry))
                       (name (cdr key)))
                   (if object (setf (gethash name merged) object)
                       (remhash name merged)))))
             (catalog-transaction-names transaction))
    merged))

(defun catalog-transaction-adjust-index-membership (database transaction)
  "Set the index lists in a detached catalog image without editing admitted tables."
  (let ((removals (make-hash-table :test #'eql))
        (additions (make-hash-table :test #'eql)))
    (maphash
     (lambda (key entry)
       (when (eq (car key) :index)
         (let ((base (catalog-transaction-entry-base entry))
               (index (catalog-transaction-entry-object entry)))
           (when base
             (push (pg-index-oid base) (gethash (pg-index-table-oid base) removals)))
           (when index
             (push (pg-index-oid index) (gethash (pg-index-table-oid index) additions))))))
     (catalog-transaction-names transaction))
    (let ((tables (database-tables database)))
      (maphash
       (lambda (name table)
         (let* ((oid (pg-class-oid table))
                (removed (gethash oid removals))
                (added (gethash oid additions)))
           (when (or removed added)
             (let ((copy (copy-structure table)))
               (setf (pg-class-indexes copy)
                     (union (remove-if (lambda (id) (member id removed :test #'eql))
                                       (pg-class-indexes table))
                            added :test #'eql))
               (setf (gethash name tables) copy)))))
       tables)))
  (values))

(defun catalog-transaction-prepare-database (transaction database)
  "Prepare a detached catalog image while holding the catalog and write locks."
  (catalog-transaction-validate transaction database)
  (let ((view (copy-database database)))
    (setf (database-tables view) (catalog-transaction-merged-name-map transaction :table)
          (database-indexes view) (catalog-transaction-merged-name-map transaction :index)
          (database-publications view) (catalog-transaction-publications transaction database))
    (catalog-transaction-adjust-index-membership view transaction)
    view))

(defun catalog-transaction-deferred-deletes (transaction view)
  "Find displaced committed nodes that the prepared catalog no longer owns."
  (let ((nodes (make-hash-table :test #'eql)))
    (maphash (lambda (key entry)
               (declare (ignore key))
               (dolist (node (catalog-relation-storage-nodes (catalog-transaction-entry-base entry)))
                 (setf (gethash node nodes) t)))
             (catalog-transaction-names transaction))
    (when (plusp (hash-table-count nodes))
      (flet ((retain (name object)
               (declare (ignore name))
               (dolist (node (catalog-relation-storage-nodes object))
                 (remhash node nodes))))
        (maphash #'retain (database-tables view))
        (maphash #'retain (database-indexes view))))
    (sort (loop for node being the hash-keys of nodes collect node) #'<)))

(defun catalog-transaction-prepare-wal (transaction database)
  "Return a validated catalog WAL payload and its detached database."
  (let ((view (catalog-transaction-prepare-database transaction database)))
    (values (wal-encode-catalog-payload
             (catalog-image-bytes view) (catalog-transaction-deferred-deletes transaction view))
            view)))

(defun catalog-transaction-prepare-live-tables (transaction database view)
  "Reuse admitted table descriptors when only their index membership changed."
  (let ((updates nil)
        (staged (catalog-transaction-names transaction))
        (tables (database-tables view)))
    (maphash
     (lambda (name table)
       (let ((prior (gethash name (database-tables database))))
         (when (and prior (not (eq prior table))
                    (null (gethash (cons :table name) staged)))
           (push (cons prior (pg-class-indexes table)) updates)
           (setf (gethash name tables) prior))))
     tables)
    updates))

(defun prepare-catalog-logical-commit (backend transaction database view changes)
  "Check the subscriber contract and attach the publisher contract before sealing WAL."
  (let ((apply-state (catalog-transaction-logical-apply-state transaction))
        (capture-p (catalog-transaction-captured-ddl-p transaction)))
    (when (and apply-state (not (consp apply-state)))
      (ereport "08P01" "missing transactional logical DDL schema contract"))
    (when (or apply-state capture-p)
      (let* ((before (logical-current-schema-contract-under-write-lock database))
             (after (if (catalog-transaction-has-changes-p transaction)
                        (logical-current-schema-contract-under-write-lock view)
                        before)))
        (when (and apply-state
                   (not (and (equal (getf apply-state :schema-before) before)
                             (equal (getf apply-state :schema-after) after))))
          (ereport "55000" "subscriber schema differs from publisher transaction contract"))
        (when capture-p
          (return-from prepare-catalog-logical-commit
            (logical-append-transaction-schema backend (database-oid database) before after)))))
    changes))

(defun commit-backend-wal (backend xid completion changes source local-durability on-durable on-accepted)
  "Commit the backend's WAL, installing any live catalog at the local visibility boundary."
  (let ((transaction (backend-catalog-transaction backend)))
    (unless (or (catalog-transaction-has-changes-p transaction)
                (catalog-transaction-captured-ddl-p transaction)
                (and transaction (catalog-transaction-logical-apply-state transaction)))
      (macrolet ((publish-rows (&rest admission)
                   `(wal-commit-transaction xid completion :logical-changes changes :source-id source
                                                           :local-durability local-durability
                                                           :terminal-durable-function on-durable :accepted-function on-accepted
                                                           ,@admission)))
        (return-from commit-backend-wal
          ;; A transient relation can have row changes with an empty final overlay.
          (if (and transaction (catalog-transaction-staged-ddl-p transaction))
              (publish-rows :catalog-database-oid (database-oid (catalog-transaction-database transaction)))
              (publish-rows)))))
    (with-write-catalog-locks
      (let ((database (current-database)))
        (multiple-value-bind (payload view) (catalog-transaction-prepare-wal transaction database)
          (let ((changes (prepare-catalog-logical-commit backend transaction database view changes))
                (oldest (catalog-compute-oldest-relfrozenxid view))
                (updates (catalog-transaction-prepare-live-tables transaction database view))
                (fresh-nodes (loop for node being the hash-keys of (catalog-transaction-provisional-nodes transaction)
                                   collect node)))
            (wal-commit-transaction
             xid
             (lambda (end)
               ;; WAL has applied the image and CLOG. Pair the live image swap
               ;; with removal from XIP, then leave relation and resource
               ;; ownership to normal transaction cleanup after the reply wait.
               (sb-thread:with-mutex (*xid-lock*)
                 (dolist (update updates)
                   (setf (pg-class-indexes (car update)) (cdr update)))
                 (setf (database-tables database) (database-tables view)
                       (database-indexes database) (database-indexes view)
                       (database-publications database) (database-publications view)
                       *catalog-oldest-relfrozenxid* oldest
                       *active-xids* (delete xid *active-xids* :test #'eql))
                 (incf *catalog-durable-generation*))
               (funcall completion end))
             :logical-changes changes :source-id source :local-durability :flush
             :terminal-durable-function on-durable :accepted-function on-accepted
             :catalog-payload payload :catalog-create-nodes fresh-nodes
             :catalog-database-oid (database-oid database)
             :catalog-logical-ddl (catalog-transaction-logical-ddl-p transaction))))))))

(defun catalog-transaction-create-storage (relfilenode kind)
  "Create a newly reserved node in the owning backend's private fork store."
  (let* ((transaction (backend-catalog-transaction *backend*))
         (nodes (catalog-transaction-provisional-nodes transaction))
         (completed nil))
    (when (or (gethash relfilenode nodes)
              (current-relation-replacement relfilenode :main)
              (probe-file (relation-file-path relfilenode)))
      (error "provisional storage already exists for relation ~d" relfilenode))
    (unwind-protect
         (progn
           (transaction-replace-relation relfilenode)
           (ecase kind
             (:heap nil)
             (:btree (btree-init relfilenode)))
           (sb-sys:without-interrupts
             (setf (gethash relfilenode nodes) t
                   completed t)))
      (unless completed
        (discard-transaction-replacements *backend* relfilenode)
        (setf (backend-dirty-relations *backend*)
              (delete relfilenode (backend-dirty-relations *backend*) :test #'eql)))))
  (values))

(defun catalog-transaction-drop-provisional-storage (relfilenode)
  "Retire an uncommitted node without scheduling a committed-file deletion."
  (let* ((transaction (backend-catalog-transaction *backend*))
         (nodes (catalog-transaction-provisional-nodes transaction)))
    (when (gethash relfilenode nodes)
      (sb-sys:without-interrupts
        (discard-transaction-replacements *backend* relfilenode)
        (setf (backend-dirty-relations *backend*)
              (delete relfilenode (backend-dirty-relations *backend*) :test #'eql))
        (remhash relfilenode nodes)))))
