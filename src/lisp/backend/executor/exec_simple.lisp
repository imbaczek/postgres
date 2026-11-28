;;;; exec_simple.lisp -- simple-query dispatch, DDL, transactions, SET/SHOW.

(in-package #:pg)

(defmacro with-table-ddl ((table &key refresh-snapshot indexes wait-readers ast)
                          &body body)
  "Gate TABLE and INDEXES and revalidate TABLE before publishing BODY.
BODY must revalidate indexes as needed. AST identifies the DDL for a fresh schema
precondition under the write and catalog locks, after acquiring the gates."
  (let* ((table-value (gensym "TABLE-"))
         (index-values (gensym "INDEXES-"))
         (relfilenodes (gensym "RELFILENODES-"))
         (publication
           `(with-write-catalog-locks
              (ensure-current-table ,table-value)
              (call-with-logical-ddl-publication-under-lock
               ,ast (lambda () ,@body)))))
    `(with-schema-write-lock
       (let* ((,table-value ,table)
              (,index-values ,indexes)
              (,relfilenodes
                (cons (pg-class-relfilenode ,table-value)
                      (mapcar #'pg-index-relfilenode ,index-values))))
         (with-relation-build-locks (,relfilenodes ,wait-readers)
           ,(if refresh-snapshot
                `(let ((*current-snapshot* (refresh-statement-snapshot)))
                   ,publication)
                publication))))))

(defun ddl-p (ast)
  (member (getf ast :type)
          '(:create-table :drop-table :create-view :drop-view :create-index :drop-index
            :create-function :drop-function :alter-index :alter-table :create-sequence :alter-sequence
            :drop-sequence :apply-sequence-state
            :create-publication :alter-publication
            :drop-publication)))

(defun catalog-change-p (ast)
  "Return whether AST changes durable catalog state, including ANALYZE."
  (or (ddl-p ast) (eq (getf ast :type) :analyze)))

(defun truncate-table-storage-state (table)
  "Capture every storage identity that TRUNCATE must replace under catalog locks."
  (let ((toast (pg-class-toast table)))
    (list (pg-class-relfilenode table)
          (mapcar (lambda (index) (cons index (pg-index-relfilenode index)))
                  (table-indexes table))
          toast
          (when toast (pg-toast-relfilenode toast))
          (when toast (pg-toast-index-relfilenode toast)))))

(defun check-no-pending-foreign-key-events (table)
  "Protect tuple identities in pending events from storage replacement."
  (let ((oid (pg-class-oid table)))
    (when (loop for (child parent) in (backend-deferred-foreign-key-checks *backend*)
                  thereis (or (eql oid (pg-class-oid child)) (eql oid (pg-class-oid parent))))
      (ereport "55006" "cannot change storage of table ~s because it has pending foreign key checks"
               (pg-class-name table)))))

(defun check-truncate-table-not-in-use (table)
  "Reject storage replacement while this backend retains an executor for TABLE.

An initialized portal can retain its executor after execution completes,
until Close releases it.  Transaction reader registrations alone do not imply
that an executor is still using the table.  Relation gates exclude other backends,
but cannot protect a retained executor in this backend from its own TRUNCATE."
  (when (some (lambda (owner)
                (and (eq *backend* (query-execution-owner-backend owner))
                     (not (query-execution-owner-closed-p owner))
                     (member table
                             (query-execution-owner-binding owner '*query-executor-tables*)
                             :test #'eq)))
              (query-execution-owner-reload-state))
    (ereport "55006" "cannot TRUNCATE \"~a\" because it is being used by active queries in this session"
             (pg-class-name table))))

(defun exec-truncate (stream ast)
  "Replace table, index and TOAST storage while retaining gates through transaction end.
Recheck identities after waiting and under catalog locks before changing storage."
  (get-current-xid)
  ;; TRUNCATE can replace storage covered by our retained snapshot. Give up
  ;; export eligibility before taking the schema write side.
  (release-backend-transaction-schema-lease *backend*)
  (acquire-transaction-schema-write-lock *backend*)
  (let ((names nil) (oids nil) (relations nil) (tables nil))
    (with-schema-write-lock
      (let* ((captured
               (with-write-lock
                 (mapcar
                  (lambda (name)
                    (let ((table (resolve-table-or-error name)))
                      (check-no-pending-foreign-key-events table)
                      (check-truncate-table-not-in-use table)
                      (cons table (truncate-table-storage-state table))))
                  (remove-duplicates (getf ast :tables) :test #'equal :from-end t))))
             (targets (mapcar #'car captured))
             (index-nodes
               (loop for entry in captured
                     for state = (cdr entry)
                     append (append (mapcar #'cdr (second state))
                                    (when (fifth state) (list (fifth state))))))
             (nodes
               (append index-nodes
                       (loop for entry in captured
                             for state = (cdr entry)
                             append (cons (first state)
                                          (when (fourth state) (list (fourth state))))))))
        (dolist (target targets)
          (dolist (reference (referencing-foreign-keys target))
            (unless (member (car reference) targets :test #'eq)
              (ereport "0A000" "cannot truncate a table referenced in a foreign key constraint"))))
        (loop
          (acquire-transaction-relation-locks nodes)
          (let ((current
                  (with-write-lock
                    (mapcar (lambda (table)
                              (ensure-current-table table)
                              (cons table (truncate-table-storage-state table)))
                            targets))))
            (when (equal captured current) (return))
            (setf captured current
                  index-nodes
                  (loop for entry in captured
                        for state = (cdr entry)
                        append (append (mapcar #'cdr (second state))
                                       (when (fifth state) (list (fifth state)))))
                  nodes
                  (append index-nodes
                          (loop for entry in captured
                                for state = (cdr entry)
                                append (cons (first state)
                                             (when (fourth state) (list (fourth state)))))))))
        (with-btree-relation-locks (index-nodes)
          (with-write-catalog-locks
            (dolist (entry captured)
              (ensure-current-table (car entry))
              (unless (equal (cdr entry) (truncate-table-storage-state (car entry)))
                (ereport "55000" "relation \"~a\" changed while waiting for truncate lock"
                         (pg-class-name (car entry)))))
            (dolist (node nodes)
              (transaction-replace-relation node))
            (dolist (table targets)
              (dolist (index (table-indexes table))
                (btree-init (pg-index-relfilenode index)))
              (when (pg-class-toast table)
                (btree-init (pg-toast-index-relfilenode (pg-class-toast table))))
              (push (copy-seq (pg-class-name table)) names)
              (push (pg-class-oid table) oids)
              (push table tables)
              (push (logical-relation-description table) relations))))))
    (logical-note-transaction-event
     :truncate (list :tables (nreverse names) :relation-oids (nreverse oids)
                     :relations (nreverse relations))
     :publication-selection
     (with-write-lock (logical-capture-publications (nreverse tables) :truncate))))
  (send-command-complete stream "TRUNCATE TABLE"))

(defun exec-apply-sequence-state (stream ast)
  (let* ((state (getf ast :state))
         (name (getf state :name))
         (oid (getf state :oid))
         (definition (getf state :definition))
         (last (getf state :last))
         (called (getf state :called))
         (db (current-database))
         (previous nil))
    (unless (and (stringp name)
                 (and (integerp oid) (<= 1 oid #xFFFFFFFF))
                 (stringp definition)
                 (sequence-value-range-p last)
                 (member called '(t nil)))
      (ereport "08P01" "invalid replicated sequence state"))
    (call-with-logical-ddl-durability-rollback
     ast
     (lambda ()
       (when previous
         (setf (gethash name (database-sequences db)) previous)))
     (lambda ()
       (let ((sequence (gethash name (database-sequences db))))
         (unless sequence
           (ereport "42P01" "replicated sequence \"~a\" does not exist" name))
         (unless (string= definition (catalog-sequence-definition-sql sequence))
           (ereport "55000"
                    "replicated sequence \"~a\" definition does not match publisher"
                    name))
         ;; Sequence state events carry values only. ALTER SEQUENCE is a
         ;; separate ordered DDL event and must establish matching definitions.
         (setf previous (copy-pg-sequence sequence)
               (pg-sequence-last-value sequence) last
               (pg-sequence-called-p sequence) called)
         (save-catalog :final-ddl-p t :logical-event (logical-sequence-state-event name)))))
    (send-command-complete stream "APPLY LOGICAL EVENT")))

(defun logical-event-ddl-ast (payload)
  (getf (logical-validate-ddl-event-envelope
         (logical-read-data-string payload))
        :ast))

(defun logical-event-sequence-state-ast (payload)
  (let* ((state (logical-read-data-string payload))
         (source-database-oid (getf state :database-oid)))
    (unless (and (integerp source-database-oid)
                 (<= 1 source-database-oid #xFFFFFFFF))
      (ereport "08P01" "invalid replicated sequence event database"))
    (list :type :apply-sequence-state :state state)))

(defvar *applying-transaction-ddl* nil
  "True while applying an ordered publisher operation inside the subscriber transaction.")

(defun reject-logical-transaction-ddl-private-table (name)
  (when (and *applying-transaction-ddl* (logical-private-metadata-table-name-p name))
    (ereport "42501" "replicated DDL cannot target subscriber metadata relation ~s" name)))

(defun exec-apply-logical-transaction-ddl (stream payload)
  "Stage an ordered operation or record the schema contract for target commit."
  (unless (and (backend-explicit *backend*) (eq (backend-state *backend*) :in-progress))
    (ereport "25001" "transactional logical DDL requires an explicit apply transaction"))
  (multiple-value-bind (ast contract) (logical-decode-transaction-ddl payload)
    (let* ((transaction (backend-catalog-transaction *backend*))
           (state (and transaction (catalog-transaction-logical-apply-state transaction))))
      (cond
        (contract
         (unless (eq state :pending)
           (ereport "08P01" "unexpected transactional logical DDL schema contract"))
         (setf (catalog-transaction-logical-apply-state transaction) contract))
        (ast
         (when (consp state)
           (ereport "08P01" "transactional logical DDL follows its schema contract"))
         (let ((*applying-transaction-ddl* t))
           (recovery-check-statement ast)
           (execute-in-transaction (make-broadcast-stream) ast))
         (setf (catalog-transaction-logical-apply-state
                (backend-catalog-transaction *backend*)) :pending)))))
  (send-command-complete stream "APPLY LOGICAL EVENT"))

(defun exec-apply-logical-event (stream command)
  (unless (and *backend*
               (string= (or (backend-application-name *backend*) "")
                        "pg-lisp-subscribe-apply"))
    (ereport "42501" "APPLY LOGICAL EVENT is restricted to the replication worker"))
  (let ((key (getf command :event-key))
        (lsn (getf command :event-lsn))
        (payload (getf command :payload)))
    (unless (and (stringp key) (<= 1 (length key) 1024)
                 (integerp lsn) (<= 1 lsn #xFFFFFFFFFFFFFFFF)
                 (stringp payload))
      (ereport "08P01" "invalid logical replication event envelope"))
    (when (string= key "transaction-ddl")
      (return-from exec-apply-logical-event
        (exec-apply-logical-transaction-ddl stream payload)))
    (when (or (backend-explicit *backend*) (not (eq (backend-state *backend*) :idle)))
      (ereport "25001" "APPLY LOGICAL EVENT cannot run inside a transaction block"))
    (sb-thread:with-mutex (*logical-event-apply-lock*)
      (let* ((db (current-database))
             (last-lsn (with-write-lock
                         (gethash key
                                  (database-applied-logical-event-ids db)))))
        (when (and last-lsn (>= last-lsn lsn))
          (send-command-complete stream "APPLY LOGICAL EVENT")
          (return-from exec-apply-logical-event nil)))
      (let* ((envelope (logical-read-data-string payload))
             (kind (if (getf envelope :ast) :ddl :sequence-state))
             (schema-contract
               (when (eq kind :ddl)
                 (logical-validate-ddl-event-envelope envelope)))
             (event-ast (ecase kind
                          (:ddl (getf schema-contract :ast))
                          (:sequence-state
                           (logical-event-sequence-state-ast payload))))
             (*logical-event-apply-id* (cons key lsn))
             (*logical-event-schema-contract* schema-contract))
        ;; The subscriber's precondition is checked inside the schema write
        ;; lease; save-catalog checks the expected post-shape before WAL commit.
        ;; This follows the apply-then-feedback ordering in logical/worker.c.
        (execute-in-transaction (make-broadcast-stream) event-ast)))
    (send-command-complete stream "APPLY LOGICAL EVENT")))

(defun exec-create-sequence (stream ast)
  (let* ((db (current-database))
         (name (getf ast :name))
         (increment (getf ast :increment))
         (start (getf ast :start))
         (minimum (getf ast :min))
         (maximum (getf ast :max))
         (created nil))
    (call-with-logical-ddl-durability-rollback
     ast
     (lambda ()
       (when created
         (remhash name (database-sequences db))))
     (lambda ()
       (when (relation-name-in-use-p db name)
         (if (getf ast :if-not-exists)
             (progn
               (send-notice stream
                            (format nil "relation \"~a\" already exists, skipping" name)
                            "NOTICE" "42P07")
               (send-command-complete stream "CREATE SEQUENCE")
               (return-from exec-create-sequence nil))
             (ereport "42P07" "relation \"~a\" already exists" name)))
       (multiple-value-bind (default-min default-max)
           (sequence-default-limits increment)
         (setf minimum (or minimum default-min)
               maximum (or maximum default-max))
         (unless (and (sequence-value-range-p minimum)
                      (sequence-value-range-p maximum)
                      (< minimum maximum))
           (ereport "22023" "MINVALUE (~a) must be less than MAXVALUE (~a)"
                    minimum maximum))
         (unless (<= minimum start maximum)
           (ereport "22023" "sequence start value is out of bounds (~a..~a)"
                    minimum maximum))
         (setf (gethash name (database-sequences db))
               (make-pg-sequence :oid (allocate-oid) :name name
                                 :increment increment :start-value start
                                 :min-value minimum :max-value maximum
                                 :last-value start :called-p nil)
               created t))
       (save-catalog :final-ddl-p t :logical-event
                     (logical-catalog-event-for-ast ast))))
    (send-command-complete stream "CREATE SEQUENCE")))

(defun exec-alter-sequence (stream ast)
  (let* ((db (current-database))
         (name (getf ast :name))
         (previous nil))
    (call-with-logical-ddl-durability-rollback
     ast
     (lambda ()
       (when previous (setf (gethash name (database-sequences db)) previous)))
     (lambda ()
       (let ((sequence (gethash name (database-sequences db))))
         (unless sequence
           (ereport "42P01" "sequence \"~a\" does not exist" name))
         (setf previous (copy-pg-sequence sequence))
         (when (getf ast :increment)
           (let ((increment (getf ast :increment)))
             (unless (and (sequence-value-range-p increment) (not (zerop increment)))
               (ereport "22023" "sequence increment must be a nonzero int8"))
             ;; AlterSequence() in src/backend/commands/sequence.c retains
             ;; current bounds and state when only INCREMENT changes. Defaults
             ;; are selected by init_params() for CREATE or explicit bound edits.
             (setf (pg-sequence-increment sequence) increment)
             (unless (<= (pg-sequence-min-value sequence)
                         (pg-sequence-start-value sequence)
                         (pg-sequence-max-value sequence))
               (ereport "22023" "sequence start value is out of bounds"))))
         (when (getf ast :restart)
           (let ((restart (getf ast :restart)))
             (unless (and (sequence-value-range-p restart)
                          (<= (pg-sequence-min-value sequence) restart
                              (pg-sequence-max-value sequence)))
               (ereport "22003" "sequence restart value is out of bounds"))
             (setf (pg-sequence-last-value sequence) restart
                   (pg-sequence-called-p sequence) nil)))
         (save-catalog :final-ddl-p t :logical-event
                       (logical-catalog-event-for-ast ast)))))
    (send-command-complete stream "ALTER SEQUENCE")))

(defun sequence-default-references (db sequence-name)
  (loop for table being the hash-values of (database-tables db)
        when (member sequence-name (catalog-table-sequence-names table) :test #'string=)
          collect (list (pg-class-name table) sequence-name)))

(defun exec-drop-sequence (stream ast)
  (let ((db (current-database))
        (names (getf ast :names))
        (if-exists (getf ast :if-exists))
        (removed '())
        (publication-state '()))
    (call-with-logical-ddl-durability-rollback
     ast
     (lambda ()
       (dolist (entry removed)
         (setf (gethash (car entry) (database-sequences db)) (cdr entry)))
       (dolist (entry publication-state)
         (setf (pg-publication-sequences (first entry)) (second entry)
               (pg-publication-generation (first entry)) (third entry))))
     (lambda ()
       (dolist (name names)
         (let ((sequence (gethash name (database-sequences db))))
           (unless (or sequence if-exists)
             (ereport "42P01" "sequence \"~a\" does not exist" name))
           (when (and sequence
                      (loop for table being the hash-values of (database-tables db)
                            for view = (pg-class-view-definition table)
                              thereis (and view
                                           (rassoc (pg-sequence-oid sequence)
                                                   (pg-view-definition-sequence-dependencies view)
                                                   :test #'eql))))
             (ereport "2BP01" "cannot drop sequence \"~a\" because a view depends on it" name))
           (when (and sequence (sequence-default-references db name))
             (ereport "2BP01" "cannot drop sequence \"~a\" because a column default depends on it"
                      name))))
       (dolist (name names)
         (let ((sequence (gethash name (database-sequences db))))
           (if sequence
               (progn
                 (push (cons name sequence) removed)
                 (maphash
                  (lambda (publication-name publication)
                    (declare (ignore publication-name))
                    (let ((published (pg-publication-sequences publication)))
                      (when (and (not (eq published :all))
                                 (member name published :test #'string=))
                        (pushnew
                         (list publication published
                               (pg-publication-generation publication))
                         publication-state :key #'first)
                        (setf (pg-publication-sequences publication)
                              (remove name published :test #'string=))
                        (incf (pg-publication-generation publication)))))
                  (database-publications db))
                 (remhash name (database-sequences db)))
               (when if-exists
                 (send-notice stream
                              (format nil "sequence \"~a\" does not exist, skipping" name))))))
       (save-catalog :final-ddl-p t :logical-event
                     (logical-catalog-event-for-ast ast))))
    (send-command-complete stream "DROP SEQUENCE")))

(defun validate-publication-table-entry (entry db)
  (let* ((name (getf entry :name))
         (table (gethash name (database-tables db)))
         (filter (getf entry :filter)))
    (unless table
      (ereport "42P01" "publication table \"~a\" does not exist" name))
    (when (pg-class-view-definition table)
      (ereport "22023" "cannot add view \"~a\" to a publication" name))
    (setf (getf entry :table-oid) (pg-class-oid table))
    (when filter
      (unless (eq (expr-type (analyze-expr filter table)) :bool)
        (ereport "42804" "publication row filter must return boolean")))
    entry))

(defun validate-publication-definition (publication db)
  (unless (every (lambda (action) (member action '(:insert :update :delete :truncate)))
                 (pg-publication-publish-actions publication))
    (ereport "0A000" "unsupported publication actions"))
  (unless (pg-publication-all-tables-p publication)
    (let ((names (mapcar (lambda (entry) (getf entry :name))
                         (pg-publication-tables publication))))
      (when (/= (length names)
                (length (remove-duplicates names :test #'string=)))
        (ereport "42710" "publication lists a table more than once"))
      (dolist (entry (pg-publication-tables publication))
        (validate-publication-table-entry entry db))))
  (unless (eq (pg-publication-sequences publication) :all)
    (when (/= (length (pg-publication-sequences publication))
              (length (remove-duplicates
                       (pg-publication-sequences publication)
                       :test #'string=)))
      (ereport "42710" "publication lists a sequence more than once"))
    (dolist (name (pg-publication-sequences publication))
      (unless (gethash name (database-sequences db))
        (ereport "42P01" "publication sequence \"~a\" does not exist" name))))
  publication)

(defun exec-create-publication (stream ast)
  (let* ((db (current-database))
         (name (getf ast :name))
         (publication
           (make-pg-publication :name name
                                :all-tables-p (getf ast :all-tables-p)
                                :tables (copy-tree (getf ast :tables))
                                :sequences (copy-tree (getf ast :sequences))
                                :generation 1
                                :publish-actions
                                (copy-list (getf ast :publish-actions
                                                 '(:insert :update :delete :truncate)))))
         (created nil))
    (call-with-logical-ddl-durability-rollback
     ast
     (lambda ()
       (when created
         (remhash name (database-publications db))))
     (lambda ()
       (when (gethash name (database-publications db))
         (ereport "42710" "publication \"~a\" already exists" name))
       (validate-publication-definition publication db)
       (setf (pg-publication-oid publication) (allocate-oid))
       (setf (gethash name (database-publications db)) publication
             created t)
       (save-catalog :final-ddl-p t :logical-event
                     (logical-catalog-event-for-ast ast))))
    (send-command-complete stream "CREATE PUBLICATION")))

(defun alter-publication-tables (publication ast)
  (let ((entries (copy-tree (pg-publication-tables publication))))
    (when (and (pg-publication-all-tables-p publication)
               (not (eq (getf ast :action) :set-table)))
      (ereport "0A000" "use ALTER PUBLICATION SET TABLE before editing an all-tables publication"))
    (case (getf ast :action)
      (:add-table
       (dolist (entry (getf ast :tables))
         (when (find (getf entry :name) entries
                     :key (lambda (current) (getf current :name))
                     :test #'string=)
           (ereport "42710" "publication table \"~a\" is already present"
                    (getf entry :name)))
         (push (copy-tree entry) entries)))
      (:drop-table
       (dolist (entry (getf ast :tables))
         (let ((name (getf entry :name)))
           (unless (find name entries :key (lambda (current)
                                             (getf current :name))
                                      :test #'string=)
             (ereport "42704" "publication table \"~a\" is not present" name))
           (setf entries
                 (remove name entries :key (lambda (current)
                                             (getf current :name))
                                      :test #'string=)))))
      (:set-table (setf entries (copy-tree (getf ast :tables)))))
    (setf (pg-publication-tables publication) (nreverse entries)
          (pg-publication-all-tables-p publication) nil)))

(defun alter-publication-sequences (publication ast)
  (let ((sequences (pg-publication-sequences publication)))
    (case (getf ast :action)
      (:add-sequence
       (when (eq sequences :all)
         (ereport "42710" "publication already includes all sequences"))
       (setf sequences (copy-list sequences))
       (dolist (name (getf ast :sequences))
         (when (member name sequences :test #'string=)
           (ereport "42710" "publication sequence \"~a\" is already present" name))
         (push name sequences)))
      (:drop-sequence
       (when (eq sequences :all)
         (ereport "0A000" "use ALTER PUBLICATION SET SEQUENCES before dropping a sequence"))
       (setf sequences (copy-list sequences))
       (dolist (name (getf ast :sequences))
         (unless (member name sequences :test #'string=)
           (ereport "42704" "publication sequence \"~a\" is not explicitly present" name))
         (setf sequences (remove name sequences :test #'string=))))
      (:set-sequences
       (setf sequences (copy-tree (getf ast :sequences)))))
    (setf (pg-publication-sequences publication) sequences)))

(defun exec-alter-publication (stream ast)
  (let* ((db (current-database))
         (name (getf ast :name))
         (previous nil))
    (call-with-logical-ddl-durability-rollback
     ast
     (lambda ()
       (when previous
         (setf (gethash name (database-publications db)) previous)))
     (lambda ()
       (let ((current (gethash name (database-publications db))))
         (unless current
           (ereport "42704" "publication \"~a\" does not exist" name))
         (setf previous current)
         (let ((candidate (copy-pg-publication current)))
           (setf (pg-publication-tables candidate)
                 (copy-tree (pg-publication-tables current))
                 (pg-publication-sequences candidate)
                 (copy-tree (pg-publication-sequences current)))
           (cond
             ((eq (getf ast :action) :set-options)
              (setf (pg-publication-publish-actions candidate)
                    (copy-list (getf ast :publish-actions))))
             ((member (getf ast :action) '(:add-table :drop-table :set-table))
              (alter-publication-tables candidate ast))
             (t (alter-publication-sequences candidate ast)))
           (incf (pg-publication-generation candidate))
           (validate-publication-definition candidate db)
           (setf (gethash name (database-publications db)) candidate)
           (save-catalog :final-ddl-p t :logical-event (logical-catalog-event-for-ast ast))))))
    (send-command-complete stream "ALTER PUBLICATION")))

(defun exec-drop-publication (stream ast)
  (let ((db (current-database))
        (if-exists (getf ast :if-exists))
        (removed '()))
    (call-with-logical-ddl-durability-rollback
     ast
     (lambda ()
       (dolist (entry removed)
         (setf (gethash (car entry) (database-publications db)) (cdr entry))))
     (lambda ()
       (dolist (name (getf ast :names))
         (unless (or (gethash name (database-publications db)) if-exists)
           (ereport "42704" "publication \"~a\" does not exist" name)))
       (dolist (name (getf ast :names))
         (let ((publication (gethash name (database-publications db))))
           (if publication
               (progn (push (cons name publication) removed)
                      (remhash name (database-publications db)))
               (when if-exists
                 (send-notice stream
                              (format nil "publication \"~a\" does not exist, skipping"
                                      name))))))
       (save-catalog :final-ddl-p t :logical-event
                     (logical-catalog-event-for-ast ast))))
    (send-command-complete stream "DROP PUBLICATION")))

(defun note-query-error ()
  "Record that the current statement failed, matching PostgreSQL.

An error inside an explicit transaction block moves it to the aborted
state even when it happens while parsing, before EXECUTE-STATEMENT has
run, so the next command reports 25P02.  An implicit transaction is
rolled back immediately."
  (when *backend*
    (when (eq (backend-state *backend*) :in-progress)
      (if (backend-explicit *backend*)
          (abort-transaction-block)
          (abort-transaction)))
    (wal-cleanup-backend-physical-slots *backend*)))

;;; ---- transaction commands ----

(defun copy-session-settings (settings)
  (let ((copy (make-hash-table :test (hash-table-test settings))))
    (maphash (lambda (key value) (setf (gethash key copy) value)) settings)
    copy))

(defun restore-session-settings (stream)
  "Restore the saved settings table after abort and republish the activity value.
Do not perform wire I/O while restoring transaction state."
  (declare (ignore stream))
  (sb-sys:without-interrupts
    (let ((settings (backend-settings-transaction-start *backend*)))
      (when settings
        (setf (backend-settings *backend*) settings
              (backend-settings-transaction-start *backend*) nil)
        (session-publish-application-name)))))

(declaim (ftype function exec-set-transaction))

(defun command-begin (stream ast)
  (when (and (member :read-only ast) (not (getf ast :read-only)))
    (recovery-check-read-write))
  (when (and (eq (backend-state *backend*) :in-progress)
             (backend-implicit-query-block *backend*)
             (not (backend-explicit *backend*)))
    (setf (backend-explicit *backend*) t
          (backend-implicit-query-block *backend*) nil)
    (exec-set-transaction ast)
    (send-command-complete stream "BEGIN")
    (return-from command-begin))
  (if (eq (backend-state *backend*) :idle)
      (progn
        (clear-backend-command-id-used *backend*)
        (setf (backend-explicit *backend*) t
              (backend-isolation-level *backend*)
              (or (getf ast :isolation-level) :read-committed)
              (backend-read-only *backend*) (or (recovery-in-progress-p) (getf ast :read-only))
              (backend-command-id *backend*) 0
              (backend-xid *backend*) nil
              (backend-snapshot *backend*) nil
              (backend-transaction-snapshot *backend*) nil
              (backend-transaction-snapshot-registration *backend*) nil
              (backend-first-snapshot-set *backend*) nil
              (backend-dirty-relations *backend*) '()
              (backend-settings-transaction-start *backend*)
              (copy-session-settings (backend-settings *backend*)))
        (set-backend-state *backend* :in-progress))
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
     ;; Commit record and target progress are durable before the worker can
     ;; acknowledge the publisher, as logical/worker.c expects.
     (when (string= (or (backend-application-name *backend*) "")
                    "pg-lisp-subscribe-apply")
       (logical-test-failpoint :after-target-commit-before-command-response))
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
  (if (recovery-in-progress-p)
      (physical-recovery-checkpoint)
      (wal-checkpoint))
  (send-command-complete stream "CHECKPOINT"))

;;; ---- SET / SHOW / RESET ----

(defun known-show-setting-p (name)
  (member name '("transaction_isolation" "server_version"
                 "server_version_num" "client_encoding" "server_encoding"
                 "timezone" "datestyle" "standard_conforming_strings"
                 "is_superuser" "search_path")
          :test #'string=))

(defun valid-timezone-value-p (value)
  (and (sql-timezone-name-valid-p value)
       (handler-case (progn (sql-load-timezone value) t)
         (error () nil))))

(defun transaction-isolation-setting-value (value)
  (cond
    ((and (stringp value) (string-equal value "read committed"))
     :read-committed)
    ((and (stringp value) (string-equal value "repeatable read"))
     :repeatable-read)
    ((and (stringp value) (string-equal value "serializable"))
     (ereport "0A000" "serializable is not supported"))
    ((and (stringp value) (string-equal value "read uncommitted"))
     (ereport "0A000" "read uncommitted is not supported"))
    (t
     (ereport "22023" "invalid value for parameter \"transaction_isolation\": ~s"
              value))))

(defvar *executing-do-p* nil)

(defun exec-set (stream ast)
  (let* ((name (or (and (getf ast :name)
                        (string-downcase (getf ast :name)))
                   ""))
         (raw-value (getf ast :value))
         (value (if (eq raw-value :default)
                    (cond
                      ((string= name "timezone") "UTC")
                      ((string= name "transaction_isolation") "read committed")
                      ((string= name "client_encoding") "UTF8")
                      ((string= name "standard_conforming_strings") "on")
                      (t :default))
                    raw-value)))
    (when (and (eq raw-value :default) (not (session-extra-setting-p name))
               (nth-value 1 (gethash name (backend-settings-reset *backend*))))
      (setf raw-value (gethash name (backend-settings-reset *backend*)) value raw-value))
    (startup-check-session-setting name)
    (cond
      ((and (member name *session-local-setting-names* :test #'string=)
            (getf ast :local-p))
       (if (or *executing-do-p* (backend-explicit *backend*) (backend-implicit-query-block *backend*))
           (cond ((string= name "application_name") (session-set-application-name raw-value t))
                 ((string= name "work_mem") (session-set-work-memory raw-value t))
                 (t (session-set-synchronous-commit raw-value t)))
           (progn
             (send-notice stream "SET LOCAL can only be used in transaction blocks" "WARNING" "25P01")
             (unless (or (eq raw-value :default) (eq raw-value +null+))
               (cond ((string= name "application_name") (session-application-name-value raw-value))
                     ((string= name "work_mem") (session-normalize-extra-setting name raw-value))
                     (t (session-normalize-synchronous-commit raw-value)))))))
      ((session-extra-setting-p name) (session-set-extra-setting name raw-value))
      ((string= name "search_path")
       (set-session-search-path (if (eq raw-value :default) +null+ value)))
      ((getf ast :transaction-options-p)
       (exec-set-transaction (getf ast :transaction-options)))
      ;; Session characteristics set defaults for future transactions. This
      ;; server's only supported default is READ COMMITTED, so accepting it is
      ;; a no-op even when the current transaction uses REPEATABLE READ.
      ((getf ast :session-characteristics-p)
       (values))
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
       )
      ((string= name "transaction_isolation")
       (let ((isolation-level (transaction-isolation-setting-value value)))
         (if (and (or *executing-do-p* (backend-explicit *backend*))
                  (eq (backend-state *backend*) :in-progress))
             (exec-set-transaction (list :isolation-level isolation-level))
             (unless (eq isolation-level :read-committed)
               (ereport "0A000"
                        "session transaction isolation defaults are not supported")))))
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

(defun exec-set-transaction-snapshot (stream ast)
  (import-exported-logical-snapshot (getf ast :token))
  (send-command-complete stream "SET"))

(defun exec-set-transaction (characteristics)
  (let* ((backend *backend*)
         (isolation-specified-p (member :isolation-level characteristics))
         (new-isolation (getf characteristics :isolation-level))
         (read-mode-specified-p (member :read-only characteristics))
         (new-read-only (getf characteristics :read-only)))
    (unless (and (or *executing-do-p* (backend-explicit backend))
                 (eq (backend-state backend) :in-progress))
      (ereport "25001" "SET TRANSACTION can only be used in transaction blocks"))
    ;; PostgreSQL permits idempotent isolation changes after the first query.
    ;; A real isolation change must happen before the transaction snapshot.
    (when (and isolation-specified-p
               (not (eq new-isolation (backend-isolation-level backend)))
               (backend-first-snapshot-set backend))
      (ereport "25001"
               "SET TRANSACTION ISOLATION LEVEL must be called before any query"))
    ;; Like PostgreSQL's transaction_read_only GUC, READ WRITE cannot be
    ;; enabled in a read-only transaction after its first snapshot. Switching
    ;; a read-write transaction to read-only remains legal after reads.
    (when (and read-mode-specified-p
               (backend-read-only backend)
               (not new-read-only)
               (backend-first-snapshot-set backend))
      (ereport "25001"
               "transaction read-write mode must be set before any query"))
    (when (and read-mode-specified-p (not new-read-only))
      (recovery-check-read-write))
    (when isolation-specified-p
      (setf (backend-isolation-level backend)
            new-isolation))
    (when read-mode-specified-p
      (setf (backend-read-only backend)
            new-read-only)))
  (values))

(defun exec-reset (stream ast)
  (let ((name (string-downcase (getf ast :name))))
    (startup-check-session-setting name)
    (cond
      ((session-extra-setting-p name) (session-set-extra-setting name :default))
      ((string= name "search_path")
       (set-session-search-path (gethash name (backend-settings-reset *backend*) +null+)))
      ((string= name "all")
       (set-session-search-path +null+)
       (session-reset-all-settings)
       )
      ((string= name "timezone")
       (let ((baseline (gethash "timezone" (backend-settings-reset *backend*))))
         (if baseline (setf (gethash "timezone" (backend-settings *backend*)) baseline)
             (remhash "timezone" (backend-settings *backend*))))
       )
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

(defun sql-setting-value (name &optional missing-ok)
  "Shared SHOW/current_setting value path, using C's human unit formatting."
  (setf name (string-downcase name))
  (when (string= name "effective_wal_level")
    (return-from sql-setting-value (startup-effective-wal-level)))
  (multiple-value-bind (value present-p) (session-extra-setting-value name)
    (when present-p (return-from sql-setting-value value)))
  (multiple-value-bind (value present-p) (startup-setting-show-value name)
    (when present-p (return-from sql-setting-value value)))
  (cond
    ((string= name "transaction_isolation")
     (ecase (backend-isolation-level *backend*)
       (:read-committed "read committed") (:repeatable-read "repeatable read")))
    ((string= name "transaction_read_only")
     (if (backend-transaction-read-only-p) "on" "off"))
    ((string= name "in_hot_standby") (if (recovery-in-progress-p) "on" "off"))
    ((string= name "default_transaction_read_only") "off")
    ((string= name "wal_segment_size") (startup-format-unit +wal-segment-size+ :bytes))
    ((string= name "data_directory_mode")
     (format nil "~4,'0o" (logand #o777 (sb-posix:stat-mode (sb-posix:stat (namestring *pgdata*))))))
    ((string= name "server_version") *server-version*)
    ((string= name "server_version_num") (princ-to-string *server-version-num*))
    ((string= name "search_path") (session-search-path))
    ((member name '("client_encoding" "server_encoding") :test #'string=) "UTF8")
    ((string= name "timezone") (or (gethash "timezone" (backend-settings *backend*)) "UTC"))
    ((string= name "datestyle") "ISO, MDY")
    ((string= name "standard_conforming_strings") "on")
    ((string= name "is_superuser") "on")
    (missing-ok +null+)
    (t (ereport "42704" "unrecognized configuration parameter ~s" name))))

(defun exec-show (stream ast)
  (let* ((name (string-downcase (getf ast :name)))
         (value (sql-setting-value name)))
    (send-row-description
     stream (list (make-result-column :name name :type-oid +textoid+
                                      :typlen -1 :typmod -1)))
    (send-data-row-text stream (list value))
    (send-command-complete stream "SHOW")))

;;; ---- DDL ----

(defun resolve-attnums (table names)
  (mapcar (lambda (n)
            (pg-column-attnum (resolve-column-or-error table n)))
          names))

(defun resolve-foreign-key-target (table names &optional (indexes (table-indexes table)))
  "Return the referenced attnums in SQL order and the supporting unique index.
An omitted column list uses the primary key's order."
  (let ((attnums (and names (resolve-attnums table names))))
    (when (and attnums (/= (length attnums) (length (remove-duplicates attnums))))
      (ereport "42830" "foreign key referenced-columns list must not contain duplicates"))
    (let ((index
            (find-if (lambda (candidate)
                       (and (pg-index-live-p candidate)
                            (pg-index-ready-p candidate)
                            (pg-index-valid-p candidate)
                            (pg-index-unique-p candidate)
                            (not (index-has-expressions-p candidate))
                            (if attnums
                                (let ((keys (pg-index-columns candidate)))
                                  (and (= (length keys) (length attnums))
                                       (subsetp attnums keys :test #'eql)))
                                (pg-index-primary-p candidate))))
                     indexes)))
      (unless index
        (if names
            (ereport "42830" "there is no unique constraint matching given keys for referenced table ~s"
                     (pg-class-name table))
            (ereport "42830" "there is no primary key for referenced table ~s" (pg-class-name table))))
      (values (or attnums (copy-list (pg-index-columns index))) index))))

(defun cook-foreign-key (child parent definition oid &optional (parent-indexes (table-indexes parent)))
  "Bind a parsed constraint to the target index and to physical attribute identities."
  (let* ((names (getf definition :columns))
         (columns (resolve-attnums child names))
         (base (format nil "~a_~{~a~^_~}_fkey" (pg-class-name child) names))
         (name (or (getf definition :name)
                   (loop for suffix from 0
                         for candidate = (if (zerop suffix) base (format nil "~a~d" base suffix))
                         unless (table-constraint-name-used-p child candidate) return candidate))))
    (when (table-constraint-name-used-p child name)
      (ereport "42710" "constraint ~a for relation ~a already exists" name (pg-class-name child)))
    (multiple-value-bind (targets index)
        (resolve-foreign-key-target parent (getf definition :referenced-columns) parent-indexes)
      (unless (= (length columns) (length targets))
        (ereport "42830" "number of referencing and referenced columns for foreign key disagree"))
      (loop for child-slot in columns
            for parent-slot in targets
            for child-type = (pg-column-type (aref (pg-class-columns child) (1- child-slot)))
            for parent-type = (pg-column-type (aref (pg-class-columns parent) (1- parent-slot)))
            unless (or (eq child-type parent-type)
                       (and (member child-type '(:int2 :int4 :int8))
                            (member parent-type '(:int2 :int4 :int8)))
                       (and (member child-type '(:text :varchar))
                            (member parent-type '(:text :varchar))))
              do (ereport "42804" "foreign key constraint ~a cannot match ~a to ~a"
                          name child-type parent-type))
      (make-pg-foreign-key :oid oid :name name :columns columns :referenced-columns targets
                           :referenced-table-oid (pg-class-oid parent)
                           :referenced-index-oid (pg-index-oid index)
                           :update-action (getf definition :update-action :no-action)
                           :delete-action (getf definition :delete-action :no-action)
                           :match-type (getf definition :match-type :simple)
                           :deferrable-p (getf definition :deferrable)
                           :initially-deferred-p (getf definition :initially-deferred)
                           :validated-p (not (getf definition :not-valid))))))

(defstruct child-foreign-key-probe
  constraint parent index slots types)

(defun prepare-child-foreign-key-probe (constraint parent index)
  "Map child row slots to the referenced index's physical key order."
  (let ((child-columns (pg-foreign-key-columns constraint))
        (parent-columns (pg-foreign-key-referenced-columns constraint)))
    (make-child-foreign-key-probe
     :constraint constraint :parent parent :index index
     :slots (mapcar (lambda (attnum)
                      (1- (nth (position attnum parent-columns :test #'eql) child-columns)))
                    (pg-index-columns index))
     :types (index-types index parent))))

(defun prepare-child-foreign-key-probes (child)
  "Resolve the counterpart descriptors after the statement has admitted them."
  (when (pg-class-foreign-keys child)
    (with-write-lock
      (loop for constraint in (pg-class-foreign-keys child)
            for parent = (find-table-by-oid (pg-foreign-key-referenced-table-oid constraint))
            for index = (find-index-by-oid (pg-foreign-key-referenced-index-oid constraint))
            collect (prepare-child-foreign-key-probe constraint parent index)))))

(defun child-foreign-key-integer-overflow-p (value type)
  (case type
    (:int2 (not (typep value '(signed-byte 16))))
    (:int4 (not (typep value '(signed-byte 32))))
    (:int8 (not (typep value '(signed-byte 64))))))

(defun encode-child-foreign-key (probe row &optional varlena columns)
  "Encode ROW in the referenced index's domain, or return a reason it has no key."
  (let ((slots (child-foreign-key-probe-slots probe))
        (types (child-foreign-key-probe-types probe)))
    (when (and slots (null (rest slots)))
      (let* ((slot (first slots))
             (value (aref row slot))
             (type (first types)))
        (return-from encode-child-foreign-key
          (cond ((eq value +null+) (values nil :null))
                ((child-foreign-key-integer-overflow-p value type) (values nil :out-of-range))
                (t (values
                    (or (and varlena (stringp value)
                             (member type '(:text :varchar))
                             (member (pg-column-type (aref columns slot)) '(:text :varchar))
                             (encode-index-inline-text-key (aref varlena slot)))
                        (encode-index-key (list value) types))
                    :key)))))))
  (let* ((values (mapcar (lambda (slot) (aref row slot)) (child-foreign-key-probe-slots probe)))
         (types (child-foreign-key-probe-types probe))
         (nulls (count +null+ values :test #'eq)))
    (cond ((plusp nulls)
           (values nil (if (and (< nulls (length values))
                                (eq :full (pg-foreign-key-match-type
                                           (child-foreign-key-probe-constraint probe))))
                           :mixed-null
                           :null)))
          ((loop for value in values for type in types
                   thereis (child-foreign-key-integer-overflow-p value type))
           (values nil :out-of-range))
          (t (values (encode-index-key values types) :key)))))

(defun foreign-key-child-index-key (constraint parent index row)
  "Encode one child key when no prepared probe is available."
  (encode-child-foreign-key (prepare-child-foreign-key-probe constraint parent index) row))

(defun foreign-key-parent-visible-p (parent index key snapshot &optional probe read-parent-page)
  "Probe the referenced index for a parent visible to SNAPSHOT."
  (let ((node (pg-class-relfilenode parent)))
    (flet ((visible (block offnum)
             (check-for-interrupts)
             (if read-parent-page
                 (heapam-page-tid-visible-p (funcall read-parent-page block) offnum snapshot)
                 (heapam-tid-visible-p node block offnum snapshot))))
      (declare (dynamic-extent #'visible))
      (if probe
          (funcall probe key #'visible)
          (btree-some-equal-tid (pg-index-relfilenode index) key #'visible)))))

(defun check-foreign-key-parent (child parent constraint index key latest retained &optional probe read-parent-page)
  "Probe under already acquired key grants and the statement's check horizons."
  (let ((has-parent (foreign-key-parent-visible-p parent index key latest probe read-parent-page)))
    (when retained
      (let ((previously-visible (foreign-key-parent-visible-p parent index key retained probe read-parent-page)))
        (when (and previously-visible (not has-parent))
          (ereport "40001" "could not serialize access due to concurrent update"))
        (setf has-parent (and has-parent previously-visible))))
    (unless has-parent
      (ereport "23503" "insert or update on table ~s violates foreign key constraint ~s"
               (pg-class-name child) (pg-foreign-key-name constraint)))))

(defun lock-foreign-key-child-references (child probes row tid &optional old-row old-own-p varlena)
  "Hold new non-null child references before probing at statement end."
  (loop for probe in probes
        for constraint = (child-foreign-key-probe-constraint probe)
        unless (and old-row (not (and old-own-p (foreign-key-deferred-p constraint)))
                    (loop for slot in (child-foreign-key-probe-slots probe)
                          for before = (aref old-row slot)
                          for after = (aref row slot)
                          always (or (eql before after) (true-p (sql-compare "=" before after)))))
          append
          (let ((parent (child-foreign-key-probe-parent probe))
                (index (child-foreign-key-probe-index probe)))
            (multiple-value-bind (key status) (encode-child-foreign-key probe row varlena (pg-class-columns child))
              (ecase status
                (:null nil)
                (:key
                 (let ((grant (acquire-referenced-key-lock (pg-index-oid index) key :shared)))
                   (list (list parent constraint index key tid grant))))
                ((:mixed-null :out-of-range)
                 (cond ((foreign-key-deferred-p constraint)
                        (list (list parent constraint index status tid)))
                       ((eq status :mixed-null)
                        (ereport "23503" "MATCH FULL does not allow mixing null and nonnull key values"))
                       (t
                        (ereport "23503" "insert or update on table ~s violates foreign key constraint ~s"
                                 (pg-class-name child) (pg-foreign-key-name constraint))))))))))

(defun foreign-key-deferred-p (constraint)
  (when (pg-foreign-key-deferrable-p constraint)
    (let* ((modes (backend-foreign-key-constraint-modes *backend*))
           (override (or (assoc (pg-foreign-key-oid constraint) modes)
                         (assoc :all modes))))
      (if override (eq (cdr override) :deferred)
          (pg-foreign-key-initially-deferred-p constraint)))))

(defun queue-deferred-foreign-key-check (child parent constraint index key &optional child-tid)
  (push (list child parent constraint index key child-tid)
        (backend-deferred-foreign-key-checks *backend*)))

(defun foreign-key-queued-child-visible-p (child identity snapshot)
  "Check only the inserted version that queued the child-side event."
  (destructuring-bind (block off cid) identity
    (let ((tuple (heapam-get-tuple (pg-class-relfilenode child) block off (pg-class-columns child))))
      (and tuple (eql (htup-xmin tuple) (backend-xid *backend*))
           (htup-visible-p tuple snapshot)
           (eql cid (htup-cid tuple))))))

(defun check-deferred-foreign-key-event (event latest retained)
  (destructuring-bind (child parent constraint index key child-tid) event
    (when (if child-tid
              (foreign-key-queued-child-visible-p child child-tid latest)
              (foreign-key-child-visible-p child parent constraint index key latest))
      (let* ((latest-parent (and (not (symbolp key))
                                 (foreign-key-parent-visible-p parent index key latest)))
             (retained-parent (and retained child-tid (not (symbolp key))
                                   (foreign-key-parent-visible-p parent index key retained))))
        (when (and retained
                   (if child-tid
                       (and retained-parent (not latest-parent))
                       (and (not latest-parent)
                            (not (foreign-key-child-visible-p child parent constraint index key retained)))))
          (ereport "40001" "could not serialize access due to concurrent update"))
        (unless (and latest-parent (or (null retained) (null child-tid) retained-parent))
          (ereport "23503" "foreign key constraint ~s on table ~s has no referenced row in ~s"
                   (pg-foreign-key-name constraint) (pg-class-name child) (pg-class-name parent)))))))

(defmacro with-fresh-foreign-key-snapshot ((snapshot command-id) &body body)
  "Read the current visibility horizon at COMMAND-ID, with registered cleanup."
  (let ((registration (gensym "REGISTRATION-")))
    `(with-statement-snapshot
         (,snapshot (lambda (,registration) (make-snapshot ,command-id :register ,registration)))
       ,@body)))

(defun run-deferred-foreign-key-checks (&optional constraint-oids)
  "Check surviving references for selected constraints, or all pending checks."
  (let* ((cid (1+ (backend-command-id *backend*)))
         (base (backend-transaction-snapshot *backend*))
         (retained (when (and base (member (backend-isolation-level *backend*) '(:repeatable-read :serializable)))
                     (copy-snapshot base)))
         (remaining nil))
    (when retained (setf (snapshot-curcid retained) cid))
    (with-fresh-foreign-key-snapshot (latest cid)
      (dolist (event (backend-deferred-foreign-key-checks *backend*))
        (if (or (null constraint-oids) (member (pg-foreign-key-oid (third event)) constraint-oids))
            (check-deferred-foreign-key-event event latest retained)
            (push event remaining))))
    (setf (backend-deferred-foreign-key-checks *backend*) (nreverse remaining)))
  (values))

(defun foreign-key-mode-targets (names mode)
  "Resolve all visible foreign key constraints with each requested name."
  (let ((matches (make-hash-table :test #'equal)))
    (with-write-lock
      (catalog-map-visible-relations
       (lambda (name table)
         (declare (ignore name))
         (dolist (constraint (pg-class-foreign-keys table))
           (when (member (pg-foreign-key-name constraint) names :test #'string=)
             (push constraint (gethash (pg-foreign-key-name constraint) matches)))))
       (current-database) :table))
    (loop for name in names
          for constraints = (gethash name matches)
          unless constraints do (ereport "42704" "constraint ~s does not exist" name)
            append (loop for constraint in constraints
                         when (and (eq :deferred mode) (not (pg-foreign-key-deferrable-p constraint)))
                           do (ereport "42809" "constraint ~s is not deferrable" name)
                         collect (pg-foreign-key-oid constraint)))))

(defun exec-set-constraints (stream ast)
  "Change transaction-local timing, first checking events made immediate."
  (let* ((names (getf ast :names))
         (mode (getf ast :mode))
         (targets (when names (foreign-key-mode-targets names mode))))
    (when (and (eq mode :immediate) (backend-deferred-foreign-key-checks *backend*))
      (run-deferred-foreign-key-checks targets))
    (if names
        (dolist (oid targets)
          (push (cons oid mode) (backend-foreign-key-constraint-modes *backend*)))
        (setf (backend-foreign-key-constraint-modes *backend*) (list (cons :all mode)))))
  (send-command-complete stream "SET CONSTRAINTS"))

(defun check-foreign-key-child-references (child references)
  "Check immediate references under one fresh snapshot after acquiring all key grants."
  (when references
    (let ((checked nil))
      (labels ((check-reference (reference latest retained probe read-parent-page)
                 (destructuring-bind (parent constraint index key tid &optional grant) reference
                   (cond ((foreign-key-deferred-p constraint)
                          (queue-deferred-foreign-key-check child parent constraint index key tid))
                         ((not (and grant (gethash grant checked)))
                          ;; A grant identifies an index and canonical key.
                          ;; Immediate probes share the same visibility horizons.
                          (check-foreign-key-parent child parent constraint index key latest retained probe
                                                    read-parent-page)
                          (when grant (setf (gethash grant checked) t))))))
               (check-references (latest retained)
                 (loop with pending = references
                       while pending
                       for index = (third (first pending))
                       do (if (and (rest pending) (eq index (third (second pending))))
                              (call-with-btree-equal-tid-probe
                               (pg-index-relfilenode index)
                               (lambda (probe)
                                 ;; The registered snapshots protect heap versions and
                                 ;; the index lease prevents TID reuse in this chunk.
                                 (call-with-buf-read-page-cache
                                  (pg-class-relfilenode (first (first pending)))
                                  (lambda (read-parent-page)
                                    ;; Yield the lease between chunks during large COPY.
                                    (loop repeat 64
                                          while (and pending (eq index (third (first pending))))
                                          do (check-reference (pop pending) latest retained
                                                              probe read-parent-page))))))
                              (check-reference (pop pending) latest retained nil nil)))))
        (if (every (lambda (reference) (foreign-key-deferred-p (second reference))) references)
            (dolist (reference references) (check-reference reference nil nil nil nil))
            (let ((cid (1+ (backend-command-id *backend*)))
                  (retained
                    (when (member (backend-isolation-level *backend*) '(:repeatable-read :serializable))
                      (copy-snapshot *current-snapshot*))))
              (setf checked (make-hash-table :test #'eq))
              (when retained (setf (snapshot-curcid retained) cid))
              (with-fresh-foreign-key-snapshot (latest cid)
                (check-references latest retained))))))))

(defun referencing-foreign-keys-locked (parent)
  "Read child constraints in the current catalog view while holding the write lock."
  (let* ((transaction (backend-catalog-transaction *backend*))
         (tables (if transaction
                     (catalog-transaction-merged-name-map transaction :table)
                     (database-tables (current-database)))))
    (loop for child being the hash-values of tables
          append (loop for constraint in (pg-class-foreign-keys child)
                       when (eql (pg-class-oid parent) (pg-foreign-key-referenced-table-oid constraint))
                         collect (cons child constraint)))))

(defun referencing-foreign-keys (parent)
  "Return child tables and constraints that use PARENT as their referenced relation."
  (with-write-lock
    (referencing-foreign-keys-locked parent)))

(defun foreign-key-stored-keys-equal-p (parent index old new)
  ;; Compare stored datum images when SQL equality hides a representation change.
  (or (every #'eq old new)
      (let ((columns (map 'vector (lambda (attnum)
                                    (aref (pg-class-columns parent) (1- attnum)))
                          (pg-index-columns index))))
        (equalp (form-heap-tuple (coerce old 'vector) columns 0 0 0 0 0)
                (form-heap-tuple (coerce new 'vector) columns 0 0 0 0 0)))))

(defun lock-foreign-key-parent-change (parent references tuple &optional new-values)
  "Lock the old referenced keys of the actual version that will be changed."
  (let ((old-values (htup-values (materialize-select-tuple parent tuple *current-snapshot*))))
    (loop for (child . constraint) in references
          for index = (find-index-by-oid (pg-foreign-key-referenced-index-oid constraint))
          for types = (index-types index parent)
          for old = (index-values index old-values)
          unless (member +null+ old :test #'eq)
            append (let ((key (encode-index-key old types)))
                     (unless (and new-values
                                  (let ((new (index-values index new-values)))
                                    (and (equalp key (encode-index-key new types))
                                         (or (eq (pg-foreign-key-update-action constraint) :no-action)
                                             (foreign-key-stored-keys-equal-p parent index old new)))))
                       (acquire-referenced-key-lock (pg-index-oid index) key :exclusive)
                       (list (list child constraint index key
                                   (loop for attnum in (pg-foreign-key-referenced-columns constraint)
                                         collect (aref old-values (1- attnum)))
                                   (when new-values
                                     (loop for attnum in (pg-foreign-key-referenced-columns constraint)
                                           collect (aref new-values (1- attnum)))))))))))

(defun foreign-key-child-visible-p (child parent constraint index key snapshot)
  "Find a referencing row using the same canonical encoding as the parent grant."
  (let ((demand (cons child (pg-foreign-key-columns constraint)))
        (probe (prepare-child-foreign-key-probe constraint parent index)))
    (heapam-scan
     (pg-class-relfilenode child) (pg-class-columns child) snapshot
     (lambda (tuple)
       (check-for-interrupts)
       (let ((row (htup-values (materialize-select-tuple child tuple snapshot :demand demand))))
         (when (equalp key (encode-child-foreign-key probe row))
           (return-from foreign-key-child-visible-p t))))))
  nil)

(defun apply-foreign-key-action (parent child constraint action old new &optional additional-old)
  "Run the referential action through ordinary DML in the same transaction."
  (let* ((parent-columns (pg-class-columns parent))
         (child-columns (pg-class-columns child))
         (attnums (pg-foreign-key-columns constraint))
         (types (mapcar (lambda (attnum) (pg-column-type (aref parent-columns (1- attnum))))
                        (pg-foreign-key-referenced-columns constraint)))
         (delete-p (and (eq action :cascade) (null new)))
         (predicate
           (if (and additional-old (null (rest attnums))
                    (eq (first types) (pg-column-type (aref child-columns (1- (first attnums))))))
               (list :in (list :column (pg-column-name (aref child-columns (1- (first attnums)))))
                     (loop for key in (cons old additional-old)
                           collect (list :foreign-key-datum (first key) (first types)))
                     nil)
               (flet ((match-key (key)
                        (reduce (lambda (left right) (list :and left right))
                                (loop for attnum in attnums for value in key for type in types
                                      collect (list :binop "="
                                                    (list :column (pg-column-name (aref child-columns (1- attnum))))
                                                    (list :foreign-key-datum value type))))))
                 (reduce (lambda (left right) (list :or left right))
                         (mapcar #'match-key (cons old additional-old))))))
         (sets
           (unless delete-p
             (loop for attnum in attnums for type in types for position from 0
                   collect (list :column (pg-column-name (aref child-columns (1- attnum)))
                                 :expr (ecase action
                                         (:cascade (list :foreign-key-datum (nth position new) type))
                                         (:set-null (list :const +null+))
                                         (:set-default (list :default)))))))
         (ast (list :type (if delete-p :delete :update) :table (pg-class-name child)
                    :where predicate
                    :sets sets)))
    (advance-used-backend-command-id)
    (detach-select-index-scans)
    (let ((*select-index-lease-domain* nil)
          (*select-index-lease-groups* nil)
          (*select-index-scans* nil)
          (*select-suppress-description* nil)
          (*select-expected-result-columns* nil)
          (crosscheck (when (backend-transaction-snapshot *backend*)
                        (copy-snapshot (backend-transaction-snapshot *backend*)))))
      (when crosscheck
        (setf (snapshot-curcid crosscheck) (backend-command-id *backend*)))
      (with-fresh-foreign-key-snapshot (snapshot (backend-command-id *backend*))
        (note-backend-command-id-used)
        (if delete-p
            (exec-delete-subqueries (make-broadcast-stream) ast t crosscheck)
            (exec-update-subqueries (make-broadcast-stream) ast crosscheck))))))

(defun reject-referenced-key-change (parent child constraint)
  (ereport "23503" "update or delete on table ~s violates foreign key constraint ~s on table ~s"
           (pg-class-name parent) (pg-foreign-key-name constraint) (pg-class-name child)))

(defun check-foreign-key-set-default-result (parent child constraint index key)
  "SET DEFAULT can leave an unchanged reference to the removed key."
  (let ((cid (1+ (backend-command-id *backend*))))
    (with-fresh-foreign-key-snapshot (latest cid)
      (when (and (not (foreign-key-parent-visible-p parent index key latest))
                 (foreign-key-child-visible-p child parent constraint index key latest))
        (reject-referenced-key-change parent child constraint)))))

(defun foreign-key-cascade-group-limit (parent child constraint)
  "Use a larger bounded group when child matching uses a constant hash lookup."
  (let ((columns (pg-foreign-key-columns constraint)))
    (if (null (rest columns))
        (let ((child-type (pg-column-type (aref (pg-class-columns child) (1- (first columns)))))
              (parent-type (pg-column-type
                            (aref (pg-class-columns parent)
                                  (1- (first (pg-foreign-key-referenced-columns constraint)))))))
          (if (and (eq child-type parent-type) (hashed-row-membership-type-p child-type))
              64 16))
        16)))

(defun check-foreign-key-parent-changes (parent changes update-p)
  "Run referential actions or verify immediate restrictions at statement end."
  (when changes
    (let ((retained
            (when (backend-transaction-snapshot *backend*)
              (copy-snapshot (backend-transaction-snapshot *backend*)))))
      (loop for pending = changes then (rest pending)
            while pending
            for (child constraint index key old new) = (first pending)
            for action = (if update-p (pg-foreign-key-update-action constraint)
                             (pg-foreign-key-delete-action constraint))
            for cid = (1+ (backend-command-id *backend*))
            do
               (cond
                 ((and (eq action :no-action) (foreign-key-deferred-p constraint))
                  (queue-deferred-foreign-key-check child parent constraint index key))
                 ((and (eq action :cascade) (not update-p) (null retained)
                       (null (referencing-foreign-keys child)))
                  ;; Leaf DELETE rechecks child rows under a fresh snapshot.
                  ;; For chains, avoid admitting downstream tables on a miss.
                  (let ((additional-old
                          (when (and (rest pending)
                                     (not (logical-capture-xid-needed-p (backend-xid *backend*)))
                                     (with-write-lock
                                       (null (table-indexes child nil #'pg-index-ready-p))))
                            (loop repeat (1- (foreign-key-cascade-group-limit parent child constraint))
                                  for next = (second pending)
                                  while (and next (eq (first next) child) (eq (second next) constraint))
                                  collect (fifth next)
                                  do (pop pending)))))
                    (apply-foreign-key-action parent child constraint action old new additional-old)))
                 (t
                  (when retained (setf (snapshot-curcid retained) cid))
                  (with-fresh-foreign-key-snapshot (latest cid)
                    (when (and (or (not (eq action :no-action))
                                   (not (foreign-key-parent-visible-p parent index key latest)))
                               (foreign-key-child-visible-p child parent constraint index key latest))
                      (when (and retained (member action '(:no-action :restrict))
                                 (not (foreign-key-child-visible-p child parent constraint index key retained)))
                        (ereport "40001" "could not serialize access due to concurrent update"))
                      (if (member action '(:no-action :restrict))
                          (reject-referenced-key-change parent child constraint)
                          (progn
                            (apply-foreign-key-action parent child constraint action old new)
                            (when (eq :set-default action)
                              (check-foreign-key-set-default-result parent child constraint index key))))))))))))

(defun check-distinct-constraint-columns (table attnums primary-p)
  (loop for attnum in attnums
        for offset from 0
        when (position attnum attnums :end offset)
          do (ereport "42701" "column \"~a\" appears twice in ~a constraint"
                      (pg-column-name
                       (aref (pg-class-columns table) (1- attnum)))
                      (if primary-p "primary key" "unique"))))

(defun fsync-index (idx)
  (smgr-fsync (pg-index-relfilenode idx)))

(defun index-build-toast-reader (relfilenode function)
  "Read TOAST under the parent build gate and, when needed, an index lease.
The gate has drained writers, so a build already holding the write lock can read
without acquiring the TOAST index lock in the wrong order. Otherwise acquire a
read lease to protect the index lookup."
  (if (sb-thread:holding-mutex-p *write-lock*)
      (funcall function)
      (call-with-btree-relation-read-lock relfilenode function)))

(defun index-build-predicate-columns (index)
  "Collect the predicate's attribute dependencies once per heap scan."
  (let ((predicate (pg-index-predicate index))
        (columns nil))
    (when predicate
      (flet ((visit (node walk)
               (declare (ignore walk))
               (when (eq (first node) :col)
                 (pushnew (second node) columns :test #'eql))
               nil))
        (declare (dynamic-extent #'visit))
        (some-local-plan-expression #'visit predicate)))
    columns))

(defun index-build-external-attributes-p (values columns)
  (some (lambda (attnum) (heap-external-varlena-p (aref values (1- attnum)))) columns))

(defun index-build-physical-values (index tuple)
  "Save compression and external metadata for plain index keys."
  (let ((values (htup-values tuple))
        (compressed (htup-physical-compressed tuple))
        (keys (index-key-specifications index)))
    (when (some (lambda (key)
                  (and (integerp key)
                       (let ((slot (1- key)))
                         (or (and compressed (aref compressed slot))
                             (heap-external-varlena-p (aref values slot))))))
                keys)
      (let ((physical (copy-seq values)))
        (when compressed
          (dolist (key keys)
            (when (integerp key)
              (let ((slot (1- key)))
                (when (aref compressed slot)
                  (setf (aref physical slot) (aref compressed slot)))))))
        physical))))

(defun materialize-index-build-attributes (table tuple snapshot columns)
  (materialize-select-tuple table tuple (statement-protection-snapshot snapshot)
                            :index-reader #'index-build-toast-reader
                            :demand (cons table columns) :retain-external-p t))

(defun index-build-key (index table tuple snapshot &optional (predicate-columns (index-build-predicate-columns index)))
  "Evaluate the predicate before fetching external attributes needed only by the key."
  (let* ((values (htup-values tuple))
         (predicate-external (index-build-external-attributes-p values predicate-columns))
         ;; A predicate can materialize a plain key column in place.
         (physical (when predicate-external (index-build-physical-values index tuple))))
    (when predicate-external
      (materialize-index-build-attributes table tuple snapshot predicate-columns))
    (when (index-tuple-qualifies-p index values)
      (let* ((columns (or (pg-index-referenced-columns index) (pg-index-columns index)))
             (external (index-build-external-attributes-p values columns)))
        (when (and (not predicate-external) (or external (htup-physical-compressed tuple)))
          (setf physical (index-build-physical-values index tuple)))
        (when external
          (materialize-index-build-attributes table tuple snapshot columns))
        (let ((keys (evaluate-index-key index values)))
          (values (index-storage-key index table keys physical) keys))))))

(defun check-index-build-uniqueness (index table snapshot key name)
  "Reject a conflicting version visible to the build horizon."
  (let ((heap (pg-class-relfilenode table))
        (columns (pg-class-columns table)))
    (btree-scan-equal
     (pg-index-relfilenode index) key
     (lambda (tid)
       (let ((candidate (heapam-get-tuple heap (car tid) (cdr tid) columns)))
         (when (and candidate (htup-visible-p candidate snapshot))
           (ereport "23505" "could not create unique index \"~a\"" name)))))))

(defun build-concurrent-index (index table snapshot)
  "Build from one MVCC horizon while table writers still ignore this index."
  (let ((heap (pg-class-relfilenode table))
        (node (pg-index-relfilenode index))
        (columns (pg-class-columns table))
        (predicate-columns (index-build-predicate-columns index)))
    (heapam-scan
     heap columns snapshot
     (lambda (tuple)
       (check-for-interrupts)
       (multiple-value-bind (key values) (index-build-key index table tuple snapshot predicate-columns)
         (when key
           (when (index-key-requires-uniqueness-check-p index values)
             (check-index-build-uniqueness index table snapshot key (pg-index-name index)))
           (btree-insert node key (htup-block tuple) (htup-offnum tuple))))))))

(defun concurrent-index-validation-conflict (table tuple cursor)
  "Return the live conflict or XID wait that can still affect TUPLE's key."
  (let ((wait nil))
    (loop for tid = (btree-cursor-next-tid cursor)
          while tid
          finally (return wait)
          do
             (let ((candidate (heapam-get-tuple (pg-class-relfilenode table)
                                                (car tid) (cdr tid) (pg-class-columns table))))
               (when candidate
                 (let ((status (unique-conflict-status candidate nil)))
                   (cond
                     ((eq status :violation)
                      ;; The source tuple might have been replaced since the
                      ;; validation snapshot. Its successor can live under this key.
                      (let* ((source (heapam-get-tuple (pg-class-relfilenode table)
                                                       (htup-block tuple) (htup-offnum tuple)
                                                       (pg-class-columns table)))
                             (source-status (and source (unique-conflict-status source nil))))
                        (return (if (eq source-status :free) nil source-status))))
                     ((consp status) (setf wait status)))))))))

(defun validate-concurrent-index-tuple (index table tuple key unique)
  "Insert a reference-snapshot TID once, serializing with ordinary index writes."
  (let ((node (pg-index-relfilenode index))
        (block (htup-block tuple))
        (offset (htup-offnum tuple)))
    (loop
      (check-for-interrupts)
      (let ((status
              (with-btree-relation-lock (node)
                (let ((cursor (open-btree-equal-cursor-unlocked node key))
                      (matching nil))
                  (loop for tid = (btree-cursor-next-tid cursor)
                        while tid
                        do (setf matching t)
                           (when (and (= block (car tid)) (= offset (cdr tid)))
                             (return-from validate-concurrent-index-tuple nil)))
                  (with-write-lock
                    (let ((conflict
                            (when (and unique matching)
                              (concurrent-index-validation-conflict
                               table tuple (open-btree-equal-cursor-unlocked node key)))))
                      (unless conflict (btree-insert-unlocked node key block offset))
                      conflict))))))
        (cond
          ((null status) (return))
          ((eq status :violation)
           (ereport "23505" "could not create unique index \"~a\"" (pg-index-name index)))
          (t (wait-for-xid (cdr status))))))))

(defun validate-concurrent-index (index table snapshot built-snapshot)
  "Add missing TIDs visible to the reference snapshot.
Every version visible to BUILT-SNAPSHOT was indexed by the completed build.
The heap maintenance gate keeps these versions and their index entries stable
between scans. Checking the original horizon avoids probing the index for
unchanged rows, even when one key has many entries."
  (let ((predicate-columns (index-build-predicate-columns index)))
    (heapam-scan
     (pg-class-relfilenode table) (pg-class-columns table) snapshot
     (lambda (tuple)
       (check-for-interrupts)
       (unless (htup-visible-p tuple built-snapshot)
         (multiple-value-bind (key values) (index-build-key index table tuple snapshot predicate-columns)
           (when key
             (validate-concurrent-index-tuple index table tuple key
                                              (index-key-requires-uniqueness-check-p index values)))))))))

(defun build-index-entries (idx table &key unique name (removal-horizon (vacuum-data-oldest-xmin)))
  "Populate IDX with heap versions retained by REMOVAL-HORIZON.
The default horizon includes local activity and physical slots.

UNIQUE checks only tuples visible to the fresh statement snapshot.  The caller
holds the table DDL gate, which has drained other writers and maintenance
before this removal horizon is captured."
  (let* ((heap-relfilenode (pg-class-relfilenode table))
         (index-relfilenode (pg-index-relfilenode idx))
         (columns (pg-class-columns table))
         (snapshot *current-snapshot*)
         (predicate-columns (index-build-predicate-columns idx)))
    (loop for block below (buf-nblocks heap-relfilenode)
          for page = (buf-read heap-relfilenode block)
          when page
            do (do-live-page-items (page offnum off len)
                 (let ((tuple (heapam-deform-page-item page block offnum off len columns)))
                   (when (and tuple
                              (not (tuple-prunable-p (htup-xmin tuple)
                                                     (htup-xmax tuple)
                                                     removal-horizon)))
                     (multiple-value-bind (key vals) (index-build-key idx table tuple snapshot predicate-columns)
                       ;; Retain recently dead versions for old snapshots, but
                       ;; don't let those versions create unique violations.
                       (when key
                         (when (and (index-key-requires-uniqueness-check-p idx vals unique)
                                    (htup-visible-p tuple snapshot))
                           (check-index-build-uniqueness idx table snapshot key name))
                         (btree-insert index-relfilenode key
                                       (htup-block tuple)
                                       (htup-offnum tuple))))))))
    ;; A missing :VALID field in an older catalog decodes as NIL.  Publish
    ;; the index only after every candidate TID has been inserted.
    (setf (pg-index-valid-p idx) t))
  (values))

(defun build-unique-index (idx table name)
  (build-index-entries idx table :unique t :name name))

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

(defun table-constraint-reserved-names (columns constraints)
  "Collect explicit names that must be preserved during default index naming."
  (remove nil
          (append (mapcar (lambda (constraint) (getf constraint :name)) constraints)
                  (loop for column in columns
                        append (list* (getf column :primary-name)
                                      (getf column :unique-name)
                                      (mapcar (lambda (check) (getf check :name))
                                              (getf column :checks)))))))

(defun exec-create-table (stream ast)
  (let* ((name (getf ast :name))
         (db nil)
         (existing nil))
    ;; Catalog hash tables are mutated by DDL under the write lock.  Capture a
    ;; consistent existence check before doing any work outside that lock.
    (with-write-lock
      (setf db (current-database)
            existing (relation-name-in-use-p db name)))
    (when existing
      (if (getf ast :if-not-exists)
          (progn
            (send-notice stream
                         (format nil "relation \"~a\" already exists, skipping" name)
                         "NOTICE" "42P07")
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
      (with-logical-ddl-publication (ast)
        (let* ((*catalog-save-error-after-durable-p* nil)
               (reserved-names (table-constraint-reserved-names cols constraints))
               (class (create-table* db name cols reserved-names))
               (durable-generation *catalog-durable-generation*)
               (terminal-appended nil))
          ;; Once the terminal catalog WAL record is appended, recovery owns the
          ;; outcome even if a later flush or catalog image write fails. Do not
          ;; drop the table or its owned relations after that point.
          (with-error-cleanup
              (e (unless (or terminal-appended *catalog-save-error-after-durable-p*
                             (> *catalog-durable-generation* durable-generation))
                   (progn
                     ;; CREATE TABLE is one statement. A failed constraint
                     ;; build must not leave earlier indexes published.
                     (when (eq class (lookup-table name))
                       (drop-table* db name))
                     (save-catalog :applied-logical-event-id nil))))
            (install-table-expressions class cols constraints)
            (dolist (c (remove-if (lambda (c) (eq (getf c :kind) :check)) constraints))
              (let ((attnums (resolve-attnums class (getf c :columns))))
                (check-distinct-constraint-columns class attnums
                                                   (eq (getf c :kind) :primary))
                (case (getf c :kind)
                  (:primary
                   (dolist (attnum attnums)
                     (setf (pg-column-notnull
                            (aref (pg-class-columns class) (1- attnum)))
                           t))
                   (let ((idx (create-index*
                               db
                               (or (getf c :name)
                                   (generate-index-name db name "pkey" reserved-names))
                               class attnums :unique t :primary t
                               :constraint t)))
                     (build-unique-index idx class (pg-index-name idx))
                     (fsync-index idx)))
                  (:unique
                   (let ((idx (create-index*
                               db
                               (or (getf c :name)
                                   (generate-unique-index-name
                                    db name class attnums reserved-names))
                               class attnums :unique t
                               :constraint t
                               :nulls-not-distinct
                               (getf c :nulls-not-distinct))))
                     (build-unique-index idx class (pg-index-name idx))
                     (fsync-index idx))))))
            (dolist (idx (table-indexes class))
              ;; CREATE TABLE starts with an empty heap; table-level indexes
              ;; have been built above and inline indexes are empty here.
              (setf (pg-index-valid-p idx) t)
              (fsync-index idx))
            (save-catalog :final-ddl-p t :logical-event
                          (logical-catalog-event-for-ast ast)
                          :on-record-appended (lambda () (setf terminal-appended t))))))
      (send-command-complete stream "CREATE TABLE"))))

(defun check-no-dependent-view (relation relations)
  (multiple-value-bind (oid kind name)
      (etypecase relation
        (pg-class (values (pg-class-oid relation) "table" (pg-class-name relation)))
        (pg-index (values (pg-index-oid relation) "index" (pg-index-name relation))))
    (let ((dependent (first (dependent-views relations oid))))
      (when dependent
        (ereport "2BP01" "cannot drop ~a ~s because view ~s depends on it"
                 kind name (pg-class-name dependent))))))

(defun exec-drop-table (stream ast)
  (let* ((names (getf ast :names))
         (if-exists (getf ast :if-exists))
         (db nil)
         (relations nil)
         (relfilenodes nil))
    (with-write-lock
      (setf db (current-database)
            relations
            (mapcar (lambda (name)
                      (let ((class (lookup-sql-user-relation name)))
                        (unless (or class if-exists)
                          (ereport "42P01" "table \"~a\" does not exist" name))
                        (cons name class)))
                    names)
            relfilenodes
            (loop for entry in relations
                  for class = (cdr entry)
                  when class collect (pg-class-relfilenode class))))
    (with-relation-build-locks (relfilenodes t)
      (with-logical-ddl-publication (ast)
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
        (with-catalog-mutation-rollback (db)
          (dolist (entry relations)
            (let* ((name (car entry))
                   (original (cdr entry))
                   (current (gethash name (database-tables db))))
              (if (and original (eq original current))
                  (progn
                    (check-no-dependent-view current (database-tables db))
                    (dolist (index (table-indexes current))
                      (check-no-dependent-view index (database-tables db)))
                    (drop-table* db name))
                  (send-notice stream
                               (format nil "table \"~a\" does not exist, skipping"
                                       name)))))
          (save-catalog :final-ddl-p t :logical-event
                        (logical-catalog-event-for-ast ast)))))
    (send-command-complete stream "DROP TABLE")))

(defun drop-index-relation-lock-set (entries table-lookup)
  "Return each index and owning heap relfilenode from captured ENTRIES."
  (loop for entry in entries
        for index = (cdr entry)
        for table = (when index
                      (funcall table-lookup (pg-index-table-oid index)))
        when index
          append (cons (pg-index-relfilenode index)
                       (when table (list (pg-class-relfilenode table))))))

(defun expression-index-definition-p (ast)
  (or (getf ast :index-keys) (getf ast :predicate)))

(defun reserve-index-definition-identity (definition)
  "Reserve one identity for a detached index and its provisional B-tree."
  (let ((oid (allocate-oid)))
    (setf (pg-index-oid definition) oid
          (pg-index-relfilenode definition) oid)
    definition))

(defun allocate-index-definition (table ast columns)
  "Analyze a detached index before allocating its persistent storage identity."
  (let ((name (getf ast :name)))
    (reserve-index-definition-identity
     (if (expression-index-definition-p ast)
         (make-analyzed-index-object nil name table ast)
         (make-index-object nil name table columns :unique (getf ast :unique)
                                                   :nulls-not-distinct (getf ast :nulls-not-distinct))))))

(defun exec-create-index (stream ast)
  "Build privately while holding the table gate, then publish the index.
OID reservation, build, file sync and detached relfile cleanup stay outside the
final publication scope. Under the write and catalog locks, revalidate and capture
a fresh schema precondition before mutation, event creation and durable save or
catalog rollback.
Release these locks before waiting for commit or sending replies."
  (let* ((*catalog-save-error-after-durable-p* nil)
         (name (getf ast :name))
         (advanced-p (expression-index-definition-p ast))
         (columns (getf ast :columns))
         (if-not-exists (and (null *logical-event-apply-id*) (getf ast :if-not-exists)))
         (db nil)
         (table nil)
         (existing nil)
         (skipped nil))
    (with-write-lock
      (setf db (current-database)
            table (resolve-table-or-error (getf ast :table)))
      (unless advanced-p (check-index-key-count columns))
      (setf existing (relation-name-in-use-p db name))
      (when (and existing (not advanced-p))
        (let ((attnums (resolve-attnums table columns)))
          (when if-not-exists (check-index-definition table attnums)))))
    (when existing
      (if if-not-exists
          (progn
            (send-notice stream
                         (format nil "relation \"~a\" already exists, skipping" name)
                         "NOTICE" "42P07")
            (send-command-complete stream "CREATE INDEX")
            (return-from exec-create-index nil))
          (ereport "42P07" "relation \"~a\" already exists" name)))
    (with-relation-build-lock ((pg-class-relfilenode table))
      (let ((*current-snapshot* (refresh-statement-snapshot))
            (idx nil)
            (preserve-relfile nil))
        ;; The table gate prevents schema changes and writers while the build
        ;; runs. Resolve the columns and reserve the OID only after the gate is
        ;; acquired, then persist the reservation before creating the relfile.
        (with-write-catalog-locks
          (ensure-current-table table)
          (let ((attnums (unless advanced-p (resolve-attnums table columns))))
            (setf existing (relation-name-in-use-p db name))
            (when existing
              (if if-not-exists
                  (setf skipped t)
                  (ereport "42P07" "relation \"~a\" already exists" name)))
            (unless advanced-p (check-index-definition table attnums))
            (unless skipped
              (setf idx (allocate-index-definition table ast attnums))
              (save-catalog :applied-logical-event-id nil))))
        (unless skipped
          (handler-case
              (progn
                (btree-init (pg-index-relfilenode idx))
                (build-index-entries idx table :unique (pg-index-unique-p idx) :name name)
                (fsync-index idx)
                (wal-test-pause :after-ordinary-index-build)
                (with-logical-ddl-publication (ast)
                  (ensure-current-table table)
                  (setf existing (relation-name-in-use-p db name))
                  (when existing
                    (if if-not-exists
                        (setf skipped t)
                        (ereport "42P07" "relation \"~a\" already exists" name)))
                  (let ((durable-generation *catalog-durable-generation*))
                    (handler-case
                        (progn
                          (if skipped
                              (progn
                                (wal-defer-relation-delete (pg-index-relfilenode idx))
                                (save-catalog :final-ddl-p t :applied-logical-event-id nil))
                              (progn
                                (publish-index* db idx table)
                                (save-catalog :final-ddl-p t :logical-event
                                              (logical-catalog-event-for-ast ast)))))
                      (error (publish-condition)
                        ;; Do not roll back memory if WAL has published this
                        ;; catalog image: later writes must keep maintaining
                        ;; any index that restart recovery will restore.
                        (if (or *catalog-save-error-after-durable-p*
                                (> *catalog-durable-generation* durable-generation))
                            (setf preserve-relfile t)
                            ;; Keep catalog readers behind *write-lock* while
                            ;; undoing a publication that was not durable.
                            (when (eq idx (gethash name (database-indexes db)))
                              (remhash name (database-indexes db))
                              (setf (pg-class-indexes table)
                                    (delete (pg-index-oid idx)
                                            (pg-class-indexes table)))))
                        (error publish-condition))))))
            (error (condition)
              ;; Before publication, no reader can know this relfilenode, so
              ;; discard its pages together with a catalog save.
              (unless (or preserve-relfile *wal-poisoned*)
                (handler-case
                    (with-write-catalog-locks
                      (wal-defer-relation-delete (pg-index-relfilenode idx))
                      (save-catalog :applied-logical-event-id nil))
                  (error (cleanup-condition)
                    (declare (ignore cleanup-condition)))))
              (error condition))))))
    (when skipped
      (send-notice stream
                   (format nil "relation \"~a\" already exists, skipping" name)
                   "NOTICE" "42P07"))
    (send-command-complete stream "CREATE INDEX")))

(defun refresh-index-ddl-target (name original &optional if-exists)
  "Follow catalog versions of the same locked index, never a replacement."
  (let ((current (lookup-index name)))
    (cond
      ((and current (= (pg-index-oid current) (pg-index-oid original))
            (= (pg-index-relfilenode current) (pg-index-relfilenode original)))
       current)
      (current
       (ereport "55000" "index \"~a\" changed while waiting for lock" name))
      ((not if-exists)
       (ereport "42704" "index \"~a\" does not exist" name)))))

(defun logical-index-drop-ast (database ast entries)
  "Exclude local-only indexes and relations that appeared after target capture."
  (let ((names
          (loop for (name . index) in entries
                for current = (gethash name (database-indexes database))
                when (or (null current)
                         (and (eq index current) (pg-index-logical-visible-p current)))
                  collect name)))
    (when names
      (let ((event (copy-list ast)))
        (setf (getf event :names) names)
        event))))

(defun logical-index-ddl-event (ast)
  "Verify the received event's target shape even when local cleanup emits no event."
  (cond
    (ast (logical-catalog-event-for-ast ast))
    (*logical-event-schema-contract*
     (logical-validate-schema-contract-under-write-lock
      (getf *logical-event-schema-contract* :schema-after) :after)
     nil)))

(defun exec-drop-index (stream ast)
  (let* ((names (getf ast :names))
         (if-exists (getf ast :if-exists))
         (database nil)
         (entries nil)
         (relfilenodes nil))
    (with-write-lock
      (setf database (current-database)
            entries (mapcar (lambda (name) (cons name (lookup-index name))) names)
            relfilenodes (drop-index-relation-lock-set entries #'find-table-by-oid)))
    (dolist (entry entries)
      (unless (or (cdr entry) if-exists)
        (ereport "42704" "index \"~a\" does not exist" (car entry))))
    (with-relation-build-locks (relfilenodes t)
      (with-logical-ddl-publication (ast)
        (dolist (entry entries)
          (when (cdr entry)
            (setf (cdr entry) (refresh-index-ddl-target (car entry) (cdr entry) if-exists))
            (when (cdr entry)
              (check-no-dependent-view (cdr entry) (database-tables database)))))
        ;; Preflight every dependency before changing any part of a multi-index drop.
        (loop for (name . index) in entries
              when (and index (pg-index-constraint-owned-p index))
                do (let ((table (find-table-by-oid (pg-index-table-oid index))))
                     (ereport "2BP01"
                              "cannot drop index \"~a\" because constraint \"~a\" on table \"~a\" requires it"
                              name name (if table (pg-class-name table) "?"))))
        (let ((logical-ast (logical-index-drop-ast database ast entries)))
          (with-catalog-mutation-rollback (database)
            (loop for (name . index) in entries
                  do (if (and index (eq index (gethash name (database-indexes database))))
                         (drop-index* database name)
                         (send-notice stream (format nil "index \"~a\" does not exist, skipping" name))))
            (save-catalog :final-ddl-p t :logical-event (logical-index-ddl-event logical-ast))))))
    (send-command-complete stream "DROP INDEX")))

(defun concurrent-drop-target (table name original if-exists)
  (let ((current (refresh-index-ddl-target name original if-exists)))
    (when current (ensure-current-table table))
    current))

(defun call-with-concurrent-index-phase (function)
  "Finish one durable phase without retaining transaction resources in the wait."
  (let ((*catalog-save-error-after-durable-p* nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (set-backend-state *backend* :in-progress)
             (sb-sys:with-local-interrupts
               (call-with-prepared-final-ddl
                (lambda ()
                  (funcall function)
                  (commit-transaction)))))
        (when (eq (backend-state *backend*) :in-progress)
          (abort-transaction))))))

(defun write-concurrent-index-state (database old new &optional logical-ast)
  "Replace the catalog object so admitted writers keep their original index state."
  (let* ((indexes (database-indexes database))
         (name (pg-index-name old)))
    (unless (eq old (gethash name indexes))
      (error "concurrent index phase lost catalog ownership"))
    (when (and (pg-index-logical-visible-p old) (not (pg-index-logical-visible-p new)))
      (check-no-dependent-view old (database-tables database)))
    (call-with-catalog-durability-rollback-under-lock
     (lambda () (setf (gethash name indexes) old))
     (lambda ()
       (setf (gethash name indexes) new)
       (save-catalog :final-ddl-p t :applied-logical-event-id nil
                     :logical-event (when logical-ast
                                      (logical-catalog-event-for-ast logical-ast)))))))

(defun concurrent-index-identity-changes-p (table old new)
  (with-write-lock
    (let* ((indexes (table-indexes table))
           (before (logical-table-replica-identity table indexes))
           (after (logical-table-replica-identity table (substitute new old indexes :test #'eq))))
      (not (equal (sort before #'<) (sort after #'<))))))

(defun publish-concurrent-index-state (database table old new logical-ast)
  "Keep old logical row keys ordered before the catalog event that changes them.
When the identity changes, block new writers and drain those admitted under the
old identity before publishing the event and replacement catalog object."
  (call-with-concurrent-index-phase
   (lambda ()
     (if logical-ast
         (with-schema-write-lock
           (flet ((publish ()
                    (with-logical-ddl-publication (logical-ast)
                      (write-concurrent-index-state database old new logical-ast))))
             (if (concurrent-index-identity-changes-p table old new)
                 (call-with-concurrent-index-writer-fence (pg-class-relfilenode table) #'publish)
                 (publish))))
         (with-write-catalog-locks
           (write-concurrent-index-state database old new))))))

(defun set-concurrent-drop-state (database table index &key dead)
  (let* ((new (copy-pg-index index))
         (logical-ast (when (pg-index-logical-visible-p index)
                        (list :type :drop-index :names (list (pg-index-name index))))))
    (setf (pg-index-valid-p new) nil
          (pg-index-logical-visible-p new) nil)
    (when dead
      (setf (pg-index-ready-p new) nil
            (pg-index-live-p new) nil))
    (publish-concurrent-index-state database table index new logical-ast)
    new))

(defun run-concurrent-index-drop (database table index)
  ;; First stop new scans, then drain users before marking the index dead.
  ;; Drain users of the old state again before removing its catalog and storage.
  (let ((node (pg-class-relfilenode table)))
    (when (or (pg-index-valid-p index) (pg-index-logical-visible-p index))
      (setf index (set-concurrent-drop-state database table index)))
    (when (pg-index-live-p index)
      (wal-test-pause :after-concurrent-index-invalid)
      (wait-for-concurrent-index-users node :readers t)
      (setf index (set-concurrent-drop-state database table index :dead t)))
    (wal-test-pause :after-concurrent-index-dead)
    (wait-for-concurrent-index-users node :readers t)
    (call-with-concurrent-index-phase
     (lambda ()
       (with-write-catalog-locks
         (ensure-current-table table)
         (unless (eq index (lookup-index (pg-index-name index)))
           (error "concurrent index drop lost catalog ownership"))
         (check-no-dependent-view index (database-tables database))
         (with-catalog-mutation-rollback (database)
           (drop-index* database (pg-index-name index))
           (save-catalog :final-ddl-p t :applied-logical-event-id nil)))))))

(defun exec-drop-index-concurrently (stream ast)
  (let* ((name (first (getf ast :names)))
         (if-exists (getf ast :if-exists))
         (database (current-database))
         (index nil)
         (table nil)
         (removed nil))
    (with-write-lock
      (setf index (lookup-index name)
            table (and index (find-table-by-oid (pg-index-table-oid index))))
      (when (and (null index) (relation-name-in-use-p database name))
        (ereport "42809" "\"~a\" is not an index" name)))
    (when (and index (null table))
      (error "index has no owning table"))
    (when table
      (with-relation-maintenance-locks
          ((list (pg-class-relfilenode table) (pg-index-relfilenode index)) :concurrent-index)
        (let ((target (with-write-lock
                        (concurrent-drop-target table name index if-exists))))
          (when target
            (when (pg-index-constraint-owned-p target)
              (ereport "2BP01"
                       "cannot drop index \"~a\" because constraint \"~a\" on table \"~a\" requires it"
                       name name (pg-class-name table)))
            (run-concurrent-index-drop database table target)
            (setf removed t)))))
    (unless removed
      (unless if-exists (ereport "42704" "index \"~a\" does not exist" name))
      (send-notice stream (format nil "index \"~a\" does not exist, skipping" name)))
    (send-command-complete stream "DROP INDEX")))

(defun concurrent-index-name-available-p (database name if-not-exists)
  (or (not (relation-name-in-use-p database name))
      (and (not if-not-exists)
           (ereport "42P07" "relation \"~a\" already exists" name))))

(defun reserve-concurrent-index (database table ast)
  (with-write-catalog-locks
    (ensure-current-table table)
    (let* ((name (getf ast :name))
           (advanced-p (expression-index-definition-p ast))
           (columns (unless advanced-p (resolve-attnums table (getf ast :columns)))))
      (unless advanced-p
        (check-index-definition table columns))
      (when (concurrent-index-name-available-p database name (getf ast :if-not-exists))
        (let ((index (allocate-index-definition table ast columns)))
          (setf (pg-index-ready-p index) nil
                (pg-index-logical-visible-p index) nil)
          (save-catalog :applied-logical-event-id nil)
          index)))))

(defun publish-concurrent-index-definition (database table index if-not-exists)
  (let ((published nil))
    (call-with-concurrent-index-phase
     (lambda ()
       (with-write-catalog-locks
         (ensure-current-table table)
         (let ((name (pg-index-name index))
               (old-indexes (pg-class-indexes table)))
           (when (concurrent-index-name-available-p database name if-not-exists)
             (call-with-catalog-durability-rollback-under-lock
              (lambda ()
                (remhash name (database-indexes database))
                (setf (pg-class-indexes table) old-indexes))
              (lambda ()
                (publish-index* database index table)
                (save-catalog :final-ddl-p t :applied-logical-event-id nil)
                (setf published t))))))))
    published))

(defun cleanup-unpublished-concurrent-index (database index)
  "Discard reserved storage if no durable catalog phase owns it."
  (unless *wal-poisoned*
    (with-write-catalog-locks
      (unless (eq index (gethash (pg-index-name index) (database-indexes database)))
        (wal-defer-relation-delete (pg-index-relfilenode index))
        (save-catalog :applied-logical-event-id nil)))))

(defun install-concurrent-index-definition (database table index ast)
  (let ((published nil))
    (unwind-protect
         (progn
           (btree-init (pg-index-relfilenode index))
           (setf published
                 (publish-concurrent-index-definition
                  database table index (getf ast :if-not-exists))))
      (unless published
        (cleanup-unpublished-concurrent-index database index)))))

(defun set-concurrent-create-state (database table index &optional logical-ast)
  (let ((new (copy-pg-index index)))
    (setf (pg-index-ready-p new) t)
    (when logical-ast
      (setf (pg-index-valid-p new) t
            (pg-index-logical-visible-p new) t))
    (publish-concurrent-index-state database table index new logical-ast)
    new))

(defun run-concurrent-index-create (database table index ast)
  ;; Wait out writers that did not see the definition, build, then make writers
  ;; maintain the index.  After those using the unready state have finished,
  ;; validate against a fresh snapshot and drain older snapshots before making
  ;; the index valid.  Each published state survives a later phase failure.
  (let ((heap (pg-class-relfilenode table))
        (logical-ast (copy-list ast))
        (safe-p (not (expression-index-definition-p ast)))
        (*wal-row-page-write-p* nil))
    (remf logical-ast :concurrently)
    (wal-test-pause :after-concurrent-index-unready)
    (wait-for-concurrent-index-users heap)
    (let ((built-snapshot
            (with-statement-snapshot (snapshot (lambda (registration)
                                                 (capture-concurrent-index-snapshot registration safe-p)))
              (build-concurrent-index index table snapshot)
              snapshot)))
      (wal-test-pause :after-concurrent-index-build)
      (setf index (set-concurrent-create-state database table index))
      (wal-test-pause :after-concurrent-index-ready)
      (wait-for-concurrent-index-users heap)
      (let ((reference
              (with-statement-snapshot (snapshot (lambda (registration)
                                                   (capture-concurrent-index-snapshot registration safe-p)))
                (wal-test-pause :before-concurrent-index-validation)
                (validate-concurrent-index index table snapshot built-snapshot)
                snapshot)))
        (wal-test-pause :after-concurrent-index-validation)
        (wait-for-concurrent-index-snapshots reference)))
    (set-concurrent-create-state database table index logical-ast)))

(defun exec-create-index-concurrently (stream ast)
  (let* ((database (current-database))
         (table (with-write-lock (resolve-table-or-error (getf ast :table))))
         (built nil))
    (check-index-key-count (or (getf ast :index-keys) (getf ast :columns)))
    (with-relation-maintenance-locks ((list (pg-class-relfilenode table)) :concurrent-index)
      (let ((index (reserve-concurrent-index database table ast)))
        (when (and index (install-concurrent-index-definition database table index ast))
          (run-concurrent-index-create database table index ast)
          (setf built t))))
    (unless built
      (send-notice stream
                   (format nil "relation \"~a\" already exists, skipping" (getf ast :name))
                   "NOTICE" "42P07"))
    (send-command-complete stream "CREATE INDEX")))

(defun exec-alter-index (stream ast)
  (let* ((name (getf ast :name))
         (new-name (getf ast :new-name))
         (db nil)
         (idx nil)
         (table nil))
    (with-write-lock
      (setf db (current-database)
            idx (lookup-index name)
            table (when idx (find-table-by-oid (pg-index-table-oid idx)))))
    (unless idx
      (ereport "42704" "index \"~a\" does not exist" name))
    (unless table
      (ereport "42P01" "table for index \"~a\" does not exist" name))
    (with-table-ddl (table :ast ast :indexes (list idx) :wait-readers t)
      (let ((current (refresh-index-ddl-target name idx)))
        (with-catalog-mutation-rollback (db)
          (rename-index* db name new-name)
          (save-catalog :final-ddl-p t :logical-event
                        (logical-index-ddl-event (when (pg-index-logical-visible-p current) ast))))))
    (send-command-complete stream "ALTER INDEX")))

(defun exec-alter-table-using-index (db table ast primary)
  (let* ((index-name (getf ast :using-index))
         (idx (with-write-lock (lookup-index index-name))))
    (unless idx
      (ereport "42704" "index \"~a\" does not exist" index-name))
    (with-table-ddl (table :ast ast :refresh-snapshot t
                           :indexes (list idx)
                           :wait-readers t)
      ;; Recheck after waiting for both the heap and index readers.
      (setf idx (refresh-index-ddl-target index-name idx))
      (unless (pg-index-valid-p idx)
        (ereport "55000" "index \"~a\" is not valid" index-name))
      (when (pg-index-constraint-owned-p idx)
        (ereport "55000"
                 "index \"~a\" is already associated with a constraint"
                 index-name))
      (unless (= (pg-index-table-oid idx) (pg-class-oid table))
        (ereport "55000" "index \"~a\" does not belong to table \"~a\""
                 index-name (pg-class-name table)))
      (unless (pg-index-unique-p idx)
        (ereport "42809" "\"~a\" is not a unique index" index-name))
      (when (index-has-expressions-p idx)
        (ereport "42809" "index ~s has expressions or a predicate" index-name))
      (check-distinct-constraint-columns table (pg-index-columns idx) primary)
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
        (when (table-constraint-name-used-p table new-name)
          (ereport "23505" "constraint ~s already exists" new-name))
        (unless (string= new-name (pg-index-name idx))
          (check-new-relation-name db new-name))
        (with-catalog-mutation-rollback (db)
          (when primary
            (dolist (attnum (pg-index-columns idx))
              (setf (pg-column-notnull
                     (aref (pg-class-columns table) (1- attnum)))
                    t))
            (setf (pg-index-primary-p idx) t))
          (setf (pg-index-constraint-p idx) t)
          (unless (string= new-name (pg-index-name idx))
            (rename-index* db (pg-index-name idx) new-name))
          (save-catalog :final-ddl-p t :logical-event
                        (logical-catalog-event-for-ast ast)))))))

(defun exec-alter-table-constraint (stream db table ast)
  (let* ((name (getf ast :constraint-name))
         (action (getf ast :action))
         (if-exists (getf ast :constraint-if-exists))
         (original (with-write-lock
                     (gethash name (database-indexes db)))))
    (with-table-ddl (table :ast ast :indexes (when original (list original))
                           :wait-readers t)
      (let* ((idx (gethash name (database-indexes db)))
             (belongs-to-table
               (and idx
                    (= (pg-index-table-oid idx) (pg-class-oid table))
                    (pg-index-constraint-owned-p idx))))
        (unless (eq original idx)
          (ereport "55000" "constraint \"~a\" changed while waiting for lock"
                   name))
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
        (with-catalog-mutation-rollback (db)
          (case action
            (:rename-constraint
             (rename-index* db name (getf ast :new-name)))
            (:drop-constraint
             (check-no-dependent-view idx (database-tables db))
             (drop-index* db name)))
          (save-catalog :final-ddl-p t :logical-event
                        (logical-catalog-event-for-ast ast)))))
    t))

(defun exec-alter-table-replace-primary (stream db table actions ast)
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
         (index-name (getf add-action :using-index))
         (original-old-idx nil)
         (original-new-idx nil))
    (with-write-lock
      (setf original-old-idx (gethash old-name (database-indexes db))
            original-new-idx (gethash index-name (database-indexes db))))
    (with-table-ddl (table :ast ast :refresh-snapshot t
                           :indexes (remove nil
                                            (list original-old-idx
                                                  original-new-idx))
                           :wait-readers t)
      (let* ((old-idx (gethash old-name (database-indexes db)))
             (old-constraint
               (and old-idx
                    (= (pg-index-table-oid old-idx) (pg-class-oid table))
                    (pg-index-constraint-owned-p old-idx)
                    old-idx))
             (new-idx (gethash index-name (database-indexes db)))
             (new-name (or (getf add-action :constraint-name)
                           index-name)))
        (unless (and (eq original-old-idx old-idx)
                     (eq original-new-idx new-idx))
          (ereport "55000" "index metadata changed while waiting for lock"))
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
        (when (index-has-expressions-p new-idx)
          (ereport "42809" "index ~s has expressions or a predicate" index-name))
        (check-distinct-constraint-columns table (pg-index-columns new-idx) t)
        (check-not-null-existing table (pg-index-columns new-idx) new-name)
        (when (or (gethash new-name (database-tables db))
                  (let ((named (gethash new-name (database-indexes db))))
                    (and named
                         (not (eq named old-constraint))
                         (not (eq named new-idx)))))
          (ereport "42P07" "relation \"~a\" already exists" new-name))
        (when (find new-name (pg-class-checks table) :key (lambda (check) (getf check :name)) :test #'string=)
          (ereport "42710" "constraint ~s already exists" new-name))
        (with-catalog-mutation-rollback (db)
          (when old-constraint
            (check-no-dependent-view old-constraint (database-tables db))
            (drop-index* db old-name))
          (setf (pg-index-primary-p new-idx) t
                (pg-index-constraint-p new-idx) t)
          (dolist (attnum (pg-index-columns new-idx))
            (setf (pg-column-notnull
                   (aref (pg-class-columns table) (1- attnum)))
                  t))
          (unless (string= new-name (pg-index-name new-idx))
            (rename-index* db (pg-index-name new-idx) new-name))
          (save-catalog :final-ddl-p t :logical-event
                        (logical-catalog-event-for-ast ast)))))))

(defun exec-alter-column-storage (db table ast)
  "Change a column's policy for future tuples and publish the catalog change."
  (with-table-ddl (table :ast ast :refresh-snapshot t)
    (let* ((name (getf ast :column))
           (column (find-column table name))
           (requested (getf ast :storage)))
      (unless column
        (if (member name '("ctid" "xmin" "xmax" "cmin" "cmax" "tableoid") :test #'string=)
            (ereport "0A000" "cannot alter system column ~s" name)
            (ereport "42703" "column ~s does not exist" name)))
      (let* ((default (sql-type-storage-code (pg-column-type column)))
             (storage (cond ((string-equal requested "default") default)
                            ((string-equal requested "plain") #\p)
                            ((string-equal requested "external") #\e)
                            ((string-equal requested "extended") #\x)
                            ((string-equal requested "main") #\m)
                            (t (ereport "22023" "invalid storage type ~s" requested)))))
        (when (and (char= default #\p) (char/= storage #\p))
          (ereport "0A000" "column data type ~a can only have storage PLAIN"
                   (sql-type-catalog-name (pg-column-type column))))
        (with-catalog-mutation-rollback (db)
          (let ((replacement (copy-pg-column column))
                (columns (copy-seq (pg-class-columns table))))
            (setf (pg-column-storage replacement) storage
                  (aref columns (1- (pg-column-attnum column))) replacement
                  (pg-class-columns table) columns))
          (save-catalog :final-ddl-p t :logical-event (logical-catalog-event-for-ast ast)))))))

(defun exec-alter-table (stream ast)
  (let* ((name (getf ast :table))
         (primary (eq (getf ast :action) :add-primary-key))
         (db nil)
         (table nil))
    (with-write-lock
      (setf db (current-database)
            table (lookup-sql-user-relation name)))
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
      ((eq (getf ast :action) :set-storage)
       (exec-alter-column-storage db table ast))
      ((or (member (getf ast :action) '(:set-default :drop-default :add-check :validate-constraint))
           (and (member (getf ast :action) '(:rename-constraint :drop-constraint))
                (find (getf ast :constraint-name) (pg-class-checks table)
                      :key (lambda (check) (getf check :name)) :test #'string=)))
       (exec-alter-stored-expression stream db table ast))
      ((getf ast :actions)
       (exec-alter-table-replace-primary
        stream db table (getf ast :actions) ast))
      ((getf ast :using-index)
       (exec-alter-table-using-index db table ast primary))
      ((member (getf ast :action) '(:rename-constraint :drop-constraint))
       (exec-alter-table-constraint stream db table ast))
      (t
       (let ((attnums (resolve-attnums table (getf ast :columns))))
         (check-distinct-constraint-columns table attnums primary)
         (with-table-ddl (table :ast ast :refresh-snapshot t)
           (when (and primary
                      (find-if #'pg-index-primary-p (table-indexes table)))
             (ereport "42P16"
                      "multiple primary keys for table \"~a\" are not allowed"
                      name))
           (when primary
             (check-not-null-existing table attnums (getf ast :constraint-name)))
           (let* ((*catalog-save-error-after-durable-p* nil)
                  (durable-generation *catalog-durable-generation*)
                  (previous-notnull
                    (when primary
                      (mapcar (lambda (attnum)
                                (cons attnum
                                      (pg-column-notnull
                                       (aref (pg-class-columns table)
                                             (1- attnum)))))
                              attnums)))
                  (reserved-names
                    (append (mapcar (lambda (check) (getf check :name)) (pg-class-checks table))
                            (mapcar #'pg-foreign-key-name (pg-class-foreign-keys table))))
                  (constraint-name (getf ast :constraint-name))
                  (index-name
                    (cond ((member constraint-name reserved-names :test #'equal)
                           (ereport "42710" "constraint ~s for relation ~s already exists"
                                    constraint-name name))
                          (constraint-name)
                          (primary (generate-index-name db name "pkey" reserved-names))
                          (t (generate-unique-index-name db name table attnums reserved-names))))
                  (idx (create-index*
                        db index-name
                        table attnums :unique t :primary primary
                        :constraint t
                        :nulls-not-distinct
                        (getf ast :nulls-not-distinct))))
             (with-error-cleanup
                 (e (unless (or *catalog-save-error-after-durable-p*
                                (> *catalog-durable-generation* durable-generation))
                      (progn
                        (drop-index* db (pg-index-name idx))
                        (dolist (entry previous-notnull)
                          (setf (pg-column-notnull
                                 (aref (pg-class-columns table)
                                       (1- (car entry))))
                                (cdr entry))))))
               (build-unique-index idx table (pg-index-name idx))
               (when primary
                 (dolist (attnum attnums)
                   (setf (pg-column-notnull
                          (aref (pg-class-columns table)
                                (1- attnum)))
                         t)))
               (fsync-index idx)
               (save-catalog :final-ddl-p t :logical-event
                             (logical-catalog-event-for-ast ast))))))))
    (send-command-complete stream "ALTER TABLE")))

;;; ---- simple-query execution ----

;;; ---- simple-query execution ----

(defun commit-ddl-and-flush-responses (responses)
  "Commit a DDL command before sending its deferred success and notice replies."
  (commit-transaction)
  (flush-logical-ddl-deferred-responses responses))

(defun do-statement-block (ast)
  "Validate DO options and compile the supported PL/pgSQL source."
  (let ((source nil) (source-p nil) (language nil) (language-p nil))
    (dolist (option (getf ast :options))
      (ecase (car option)
        (:source
         (when source-p (ereport "42601" "conflicting or redundant options"))
         (setf source (cdr option) source-p t))
        (:language
         (when language-p (ereport "42601" "conflicting or redundant options"))
         (setf language (cdr option) language-p t))))
    (unless source-p (ereport "42601" "no inline code specified"))
    (unless (or (not language-p) (string= language "plpgsql"))
      (if (member language '("sql" "c" "internal") :test #'string=)
          (ereport "0A000" "language ~a does not support inline code execution" language)
          (ereport "42704" "language ~a does not exist" language)))
    (compile-do-source source)))

(defun exec-role-change (stream ast)
  (when (backend-transaction-read-only-p)
    (ereport "25006" "cannot change roles in a read-only transaction"))
  (unless (string= (backend-startup-user *backend*) "postgres")
    (ereport "42501" "permission denied to change roles"))
  (when (or (backend-xid *backend*) (backend-final-ddl-committed-p *backend*))
    (ereport "0A000" "role changes cannot share a transaction with data or other catalog changes"))
  (stage-catalog-role-change *backend* ast)
  (send-command-complete stream (ecase (getf ast :type)
                                  (:create-role "CREATE ROLE")
                                  (:alter-role "ALTER ROLE")
                                  (:grant-role-function "GRANT"))))

(defvar *plpgsql-routine* nil)

(defvar *plpgsql-found-variable* nil)

(defvar *plpgsql-return-tag* nil)

(defvar *plpgsql-loop-tag* nil)

(defun eval-plpgsql-expression (syntax &optional boolean-p)
  (let* ((expression
           (if (raw-query-sublinks-p syntax)
               (call-with-plpgsql-statement-snapshot
                (lambda ()
                  (multiple-value-bind (row targets)
                      (read-plpgsql-select-row
                       (list :type :select :targets (list (list :expr syntax))) nil)
                    (let ((target (first targets)))
                      (list :const (aref row 0) :explicit-cast t
                            :type (getf target :type) :typmod (getf target :typmod))))))
               (analyze-expr syntax nil)))
         (typed (if boolean-p (coerce-boolean-expression expression "PL/pgSQL condition") expression)))
    (values (eval-expr typed #()) (expr-type typed))))

(defun plpgsql-cast-value (value source target typmod)
  (cond ((eq value +null+) +null+)
        ((sql-assignment-coercion-p source target) (sql-cast-value value source target typmod))
        (t (sql-cast-value (value->text value source) :text target typmod))))

(defun plpgsql-target-variable (name)
  (let ((variable (find name *plpgsql-variables* :key #'plpgsql-variable-name :test #'equal)))
    (unless variable (ereport "42601" "\"~a\" is not a known variable" name))
    (when (plpgsql-variable-constant-p variable)
      (ereport "22005" "variable \"~a\" is declared CONSTANT" name))
    variable))

(defun assign-plpgsql-variable (variable value source)
  (let ((coerced (plpgsql-cast-value (copy-subquery-value value) source
                                     (plpgsql-variable-type variable)
                                     (plpgsql-variable-typmod variable)))
        (slot (plpgsql-variable-argument-slot variable)))
    (setf (plpgsql-variable-value variable) coerced)
    (when slot (setf (svref *query-parameter-values* slot) coerced))))

(defun initialize-plpgsql-block-variables (declarations)
  (let ((names nil))
    (dolist (declaration declarations)
      (destructuring-bind (name type typmod default) declaration
        (when (member name names :test #'equal)
          (ereport "42601" "duplicate declaration of variable \"~a\"" name))
        (push name names)
        (let ((variable (make-plpgsql-variable :name name :type type :typmod typmod :value +null+)))
          (when default
            (multiple-value-bind (value source) (eval-plpgsql-expression default)
              (assign-plpgsql-variable variable value source)))
          (push variable *plpgsql-variables*))))))

(defun plpgsql-raise-message (ast)
  (cond
    ((getf ast :format)
     (let ((format-text (getf ast :format))
           (arguments (copy-list (getf ast :arguments))))
       (with-output-to-string (output)
         (loop for index from 0 below (length format-text)
               for character = (char format-text index)
               do (cond
                    ((char/= character #\%) (write-char character output))
                    ((and (< (1+ index) (length format-text))
                          (char= #\% (char format-text (1+ index))))
                     (incf index)
                     (write-char #\% output))
                    (arguments
                     (multiple-value-bind (value type) (eval-plpgsql-expression (pop arguments))
                       (write-string (if (eq value +null+) "<NULL>" (value->text value type)) output)))
                    (t (ereport "42601" "too few parameters specified for RAISE"))))
         (when arguments (ereport "42601" "too many parameters specified for RAISE")))))
    ((getf ast :message)
     (multiple-value-bind (value type) (eval-plpgsql-expression (getf ast :message))
       (when (eq value +null+) (ereport "22004" "RAISE statement option cannot be null"))
       (value->text value type)))
    (t (or (getf ast :condition-name) (getf ast :state)))))

(defun read-plpgsql-select-row (query strict-p &key drain-p found-variable)
  "Copy the first result and check cardinality; dynamic EXECUTE drains all rows."
  (let* ((plan (plan-select-query query))
         (targets (getf (query-plan-prepared plan) :targets))
         (row nil)
         (additional-row-p nil))
    (call-with-admitted-select-plan
     plan (lambda (relations)
            (map-query-plan-rows
             plan relations
             (lambda (result)
               (cond (row
                      (setf additional-row-p t)
                      (unless drain-p :stop))
                     (t
                      (setf row (copy-subquery-value (car result)))
                      (unless (or strict-p drain-p) :stop)))))))
    (when found-variable
      (setf (plpgsql-variable-value found-variable) (not (null row))))
    (when strict-p
      (cond ((null row) (ereport "P0002" "query returned no rows"))
            (additional-row-p (ereport "P0003" "query returned more than one row"))))
    (values row targets)))

(defun assign-plpgsql-select-row (variables row targets)
  (loop for variable in variables for column from 0
        for target = (nth column targets)
        for value = (if (and row target) (aref row column) +null+)
        do (assign-plpgsql-variable variable value (getf target :type))))

(defun execute-plpgsql-select-into (ast)
  (let ((variables (mapcar #'plpgsql-target-variable (getf ast :names))))
    (multiple-value-bind (row targets)
        (read-plpgsql-select-row (getf ast :query) (getf ast :strict)
                                 :found-variable *plpgsql-found-variable*)
      (assign-plpgsql-select-row variables row targets))))

(defun evaluate-plpgsql-execute-parameters (syntax)
  "Evaluate USING expressions once before switching to the dynamic SQL scope."
  (let ((types (make-array (length syntax)))
        (values (make-array (length syntax))))
    (loop for expression in syntax for slot from 0
          do (multiple-value-bind (value type) (eval-plpgsql-expression expression)
               (setf (svref values slot) value
                     (svref types slot) type)))
    (values types values)))

(defun execute-plpgsql-dynamic-sql (ast)
  (multiple-value-bind (value type) (eval-plpgsql-expression (getf ast :expression))
    (when (eq value +null+) (ereport "22004" "query string argument of EXECUTE is null"))
    (let ((query (value->text value type))
          (variables (mapcar #'plpgsql-target-variable (getf ast :names))))
      (multiple-value-bind (types values) (evaluate-plpgsql-execute-parameters (getf ast :using))
        (let* ((statements (parse-statements query))
               (statement (first statements)))
          (unless (and statement (null (rest statements)))
            (ereport "0A000" "EXECUTE requires exactly one SQL statement"))
          (multiple-value-bind (row targets)
              (let ((*query-parameter-types* types)
                    (*query-parameter-values* values)
                    (*sql-routine-parse-context* nil)
                    (*plpgsql-execution-p* nil)
                    (executor (scalar-routine-dml-executor statement)))
                (recovery-check-statement statement)
                (cond
                  ((eq (getf statement :type) :select)
                   (if variables
                       (read-plpgsql-select-row statement (getf ast :strict) :drain-p t)
                       (progn (exec-perform statement) (values nil nil))))
                  ((and (not variables) executor)
                   (when (and *plpgsql-routine*
                              (not (eq (pg-routine-volatility *plpgsql-routine*) :volatile)))
                     (ereport "0A000" "write commands are not allowed in a non-volatile function"))
                   (execute-scalar-routine-dml executor statement nil)
                   (values nil nil))
                  (t (ereport "0A000" "unsupported statement for PL/pgSQL EXECUTE"))))
            (when variables (assign-plpgsql-select-row variables row targets))))))))

(defun call-with-fresh-routine-snapshot (function)
  "Advance the command ID, run FUNCTION and restore the caller's snapshot."
  (declare (dynamic-extent function))
  (let ((saved-snapshot (backend-snapshot *backend*))
        (*statement-snapshot-override* nil))
    (unwind-protect
         (with-statement-snapshot
             (snapshot (lambda (registration) (statement-start registration t)))
           (funcall function))
      (setf (backend-snapshot *backend*) saved-snapshot))))

(defun call-with-plpgsql-statement-snapshot (function)
  (declare (dynamic-extent function))
  (let ((*subquery-execution-active-p* nil))
    (if *plpgsql-routine*
        (call-with-sql-routine-statement *plpgsql-routine* function)
        (call-with-fresh-routine-snapshot function))))

(defun execute-plpgsql-return (ast)
  (let ((expression (getf ast :expression)))
    (cond
      (*plpgsql-routine*
       (unless expression (ereport "42601" "RETURN must specify a value"))
       (multiple-value-bind (value source) (eval-plpgsql-expression expression)
         (throw *plpgsql-return-tag*
           (plpgsql-cast-value (copy-subquery-value value) source
                               (pg-routine-result-type *plpgsql-routine*) -1))))
      (expression (ereport "42804" "RETURN cannot have a parameter in a DO block"))
      (t (throw *plpgsql-return-tag* nil)))))

(defvar *plpgsql-exception* nil)

(defvar *plpgsql-subtransaction-depth* 0)

(defun call-with-plpgsql-subtransaction (function)
  "Run a block under the heap and resource checkpoints used by SAVEPOINT."
  (declare (dynamic-extent function))
  (let* ((backend *backend*)
         (outer (backend-savepoints backend))
         (checkpoint (capture-backend-savepoint backend nil))
         (*plpgsql-subtransaction-depth* (1+ *plpgsql-subtransaction-depth*)))
    (unwind-protect
         (progn
           (push checkpoint (backend-savepoints backend))
           (handler-case (progn (funcall function) nil)
             (error (condition)
               (when (or (fatal-error-p condition)
                         (not (member checkpoint (backend-savepoints backend))))
                 (error condition))
               (rollback-backend-savepoint backend checkpoint)
               condition)))
      (sb-sys:without-interrupts
        (when (member checkpoint (backend-savepoints backend))
          (setf (backend-savepoints backend) outer)
          (unless outer (setf (backend-heap-undo-log backend) nil)))))))

(defun plpgsql-exception-matches-p (state condition)
  (if (eq condition :others)
      (not (member state '("57014" "P0004") :test #'equal))
      (or (string= state condition)
          (and (string= condition "000" :start1 2)
               (string= state condition :end1 2 :end2 2)))))

(defun execute-plpgsql-exception-handler (stream condition handlers)
  (unless (typep condition 'pg-error) (error condition))
  (let* ((state (pg-error-sqlstate condition))
         (handler (find-if (lambda (entry)
                             (some (lambda (name) (plpgsql-exception-matches-p state name))
                                   (first entry)))
                           handlers)))
    (unless handler (error condition))
    (let ((*plpgsql-exception* condition)
          (*plpgsql-variables*
            (list* (make-plpgsql-variable :name "sqlstate" :type :text :typmod -1
                                          :value state :constant-p t)
                   (make-plpgsql-variable :name "sqlerrm" :type :text :typmod -1
                                          :value (pg-error-message condition) :constant-p t)
                   *plpgsql-variables*)))
      (dolist (statement (second handler))
        (execute-do-statement stream statement)))))

(defun execute-plpgsql-block-body (stream ast)
  (let ((handlers (getf ast :handlers))
        (statements (getf ast :statements)))
    (flet ((body ()
             (dolist (statement statements)
               (execute-do-statement stream statement))))
      (if handlers
          (let ((condition (call-with-plpgsql-subtransaction #'body)))
            (when condition (execute-plpgsql-exception-handler stream condition handlers)))
          (body)))))

(defun execute-plpgsql-stacked-diagnostics (assignments)
  (let ((condition *plpgsql-exception*))
    (unless condition
      (ereport "0Z002" "GET STACKED DIAGNOSTICS cannot be used outside an exception handler"))
    (dolist (assignment assignments)
      (destructuring-bind (name item) assignment
        (let ((value (cond ((string= item "returned_sqlstate") (pg-error-sqlstate condition))
                           ((string= item "message_text") (pg-error-message condition))
                           ((string= item "pg_exception_detail") (pg-error-detail condition))
                           ((string= item "pg_exception_hint") (pg-error-hint condition))
                           (t (ereport "0A000" "unsupported stacked diagnostics item ~a" item)))))
          (assign-plpgsql-variable (plpgsql-target-variable name) (or value "") :text))))))

(defun execute-do-statement (stream ast)
  "Execute a PL/pgSQL statement in the surrounding transaction."
  (check-for-interrupts)
  (case (getf ast :type)
    (:do-null (values))
    (:do-block
        (let ((*plpgsql-variables* *plpgsql-variables*))
          (initialize-plpgsql-block-variables (getf ast :declarations))
          (execute-plpgsql-block-body stream ast)))
    (:do-if
        (dolist (statement (getf ast (if (true-p (eval-plpgsql-expression (getf ast :condition) t))
                                         :then :else)))
          (execute-do-statement stream statement)))
    (:do-loop
        (let ((*plpgsql-loop-tag* (gensym "PLPGSQL-LOOP-"))
              (condition (getf ast :condition)))
          (catch *plpgsql-loop-tag*
            (loop while (or (null condition) (true-p (eval-plpgsql-expression condition t)))
                  do (check-for-interrupts)
                     (dolist (statement (getf ast :statements))
                       (execute-do-statement stream statement))))))
    (:do-exit
        (unless *plpgsql-loop-tag* (ereport "42601" "EXIT cannot be used outside a loop"))
      (when (or (null (getf ast :condition))
                (true-p (eval-plpgsql-expression (getf ast :condition) t)))
        (throw *plpgsql-loop-tag* nil)))
    (:do-assign
        (let ((variable (plpgsql-target-variable (getf ast :name))))
          (multiple-value-bind (value type) (eval-plpgsql-expression (getf ast :expression))
            (assign-plpgsql-variable variable value type))))
    (:do-return (execute-plpgsql-return ast))
    (:do-raise (ereport (getf ast :state) "~a" (plpgsql-raise-message ast)))
    (:do-reraise
        (if *plpgsql-exception*
            (error *plpgsql-exception*)
            (ereport "0Z002" "RAISE without parameters cannot be used outside an exception handler")))
    (:do-stacked-diagnostics (execute-plpgsql-stacked-diagnostics (getf ast :assignments)))
    (otherwise
     (call-with-plpgsql-statement-snapshot
      (lambda ()
        (unless (eq (getf ast :type) :do-execute)
          (recovery-check-statement (or (getf ast :query) ast)))
        (case (getf ast :type)
          (:perform
           (setf (plpgsql-variable-value *plpgsql-found-variable*) (exec-perform (getf ast :query))))
          (:do-select-into (execute-plpgsql-select-into ast))
          (:do-execute (execute-plpgsql-dynamic-sql ast))
          (otherwise
           (when (or *plpgsql-routine* (plusp *plpgsql-subtransaction-depth*))
             (ereport "0A000" "utility commands are not supported in this PL/pgSQL context"))
           (case (getf ast :type)
             ((:create-role :alter-role :grant-role-function) (exec-role-change stream ast))
             (:set (exec-set stream ast))
             (:reset (exec-reset stream ast))
             (t (ereport "0A000" "unsupported PL/pgSQL statement"))))))))))

(defun validate-plpgsql-block (block routine)
  "Resolve procedural targets without evaluating expressions or executing SQL."
  (labels ((require-target (name scope)
             (let ((binding (assoc name scope :test #'equal)))
               (unless binding (ereport "42601" "\"~a\" is not a known variable" name))
               (when (cdr binding)
                 (ereport "22005" "variable \"~a\" is declared CONSTANT" name))))
           (visit-list (statements scope in-loop-p)
             (dolist (statement statements) (visit statement scope in-loop-p)))
           (visit (ast scope in-loop-p)
             (case (getf ast :type)
               (:do-block
                   (let ((locals nil))
                     (dolist (declaration (getf ast :declarations))
                       (let ((name (first declaration)))
                         (when (assoc name locals :test #'equal)
                           (ereport "42601" "duplicate declaration of variable \"~a\"" name))
                         (push (cons name nil) locals)))
                     (let ((nested (append locals scope)))
                       (visit-list (getf ast :statements) nested in-loop-p)
                       (dolist (handler (getf ast :handlers))
                         (visit-list (second handler)
                                     (list* '("sqlstate" . t) '("sqlerrm" . t) nested)
                                     in-loop-p)))))
               (:do-if
                   (visit-list (getf ast :then) scope in-loop-p)
                 (visit-list (getf ast :else) scope in-loop-p))
               (:do-loop (visit-list (getf ast :statements) scope t))
               (:do-exit
                   (unless in-loop-p (ereport "42601" "EXIT cannot be used outside a loop")))
               (:do-assign (require-target (getf ast :name) scope))
               ((:do-select-into :do-execute)
                (dolist (name (getf ast :names)) (require-target name scope)))
               (:do-stacked-diagnostics
                   (dolist (assignment (getf ast :assignments))
                     (require-target (first assignment) scope)))
               (:do-return
                   (cond ((and routine (null (getf ast :expression)))
                          (ereport "42601" "RETURN must specify a value"))
                         ((and (null routine) (getf ast :expression))
                          (ereport "42804" "RETURN cannot have a parameter in a DO block")))))))
    (visit block
           (cons '("found") (when routine
                              (loop for name in (pg-routine-argument-names routine)
                                    when name collect (cons name nil))))
           nil))
  block)

(defun run-plpgsql-block (stream block &optional routine arguments)
  "Share the procedural frame between anonymous blocks and scalar routines."
  (validate-plpgsql-block block routine)
  (let* ((*plpgsql-routine* routine)
         (*plpgsql-execution-p* t)
         (*statement-nested-p* t)
         (*plpgsql-found-variable*
           (make-plpgsql-variable :name "found" :type :bool :typmod -1 :value nil))
         (*plpgsql-exception* nil)
         (*plpgsql-loop-tag* nil)
         (*plpgsql-return-tag* (gensym "PLPGSQL-RETURN-"))
         (*sql-routine-parse-context* routine)
         (*query-parameter-types* (when routine (coerce (pg-routine-argument-types routine) 'vector)))
         (*query-parameter-values* (when routine (coerce arguments 'vector)))
         (*plpgsql-variables*
           (cons *plpgsql-found-variable*
                 (when routine
                   (loop for name in (pg-routine-argument-names routine)
                         for type in (pg-routine-argument-types routine)
                         for slot from 0
                         when name collect (make-plpgsql-variable :name name :type type :typmod -1
                                                                  :argument-slot slot
                                                                  :value (svref *query-parameter-values* slot)))))))

    (catch *plpgsql-return-tag*
      (execute-do-statement stream block)
      (when routine (ereport "2F005" "control reached end of function without RETURN")))))

(defun exec-do (stream ast)
  "Execute an anonymous block and publish only the enclosing DO completion."
  (let ((block (do-statement-block ast))
        (*executing-do-p* t)
        (*defer-command-complete-p* t)
        (*deferred-command-completes* nil))
    (session-remember-settings)
    (run-plpgsql-block stream block))
  (send-command-complete stream "DO"))

(defun column-evolution-ddl-p (ast)
  (and (eq :alter-table (getf ast :type))
       (flet ((supported-action-p (action)
                (member (getf action :action) '(:add-column :drop-column :rename-column :set-not-null :drop-not-null :alter-column-type))))
         (or (supported-action-p ast) (some #'supported-action-p (getf ast :actions))))))

(defun foreign-key-create-ddl-p (ast)
  (and (eq (getf ast :type) :create-table)
       (find :foreign-key (getf ast :constraints) :key (lambda (constraint) (getf constraint :kind)))))

(defun foreign-key-add-ddl-p (ast)
  (and (eq (getf ast :type) :alter-table)
       (eq (getf ast :action) :add-foreign-key)))

(defun foreign-key-validation-ddl-p (ast)
  (and (eq (getf ast :type) :alter-table)
       (eq (getf ast :action) :validate-constraint)
       (with-write-lock
         (let ((table (lookup-sql-user-relation (getf ast :table))))
           (and table
                (find (getf ast :constraint-name) (pg-class-foreign-keys table)
                      :key #'pg-foreign-key-name :test #'string=))))))

(defun stageable-catalog-ddl-p (ast)
  (or (column-evolution-ddl-p ast)
      (foreign-key-add-ddl-p ast)
      (foreign-key-validation-ddl-p ast)
      (and (member (getf ast :type) '(:create-table :drop-table :create-view :drop-view :create-index :drop-index
                                      :create-function :drop-function))
           (not (getf ast :concurrently)))))

(defun staged-catalog-ddl-p (ast)
  (and (stageable-catalog-ddl-p ast)
       (or (member (getf ast :type) '(:create-view :drop-view :create-function :drop-function))
           (and (member (getf ast :type) '(:drop-table :drop-index)) (getf ast :cascade))
           (column-evolution-ddl-p ast) (foreign-key-create-ddl-p ast) (foreign-key-add-ddl-p ast) (foreign-key-validation-ddl-p ast) (backend-explicit *backend*) (backend-implicit-query-block *backend*))))

(defun append-staged-transaction-ddl (transaction ast)
  "Append a cooked operation beside the surrounding row changes."
  (let* ((database-oid (database-oid (catalog-transaction-database transaction)))
         (change (make-logical-change
                  :kind :transaction-ddl :database-oid database-oid
                  :event-payload
                  (catalog-readable-data-string
                   (list :version 1 :database-oid database-oid :ast ast)))))
    (logical-capture-change *backend* change +max-logical-transaction-bytes+)
    (setf (catalog-transaction-logical-ddl-p transaction) t)))

(defmacro capture-staged-transaction-ddl ((transaction) &body cooked-ast)
  "Build the replay AST only when this XID captures logical changes. In particular,
avoid acquiring the catalog read lock or allocating a thunk on the local-only path."
  (let ((owner (gensym "TRANSACTION-")))
    `(let ((,owner ,transaction))
       (when (and (catalog-transaction-capture-ddl-p ,owner)
                  (logical-note-xid-mutation-needs-capture-p (backend-xid *backend*)))
         (append-staged-transaction-ddl ,owner (progn ,@cooked-ast))))))

(defun begin-catalog-ddl ()
  "Retain schema admission and create the transaction-private catalog on demand."
  (when (or *logical-event-schema-contract* *logical-event-apply-id*)
    (ereport "0A000" "transactional DDL cannot yet apply a logical catalog event"))
  (when (backend-final-ddl-committed-p *backend*)
    (ereport "0A000" "transactional DDL cannot follow separately published catalog changes"))
  (unless *wal-ready*
    (ereport "0A000" "transactional catalog changes require native WAL"))
  (get-current-xid)
  (release-backend-transaction-schema-lease *backend*)
  (acquire-transaction-schema-write-lock *backend*)
  (with-write-catalog-locks
    (or (backend-catalog-transaction *backend*)
        (setf (backend-catalog-transaction *backend*) (make-catalog-transaction (current-database))))))

(defun admit-staged-catalog-change (transaction)
  "Check slot admission under the catalog locks before an effective change.
The first change chooses the capture mode. Earlier no-ops leave it undecided."
  (let* ((database (catalog-transaction-database transaction))
         (mode (sb-thread:with-mutex (*wal-lock*)
                 (wal-transactional-ddl-mode-under-lock (database-oid database))))
         (capture-p (if (catalog-transaction-staged-ddl-p transaction)
                        (catalog-transaction-capture-ddl-p transaction)
                        (and (eq mode :transactional)
                             (logical-capture-xid-needed-p (backend-xid *backend*))))))
    (when (and mode (or (eq mode :incompatible) (not capture-p)))
      (ereport "0A000" "transactional DDL requires compatible logical replication slots"))
    (setf (catalog-transaction-capture-ddl-p transaction) capture-p)))

(defun staged-relation-name-in-use-p (transaction name)
  (or (catalog-transaction-lookup transaction :table name)
      (catalog-transaction-lookup transaction :index name)
      (gethash name (database-sequences (catalog-transaction-database transaction)))))

(defun stage-new-table-constraint-indexes (transaction table columns constraints)
  "Create private empty B-trees for a newly staged table's key constraints."
  (let* ((database (catalog-transaction-database transaction))
         (table-name (pg-class-name table))
         (indexes (remove-if-not (lambda (constraint) (member (getf constraint :kind) '(:primary :unique)))
                                 constraints))
         (reserved-names (table-constraint-reserved-names columns constraints))
         (inline (loop for column in columns
                       for primary = (getf column :primary)
                       when (or primary (getf column :unique))
                         collect (list :kind (if primary :primary :unique)
                                       :name (getf column (if primary :primary-name :unique-name))
                                       :columns (list (getf column :name))
                                       :nulls-not-distinct (and (not primary)
                                                                (getf column :nulls-not-distinct))))))
    (when (> (count :primary (append inline indexes) :key (lambda (c) (getf c :kind))) 1)
      (ereport "42P16" "multiple primary keys for table ~s are not allowed" table-name))
    (flet ((name-in-use-p (db name)
             (declare (ignore db))
             (staged-relation-name-in-use-p transaction name)))
      (dolist (constraint (append inline indexes))
        (let ((index
                (with-write-catalog-locks
                  (let* ((primary (eq (getf constraint :kind) :primary))
                         (attnums (resolve-attnums table (getf constraint :columns)))
                         (name (or (getf constraint :name)
                                   (if primary
                                       (generate-index-name database table-name "pkey" reserved-names #'name-in-use-p)
                                       (generate-unique-index-name database table-name table attnums
                                                                   reserved-names #'name-in-use-p)))))
                    (check-distinct-constraint-columns table attnums primary)
                    (check-index-definition table attnums)
                    (when (find name (pg-class-checks table)
                                :key (lambda (check) (getf check :name)) :test #'string=)
                      (ereport "42710" "constraint ~s for relation ~s already exists" name table-name))
                    (when (name-in-use-p database name)
                      (ereport "42P07" "relation ~s already exists" name))
                    (when primary
                      (dolist (attnum attnums)
                        (setf (pg-column-notnull (aref (pg-class-columns table) (1- attnum))) t)))
                    (make-index-object (allocate-oid) name table attnums
                                       :unique t :primary primary :constraint t
                                       :nulls-not-distinct (getf constraint :nulls-not-distinct))))))
          (catalog-transaction-create-storage (pg-index-relfilenode index) :btree)
          (setf (pg-index-valid-p index) t)
          (with-write-catalog-locks
            (catalog-transaction-stage transaction :index (pg-index-name index) index)))))))

(defun validate-existing-foreign-key (child parent constraint index)
  "Validate a frozen child heap against the referenced index after both writer gates drain."
  (let ((demand (cons child (pg-foreign-key-columns constraint)))
        (probe (prepare-child-foreign-key-probe constraint parent index))
        (cid (1+ (backend-command-id *backend*))))
    (with-fresh-foreign-key-snapshot (snapshot cid)
      (heapam-scan
       (pg-class-relfilenode child) (pg-class-columns child) snapshot
       (lambda (tuple)
         (check-for-interrupts)
         (let ((row (htup-values (materialize-select-tuple child tuple snapshot :demand demand))))
           (multiple-value-bind (key status) (encode-child-foreign-key probe row)
             (unless (or (eq status :null)
                         (and (eq status :key) (foreign-key-parent-visible-p parent index key snapshot)))
               (ereport "23503" "insert or update on table ~s violates foreign key constraint ~s"
                        (pg-class-name child) (pg-foreign-key-name constraint))))))))))

(defun exec-stage-add-foreign-key (stream ast)
  "Retain both relation gates until the validating descriptor commits."
  (let* ((transaction (begin-catalog-ddl))
         (definition (getf ast :foreign-key))
         (child (with-write-lock (lookup-sql-user-relation (getf ast :table)))))
    (cond
      (child
       (let ((parent (with-write-lock (resolve-table-or-error (getf definition :referenced-table))))
             (replacement nil)
             (constraint nil)
             (index nil))
         (acquire-transaction-relation-locks
          (append (catalog-relation-storage-nodes child)
                  (catalog-relation-storage-nodes parent)))
         (with-write-catalog-locks
           (ensure-current-table child)
           (ensure-current-table parent)
           (reject-logical-transaction-ddl-private-table (pg-class-name child))
           (admit-staged-catalog-change transaction)
           (when (catalog-transaction-capture-ddl-p transaction)
             (ereport "0A000" "foreign key DDL is unsupported during logical capture"))
           (setf replacement (copy-pg-class child)
                 constraint (cook-foreign-key child parent definition (allocate-oid))
                 index (find-index-by-oid (pg-foreign-key-referenced-index-oid constraint)))
           (check-supported-foreign-key-options constraint)
           (catalog-transaction-observe-table transaction parent))
         (when (pg-foreign-key-validated-p constraint)
           (validate-existing-foreign-key child parent constraint index))
         (push constraint (pg-class-foreign-keys replacement))
         (with-write-catalog-locks
           (catalog-transaction-stage transaction :table (pg-class-name child) replacement))))
      ((getf ast :if-exists)
       (send-notice stream (format nil "relation ~s does not exist, skipping" (getf ast :table))))
      (t (ereport "42P01" "relation ~s does not exist" (getf ast :table)))))
  (send-command-complete stream "ALTER TABLE"))

(defun exec-stage-validate-foreign-key (stream ast)
  "Replace the constraint's validation flag only after scanning a stable pair of relations."
  (let* ((transaction (begin-catalog-ddl))
         (name (getf ast :constraint-name))
         (child (with-write-lock (resolve-table-or-error (getf ast :table))))
         (constraint (find name (pg-class-foreign-keys child) :key #'pg-foreign-key-name :test #'string=)))
    (unless constraint
      (ereport "42704" "constraint ~s of relation ~s does not exist" name (pg-class-name child)))
    (unless (pg-foreign-key-validated-p constraint)
      (multiple-value-bind (parent index)
          (with-write-lock
            (values (find-table-by-oid (pg-foreign-key-referenced-table-oid constraint))
                    (find-index-by-oid (pg-foreign-key-referenced-index-oid constraint))))
        (acquire-transaction-relation-locks
         (append (catalog-relation-storage-nodes child)
                 (catalog-relation-storage-nodes parent)))
        (with-write-catalog-locks
          (ensure-current-table child)
          (ensure-current-table parent)
          (admit-staged-catalog-change transaction)
          (when (catalog-transaction-capture-ddl-p transaction)
            (ereport "0A000" "foreign key DDL is unsupported during logical capture"))
          (catalog-transaction-observe-table transaction parent))
        (validate-existing-foreign-key child parent constraint index)
        (with-write-catalog-locks
          (let ((replacement (copy-pg-class child))
                (validated (copy-pg-foreign-key constraint)))
            (setf (pg-foreign-key-validated-p validated) t
                  (pg-class-foreign-keys replacement)
                  (substitute validated constraint (pg-class-foreign-keys child)))
            (catalog-transaction-stage transaction :table (pg-class-name child) replacement))))))
  (send-command-complete stream "ALTER TABLE"))

(defun check-supported-foreign-key-options (constraint)
  (unless (and (member (pg-foreign-key-match-type constraint) '(:simple :full))
               (member (pg-foreign-key-update-action constraint) '(:no-action :restrict :cascade :set-null :set-default))
               (member (pg-foreign-key-delete-action constraint) '(:no-action :restrict :cascade :set-null :set-default)))
    (ereport "0A000" "requested foreign key options are not supported")))

(defun stage-new-table-foreign-keys (transaction child definitions)
  "Resolve references after the new table's indexes are staged."
  (let ((references
          (with-write-lock
            (loop for definition in definitions
                  collect (cons definition
                                (resolve-table-or-error (getf definition :referenced-table)))))))
    (acquire-transaction-relation-locks
     (loop for (nil . parent) in references
           unless (eq child parent) append (catalog-relation-storage-nodes parent)))
    (with-write-catalog-locks
      (dolist (reference references)
        (let ((parent (cdr reference)))
          (ensure-current-table parent)
          (catalog-transaction-observe-table transaction parent)
          (let ((constraint (cook-foreign-key child parent (car reference) (allocate-oid))))
            (check-supported-foreign-key-options constraint)
            (push constraint (pg-class-foreign-keys child))))))))

(defun view-ddl-relation-name (reference creation-p)
  (let ((schema (and (sql-relation-name-p reference)
                     (sql-relation-name-schema reference))))
    (cond ((and schema (not (string= schema "public")))
           (ereport "0A000" "view DDL in schema \"~a\" is not supported" schema))
          ((and creation-p (null schema)) (ensure-sql-creation-schema)))
    (sql-relation-base-name reference)))

(defun admit-staged-view-ddl (transaction)
  (admit-staged-catalog-change transaction)
  (when (or *applying-transaction-ddl* (catalog-transaction-capture-ddl-p transaction))
    (ereport "0A000" "view DDL is unsupported during logical capture")))

(defun view-output-columns (prepared aliases)
  (let ((targets (getf prepared :targets))
        (seen (make-hash-table :test #'equal)))
    (when (> (length aliases) (length targets))
      (ereport "42601" "CREATE VIEW specifies more column names than columns"))
    (map 'vector #'identity
         (loop for target in targets for attnum from 1
               for name = (if aliases (pop aliases) (getf target :name))
               do (when (gethash name seen)
                    (ereport "42701" "column \"~a\" specified more than once" name))
                  (setf (gethash name seen) t)
               collect (make-pg-column :name name :attnum attnum
                                       :type (getf target :type)
                                       :typmod (getf target :typmod))))))

(defun check-view-replacement-columns (previous columns)
  (let ((old-columns (pg-class-columns previous)))
    (when (> (length old-columns) (length columns))
      (ereport "42P16" "cannot drop columns from view"))
    (loop for old across old-columns for new across columns
          unless (string= (pg-column-name old) (pg-column-name new))
            do (ereport "42P16" "cannot change name of view column \"~a\" to \"~a\""
                        (pg-column-name old) (pg-column-name new))
          unless (and (eq (pg-column-type old) (pg-column-type new))
                      (eql (pg-column-typmod old) (pg-column-typmod new)))
            do (ereport "42P16" "cannot change data type of view column \"~a\""
                        (pg-column-name old)))))

(defun exec-stage-create-view (stream ast)
  (let* ((name (view-ddl-relation-name (getf ast :name) t))
         (transaction (begin-catalog-ddl)))
    (with-write-catalog-locks
      (let* ((previous (catalog-transaction-lookup transaction :table name))
             (*expanding-view-oids* (when previous (list (pg-class-oid previous)))))
        (when (and (staged-relation-name-in-use-p transaction name)
                   (not (and previous (getf ast :replace))))
          (ereport "42P07" "relation \"~a\" already exists" name))
        (when (and previous (not (pg-class-view-definition previous)))
          (ereport "42809" "\"~a\" is not a view" name))
        (admit-staged-view-ddl transaction)
        (multiple-value-bind (prepared query dependencies attribute-dependencies sequence-dependencies)
            (analyze-view-query (getf ast :query))
          (let ((columns (view-output-columns prepared (getf ast :aliases))))
            (when previous (check-view-replacement-columns previous columns))
            (dolist (oid dependencies)
              (let ((relation (catalog-transaction-find-by-oid transaction :table oid)))
                (when relation
                  (catalog-transaction-observe-table transaction relation))))
            (catalog-transaction-stage
             transaction :table name
             (make-pg-class :oid (if previous (pg-class-oid previous) (allocate-oid))
                            :name name :columns columns
                            :view-definition
                            (make-pg-view-definition
                             :query query :source (getf ast :source)
                             :owner (if previous
                                        (pg-view-definition-owner (pg-class-view-definition previous))
                                        (current-user-name))
                             :dependencies dependencies
                             :attribute-dependencies attribute-dependencies
                             :sequence-dependencies sequence-dependencies)))))))
    (send-command-complete stream "CREATE VIEW")))

(defun dependent-views (relations oid &optional attnum)
  "Find views directly dependent on the relation or on its specified attribute."
  (loop for candidate being the hash-values of relations
        for definition = (pg-class-view-definition candidate)
        when (and definition
                  (if attnum
                      (member (list oid attnum)
                              (pg-view-definition-attribute-dependencies definition)
                              :test #'equal)
                      (member oid (pg-view-definition-dependencies definition))))
          collect candidate))

(defun stage-dependent-view-drops (transaction roots cascade-p &optional attnum)
  "Check or stage views depending on relations or indexes in ROOTS."
  (let ((relations (catalog-transaction-merged-name-map transaction :table))
        (seen (make-hash-table :test #'eql))
        (pending (mapcar (lambda (relation) (cons relation attnum)) roots))
        (drops nil))
    (labels ((object-oid (relation)
               (etypecase relation
                 (pg-class (pg-class-oid relation))
                 (pg-index (pg-index-oid relation))))
             (object-name (relation)
               (etypecase relation
                 (pg-class (pg-class-name relation))
                 (pg-index (pg-index-name relation)))))
      (dolist (root roots) (setf (gethash (object-oid root) seen) t))
      (loop while pending
            for (relation . attribute) = (pop pending)
            do (dolist (dependent (dependent-views relations (object-oid relation) attribute))
                 (unless (gethash (pg-class-oid dependent) seen)
                   (unless cascade-p
                     (ereport "2BP01" "view ~s depends on ~s" (pg-class-name dependent)
                              (object-name relation)))
                   (setf (gethash (pg-class-oid dependent) seen) t)
                   (push dependent drops)
                   (push (cons dependent nil) pending)))))
    (when drops (admit-staged-view-ddl transaction))
    (dolist (view drops)
      (catalog-transaction-stage transaction :table (pg-class-name view) nil))))

(defun exec-stage-drop-view (stream ast)
  (let ((transaction (begin-catalog-ddl))
        (roots nil))
    (with-write-catalog-locks
      (dolist (reference (getf ast :names))
        (let* ((name (view-ddl-relation-name reference nil))
               (relation (lookup-sql-relation reference)))
          (cond ((and relation (pg-class-view-definition relation))
                 (pushnew relation roots))
                ((or relation
                     (and (or (sql-relation-name-p reference)
                              (member "public" (effective-sql-search-path) :test #'string=))
                          (staged-relation-name-in-use-p transaction name)))
                 (ereport "42809" "\"~a\" is not a view" name))
                ((getf ast :if-exists)
                 (send-notice stream (format nil "view ~s does not exist, skipping" name)
                              "NOTICE" "42P01"))
                (t (ereport "42P01" "view \"~a\" does not exist" name)))))
      (when roots
        (admit-staged-view-ddl transaction)
        (stage-dependent-view-drops transaction roots (getf ast :cascade))
        (dolist (view roots)
          (catalog-transaction-stage transaction :table (pg-class-name view) nil))))
    (send-command-complete stream "DROP VIEW")))

(defun routine-ddl-schema (reference)
  "Resolve the creation namespace using the supported schema search path."
  (if (sql-relation-name-p reference)
      (let ((schema (sql-relation-name-schema reference)))
        (unless (string= schema "public")
          (ereport "0A000" "routine creation in schema \"~a\" is not supported" schema))
        schema)
      (progn (ensure-sql-creation-schema) "public")))

(defun check-routine-owner (routine)
  (unless (or (string= (current-user-name) (pg-routine-owner routine))
              (string= (current-user-name) "postgres"))
    (ereport "42501" "must be owner of function ~a" (pg-routine-name routine))))

(defun check-routine-input-parameters (parameters)
  (let ((names (make-hash-table :test #'equal))
        (has-defaults-p nil))
    (loop for parameter in parameters
          for name = (first parameter)
          for default = (third parameter)
          do (when name
               (when (gethash name names)
                 (ereport "42P13" "parameter name \"~a\" used more than once" name))
               (setf (gethash name names) t))
             (cond (default
                    (setf has-defaults-p t)
                    (let ((*query-parameter-types* nil)
                          (*sql-routine-parse-context* nil)
                          (*defining-persistent-expression-p* t))
                      (validate-stored-expression default :default)
                      (coerce-common-expression (analyze-expr default nil) (second parameter))))
                   (has-defaults-p
                    (ereport "42P13" "input parameters after one with a default value must also have defaults"))))))

(defun check-scalar-routine-replacement (prior ast)
  (unless (getf ast :replace)
    (ereport "42723" "function \"~a\" already exists with same argument types" (pg-routine-name prior)))
  (check-routine-owner prior)
  (unless (and (eq (pg-routine-result-type prior) (getf ast :result-type))
               (eq (pg-routine-returns-set-p prior) (getf ast :returns-set-p))
               (or (not (eq :record (pg-routine-result-type prior)))
                   (equal (pg-routine-result-fields prior) (getf ast :result-fields))))
    (ereport "42P13" "cannot change return type of existing function"))
  (loop for old in (pg-routine-argument-names prior)
        for (new) in (getf ast :parameters)
        when (and old (not (equal old new)))
          do (ereport "42P13" "cannot change name of input parameter \"~a\"" old))
  (when (loop for old in (pg-routine-argument-defaults prior)
              for parameter in (getf ast :parameters)
                thereis (and old (null (third parameter))))
    (ereport "42P13" "cannot remove parameter defaults from existing function")))

(defun parse-scalar-sql-routine-statements (source)
  "Check admitted statement kinds and the presence of a result-producing final statement."
  (let ((statements (parse-statements source)))
    (unless statements (ereport "42P13" "empty function body"))
    (dolist (statement statements)
      (unless (member (getf statement :type) '(:select :insert :update :delete))
        (ereport "0A000" "statement type ~a is not supported in a SQL function"
                 (getf statement :type))))
    (let ((last (car (last statements))))
      (unless (or (eq (getf last :type) :select) (getf last :returning))
        (ereport "42P13" "function's final statement must be SELECT or have RETURNING")))
    statements))

(defun prepare-scalar-sql-routine-ddl (transaction ast &optional (check-body-p t))
  "Build a detached routine descriptor and parse its body when requested."
  (let* ((reference (getf ast :name))
         (schema (routine-ddl-schema reference))
         (name (sql-relation-base-name reference))
         (parameters (getf ast :parameters))
         (types (mapcar #'second parameters))
         (signature (list schema name types))
         (prior (catalog-transaction-lookup transaction :routine signature))
         (options (getf ast :options))
         (source (cdr (assoc :source options)))
         (language-name (cdr (assoc :language options)))
         (language (cond ((equal language-name "sql") :sql)
                         ((equal language-name "plpgsql") :plpgsql))))
    (unless language-name (ereport "42P13" "no language specified"))
    (unless language (ereport "0A000" "routine language \"~a\" is not supported" language-name))
    (when (and (eq language :plpgsql) (getf ast :returns-set-p))
      (ereport "0A000" "set-returning stored PL/pgSQL routines are not supported"))
    (unless source (ereport "42P13" "no function body specified"))
    (check-routine-input-parameters parameters)
    (let* ((fields (getf ast :result-fields))
           (duplicate (loop for (field) in fields
                            for tail on (cdr fields)
                            when (member field tail :key #'first :test #'equal)
                              return field)))
      (when duplicate
        (ereport "42P13" "parameter name \"~a\" used more than once" duplicate)))
    (when prior (check-scalar-routine-replacement prior ast))
    (let ((statements (when check-body-p
                        (ecase language
                          (:sql (parse-scalar-sql-routine-statements source))
                          (:plpgsql (compile-do-source source))))))
      (values (make-pg-routine :oid (if prior (pg-routine-oid prior) (allocate-oid))
                               :schema schema :name name :argument-types types
                               :argument-names (mapcar #'first parameters)
                               :argument-defaults (mapcar #'third parameters)
                               :result-type (getf ast :result-type)
                               :result-fields (getf ast :result-fields)
                               :returns-set-p (getf ast :returns-set-p)
                               :language language :source source
                               :owner (if prior (pg-routine-owner prior) (current-user-name))
                               :generation (if prior (1+ (pg-routine-generation prior)) 0)
                               :volatility (or (cdr (assoc :volatility options)) :volatile)
                               :strict-p (cdr (assoc :strict options)))
              statements))))

(defun find-routine-to-drop (transaction reference specified-p types)
  "Resolve a DROP target by signature or require a unique visible overload."
  (let* ((name (sql-relation-base-name reference))
         (schemas (if (sql-relation-name-p reference)
                      (list (sql-relation-name-schema reference))
                      (effective-sql-search-path))))
    (if specified-p
        (loop for schema in schemas
                thereis (catalog-transaction-lookup transaction :routine (list schema name types)))
        (let ((map (catalog-transaction-merged-name-map transaction :routine))
              (visible (make-hash-table :test #'equal))
              (result nil))
          (dolist (schema schemas result)
            (maphash (lambda (key routine)
                       (when (and (equal schema (first key)) (equal name (second key))
                                  (not (gethash (third key) visible)))
                         (when result (ereport "42725" "function name \"~a\" is not unique" name))
                         (setf (gethash (third key) visible) t result routine)))
                     map))))))

(defun collect-sql-routines-to-drop (transaction ast missing)
  "Resolve and check ownership of each DROP FUNCTION target."
  (declare (dynamic-extent missing))
  (let (routines)
    (dolist (signature (getf ast :signatures))
      (destructuring-bind (reference specified-p types) signature
        (let ((routine (find-routine-to-drop transaction reference specified-p types))
              (name (sql-relation-base-name reference)))
          (cond (routine (pushnew routine routines))
                ((getf ast :if-exists) (funcall missing name))
                (t (ereport "42883" "function \"~a\" does not exist" name))))))
    (mapc #'check-routine-owner routines)
    routines))

(defvar *sql-routine-call-depth* 0)

(defun scalar-routine-source-type (targets result-type)
  "Validate a scalar SQL result descriptor and return its input type."
  (let ((source-type (getf (first targets) :type)))
    (unless (and targets (null (rest targets))
                 (sql-assignment-coercion-p source-type result-type))
      (ereport "42P13" "return type mismatch in function declared to return ~a" result-type))
    source-type))

(defun evaluate-scalar-routine-select (statement result-type)
  "Read the first result row and close the plan, including on an early return."
  (let* ((plan (plan-select-query statement))
         (targets (getf (query-plan-prepared plan) :targets))
         (source-type (scalar-routine-source-type targets result-type)))
    (call-with-admitted-select-plan
     plan (lambda (relations)
            (map-query-plan-rows
             plan relations
             (lambda (row)
               (return-from evaluate-scalar-routine-select
                 (sql-cast-value (copy-subquery-value (aref (car row) 0))
                                 source-type result-type -1))))))
    +null+))

(defun scalar-routine-dml-executor (statement)
  (case (getf statement :type)
    (:insert #'exec-insert)
    (:update #'exec-update)
    (:delete #'exec-delete)))

(defun call-with-sql-routine-statement (routine function)
  "Stable routines retain the caller snapshot; volatile statements get a fresh command ID."
  (declare (dynamic-extent function))
  (if (and *backend* (eq (pg-routine-volatility routine) :volatile))
      (call-with-fresh-routine-snapshot function)
      (funcall function)))

(defun execute-scalar-routine-dml (executor statement result-type)
  "Execute mutations completely and keep at most the first returned value."
  (when (backend-transaction-read-only-p)
    (ereport "25006" "cannot execute write statement in a read-only transaction"))
  (let* ((returning (getf statement :returning))
         (result +null+)
         (*defer-command-complete-p* t)
         (*deferred-command-completes* nil)
         (*select-suppress-description* nil)
         (*wal-row-page-write-p* t)
         (*dml-returning-spool* (when returning (make-query-row-spool)))
         (*dml-returning-receiver*
           (when returning
             (lambda (spool targets tag)
               (declare (ignore tag))
               (when result-type
                 (let ((source-type (scalar-routine-source-type targets result-type)))
                   (block first-row
                     (map-query-row-spool
                      spool (lambda (row index)
                              (declare (ignore index))
                              (setf result (sql-cast-value (copy-subquery-value (aref row 0))
                                                           source-type result-type -1))
                              (return-from first-row nil))))))))))
    (unwind-protect
         (progn
           (note-backend-command-id-used)
           (funcall executor nil statement)
           result)
      (when *dml-returning-spool* (close-query-row-spool *dml-returning-spool*)))))

(defun execute-sql-routine-statement (routine statement result-type)
  (call-with-sql-routine-statement
   routine
   (lambda ()
     (let ((executor (scalar-routine-dml-executor statement)))
       (cond
         (executor
          (unless (eq (pg-routine-volatility routine) :volatile)
            (ereport "0A000" "~a is not allowed in a non-volatile function" (getf statement :type)))
          (execute-scalar-routine-dml executor statement result-type))
         (result-type (evaluate-scalar-routine-select statement result-type))
         (t (exec-perform statement)))))))

(defun evaluate-scalar-sql-routine (routine arguments)
  "Execute a scalar SQL function in the caller's statement and transaction."
  (when (and (pg-routine-strict-p routine) (find +null+ arguments))
    (return-from evaluate-scalar-sql-routine +null+))
  (when (>= *sql-routine-call-depth* 64)
    (ereport "54001" "SQL routine stack depth limit exceeded"))
  (detach-select-index-scans)
  (let* ((*plpgsql-execution-p* nil)
         (*select-index-scans* nil)
         (*select-index-lease-domain* nil)
         (*select-index-lease-groups* nil)
         (*sql-routine-call-depth* (1+ *sql-routine-call-depth*))
         (*sql-routine-parse-context* routine)
         (*query-parameter-types* (coerce (pg-routine-argument-types routine) 'vector))
         (*query-parameter-values* (coerce arguments 'vector))
         (*subquery-execution-active-p* nil)
         (statements (parse-scalar-sql-routine-statements (pg-routine-source routine))))
    (loop for (statement . rest) on statements
          for result-type = (unless rest (pg-routine-result-type routine))
          for value = (execute-sql-routine-statement routine statement result-type)
          unless rest return value)))

(defun evaluate-stored-scalar-routine (routine arguments)
  (ecase (pg-routine-language routine)
    (:sql (evaluate-scalar-sql-routine routine arguments))
    (:plpgsql
     (when (and (pg-routine-strict-p routine) (find +null+ arguments))
       (return-from evaluate-stored-scalar-routine +null+))
     (when (>= *sql-routine-call-depth* 64)
       (ereport "54001" "SQL routine stack depth limit exceeded"))
     (detach-select-index-scans)
     (let ((*select-index-scans* nil)
           (*select-index-lease-domain* nil)
           (*select-index-lease-groups* nil)
           (*sql-routine-call-depth* (1+ *sql-routine-call-depth*)))
       (run-plpgsql-block nil (compile-do-source (pg-routine-source routine)) routine arguments)))))

(defun require-routine-set-select (statement)
  (unless (eq (getf statement :type) :select)
    (ereport "0A000" "set-returning SQL routines require a final SELECT")))

(defun routine-set-source-types (targets routine)
  (let ((fields (pg-routine-result-fields routine)))
    (if (rest fields)
        (progn
          (unless (and (= (length targets) (length fields))
                       (every (lambda (target field)
                                (sql-assignment-coercion-p (getf target :type) (second field)))
                              targets fields))
            (ereport "42P13" "return type mismatch in function declared to return a table"))
          (mapcar (lambda (target) (getf target :type)) targets))
        (list (scalar-routine-source-type targets (pg-routine-result-type routine))))))

(defun coerce-routine-set-row (row source-types routine)
  (let ((fields (pg-routine-result-fields routine)))
    (if (rest fields)
        (make-sql-record :fields fields
                         :values (map 'vector
                                      (lambda (value source field)
                                        (sql-cast-value (copy-subquery-value value) source (second field) -1))
                                      row source-types fields))
        (sql-cast-value (copy-subquery-value (aref row 0))
                        (first source-types) (pg-routine-result-type routine) -1))))

(defun prepare-routine-set-query (owner routine statements)
  "Run preceding statements, then register the final SELECT for resumable pulls."
  (call-with-query-execution-owner
   owner
   (lambda ()
     (loop for (statement . rest) on statements while rest
           do (let ((*subquery-execution-active-p* nil))
                (execute-sql-routine-statement routine statement nil)))
     (when (eq (pg-routine-volatility routine) :volatile)
       (let ((outer-snapshot (backend-snapshot *backend*))
             (*statement-snapshot-override* nil))
         (unwind-protect
              (query-execution-owner-acquire-snapshot
               owner (lambda (registration) (statement-start registration t)))
           (setf (backend-snapshot *backend*) outer-snapshot))))
     (let* ((plan (plan-select-query (car (last statements))))
            (targets (getf (query-plan-prepared plan) :targets))
            (source-types (routine-set-source-types targets routine)))
       (call-with-admitted-select-plan
        plan (lambda (relations)
               (setf (query-execution-owner-binding owner '*subquery-registered-relations*) relations)
               (retain-query-plan-executor-tables plan relations)))
       (values plan source-types)))))

(defun pull-routine-set-row (owner plan iterator source-types routine)
  (detach-select-index-scans)
  (call-with-query-execution-owner
   owner
   (lambda ()
     (let ((relations *subquery-registered-relations*))
       (call-with-select-index-lease-domain
        (query-plan-index-relfilenodes plan relations)
        (lambda ()
          (let ((*iterator-index-scan-domain-p* t)
                (suspend-p nil))
            (unwind-protect
                 (progn
                   (unless (car iterator)
                     (setf (car iterator) (make-resumable-query-plan-iterator plan relations)))
                   (multiple-value-bind (row present) (sql-iterator-next (car iterator))
                     (when present
                       (let ((value (coerce-routine-set-row (car row) source-types routine)))
                         (detach-select-index-scans)
                         (setf suspend-p t)
                         (values value t)))))
              (unless suspend-p
                ;; An active borrowed cursor must close before the lease group unwinds.
                (sb-sys:without-interrupts
                  (unwind-protect
                       (sb-sys:with-local-interrupts (close-query-execution-owner owner))
                    (setf *select-index-scans*
                          (delete-if #'select-index-scan-borrowed-lease-p
                                     *select-index-scans*)))))))))))))

(defun make-stored-routine-set-iterator (routine arguments)
  "Own the body snapshot, plan and iterators across suspended outer pulls."
  (when (and (pg-routine-strict-p routine) (find +null+ arguments))
    (return-from make-stored-routine-set-iterator
      (make-sql-iterator (lambda () (values nil nil)))))
  (when (>= *sql-routine-call-depth* 64)
    (ereport "54001" "SQL routine stack depth limit exceeded"))
  (let* ((statements (parse-scalar-sql-routine-statements (pg-routine-source routine)))
         (depth (1+ *sql-routine-call-depth*)))
    (require-routine-set-select (car (last statements)))
    (detach-select-index-scans)
    (let ((owner nil) (complete nil))
      (unwind-protect
           (progn
             (setf owner
                   (make-query-execution-owner
                    :bindings
                    (list (cons '*query-parameter-types* (coerce (pg-routine-argument-types routine) 'vector))
                          (cons '*query-parameter-values* (coerce arguments 'vector))
                          (cons '*datum-origin-capture* nil))))
             (unless (eq (pg-routine-volatility routine) :volatile)
               (query-execution-owner-pin-snapshot owner *current-snapshot*))
             (let ((*plpgsql-execution-p* nil)
                   (*sql-routine-parse-context* routine)
                   (*sql-routine-call-depth* depth))
               (multiple-value-bind (plan source-types) (prepare-routine-set-query owner routine statements)
                 (let ((iterator (list nil)))
                   (prog1
                       (make-sql-iterator
                        (lambda ()
                          (let ((*plpgsql-execution-p* nil)
                                (*sql-routine-parse-context* routine)
                                (*sql-routine-call-depth* depth))
                            (pull-routine-set-row owner plan iterator source-types routine)))
                        (lambda ()
                          ;; A suspended body has detached its own scans. The
                          ;; caller can hold unrelated leases when it closes it.
                          (let ((*btree-read-leases* nil))
                            (close-query-execution-owner owner))))
                     (setf complete t))))))
        (when (and owner (not complete)) (close-query-execution-owner owner))))))

(defun admit-staged-routine-ddl (transaction)
  (admit-staged-catalog-change transaction)
  (when (or *applying-transaction-ddl* (catalog-transaction-capture-ddl-p transaction))
    (ereport "0A000" "routine DDL is unsupported during logical capture")))

(defun validate-scalar-sql-routine-body (routine statements)
  "Analyze the body without running it, including a recursive reference."
  (when (pg-routine-returns-set-p routine)
    (require-routine-set-select (car (last statements))))
  (let ((*validating-sql-routine* routine)
        (*sql-routine-parse-context* routine)
        (*query-parameter-types* (coerce (pg-routine-argument-types routine) 'vector)))
    (loop for (statement . rest) on statements
          for select-p = (eq (getf statement :type) :select)
          for analyzed = (if select-p (prepare-select statement)
                             (analyze-dml-subqueries statement))
          unless rest
            do (let ((targets (getf analyzed (if select-p :targets :returning))))
                 (if (pg-routine-returns-set-p routine)
                     (routine-set-source-types targets routine)
                     (scalar-routine-source-type targets (pg-routine-result-type routine))))))
  routine)

(defun exec-stage-create-function (stream ast)
  (let ((transaction (begin-catalog-ddl)))
    (multiple-value-bind (routine statements)
        (with-write-catalog-locks
          (admit-staged-routine-ddl transaction)
          (prepare-scalar-sql-routine-ddl
           transaction ast (not (equal "off" (session-extra-setting-value "check_function_bodies")))))
      (when statements
        (ecase (pg-routine-language routine)
          (:sql (validate-scalar-sql-routine-body routine statements))
          (:plpgsql (validate-plpgsql-block statements routine))))
      (with-write-catalog-locks
        (catalog-transaction-stage transaction :routine (pg-routine-signature routine) routine))))
  (send-command-complete stream "CREATE FUNCTION"))

(defun routine-table-dependencies (transaction routines)
  "Return affected live table descriptors and their bound defaults and checks."
  (let ((oids (mapcar #'pg-routine-oid routines))
        (dependencies nil))
    (when oids
      (catalog-map-visible-relations
       (lambda (name table)
         (declare (ignore name))
         (let ((defaults (loop for column across (pg-class-columns table)
                               when (and (not (pg-column-dropped-p column))
                                         (catalog-expression-uses-routine-p
                                          (pg-column-default-expression column) oids))
                                 collect column))
               (checks (remove-if-not
                        (lambda (check)
                          (catalog-expression-uses-routine-p (getf check :expression) oids))
                        (pg-class-checks table))))
           (when (or defaults checks) (push (list table defaults checks) dependencies))))
       (catalog-transaction-database transaction) :table))
    dependencies))

(defun stage-routine-dependent-expression-drop (transaction dependency)
  "Remove the cooked expressions while preserving the table and missing datums."
  (destructuring-bind (table defaults checks) dependency
    (ensure-current-table table)
    (let* ((replacement (copy-pg-class table))
           (columns (copy-seq (pg-class-columns table))))
      (dolist (column defaults)
        (let ((copy (copy-pg-column column)))
          (setf (pg-column-default-expression copy) nil
                (pg-column-default-sequence-name copy) nil
                (pg-column-default-sql copy) nil
                (aref columns (1- (pg-column-attnum column))) copy)))
      (setf (pg-class-columns replacement) columns
            (pg-class-checks replacement)
            (remove-if (lambda (check) (member check checks)) (pg-class-checks table)))
      (catalog-transaction-stage transaction :table (pg-class-name table) replacement))))

(defun exec-stage-drop-function (stream ast)
  (let* ((transaction (begin-catalog-ddl))
         (routines
           (with-write-catalog-locks
             (collect-sql-routines-to-drop
              transaction ast
              (lambda (name)
                (send-notice stream (format nil "function ~s does not exist, skipping" name)
                             "NOTICE" "42883")))))
         (dependencies (with-write-lock (routine-table-dependencies transaction routines))))
    (when dependencies
      (unless (getf ast :cascade)
        (ereport "2BP01" "stored expressions on table ~s depend on the function"
                 (pg-class-name (caar dependencies))))
      (acquire-transaction-relation-locks
       (mapcar (lambda (dependency) (pg-class-relfilenode (first dependency))) dependencies)))
    (when routines
      (with-write-catalog-locks
        (admit-staged-routine-ddl transaction)
        (dolist (dependency dependencies)
          (stage-routine-dependent-expression-drop transaction dependency))
        (dolist (routine routines)
          (catalog-transaction-stage transaction :routine (pg-routine-signature routine) nil)))))
  (send-command-complete stream "DROP FUNCTION"))

(defun exec-stage-create-table (stream ast)
  "Create table metadata and storage without publishing either before COMMIT."
  (let* ((name (getf ast :name))
         (columns (getf ast :columns))
         (constraints (getf ast :constraints))
         (foreign-keys (remove-if-not (lambda (constraint) (eq :foreign-key (getf constraint :kind))) constraints))
         (transaction (begin-catalog-ddl))
         (class nil)
         (sequence-dependencies nil))
    (with-write-catalog-locks
      (reject-logical-transaction-ddl-private-table name)
      (unless (staged-relation-name-in-use-p transaction name)
        (admit-staged-catalog-change transaction)
        (when (and foreign-keys (or *applying-transaction-ddl* (catalog-transaction-capture-ddl-p transaction)))
          (ereport "0A000" "foreign key DDL is unsupported during logical capture"))
        (let ((names (mapcar (lambda (column) (getf column :name)) columns)))
          (unless (= (length names) (length (remove-duplicates names :test #'string=)))
            (ereport "42701" "column specified more than once")))
        (setf class (make-table-definition (catalog-transaction-database transaction) name columns))
        (install-table-expressions class columns constraints)
        (setf sequence-dependencies
              (nth-value 1 (catalog-transaction-stage transaction :table name class)))))
    (cond (class
           (catalog-transaction-create-storage (pg-class-relfilenode class) :heap)
           (create-table-toast-storage class t)
           (when (or (some (lambda (column) (or (getf column :primary) (getf column :unique))) columns)
                     (some (lambda (constraint) (not (eq :check (getf constraint :kind)))) constraints))
             (stage-new-table-constraint-indexes transaction class columns constraints))
           (when foreign-keys
             (stage-new-table-foreign-keys transaction class foreign-keys))
           (when *applying-transaction-ddl*
             (retain-logical-table-sequence-dependencies transaction name sequence-dependencies))
           (capture-staged-transaction-ddl (transaction)
             (unless *applying-transaction-ddl*
               (retain-logical-table-sequence-dependencies transaction name sequence-dependencies))
             (with-write-lock (first (parse-statements (catalog-table-definition-sql class))))))
          ((getf ast :if-not-exists)
           (send-notice stream (format nil "relation ~s already exists, skipping" name) "NOTICE" "42P07"))
          (t (ereport "42P07" "relation ~s already exists" name)))
    (send-command-complete stream "CREATE TABLE")))

(defun make-added-column (definition attnum)
  "Cook the insert default and freeze its assignment value for older tuples."
  (let ((column (make-pg-column :name (getf definition :name)
                                :type (getf definition :type)
                                :typmod (getf definition :typmod)
                                :attnum attnum))
        (raw-default (getf definition :default-expression)))
    (when raw-default
      (let* ((cooked (cook-column-default raw-default column))
             (folded (fold-column-default cooked)))
        (unless (member (first folded) '(:const :bool :scalar-array-const))
          (ereport "0A000" "ADD COLUMN requires a default that folds to an immutable constant"))
        (setf (pg-column-default-expression column) cooked
              (pg-column-default-sql column)
              (canonical-stored-expression cooked (getf definition :default-sql) raw-default nil)
              (pg-column-missing-value column) (second folded))))
    column))

(defun admit-staged-column-evolution (transaction)
  (admit-staged-catalog-change transaction)
  (when (catalog-transaction-capture-ddl-p transaction)
    (ereport "0A000" "column evolution cannot be applied by the current logical replication protocol")))

(defun prepare-staged-column-addition (transaction table ast)
  "Build a detached descriptor whose existing column slots keep their identities."
  (let* ((definition (getf ast :column-definition))
         (name (getf definition :name))
         (columns (pg-class-columns table)))
    (when (find name columns :key #'pg-column-name :test #'string=)
      (if (getf ast :column-if-not-exists)
          (return-from prepare-staged-column-addition nil)
          (ereport "42701" "column ~s already exists" name)))
    (when (or (getf ast :column-keys) (getf definition :checks)
              (getf definition :notnull))
      (ereport "0A000" "ADD COLUMN currently requires a nullable column without constraints"))
    (admit-staged-column-evolution transaction)
    (let ((replacement (copy-pg-class table)))
      (setf (pg-class-columns replacement)
            (concatenate 'vector columns
                         (vector (make-added-column definition (1+ (length columns))))))
      replacement)))

(defun stored-expression-references-attnum-p (expression attnum)
  "Find a cooked column reference for this table's physical attribute number."
  (and (consp expression)
       (or (and (eq (first expression) :col) (eql (second expression) attnum))
           (stored-expression-references-attnum-p (car expression) attnum)
           (stored-expression-references-attnum-p (cdr expression) attnum))))

(defmacro with-copied-column-descriptor ((replacement column) table original &body body)
  "Run BODY with one detached column installed in its original physical slot."
  (let ((columns (gensym "COLUMNS-")))
    `(let* ((,replacement (copy-pg-class ,table))
            (,columns (copy-seq (pg-class-columns ,replacement)))
            (,column (copy-pg-column ,original)))
       (setf (aref ,columns (1- (pg-column-attnum ,column))) ,column
             (pg-class-columns ,replacement) ,columns)
       ,@body)))

(defun prepare-staged-column-rename (transaction table ast)
  "Rename the catalog slot and refresh any CHECK definition that names it."
  (let ((original (resolve-column-or-error table (getf ast :column)))
        (new-name (getf ast :new-name)))
    (when (find-column table new-name)
      (ereport "42701" "column ~s already exists" new-name))
    (when (catalog-explicit-publication-table-p (catalog-transaction-database transaction) table)
      (ereport "0A000" "RENAME COLUMN cannot yet update explicit publication metadata"))
    (admit-staged-column-evolution transaction)
    (with-copied-column-descriptor (replacement column) table original
      (setf (pg-column-name column) new-name
            (pg-class-checks replacement)
            (loop for check in (pg-class-checks table)
                  for expression = (getf check :expression)
                  collect (if (stored-expression-references-attnum-p expression (pg-column-attnum column))
                              (let ((copy (copy-list check)))
                                (setf (getf copy :sql)
                                      (catalog-canonical-expression-sql expression replacement))
                                copy)
                              check)))
      replacement)))

(defun prepare-staged-column-not-null (transaction table ast)
  "Return a replacement descriptor and the attribute that needs validation."
  (let* ((name (getf ast :column))
         (original (resolve-column-or-error table name))
         (required-p (eq (getf ast :action) :set-not-null)))
    (let ((attnum (pg-column-attnum original)))
      (when (eql required-p (pg-column-notnull original))
        (return-from prepare-staged-column-not-null nil))
      (when (and (not required-p)
                 (some (lambda (index)
                         (and (pg-index-primary-p index)
                              (member attnum (pg-index-columns index))))
                       (table-indexes table)))
        (ereport "42P16" "column ~s is in a primary key" name))
      (admit-staged-column-evolution transaction)
      (with-copied-column-descriptor (replacement column) table original
        (setf (pg-column-notnull column) required-p)
        (values replacement (and required-p attnum))))))

(defun reject-drop-column-publication-dependency (transaction table column cascade-p)
  "Reject referenced publication filters until they can join catalog staging."
  (let ((database (catalog-transaction-database transaction)))
    (loop for publication being the hash-values of (catalog-transaction-publications transaction database)
          do (dolist (entry (pg-publication-tables publication))
               (let ((filter (getf entry :filter)))
                 (when (and filter (equal (getf entry :name) (pg-class-name table))
                            (stored-expression-references-attnum-p
                             (analyze-expr filter table) (pg-column-attnum column)))
                   (if cascade-p
                       (catalog-transaction-remove-publication-member
                        transaction publication (pg-class-name table))
                       (ereport "2BP01" "publication ~s depends on column ~s"
                                (pg-publication-name publication) (pg-column-name column)))))))))

(defun prepare-staged-column-drop (transaction table ast)
  "Keep the physical layout and retire the dropped attribute's owned dependencies."
  (let* ((name (getf ast :column))
         (original (find-column table name)))
    (unless original
      (if (getf ast :column-if-exists)
          (return-from prepare-staged-column-drop nil)
          (ereport "42703" "column ~s does not exist" name)))
    (admit-staged-column-evolution transaction)
    (when (logical-capture-xid-needed-p (backend-xid *backend*))
      (ereport "0A000" "DROP COLUMN requires a logical protocol that can project dropped attributes"))
    (reject-drop-column-publication-dependency transaction table original (getf ast :cascade))
    (let ((attnum (pg-column-attnum original)))
      (stage-dependent-view-drops transaction (list table) (getf ast :cascade) attnum)
      (dolist (index (table-indexes table))
        (when (member attnum (or (pg-index-referenced-columns index) (pg-index-columns index)))
          (loop for (child . constraint) in (referencing-foreign-keys-locked table)
                when (and (eql (pg-index-oid index) (pg-foreign-key-referenced-index-oid constraint))
                          (not (and (eql (pg-class-oid child) (pg-class-oid table))
                                    (member attnum (pg-foreign-key-columns constraint)))))
                  do (ereport "2BP01" "foreign key ~s on table ~s depends on column ~s"
                              (pg-foreign-key-name constraint) (pg-class-name child) name))
          (stage-dependent-view-drops transaction (list index) (getf ast :cascade))
          (catalog-transaction-stage transaction :index (pg-index-name index) nil)))
      (with-copied-column-descriptor (replacement column) table original
        (setf (pg-column-name column) (format nil "........pg.dropped.~d........" attnum)
              (pg-column-dropped-p column) t
              (pg-column-notnull column) nil
              (pg-column-default-expression column) nil
              (pg-column-default-sql column) nil
              (pg-column-default-sequence-name column) nil
              (pg-column-missing-value column) +null+
              (pg-class-statistics replacement) nil
              (pg-class-foreign-keys replacement)
              (remove-if (lambda (constraint) (member attnum (pg-foreign-key-columns constraint)))
                         (pg-class-foreign-keys table))
              (pg-class-checks replacement)
              (remove-if (lambda (check)
                           (stored-expression-references-attnum-p (getf check :expression) attnum))
                         (pg-class-checks table)))
        replacement))))

(defun column-evolution-action-pass (action)
  "Use PostgreSQL's ALTER TABLE catalog pass order for column actions."
  (case (getf action :action)
    ((:drop-column :drop-not-null :drop-default) 0)
    (:add-column 1)
    (:set-not-null 2)
    (:set-default 3)
    (otherwise
     (ereport "0A000" "unsupported action in a multi-action column ALTER TABLE"))))

(defun prepare-staged-column-default (transaction table ast)
  "Change the insert default without changing the missing datum for existing tuples."
  (let ((original (resolve-column-or-error table (getf ast :column))))
    (admit-staged-column-evolution transaction)
    (with-copied-column-descriptor (replacement column) table original
      (let* ((raw (getf ast :expression))
             (cooked (when raw (cook-column-default raw column))))
        (setf (pg-column-default-expression column) cooked
              (pg-column-default-sequence-name column) nil
              (pg-column-default-sql column)
              (when cooked (canonical-stored-expression cooked (getf ast :sql) raw nil))))
      replacement)))

(defun prepare-staged-column-actions (transaction table actions)
  "Apply catalog passes to a detached descriptor and collect final scan requirements."
  (let ((descriptor table)
        (validate nil)
        (skipped nil))
    (dolist (ast actions)
      (multiple-value-bind (replacement attnum)
          (case (getf ast :action)
            (:add-column (prepare-staged-column-addition transaction descriptor ast))
            (:drop-column (prepare-staged-column-drop transaction descriptor ast))
            (:rename-column (prepare-staged-column-rename transaction descriptor ast))
            ((:set-not-null :drop-not-null)
             (prepare-staged-column-not-null transaction descriptor ast))
            ((:set-default :drop-default)
             (prepare-staged-column-default transaction descriptor ast)))
        (when attnum (pushnew attnum validate))
        (cond (replacement (setf descriptor replacement))
              ((eq :add-column (getf ast :action))
               (push (getf (getf ast :column-definition) :name) skipped)))))
    (values descriptor (nreverse validate) (nreverse skipped))))

(defun prepare-column-type-rewrite (transaction source ast)
  "Reserve a descriptor and index definitions for the supported conversion matrix."
  (let* ((name (getf ast :column))
         (old (resolve-column-or-error source name))
         (target-type (getf ast :target-type))
         (attnum (pg-column-attnum old))
         (using (getf ast :using)))
    (unless (or (and (eq (pg-column-type old) :int4) (eq target-type :int8))
                (and (eq (pg-column-type old) :text) (eq target-type :int4) using))
      (ereport "0A000" "column type conversion is outside the supported matrix"))
    (when (or (pg-column-default-expression old) (pg-column-default-sequence-name old)
              (some (lambda (check)
                      (stored-expression-references-attnum-p (getf check :expression) attnum))
                    (pg-class-checks source)))
      (ereport "0A000" "ALTER TYPE cannot yet convert a dependent default or CHECK"))
    (reject-drop-column-publication-dependency transaction source old nil)
    (admit-staged-catalog-change transaction)
    (let* ((target (copy-pg-class source))
           (columns (map 'vector #'copy-pg-column (pg-class-columns source)))
           (column (aref columns (1- attnum)))
           (indexes (loop for index in (table-indexes source)
                          collect (let ((copy (copy-pg-index index)))
                                    (when (and (index-has-expressions-p index)
                                               (member attnum (pg-index-referenced-columns index)))
                                      (ereport "0A000" "ALTER TYPE cannot yet reanalyze a dependent index expression"))
                                    (setf (pg-index-relfilenode copy) (allocate-oid))
                                    copy))))
      (setf (pg-class-relfilenode target) (allocate-oid)
            (pg-class-toast target) nil
            (pg-class-statistics target) nil
            (pg-class-columns target) columns
            (pg-column-type column) target-type
            (pg-column-typmod column) (getf ast :target-typmod)
            (pg-column-storage column) nil)
      (loop for copy across columns do (setf (pg-column-missing-value copy) +null+))
      (let ((raw (or using (list :column name))))
        (validate-stored-expression raw :check source)
        (let ((expression (analyze-expr raw source)))
          (validate-index-expression
           expression "ALTER TYPE USING" (constantly nil)
           :replay-sql-p (or *applying-transaction-ddl*
                             (catalog-transaction-capture-ddl-p transaction)))
          (values target indexes attnum (analyze-subquery-assignment expression column)))))))

(defun copy-column-type-rewrite-rows (source target attnum expression)
  "Convert visible rows into the transaction's replacement heap and TOAST."
  (let* ((columns (pg-class-columns target))
         (toast (pg-class-toast target))
         (xid (backend-xid *backend*))
         (cid (backend-command-id *backend*))
         (protection (statement-protection-snapshot)))
    (note-backend-command-id-used)
    (do-table-tuples (tuple source)
      (let ((values (htup-values (materialize-select-tuple source tuple protection))))
        (setf (aref values (1- attnum)) (eval-subquery-assignment expression values))
        (check-not-null values columns)
        (check-table-constraints target values)
        (with-btree-relation-locks ((when toast (list (pg-toast-index-relfilenode toast))))
          (with-write-lock
            (heapam-insert
             (pg-class-relfilenode target) columns
             (toast-externalize-tuple-unlocked toast values columns xid cid) xid cid)))))))

(defun exec-stage-column-type-rewrite (stream ast)
  "Drain users of the old storage and publish the replacement via catalog WAL."
  (let* ((transaction (begin-catalog-ddl))
         (reference (getf ast :table))
         (source (with-write-lock (lookup-sql-user-relation reference))))
    (cond
      (source
       (acquire-transaction-relation-locks
        (with-write-lock
          (append (catalog-relation-storage-nodes source)
                  (mapcar #'pg-index-relfilenode (table-indexes source)))))
       (let ((attnum (pg-column-attnum (resolve-column-or-error source (getf ast :column)))))
         (when (or (some (lambda (constraint) (member attnum (pg-foreign-key-columns constraint)))
                         (pg-class-foreign-keys source))
                   (some (lambda (reference) (member attnum (pg-foreign-key-referenced-columns (cdr reference))))
                         (referencing-foreign-keys source)))
           (ereport "0A000" "ALTER TYPE cannot yet rewrite a foreign key column")))
       (multiple-value-bind (target indexes attnum expression)
           (with-write-catalog-locks
             (ensure-current-table source)
             (reject-logical-transaction-ddl-private-table (pg-class-name source))
             (check-no-pending-foreign-key-events source)
             (check-truncate-table-not-in-use source)
             (stage-dependent-view-drops
              transaction (list source) nil
              (pg-column-attnum (resolve-column-or-error source (getf ast :column))))
             (prepare-column-type-rewrite transaction source ast))
         (catalog-transaction-create-storage (pg-class-relfilenode target) :heap)
         (create-table-toast-storage target t)
         ;; Every replacement heap and TOAST tuple carries this transaction's XID.
         (let ((xid (backend-xid *backend*)))
           (setf (pg-class-relfrozenxid target) xid)
           (when (pg-class-toast target)
             (setf (pg-toast-relfrozenxid (pg-class-toast target)) xid)))
         (let ((*current-snapshot* (refresh-statement-snapshot)))
           (copy-column-type-rewrite-rows source target attnum expression)
           ;; Index validation must see the tuples written by this same command.
           (let ((*current-snapshot* (copy-snapshot *current-snapshot*)))
             (incf (snapshot-curcid *current-snapshot*))
             (dolist (index indexes)
               (catalog-transaction-create-storage (pg-index-relfilenode index) :btree)
               (let ((valid-p (pg-index-valid-p index)))
                 (build-index-entries index target :unique (and valid-p (pg-index-unique-p index))
                                                   :name (pg-index-name index))
                 (setf (pg-index-valid-p index) valid-p)))))
         (with-write-catalog-locks
           (ensure-current-table source)
           (catalog-transaction-stage transaction :table (pg-class-name source) target)
           (dolist (index indexes)
             (catalog-transaction-stage transaction :index (pg-index-name index) index)))
         (capture-staged-transaction-ddl (transaction)
           (list :type :alter-table :table (pg-class-name source) :action :alter-column-type
                 :column (getf ast :column) :target-type (getf ast :target-type)
                 :target-typmod (getf ast :target-typmod) :using (getf ast :using)))))
      ((getf ast :if-exists)
       (send-notice stream (format nil "relation ~s does not exist, skipping" reference)))
      (t (ereport "42P01" "relation ~s does not exist" reference))))
  (send-command-complete stream "ALTER TABLE"))

(defun exec-stage-column-evolution (stream ast)
  "Retain relation admission through validation and descriptor publication."
  (when (eq (getf ast :action) :alter-column-type)
    (return-from exec-stage-column-evolution (exec-stage-column-type-rewrite stream ast)))
  (let* ((reference (getf ast :table))
         (actions (if (getf ast :actions)
                      (stable-sort (copy-list (getf ast :actions)) #'<
                                   :key #'column-evolution-action-pass)
                      (list ast)))
         (transaction (begin-catalog-ddl))
         (table (with-write-lock (lookup-sql-user-relation reference))))
    (cond
      (table
       (acquire-transaction-relation-locks
        (with-write-lock
          (cons (pg-class-relfilenode table)
                (when (find :drop-column actions :key (lambda (item) (getf item :action)))
                  (mapcar #'pg-index-relfilenode (table-indexes table))))))
       (multiple-value-bind (descriptor validate skipped)
           (with-write-catalog-locks
             (ensure-current-table table)
             (check-truncate-table-not-in-use table)
             (prepare-staged-column-actions transaction table actions))
         (unless (eq descriptor table)
           (when validate
             (let ((*current-snapshot* (refresh-statement-snapshot)))
               (check-not-null-existing descriptor validate nil)))
           (when (and (> (length (pg-class-columns descriptor)) (length (pg-class-columns table)))
                      (null (pg-class-toast descriptor)))
             (create-table-toast-storage descriptor t))
           (with-write-catalog-locks
             (ensure-current-table table)
             (catalog-transaction-stage transaction :table (pg-class-name table) descriptor)))
         (dolist (name skipped)
           (send-notice stream (format nil "column ~s already exists, skipping" name)
                        "NOTICE" "42701"))))
      ((getf ast :if-exists)
       (send-notice stream (format nil "relation ~s does not exist, skipping" reference)))
      (t (ereport "42P01" "relation ~s does not exist" reference))))
  (send-command-complete stream "ALTER TABLE"))

(defun exec-stage-drop-table (stream ast)
  "Stage all drops after retaining the heap and dependent storage gates."
  (let* ((transaction (begin-catalog-ddl))
         (database (catalog-transaction-database transaction))
         (targets (with-write-lock
                    (loop for reference in (remove-duplicates (getf ast :names) :test #'equal :from-end t)
                          for table = (lookup-sql-user-relation reference)
                          collect (cons reference table)))))
    (dolist (target targets)
      (unless (or (cdr target) (getf ast :if-exists))
        (ereport "42P01" "table ~s does not exist" (car target))))
    (let ((tables (remove nil (mapcar #'cdr targets))))
      (acquire-transaction-relation-locks
       (with-write-lock
         (loop for table in tables
               append (append (catalog-relation-storage-nodes table)
                              (mapcar #'pg-index-relfilenode (table-indexes table))))))
      (with-write-catalog-locks
        (dolist (table tables)
          (ensure-current-table table)
          (reject-logical-transaction-ddl-private-table (pg-class-name table))
          (check-truncate-table-not-in-use table)

          (loop for (child . constraint) in (referencing-foreign-keys-locked table)
                unless (find (pg-class-oid child) tables :key #'pg-class-oid)
                  do (ereport "2BP01" "foreign key ~s on table ~s depends on table ~s"
                              (pg-foreign-key-name constraint) (pg-class-name child) (pg-class-name table)))
          (when (catalog-explicit-publication-table-p database table)
            (ereport "0A000" "transactional DROP TABLE does not yet support explicit publication membership")))
        (when tables
          (admit-staged-catalog-change transaction)
          (stage-dependent-view-drops
           transaction (append tables (loop for table in tables append (table-indexes table)))
           (getf ast :cascade)))
        (dolist (table tables)
          (catalog-transaction-stage transaction :table (pg-class-name table) nil)))
      (when tables
        (capture-staged-transaction-ddl (transaction)
          (list :type :drop-table :names (mapcar #'pg-class-name tables)))))
    (dolist (target targets)
      (unless (cdr target)
        (send-notice stream (format nil "table ~s does not exist, skipping" (car target))))))
  (send-command-complete stream "DROP TABLE"))

(defun catalog-index-replay-ast (cooked-sql)
  "Build an effective CREATE INDEX from the canonical catalog definition."
  (let ((ast (first (parse-statements cooked-sql))))
    (remf ast :if-not-exists)
    ast))

(defun exec-stage-create-index (stream ast)
  "Build an ordinary index in private storage with retained relation admission."
  (let* ((transaction (begin-catalog-ddl))
         (table (with-write-lock (resolve-table-or-error (getf ast :table))))
         (name (getf ast :name))
         (unique (getf ast :unique))
         (index nil))
    (acquire-transaction-relation-locks (list (pg-class-relfilenode table)))
    (with-write-catalog-locks
      (ensure-current-table table)
      (reject-logical-transaction-ddl-private-table (pg-class-name table))
      (let* ((expression-p (expression-index-definition-p ast))
             (definition (when expression-p (make-analyzed-index-object nil name table ast)))
             (attnums (unless expression-p (resolve-attnums table (getf ast :columns)))))
        (unless expression-p (check-index-definition table attnums))
        (unless (staged-relation-name-in-use-p transaction name)
          (admit-staged-catalog-change transaction)
          (setf index
                (reserve-index-definition-identity
                 (or definition
                     (make-index-object nil name table attnums :unique unique
                                                               :nulls-not-distinct (getf ast :nulls-not-distinct))))))))
    (cond (index
           (let ((*current-snapshot* (refresh-statement-snapshot)))
             (catalog-transaction-create-storage (pg-index-relfilenode index) :btree)
             (build-index-entries index table :unique unique :name name))
           (with-write-catalog-locks
             (catalog-transaction-stage transaction :index name index))
           (capture-staged-transaction-ddl (transaction)
             (catalog-index-replay-ast (catalog-standalone-index-definition-sql index table))))
          ((getf ast :if-not-exists)
           (send-notice stream (format nil "relation ~s already exists, skipping" name) "NOTICE" "42P07"))
          (t (ereport "42P07" "relation ~s already exists" name)))
    (send-command-complete stream "CREATE INDEX")))

(defun exec-stage-drop-index (stream ast)
  "Retain admission while staging a drop of index metadata and storage."
  (let* ((transaction (begin-catalog-ddl))
         (entries (with-write-lock
                    (loop for name in (remove-duplicates (getf ast :names) :test #'equal :from-end t)
                          collect (cons name (lookup-index name))))))
    (dolist (entry entries)
      (unless (or (cdr entry) (getf ast :if-exists))
        (ereport "42704" "index ~s does not exist" (car entry))))
    (acquire-transaction-relation-locks
     (with-write-lock (drop-index-relation-lock-set entries #'find-table-by-oid)))
    (with-write-catalog-locks
      (loop for (name . index) in entries
            when index
              do (refresh-index-ddl-target name index)
                 (when (pg-index-constraint-owned-p index)
                   (ereport "2BP01" "cannot drop index ~s because a table constraint requires it" name))
                 (let ((table (find-table-by-oid (pg-index-table-oid index))))
                   (reject-logical-transaction-ddl-private-table (pg-class-name table))
                   (check-truncate-table-not-in-use table)
                   (loop for (child . constraint) in (referencing-foreign-keys-locked table)
                         when (eql (pg-index-oid index) (pg-foreign-key-referenced-index-oid constraint))
                           do (ereport "2BP01" "foreign key ~s on table ~s depends on index ~s"
                                       (pg-foreign-key-name constraint) (pg-class-name child) name))))
      (let ((indexes (remove nil (mapcar #'cdr entries))))
        (when indexes
          (admit-staged-catalog-change transaction)
          (stage-dependent-view-drops transaction indexes (getf ast :cascade))))
      (loop for (nil . index) in entries
            when index do (catalog-transaction-stage transaction :index (pg-index-name index) nil)))
    (when (some #'cdr entries)
      (capture-staged-transaction-ddl (transaction)
        (list :type :drop-index :names (loop for (nil . index) in entries
                                             when index collect (pg-index-name index)))))
    (loop for (name . index) in entries
          unless index do (send-notice stream (format nil "index ~s does not exist, skipping" name))))
  (send-command-complete stream "DROP INDEX"))

(defun execute-in-transaction-body (stream ast)
  "Execute AST and collect DDL replies through transaction completion.
Replicated DDL gets a short subscriber preflight even if execution returns
without a change; each actual publication captures its own schema precondition."
  (let ((implicit (not (or (backend-explicit *backend*)
                           (backend-implicit-query-block *backend*))))
        (stage-ddl-p (staged-catalog-ddl-p ast))
        (logical-ddl-responses nil)
        (copy-row-count nil)
        (*defining-persistent-expression-p*
          (member (getf ast :type) '(:create-table :alter-table :create-view :create-index)))
        (*concurrent-index-snapshot-safe-p*
          (or (member (getf ast :type) '(:create-index :drop-index :alter-index
                                         :drop-table :alter-table))
              (and (eq :truncate (getf ast :type))
                   (eq :read-committed (backend-isolation-level *backend*))))))
    (when (and (backend-transaction-read-only-p)
               (member (getf ast :type)
                       '(:insert :update :delete :copy-from :truncate :create-table :drop-table
                         :create-view :drop-view :create-index :drop-index :alter-index :alter-table
                         :create-function :drop-function
                         :apply-sequence-state :analyze)))
      (ereport "25006" "cannot execute write statement in a read-only transaction"))
    (when (find :add-foreign-key (getf ast :actions) :key (lambda (action) (getf action :action)))
      (ereport "0A000" "foreign key enforcement is not available"))
    (when (and (catalog-change-p ast) (backend-role-changes *backend*))
      (ereport "0A000" "role changes cannot share a transaction with other catalog changes"))
    (when (and (catalog-change-p ast) (not stage-ddl-p)
               (or (backend-explicit *backend*)
                   (catalog-transaction-has-changes-p (backend-catalog-transaction *backend*))))
      (ereport "0A000"
               "this catalog change is not supported inside the current transaction block"))
    (when (member (getf ast :type) '(:create-table :create-sequence))
      (ensure-sql-creation-schema))
    (with-statement-snapshot (snap #'statement-start)
      (handler-case
          (flet ((execute-body ()
                   (case (getf ast :type)
                     (:select (exec-select stream ast))
                     (:do (exec-do stream ast))
                     (:set-constraints (exec-set-constraints stream ast))
                     ((:create-role :alter-role :grant-role-function) (exec-role-change stream ast))
                     (:copy-to (setf copy-row-count (exec-copy-to stream ast)))
                     (:copy-from
                      (note-backend-command-id-used)
                      (setf copy-row-count (exec-copy-from stream ast)))
                     (:explain (exec-explain stream ast))
                     (:analyze (exec-analyze stream ast))
                     (:insert (note-backend-command-id-used)
                      (let ((*wal-row-page-write-p* t)) (exec-insert stream ast)))
                     (:update (note-backend-command-id-used)
                      (let ((*wal-row-page-write-p* t)) (exec-update stream ast)))
                     (:delete (note-backend-command-id-used)
                      (let ((*wal-row-page-write-p* t)) (exec-delete stream ast)))
                     (:truncate (exec-truncate stream ast))
                     (:create-table (if stage-ddl-p
                                        (exec-stage-create-table stream ast)
                                        (exec-create-table stream ast)))
                     (:drop-table (if stage-ddl-p
                                      (exec-stage-drop-table stream ast)
                                      (exec-drop-table stream ast)))
                     (:create-view (exec-stage-create-view stream ast))
                     (:drop-view (exec-stage-drop-view stream ast))
                     (:create-function (exec-stage-create-function stream ast))
                     (:drop-function (exec-stage-drop-function stream ast))
                     (:create-sequence (exec-create-sequence stream ast))
                     (:alter-sequence (exec-alter-sequence stream ast))
                     (:apply-sequence-state
                      (exec-apply-sequence-state stream ast))
                     (:drop-sequence (exec-drop-sequence stream ast))
                     (:create-publication (exec-create-publication stream ast))
                     (:alter-publication (exec-alter-publication stream ast))
                     (:drop-publication (exec-drop-publication stream ast))
                     (:create-index (if stage-ddl-p
                                        (exec-stage-create-index stream ast)
                                        (exec-create-index stream ast)))
                     (:drop-index (if stage-ddl-p
                                      (exec-stage-drop-index stream ast)
                                      (exec-drop-index stream ast)))
                     (:alter-index (exec-alter-index stream ast))
                     (:alter-table
                      (cond ((foreign-key-add-ddl-p ast) (exec-stage-add-foreign-key stream ast))
                            ((and stage-ddl-p (eq :validate-constraint (getf ast :action)))
                             (exec-stage-validate-foreign-key stream ast))
                            ((column-evolution-ddl-p ast) (exec-stage-column-evolution stream ast))
                            (t (exec-alter-table stream ast))))
                     (t (ereport "0A000" "unsupported statement")))))
            (if (and (ddl-p ast) (not stage-ddl-p))
                (with-schema-write-lock
                  (multiple-value-bind (result responses)
                      (call-with-logical-ddl-response-collection
                       (lambda ()
                         (when *logical-event-schema-contract*
                           (with-logical-ddl-publication (ast) (values)))
                         (execute-body)))
                    (setf logical-ddl-responses responses)
                    result))
                (execute-body)))
        (error (e)
          (if (or (fatal-error-p e) (backend-final-ddl-committed-p *backend*))
              (error e)
              (progn
                (if implicit
                    (progn
                      (abort-transaction)
                      (restore-session-settings stream))
                    (abort-transaction-block))
                (error e))))))
    ;; postgres.c:exec_simple_query commits before EndCommand reports success.
    (when implicit
      (if logical-ddl-responses
          (commit-ddl-and-flush-responses logical-ddl-responses)
          (commit-transaction)))
    (when copy-row-count
      (send-copy-command-complete stream copy-row-count))))

(defun call-with-prepared-final-ddl (function)
  "Protect cleanup after durable final DDL publication on every exit."
  (prepare-backend-final-ddl *backend*)
  (let ((context (backend-wal-obligation *backend*))
        (complete nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (multiple-value-prog1
               (sb-sys:allow-with-interrupts
                 (sb-sys:with-local-interrupts (funcall function)))
             (setf complete t))
        (when (and (not complete) (transaction-wal-obligation-final-ddl-p context))
          (finish-failed-final-ddl *backend*))))))

(defun concurrent-index-ddl-p (ast)
  (and (getf ast :concurrently)
       (member (getf ast :type) '(:create-index :drop-index))))

(defun check-concurrent-index-transaction (ast &optional multi-statement-p)
  (when (or multi-statement-p
            (not (eq (backend-state *backend*) :idle)))
    (ereport "25001" "~a cannot run inside a transaction block"
             (if (eq :create-index (getf ast :type))
                 "CREATE INDEX CONCURRENTLY" "DROP INDEX CONCURRENTLY"))))

(defun execute-concurrent-index (stream ast)
  (check-concurrent-index-transaction ast)
  (when (backend-transaction-read-only-p)
    (ereport "25006" "cannot execute write statement in a read-only transaction"))
  (prepare-xid-wraparound-for-statement *backend*)
  (ecase (getf ast :type)
    (:drop-index (exec-drop-index-concurrently stream ast))
    (:create-index (exec-create-index-concurrently stream ast))))

(defun execute-in-transaction (stream ast)
  "Run a statement in the current transaction or in an ordinary autocommit scope."
  (when (concurrent-index-ddl-p ast)
    (ereport "25001" "concurrent index commands must run as top-level statements"))
  (if (and (ddl-p ast) (not (staged-catalog-ddl-p ast)))
      (call-with-prepared-final-ddl (lambda () (execute-in-transaction-body stream ast)))
      (execute-in-transaction-body stream ast)))

(defun rollback-backend-savepoint (backend checkpoint)
  "Restore the heap, logical stream, locks and portals to CHECKPOINT."
  (handler-case
      (progn
        (when (fboundp 'restore-savepoint-portals)
          (funcall 'restore-savepoint-portals (backend-savepoint-portal-checkpoint checkpoint)))
        (undo-heap-to-savepoint (backend-savepoint-heap-checkpoint checkpoint))
        (restore-backend-savepoint-logical-state backend checkpoint)
        (restore-on-conflict-row-locks
         (backend-xid backend) (backend-savepoint-conflict-row-checkpoint checkpoint))
        (restore-foreign-key-savepoint-state backend (backend-savepoint-foreign-key-state checkpoint))
        (restore-savepoint-relation-access backend (backend-savepoint-relation-access-checkpoint checkpoint))
        (set-backend-state backend :in-progress))
    (error (condition)
      (unless (fatal-error-p condition) (abort-transaction))
      (error condition))))

(defun exec-savepoint-command (stream ast)
  "Manage named savepoints for heap writes and deferred foreign-key state."
  (let* ((backend *backend*)
         (name (getf ast :name))
         (type (getf ast :type))
         (stack (backend-savepoints backend)))
    (unless (backend-explicit backend)
      (ereport "25P01" "savepoint commands require an explicit transaction block"))
    (if (eq type :savepoint)
        (push (capture-backend-savepoint backend name) (backend-savepoints backend))
        (let* ((tail (member name stack :key #'backend-savepoint-name :test #'string=))
               (checkpoint (first tail)))
          (unless checkpoint (ereport "3B001" "savepoint ~s does not exist" name))
          (ecase type
            (:release-savepoint
             (setf (backend-savepoints backend) (rest tail))
             (unless (backend-savepoints backend)
               (setf (backend-heap-undo-log backend) nil)))
            (:rollback-to-savepoint
             (rollback-backend-savepoint backend checkpoint)
             (setf (backend-savepoints backend) tail))))))
  (send-command-complete stream (ecase (getf ast :type)
                                  (:savepoint "SAVEPOINT")
                                  (:release-savepoint "RELEASE")
                                  (:rollback-to-savepoint "ROLLBACK"))))

(defun execute-statement (stream ast)
  (let ((type (getf ast :type)))
    (when (and (eq (backend-state *backend*) :aborted)
               (not (member type '(:commit :rollback :rollback-to-savepoint))))
      (ereport "25P02"
               "current transaction is aborted, commands ignored until end of transaction block"))
    ;; Every command still passes the cluster recovery guard before dispatch.
    ;; A failed transaction exits above without executing any command.
    (recovery-check-statement ast)
    (when (and (backend-savepoints *backend*)
               (not (member type '(:select :explain :insert :update :delete :copy-to :copy-from
                                   :savepoint :release-savepoint :rollback-to-savepoint
                                   :set-constraints :show :begin :commit :rollback))))
      (abort-transaction-block)
      (ereport "0A000" "statement is unsupported inside an active savepoint"))
    (when (concurrent-index-ddl-p ast)
      (return-from execute-statement (execute-concurrent-index stream ast)))
    (case type
      (:begin (command-begin stream ast))
      (:commit (command-commit stream))
      (:rollback (command-rollback stream))
      ((:savepoint :release-savepoint :rollback-to-savepoint) (exec-savepoint-command stream ast))
      (:checkpoint (command-checkpoint stream))
      (:vacuum (exec-vacuum stream ast))
      (:set (exec-set stream ast))
      (:set-transaction-snapshot (exec-set-transaction-snapshot stream ast))
      (:reset (exec-reset stream ast))
      (:show (exec-show stream ast))
      (:apply-logical-event (exec-apply-logical-event stream ast))
      (otherwise (execute-in-transaction stream ast)))))

(defun implicit-query-block-statements-p (statements)
  "Recognize the supported multi-statement subset for an idle implicit query block.
Transaction options and session characteristics are excluded."
  (and (rest statements)
       (every (lambda (statement)
                (and (or (stageable-catalog-ddl-p statement)
                         (member (getf statement :type) '(:select :insert :update :delete :copy-from :truncate :set :set-constraints :reset :show :do
                                                          :create-role :alter-role :grant-role-function)))
                     (not (getf statement :transaction-options-p))
                     (not (getf statement :session-characteristics-p))))
              statements)))

(defvar *extended-simple-multi-p* nil)

(defun begin-extended-implicit-query-block ()
  (sb-sys:without-interrupts
    (clear-backend-command-id-used *backend*)
    (setf (backend-read-only *backend*) (backend-transaction-read-only-p *backend*)
          (backend-settings-transaction-start *backend*) (copy-session-settings (backend-settings *backend*))
          (backend-implicit-query-block *backend*) t
          (backend-command-id *backend*) 0)
    (set-backend-state *backend* :in-progress)))

(defun execute-simple-query-statement (stream ast)
  "Withhold a standalone autocommit tag until execution and commit succeed."
  (if (or *defer-command-complete-p*
          (backend-explicit *backend*)
          (backend-implicit-query-block *backend*))
      (execute-statement stream ast)
      (let ((*deferred-command-completes* nil))
        (let ((*defer-command-complete-p* t))
          (execute-statement stream ast))
        ;; EXECUTE-STATEMENT commits autocommit work before returning. An
        ;; execution or commit error unwinds the private queue without output.
        (dolist (response (nreverse *deferred-command-completes*))
          (apply #'send-command-complete response)))))

(defun execute-query-statements (stream statements implicit-block-p)
  "Execute statements in order, deferring the last command tag in an implicit block.
Advance the command ID after each successful non-transaction statement."
  (loop for tail on statements
        for ast = (first tail)
        do (publish-backend-activity-query *backend* (getf ast :activity-statement-text))
           (when (and *extended-simple-multi-p*
                      (eq (backend-state *backend*) :idle)
                      (not (member (getf ast :type) '(:begin :commit :rollback))))
             (begin-extended-implicit-query-block))
           (arm-statement-timeout)
           (let ((*defer-command-complete-p* (and implicit-block-p (null (rest tail)))))
             (unwind-protect
                  (progn
                    (check-for-interrupts)
                    (let ((*statement-command-start-advance-p* nil))
                      (execute-simple-query-statement stream ast))
                    (when (and (eq (backend-state *backend*) :in-progress)
                               (not (member (getf ast :type) '(:begin :commit :rollback))))
                      (incf (backend-command-id *backend*))))
               (clear-statement-timeout)))))

(defun execute-simple-query-statements (stream statements)
  "Execute an eligible idle simple-query message as one implicit transaction.
Protect setup, commit completion and abort cleanup from interrupts; statement
execution and final network output remain interruptible. The last command tag
is sent only after commit."
  (check-rewind-sql-statements statements)
  (when (rest statements)
    (let ((concurrent (find-if #'concurrent-index-ddl-p statements)))
      (when concurrent (check-concurrent-index-transaction concurrent t))))
  (when *extended-simple-multi-p*
    (return-from execute-simple-query-statements
      (execute-query-statements stream statements t)))
  (if (and (eq (backend-state *backend*) :idle)
           (implicit-query-block-statements-p statements))
      (let ((*deferred-command-completes* nil)
            (complete-p nil))
        (sb-sys:without-interrupts
          (unwind-protect
               (progn
                 (clear-backend-command-id-used *backend*)
                 (setf (backend-settings-transaction-start *backend*)
                       (copy-session-settings (backend-settings *backend*))
                       (backend-implicit-query-block *backend*) t
                       (backend-command-id *backend*) 0)
                 (set-backend-state *backend* :in-progress)
                 (sb-sys:with-local-interrupts
                   (execute-query-statements stream statements t))
                 (commit-transaction)
                 (setf complete-p t)
                 (sb-sys:with-local-interrupts
                   (dolist (response (nreverse *deferred-command-completes*))
                     (apply #'send-command-complete response))))
            (unless complete-p
              (unwind-protect (abort-transaction)
                (restore-session-settings stream))))))
      (execute-query-statements stream statements nil)))

(defun exec-simple-query (stream query &key (start-execution-p t) (finish-execution-p t)
                                         (statements nil statements-p) prepare-only-p)
  (with-backend-query-execution (:start start-execution-p :finish finish-execution-p)
    (let ((completed-p nil) (result nil))
      (unwind-protect
           (setf result
                 (handler-case
                     (progn
                       (unless statements-p (publish-backend-activity-query *backend* query))
                       (arm-statement-timeout)
                       (let ((stmts (if statements-p statements
                                        (parse-statements
                                         query :collect-activity-text-p t
                                               :collect-statement-text-p (when *statement-statistics-store* :defer)))))
                         (check-for-interrupts)
                         (prog1
                             (cond
                               (prepare-only-p stmts)
                               ((null stmts) (send-empty-query-response stream))
                               (t
                                (execute-simple-query-statements stream stmts)))
                           (setf completed-p t))))
                   (pg-error (e)
                     (clear-statement-timeout)
                     (if (fatal-error-p e) (error e)
                         (progn (note-query-error) (send-error-response stream e))))
                   (error (e)
                     (when (fatal-error-p e) (error e))
                     (clear-statement-timeout)
                     (note-query-error)
                     (send-error-response
                      stream (make-condition 'pg-error :sqlstate "XX000"
                                                       :message (princ-to-string e))))))
        ;; A drain-lane parse-only pass shares the first statement's deadline
        ;; with its immediate execution pass. The frontend lifecycle clears it too.
        (unless (and prepare-only-p completed-p) (clear-statement-timeout)))
      (values result completed-p))))
