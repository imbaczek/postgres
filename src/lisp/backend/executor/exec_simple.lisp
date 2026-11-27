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
          '(:create-table :drop-table :create-index :drop-index
            :alter-index :alter-table :create-sequence :alter-sequence
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
                  (drop-table* db name)
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
            (setf (cdr entry) (refresh-index-ddl-target (car entry) (cdr entry) if-exists))))
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
                  (reserved-names (mapcar (lambda (check) (getf check :name))
                                          (pg-class-checks table)))
                  (idx (create-index*
                        db
                        (or (getf ast :constraint-name)
                            (if primary
                                (generate-index-name db name "pkey" reserved-names)
                                (generate-unique-index-name
                                 db name table attnums reserved-names)))
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

(defvar *inline-do-found-p* nil)

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

(defun execute-do-statement (stream ast)
  "Run an inline statement without starting or ending a transaction.
SQL children register a statement snapshot and advance the command ID; cleanup
restores the caller's snapshot. Blocks and NULL need no snapshot of their own."
  (check-for-interrupts)
  (case (getf ast :type)
    (:do-null (values))
    (:do-block
        (dolist (statement (getf ast :statements))
          (execute-do-statement stream statement)))
    (:do-if-found
        (dolist (statement (getf ast (if (eq *inline-do-found-p* (getf ast :negated-p))
                                         :else
                                         :then)))
          (execute-do-statement stream statement)))
    (otherwise
     (let ((saved-snapshot (backend-snapshot *backend*))
           (*statement-snapshot-override* nil))
       (unwind-protect
            (with-statement-snapshot (snapshot
                                      (lambda (registration) (statement-start registration t)))
              (recovery-check-statement
               (if (eq (getf ast :type) :perform) (getf ast :query) ast))
              (case (getf ast :type)
                (:perform (setf *inline-do-found-p* (exec-perform (getf ast :query))))
                ((:create-role :alter-role :grant-role-function) (exec-role-change stream ast))
                (:set (exec-set stream ast))
                (:reset (exec-reset stream ast))
                (t (ereport "0A000" "unsupported inline statement"))))
         (setf (backend-snapshot *backend*) saved-snapshot))))))

(defun exec-do (stream ast)
  "Compile and execute an inline block in the surrounding transaction.
The caller owns commit and abort. Save settings for rollback and retain LOCAL
bindings until transaction end. Discard child command tags and emit one DO
completion after success; anonymous source cannot inherit Bind parameters."
  (let ((block (do-statement-block ast))
        (*executing-do-p* t)
        (*statement-nested-p* t)
        (*inline-do-found-p* nil)
        (*query-parameter-types* nil)
        (*query-parameter-values* nil))
    (session-remember-settings)
    (let ((*defer-command-complete-p* t)
          (*deferred-command-completes* nil))
      (execute-do-statement stream block))
    (send-command-complete stream "DO")))

(defun column-evolution-ddl-p (ast)
  (and (eq :alter-table (getf ast :type))
       (flet ((supported-action-p (action)
                (member (getf action :action) '(:add-column :drop-column :rename-column :set-not-null :drop-not-null :alter-column-type))))
         (or (supported-action-p ast) (some #'supported-action-p (getf ast :actions))))))

(defun stageable-catalog-ddl-p (ast)
  (or (column-evolution-ddl-p ast)
      (and (member (getf ast :type) '(:create-table :drop-table :create-index :drop-index))
           (not (getf ast :concurrently)))))

(defun staged-catalog-ddl-p (ast)
  (and (stageable-catalog-ddl-p ast)
       (or (column-evolution-ddl-p ast) (backend-explicit *backend*) (backend-implicit-query-block *backend*))))

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
         (indexes (remove-if (lambda (constraint) (eq (getf constraint :kind) :check))
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

(defun exec-stage-create-table (stream ast)
  "Create table metadata and storage without publishing either before COMMIT."
  (let* ((name (getf ast :name))
         (columns (getf ast :columns))
         (constraints (getf ast :constraints))
         (transaction (begin-catalog-ddl))
         (class nil)
         (sequence-dependencies nil))
    (with-write-catalog-locks
      (reject-logical-transaction-ddl-private-table name)
      (unless (staged-relation-name-in-use-p transaction name)
        (admit-staged-catalog-change transaction)
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
      (dolist (index (table-indexes table))
        (when (member attnum (or (pg-index-referenced-columns index) (pg-index-columns index)))
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
       (multiple-value-bind (target indexes attnum expression)
           (with-write-catalog-locks
             (ensure-current-table source)
             (reject-logical-transaction-ddl-private-table (pg-class-name source))
             (check-truncate-table-not-in-use source)
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
          (when (catalog-explicit-publication-table-p database table)
            (ereport "0A000" "transactional DROP TABLE does not yet support explicit publication membership")))
        (when tables (admit-staged-catalog-change transaction))
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
                   (check-truncate-table-not-in-use table)))
      (when (some #'cdr entries) (admit-staged-catalog-change transaction))
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
        (*concurrent-index-snapshot-safe-p*
          (or (member (getf ast :type) '(:create-index :drop-index :alter-index
                                         :drop-table :alter-table))
              (and (eq :truncate (getf ast :type))
                   (eq :read-committed (backend-isolation-level *backend*))))))
    (when (and (backend-transaction-read-only-p)
               (member (getf ast :type)
                       '(:insert :update :delete :truncate :create-table :drop-table
                         :create-index :drop-index :alter-index :alter-table
                         :apply-sequence-state :analyze)))
      (ereport "25006" "cannot execute write statement in a read-only transaction"))
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
                     ((:create-role :alter-role :grant-role-function) (exec-role-change stream ast))
                     (:copy-to (setf copy-row-count (exec-copy-to stream ast)))
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
                     (:alter-table (if (column-evolution-ddl-p ast)
                                       (exec-stage-column-evolution stream ast)
                                       (exec-alter-table stream ast)))
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

(defun execute-statement (stream ast)
  (let ((type (getf ast :type)))
    (when (and (eq (backend-state *backend*) :aborted)
               (not (member type '(:commit :rollback))))
      (ereport "25P02"
               "current transaction is aborted, commands ignored until end of transaction block"))
    ;; Every command still passes the cluster recovery guard before dispatch.
    ;; A failed transaction exits above without executing any command.
    (recovery-check-statement ast)
    (when (concurrent-index-ddl-p ast)
      (return-from execute-statement (execute-concurrent-index stream ast)))
    (case type
      (:begin (command-begin stream ast))
      (:commit (command-commit stream))
      (:rollback (command-rollback stream))
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
                         (member (getf statement :type) '(:select :insert :update :delete :truncate :set :reset :show :do
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
