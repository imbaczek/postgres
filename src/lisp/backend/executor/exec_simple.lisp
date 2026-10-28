;;;; exec_simple.lisp -- simple-query dispatch, DDL, transactions, SET/SHOW.

(in-package #:pg)

(defmacro with-table-ddl ((table &key refresh-snapshot indexes wait-readers)
                          &body body)
  "Lock and revalidate TABLE and INDEXES before running catalog change BODY."
  (let ((table-value (gensym "TABLE-"))
        (index-values (gensym "INDEXES-"))
        (relfilenodes (gensym "RELFILENODES-")))
    `(with-schema-write-lock
       (let* ((,table-value ,table)
              (,index-values ,indexes)
              (,relfilenodes
                (cons (pg-class-relfilenode ,table-value)
                      (mapcar #'pg-index-relfilenode ,index-values))))
         (with-relation-build-locks (,relfilenodes ,wait-readers)
           ,(if refresh-snapshot
                `(let ((*current-snapshot* (refresh-statement-snapshot)))
                   (with-write-catalog-locks
                     (ensure-current-table ,table-value)
                     ,@body))
                `(with-write-catalog-locks
                   (ensure-current-table ,table-value)
                   ,@body)))))))

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

(defun exec-truncate (stream ast)
  ;; Capture one transactional event, including immutable source relation OIDs,
  ;; even for empty tables. Tuple deletion preserves rollback/MVCC semantics.
  (get-current-xid)
  (let ((names nil) (oids nil) (relations nil) (tables nil))
    (dolist (name (remove-duplicates (getf ast :tables) :test #'equal :from-end t))
      (let ((table (resolve-table-or-error name)))
        (with-registered-table (table rel xid :write)
          (push (copy-seq (pg-class-name table)) names)
          (push (pg-class-oid table) oids)
          (push table tables)
          (push (logical-relation-description table) relations)
          (let ((columns (pg-class-columns table)))
            (dolist (tid (collect-matching-tids table nil))
              (check-for-interrupts)
              (multiple-value-bind (deleted tuple block offnum status)
                  (heapam-delete-tid
                   rel columns (car tid) (cdr tid) xid
                   (backend-command-id *backend*))
                (declare (ignore tuple block offnum))
                (check-heapam-write-status status)
                (unless deleted
                  (ereport "40001"
                           "could not truncate relation after concurrent update"))))))))
    (logical-note-transaction-event
     :truncate (list :tables (nreverse names) :relation-oids (nreverse oids)
                     :relations (nreverse relations))
     :publication-selection (with-write-lock (logical-capture-publications (nreverse tables) :truncate))))
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
    (call-with-catalog-durability-rollback
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
         (save-catalog :logical-event (logical-sequence-state-event name)))))
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

(defun exec-apply-logical-event (stream command)
  (unless (and *backend*
               (string= (or (backend-application-name *backend*) "")
                        "pg-lisp-subscribe-apply"))
    (ereport "42501" "APPLY LOGICAL EVENT is restricted to the replication worker"))
  (when (or (backend-explicit *backend*)
            (not (eq (backend-state *backend*) :idle)))
    (ereport "25001" "APPLY LOGICAL EVENT cannot run inside a transaction block"))
  (let ((key (getf command :event-key))
        (lsn (getf command :event-lsn))
        (payload (getf command :payload)))
    (unless (and (stringp key) (<= 1 (length key) 1024)
                 (integerp lsn) (<= 1 lsn #xFFFFFFFFFFFFFFFF)
                 (stringp payload))
      (ereport "08P01" "invalid logical replication event envelope"))
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
    (call-with-catalog-durability-rollback
     (lambda ()
       (when created
         (remhash name (database-sequences db))))
     (lambda ()
       (when (gethash name (database-sequences db))
         (if (getf ast :if-not-exists)
             (progn
               (send-notice stream
                            (format nil "sequence \"~a\" already exists, skipping" name))
               (send-command-complete stream "CREATE SEQUENCE")
               (return-from exec-create-sequence nil))
             (ereport "42P07" "sequence \"~a\" already exists" name)))
       (check-new-relation-name db name)
       (multiple-value-bind (default-min default-max)
           (sequence-default-limits increment)
         (setf minimum (or minimum default-min)
               maximum (or maximum default-max))
         (unless (and (sequence-value-range-p minimum)
                      (sequence-value-range-p maximum)
                      (<= minimum start maximum))
           (ereport "22023" "sequence start value is out of bounds (~a..~a)"
                    minimum maximum))
         (setf (gethash name (database-sequences db))
               (make-pg-sequence :oid (allocate-oid) :name name
                                 :increment increment :start-value start
                                 :min-value minimum :max-value maximum
                                 :last-value start :called-p nil)
               created t))
       (save-catalog :logical-event
                     (logical-catalog-event-for-ast ast))))
    (send-command-complete stream "CREATE SEQUENCE")))

(defun exec-alter-sequence (stream ast)
  (let* ((db (current-database))
         (name (getf ast :name))
         (previous nil))
    (call-with-catalog-durability-rollback
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
         (save-catalog :logical-event
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
    (call-with-catalog-durability-rollback
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
       (save-catalog :logical-event
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
    (call-with-catalog-durability-rollback
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
       (save-catalog :logical-event
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
    (call-with-catalog-durability-rollback
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
           (save-catalog :logical-event (logical-catalog-event-for-ast ast))))))
    (send-command-complete stream "ALTER PUBLICATION")))

(defun exec-drop-publication (stream ast)
  (let ((db (current-database))
        (if-exists (getf ast :if-exists))
        (removed '()))
    (call-with-catalog-durability-rollback
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
       (save-catalog :logical-event
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
          (abort-transaction)))))

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

(defun command-begin (stream ast)
  (when (and (member :read-only ast) (not (getf ast :read-only)))
    (recovery-check-read-write))
  (if (eq (backend-state *backend*) :idle)
      (progn
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
      ((and (string= name "application_name") (getf ast :local-p))
       (if (or (backend-explicit *backend*) (backend-implicit-query-block *backend*))
           (session-set-application-name raw-value t)
           (progn
             (send-notice stream "SET LOCAL can only be used in transaction blocks" "WARNING" "25P01")
             (unless (or (eq raw-value :default) (eq raw-value +null+))
               (session-application-name-value raw-value)))))
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
         (if (and (backend-explicit *backend*)
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
    (unless (and (backend-explicit backend)
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

(defun fsync-index (idx)
  (smgr-fsync (pg-index-relfilenode idx)))

(defun build-index-entries (idx table &key unique name)
  "Populate IDX from every heap version that an active snapshot may need.

UNIQUE checks only tuples visible to the fresh statement snapshot.  The caller
holds the table DDL gate, which has drained other writers and maintenance
before this removal horizon is captured."
  (let* ((heap-relfilenode (pg-class-relfilenode table))
         (index-relfilenode (pg-index-relfilenode idx))
         (columns (pg-class-columns table))
         (types (index-types idx table))
         (snapshot *current-snapshot*)
         (removal-horizon (oldest-xmin)))
    (loop for block below (buf-nblocks heap-relfilenode)
          for page = (buf-read heap-relfilenode block)
          when page
            do (do-live-page-items (page offnum off len)
                 (let ((tuple (heapam-get-tuple heap-relfilenode block offnum
                                                columns)))
                   (when (and tuple
                              (not (tuple-prunable-p (htup-xmin tuple)
                                                     (htup-xmax tuple)
                                                     removal-horizon)))
                     (let* ((vals (index-values idx (htup-values tuple)))
                            (key (encode-index-key vals types)))
                       ;; Retain recently dead versions for old snapshots, but
                       ;; don't let those versions create unique violations.
                       (when (and unique
                                  (htup-visible-p tuple snapshot)
                                  (not (and (key-has-null-p vals)
                                            (not (pg-index-nulls-not-distinct-p
                                                  idx)))))
                         (dolist (tid (btree-search index-relfilenode key))
                           (let ((candidate
                                   (heapam-get-tuple heap-relfilenode
                                                     (car tid) (cdr tid)
                                                     columns)))
                             (when (and candidate
                                        (htup-visible-p candidate snapshot))
                               (ereport "23505"
                                        "could not create unique index \"~a\""
                                        name)))))
                       (btree-insert index-relfilenode key
                                     (htup-block tuple)
                                     (htup-offnum tuple)))))))
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

(defun exec-create-table (stream ast)
  (let* ((name (getf ast :name))
         (db nil)
         (existing nil))
    ;; Catalog hash tables are mutated by DDL under the write lock.  Capture a
    ;; consistent existence check before doing any work outside that lock.
    (with-write-lock
      (setf db (current-database)
            existing (lookup-table name)))
    (when existing
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
        (let* ((*catalog-save-error-after-durable-p* nil)
               (class (create-table* db name cols))
               (durable-generation *catalog-durable-generation*))
          (with-error-cleanup
              (e (unless (or *catalog-save-error-after-durable-p*
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
              ;; CREATE TABLE starts with an empty heap; table-level indexes
              ;; have been built above and inline indexes are empty here.
              (setf (pg-index-valid-p idx) t)
              (fsync-index idx))
            (save-catalog :logical-event
                          (logical-catalog-event-for-ast ast)))))
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
          (save-catalog :logical-event
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

(defun exec-create-index (stream ast)
  (let* ((*catalog-save-error-after-durable-p* nil)
         (name (getf ast :name))
         (unique (getf ast :unique))
         (if-not-exists (getf ast :if-not-exists))
         (db nil)
         (table nil)
         (existing nil)
         (skipped nil))
    (with-write-lock
      (setf db (current-database)
            table (resolve-table-or-error (getf ast :table))
            existing (lookup-index name)))
    (when existing
      (if if-not-exists
          (progn
            (send-notice stream
                         (format nil "relation \"~a\" already exists, skipping" name))
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
          (setf existing (gethash name (database-indexes db)))
          (when existing
            (if if-not-exists
                (setf skipped t)
                (ereport "42P07" "relation \"~a\" already exists" name)))
          (unless skipped
            (check-new-relation-name db name)
            (setf idx
                  (make-index-object
                   (allocate-oid) name table
                   (resolve-attnums table (getf ast :columns))
                   :unique unique
                   :nulls-not-distinct (getf ast :nulls-not-distinct)))
            (save-catalog :applied-logical-event-id nil)))
        (unless skipped
          (handler-case
              (progn
                (btree-init (pg-index-relfilenode idx))
                (build-index-entries idx table :unique unique :name name)
                (fsync-index idx)
                (with-write-catalog-locks
                  (ensure-current-table table)
                  (setf existing (gethash name (database-indexes db)))
                  (when existing
                    (if if-not-exists
                        (setf skipped t)
                        (ereport "42P07" "relation \"~a\" already exists" name)))
                  (let ((durable-generation *catalog-durable-generation*))
                    (handler-case
                        (progn
                          (unless skipped
                            (publish-index* db idx table))
                          (when skipped
                            (wal-defer-relation-delete
                             (pg-index-relfilenode idx)))
                          (save-catalog :logical-event
                                        (logical-catalog-event-for-ast ast)))
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
                                            (pg-class-indexes table)))
                              (setf preserve-relfile t)))
                        (error publish-condition))))))
            (error (condition)
              ;; Before publication, no reader can know this relfilenode, so
              ;; discard its pages together with a catalog save.
              (unless preserve-relfile
                (handler-case
                    (with-write-catalog-locks
                      (wal-defer-relation-delete (pg-index-relfilenode idx))
                      (save-catalog :applied-logical-event-id nil))
                  (error (cleanup-condition)
                    (declare (ignore cleanup-condition)))))
              (error condition))))))
    (when skipped
      (send-notice stream
                   (format nil "relation \"~a\" already exists, skipping" name)))
    (send-command-complete stream "CREATE INDEX")))

(defun exec-drop-index (stream ast)
  (let* ((names (getf ast :names))
         (if-exists (getf ast :if-exists))
         (db nil)
         (entries nil)
         (relfilenodes nil))
    (with-write-lock
      (setf db (current-database)
            entries
            (mapcar (lambda (name)
                      (cons name (gethash name (database-indexes db))))
                    names)
            relfilenodes
            (drop-index-relation-lock-set entries #'find-table-by-oid)))
    (dolist (entry entries)
      (unless (or (cdr entry) if-exists)
        (ereport "42704" "index \"~a\" does not exist" (car entry))))
    (with-relation-build-locks (relfilenodes t)
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
        (with-catalog-mutation-rollback (db)
          (dolist (entry entries)
            (let* ((name (car entry))
                   (original (cdr entry)))
              (if (and original
                       (eq original (gethash name (database-indexes db))))
                  (drop-index* db name)
                  (send-notice stream
                               (format nil "index \"~a\" does not exist, skipping"
                                       name)))))
          (save-catalog :logical-event
                        (logical-catalog-event-for-ast ast)))))
    (send-command-complete stream "DROP INDEX")))

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
    (with-table-ddl (table :indexes (list idx) :wait-readers t)
      (let ((current (lookup-index name)))
        (unless (eq idx current)
          (if current
              (ereport "55000" "index \"~a\" changed while waiting for lock"
                       name)
              (ereport "42704" "index \"~a\" does not exist" name)))
        (with-catalog-mutation-rollback (db)
          (rename-index* db name new-name)
          (save-catalog :logical-event
                        (logical-catalog-event-for-ast ast)))))
    (send-command-complete stream "ALTER INDEX")))

(defun exec-alter-table-using-index (db table ast primary)
  (let* ((index-name (getf ast :using-index))
         (idx (with-write-lock (lookup-index index-name))))
    (unless idx
      (ereport "42704" "index \"~a\" does not exist" index-name))
    (with-table-ddl (table :refresh-snapshot t
                           :indexes (list idx)
                           :wait-readers t)
      ;; Recheck after waiting for both the heap and index readers.
      (let ((current (lookup-index index-name)))
        (unless (eq idx current)
          (if current
              (ereport "55000" "index \"~a\" changed while waiting for lock"
                       index-name)
              (ereport "42704" "index \"~a\" does not exist" index-name))))
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
          (save-catalog :logical-event
                        (logical-catalog-event-for-ast ast)))))))

(defun exec-alter-table-constraint (stream db table ast)
  (let* ((name (getf ast :constraint-name))
         (action (getf ast :action))
         (if-exists (getf ast :constraint-if-exists))
         (original (with-write-lock
                     (gethash name (database-indexes db)))))
    (with-table-ddl (table :indexes (when original (list original))
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
          (save-catalog :logical-event
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
    (with-table-ddl (table :refresh-snapshot t
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
          (save-catalog :logical-event
                        (logical-catalog-event-for-ast ast)))))))

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
         (with-table-ddl (table :refresh-snapshot t)
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
               (save-catalog :logical-event
                             (logical-catalog-event-for-ast ast))))))))
    (send-command-complete stream "ALTER TABLE")))

;;; ---- simple-query execution ----

(defun commit-ddl-and-flush-responses (responses)
  "Commit a DDL command before sending its deferred success and notice replies."
  (commit-transaction)
  (flush-logical-ddl-deferred-responses responses))

(defun execute-in-transaction (stream ast)
  (let ((implicit (not (or (backend-explicit *backend*)
                           (backend-implicit-query-block *backend*))))
        (logical-ddl-responses nil)
        (copy-row-count nil))
    (when (and (backend-transaction-read-only-p)
               (member (getf ast :type)
                       '(:insert :update :delete :truncate :create-table :drop-table
                         :create-index :drop-index :alter-index :alter-table
                         :apply-sequence-state :analyze)))
      (ereport "25006" "cannot execute write statement in a read-only transaction"))
    (when (and (catalog-change-p ast) (backend-explicit *backend*))
      (ereport "0A000"
               "catalog changes are not supported inside an explicit transaction block"))
    (when (member (getf ast :type) '(:create-table :create-sequence))
      (ensure-sql-creation-schema))
    (with-statement-snapshot (snap #'statement-start)
      (handler-case
          (flet ((execute-body ()
                   (case (getf ast :type)
                     (:select (exec-select stream ast))
                     (:copy-to (setf copy-row-count (exec-copy-to stream ast)))
                     (:explain (exec-explain stream ast))
                     (:analyze (exec-analyze stream ast))
                     (:insert (let ((*wal-row-page-write-p* t)) (exec-insert stream ast)))
                     (:update (let ((*wal-row-page-write-p* t)) (exec-update stream ast)))
                     (:delete (let ((*wal-row-page-write-p* t)) (exec-delete stream ast)))
                     (:truncate (exec-truncate stream ast))
                     (:create-table (exec-create-table stream ast))
                     (:drop-table (exec-drop-table stream ast))
                     (:create-sequence (exec-create-sequence stream ast))
                     (:alter-sequence (exec-alter-sequence stream ast))
                     (:apply-sequence-state
                      (exec-apply-sequence-state stream ast))
                     (:drop-sequence (exec-drop-sequence stream ast))
                     (:create-publication (exec-create-publication stream ast))
                     (:alter-publication (exec-alter-publication stream ast))
                     (:drop-publication (exec-drop-publication stream ast))
                     (:create-index (exec-create-index stream ast))
                     (:drop-index (exec-drop-index stream ast))
                     (:alter-index (exec-alter-index stream ast))
                     (:alter-table (exec-alter-table stream ast))
                     (t (ereport "0A000" "unsupported statement")))))
            (if (ddl-p ast)
                (with-schema-write-lock
                  ;; The schema contract is database-wide, while tablecmds.c
                  ;; relation locks permit disjoint DDL to overlap. Serialize
                  ;; before-image capture through WAL publication so event
                  ;; schema-before follows the preceding schema-after.
                  (multiple-value-bind (result responses)
                      (call-with-logical-ddl-event-order
                       (lambda ()
                         (let* ((*logical-source-schema-precondition*
                                  (logical-current-schema-contract))
                                (expected-before
                                  (and *logical-event-schema-contract*
                                       (getf *logical-event-schema-contract*
                                             :schema-before))))
                           (when (and expected-before
                                      (not (equal expected-before
                                                  *logical-source-schema-precondition*)))
                             (error
                              "subscriber schema differs from publisher schema before DDL"))
                           (when *logical-event-schema-contract*
                             (let ((target-name
                                     (logical-private-metadata-ddl-target-name
                                      (current-database) ast)))
                               (when target-name
                                 (error
                                  "replicated DDL cannot target subscriber metadata relation ~s"
                                  target-name))))
                           (execute-body))))
                    (setf logical-ddl-responses responses)
                    result))
                (execute-body)))
        (error (e)
          (if (fatal-error-p e)
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

(defun execute-statement (stream ast)
  (let ((type (getf ast :type)))
    (when (and (eq (backend-state *backend*) :aborted)
               (not (member type '(:commit :rollback))))
      (ereport "25P02"
               "current transaction is aborted, commands ignored until end of transaction block"))
    ;; Every command still passes the cluster recovery guard before dispatch.
    ;; A failed transaction exits above without executing any command.
    (recovery-check-statement ast)
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
                (and (member (getf statement :type) '(:select :insert :update :delete :set :reset :show))
                     (not (getf statement :transaction-options-p))
                     (not (getf statement :session-characteristics-p))))
              statements)))

(defun execute-query-statements (stream statements implicit-block-p)
  "Execute statements in order, deferring the last command tag in an implicit block."
  (loop for tail on statements
        for ast = (first tail)
        do (arm-statement-timeout)
           (let ((*defer-command-complete-p* (and implicit-block-p (null (rest tail)))))
             (unwind-protect
                  (progn (check-for-interrupts) (execute-statement stream ast))
               (clear-statement-timeout)))))

(defun execute-simple-query-statements (stream statements)
  "Execute an eligible idle simple-query message as one implicit transaction.
Protect setup, commit completion and abort cleanup from interrupts; statement
execution and final network output remain interruptible. The last command tag
is sent only after commit."
  (if (and (eq (backend-state *backend*) :idle)
           (implicit-query-block-statements-p statements))
      (let ((*deferred-command-completes* nil)
            (complete-p nil))
        (sb-sys:without-interrupts
          (unwind-protect
               (progn
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
                       (arm-statement-timeout)
                       (let ((stmts (if statements-p statements (parse-statements query))))
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
                     (clear-statement-timeout)
                     (note-query-error)
                     (send-error-response
                      stream (make-condition 'pg-error :sqlstate "XX000"
                                                       :message (princ-to-string e))))))
        ;; A drain-lane parse-only pass shares the first statement's deadline
        ;; with its immediate execution pass. The frontend lifecycle clears it too.
        (unless (and prepare-only-p completed-p) (clear-statement-timeout)))
      (values result completed-p))))
