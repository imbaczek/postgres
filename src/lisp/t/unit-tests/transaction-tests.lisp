;;;; transaction-tests.lisp -- transaction ID and session tests.

(in-package #:pg)

(defun test-postgres-special-xid-statuses ()
  ;; Port the status cases from src/test/regress/sql/xid.sql. PostgreSQL's
  ;; TransactionLogFetch treats bootstrap/frozen as committed and invalid as
  ;; aborted. This Lisp check also uses the active-XID set to distinguish
  ;; in-progress XIDs from abandoned zero-status entries.
  (let* ((committed 7021)
         (aborted 7022)
         (active 7023)
         (abandoned 7024)
         (*active-xids* (list active)))
    (clog-set committed +clog-committed+)
    (clog-set aborted +clog-aborted+)
    (clog-set active +clog-in-progress+)
    (check (xid-committed-p committed) "committed XID status is preserved")
    (check (xid-aborted-p aborted) "aborted XID status is preserved")
    (check (xid-in-progress-p active) "active XID status is in progress")
    (check (xid-aborted-p abandoned)
           "inactive zero-status XID is treated as abandoned")
    (check (xid-committed-p +bootstrap-xid+)
           "bootstrap XID is always committed")
    (check (xid-committed-p +frozen-xid+)
           "frozen XID is always committed")
    (check (not (xid-aborted-p +bootstrap-xid+))
           "bootstrap XID is never aborted")
    (check (not (xid-aborted-p +frozen-xid+))
           "frozen XID is never aborted")
    (check (xid-aborted-p +invalid-xid+)
           "invalid XID is treated as aborted")))

(defun test-xid-control-state ()
  (let* ((*pgdata* (merge-pathnames "xid-state/" *pgdata*))
         (control (control-path))
         (temporary (pgdata-path "global/pg_control.tmp"))
         (clog (clog-path)))
    (check (= (load-next-xid) +first-normal-xid+)
           "missing XID control state initializes an empty cluster")
    (ensure-directories-exist clog)
    (with-open-file (stream clog
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (write-byte 0 stream))
    (check (handler-case (progn (load-next-xid) nil) (error () t))
           "missing XID control state is rejected when persistent state exists")
    (ensure-directories-exist control)
    ;; An empty file models a crash that truncated pg_control in place.
    (with-open-file (stream control
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (write-string "" stream))
    (check (handler-case (progn (load-next-xid) nil) (error () t))
           "truncated XID control state is rejected")
    (with-open-file (stream control
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (format stream "not-an-xid~%"))
    (check (handler-case (progn (load-next-xid) nil) (error () t))
           "invalid XID control state is rejected")
    (with-open-file (stream control
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create)
      (format stream "~a~%" (+ +first-normal-xid+ 100)))
    (check (= (load-next-xid) (+ +first-normal-xid+ 100))
           "valid XID control state loads exactly")
    (let ((fsync-count 0)
          (*xid-batch-end* (+ +first-normal-xid+ 200)))
      (save-next-xid
       :fsync-function
       (lambda (fd)
         (incf fsync-count)
         (sb-posix:fsync fd))
       :fsync-directory-function
       (lambda (directory)
         (incf fsync-count)
         (fsync-directory directory)))
      (check (= (load-next-xid) *xid-batch-end*)
             "XID control save installs the new boundary")
      (check (= fsync-count 2)
             "XID control save fsyncs its file and parent directory")
      (check (not (probe-file temporary))
             "successful XID control save removes its temporary file")
      (let ((*xid-batch-end* (+ +first-normal-xid+ 300)))
        (check (handler-case
                   (progn
                     (save-next-xid
                      :rename-function
                      (lambda (from to)
                        (declare (ignore from to))
                        (error "injected XID control rename failure")))
                     nil)
                 (error () t))
               "XID control rename errors are reported")
        (check (= (load-next-xid) (+ +first-normal-xid+ 200))
               "failed XID control replacement preserves prior state")
        (check (not (probe-file temporary))
               "failed XID control save removes its temporary file")))))

(defun test-data-row-first-column (payload)
  (when (and (>= (length payload) 6)
             (plusp (bytes-to-u16 payload 0)))
    (let ((length (bytes-to-u32 payload 2)))
      (unless (= length #xFFFFFFFF)
        (when (<= (+ 6 length) (length payload))
          (bytes->string payload :start 6 :end (+ 6 length)))))))

(defun test-show-timezone-values (path)
  (with-open-file (stream path
                          :direction :input
                          :element-type '(unsigned-byte 8))
    (loop for message = (test-read-frontend-message stream)
          while message
          when (char= (frontend-message-type message) #\D)
            collect (test-data-row-first-column
                     (frontend-message-payload message)))))

(defun test-session-setting-rollback ()
  (let ((*backend* (make-backend))
        (path (merge-pathnames "session-settings.out" *pgdata*)))
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream
                         (concatenate 'string
                                      "SET TIME ZONE 'Europe/Warsaw'; "
                                      "SHOW timezone; "
                                      "BEGIN; "
                                      "SET TIME ZONE 'America/New_York'; "
                                      "SHOW timezone; "
                                      "ROLLBACK; "
                                      "SHOW timezone; "
                                      "RESET timezone; "
                                      "SHOW timezone;")))
    (check (equal (test-show-timezone-values path)
                  '("Europe/Warsaw" "America/New_York" "Europe/Warsaw" "UTC"))
           "SHOW reports SET values and the value restored by ROLLBACK")
    (check (not (nth-value 1 (gethash "timezone"
                                      (backend-settings *backend*))))
           "RESET TIME ZONE returns the backend to its default")
    (check (eq (backend-state *backend*) :idle)
           "ROLLBACK leaves the backend outside a transaction")
    (with-open-file (stream path
                            :direction :output
                            :if-exists :append
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "SET TIME ZONE 'Europe/Warsaw';"))
    (let ((caught nil))
      (handler-case
          (exec-set nil '(:name "timezone" :value "Invalid/NoSuchZone"))
        (pg-error (condition)
          (setf caught condition)))
      (check (and caught
                  (string= (pg-error-sqlstate caught) "22023"))
             "SET TIME ZONE rejects missing zoneinfo names")
      (check (string= (gethash "timezone" (backend-settings *backend*))
                      "Europe/Warsaw")
             "rejected time zone preserves the previous session setting"))))

(defun test-transaction-snapshot-lifecycle ()
  (let* ((*backend* (make-backend))
         (*next-xid* 8400)
         (*active-xids* '(8300))
         (*active-snapshots* 0)
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (*xid-wrap-pending* nil)
         (*xid-wrap-failure* nil)
         (*xid-wrap-completed-through* nil)
         (backend *backend*)
         (first nil)
         (second nil))
    (setf (backend-explicit backend) t
          (backend-state backend) :in-progress
          (backend-isolation-level backend) :repeatable-read)
    (setf first
          (with-statement-snapshot (snapshot #'statement-start)
            snapshot))
    (check (= *active-snapshots* 1)
           "REPEATABLE READ retains a transaction snapshot after the statement")
    (check (and (= (snapshot-xmin first) 8300)
                (= (snapshot-xmax first) 8400)
                (equal (snapshot-xip first) '(8300)))
           "the first REPEATABLE READ statement captures the active XID horizon")
    ;; Model a commit between statements. A transaction snapshot must retain
    ;; its original xmax/xip even though a later READ COMMITTED snapshot would
    ;; observe the changed global XID state.
    (setf *next-xid* 8500
          *active-xids* '(8401 8499))
    (setf second
          (with-statement-snapshot (snapshot #'statement-start)
            snapshot))
    (check (and (= (snapshot-xmin second) (snapshot-xmin first))
                (= (snapshot-xmax second) (snapshot-xmax first))
                (equal (snapshot-xip second) (snapshot-xip first)))
           "subsequent REPEATABLE READ statements reuse the captured horizon")
    (check (= (snapshot-curcid second) (1+ (snapshot-curcid first)))
           "the statement view advances curcid within the retained horizon")
    (check (and (not (heap-tuple-visible-p 8401 0 1 first 8401))
                (heap-tuple-visible-p 8401 0 1 second 8401))
           "a later command sees its own earlier insert through curcid")
    (check (= *active-snapshots* 1)
           "per-statement cleanup leaves only the transaction registration")
    ;; A retained snapshot contributes to the wraparound drain horizon. Its
    ;; owner must be able to keep reading and then end the transaction instead
    ;; of waiting for itself as coordinator or gate waiter.
    (setf *next-xid* (+ +xid-cycle+ +first-normal-xid+))
    (prepare-xid-wraparound-for-statement backend)
    (check (and (not *xid-wrap-pending*)
                (not (backend-wraparound-waiting backend)))
           "a retained snapshot owner does not coordinate XID wraparound")
    (setf *xid-wrap-pending* t)
    (wait-for-xid-wraparound backend)
    (check (not (backend-wraparound-waiting backend))
           "a retained snapshot owner can pass the pending wraparound gate")
    (setf *xid-wrap-pending* nil)
    ;; A read-only transaction need not have assigned an XID. Its retained
    ;; snapshot still has to be released by the no-XID commit path.
    (commit-transaction)
    (check (and (zerop *active-snapshots*)
                (null (oldest-active-snapshot-xmin))
                (eq (backend-state backend) :idle)
                (eq (backend-isolation-level backend) :read-committed)
                (null (backend-transaction-snapshot backend)))
           "no-XID commit releases the retained snapshot and resets isolation")
    (setf (backend-explicit backend) t
          (backend-state backend) :in-progress
          (backend-isolation-level backend) :read-committed
          *next-xid* 8600)
    (let ((rc-first (with-statement-snapshot (snapshot #'statement-start)
                      snapshot)))
      (setf *next-xid* 8700)
      (let ((rc-second (with-statement-snapshot (snapshot #'statement-start)
                         snapshot)))
        (check (> (snapshot-xmax rc-second) (snapshot-xmax rc-first))
               "READ COMMITTED continues to acquire a fresh statement snapshot")))
    (commit-transaction)
    (setf (backend-explicit backend) t
          (backend-state backend) :in-progress
          (backend-isolation-level backend) :repeatable-read)
    (with-statement-snapshot (snapshot #'statement-start)
      snapshot)
    (abort-transaction)
    (check (and (zerop *active-snapshots*)
                (null (backend-transaction-snapshot-registration backend)))
           "rollback releases the long-lived snapshot registration")))

(defun test-wal-enabled-repeatable-read-commit-releases-unpinned-snapshot ()
  (let* ((test-pgdata (merge-pathnames "rr-commit-snapshot-test/" *pgdata*))
         (*pgdata* test-pgdata)
         (xid 8401)
         (*backend* (make-backend :xid xid))
         (*wal-ready* t)
         (*wal-poisoned* nil)
         (*wal-output-stream* nil)
         (*wal-output-segment* nil)
         (*wal-new-segments* '())
         (*wal-end-lsn* 0)
         (*wal-durable-lsn* 0)
         (*wal-checkpoint-lsn* 0)
         (*wal-page-cache* (make-hash-table :test #'equal))
         (*wal-page-cache-xids* (make-hash-table :test #'equal))
         (*wal-xid-first-lsn* (make-hash-table :test #'eql))
         (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
         (*wal-replication-slots* (make-hash-table :test #'equal))
         (*active-snapshots* 0)
         (*active-snapshot-set* (make-hash-table :test #'eq))
         (*active-xids* (list xid))
         (*next-xid* (1+ xid))
         (*xid-batch-end* (1+ xid))
         (*clog-stream* nil)
         (registration (make-snapshot-registration))
         (snapshot (make-snapshot-struct :xmin xid :xmax (1+ xid)
                                         :xip (list xid) :curcid 1)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (wal-directory))
    ;; PostgreSQL's xact.c:CommitTransaction makes WAL durable before
    ;; transaction-end snapshot cleanup. This exercises reset from the
    ;; after-durable callback while *WAL-LOCK* is still held.
    (unwind-protect
         (progn
           (clog-init)
           (setf (backend-state *backend*) :in-progress
                 (backend-explicit *backend*) t
                 (backend-isolation-level *backend*) :repeatable-read
                 (backend-read-only *backend*) nil
                 (backend-transaction-snapshot *backend*) snapshot
                 (backend-transaction-snapshot-registration *backend*)
                 registration
                 (backend-first-snapshot-set *backend*) t)
           (register-active-snapshot snapshot registration)
           (commit-transaction)
           (check (and (= *wal-end-lsn* *wal-durable-lsn*)
                       (plusp *wal-end-lsn*)
                       (= (clog-get xid) +clog-committed+)
                       (eq (backend-state *backend*) :idle)
                       (zerop *active-snapshots*)
                       (not (snapshot-registration-releasing-p registration))
                       (not *wal-poisoned*))
                  "WAL-backed writable REPEATABLE READ commit releases its unpinned snapshot"))
      (when *wal-output-stream*
        (ignore-errors (close *wal-output-stream*)))
      (when *clog-stream*
        (ignore-errors (close *clog-stream*)))
      (setf *wal-output-stream* nil *clog-stream* nil)
      (ignore-errors
       (uiop:delete-directory-tree test-pgdata :validate t
                                               :if-does-not-exist :ignore)))))

(defun test-transaction-isolation-controls ()
  (let* ((*backend* (make-backend))
         (path (merge-pathnames "transaction-isolation.out" *pgdata*)))
    (let* ((begin (first (parse-statements
                          "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")))
           (start (first (parse-statements
                          "START TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ")))
           (set (first (parse-statements
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")))
           (session-set (first (parse-statements
                                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED"))))
      (check (and (eq (getf begin :type) :begin)
                  (eq (getf begin :isolation-level) :repeatable-read)
                  (getf begin :read-only))
             "BEGIN parses REPEATABLE READ and READ ONLY options")
      (check (and (eq (getf start :type) :begin)
                  (eq (getf start :isolation-level) :repeatable-read)
                  (getf start :read-only))
             "START TRANSACTION parses comma-separated characteristics")
      (check (and (getf set :transaction-options-p)
                  (eq (getf (getf set :transaction-options) :isolation-level)
                      :repeatable-read)
                  (getf (getf set :transaction-options) :read-only))
             "SET TRANSACTION preserves requested characteristics in its AST")
      (check (getf session-set :session-characteristics-p)
             "SET SESSION CHARACTERISTICS has a distinct AST path"))
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream
                         (concatenate 'string
                                      "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; "
                                      "SHOW transaction_isolation; SELECT 1; ROLLBACK; "
                                      "BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY; "
                                      "SHOW transaction_isolation; ROLLBACK; "
                                      "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1; "
                                      "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; ROLLBACK; "
                                      "BEGIN ISOLATION LEVEL REPEATABLE READ; "
                                      "SET transaction_isolation = 'read committed'; "
                                      "SHOW transaction_isolation; ROLLBACK; "
                                      "BEGIN ISOLATION LEVEL REPEATABLE READ; "
                                      "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED; "
                                      "SHOW transaction_isolation; ROLLBACK;"))
      (check (eq (backend-state *backend*) :idle)
             "transaction controls leave the backend idle after rollback")
      (check (eq (backend-isolation-level *backend*) :read-committed)
             "transaction isolation returns to READ COMMITTED after rollback")
      (check (not (backend-read-only *backend*))
             "transaction read-only state resets after rollback")
      (exec-simple-query stream "BEGIN; SELECT 1; SET TRANSACTION READ ONLY;"))
    (check (and (eq (backend-state *backend*) :in-progress)
                (backend-read-only *backend*))
           "a read-write transaction may become read-only after a SELECT")
    (with-open-file (stream path
                            :direction :output
                            :if-exists :append
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "INSERT INTO missing VALUES (1);"))
    (check (eq (backend-state *backend*) :aborted)
           "a write in READ ONLY aborts the explicit transaction")
    (with-open-file (stream path
                            :direction :output
                            :if-exists :append
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream
                         "ROLLBACK; BEGIN; SELECT 1; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;"))
    (check (eq (backend-state *backend*) :aborted)
           "a changed isolation level after a snapshot aborts the transaction")
    (with-open-file (stream path
                            :direction :output
                            :if-exists :append
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "ROLLBACK;"))
    (check (equal (test-show-timezone-values path)
                  '("repeatable read" "1" "repeatable read" "1"
                    "read committed" "repeatable read" "1" "1"))
           (format nil "SHOW transaction_isolation returns ~s"
                   (test-show-timezone-values path)))
    (check (zerop *active-snapshots*)
           "isolation control and rollback paths release transaction snapshots")))

(defun test-repeatable-read-write-conflict-horizon ()
  (let* ((*backend* (make-backend))
         (backend *backend*))
    (setf (backend-isolation-level backend) :repeatable-read
          (backend-transaction-snapshot backend)
          (make-snapshot-struct :xmin 100 :xmax 120 :xip '(115) :curcid 1))
    (check (repeatable-read-conflict-p 120)
           "a committed XID at the snapshot xmax is a write conflict")
    (check (repeatable-read-conflict-p 115)
           "an XID active when the repeatable-read snapshot was taken is a write conflict")
    (check (not (repeatable-read-conflict-p 119))
           "a committed XID below xmax and outside xip predates the snapshot")
    (setf (backend-isolation-level backend) :read-committed)
    (check (not (repeatable-read-conflict-p 120))
           "READ COMMITTED does not use the retained-snapshot conflict rule")))

(defun test-xid-wraparound-freezes-tuples ()
  ;; Deterministic one-cycle counterpart to xid_wraparound/t/003_wraparounds.pl.
  (let* ((*backend* (make-backend))
         (*catalog* (make-catalog))
         (*next-xid* (+ +xid-cycle+ +first-normal-xid+))
         (*xid-batch-end* *next-xid*)
         (*active-xids* '())
         (*active-snapshots* 0)
         (*xid-wrap-pending* nil)
         (*xid-wrap-completed-through* nil)
         (relfilenode 918273645)
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (db (make-database :name "postgres" :oid 1))
         (table (make-pg-class :oid 2 :name "wrap_rows"
                               :relfilenode relfilenode
                               :columns columns :indexes nil))
         (page (page-init (make-page))))
    (setf (gethash "postgres" (catalog-databases *catalog*)) db
          (gethash "wrap_rows" (database-tables db)) table)
    (ensure-directories-exist (relation-file-path relfilenode))
    (dolist (spec '((7 0 :committed nil)
                    (7 8 :committed :committed)
                    (9 0 :aborted nil)
                    (7 10 :committed :aborted)))
      (destructuring-bind (xmin xmax xmin-status xmax-status) spec
        (when xmin-status
          (clog-set xmin (ecase xmin-status
                           (:committed +clog-committed+)
                           (:aborted +clog-aborted+))))
        (when xmax-status
          (clog-set xmax (ecase xmax-status
                           (:committed +clog-committed+)
                           (:aborted +clog-aborted+))))
        (page-add-item page
                       (form-heap-tuple (vector 1) columns xmin xmax 0 0 0))))
    (unwind-protect
         (progn
           (buf-extend relfilenode page)
           (complete-xid-wraparound)
           (let ((frozen-page (buf-read relfilenode 0)))
             (loop for expected-flags in (list +lp-normal+ +lp-unused+
                                               +lp-unused+ +lp-normal+)
                   for expected-xmin in '(2 nil nil 2)
                   for expected-xmax in '(0 nil nil 0)
                   for offnum from 1
                   do (multiple-value-bind (off flags len)
                          (page-get-lp frozen-page offnum)
                        (declare (ignore len))
                        (check (= flags expected-flags)
                               "wraparound prunes old dead tuples")
                        (when (= flags +lp-normal+)
                          (check (= (page-u32 frozen-page
                                              (+ off +heap-tuple-xmin-off+))
                                    expected-xmin)
                                 "wraparound freezes old xmin status")
                          (check (= (page-u32 frozen-page
                                              (+ off +heap-tuple-xmax-off+))
                                    expected-xmax)
                                 "wraparound clears old xmax status")))))
           (let ((snapshot (make-snapshot-struct :xmin +first-normal-xid+
                                                 :xmax *next-xid*
                                                 :xip '() :curcid 10)))
             (loop for expected-visible in '(t nil nil t)
                   for offnum from 1
                   for tuple = (heapam-get-tuple relfilenode 0 offnum columns)
                   do (if expected-visible
                          (check (and tuple
                                      (heap-tuple-visible-p
                                       (htup-xmin tuple) (htup-xmax tuple)
                                       (htup-cid tuple) snapshot nil))
                                 "pre-wrap tuple visibility survives freezing")
                          (check (null tuple)
                                 "wraparound removes no-longer-visible tuples"))))
           (check (= (clog-get 7) +clog-in-progress+)
                  "wraparound resets the old CLOG cycle")
           (check (xid-aborted-p 7)
                  "cleared CLOG IDs are abandoned when no longer active")
           (check (not (xid-aborted-p +frozen-xid+))
                  "frozen XID is not aborted after CLOG reset")
           (check (not (xid-aborted-p +bootstrap-xid+))
                  "bootstrap XID is not aborted after CLOG reset")
           (check (eq (unique-conflict-status
                       (make-htup :xmin +frozen-xid+ :xmax 0) nil)
                      :violation)
                  "frozen live tuples still block duplicate unique keys")
           (check (eq (unique-conflict-status
                       (make-htup :xmin +frozen-xid+
                                  :xmax +frozen-xid+)
                       nil)
                      :free)
                  "frozen committed deletes free unique keys")
           (check (not (probe-file (xid-wrap-marker-path)))
                  "completed wraparound removes its recovery marker")
           ;; Replay after a crash that left the durable marker behind even
           ;; though freezing and the CLOG reset had already completed.
           (save-xid-wrap-marker *next-xid*)
           (setf *xid-wrap-pending* t
                 *xid-wrap-completed-through* nil)
           (complete-xid-wraparound)
           (check (not (probe-file (xid-wrap-marker-path)))
                  "wraparound marker replay completes idempotently")
           (check (null *xid-wrap-pending*)
                  "completed wraparound releases the XID allocation gate")
           ;; A cancellation delivered by the marker unlink must be reported
           ;; only after the completed state has opened the XID gate.
           (save-xid-wrap-marker *next-xid*)
           (setf *xid-wrap-pending* t
                 *xid-wrap-completed-through* nil)
           (let ((cancel-error nil)
                 (shutdown-count 0))
             (handler-case
                 (complete-xid-wraparound
                  :delete-marker-function
                  (lambda ()
                    (delete-xid-wrap-marker)
                    (setf (backend-cancel-pending *backend*) t))
                  :shutdown-function
                  (lambda () (incf shutdown-count)))
               (pg-error (condition)
                 (setf cancel-error condition)))
             (check (and cancel-error
                         (string= (pg-error-sqlstate cancel-error) "57014"))
                    "post-marker cancellation is an ordinary query cancel")
             (check (zerop shutdown-count)
                    "post-marker cancellation does not request server shutdown")
             (check (and (not *xid-wrap-pending*)
                         (eql *xid-wrap-completed-through* *next-xid*))
                    "post-marker cancellation publishes the completed transition")
             (check (not (probe-file (xid-wrap-marker-path)))
                    "post-marker cancellation leaves no recovery marker"))
           (let ((expected-xid *next-xid*))
             (check (= (assign-xid) expected-xid)
                    "new-epoch XIDs can be allocated after freezing")))
      (smgr-forget relfilenode :delete t))))

(defun test-prepare-wraparound-fixture (relfilenode)
  (smgr-forget relfilenode :delete t)
  (ignore-errors (delete-file (xid-wrap-marker-path)))
  (ignore-errors (delete-file (pgdata-path "global/pg_xid_wrap_pending.tmp")))
  (let* ((catalog (make-catalog))
         (columns (vector (make-pg-column :name "value" :type :int4
                                          :typmod -1 :attnum 1
                                          :notnull nil)))
         (db (make-database :name "postgres" :oid 1))
         (table (make-pg-class :oid 2 :name "wrap_fault_rows"
                               :relfilenode relfilenode
                               :columns columns :indexes nil))
         (boundary (+ +xid-cycle+ +first-normal-xid+)))
    (setf (gethash "postgres" (catalog-databases catalog)) db
          (gethash "wrap_fault_rows" (database-tables db)) table
          *next-xid* boundary
          *xid-batch-end* boundary
          *active-xids* '()
          *active-snapshots* 0
          *xid-wrap-pending* nil
          *xid-wrap-failure* nil
          *xid-wrap-completed-through* nil)
    ;; XID allocation persisted this reserved batch boundary before wrap.
    (save-next-xid)
    (clog-reset)
    (clog-set 7 +clog-committed+)
    (ensure-directories-exist (relation-file-path relfilenode))
    (dotimes (block 2)
      (let ((page (page-init (make-page))))
        (page-add-item page (form-heap-tuple (vector (1+ block)) columns 7 0 0 0 0))
        (buf-extend relfilenode page)))
    catalog))

(defun test-restart-wraparound-fixture (relfilenode)
  ;; Reopen heap and CLOG files and rebuild XID state from durable storage.
  (smgr-forget relfilenode)
  (when *clog-stream*
    (close *clog-stream*)
    (setf *clog-stream* nil))
  (setf *active-xids* '()
        *active-snapshots* 0
        *xid-wrap-pending* nil
        *xid-wrap-failure* nil
        *xid-wrap-completed-through* nil)
  (xact-init)
  (complete-xid-wraparound))

(defun test-xid-wraparound-fault-recovery ()
  (let ((*backend* nil)
        (*catalog* nil)
        (*next-xid* *next-xid*)
        (*xid-batch-end* *xid-batch-end*)
        (*active-xids* *active-xids*)
        (*active-snapshots* *active-snapshots*)
        (*xid-wrap-pending* *xid-wrap-pending*)
        (*xid-wrap-failure* *xid-wrap-failure*)
        (*xid-wrap-completed-through* *xid-wrap-completed-through*)
        (relfilenode 918273646))
    (labels ((run-failure (name inject expected-marker-p
                           &optional (restart-marker-p expected-marker-p))
               (let ((catalog (test-prepare-wraparound-fixture relfilenode))
                     (boundary (+ +xid-cycle+ +first-normal-xid+))
                     (shutdown-count 0))
                 (setf *catalog* catalog)
                 (check (handler-case
                            (progn
                              (funcall inject
                                       (lambda () (incf shutdown-count)))
                              nil)
                          (error () t))
                        (format nil "~a failure is reported" name))
                 (check (and *xid-wrap-pending* *xid-wrap-failure*)
                        (format nil "~a keeps the XID gate closed" name))
                 (check (= shutdown-count 1)
                        (format nil "~a requests server shutdown" name))
                 (check (eql (not (null (probe-file (xid-wrap-marker-path))))
                             expected-marker-p)
                        (format nil "~a leaves the expected recovery marker state"
                                name))
                 (when (string= name "partial relation freeze")
                   (let* ((first-page (buf-read relfilenode 0))
                          (second-page (buf-read relfilenode 1))
                          (first-off (nth-value 0 (page-get-lp first-page 1)))
                          (second-off (nth-value 0 (page-get-lp second-page 1))))
                     (check (= (page-u32 first-page
                                         (+ first-off +heap-tuple-xmin-off+))
                               +frozen-xid+)
                            "freeze fault leaves the first page partially frozen")
                     (check (= (page-u32 second-page
                                         (+ second-off +heap-tuple-xmin-off+))
                               7)
                            "freeze fault leaves the next page untouched")))
                 (check (handler-case
                            (progn (wait-for-xid-wraparound (make-backend)) nil)
                          (pg-error (condition)
                            (string= (pg-error-severity condition) "FATAL")))
                        (format nil "~a rejects sessions at the failed gate" name))
                 ;; A directory-fsync failure can leave either rename
                 ;; outcome after a crash. Exercise both marker states using
                 ;; a startup reload of the durable XID boundary and CLOG.
                 (if restart-marker-p
                     (save-xid-wrap-marker boundary)
                     (ignore-errors (delete-file (xid-wrap-marker-path))))
                 (test-restart-wraparound-fixture relfilenode)
                 (check (= *next-xid* boundary)
                        (format nil "~a restart reloads the XID boundary" name))
                 (check (not *xid-wrap-pending*)
                        (format nil "~a recovery opens the gate" name))
                 (check (not (probe-file (xid-wrap-marker-path)))
                        (format nil "~a recovery removes the marker" name))
                 (dotimes (block 2)
                   (let* ((page (buf-read relfilenode block))
                          (off (nth-value 0 (page-get-lp page 1))))
                     (check (= (page-u32 page (+ off +heap-tuple-xmin-off+))
                               +frozen-xid+)
                            (format nil "~a recovery freezes page ~d" name block)))))))
      (run-failure
       "control file fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-next-xid-function
          (lambda ()
            (save-next-xid
             :fsync-function (lambda (fd)
                               (declare (ignore fd))
                               (error "injected control file fsync failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "control file rename"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-next-xid-function
          (lambda ()
            (save-next-xid
             :rename-function (lambda (from to)
                                (declare (ignore from to))
                                (error "injected control file rename failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "control file directory fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-next-xid-function
          (lambda ()
            (save-next-xid
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected control directory fsync failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "marker file fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-marker-function
          (lambda (boundary)
            (save-xid-wrap-marker
             boundary
             :fsync-function (lambda (fd)
                               (declare (ignore fd))
                               (error "injected marker file fsync failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "marker rename"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-marker-function
          (lambda (boundary)
            (save-xid-wrap-marker
             boundary
             :rename-function (lambda (from to)
                                (declare (ignore from to))
                                (error "injected marker rename failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "marker directory fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-marker-function
          (lambda (boundary)
            (save-xid-wrap-marker
             boundary
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected marker directory fsync failure"))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "marker directory fsync with rename lost"
       (lambda (shutdown)
         (complete-xid-wraparound
          :save-marker-function
          (lambda (boundary)
            (save-xid-wrap-marker
             boundary
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected marker directory fsync failure"))))
          :shutdown-function shutdown))
       t nil)
      (run-failure
       "partial relation freeze"
       (lambda (shutdown)
         (complete-xid-wraparound
          :freeze-function
          (lambda (old-epoch)
            (freeze-all-heap-xids
             old-epoch
             :after-page-function
             (lambda (relation block)
               (declare (ignore relation))
               (when (zerop block)
                 (error "injected partial relation freeze failure")))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "relation fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :freeze-function
          (lambda (old-epoch)
            (freeze-all-heap-xids
             old-epoch
             :fsync-function (lambda (relation)
                               (declare (ignore relation))
                               (error "injected relation fsync failure"))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "CLOG file fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :reset-clog-function
          (lambda (&key fsync-function fsync-directory-function)
            (declare (ignore fsync-function fsync-directory-function))
            (clog-reset
             :fsync-function (lambda (stream)
                               (declare (ignore stream))
                               (error "injected CLOG file fsync failure"))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "CLOG directory fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :reset-clog-function
          (lambda (&key fsync-function fsync-directory-function)
            (declare (ignore fsync-function fsync-directory-function))
            (clog-reset
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected CLOG directory fsync failure"))))
          :shutdown-function shutdown))
       t)
      (run-failure
       "marker removal directory fsync"
       (lambda (shutdown)
         (complete-xid-wraparound
          :delete-marker-function
          (lambda (&key fsync-directory-function)
            (declare (ignore fsync-directory-function))
            (delete-xid-wrap-marker
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected marker removal directory fsync failure"))))
          :shutdown-function shutdown))
       nil)
      (run-failure
       "marker removal directory fsync with unlink lost"
       (lambda (shutdown)
         (complete-xid-wraparound
          :delete-marker-function
          (lambda (&key fsync-directory-function)
            (declare (ignore fsync-directory-function))
            (delete-xid-wrap-marker
             :fsync-directory-function
             (lambda (directory)
               (declare (ignore directory))
               (error "injected marker removal directory fsync failure"))))
          :shutdown-function shutdown))
       nil t))))

(defun test-xid-wraparound-coordinator-cancel ()
  (let* ((relfilenode 918273647)
         (*backend* nil)
         (*catalog* nil)
         (old-next-xid *next-xid*)
         (old-batch-end *xid-batch-end*)
         (old-active-xids *active-xids*)
         (old-active-snapshots *active-snapshots*)
         (old-wrap-pending *xid-wrap-pending*)
         (old-wrap-failure *xid-wrap-failure*)
         (old-completed-through *xid-wrap-completed-through*)
         (pgdata *pgdata*)
         (coordinator (make-backend))
         (waiter (make-backend))
         (coordinator-waiting nil)
         (coordinator-error nil)
         (waiter-error nil)
         (shutdown-count 0)
         (catalog nil)
         (coordinator-thread nil)
         (waiter-thread nil))
    (unwind-protect
         (progn
           (setf catalog (test-prepare-wraparound-fixture relfilenode))
           ;; Keep the active-XID state global so both SBCL threads observe it.
           (setf *active-xids* '(77))
           (setf coordinator-thread
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*backend* coordinator)
                          (*catalog* catalog)
                          (*pgdata* pgdata))
                      (handler-case
                          (complete-xid-wraparound
                           :wait-for-xids-function
                           (lambda ()
                             (setf coordinator-waiting t)
                             (wait-for-no-active-xids))
                           :shutdown-function
                           (lambda () (incf shutdown-count)))
                        (error (condition)
                          (setf coordinator-error condition)))))))
           (check (test-wait-until (lambda () coordinator-waiting))
                  "wraparound coordinator waits for active XIDs")
           (setf waiter-thread
                 (sb-thread:make-thread
                  (lambda ()
                    (let ((*backend* waiter)
                          (*catalog* catalog)
                          (*pgdata* pgdata))
                      (handler-case
                          (prepare-xid-wraparound-for-statement waiter)
                        (error (condition)
                          (setf waiter-error condition)))))))
           (check (test-wait-until
                   (lambda () (backend-wraparound-waiting waiter)))
                  "another backend waits on the wraparound gate")
           (setf (backend-cancel-pending coordinator) t)
           (check (test-wait-until
                   (lambda () (and coordinator-error waiter-error)))
                  "coordinator cancellation releases waiting backends")
           (check (and (typep coordinator-error 'pg-error)
                       (string= (pg-error-sqlstate coordinator-error) "XX001"))
                  "failed coordinator receives a fatal wraparound error")
           (check (and (typep waiter-error 'pg-error)
                       (string= (pg-error-sqlstate waiter-error) "XX001"))
                  "waiting backend receives a fatal wraparound error")
           (check (and (typep *xid-wrap-failure* 'pg-error)
                       (string= (pg-error-sqlstate *xid-wrap-failure*) "57014"))
                  "wraparound failure records the original cancellation")
           (check (= shutdown-count 1)
                  "wraparound cancellation requests server shutdown once"))
      (setf *active-xids* '())
      (broadcast-xact)
      (sb-thread:with-mutex (*xid-wrap-lock*)
        (sb-thread:condition-broadcast *xid-wrap-cv*))
      (dolist (thread (list coordinator-thread waiter-thread))
        (when (and thread (sb-thread:thread-alive-p thread))
          (ignore-errors (sb-thread:join-thread thread :timeout 2 :default nil))))
      (ignore-errors (delete-file (xid-wrap-marker-path)))
      (smgr-forget relfilenode :delete t)
      (setf *next-xid* old-next-xid
            *xid-batch-end* old-batch-end
            *active-xids* old-active-xids
            *active-snapshots* old-active-snapshots
            *xid-wrap-pending* old-wrap-pending
            *xid-wrap-failure* old-wrap-failure
            *xid-wrap-completed-through* old-completed-through))))
