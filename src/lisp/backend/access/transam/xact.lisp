;;;; xact.lisp -- transaction state, XID allocation, commit log integration.

(in-package #:pg)

(defstruct backend
  stream
  socket
  (pid 0)
  (cancel-key nil)
  (database "postgres")
  (replication-mode nil)
  (application-name nil)
  (reported-settings (make-hash-table :test #'equal))
  (xid nil)
  (state :idle)                 ; :idle | :in-progress | :aborted
  (explicit nil)
  (implicit-query-block nil)
  (isolation-level :read-committed)
  (read-only (recovery-in-progress-p))
  (command-id 0)
  (snapshot nil)
  (transaction-snapshot nil)
  (transaction-snapshot-registration nil)
  (transaction-schema-read-lock-held-p nil)
  (first-snapshot-set nil)
  (dirty-relations '())
  (logical-changes '())
  (logical-spool nil)
  (logical-change-bytes 0)
  (sequence-currvals (make-hash-table :test #'eql))
  (settings (make-hash-table :test #'equal))
  (settings-transaction-start nil)
  (transaction-start-unix nil)
  (transaction-start-unix-microseconds nil)
  (settings-reset (make-hash-table :test #'equal))
  (statement-deadline nil)
  ;; Interrupt state: the equivalents of QueryCancelPending and the
  ;; backend's own thread, used by pg_cancel_backend()/pg_terminate_backend().
  (executing nil)
  (cancel-pending nil)
  ;; Published under the admission mutex and retained across query boundaries
  ;; until the target delivers termination.  Future masked sync waits may inspect
  ;; this flag without consuming it before commit cleanup.
  (termination-pending nil)
  (wal-failure nil)
  ;; Visible to the local control REPL so concurrency tests can synchronize on
  ;; a real lock wait instead of relying on scheduler timing.
  (waiting-xid nil)
  (wraparound-waiting nil)
  (thread nil)
  (virtual-reader-xid nil))

(defvar *backend* nil)

(declaim (special *live-operation-lock*))
(declaim (special *live-operation-cv*))
(declaim (special *live-operation-backend-query-tokens*))
(declaim (notinline get-database database-oid))

(defun set-backend-state (backend state)
  "Publish transaction state atomically with reload admission."
  (sb-thread:with-mutex (*live-operation-lock*)
    (when (and (eq state :in-progress) (eq (backend-state backend) :idle))
      (multiple-value-bind (seconds microseconds) (sb-ext:get-time-of-day)
        (setf (backend-transaction-start-unix backend) seconds
              (backend-transaction-start-unix-microseconds backend) (+ (* seconds 1000000) microseconds))))
    (when (eq state :idle)
      (setf (backend-transaction-start-unix backend) nil
            (backend-transaction-start-unix-microseconds backend) nil))
    (setf (backend-state backend) state)
    (sb-thread:condition-broadcast *live-operation-cv*))
  state)

(defmacro with-backend-wait-state ((backend accessor value) &body body)
  "Publish BACKEND's wait state for BODY and clear it on every exit."
  (let ((backend-value (gensym "BACKEND-"))
        (wait-value (gensym "WAIT-VALUE-")))
    `(let ((,backend-value ,backend)
           (,wait-value ,value))
       (unwind-protect
            (progn
              (when ,backend-value
                (setf (,accessor ,backend-value) ,wait-value))
              ,@body)
         (when ,backend-value
           (setf (,accessor ,backend-value) nil))))))

(defun clear-statement-timeout (&optional (backend *backend*))
  (when backend (setf (backend-statement-deadline backend) nil)))

(defun arm-statement-timeout (&optional (backend *backend*))
  "Arm once after admission; parsing and the first statement share one deadline."
  (when (and backend (not (backend-statement-deadline backend)))
    (let ((milliseconds (gethash "statement_timeout" (backend-settings backend) 0)))
      (when (plusp milliseconds)
        (setf (backend-statement-deadline backend)
              (+ (get-internal-real-time)
                 (ceiling (* milliseconds internal-time-units-per-second) 1000)))))))

(defun backend-wait-timeout ()
  "Bound cooperative waits by the statement deadline, with a short cancel poll."
  (let ((deadline (and *backend* (backend-statement-deadline *backend*))))
    (if deadline
        (max 0.001 (min 0.1 (/ (- deadline (get-internal-real-time))
                               (float internal-time-units-per-second 1d0))))
        0.1)))

(defun note-backend-wal-failure (condition)
  (when (and *backend* (not (backend-wal-failure *backend*)))
    (setf (backend-wal-failure *backend*) condition))
  (values))

(defun current-backend-wal-failed-p ()
  (and *backend* (not (null (backend-wal-failure *backend*)))))

(defun deliver-backend-termination (backend)
  ;; Consume only at delivery, and let termination supersede query cancel.
  ;; A queued helper becomes a no-op if a cooperative check delivered it first.
  (when (and backend (backend-termination-pending backend)
             (not (backend-wal-failure backend)))
    (setf (backend-termination-pending backend) nil
          (backend-cancel-pending backend) nil)
    (ereport-fatal "57P01" "terminating connection due to administrator command"))
  (values))

(defun check-for-interrupts ()
  "Deliver pending termination before checking statement timeout or cancel."
  (when (current-backend-wal-failed-p)
    (return-from check-for-interrupts (values)))
  (deliver-backend-termination *backend*)
  (when (and *backend* (backend-statement-deadline *backend*)
             (>= (get-internal-real-time) (backend-statement-deadline *backend*)))
    (clear-statement-timeout)
    (setf (backend-cancel-pending *backend*) nil)
    (ereport "57014" "canceling statement due to statement timeout"))
  (when (and *backend* (backend-cancel-pending *backend*))
    (setf (backend-cancel-pending *backend*) nil)
    (ereport "57014" "canceling statement due to user request")))

(defun begin-backend-query-execution (backend)
  "Mark a frontend query active before it waits for reload admission."
  (when backend
    (sb-thread:with-mutex (*live-operation-lock*)
      ;; Clear an idle cancel before publishing EXECUTING. A cancel request
      ;; arriving after this transition will be observed by admission waiters.
      (setf (backend-cancel-pending backend) nil
            (backend-executing backend) t
            (gethash backend *live-operation-backend-query-tokens*)
            (cons nil nil))))
  (values))

(defun end-backend-query-execution (backend)
  "Clear frontend query interrupt state after dispatch and any gate wait."
  (clear-statement-timeout backend)
  (when backend
    (sb-thread:with-mutex (*live-operation-lock*)
      (setf (backend-executing backend) nil
            (backend-cancel-pending backend) nil)
      (remhash backend *live-operation-backend-query-tokens*)
      (sb-thread:condition-broadcast *live-operation-cv*)))
  (values))

(defmacro with-backend-query-execution
    ((&key (backend '*backend*) (start t) (finish t) (cleanup :direct)) &body body)
  "Capture query lifecycle controls once and guard cleanup before publication.
START and FINISH are independent. CLEANUP selects :DIRECT for in-image callers
or :QUERY-STATE for the wire dispatch's existing reload/shutdown cleanup lane."
  (let ((captured (gensym "BACKEND-"))
        (start-p (gensym "START-"))
        (finish-p (gensym "FINISH-")))
    `(let ((,captured ,backend)
           (,start-p ,start)
           (,finish-p ,finish))
       (unwind-protect
            (progn
              (when (and ,captured ,start-p)
                (begin-backend-query-execution ,captured))
              ,@body)
         (when (and ,captured ,finish-p)
           ,(ecase cleanup
              (:direct `(end-backend-query-execution ,captured))
              (:query-state
               `(let ((*backend* ,captured))
                  (with-live-operation-query-state-cleanup
                    (end-backend-query-execution ,captured))))))))))

;;; ---- pg_control: nextXid, fsynced write-ahead of XID allocation ----

(defun pgdata-path (relative)
  (merge-pathnames relative *pgdata*))

(defun control-path ()
  (pgdata-path "global/pg_control"))

(defun xid-persistent-state-exists-p ()
  (let ((clog (clog-path)))
    (or (probe-file (pgdata-path "global/pg_catalog.dat"))
        (and (probe-file clog)
             (with-open-file (s clog :direction :input
                                     :element-type '(unsigned-byte 8))
               (plusp (file-length s))))
        (not (null (directory (pgdata-path "base/*")))))))

(defun load-next-xid ()
  (let ((path (control-path)))
    (if (probe-file path)
        (handler-case
            (with-open-file (s path :direction :input :if-does-not-exist nil)
              (let* ((line (read-line s nil nil))
                     (xid (and line (parse-integer line :junk-allowed nil))))
                (unless (and xid
                             (<= +first-normal-xid+ xid +max-xid-one-past+)
                             (null (read-line s nil nil)))
                  (error "invalid next XID value"))
                xid))
          (error (e)
            (error "cannot load XID control file ~a: ~a" path e)))
        (progn
          (when (xid-persistent-state-exists-p)
            (error "XID control file ~a is missing while persistent database state exists"
                   path))
          +first-normal-xid+))))

(defvar *xid-batch-end* +first-normal-xid+)

(defun save-next-xid (&key (fsync-function #'sb-posix:fsync)
                        (rename-function #'sb-posix:rename)
                        (fsync-directory-function #'fsync-directory))
  (with-atomic-output (stream
                       (control-path)
                       (pgdata-path "global/pg_control.tmp")
                       (pgdata-path "global/")
                       :fsync-function fsync-function
                       :rename-function rename-function
                       :fsync-directory-function fsync-directory-function)
    (format stream "~a~%" *xid-batch-end*)))

(defvar *xid-lock* (sb-thread:make-mutex :name "pg xid lock"))
(defvar *next-xid* +first-normal-xid+)
(defvar *active-xids* '())

;; All three structures are protected by *XID-LOCK*.  Capture mode is chosen
;; once per XID; arming a slot enables new XIDs before old uncaptured XIDs drain.
(defvar *logical-capture-xid-modes* (make-hash-table :test #'eql))

(defvar *logical-binary-required-xids* (make-hash-table :test #'eql)
  "Database OIDs of active XIDs requiring binary bytea; protected by *XID-LOCK*.")

(defun logical-require-binary-bytea-for-xid (xid database-oid)
  (unless (and (integerp database-oid) (<= 1 database-oid #xffffffff))
    (error "invalid database OID for binary transport"))
  (sb-thread:with-mutex (*xid-lock*)
    (unless (member xid *active-xids* :test #'eql)
      (error "cannot mark an inactive transaction for binary transport"))
    (let ((prior (gethash xid *logical-binary-required-xids*)))
      (when (and prior (/= prior database-oid))
        (error "binary transport transaction database changed"))
      (setf (gethash xid *logical-binary-required-xids*) database-oid)))
  (values))

(declaim (special *logical-capture-arming*))

(defun wal-validate-logical-binary-capability-under-lock (xid)
  "Reject incompatible live slots before beginning XID's WAL publication."
  (multiple-value-bind (database-oid slot-creation-p)
      (sb-thread:with-mutex (*xid-lock*)
        (values (gethash xid *logical-binary-required-xids*)
                (plusp (hash-table-count *logical-capture-arming*))))
    (when database-oid
      (when slot-creation-p
        (wal-invalid-input "replication slot creation conflicts with binary-only bytea transaction"))
      (maphash
       (lambda (name slot)
         (declare (ignore name))
         (when (and (eq (wal-replication-slot-wal-status slot) :reserved)
                    (or (null (wal-replication-slot-database-oid slot))
                        (= database-oid (wal-replication-slot-database-oid slot)))
                    (not (eq (wal-slot-profile slot) :binary-bytea)))
           (wal-invalid-input "active replication slot cannot receive binary-only bytea")))
       *wal-replication-slots*)))
  (values))

(defvar *logical-capture-arming* (make-hash-table :test #'eq))

(defvar *logical-capture-live-slots* nil
  "Live slot count, or NIL until authoritatively refreshed under WAL lock.")

(defvar *wal-level* :replica
  "Installed WAL level used to select each new XID's logical capture mode.")

(defvar *xid-wrap-lock* (sb-thread:make-mutex :name "pg xid wrap gate"))
(defvar *xid-wrap-work-lock* (sb-thread:make-mutex :name "pg xid wrap worker"))
(defvar *xid-wrap-cv* (sb-thread:make-waitqueue :name "pg xid wrap waitqueue"))
(defvar *xid-wrap-pending* nil)
(defvar *xid-wrap-failure* nil)
(defvar *xid-wrap-completed-through* nil)

(defvar *xact-cv* (sb-thread:make-waitqueue :name "pg xact waitqueue"))
(defvar *xact-cv-mutex* (sb-thread:make-mutex :name "pg xact cv mutex"))

;;; Relation access registrations let relation DDL wait for active transactions
;;; that have used a table, while preventing new accesses until it completes.
;;; Registrations live until the XID commits or aborts.
(defvar *relation-lock-mutex* (sb-thread:make-mutex :name "pg relation lock"))
(defvar *relation-lock-cv* (sb-thread:make-waitqueue :name "pg relation waitqueue"))
(defvar *relation-builders* (make-hash-table :test #'eql))
(defvar *relation-reader-blockers* (make-hash-table :test #'eql))
(defvar *relation-build-groups* (make-hash-table :test #'eq))
(defvar *relation-writers* (make-hash-table :test #'eql))
(defvar *relation-readers* (make-hash-table :test #'eql))

(defvar *recovery-reader-xids* (make-hash-table :test #'eql)
  "Process-local negative reader IDs, protected by *RELATION-LOCK-MUTEX*.
Membership lasts until transaction cleanup, including deferred backend cleanup.
These identities never enter the real active-XID set, CLOG or WAL.")

(defvar *relation-maintainers* (make-hash-table :test #'eql))

(defvar *relation-lock-object-limit* nil
  "Private object cap, or NIL before startup installs the relation limits.
Startup sizes this shared pool from the active max_locks_per_transaction setting;
a pending change leaves the installed pool in use until restart.")

(defvar *relation-lock-owner-limit* nil
  "Private relation-owner pair cap, or NIL for no limit; see object cap.")

(defvar *relation-resource-requests* (make-hash-table :test #'eq)
  "Pending gate tokens and copied relation sets, protected by the relation mutex.
These reserve capacity only; they do not grant conflict permissions.")

(defun print-relation-gate-token (object stream depth)
  (declare (ignore depth))
  (print-unreadable-object (object stream :type t :identity t)))

(defstruct (relation-gate-token
            (:constructor make-relation-gate-token (owner))
            (:print-function print-relation-gate-token))
  "A gate scope with an owner independent of the build group metadata.
OWNER may be an XID or backend; a token without an owner identifies itself."
  (owner nil :read-only t))

(defun relation-resource-footprint-under-lock (&optional requested owner)
  "Count distinct relations and relation-owner pairs under relation exclusion.
Count published reader, writer, builder and maintainer registrations together
with pending requests.
Modes and upgrades for the same pair count once. REQUESTED adds the whole
prospective set. Resolve each backend once per count, using its virtual or real
XID if present, otherwise the backend itself; do not force XID allocation.
Recounting after release needs no separate ledger."
  (unless (sb-thread:holding-mutex-p *relation-lock-mutex*)
    (error "relation resource accounting requires relation exclusion"))
  (let ((objects (make-hash-table :test #'eql))
        (backends (make-hash-table :test #'eq)))
    (labels ((identity-of (value)
               (cond
                 ((relation-gate-token-p value)
                  (let ((actor (relation-gate-token-owner value)))
                    (if actor (identity-of actor) value)))
                 ((backend-p value)
                  (or (gethash value backends)
                      (setf (gethash value backends)
                            (or (backend-virtual-reader-xid value)
                                (backend-xid value) value))))
                 (t value)))
             (note (node actor)
               (let ((owners (or (gethash node objects)
                                 (setf (gethash node objects)
                                       (make-hash-table :test #'eql)))))
                 (setf (gethash (identity-of actor) owners) t))))
      (dolist (registrations (list *relation-readers* *relation-writers*))
        (maphash (lambda (node owners)
                   (dolist (actor owners) (note node actor)))
                 registrations))
      (dolist (registrations (list *relation-builders* *relation-maintainers*))
        (maphash #'note registrations))
      (maphash (lambda (request nodes)
                 (dolist (node nodes) (note node request)))
               *relation-resource-requests*)
      (dolist (node requested) (note node owner))
      (values (hash-table-count objects)
              (loop for owners being the hash-values of objects
                    sum (hash-table-count owners))))))

(defun relation-resource-footprint ()
  "Return the current object and owner-pair counts under relation exclusion."
  (sb-thread:with-mutex (*relation-lock-mutex*)
    (relation-resource-footprint-under-lock)))

(defun relation-resource-check-under-lock (requested owner)
  "Check the complete prospective set before publishing any registration."
  (when (or *relation-lock-object-limit* *relation-lock-owner-limit*)
    (multiple-value-bind (objects owners)
        (relation-resource-footprint-under-lock requested owner)
      (when (or (and *relation-lock-object-limit* (> objects *relation-lock-object-limit*))
                (and *relation-lock-owner-limit* (> owners *relation-lock-owner-limit*)))
        (ereport "53200" "out of shared memory"
                 :hint "You might need to increase \"max_locks_per_transaction\"."))))
  (values))

(defun call-with-relation-resource-request (requested token function)
  "Reserve capacity for REQUESTED before calling FUNCTION outside the mutex.
TOKEN is fresh and carries the eventual owner's identity. Initial mutex entry is
interruptible; the check and insertion are masked. FUNCTION waits for conflicts
and, under the mutex with interrupts masked, removes the request and publishes
the actual registrations together. It owns cleanup of any granted gates. On
exit this wrapper removes any remaining request with interrupts masked, so
cancellation and the grant handoff both leave no pending reservation."
  (when (sb-thread:holding-mutex-p *relation-lock-mutex*)
    (error "relation requests must enter outside relation exclusion"))
  (let ((nodes (remove-duplicates (copy-list requested) :test #'=))
        (published nil))
    (unwind-protect
         (progn
           (sb-thread:with-mutex (*relation-lock-mutex*)
             (sb-sys:without-interrupts
               (when (nth-value 1 (gethash token *relation-resource-requests*))
                 (error "relation request is already queued"))
               (relation-resource-check-under-lock nodes token)
               (setf published t)
               (setf (gethash token *relation-resource-requests*) nodes)))
           (funcall function token))
      (when published
        (sb-sys:without-interrupts
          (sb-thread:with-mutex (*relation-lock-mutex*)
            (remhash token *relation-resource-requests*)))))))

;; Exported initial-copy snapshots pin a schema view across multiple relation
;; reads. PostgreSQL holds relation locks for DDL and AccessExclusiveLock for
;; VACUUM FULL. This global gate excludes exported snapshot scopes from schema
;; changers, while allowing independent DDL operations to overlap. Conflicting
;; DDL still uses the relation gates and catalog locks below. Ordinary VACUUM
;; stays outside because its pruning horizon honors the MVCC snapshot.
(defvar *schema-lock-mutex* (sb-thread:make-mutex :name "pg schema lock"))
(defvar *schema-lock-cv* (sb-thread:make-waitqueue :name "pg schema waitqueue"))
(defvar *schema-read-lock-count* 0)
(defvar *schema-write-lock-owners* nil)
(defvar *schema-write-lock-waiters* 0)
(defvar *schema-read-lock-held* nil)
(defvar *schema-write-lock-held* nil)

(defvar *exported-snapshot-backend* nil)
(defvar *exported-snapshot-cleanup* nil)

(defun broadcast-xact ()
  (sb-thread:with-mutex (*xact-cv-mutex*)
    (sb-thread:condition-broadcast *xact-cv*)))

(defun condition-wait-with-timeout (waitqueue mutex)
  "Wait with bounded interrupt latency, returning with MUTEX held after timeout."
  (let ((notified (sb-thread:condition-wait waitqueue mutex :timeout (backend-wait-timeout))))
    ;; SBCL returns NIL on timeout without reacquiring MUTEX.
    (unless notified
      (sb-thread:grab-mutex mutex))
    notified))

(defun acquire-schema-read-lock (on-acquired)
  "Acquire the schema read side and publish ownership before interrupts resume."
  (sb-thread:with-mutex (*schema-lock-mutex*)
    (loop while (or *schema-write-lock-owners*
                    (plusp *schema-write-lock-waiters*))
          do (check-for-interrupts)
             (condition-wait-with-timeout *schema-lock-cv*
                                          *schema-lock-mutex*))
    (sb-sys:without-interrupts
      (incf *schema-read-lock-count*)
      (funcall on-acquired)))
  (values))

(defun release-schema-read-lock ()
  (sb-sys:without-interrupts
    (sb-thread:with-mutex (*schema-lock-mutex*)
      (unless (plusp *schema-read-lock-count*)
        (error "schema read lock released without an owner"))
      (decf *schema-read-lock-count*)
      (when (zerop *schema-read-lock-count*)
        (sb-thread:condition-broadcast *schema-lock-cv*))))
  (values))

(defun acquire-schema-write-lock (on-acquired)
  "Join the schema changer cohort; ON-ACQUIRED publishes cleanup ownership.

The wait remains interruptible. Waiter accounting and the successful ownership
handoff are interrupt-free so cancellation cannot strand either state."
  (let ((token (gensym "SCHEMA-WRITER-"))
        (queued nil))
    (unwind-protect
         (progn
           (sb-thread:with-mutex (*schema-lock-mutex*)
             (sb-sys:without-interrupts
               (incf *schema-write-lock-waiters*)
               (setf queued t))
             (loop while (plusp *schema-read-lock-count*)
                   do (check-for-interrupts)
                      (condition-wait-with-timeout *schema-lock-cv*
                                                   *schema-lock-mutex*))
             (sb-sys:without-interrupts
               (push token *schema-write-lock-owners*)
               (decf *schema-write-lock-waiters*)
               (setf queued nil)
               (funcall on-acquired token)
               (sb-thread:condition-broadcast *schema-lock-cv*)))
           token)
      (sb-sys:without-interrupts
        (when queued
          (sb-thread:with-mutex (*schema-lock-mutex*)
            (decf *schema-write-lock-waiters*)
            (sb-thread:condition-broadcast *schema-lock-cv*)))))))

(defun release-schema-write-lock (token)
  (sb-sys:without-interrupts
    (sb-thread:with-mutex (*schema-lock-mutex*)
      (unless (member token *schema-write-lock-owners* :test #'eq)
        (error "schema write lock released by a non-owner"))
      (setf *schema-write-lock-owners*
            (delete token *schema-write-lock-owners* :test #'eq))
      (sb-thread:condition-broadcast *schema-lock-cv*)))
  (values))

(defun backend-holds-schema-read-lock-p (&optional (backend *backend*))
  (and backend (backend-transaction-schema-read-lock-held-p backend)))

(defmacro with-schema-read-lock (&body body)
  "Hold a schema-stable view through BODY, blocking incompatible DDL."
  (let ((acquired (gensym "SCHEMA-READ-ACQUIRED-")))
    `(if (or *schema-write-lock-held* *schema-read-lock-held*
             (backend-holds-schema-read-lock-p))
         (progn ,@body)
         (let ((,acquired nil))
           (unwind-protect
                (progn
                  (acquire-schema-read-lock
                   (lambda () (setf ,acquired t)))
                  (let ((*schema-read-lock-held* t))
                    ,@body))
             (sb-sys:without-interrupts
               (when ,acquired
                 (release-schema-read-lock))))))))

(defmacro with-schema-write-lock (&body body)
  "Exclude exported snapshots while BODY changes schema.
Independent schema changers may overlap; relation/catalog locks order DDL."
  (let ((token (gensym "SCHEMA-WRITE-TOKEN-"))
        (acquired (gensym "SCHEMA-WRITE-ACQUIRED-")))
    `(cond
       (*schema-write-lock-held* (progn ,@body))
       ((or *schema-read-lock-held* (backend-holds-schema-read-lock-p))
        (ereport "25001"
                 "schema changes cannot run inside an exported snapshot scope"))
       (t
        (let ((,token nil)
              (,acquired nil))
          (unwind-protect
               (progn
                 (acquire-schema-write-lock
                  (lambda (owner)
                    (setf ,token owner
                          ,acquired t)))
                 (let ((*schema-write-lock-held* t))
                   ,@body))
            (sb-sys:without-interrupts
              (when ,acquired
                (release-schema-write-lock ,token)))))))))

(defun first-normal-xid-of-epoch-p (xid)
  (and (>= xid +xid-cycle+)
       (= (mod xid +xid-cycle+) +first-normal-xid+)))

(defun xid-next-epoch-boundary (xid)
  (+ (* (1+ (floor xid +xid-cycle+)) +xid-cycle+)
     +first-normal-xid+))

(defun canonical-next-full-xid (xid)
  "Round a reservation end past special low IDs; preserve the exhaustion sentinel."
  (unless (typep xid '(integer 3 18446744073709551616))
    (error "invalid FullXID reservation end"))
  (if (= xid +max-xid-one-past+) xid
      (+ xid (max 0 (- +first-normal-xid+ (mod xid +xid-cycle+))))))

(defun xid-reservation-end (next-xid)
  (unless (and (typep next-xid '(integer 3 18446744073709551615))
               (>= (mod next-xid +xid-cycle+) +first-normal-xid+))
    (error "cannot reserve a batch from a special or exhausted FullXID"))
  (canonical-next-full-xid
   (min +max-xid-one-past+ (xid-next-epoch-boundary next-xid) (+ next-xid 1000))))

(defun xid-epoch-start (xid)
  (+ (* (floor xid +xid-cycle+) +xid-cycle+)
     +first-normal-xid+))

(defun next-full-xid ()
  (sb-thread:with-mutex (*xid-lock*)
    *next-xid*))

(defun advance-full-xid (xid)
  "Advance a FullTransactionId, skipping the three special low XIDs."
  (if (= xid +max-xid+)
      +max-xid-one-past+
      (let ((next (1+ xid)))
        (loop while (< (mod next +xid-cycle+) +first-normal-xid+)
              do (incf next))
        next)))

(defun full-xid-from-32 (xid &optional (reference *next-xid*))
  "Reconstruct the current-cycle FullTransactionId for a stored XID.

Old-cycle heap XIDs are frozen before their low 32 bits can be reused."
  (let ((xid (logand xid +xid-mask+)))
    (if (< xid +first-normal-xid+)
        xid
        (let* ((epoch (floor reference +xid-cycle+))
               (candidate (+ (* epoch +xid-cycle+) xid)))
          (when (and (plusp epoch) (>= candidate reference))
            (decf candidate +xid-cycle+))
          candidate))))

(defun xid-wrap-marker-path ()
  (pgdata-path "global/pg_xid_wrap_pending"))

(defun load-xid-wrap-marker ()
  (let ((path (xid-wrap-marker-path)))
    (when (probe-file path)
      (with-open-file (s path :direction :input :if-does-not-exist nil)
        (let ((line (and s (read-line s nil nil))))
          (or (and line (parse-integer line :junk-allowed nil))
              (error "invalid XID wrap marker ~a" path)))))))

(defun save-xid-wrap-marker (boundary &key (fsync-function #'sb-posix:fsync)
                                        (rename-function #'sb-posix:rename)
                                        (fsync-directory-function
                                         #'fsync-directory))
  (with-atomic-output (stream
                       (xid-wrap-marker-path)
                       (pgdata-path "global/pg_xid_wrap_pending.tmp")
                       (pgdata-path "global/")
                       :fsync-function fsync-function
                       :rename-function rename-function
                       :fsync-directory-function fsync-directory-function)
    (format stream "~a~%" boundary)))

(defun delete-xid-wrap-marker (&key (fsync-directory-function
                                     #'fsync-directory))
  (let ((path (xid-wrap-marker-path)))
    (when (probe-file path)
      (delete-file path)
      (funcall fsync-directory-function (pgdata-path "global/"))))
  (values))

(defconstant +manual-vacuum-xid-stop-age+ 200000000)

(declaim (ftype function catalog-oldest-relfrozenxid))

(defun active-xids-empty-p ()
  (sb-thread:with-mutex (*xid-lock*)
    (null *active-xids*)))

(defun xid-wrap-users-empty-p ()
  "Return true when both virtual readers and real XIDs have drained.

Take the relation lock before the XID lock, as in transaction cleanup."
  (sb-thread:with-mutex (*relation-lock-mutex*)
    (and (zerop (hash-table-count *recovery-reader-xids*))
         (active-xids-empty-p))))

(defun wait-for-no-active-xids ()
  (loop until (xid-wrap-users-empty-p)
        do (check-for-interrupts)
           (sb-thread:with-mutex (*xact-cv-mutex*)
             (unless (xid-wrap-users-empty-p)
               (condition-wait-with-timeout *xact-cv* *xact-cv-mutex*))))
  (values))

(defun active-snapshots-empty-p ()
  (sb-thread:with-mutex (*snapshot-count-lock*)
    (zerop *active-snapshots*)))

(defun wait-for-no-active-snapshots ()
  (loop until (active-snapshots-empty-p)
        do (check-for-interrupts)
           (sb-thread:with-mutex (*snapshot-cv-mutex*)
             (unless (active-snapshots-empty-p)
               (condition-wait-with-timeout *snapshot-cv*
                                            *snapshot-cv-mutex*))))
  (values))

(defun xid-wraparound-failed-error ()
  (ereport-fatal "XX001"
                 "XID wraparound failed; the server is shutting down"))

(defun signal-xid-wraparound-shutdown ()
  (when (fboundp 'request-fast-shutdown)
    (funcall (symbol-function 'request-fast-shutdown)))
  (values))

(defun fail-xid-wraparound (condition shutdown-function)
  (let ((first-failure nil))
    (sb-thread:with-mutex (*xid-wrap-lock*)
      (unless *xid-wrap-failure*
        (setf *xid-wrap-failure* condition
              first-failure t))
      ;; Keep the gate closed. The marker and next-XID boundary make recovery
      ;; safe after restart, even if heap or CLOG changes were only partial.
      (setf *xid-wrap-pending* t)
      (sb-thread:condition-broadcast *xid-wrap-cv*))
    (when first-failure
      (ignore-errors (funcall shutdown-function))))
  (values))

(defun complete-xid-wraparound
    (&key (load-marker-function #'load-xid-wrap-marker)
       (save-next-xid-function #'save-next-xid)
       (save-marker-function #'save-xid-wrap-marker)
       (wait-for-xids-function #'wait-for-no-active-xids)
       (wait-for-snapshots-function #'wait-for-no-active-snapshots)
       (freeze-function #'freeze-all-heap-xids)
       (reset-clog-function #'clog-reset)
       (delete-marker-function #'delete-xid-wrap-marker)
       (shutdown-function #'signal-xid-wraparound-shutdown))
  "Freeze the previous XID cycle before reusing its 32-bit tuple IDs."
  (let ((completed nil))
    (handler-case
        (sb-thread:with-mutex (*xid-wrap-work-lock*)
          (sb-thread:with-mutex (*xid-wrap-lock*)
            (when *xid-wrap-failure*
              (xid-wraparound-failed-error)))
          (let* ((boundary (sb-thread:with-mutex (*xid-lock*) *next-xid*))
                 (marker (funcall load-marker-function))
                 (needed (or marker
                             (and (first-normal-xid-of-epoch-p boundary)
                                  (not (eql boundary
                                            *xid-wrap-completed-through*))))))
            (when needed
              (unless (first-normal-xid-of-epoch-p boundary)
                (error "XID wrap marker does not match the next FullTransactionId"))
              (when (and marker (/= marker boundary))
                (error "XID wrap marker ~a does not match next XID ~a"
                       marker boundary))
              (sb-thread:with-mutex (*xid-wrap-lock*)
                (setf *xid-wrap-pending* t))
              ;; Persist the boundary before publishing the marker. No XID from
              ;; the new epoch is handed out before recovery can finish.
              (sb-thread:with-mutex (*xid-lock*)
                (setf *xid-batch-end* boundary)
                (funcall save-next-xid-function))
              (unless marker
                (funcall save-marker-function boundary))
              (funcall wait-for-xids-function)
              (funcall wait-for-snapshots-function)
              (funcall freeze-function
                       (1- (floor boundary +xid-cycle+)))
              (check-for-interrupts)
              (funcall reset-clog-function)
              (check-for-interrupts)
              (funcall delete-marker-function)
              (sb-thread:with-mutex (*xid-wrap-lock*)
                (setf *xid-wrap-completed-through* boundary
                      *xid-wrap-pending* nil)
                (sb-thread:condition-broadcast *xid-wrap-cv*))
              (setf completed t))))
      (error (condition)
        (fail-xid-wraparound condition shutdown-function)
        (if *backend*
            (xid-wraparound-failed-error)
            (error condition))))
    ;; If cancellation arrives after the durable transition, the wrap is safe;
    ;; report it as an ordinary canceled statement after opening the gate.
    (when completed
      (check-for-interrupts))
    (values)))

(defun backend-xid-wrap-resource-free-p (backend)
  "Return true if BACKEND holds no resource that wraparound must drain.

Both wraparound waiting and coordinator selection use this test.  A backend
holding a portal snapshot must be allowed to run so it can release its pin."
  (and (null (backend-xid backend))
       (null (backend-virtual-reader-xid backend))
       (null (backend-transaction-snapshot backend))
       (not (and (fboundp 'extended-backend-holds-snapshot-p)
                 (funcall 'extended-backend-holds-snapshot-p backend)))))

(defun wait-for-xid-wraparound (backend &optional ready-function)
  (loop
    do (sb-thread:with-mutex (*xid-wrap-lock*)
         (when *xid-wrap-failure*
           (xid-wraparound-failed-error))
         ;; Real XIDs, virtual IDs and retained snapshots must drain before
         ;; wraparound. Let their owners run so they can end; ASSIGN-XID still
         ;; refuses a new writer at the epoch edge.
         (if (and *xid-wrap-pending*
                  (backend-xid-wrap-resource-free-p backend))
             (with-backend-wait-state (backend backend-wraparound-waiting t)
               (check-for-interrupts)
               (condition-wait-with-timeout *xid-wrap-cv* *xid-wrap-lock*))
             (return-from wait-for-xid-wraparound
               (if ready-function
                   (funcall ready-function)
                   (values)))))
       (values)))

(defun prepare-xid-wraparound-for-statement (backend)
  ;; Do not make a transaction with a real XID, virtual reader ID or retained
  ;; snapshot coordinate wraparound: it would wait for its own resources.
  (when (backend-xid-wrap-resource-free-p backend)
    (let ((should-coordinate
            (sb-thread:with-mutex (*xid-wrap-lock*)
              (and (not *xid-wrap-pending*)
                   (not *xid-wrap-failure*)))))
      (when should-coordinate
        (complete-xid-wraparound))))
  (wait-for-xid-wraparound backend))

(defun begin-statement-snapshot (backend curcid registration)
  (let ((snapshot
          (wait-for-xid-wraparound
           backend
           (lambda ()
             (make-snapshot curcid :register registration)))))
    (wal-test-pause-once :after-statement-snapshot)
    snapshot))

(defun xid-check-allocation-under-lock (oldest-relfrozenxid)
  ;; A waiter may have passed its first health check before another writer failed.
  (when *wal-ready* (wal-check-healthy))
  (when (> *next-xid* +max-xid+)
    (ereport-fatal "54000" "transaction ID space exhausted"))
  (when (and oldest-relfrozenxid
             (>= (- *next-xid* oldest-relfrozenxid) +manual-vacuum-xid-stop-age+))
    (ereport "55000" "transaction ID allocation stopped because manual VACUUM is required"))
  (when (and (first-normal-xid-of-epoch-p *next-xid*)
             (not (eql *next-xid* *xid-wrap-completed-through*)))
    (ereport "55000" "transaction ID wraparound is waiting for a safe tuple freeze; retry the statement")))

(defun xid-check-allocation-owner (owner)
  "Require an optional allocation owner to be a backend without an existing XID."
  (when owner
    (unless (backend-p owner) (error "invalid XID allocation backend"))
    (when (backend-xid owner) (error "backend already owns a transaction ID")))
  (values))

(defun xid-allocate-under-lock (&optional owner)
  "Assign an XID and fix its logical capture decision under the XID lock.
Logical level always captures. At replica level, live slots, an arming slot or
an unknown live-slot count require capture. The decision stays with the XID as
its mutations arrive.
When OWNER is supplied, publish its XID with the active registration and next
counter under the XID lock and interrupt mask, so cleanup can find it."
  (xid-check-allocation-owner owner)
  (let* ((xid *next-xid*)
         (next (advance-full-xid xid))
         (link (list xid))
         (capture (or (eq *wal-level* :logical)
                      (not (eql *logical-capture-live-slots* 0))
                      (plusp (hash-table-count *logical-capture-arming*)))))
    (sb-sys:without-interrupts
      (setf (gethash xid *logical-capture-xid-modes*) (if capture :capture :skip)
            (cdr link) *active-xids*
            *active-xids* link
            *next-xid* next)
      (when owner (setf (backend-xid owner) xid)))
    xid))

(defun assign-xid (&optional owner)
  "Allocate an XID, optionally publishing it to OWNER before releasing the XID lock.
Without an owner, the caller remains responsible for releasing the active XID."
  (prevent-recovery-write "transaction ID assignment")
  (when *wal-ready* (wal-check-healthy))
  (let ((oldest (catalog-oldest-relfrozenxid)))
    (sb-thread:with-mutex (*xid-lock*)
      (xid-check-allocation-owner owner)
      (xid-check-allocation-under-lock oldest)
      (when (< *next-xid* *xid-batch-end*)
        (return-from assign-xid (xid-allocate-under-lock owner)))
      (unless *wal-ready*
        ;; Standalone storage use retains the pre-WAL file durability contract.
        (let ((*xid-batch-end* (xid-reservation-end *next-xid*))) (save-next-xid))
        (setf *xid-batch-end* (xid-reservation-end *next-xid*))
        (return-from assign-xid (xid-allocate-under-lock owner))))
    ;; Never acquire WAL while holding XID; checkpoints take WAL then XID.
    (xid-assign-with-wal-reservation oldest owner)))

(defun relation-build-group-has-user-p (token xid writers-p readers-p)
  (let ((group (gethash token *relation-build-groups*)))
    (and group
         (some (lambda (relfilenode)
                 (or (and writers-p
                          (member xid (gethash relfilenode *relation-writers*)
                                  :test #'=))
                     (and readers-p
                          (member xid (gethash relfilenode *relation-readers*)
                                  :test #'=))))
               (first group)))))

(defun relation-writer-admissible-p (relfilenode xid)
  (let* ((token (gethash relfilenode *relation-builders*))
         (group (gethash token *relation-build-groups*)))
    (or (null token)
        (member xid (gethash relfilenode *relation-writers*) :test #'=)
        (relation-build-group-has-user-p token xid t (and group (second group))))))

(defun logical-note-xid-mutation-needs-capture-p (xid)
  "Record the first row or transactional utility change and return capture mode."
  (sb-thread:with-mutex (*xid-lock*)
    (case (gethash xid *logical-capture-xid-modes*)
      ((:skip :skip-written)
       (setf (gethash xid *logical-capture-xid-modes*) :skip-written)
       nil)
      (otherwise
       (setf (gethash xid *logical-capture-xid-modes*) :capture-written)
       t))))

(defun register-relation-accesses (write-relfilenodes read-relfilenodes xid)
  "Admit a mutation and all subquery reads atomically, with writes taking precedence.
Reserve the complete set before waiting; transaction cleanup owns granted access."
  (when (and write-relfilenodes *backend* (eql xid (backend-xid *backend*)))
    (logical-note-xid-mutation-needs-capture-p xid))
  (let* ((writes (sort (remove-duplicates (copy-list write-relfilenodes) :test #'=) #'<))
         (reads (sort (set-difference (copy-list read-relfilenodes) writes :test #'=) #'<)))
    (call-with-relation-resource-request
     (append writes reads) (make-relation-gate-token xid)
     (lambda (request)
       (sb-thread:with-mutex (*relation-lock-mutex*)
         (loop
           (when (and (every (lambda (node) (relation-writer-admissible-p node xid)) writes)
                      (every (lambda (node) (relation-reader-admissible-p node xid)) reads))
             (sb-sys:without-interrupts
               (remhash request *relation-resource-requests*)
               (dolist (node writes)
                 (pushnew xid (gethash node *relation-writers*) :test #'=))
               (dolist (node reads)
                 (pushnew xid (gethash node *relation-readers*) :test #'=)))
             (return))
           (check-for-interrupts)
           (condition-wait-with-timeout *relation-lock-cv* *relation-lock-mutex*))))))
  (values))

(defun register-relation-writer (relfilenode xid)
  "Register XID as a writer, waiting if an index build owns the relation."
  (register-relation-accesses (list relfilenode) nil xid))

(defun relation-reader-admissible-p (relfilenode xid)
  (let ((readers (gethash relfilenode *relation-readers*))
        (token (gethash relfilenode *relation-reader-blockers*)))
    (or (null token)
        (member xid readers :test #'=)
        (member xid (gethash relfilenode *relation-writers*) :test #'=)
        ;; Multi-relation DDL waits for existing users on every member. Let
        ;; those transactions finish their work on the rest of that group.
        (relation-build-group-has-user-p token xid t t))))

(defun register-relation-readers (relfilenodes xid)
  "Register every relation together, waiting without partial ownership."
  (register-relation-accesses nil relfilenodes xid))

(defun register-relation-reader (relfilenode xid)
  "Register XID as a reader, waiting if relation DDL excludes readers."
  (register-relation-readers (list relfilenode) xid))

(defun relation-build-user (relfilenodes own-xid wait-readers-p)
  (some (lambda (relfilenode)
          (or (let ((token (gethash relfilenode *relation-maintainers*)))
                (when token (list :maintenance token)))
              (find-if (lambda (xid)
                         (or (null own-xid) (/= xid own-xid)))
                       (gethash relfilenode *relation-writers*))
              (when wait-readers-p
                (find-if (lambda (xid)
                           (or (null own-xid) (/= xid own-xid)))
                         (gethash relfilenode *relation-readers*)))))
        relfilenodes))

(defmacro with-acquired-relation-locks
    ((relations token acquire-function release-function) &body body)
  "Protect ownership before ACQUIRE-FUNCTION can publish any relation gates.
ACQUIRE-FUNCTION receives a callback accepting the owned relation set and token.
It must call that callback with interrupts disabled as part of publication. Waiting
and BODY remain interruptible; RELEASE-FUNCTION runs after every exit."
  (let ((acquire (gensym "ACQUIRE-"))
        (release (gensym "RELEASE-"))
        (owned (gensym "OWNED-"))
        (published-relations (gensym "PUBLISHED-RELATIONS-"))
        (published-token (gensym "PUBLISHED-TOKEN-")))
    `(let ((,relations nil)
           (,token nil)
           (,owned nil)
           (,acquire ,acquire-function)
           (,release ,release-function))
       (declare (ignorable ,relations ,token))
       (unwind-protect
            (progn
              (funcall ,acquire
                       (lambda (,published-relations ,published-token)
                         (setf ,relations ,published-relations
                               ,token ,published-token
                               ,owned t)))
              ,@body)
         (sb-sys:without-interrupts
           (when ,owned
             (funcall ,release ,relations ,token)))))))

(defvar *transaction-exclusive-relation-locks*
  (make-hash-table :test #'eq :synchronized t))

(defun transaction-held-relation-lock-p (node backend)
  (some (lambda (entry) (member node (car entry) :test #'=))
        (gethash backend *transaction-exclusive-relation-locks*)))

(defun acquire-transaction-relation-locks (nodes)
  "Keep exclusive relation admission gates until this transaction finishes.

Wait for the complete set before registering the transaction as a writer.
Publishing writers while waiting for gates could make competing requests wait
for each other.  Gate ownership and writer registrations are published together
under the relation mutex, with interrupts disabled."
  (let* ((backend *backend*)
         (xid (get-current-xid))
         (requested (remove-if (lambda (node)
                                 (transaction-held-relation-lock-p node backend))
                               nodes)))
    (when requested
      (logical-note-xid-mutation-needs-capture-p xid)
      (acquire-relation-build-locks
       requested xid t
       (lambda (relations token)
         (push (cons relations token)
               (gethash backend *transaction-exclusive-relation-locks*))
         (dolist (node relations)
           (pushnew xid (gethash node *relation-writers*) :test #'=))))))
  (values))

(defun release-transaction-relation-locks (backend)
  (let ((entries (gethash backend *transaction-exclusive-relation-locks*)))
    (dolist (entry entries)
      (release-relation-build-locks (car entry) (cdr entry)))
    (remhash backend *transaction-exclusive-relation-locks*))
  (values))

(defun acquire-relation-build-locks
    (relfilenodes &optional own-xid wait-readers-p on-acquired)
  "Block new users as one group, then wait for all active users to finish.
Gates already held by this transaction are omitted to avoid waiting on itself.
ON-ACQUIRED publishes cleanup ownership as gates become visible. It is
called under the relation mutex with interrupts disabled. Raw callers retain
responsibility for release after successful return. Without ON-ACQUIRED they
must protect the return-to-owner handoff from interrupts; prefer the scoped macros."
  (let ((relfilenodes (sort (remove-duplicates (remove-if
                                                (lambda (node)
                                                  (and *backend*
                                                       (transaction-held-relation-lock-p node *backend*)))
                                                (copy-list relfilenodes)) :test #'=)
                            #'<))
        (token (make-relation-gate-token (or own-xid *backend*)))
        (published nil)
        (completed nil))
    (when (null relfilenodes)
      (return-from acquire-relation-build-locks nil))
    (call-with-relation-resource-request
     relfilenodes token
     (lambda (request)
       (unwind-protect
            (progn
              (sb-thread:with-mutex (*relation-lock-mutex*)
                (loop while (some (lambda (relfilenode)
                                    (gethash relfilenode *relation-builders*))
                                  relfilenodes)
                      do (check-for-interrupts)
                         (condition-wait-with-timeout *relation-lock-cv*
                                                      *relation-lock-mutex*))
                (sb-sys:without-interrupts
                  (remhash request *relation-resource-requests*)
                  ;; Own the complete set before any partial map update can fail.
                  (setf published t)
                  (setf (gethash token *relation-build-groups*)
                        (list relfilenodes wait-readers-p))
                  (dolist (relfilenode relfilenodes)
                    (setf (gethash relfilenode *relation-builders*) token)
                    (when wait-readers-p
                      (setf (gethash relfilenode *relation-reader-blockers*) token)))
                  (when on-acquired (funcall on-acquired relfilenodes token))))
              (loop
                (let ((user (sb-thread:with-mutex (*relation-lock-mutex*)
                              (relation-build-user relfilenodes own-xid
                                                   wait-readers-p))))
                  (unless user (return))
                  (if (and (consp user) (eq (first user) :maintenance))
                      (sb-thread:with-mutex (*relation-lock-mutex*)
                        (condition-wait-with-timeout *relation-lock-cv*
                                                     *relation-lock-mutex*))
                      (wait-for-xid user))))
              (setf completed t)
              token)
         ;; Also protect unscoped callers and non-error exits during acquisition.
         ;; Release is token-checked and idempotent if the outer scope also owns it.
         (when (and published (not completed))
           (sb-sys:without-interrupts
             (release-relation-build-locks relfilenodes token))))))))

(defun release-relation-build-locks (relfilenodes token)
  (sb-thread:with-mutex (*relation-lock-mutex*)
    (dolist (relfilenode relfilenodes)
      (when (eq (gethash relfilenode *relation-builders*) token)
        (remhash relfilenode *relation-builders*)
        (when (eq (gethash relfilenode *relation-reader-blockers*) token)
          (remhash relfilenode *relation-reader-blockers*))))
    (when token
      (remhash token *relation-build-groups*))
    (sb-thread:condition-broadcast *relation-lock-cv*))
  (values))

(defun acquire-relation-build-lock
    (relfilenode &optional own-xid wait-readers-p on-acquired)
  (acquire-relation-build-locks (list relfilenode) own-xid wait-readers-p on-acquired))

(defun release-relation-build-lock (relfilenode token)
  (release-relation-build-locks (list relfilenode) token))

(defmacro with-relation-build-lock ((relfilenode &optional wait-readers-p) &body body)
  `(with-relation-build-locks ((list ,relfilenode) ,wait-readers-p)
     ,@body))

(defmacro with-relation-build-locks ((relfilenodes &optional wait-readers-p) &body body)
  "Acquire relation build gates as one group and release them on exit."
  (let ((requested (gensym "REQUESTED-"))
        (rels (gensym "RELATIONS-"))
        (xid (gensym "OWN-XID-"))
        (token (gensym "BUILD-TOKEN-"))
        (wait (gensym "WAIT-READERS-")))
    `(let* ((,requested ,relfilenodes)
            (,xid (and *backend* (backend-xid *backend*)))
            (,wait ,wait-readers-p))
       (with-acquired-relation-locks
           (,rels ,token
                  (lambda (publish)
                    (acquire-relation-build-locks ,requested ,xid ,wait publish))
                  #'release-relation-build-locks)
         ,@body))))

(defun acquire-relation-maintenance-locks (relfilenodes &optional on-acquired)
  "Serialize VACUUM on relations while allowing registered DML to proceed.
ON-ACQUIRED publishes cleanup ownership atomically with the maintenance gates.
Without it raw callers must protect the return-to-owner handoff from interrupts;
prefer WITH-RELATION-MAINTENANCE-LOCKS."
  (let ((relfilenodes (sort (remove-duplicates (copy-list relfilenodes) :test #'=)
                            #'<))
        (token (make-relation-gate-token *backend*))
        (published nil)
        (completed nil))
    (when (null relfilenodes)
      (return-from acquire-relation-maintenance-locks nil))
    (call-with-relation-resource-request
     relfilenodes token
     (lambda (request)
       (unwind-protect
            (progn
              (sb-thread:with-mutex (*relation-lock-mutex*)
                (loop
                  (unless (some (lambda (relfilenode)
                                  (or (gethash relfilenode *relation-builders*)
                                      (gethash relfilenode *relation-maintainers*)))
                                relfilenodes)
                    (sb-sys:without-interrupts
                      (remhash request *relation-resource-requests*)
                      (setf published t)
                      (dolist (relfilenode relfilenodes)
                        (setf (gethash relfilenode *relation-maintainers*) token))
                      (when on-acquired (funcall on-acquired relfilenodes token)))
                    (return))
                  (check-for-interrupts)
                  (condition-wait-with-timeout *relation-lock-cv*
                                               *relation-lock-mutex*)))
              (setf completed t)
              (values relfilenodes token))
         (when (and published (not completed))
           (sb-sys:without-interrupts
             (release-relation-maintenance-locks relfilenodes token))))))))

(defun release-relation-maintenance-locks (relfilenodes token)
  (sb-thread:with-mutex (*relation-lock-mutex*)
    (dolist (relfilenode relfilenodes)
      (when (eq (gethash relfilenode *relation-maintainers*) token)
        (remhash relfilenode *relation-maintainers*)))
    (sb-thread:condition-broadcast *relation-lock-cv*))
  (values))

(defun acquire-relation-maintenance-lock (relfilenode &optional on-acquired)
  "Serialize VACUUM on one relation while allowing registered DML to proceed."
  (multiple-value-bind (relfilenodes token)
      (acquire-relation-maintenance-locks (list relfilenode) on-acquired)
    (declare (ignore relfilenodes))
    token))

(defun release-relation-maintenance-lock (relfilenode token)
  (release-relation-maintenance-locks (list relfilenode) token))

(defmacro with-relation-maintenance-locks ((relfilenodes) &body body)
  (let ((requested (gensym "REQUESTED-"))
        (rels (gensym "RELATIONS-"))
        (token (gensym "MAINTENANCE-TOKEN-")))
    `(let ((,requested ,relfilenodes))
       (with-acquired-relation-locks
           (,rels ,token
                  (lambda (publish)
                    (acquire-relation-maintenance-locks ,requested publish))
                  #'release-relation-maintenance-locks)
         ,@body))))

(defmacro with-relation-maintenance-lock ((relfilenode) &body body)
  `(with-relation-maintenance-locks ((list ,relfilenode))
     ,@body))

(defun release-relation-writer-xid (xid &optional backend)
  "Release XID's relation accesses and transaction registration.

For a virtual reader, BACKEND identifies the owner and its cached ID is cleared
under the relation lock.  Real XIDs are removed from the active set under the
XID lock.  Relation waiters are notified after the registrations are removed."
  ;; Hold the relation lock before the XID lock so the relation gate stays
  ;; closed until a real XID is no longer in the active set.
  (sb-thread:with-mutex (*relation-lock-mutex*)
    (when backend
      (unless (eql xid (backend-virtual-reader-xid backend))
        (error "virtual reader cleanup identity mismatch"))
      (setf (backend-virtual-reader-xid backend) nil))
    (remhash xid *recovery-reader-xids*)
    (sb-thread:with-mutex (*xid-lock*)
      (setf *active-xids* (delete xid *active-xids* :test #'=))
      ;; WAL snapshot-boundary state shares the XID lock so removal from the
      ;; active set and removal of its retained LSN remain one atomic change.
      (wal-forget-xid-under-xid-lock xid)
      (remhash xid *logical-capture-xid-modes*)
      (remhash xid *logical-binary-required-xids*))
    (dolist (registrations (list *relation-writers* *relation-readers*))
      (let ((empty-relations '()))
        (maphash (lambda (relfilenode xids)
                   (let ((remaining (delete xid xids :test #'=)))
                     (if remaining
                         (setf (gethash relfilenode registrations) remaining)
                         (push relfilenode empty-relations))))
                 registrations)
        (dolist (relfilenode empty-relations)
          (remhash relfilenode registrations))))
    (sb-thread:condition-broadcast *relation-lock-cv*))
  (values))

(defvar *on-conflict-row-lock-mutex* (sb-thread:make-mutex :name "pg conflict row locks"))

(defvar *on-conflict-row-locks* (make-hash-table :test #'equal))

(defvar *on-conflict-xid-row-locks* (make-hash-table :test #'eql))

(defun on-conflict-row-lock-owner (relfilenode block offnum xid)
  (sb-thread:with-mutex (*on-conflict-row-lock-mutex*)
    (let* ((key (list relfilenode block offnum))
           (owner (gethash key *on-conflict-row-locks*)))
      (when (and owner (/= owner xid))
        (if (xid-active-p owner) owner
            (progn (remhash key *on-conflict-row-locks*) nil))))))

(defun claim-on-conflict-row-lock (relfilenode block offnum xid)
  ;; Return an active owner to wait for, or record ownership until XID release.
  ;; The caller waits only after dropping the storage locks used for arbitration.
  (sb-thread:with-mutex (*on-conflict-row-lock-mutex*)
    (let* ((key (list relfilenode block offnum))
           (owner (gethash key *on-conflict-row-locks*)))
      (if (and owner (/= owner xid) (xid-active-p owner)) owner
          (progn
            (setf (gethash key *on-conflict-row-locks*) xid)
            (pushnew key (gethash xid *on-conflict-xid-row-locks*) :test #'equal)
            nil)))))

(defun release-on-conflict-row-locks (xid)
  (sb-thread:with-mutex (*on-conflict-row-lock-mutex*)
    (dolist (key (gethash xid *on-conflict-xid-row-locks*))
      (when (eql xid (gethash key *on-conflict-row-locks*))
        (remhash key *on-conflict-row-locks*)))
    (remhash xid *on-conflict-xid-row-locks*)))

(defun release-xid (xid &optional backend)
  "Release XID and wake transaction waiters after its registrations are gone.

Pass BACKEND when releasing its virtual reader ID."
  (release-relation-writer-xid xid backend)
  (release-on-conflict-row-locks xid)
  (forget-combo-command-ids xid)
  (broadcast-xact)
  (values))

(defun xid-active-p (xid)
  (sb-thread:with-mutex (*xid-lock*)
    (and (member xid *active-xids* :test #'=) t)))

(defun relation-user-active-p (xid)
  "Check a virtual reader in its registry or a real XID in the active set."
  (if (minusp xid)
      (sb-thread:with-mutex (*relation-lock-mutex*)
        (not (null (gethash xid *recovery-reader-xids*))))
      (xid-active-p xid)))

(defun wait-for-xid (xid)
  "Block until XID ends. Called without the write lock held."
  (with-backend-wait-state (*backend* backend-waiting-xid xid)
    (loop
      (unless (relation-user-active-p xid) (return))
      (check-for-interrupts)
      (sb-thread:with-mutex (*xact-cv-mutex*)
        (when (relation-user-active-p xid)
          (condition-wait-with-timeout *xact-cv* *xact-cv-mutex*)))))
  (values))

(defun logical-capture-xid-needed-p (xid)
  "Return XID's capture decision, conservatively capturing unknown IDs."
  (sb-thread:with-mutex (*xid-lock*)
    (not (member (gethash xid *logical-capture-xid-modes* :capture)
                 '(:skip :skip-written)))))

(defun wal-refresh-logical-capture-slot-mirror-under-wal-lock ()
  "Publish the live slot count under the established WAL-to-XID lock order."
  (let ((count (loop for slot being the hash-values of *wal-replication-slots*
                     count (and (wal-logical-slot-p slot)
                                (not (eq (wal-replication-slot-wal-status slot) :lost))))))
    (sb-thread:with-mutex (*xid-lock*)
      (setf *logical-capture-live-slots* count)))
  (values))

(defun call-with-logical-capture-armed (function)
  "Enable capture for new XIDs, then drain older uncaptured XIDs before FUNCTION."
  (let ((token (gensym "LOGICAL-CAPTURE-ARM-")) (old-xids nil) (registered nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (sb-thread:with-mutex (*xid-lock*)
               ;; Reads allocate XIDs in this server. A known clean XID has no
               ;; prefix to lose and can begin capturing before the boundary.
               (when (and *backend* (backend-xid *backend*))
                 (let* ((xid (backend-xid *backend*))
                        (mode (gethash xid *logical-capture-xid-modes*)))
                   (case mode
                     (:skip (setf (gethash xid *logical-capture-xid-modes*) :capture))
                     (:capture nil)
                     (otherwise
                      (ereport "25001"
                               "cannot create logical replication slot in transaction that has performed writes")))))
               (setf (gethash token *logical-capture-arming*) t
                     registered t
                     old-xids (loop for xid in *active-xids*
                                    when (member (gethash xid *logical-capture-xid-modes* :capture) '(:skip :skip-written))
                                      collect xid)))
             (sb-sys:with-local-interrupts
               ;; A retained schema lease can prevent an old writer from ending.
               (when (and old-xids
                          (or (backend-holds-schema-read-lock-p)
                              *schema-read-lock-held* *schema-write-lock-held*))
                 (ereport "55000" "cannot wait for uncaptured transactions while holding a schema lock"))
               (dolist (xid old-xids) (wait-for-xid xid))
               (funcall function)))
        (when registered
          (sb-thread:with-mutex (*xid-lock*)
            (remhash token *logical-capture-arming*)))))))

(declaim (ftype function running-xacts-check-state))

(defun make-snapshot-under-xid-lock (curcid &key register)
  "Build a snapshot while the caller holds *XID-LOCK*.

During recovery, copy complete applied source membership into XIP after
checking its next XID and replay position against the acquisition boundary.
Otherwise use local active XIDs and leave source coverage incomplete.  The
snapshot retains its coverage and XIP for its lifetime."
  (when (and register (not (snapshot-registration-p register)))
    (error "REGISTER must be a snapshot-registration token or NIL"))
  (let* ((source (and (recovery-in-progress-p) (getf *physical-recovery-state* :source-xids)))
         (covered (and source (getf source :complete)))
         (xmax *next-xid*))
    (when covered
      (running-xacts-check-state source)
      (unless (and (= xmax (getf source :next-xid))
                   (eql *last-replay-lsn* (getf source :through)))
        (error "standby snapshot source membership differs from its applied boundary")))
    (let* ((xip (copy-list (if covered (getf source :running) *active-xids*)))
           (xmin (if xip (reduce #'min xip) xmax))
           (snapshot (make-snapshot-struct :xmin xmin :xmax xmax :xip xip :curcid curcid
                                           :source-xids-complete-p (not (null covered)))))
      (when register (register-active-snapshot snapshot register))
      snapshot)))

(defun make-snapshot (curcid &key register)
  (sb-thread:with-mutex (*xid-lock*)
    (make-snapshot-under-xid-lock curcid :register register)))

(defvar *wal-test-after-snapshot-boundary-registration-hook* nil)

(defun wal-capture-snapshot-boundary-under-writer-lock
    (curcid register &key after-capture)
  "Capture a registered MVCC snapshot and its matching WAL position.

The caller holds *WRITE-LOCK*. This establishes the same brief writer barrier
used by logical slot startpoint selection in PostgreSQL; holding WAL before XID
publication makes the snapshot and durable WAL end one boundary. The returned
restart LSN reaches back to the earliest page batch that contains an XID still
active in the snapshot. Its registration pins that WAL position until release.
AFTER-CAPTURE runs under *WAL-LOCK* but after *XID-LOCK* is released."
  (unless (snapshot-registration-p register)
    (error "WAL snapshot boundaries require a snapshot-registration token"))
  (unless *wal-ready*
    (ereport "55000" "cannot capture a replication snapshot before WAL startup"))
  (sb-thread:with-mutex (*wal-lock*)
    (wal-check-healthy)
    (multiple-value-bind (snapshot boundary-lsn restart-lsn)
        (sb-thread:with-mutex (*xid-lock*)
          (let* ((snapshot (make-snapshot-under-xid-lock
                            curcid :register register))
                 (boundary-lsn *wal-durable-lsn*)
                 (restart-lsn boundary-lsn))
            (when *wal-test-after-snapshot-boundary-registration-hook*
              (funcall *wal-test-after-snapshot-boundary-registration-hook*
                       register))
            (dolist (xid (snapshot-xip snapshot))
              (let ((first-lsn (gethash xid *wal-xid-first-lsn*)))
                (when (and first-lsn (< first-lsn restart-lsn))
                  (setf restart-lsn first-lsn))))
            ;; Keep the returned restart point alive after active XIDs finish.
            ;; The registered snapshot owns this pin until its caller releases
            ;; the registration or a durable replication slot takes over.
            (sb-thread:with-mutex (*snapshot-count-lock*)
              (unless (and (snapshot-registration-active-p register)
                           (eq snapshot
                               (snapshot-registration-snapshot register)))
                (error "snapshot registration was released during WAL boundary capture"))
              (sb-sys:without-interrupts
                (setf (gethash register *wal-snapshot-retention-pins*)
                      restart-lsn
                      (snapshot-registration-wal-retention-lsn register)
                      restart-lsn)))
            (values snapshot boundary-lsn restart-lsn)))
      ;; Checkpoint recycling sees the temporary snapshot pin until durable
      ;; slot publication completes, because WAL remains locked throughout.
      (when after-capture
        (funcall after-capture snapshot boundary-lsn restart-lsn))
      (values snapshot boundary-lsn restart-lsn))))

(declaim (ftype function catalog-sequence-bootstrap-snapshot))

(defun wal-capture-logical-snapshot-boundary-under-writer-lock
    (backend registration database-oid)
  "Capture sequence state under the same writer barrier as the slot boundary.

Sequence state is not MVCC. Copying it beside the boundary prevents a value
advanced during table copy from being installed before the older WAL events
that describe that advance, matching pg_dump's separate sequence-state phase.
Register the logical event interest before returning, while writers are still
excluded, so a sequence advance before slot publication remains streamable."
  (multiple-value-bind (snapshot boundary-lsn restart-lsn)
      (wal-capture-snapshot-boundary-under-writer-lock
       (backend-command-id backend) registration)
    (setf (snapshot-sequence-database-oid snapshot)
          (database-oid (get-database (backend-database backend)))
          (snapshot-sequence-states snapshot)
          (catalog-sequence-bootstrap-snapshot
           (get-database (backend-database backend)))
          (snapshot-sequence-state-captured-p snapshot) t)
    (wal-register-logical-catalog-event-interest-under-writer-lock
     registration database-oid)
    (values snapshot boundary-lsn restart-lsn)))

(defun wal-publish-replication-slot-under-wal-lock
    (name boundary-lsn restart-lsn register active-owner
     &optional active-xids database-oid (plugin "pg_lisp"))
  "Persist and publish a slot while *WAL-LOCK* protects its retention point."
  (sb-thread:with-mutex (*xid-lock*)
    (unless (plusp (hash-table-count *logical-capture-arming*))
      (error "logical slot publication requires an armed capture gate"))
    (when (loop for xid in *active-xids*
                  thereis (member (gethash xid *logical-capture-xid-modes* :capture) '(:skip :skip-written)))
      (error "logical slot publication requires uncaptured XIDs to drain")))
  (unless (wal-valid-replication-slot-name-p name)
    (ereport "42602" "invalid replication slot name \"~a\"" name))
  (unless (and (snapshot-registration-p register)
               (snapshot-registration-active-p register)
               (nth-value 1 (gethash register *wal-snapshot-retention-pins*)))
    (error "a replication slot must inherit an active snapshot WAL pin"))
  (when (gethash name *wal-replication-slots*)
    (ereport "42710" "replication slot \"~a\" already exists" name))
  (wal-check-replication-slot-capacity)
  (let ((slot (make-wal-replication-slot
               :name name :restart-lsn restart-lsn
               :consistent-lsn boundary-lsn :plugin plugin
               :confirmed-flush-lsn (when (string= plugin "pgoutput") boundary-lsn)
               :database-oid database-oid
               :active-owner active-owner
               :active-owner-backend (and active-owner *backend*))))
    (wal-slot-set-initial-profile slot *wal-new-slot-profile*)
    ;; Reserve the in-memory pin before slot-file I/O. If atomic replace has
    ;; renamed the file but directory fsync fails, retain the pin: startup may
    ;; still observe that file and will require its WAL.
    (unwind-protect
         (progn
           (setf (gethash name *wal-replication-slots*) slot)
           (wal-slot-seed-active-xids-under-wal-lock slot active-xids)
           (wal-save-replication-slot slot)
           ;; The durable slot now owns the same restart horizon; dropping
           ;; this temporary snapshot pin keeps one retention owner and lets
           ;; the configured slot cap invalidate it atomically at checkpoint.
           (release-snapshot-registration-wal-pin-under-wal-lock register))
      (sb-sys:without-interrupts
        (unless (handler-case
                    (probe-file (wal-replication-slot-path name))
                  (error () t))
          (remhash name *wal-replication-slots*))
        (wal-refresh-logical-capture-slot-mirror-under-wal-lock)))
    slot))

(defun wal-create-replication-slot-under-writer-lock
    (name curcid register &optional active-owner database-oid)
  "Persist a named restart point before exposing its captured snapshot."
  (unless (wal-valid-replication-slot-name-p name)
    (ereport "42602" "invalid replication slot name \"~a\"" name))
  (let ((slot nil))
    (multiple-value-bind (snapshot boundary-lsn restart-lsn)
        (wal-capture-snapshot-boundary-under-writer-lock
         curcid register
         :after-capture
         (lambda (captured boundary restart)
           ;; AFTER-CAPTURE runs under *WAL-LOCK*: persist first, then publish
           ;; the in-memory retention entry seen by future checkpoints.
           (setf slot
                 (wal-publish-replication-slot-under-wal-lock
                  name boundary restart register active-owner
                  (snapshot-xip captured) database-oid))))
      (values snapshot boundary-lsn restart-lsn slot))))

(defun wal-publish-captured-replication-slot
    (name boundary-lsn restart-lsn register
     &optional active-owner active-xids database-oid (plugin "pg_lisp"))
  "Publish a captured slot after the wraparound gate has been released.

The active snapshot registration keeps RESTART-LSN pinned if a checkpoint runs
between snapshot capture and this durable publication."
  (sb-thread:with-mutex (*wal-lock*)
    (wal-check-healthy)
    (let ((slot
            (wal-publish-replication-slot-under-wal-lock
             name boundary-lsn restart-lsn register active-owner active-xids
             database-oid plugin)))
      ;; Switch atomically from the provisional snapshot interest to the durable
      ;; slot visible to catalog-event writers under this same WAL lock.
      (remhash register *wal-logical-catalog-event-interests*)
      slot)))

(declaim
 (ftype function
        register-exported-logical-snapshot
        remove-exported-logical-snapshot-token
        release-exported-logical-snapshot-for-slot))

(defun new-exported-logical-snapshot-token ()
  (let ((bytes (make-array 16 :element-type '(unsigned-byte 8))))
    (with-open-file (input #p"/dev/urandom" :element-type '(unsigned-byte 8))
      (unless (= (length bytes) (read-sequence bytes input))
        (error "could not read snapshot token entropy")))
    (format nil "pg_lisp_~{~2,'0X~}" (coerce bytes 'list))))

(defun export-transaction-snapshot ()
  "Export the current read-only RR snapshot until this transaction ends."
  (let* ((backend *backend*)
         (source (and backend (backend-transaction-snapshot backend)))
         (source-registration (and backend (backend-transaction-snapshot-registration backend)))
         (registration (make-snapshot-registration))
         (lease nil) (published nil) (state nil)
         (token (new-exported-logical-snapshot-token)))
    (unless (and backend (eq (backend-state backend) :in-progress)
                 (backend-explicit backend)
                 (backend-read-only backend)
                 (eq (backend-isolation-level backend) :repeatable-read)
                 source source-registration
                 (snapshot-registration-active-p source-registration)
                 (backend-transaction-schema-read-lock-held-p backend))
      (ereport "25001" "snapshot export requires a read-only REPEATABLE READ transaction"))
    (unwind-protect
         (let ((snapshot (copy-snapshot source)))
           (setf (snapshot-xip snapshot) (copy-list (snapshot-xip source))
                 (snapshot-sequence-states snapshot) (copy-tree (snapshot-sequence-states source)))
           (sb-sys:without-interrupts
             ;; The exporter already owns the read side; clone its lease without
             ;; queuing behind a writer that is waiting for that very lease.
             (sb-thread:with-mutex (*schema-lock-mutex*)
               (unless (backend-transaction-schema-read-lock-held-p backend)
                 (error "exporter schema lease expired"))
               (incf *schema-read-lock-count*)
               (setf lease t))
             (sb-thread:with-mutex (*xid-lock*)
               (register-active-snapshot snapshot registration))
             (setf state (make-exported-logical-snapshot
                          :token token :database-oid (database-oid (current-database))
                          :snapshot snapshot :registration registration
                          :schema-read-lock-held-p t))
             (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
               (when (or (gethash token *transaction-exported-snapshots*)
                         (gethash token *exported-logical-snapshots*)
                         (gethash token *expired-exported-logical-snapshots*))
                 (error "duplicate exported snapshot token"))
               (push state (gethash backend *transaction-exported-snapshot-owners*))
               (setf (gethash token *transaction-exported-snapshots*) state)
               (setf lease nil published t)))
           token)
      (unless published
        (sb-sys:without-interrupts
          (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
            (when (eq state (gethash token *transaction-exported-snapshots*))
              (remhash token *transaction-exported-snapshots*))
            (when state
              (let ((remaining (delete state (gethash backend *transaction-exported-snapshot-owners*) :test #'eq)))
                (if remaining
                    (setf (gethash backend *transaction-exported-snapshot-owners*) remaining)
                    (remhash backend *transaction-exported-snapshot-owners*)))))
          (unwind-protect
               (when (snapshot-registration-active-p registration)
                 (release-snapshot-registration registration))
            (when lease (release-schema-read-lock))))))))

(defun expire-transaction-exported-snapshots (backend)
  "Expire BACKEND's tokens while retaining leases for already-imported snapshots."
  (let ((states nil))
    (sb-sys:without-interrupts
      (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
        (setf states (gethash backend *transaction-exported-snapshot-owners*))
        (remhash backend *transaction-exported-snapshot-owners*)
        (dolist (state states)
          (remhash (exported-logical-snapshot-token state) *transaction-exported-snapshots*)))
      (let ((failure nil) (pending nil))
        (dolist (state states)
          (handler-case (expire-exported-logical-snapshot-state state)
            (error (condition)
              (push state pending)
              (unless failure (setf failure condition)))))
        (when pending
          (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
            (setf (gethash backend *transaction-exported-snapshot-owners*) pending)))
        (when failure (error failure)))))
  (values))

(defun validate-standard-snapshot-use (backend)
  "Match CreateReplicationSlot's checks before retaining a transaction snapshot."
  (unless (and (backend-explicit backend) (eq (backend-state backend) :in-progress))
    (ereport "XX000" "SNAPSHOT 'use' must be called inside a transaction"))
  (unless (eq (backend-isolation-level backend) :repeatable-read)
    (ereport "XX000" "SNAPSHOT 'use' requires REPEATABLE READ isolation"))
  (unless (backend-read-only backend)
    (ereport "XX000" "SNAPSHOT 'use' requires a read-only transaction"))
  (when (or (backend-first-snapshot-set backend)
            (backend-transaction-snapshot backend)
            (backend-holds-schema-read-lock-p backend))
    (ereport "XX000" "SNAPSHOT 'use' must be called before any query")))

(defun %create-standard-logical-slot (slot-name snapshot-action)
  "Publish a confirmed slot; only USE transfers MVCC and schema ownership to SQL."
  (unless *backend* (ereport "55000" "slot creation requires an active backend"))
  (unless (member snapshot-action '(:nothing :use))
    (ereport "0A000" "standard snapshot export is not implemented"))
  (when (eq snapshot-action :use) (validate-standard-snapshot-use *backend*))
  (prepare-xid-wraparound-for-statement *backend*)
  (let* ((backend *backend*)
         (registration (make-snapshot-registration))
         (owner (gensym "STANDARD-LOGICAL-SLOT-"))
         (database-id (database-oid (get-database (backend-database backend))))
         (schema-lease nil) (installed nil) (returned nil))
    (unwind-protect
         (progn
           (when (eq snapshot-action :use)
             (acquire-schema-read-lock (lambda () (setf schema-lease t))))
           (multiple-value-bind (snapshot boundary)
               (wait-for-xid-wraparound
                backend
                (lambda ()
                  (with-write-lock
                    (multiple-value-bind (snapshot boundary restart)
                        (wal-capture-snapshot-boundary-under-writer-lock
                         (backend-command-id backend) registration)
                      ;; Writers must see the slot before they can produce WAL
                      ;; after this boundary, including XIDs absent from XIP.
                      (when *logical-test-after-snapshot-capture-hook*
                        (funcall *logical-test-after-snapshot-capture-hook*
                                 slot-name boundary))
                      (logical-test-failpoint
                       :after-snapshot-boundary-capture slot-name boundary)
                      (logical-test-failpoint
                       :before-initial-slot-publication slot-name boundary)
                      (wal-publish-captured-replication-slot
                       slot-name boundary restart registration owner
                       (snapshot-xip snapshot) database-id "pgoutput")
                      (values snapshot boundary)))))
             (logical-test-failpoint :after-initial-slot-file-publication slot-name boundary)
             (when (eq snapshot-action :use)
               (sb-sys:without-interrupts
                 (setf (backend-transaction-snapshot backend) snapshot
                       (backend-transaction-snapshot-registration backend) registration
                       (backend-transaction-schema-read-lock-held-p backend) schema-lease
                       (backend-first-snapshot-set backend) t
                       schema-lease nil installed t))
               (logical-test-failpoint :after-standard-snapshot-installation slot-name boundary))
             (wal-release-replication-slot-owner slot-name owner)
             (setf owner nil returned t)
             (values boundary nil)))
      ;; A permanent slot remains durable even when the command or its SQL
      ;; transaction fails. In particular, uncertain rename/fsync keeps its pin.
      (when owner (ignore-errors (wal-release-replication-slot-owner slot-name owner)))
      (cond
        ((and installed (not returned))
         (release-backend-transaction-snapshot backend))
        ((not installed)
         (when (snapshot-registration-active-p registration)
           (release-snapshot-registration registration))))
      (when schema-lease (release-schema-read-lock)))))

(defun create-standard-logical-slot (slot-name snapshot-action)
  (unless *backend* (ereport "55000" "slot creation requires an active backend"))
  (unless (member snapshot-action '(:nothing :use))
    (ereport "0A000" "standard snapshot export is not implemented"))
  (when (eq snapshot-action :use) (validate-standard-snapshot-use *backend*))
  (call-with-logical-capture-armed
   (lambda () (%create-standard-logical-slot slot-name snapshot-action))))

(defun %create-exported-logical-snapshot-slot (slot-name)
  "Create an unconfirmed logical slot and return its copy boundary and token.

Unlike the in-process WITH-EXPORTED-SNAPSHOT helper, this keeps the snapshot
registration and schema read lease in a process registry until the wire
copy-complete handshake. This follows snapbuild.c's exported consistent
snapshot lifecycle."
  (unless *backend*
    (ereport "55000" "logical snapshot export requires an active backend"))
  (unless (eq (backend-state *backend*) :idle)
    (ereport "25001" "cannot export a replication snapshot inside a transaction"))
  (prepare-xid-wraparound-for-statement *backend*)
  (let* ((backend *backend*)
         (registration (make-snapshot-registration))
         (owner (gensym "LOGICAL-SNAPSHOT-SLOT-"))
         (database-id
           (database-oid (get-database (backend-database backend))))
         (schema-lock-held nil)
         (published nil)
         (registered nil)
         (token-published nil)
         (returned nil)
         (snapshot nil)
         (boundary-lsn nil)
         (slot-object nil)
         (token (new-exported-logical-snapshot-token)))
    (unwind-protect
         (progn
           (acquire-schema-read-lock
            (lambda () (setf schema-lock-held t)))
           (multiple-value-bind (captured captured-boundary restart-lsn)
               (wait-for-xid-wraparound
                backend
                (lambda ()
                  (with-write-lock
                    (wal-capture-logical-snapshot-boundary-under-writer-lock
                     backend registration database-id))))
             (setf snapshot captured
                   boundary-lsn captured-boundary)
             ;; Run the transition race after capture releases *WRITE-LOCK* but
             ;; before slot publication, as the in-process copy helper does.
             (when *logical-test-after-snapshot-capture-hook*
               (funcall *logical-test-after-snapshot-capture-hook*
                        slot-name boundary-lsn))
             ;; snapbuild.c pins the consistent point before exposing its slot.
             (logical-test-failpoint
              :after-snapshot-boundary-capture slot-name boundary-lsn)
             (logical-test-failpoint
              :before-initial-slot-publication slot-name boundary-lsn)
             (setf slot-object
                   (wal-publish-captured-replication-slot
                    slot-name captured-boundary restart-lsn registration owner
                    (snapshot-xip captured) database-id)
                   published t)
             (logical-test-failpoint
              :after-initial-slot-file-publication slot-name boundary-lsn)
             (logical-test-failpoint
              :before-snapshot-token-publication slot-name boundary-lsn)
             ;; The registry owns the schema lease as soon as the token becomes
             ;; visible. Keep that ownership transfer atomic with its local flags.
             (sb-sys:without-interrupts
               (register-exported-logical-snapshot
                (make-exported-logical-snapshot
                 :slot-name slot-name :slot-object slot-object
                 :token token :database-oid database-id
                 :snapshot snapshot :registration registration
                 :schema-read-lock-held-p schema-lock-held))
               (setf token-published t
                     registered t
                     schema-lock-held nil))
             ;; As with ExportSnapshot, release slot runtime ownership before
             ;; the token is returned so the importer can connect independently.
             (wal-release-replication-slot-owner slot-name owner)
             (setf owner nil)
             (logical-test-failpoint
              :after-snapshot-token-publication slot-name boundary-lsn)
             (setf returned t)
             (values boundary-lsn token)))
      (when (and token-published (not returned))
        (ignore-errors (remove-exported-logical-snapshot-token token))
        (setf token-published nil registered nil))
      (when owner
        (ignore-errors (wal-release-replication-slot-owner slot-name owner)))
      (when (and published (not registered))
        (ignore-errors (wal-drop-replication-slot slot-name)))
      (unless registered
        (when (snapshot-registration-active-p registration)
          (ignore-errors (release-snapshot-registration registration)))
        (when schema-lock-held
          (ignore-errors (release-schema-read-lock))))
      (ignore-errors
       (wal-release-logical-catalog-event-interest registration)))))

(defun create-exported-logical-snapshot-slot (slot-name)
  (unless *backend* (ereport "55000" "logical snapshot export requires an active backend"))
  (unless (eq (backend-state *backend*) :idle)
    (ereport "25001" "cannot export a replication snapshot inside a transaction"))
  (call-with-logical-capture-armed
   (lambda () (%create-exported-logical-snapshot-slot slot-name))))

(defun import-exported-logical-snapshot (token)
  "Import TOKEN into the current read-only RR transaction as an independent pin.

The generated copier carries coverage and other value slots.  The imported
snapshot owns copies of XIP and sequence states."
  (unless (and *backend*
               (eq (backend-state *backend*) :in-progress)
               (backend-explicit *backend*)
               (eq (backend-isolation-level *backend*) :repeatable-read)
               (backend-read-only *backend*)
               (null (backend-xid *backend*))
               (null (backend-transaction-snapshot *backend*))
               (not (backend-first-snapshot-set *backend*)))
    (ereport "25001"
             "SET TRANSACTION SNAPSHOT requires an unused read-only REPEATABLE READ transaction"))
  (let ((registration nil)
        (installed nil)
        (schema-lease nil)
        (database-id (database-oid (current-database))))
    (unwind-protect
         ;; Keep token validation and publication of the independent MVCC pin
         ;; serialized with invalidation. Importers take registry -> XID ->
         ;; snapshot locks, matching PostgreSQL's exported snapshot handoff.
         (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
           (let* ((generic-state (gethash token *transaction-exported-snapshots*))
                  (state (or (gethash token *exported-logical-snapshots*) generic-state)))
             (unless (and state
                          (snapshot-registration-active-p
                           (exported-logical-snapshot-registration state)))
               (ereport "22023" "exported logical snapshot token is invalid or expired"))
             (unless (= database-id
                        (exported-logical-snapshot-database-oid state))
               (ereport "22023" "exported logical snapshot belongs to another database"))
             (when *logical-test-after-snapshot-token-lookup-hook*
               (funcall *logical-test-after-snapshot-token-lookup-hook* state))
             (let* ((source-snapshot (exported-logical-snapshot-snapshot state))
                    (snapshot (copy-snapshot source-snapshot)))
               (setf (snapshot-xip snapshot) (copy-list (snapshot-xip source-snapshot))
                     (snapshot-sequence-states snapshot)
                     (copy-tree (snapshot-sequence-states source-snapshot))
                     registration (make-snapshot-registration))
               (when generic-state
                 (setf (snapshot-curcid snapshot) (backend-command-id *backend*)))
               (sb-sys:without-interrupts
                 (when generic-state
                   (sb-thread:with-mutex (*schema-lock-mutex*)
                     (unless (exported-logical-snapshot-schema-read-lock-held-p state)
                       (error "exported snapshot schema lease expired"))
                     (incf *schema-read-lock-count*)
                     (setf schema-lease t)))
                 (sb-thread:with-mutex (*xid-lock*)
                   (register-active-snapshot snapshot registration))
                 (setf (backend-transaction-snapshot *backend*) snapshot
                       (backend-transaction-snapshot-registration *backend*)
                       registration
                       (backend-first-snapshot-set *backend*) t
                       (backend-snapshot *backend*) snapshot
                       (backend-transaction-schema-read-lock-held-p *backend*) schema-lease
                       schema-lease nil
                       installed t)
                 (unless generic-state
                   (register-exported-logical-snapshot-importer
                    state registration *backend*)))
               (logical-test-failpoint :after-snapshot-import token))))
      (unless installed
        (unwind-protect
             (when registration (release-snapshot-registration registration))
          (when schema-lease (release-schema-read-lock)))))))

(defun ensure-exported-snapshot-scope-can-end (backend)
  (when (and (eq backend *exported-snapshot-backend*)
             (not *exported-snapshot-cleanup*))
    (ereport "25001"
             "cannot end the transaction inside an exported snapshot scope")))

(defun %call-with-exported-snapshot (slot-name function)
  "Run FUNCTION against a read-only RR snapshot and retain SLOT-NAME's WAL.

Like PostgreSQL's logical snapbuild path, this owns a read-only REPEATABLE READ
snapshot for the initial copy. The named slot survives the dynamic scope and
must be dropped after logical consumption. Schema-changing commands and
VACUUM FULL wait for this scope."
  (unless *backend*
    (ereport "55000" "exported snapshots require an active backend"))
  (unless (eq (backend-state *backend*) :idle)
    (ereport "25001"
             "cannot export a snapshot from inside a transaction"))
  ;; Match statement-start admission before this transaction can publish the
  ;; long-lived snapshot that wraparound coordination must drain.
  (prepare-xid-wraparound-for-statement *backend*)
  (let ((backend *backend*))
    (with-schema-read-lock
      (let ((registration (make-snapshot-registration))
            (slot-owner (gensym "EXPORTED-SNAPSHOT-SLOT-"))
            (database-id
              (database-oid (get-database (backend-database backend))))
            (started nil)
            (body-completed nil)
            (initial-copy-committed-p nil)
            (snapshot nil)
            (boundary-lsn nil))
        (let ((*exported-snapshot-backend* backend))
          (multiple-value-prog1
              (unwind-protect
                   (progn
                     (sb-sys:without-interrupts
                       ;; Mark cleanup ownership before touching backend state. A
                       ;; cancel can then never strand a partially initialized
                       ;; transaction in a reusable backend.
                       (clear-backend-command-id-used backend)
                       (setf started t
                             (backend-explicit backend) t
                             (backend-isolation-level backend) :repeatable-read
                             (backend-read-only backend) t
                             (backend-command-id backend) 0
                             (backend-xid backend) nil
                             (backend-snapshot backend) nil
                             (backend-transaction-snapshot backend) nil
                             (backend-transaction-snapshot-registration backend) nil
                             (backend-first-snapshot-set backend) nil
                             (backend-dirty-relations backend) '()
                             (backend-logical-changes backend) '()
                             (backend-logical-change-bytes backend) 0))
                     (set-backend-state backend :in-progress)
                     (multiple-value-bind (captured-snapshot captured-boundary
                                           restart-lsn)
                         ;; Ordinary statement startup holds *XID-WRAP-LOCK* while
                         ;; registering its snapshot. Keep the same admission
                         ;; handshake here; the registration's temporary WAL pin
                         ;; protects the captured boundary after the gate opens.
                         (wait-for-xid-wraparound
                          backend
                          (lambda ()
                            (with-write-lock
                              (wal-capture-logical-snapshot-boundary-under-writer-lock
                               backend registration database-id))))
                       (setf snapshot captured-snapshot
                             boundary-lsn captured-boundary)
                       (when *logical-test-after-snapshot-capture-hook*
                         (funcall *logical-test-after-snapshot-capture-hook*
                                  slot-name boundary-lsn))
                       ;; Slot fsync can take longer than snapshot capture. Publish
                       ;; after releasing *XID-WRAP-LOCK* while the registered
                       ;; snapshot continues to pin the required WAL.
                       (wal-publish-captured-replication-slot
                        slot-name captured-boundary restart-lsn registration
                        slot-owner (snapshot-xip captured-snapshot)
                        database-id)
                       ;; Publish the retained RR snapshot before user code can run.
                       (sb-sys:without-interrupts
                         (setf (backend-transaction-snapshot backend) snapshot
                               (backend-transaction-snapshot-registration backend)
                               registration
                               (backend-first-snapshot-set backend) t
                               (backend-snapshot backend) snapshot))
                       (let ((*current-snapshot* snapshot))
                         (multiple-value-prog1
                             (funcall function snapshot boundary-lsn)
                           (setf body-completed t)))))
                (unwind-protect
                     (unwind-protect
                          (when started
                            (let ((*exported-snapshot-cleanup* t))
                              (if (and body-completed
                                       (not (eq (backend-state backend) :aborted)))
                                  (handler-case
                                      (progn
                                        (commit-transaction)
                                        (setf initial-copy-committed-p t))
                                    (error (condition)
                                      (ignore-errors (abort-transaction))
                                      (error condition)))
                                  (abort-transaction))))
                       (when initial-copy-committed-p
                         ;; SnapBuild confirms its consistent point after the
                         ;; copy consumer ends, while its exact slot owner is held.
                         (wal-finalize-replication-slot-copy
                          slot-name boundary-lsn slot-owner)))
                  ;; Capture can fail after registering but before ownership moves
                  ;; into BACKEND. Release that token here; release is idempotent if
                  ;; commit or abort already retired it.
                  (unwind-protect
                       (when (snapshot-registration-active-p registration)
                         (release-snapshot-registration registration))
                    (unwind-protect
                         (wal-release-logical-catalog-event-interest registration)
                      (sb-sys:without-interrupts
                        (wal-release-replication-slot-owner
                         slot-name slot-owner))))))))))))

(defun call-with-exported-snapshot (slot-name function)
  (unless *backend* (ereport "55000" "exported snapshots require an active backend"))
  (unless (eq (backend-state *backend*) :idle)
    (ereport "25001" "cannot export a snapshot from inside a transaction"))
  (call-with-logical-capture-armed
   (lambda () (%call-with-exported-snapshot slot-name function))))

(defmacro with-exported-snapshot
    ((snapshot-var start-lsn-var &key slot-name) &body body)
  "Bind SNAPSHOT-VAR and START-LSN-VAR during a pinned initial-copy scope.

SLOT-NAME is a required lowercase replication-slot name. The durable slot
remains after BODY and should be dropped when its WAL is no longer needed."
  (unless slot-name
    (error "WITH-EXPORTED-SNAPSHOT requires :SLOT-NAME"))
  `(call-with-exported-snapshot
    ,slot-name
    (lambda (,snapshot-var ,start-lsn-var)
      ,@body)))

(defun oldest-xmin (&optional exclude-snapshot)
  "Capture the oldest XID protected by a snapshot or active transaction."
  (sb-thread:with-mutex (*xid-lock*)
    (let ((oldest *next-xid*))
      (dolist (xid *active-xids*)
        (when (< xid oldest)
          (setf oldest xid)))
      (let ((snapshot-xmin
              (oldest-active-snapshot-xmin exclude-snapshot)))
        (when (and snapshot-xmin (< snapshot-xmin oldest))
          (setf oldest snapshot-xmin)))
      oldest)))

;;; ---- per-backend transaction state ----

(defun get-current-xid ()
  "Return the backend's XID, allocating and publishing one to it if needed."
  (or (backend-xid *backend*) (assign-xid *backend*)))

(defvar *recovery-reader-xid-counter* 0)

(defun ensure-recovery-reader-identity (backend)
  "Register BACKEND's virtual reader ID, allocating one if needed.

A cached ID is enrolled again with the same value.  It belongs to the
transaction until cleanup and is not a durable XID."
  (sb-sys:without-interrupts
    (sb-thread:with-mutex (*relation-lock-mutex*)
      (when (backend-xid backend)
        (error "recovery reader has a real transaction ID"))
      (let ((xid (or (backend-virtual-reader-xid backend)
                     (- (incf *recovery-reader-xid-counter*)))))
        (unless (and (integerp xid) (minusp xid))
          (error "invalid recovery reader identity"))
        (setf (gethash xid *recovery-reader-xids*) t
              (backend-virtual-reader-xid backend) xid)
        xid))))

(defun get-current-reader-xid ()
  "Return the transaction's relation-lock identity.

A recovery reader uses a virtual ID.  Keep that ID across a role change until
transaction cleanup; otherwise obtain a real XID outside recovery."
  (if (or (backend-virtual-reader-xid *backend*) (recovery-in-progress-p))
      (ensure-recovery-reader-identity *backend*)
      (get-current-xid)))

(defun refresh-statement-snapshot ()
  "Take a fresh snapshot after DDL has waited for prior table writers."
  (let ((snapshot (make-snapshot (backend-command-id *backend*))))
    (setf (backend-snapshot *backend*) snapshot)
    snapshot))

(defun ensure-transaction-snapshot (backend curcid)
  "Acquire BACKEND's retained snapshot once, atomically publishing its owner.

In recovery, enroll the virtual reader even if it has not accessed a relation."
  (when (recovery-in-progress-p)
    (ensure-recovery-reader-identity backend))
  (when (and (null (backend-transaction-snapshot backend))
             (backend-explicit backend) (backend-read-only backend)
             (not (backend-transaction-schema-read-lock-held-p backend)))
    (if *schema-read-lock-held*
        (sb-sys:without-interrupts
          (sb-thread:with-mutex (*schema-lock-mutex*)
            (incf *schema-read-lock-count*)
            (setf (backend-transaction-schema-read-lock-held-p backend) t)))
        (acquire-schema-read-lock
         (lambda () (setf (backend-transaction-schema-read-lock-held-p backend) t)))))
  (or (backend-transaction-snapshot backend)
      (let ((registration (make-snapshot-registration))
            (installed nil))
        (unwind-protect
             (sb-sys:without-interrupts
               (let ((snapshot (make-snapshot curcid :register registration)))
                 (setf (backend-transaction-snapshot backend) snapshot
                       (backend-transaction-snapshot-registration backend)
                       registration
                       installed t)
                 snapshot))
          (unless installed
            (release-snapshot-registration registration))))))

(defun begin-transaction-snapshot (backend curcid statement-registration)
  "Return a per-statement view over BACKEND's stable transaction horizon.

The generated copier carries coverage and other value slots.  The view borrows
the retained snapshot's immutable XIP, sets CURCID, and clears sequence metadata."
  (let* ((retained (ensure-transaction-snapshot backend curcid))
         (snapshot (copy-snapshot retained)))
    (setf (snapshot-curcid snapshot) curcid
          (snapshot-sequence-database-oid snapshot) nil
          (snapshot-sequence-state-captured-p snapshot) nil
          (snapshot-sequence-states snapshot) nil)
    ;; The transaction registration protects the horizon until transaction
    ;; cleanup; register the statement view too so its dynamic lifetime follows
    ;; the existing WITH-STATEMENT-SNAPSHOT contract.
    (sb-thread:with-mutex (*xid-lock*)
      (register-active-snapshot snapshot statement-registration))
    snapshot))

(defun mark-dirty (relfilenode)
  (unless (member relfilenode (backend-dirty-relations *backend*) :test #'eql)
    (push relfilenode (backend-dirty-relations *backend*)))
  (values))

(defun backend-transaction-read-only-p (&optional (backend *backend*))
  "Use the current recovery role for a new transaction, as C StartTransaction does.
An idle backend may retain an old recovery flag after promotion. Active explicit
transactions keep their selected READ ONLY mode; the supported idle default is
READ WRITE outside recovery."
  (or (recovery-in-progress-p)
      (and backend (not (eq (backend-state backend) :idle))
           (backend-read-only backend))))

(defvar *statement-snapshot-override* nil
  "The Bind-time SELECT snapshot to use for this portal's Execute.")

(defvar *backend-used-command-ids* (make-hash-table :test #'eq :weakness :key)
  "Last command IDs used by modifying commands, cleared at Sync or transaction reset.")

(defvar *backend-used-command-ids-lock*
  (sb-thread:make-mutex :name "pg used command ids"))

(defun note-backend-command-id-used (&optional (backend *backend*))
  ;; ExecutorStart marks modifying commands even when no tuple qualifies.
  (sb-thread:with-mutex (*backend-used-command-ids-lock*)
    (setf (gethash backend *backend-used-command-ids*) (backend-command-id backend)))
  (values))

(defun clear-backend-command-id-used (backend)
  (sb-thread:with-mutex (*backend-used-command-ids-lock*)
    (remhash backend *backend-used-command-ids*))
  (values))

(defun advance-used-backend-command-id (&optional (backend *backend*))
  ;; Sync completes a transaction command without completing a suspended portal.
  ;; A Parse or completed Execute may already have advanced past this mark.
  (sb-thread:with-mutex (*backend-used-command-ids-lock*)
    (multiple-value-bind (used present) (gethash backend *backend-used-command-ids*)
      (when (and present (= used (backend-command-id backend)))
        (incf (backend-command-id backend)))
      (remhash backend *backend-used-command-ids*)))
  (values))

(defvar *statement-command-start-advance-p* t)

(defun statement-start (registration &optional (advance-command-p *statement-command-start-advance-p*))
  "Start a statement and register its snapshot in REGISTRATION.
A portal override retains the Bind-time horizon and command ID for Execute.
Portal initialization can retain the current command ID by disabling advance."
  (let ((b *backend*))
    (when (eq (backend-state b) :aborted)
      (ereport "25P02"
               "current transaction is aborted, commands ignored until end of transaction block"))
    (unless (recovery-in-progress-p)
      (prepare-xid-wraparound-for-statement b))
    (setf (backend-read-only b) (backend-transaction-read-only-p b))
    (when (eq (backend-state b) :idle)
      (clear-backend-command-id-used b)
      (setf (backend-explicit b) nil
            (backend-command-id b) 0)
      (set-backend-state b :in-progress))
    (when (recovery-in-progress-p)
      (ensure-recovery-reader-identity b))
    (when advance-command-p (incf (backend-command-id b)))
    (let ((snap (cond
                  (*statement-snapshot-override*
                   (let ((snapshot (copy-snapshot *statement-snapshot-override*)))
                     (sb-thread:with-mutex (*xid-lock*)
                       (register-active-snapshot snapshot registration))
                     snapshot))
                  ((eq (backend-isolation-level b) :repeatable-read)
                   (begin-transaction-snapshot b (backend-command-id b) registration))
                  (t (begin-statement-snapshot b (backend-command-id b) registration)))))
      (setf (backend-first-snapshot-set b) t)
      (setf (backend-snapshot b) snap)
      snap)))

(defun release-backend-transaction-snapshot (backend)
  (sb-sys:without-interrupts
    (let ((registration (backend-transaction-snapshot-registration backend))
          (schema-lease (backend-transaction-schema-read-lock-held-p backend)))
      (setf (backend-transaction-snapshot backend) nil
            (backend-transaction-snapshot-registration backend) nil
            (backend-transaction-schema-read-lock-held-p backend) nil)
      (unwind-protect
           (when registration (release-snapshot-registration registration))
        (when schema-lease (release-schema-read-lock)))))
  (values))

(defun reset-backend-transaction (b)
  "Release transaction resources and return B to idle.

Mask interrupts until cleanup and the idle transition finish, so no interrupt
can discard the owner of a virtual ID or its remaining relation locks."
  (sb-sys:without-interrupts
    (clear-backend-command-id-used b)
    (when (backend-logical-spool b)
      (close-logical-spool (backend-logical-spool b))
      (setf (backend-logical-spool b) nil))
    (discard-transaction-replacements b)
    (release-transaction-relation-locks b)
    (expire-transaction-exported-snapshots b)
    (release-backend-transaction-snapshot b)
    (when (backend-virtual-reader-xid b)
      (release-xid (backend-virtual-reader-xid b) b))
    (setf (backend-xid b) nil
          (backend-explicit b) nil
          (backend-implicit-query-block b) nil
          (backend-isolation-level b) :read-committed
          (backend-read-only b) (recovery-in-progress-p)
          (backend-command-id b) 0
          (backend-snapshot b) nil
          (backend-first-snapshot-set b) nil
          (backend-dirty-relations b) '()
          (backend-logical-changes b) '()
          (backend-logical-change-bytes b) 0)
    (set-backend-state b :idle))
  (values))

(defvar *preserve-failed-extended-portals* nil)

(defun prepare-backend-portal-transaction-end (backend &optional commit-p)
  ;; Retire portals before transaction resources; incomplete cleanup prevents
  ;; commit, while abort can continue with the remaining cleanup paths.
  (when (fboundp 'extended-clear-portals-for-backend)
    (let ((pending (funcall 'extended-clear-portals-for-backend backend)))
      (when (and pending commit-p)
        (ereport "58030" "query execution cleanup remains incomplete"))))
  (values))

(defun finish-backend-transaction (backend commit-p)
  "Reset BACKEND and finish application_name state atomically.
On commit, retire the saved settings snapshot; abort leaves it for whole-table undo."
  (sb-sys:without-interrupts
    (let ((*backend* backend))
      (reset-backend-transaction backend)
      ;; Portal resources retire before COMMIT enters WAL or ABORT releases XID locks.
      (session-finish-application-name commit-p)
      (when commit-p
        (setf (backend-settings-transaction-start backend) nil))))
  (values))

(defun finish-committed-transaction (backend xid)
  ;; Retire resources even when WAL is poisoned, without changing CLOG.  An
  ;; incomplete cleanup fail-stops after releasing the mutex; keep the first error.
  (let ((complete nil)
        (failure nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (handler-bind ((error (lambda (condition) (unless failure (setf failure condition)))))
             (unwind-protect
                  (release-xid xid)
               ;; Use the raw mutex to retire private references even if WAL is
               ;; poisoned.  This does not clear poison; any future remote wait
               ;; must run outside this mutex.
               (sb-thread:with-mutex (*wal-lock*)
                 (finish-backend-transaction backend t)))
             (setf complete t))
        (unless complete
          (wal-fail-stop
           (or failure
               (make-condition 'simple-error
                               :format-control "Committed transaction cleanup exited before completion"
                               :format-arguments nil)))))))
  (values))

(defun commit-transaction ()
  (ensure-exported-snapshot-scope-can-end *backend*)
  (prepare-backend-portal-transaction-end *backend* t)
  (let ((b *backend*))
    (if (backend-xid b)
        (let ((changes
                (when *wal-ready*
                  (if (backend-logical-changes b)
                      ;; Compatibility for internal callers that supply the old list.
                      (let ((events (append (reverse (backend-logical-changes b))
                                            (when (backend-logical-spool b)
                                              (logical-spool-seal (backend-logical-spool b))
                                              (logical-spool-events-in-order (backend-logical-spool b))))))
                        (logical-preflight-event-fragments events)
                        events)
                      (when (backend-logical-spool b)
                        (logical-preflight-spool (backend-logical-spool b)))))))
          ;; Validate the spool before publishing any WAL batch. Subsequent I/O
          ;; failure during WAL insertion follows the normal critical-failure path.
          ;; Keep the XID through local storage apply.  Mask interrupts across
          ;; publication and lexical ownership of cleanup.  A poisoned WAL can
          ;; leave the outcome indeterminate: retire resources without inventing
          ;; a CLOG commit or abort.
          (sb-sys:without-interrupts
            (let ((irreversible nil)
                  (wal-ready-at-entry-p *wal-ready*)
                  (xid (backend-xid b)))
              (unwind-protect
                   (wal-commit-transaction
                    xid
                    (lambda ()
                      (clog-set xid +clog-committed+ :fsync (not wal-ready-at-entry-p))
                      (setf irreversible t))
                    changes
                    (lambda (end)
                      (declare (ignore end))
                      (setf irreversible t)))
                (when (or irreversible (and wal-ready-at-entry-p *wal-poisoned*))
                  (finish-committed-transaction b xid))))))
        (finish-backend-transaction b t)))
  (values))

(defun abort-transaction ()
  (ensure-exported-snapshot-scope-can-end *backend*)
  (let ((b *backend*))
    (unwind-protect
         (prepare-backend-portal-transaction-end b)
      (sb-sys:without-interrupts
        (when (backend-xid b)
          (when (= (clog-get (backend-xid b)) +clog-in-progress+)
            (clog-set (backend-xid b) +clog-aborted+ :fsync t))
          (release-xid (backend-xid b)))
        (finish-backend-transaction b nil))))
  (values))

(defun abort-transaction-block ()
  "Abort resources immediately while retaining the explicit failed-block status."
  (let ((explicit (backend-explicit *backend*)))
    (let ((*preserve-failed-extended-portals* explicit))
      (abort-transaction))
    (when explicit
      (setf (backend-explicit *backend*) t)
      (set-backend-state *backend* :aborted)))
  (values))

(defun end-backend-session ()
  "Abort any transaction still open when a client disconnects.

A backend that vanishes mid-transaction must not leave its XID in
*active-xids*; otherwise another session waiting on a conflicting unique
key would block forever, and aborted heap changes would never be
reclaimable."
  (when *backend*
    (unwind-protect
         (when (or (backend-xid *backend*)
                   (not (eq (backend-state *backend*) :idle)))
           (abort-transaction))
      (wal-cleanup-backend-physical-slots *backend*))))

(defun transaction-status-char ()
  (case (and *backend* (backend-state *backend*))
    (:in-progress #\T)
    (:aborted #\E)
    (t #\I)))

;;; ---- startup ----

(defun xact-init ()
  (ensure-directories-exist (pgdata-path "global/pg_control"))
  (ensure-directories-exist (pgdata-path "base/dummy"))
  (let ((version-file (pgdata-path "PG_VERSION")))
    (unless (probe-file version-file)
      (with-open-file (s version-file
                         :direction :output
                         :if-exists :supersede
                         :if-does-not-exist :create
                         :element-type 'character)
        (format s "16~%")
        (finish-output s)
        (sb-posix:fsync (sb-sys:fd-stream-fd s)))))
  (let* ((saved-next-xid (load-next-xid))
         (next-xid (canonical-next-full-xid
                    (max saved-next-xid (1+ (or *wal-xid-reserved-through* 2))))))
    (when (> next-xid +max-xid-one-past+)
      (error "transaction ID space exhausted"))
    (setf *xid-batch-end* next-xid
          *next-xid* next-xid
          *xid-wrap-pending* nil
          *xid-wrap-completed-through* nil)
    ;; Persist the initial control state too. A missing control file on a later
    ;; startup then cannot be mistaken for a fresh data directory.
    (when (or (not (probe-file (control-path))) (/= saved-next-xid next-xid))
      (save-next-xid))
    (let ((marker (load-xid-wrap-marker)))
      (when marker
        (unless (and (= marker next-xid)
                     (first-normal-xid-of-epoch-p next-xid))
          (error "XID wrap marker ~a does not match next XID ~a"
                 marker next-xid))
        (setf *xid-wrap-pending* t))))
  (clog-init)
  (clog-recover *next-xid*)
  (values))
