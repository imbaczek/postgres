;;;; snapshot.lisp -- per-statement snapshots and MVCC visibility.

(in-package #:pg)

(defstruct (snapshot (:constructor make-snapshot-struct
                         (&key xmin xmax xip curcid sequence-database-oid
                            sequence-state-captured-p sequence-states source-xids-complete-p)))
  xmin
  xmax
  xip
  curcid
  sequence-database-oid
  sequence-state-captured-p
  sequence-states
  ;; True when source tracking covered this snapshot's acquisition boundary.
  ;; Coverage and XIP stay fixed while replay advances.
  (source-xids-complete-p nil))

(defstruct (snapshot-registration
            (:constructor make-snapshot-registration ()))
  snapshot
  wal-retention-lsn
  exported-state
  importer-backend
  (lifecycle-generation 0)
  (releasing-p nil)
  (active-p nil))

(defstruct exported-logical-snapshot
  slot-name slot-object token database-oid snapshot registration
  schema-read-lock-held-p
  (importers '())
  (expired-p nil)
  (termination-requested-backends '())
  (source-registration-released-p nil)
  (cleanup-complete-p nil))

(defvar *current-snapshot* nil)

(defvar *statement-snapshot-registrations* nil
  "Registration tokens for the dynamically enclosing statement snapshot scopes.
A token can be present before acquisition makes it active.  This list alone
does not park admission or authorize replay.")

(defvar *exported-logical-snapshot-lock*
  (sb-thread:make-mutex :name "pg exported logical snapshots"))

(defvar *exported-logical-snapshot-importer-lock*
  (sb-thread:make-mutex :name "pg exported logical snapshot importers"))

(defvar *logical-slot-lifecycle-lock*
  (sb-thread:make-mutex :name "pg logical slot lifecycle"))
(defvar *logical-test-after-snapshot-token-lookup-hook* nil)
(defvar *logical-test-after-snapshot-capture-hook* nil)
(defvar *exported-logical-snapshots* (make-hash-table :test #'equal))

(defvar *expired-exported-logical-snapshots* (make-hash-table :test #'equal))

(defvar *exported-logical-snapshot-counter* 0)

;;; Active snapshot xmin values bound the oldest tuple version a vacuum may
;;; remove. The count remains separate because XID wraparound waits for every
;;; statement snapshot to drain.
(defvar *snapshot-count-lock* (sb-thread:make-mutex :name "pg snapshot count"))
(defvar *active-snapshots* 0)
(defvar *active-snapshot-set* (make-hash-table :test #'eq))
(defvar *snapshot-cv* (sb-thread:make-waitqueue :name "pg snapshot waitqueue"))
(defvar *snapshot-cv-mutex* (sb-thread:make-mutex :name "pg snapshot cv mutex"))
(defvar *wal-test-after-snapshot-release-mark-hook* nil)

(defvar *logical-test-after-export-cleanup-claim-hook* nil)

(declaim (ftype function xid-active-p))

(declaim (ftype function finish-expired-exported-logical-snapshot)
         (ftype function release-schema-read-lock))

(defun register-active-snapshot (snapshot registration)
  "Publish SNAPSHOT's xmin while the caller still holds the XID lock.

REGISTRATION owns both the active count and the snapshot entry before
asynchronous interrupts are re-enabled. Each reuse starts a fresh cleanup
generation, so an older WAL-pin release cannot retire this snapshot."
  (unless (snapshot-registration-p registration)
    (error "active snapshots require a snapshot-registration token"))
  (sb-sys:without-interrupts
    (sb-thread:with-mutex (*snapshot-count-lock*)
      (when (or (snapshot-registration-active-p registration)
                (snapshot-registration-releasing-p registration))
        (error "snapshot-registration token is active or being released"))
      (incf (snapshot-registration-lifecycle-generation registration))
      (incf *active-snapshots*)
      (setf (gethash snapshot *active-snapshot-set*) t
            (snapshot-registration-snapshot registration) snapshot
            (snapshot-registration-active-p registration) t)))
  snapshot)

(defun oldest-active-snapshot-xmin (&optional exclude-snapshot)
  (sb-thread:with-mutex (*snapshot-count-lock*)
    (let ((oldest nil))
      (maphash (lambda (snapshot present)
                 (when (and present (not (eq snapshot exclude-snapshot)))
                   (let ((xmin (snapshot-xmin snapshot)))
                     (when (or (null oldest) (< xmin oldest))
                       (setf oldest xmin)))))
               *active-snapshot-set*)
      oldest)))

(defun finish-snapshot-registration-release (registration generation pinned)
  "Remove PINNED and detach its importer only for this release generation.

WAL precedes the snapshot-count lock when a pin exists, matching the local
boundary-capture path. PostgreSQL's snapmgr.c:AtEOXact_Snapshot is the
reference for retiring registered snapshots with transaction cleanup; this
reusable token also checks a generation before delayed cleanup."
  (let ((imported-state nil)
        (completed nil))
    (labels ((finish-under-snapshot-lock ()
               (let ((current-lsn
                       (snapshot-registration-wal-retention-lsn registration)))
                 (when (and (= generation
                               (snapshot-registration-lifecycle-generation
                                registration))
                            (snapshot-registration-releasing-p registration))
                   ;; A stale unpinned release may outlive a retry and token
                   ;; reuse. Check its generation before asserting against a
                   ;; newer registration's published pin.
                   (unless pinned
                     (assert (null current-lsn)
                             ()
                             "unpinned snapshot release found a published WAL retention pin"))
                   (when (or (null current-lsn)
                             (eql pinned current-lsn))
                     (when pinned
                       (remhash registration *wal-snapshot-retention-pins*))
                     ;; Detach before clearing RELEASING-P. Reuse may publish a
                     ;; new importer immediately after this lock is released.
                     (sb-thread:with-mutex (*exported-logical-snapshot-importer-lock*)
                       (setf imported-state
                             (snapshot-registration-exported-state registration))
                       (when imported-state
                         (setf (exported-logical-snapshot-importers imported-state)
                               (delete
                                (snapshot-registration-importer-backend registration)
                                (exported-logical-snapshot-importers imported-state)
                                :test #'eq))
                         (setf (snapshot-registration-exported-state registration) nil
                               (snapshot-registration-importer-backend registration)
                               nil)))
                     (setf (snapshot-registration-wal-retention-lsn registration) nil
                           (snapshot-registration-releasing-p registration) nil
                           completed t))))))
      ;; Unpinned registrations use only the snapshot and importer locks. This
      ;; avoids recursively taking WAL from transaction-end cleanup, and lets a
      ;; release win against a capture paused before it publishes a pin.
      (if pinned
          (sb-thread:with-mutex (*wal-lock*)
            (sb-thread:with-mutex (*snapshot-count-lock*)
              (finish-under-snapshot-lock)))
          (sb-thread:with-mutex (*snapshot-count-lock*)
            (finish-under-snapshot-lock))))
    ;; Like snapmgr.c's transaction-end cleanup, release dependent state only
    ;; after shared locks are gone. The detached state is safe across reuse.
    (when (and completed imported-state)
      (finish-expired-exported-logical-snapshot imported-state))
    completed))

(defun release-snapshot-registration (registration)
  "Release the snapshot, WAL retention pin, and active-count entry once."
  (let ((pinned nil)
        (generation nil)
        (active nil)
        (release-pending nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             ;; Boundary capture publishes the pin while holding the
             ;; snapshot-count lock. Keep the token unavailable until both the
             ;; pin and any importer association are detached.
             (sb-thread:with-mutex (*snapshot-count-lock*)
               (setf generation
                     (snapshot-registration-lifecycle-generation registration))
               (cond
                 ((and (snapshot-registration-active-p registration)
                       (not (snapshot-registration-releasing-p registration)))
                  (setf active t
                        pinned
                        (snapshot-registration-wal-retention-lsn registration))
                  (when (remhash (snapshot-registration-snapshot registration)
                                 *active-snapshot-set*)
                    (decf *active-snapshots*))
                  (setf (snapshot-registration-snapshot registration) nil
                        (snapshot-registration-active-p registration) nil
                        (snapshot-registration-releasing-p registration) t
                        release-pending t))
                 ((snapshot-registration-releasing-p registration)
                  (setf pinned
                        (snapshot-registration-wal-retention-lsn registration)
                        release-pending t))))
             ;; This hook pauses after a caller captures the release
             ;; generation, including concurrent retry callers.
             (when (and release-pending
                        *wal-test-after-snapshot-release-mark-hook*)
               (funcall *wal-test-after-snapshot-release-mark-hook* registration))
             ;; Never take WAL while holding the snapshot-count lock: capture
             ;; takes WAL->XID->snapshot-count. The generation is revalidated
             ;; while the token remains unavailable to later registrations.
             (when release-pending
               (finish-snapshot-registration-release
                registration generation pinned)))
        (when active
          (sb-thread:with-mutex (*snapshot-cv-mutex*)
            (sb-thread:condition-broadcast *snapshot-cv*))))))
  (values))

(defun release-snapshot-registration-wal-pin-under-wal-lock (registration)
  "Drop a provisional snapshot pin after its durable slot owns the same LSN.

The caller holds *WAL-LOCK*, so checkpoint recycling sees either this pin or
the already-persisted slot restart point, never a gap. If release is underway,
its owner finishes the remaining importer cleanup before reuse."
  (sb-thread:with-mutex (*snapshot-count-lock*)
    (remhash registration *wal-snapshot-retention-pins*)
    (setf (snapshot-registration-wal-retention-lsn registration) nil))
  (values))

(defun register-exported-logical-snapshot-importer (state registration backend)
  "Track an imported snapshot transaction until its MVCC pin is released."
  (sb-sys:without-interrupts
    (sb-thread:with-mutex (*exported-logical-snapshot-importer-lock*)
      (when (exported-logical-snapshot-expired-p state)
        (ereport "22023" "exported logical snapshot token is invalid or expired"))
      (setf (snapshot-registration-exported-state registration) state
            (snapshot-registration-importer-backend registration) backend)
      (pushnew backend (exported-logical-snapshot-importers state) :test #'eq)))
  (values))

(defun finish-expired-exported-logical-snapshot (state)
  "Release the source MVCC pin immediately and schema lease after importers end."
  (let ((registration nil)
        (release-schema-p nil))
    ;; Protect the ownership handoff before publishing either completion flag.
    ;; Keep the flag and its local release handle atomic as asynchronous
    ;; invalidation may interrupt an importer while its transaction is ending.
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (sb-thread:with-mutex (*exported-logical-snapshot-importer-lock*)
               (when (exported-logical-snapshot-expired-p state)
                 (setf registration
                       (exported-logical-snapshot-registration state)
                       (exported-logical-snapshot-source-registration-released-p
                        state)
                       t)
                 (when (and (null (exported-logical-snapshot-importers state))
                            (not (exported-logical-snapshot-cleanup-complete-p
                                  state)))
                   (setf (exported-logical-snapshot-cleanup-complete-p state) t
                         release-schema-p
                         (exported-logical-snapshot-schema-read-lock-held-p state)
                         (exported-logical-snapshot-schema-read-lock-held-p
                          state)
                         nil)))))
           (when *logical-test-after-export-cleanup-claim-hook*
             (funcall *logical-test-after-export-cleanup-claim-hook* state)))
      (unwind-protect
           (when registration
             (release-snapshot-registration registration))
        (when release-schema-p
          (release-schema-read-lock))))
    ;; Keep expired tokens in the pending registry until both pins have drained.
    ;; Exact state identity prevents old cleanup from removing a new token owner.
    (when (exported-logical-snapshot-cleanup-complete-p state)
      (let ((token (exported-logical-snapshot-token state)))
        (when token
          (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
            (sb-sys:without-interrupts
              (when (eq state (gethash token *exported-logical-snapshots*))
                (remhash token *exported-logical-snapshots*))
              (when (eq state
                        (gethash token *expired-exported-logical-snapshots*))
                (remhash token *expired-exported-logical-snapshots*)))))))
    (values)))

(defun expire-exported-logical-snapshot-state (state)
  "Mark STATE unavailable and finish it immediately when no importer remains."
  (when state
    (unwind-protect
         (sb-thread:with-mutex (*exported-logical-snapshot-importer-lock*)
           (sb-sys:without-interrupts
             (setf (exported-logical-snapshot-expired-p state) t)))
      (finish-expired-exported-logical-snapshot state)))
  state)

(defmacro with-statement-snapshot ((snapshot acquire-function) &body body)
  "Acquire a registered statement snapshot inside its cleanup scope.

ACQUIRE-FUNCTION receives the registration token and must return the registered
snapshot.  The actual token is in *STATEMENT-SNAPSHOT-REGISTRATIONS* during
acquisition, BODY and release; it may not yet be active during acquisition.
Nested scopes restore the outer list.  Cleanup protects the token if acquisition
is interrupted after publication but before it returns."
  (let ((registration (gensym "SNAPSHOT-REGISTRATION-")))
    `(let* ((,registration (make-snapshot-registration))
            (*statement-snapshot-registrations*
              (cons ,registration *statement-snapshot-registrations*)))
       (unwind-protect
            (let ((,snapshot (funcall ,acquire-function ,registration)))
              (declare (ignorable ,snapshot))
              (let ((*current-snapshot* ,snapshot))
                ,@body))
         (release-snapshot-registration ,registration)))))

(defun xid-committed-p (xid)
  (or (member xid (list +bootstrap-xid+ +frozen-xid+) :test #'=)
      (eql (clog-get xid) +clog-committed+)))

(defun xid-aborted-p (xid)
  (and (/= xid +bootstrap-xid+)
       (/= xid +frozen-xid+)
       (or (eql (clog-get xid) +clog-aborted+)
           (and (eql (clog-get xid) +clog-in-progress+)
                (not (xid-active-p xid))))))

(defun xid-in-progress-p (xid)
  (and (= (clog-get xid) +clog-in-progress+)
       (xid-active-p xid)))

(defvar *combo-command-id-lock* (sb-thread:make-mutex :name "pg combo command ids"))

(defvar *combo-command-ids* (make-hash-table :test #'eql))

(defun make-combo-command-id (xid cmin cmax)
  ;; An own tuple deleted in a later command needs both boundaries in its one CID
  ;; field. This mapping is transaction-local and is discarded when XID is released.
  (sb-thread:with-mutex (*combo-command-id-lock*)
    (let* ((state (or (gethash xid *combo-command-ids*)
                      (setf (gethash xid *combo-command-ids*)
                            (cons (make-array 0 :adjustable t :fill-pointer 0)
                                  (make-hash-table :test #'equal)))))
           (pair (cons cmin cmax)))
      (or (gethash pair (cdr state))
          (let ((index (length (car state))))
            (when (> index #xffffffff) (ereport "54000" "too many combo command IDs"))
            (vector-push-extend pair (car state))
            (setf (gethash pair (cdr state)) index))))))

(defun combo-command-id-values (xid cid)
  ;; Only the owning transaction can interpret a combo CID; a missing entry is
  ;; corruption of its visibility state, not a reason to guess a boundary.
  (sb-thread:with-mutex (*combo-command-id-lock*)
    (let ((state (gethash xid *combo-command-ids*)))
      (unless (and state (< -1 cid (length (car state))))
        (ereport "XX000" "missing combo command ID for current transaction"))
      (let ((pair (aref (car state) cid))) (values (car pair) (cdr pair))))))

(defun forget-combo-command-ids (xid)
  (sb-thread:with-mutex (*combo-command-id-lock*)
    (remhash xid *combo-command-ids*)))

(defun heap-tuple-visible-p (xmin xmax cid snapshot my-xid &optional (infomask 0))
  "HeapTupleSatisfiesMVCC, retaining the statement's insertion/deletion CID boundary."
  (let ((curcid (snapshot-curcid snapshot)))
    (cond
      ((and my-xid (= xmin my-xid))
       (multiple-value-bind (cmin cmax)
           (if (logtest +heap-combo-cid+ infomask)
               (combo-command-id-values my-xid cid) (values cid cid))
         (and (< cmin curcid)
              (or (/= xmax my-xid) (>= cmax curcid)))))
      ((>= xmin (snapshot-xmax snapshot)) nil)
      ((member xmin (snapshot-xip snapshot) :test #'=) nil)
      ((not (xid-committed-p xmin)) nil)
      ((and my-xid (= xmax my-xid)) (>= cid curcid))
      ((zerop xmax) t)
      ((>= xmax (snapshot-xmax snapshot)) t)
      ((member xmax (snapshot-xip snapshot) :test #'=) t)
      ((xid-committed-p xmax) nil)
      (t t))))

(defun tuple-prunable-p (xmin xmax oldest-xmin)
  "Return whether tuple XIDs are older than the captured removal horizon."
  (or (and (xid-aborted-p xmin)
           (< xmin oldest-xmin))
      (and (not (zerop xmax))
           (xid-committed-p xmax)
           (< xmax oldest-xmin))))
