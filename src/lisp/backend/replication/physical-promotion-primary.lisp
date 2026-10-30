;;;; Committed child startup never restores the retired standby generation.
;;;; Startup must choose its authority before it opens ordinary recovery storage.
;;;; Parent control plus a pending request requires the exact saved generation.
;;;; Child control instead requires a matching child checkpoint and history; it
;;;; must not depend on the old generation or receive spool, which may be gone.
;;;;
;;;; Only the owning postmaster, before listeners or under closed admission, may
;;;; finish the transition. Cold completion initializes ordinary primary storage.
;;;; A private live path can adopt verified child control while retaining readers
;;;; and storage. Both retire recovery triggers and remove pending control last.
;;;; A failure after child control is saved requires restart, not parent rollback.

(in-package #:pg)

(defun physical-promotion-check-child-control (insertion checkpoint-timeline)
  "Verify child control, its checkpoint record and timeline history without writes.
Return the checkpoint snapshot and parsed history. This check must precede WAL
scanning or tail repair. It deliberately needs no old generation, receive spool
or backup manifest: child control is now the recovery authority.  An optional
retained archive is checked against the child history, including its fixed fork."
  (multiple-value-bind (checkpoint durable snapshot timeline checkpoint-id namespace metadata) (wal-read-control-fields)
    (unless (and (typep insertion '(integer 2 4294967295))
                 (= insertion checkpoint-timeline timeline checkpoint-id)
                 (eq namespace :timeline) snapshot (wal-checkpoint-snapshot-valid-p snapshot)
                 (= checkpoint (getf snapshot :end)) (<= checkpoint durable)
                 (= insertion (getf snapshot :timeline)))
      (error "child WAL control does not name a complete child checkpoint"))
    (let* ((history (timeline-check-lineage
                     (timeline-parse-history
                      insertion (bytes->string (physical-promotion-read-file
                                                (timeline-history-path insertion *pgdata*))))))
           (parent (second history)) (start (getf snapshot :start))
           (path (wal-segment-path (floor start +wal-segment-size+) insertion :timeline)))
      (unless (and parent (= (getf snapshot :prev-timeline) (timeline-history-entry-timeline parent))
                   (= insertion (timeline-at-point start history)))
        (error "child checkpoint differs from its history"))
      (multiple-value-bind (stream size) (physical-promotion-open-file path)
        (unwind-protect
             (let ((bytes (make-array (wal-checkpoint-record-size) :element-type '(unsigned-byte 8)))
                   (offset (mod start +wal-segment-size+)))
               (unless (<= (+ offset (length bytes)) size) (error "child checkpoint is truncated"))
               (file-position stream offset)
               (unless (and (= (length bytes) (read-sequence bytes stream))
                            (equal snapshot (wal-checkpoint-record-snapshot (wal-decode-record bytes start))))
                 (error "child checkpoint differs from durable control")))
          (close stream)))
      (physical-promotion-check-primary-retained snapshot history metadata)
      (values snapshot history))))

(defun physical-promotion-check-retiring-control (snapshot history &optional system-id)
  "Check any remaining pending request against the committed child.
An absent request is normal after cleanup. If it remains, require the same
system identity, parent, child and fork, with no larger replay visibility counter.
Do not open its selected generation or receive spool."
  (let ((path (physical-recovery-control-path)))
    (when (wal-namespace-file-stat path t)
      (let* ((control (physical-recovery-read-control *pgdata*))
             (*catalog-identity-needs-persist-p* nil)
             (catalog (unless system-id (load-catalog))))
        (unless (equal (getf control :retained-reference)
                       (getf (nth-value 6 (wal-read-control-fields)) :retained-reference))
          (error "committed child differs from pending retained reference"))
        (unless (and (eql 3 (getf control :phase))
                     (= (getf control :promotion-child) (getf snapshot :timeline))
                     (= (getf control :applied-timeline (getf control :timeline)) (getf snapshot :prev-timeline))
                     (= (getf control :applied) (timeline-history-entry-begin (first history)))
                     (<= (getf control :next-xid) (getf snapshot :next-xid))
                     (= (getf control :system-id) (or system-id (catalog-system-identifier catalog))))
          (error "committed child contradicts its pending recovery metadata"))))))

(defun physical-promotion-retire-recovery ()
  "Remove recovery triggers after verifying committed child control.
Require the owning cold postmaster or closed admission under quiescence.
Check every name before deleting any, then unlink and sync the PGDATA directory
for each step. Register primary archive protection and save source cleanup before
unlinking recovery provenance. Retire both signal names present at cleanup,
rather than only names seen at startup as in C PostgreSQL. This prevents recovery
re-entry on a cold retry. Unlink names without following symlink targets.
Remove pending control last, then retire its owner after primary control readback
and a check that pending control is absent. Old generation and receive directories
are no longer required for primary startup."
  (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline) (wal-read-control-fields)
    (declare (ignore checkpoint durable))
    (multiple-value-bind (checked history) (physical-promotion-check-child-control insertion checkpoint-timeline)
      (declare (ignore checked))
      (physical-promotion-check-retiring-control snapshot history)))
  (unless (and *postmaster-data-lock* (postmaster-pid-owned-p)
               (or (and (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
                        (not *live-operation-drain-open*))
                   (and (zerop *n-backends*) (not *wal-ready*))))
    (error "committed cleanup requires owned cold startup or quiescence"))
  (let ((fields (physical-promotion-restore-primary-retained))
        (names '(("standby.signal" :after-retire-standby-signal)
                 ("recovery.signal" :after-retire-recovery-signal)
                 ("recovery.required" :after-retire-required)
                 ("physical-backup.manifest" :after-retire-manifest)
                 ("physical-follow.control" :after-retire-follow)
                 ("physical-restartpoint.control" :after-retire-pointer)
                 ("physical-recovery.control" :after-retire-pending))))
    (physical-promotion-test-point :after-primary-retained-registration)
    (let ((cleanup (physical-follow-promotion-cleanup-config)))
      (when cleanup
        (physical-follow-controller-save-config cleanup)
        (setf names (remove "physical-follow.control" names :key #'first :test #'equal))
        (physical-promotion-test-point :after-retire-follow-transfer)))
    (dolist (entry names)
      (if (member (first entry) '("standby.signal" "recovery.signal") :test #'equal)
          (physical-recovery-signal-retirement-stat (first entry))
          (wal-namespace-file-stat (merge-pathnames (first entry) *pgdata*) t)))
    (dolist (entry names)
      (let ((path (merge-pathnames (first entry) *pgdata*)))
        (if (member (first entry) '("standby.signal" "recovery.signal") :test #'equal)
            (physical-recovery-unlink-signal (first entry))
            (when (wal-namespace-file-stat path t) (sb-posix:unlink (namestring path))))
        (physical-promotion-test-point (second entry))
        (fsync-directory *pgdata*)))
    (physical-promotion-retire-pending-retained fields)
    (physical-promotion-test-point :after-pending-retained-retirement))
  (values))

(defun physical-promotion-startup-primary-p ()
  "Recognize committed child control and finish interrupted trigger cleanup.
A phase-3 request whose insertion timeline still equals its parent remains pending,
even when that parent is itself a child. Invalid committed control is an error."
  (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline) (wal-read-control-fields)
    (declare (ignore checkpoint durable snapshot))
    (when (and insertion (> insertion 1))
      (when (wal-namespace-file-stat (physical-recovery-control-path) t)
        (let ((control (physical-recovery-read-control *pgdata*)))
          (when (and (= (getf control :phase) 3) (= insertion (getf control :timeline)))
            (return-from physical-promotion-startup-primary-p nil))))
      (wal-require-supported-timelines insertion checkpoint-timeline)
      (physical-promotion-retire-recovery)
      t)))

(defun physical-promotion-startup-pending-p ()
  "Complete a phase-3 request in the owning cold postmaster before listeners.
Hold command and reload locks, then close admission for reconstruction, child
commit and primary initialization. Return true only after all three succeed.
Return NIL when there is no pending request; propagate any validation failure."
  (when (and (wal-namespace-file-stat (physical-recovery-control-path) t)
             (eql 3 (getf (physical-recovery-read-control *pgdata*) :phase)))
    (sb-thread:with-mutex (*physical-follow-command-lock*)
      (sb-thread:with-mutex (*reload-run-lock*)
        (with-live-operation-quiescence
          (physical-promotion-resume-pending
           :artifact-checker #'physical-promotion-checkpoint-artifacts
           :publisher #'physical-promotion-complete-child))))
    t))

(declaim (ftype function wal-note-primary-ready))

(defun physical-promotion-live-xid-floor-valid-p (actual floor batch-end local-floor)
  "Check that FLOOR covers allocation state without skipping a wraparound gate.

ACTUAL is the replay visibility counter; BATCH-END and LOCAL-FLOOR bound local
allocation. All four values must be canonical counters or the exhaustion sentinel."
  (and (every (lambda (value)
                (and (typep value '(integer 3 18446744073709551616))
                     (= value (canonical-next-full-xid value))))
              (list actual floor batch-end local-floor))
       (<= (max actual batch-end local-floor) floor)
       (or (= (floor actual +xid-cycle+) (floor floor +xid-cycle+))
           (= floor (min +max-xid-one-past+ (xid-next-epoch-boundary actual))))))

(defun physical-promotion-read-physical-slots (&key reconcile-live)
  "Read the canonical physical-only disk inventory under promotion exclusion.
For live promotion require an exact registry match, normalizing only inactivity
time before comparing durable fields. Return the existing objects so creator and
runtime ownership survive; do not replace live generations."
  (let* ((directory (wal-replication-slot-directory))
         (entries (directory-entry-names directory))
         (paths (durable-state-file-paths directory "slot"))
         (slots nil))
    (unless (= (length entries) (length paths))
      (error "promotion found an unrecognized slot inventory entry"))
    (dolist (path paths)
      (let* ((name (pathname-name path))
             (slot (physical-slot-decode (read-bounded-regular-file path 2048 :stabilize t) name)))
        (unless (string= (file-namestring path) (format nil "~a.slot" name))
          (error "promotion found a noncanonical slot filename"))
        (when reconcile-live
          (let ((live (gethash name *wal-replication-slots*)))
            (unless (and (wal-replication-slot-p live) (wal-physical-slot-p live))
              (error "promotion slot registry differs from its physical inventory"))
            (setf (wal-replication-slot-inactive-since slot)
                  (wal-replication-slot-inactive-since live))
            (unless (equalp (physical-slot-encode slot) (physical-slot-encode live))
              (error "promotion slot durable state differs from its live generation"))
            (setf slot live)))
        (push slot slots)))
    (when (and reconcile-live (/= (length slots) (hash-table-count *wal-replication-slots*)))
      (error "promotion slot registry has unpersisted generations"))
    slots))

(defun physical-promotion-check-slot-prefix (slots state &key committed)
  "Check physical identity and prove reserved restart floors through STATE's fork.
Null and lost slots need no WAL coverage. With no archive, only the copied fork
segment is available. A committed child must contain the same fork prefix."
  (let* ((fork (getf state :applied))
         (system-id (getf state :system-id))
         (parent (physical-recovery-replay-timeline state))
         (history (timeline-check-lineage
                   (timeline-parse-history
                    parent (if (= parent 1) "" (bytes->string (timeline-history-bytes parent))))))
         (reference (getf state :retained-reference))
         (identity (when reference (physical-promotion-retained-identity system-id reference)))
         (first (* (floor fork +wal-segment-size+) +wal-segment-size+)))
    (when reference
      (physical-promotion-check-retained-reference system-id reference parent history :end fork))
    (dolist (slot slots)
      (physical-slot-check-retained-range
       slot system-id history fork
       (lambda (segment cursor)
         (declare (ignore cursor))
         (let* ((origin (* segment +wal-segment-size+))
                (bytes
                  (if reference
                      (physical-follow-retained-segment reference identity history segment)
                      (progn
                        (unless (= origin first)
                          (error "physical slot predates the copied promotion prefix"))
                        (let ((prefix (getf state :wal-prefix-bytes)))
                          (unless (and (typep prefix '(vector (unsigned-byte 8)))
                                       (= (length prefix) (- fork origin)))
                            (error "promotion lacks its exact saved fork prefix"))
                          prefix)))))
           (when (and committed (= origin first))
             (let ((child (physical-promotion-read-file
                           (wal-segment-path segment (getf state :promotion-child) :timeline))))
               (unless (and (<= (length bytes) (length child))
                            (equalp bytes (subseq child 0 (length bytes))))
                 (error "public slot prefix differs from committed child WAL"))))
           bytes))))
    t))

(defun physical-promotion-check-pending-slots (state)
  "Check persistent disk slots before pending promotion publishes child state.
Skip valid temporary images here; cold startup removes them before resource
checks. Live promotion separately validates and preserves temporary owners."
  (let ((slots (physical-promotion-read-physical-slots)))
    (physical-promotion-check-slot-prefix
     (remove-if #'wal-replication-slot-temporary-p slots) state)))

(defun physical-promotion-check-live-retention (fork)
  "Check live public physical slots and ordinary snapshot pins before promotion.
Phase 2 uses the standby batch proof; a committed child checks its fork prefix.
Logical slots and private source pins require separate ownership rules."
  (unless (and (zerop (hash-table-count *physical-source-pins*))
               (null (directory-entry-names (physical-source-pin-directory))))
    (error "live promotion requires an empty private source-pin inventory"))
  (let ((slots (physical-promotion-read-physical-slots :reconcile-live t)))
    (when slots
      (unless (and *physical-recovery-state*
                   (= fork (if (= 2 (getf *physical-recovery-state* :phase))
                               *last-replay-lsn*
                               (getf *physical-recovery-state* :applied))))
        (error "promotion physical slots require the exact applied fork"))
      (if (= 2 (getf *physical-recovery-state* :phase))
          (physical-slot-check-standby-slots slots (catalog-system-identifier))
          (physical-promotion-check-slot-prefix
           slots *physical-recovery-state* :committed t))))
  (let ((first (* (floor fork +wal-segment-size+) +wal-segment-size+)))
    (maphash (lambda (owner lsn)
               (declare (ignore owner))
               (unless (and (typep lsn '(unsigned-byte 64)) (<= first lsn fork))
                 (error "retained WAL predates the planned child prefix")))
             *wal-snapshot-retention-pins*)))

(defun physical-promotion-check-clog-tail (actual next)
  "Reject settled status at and above ACTUAL without changing CLOG.

ACTUAL is replay's next FullXID; NEXT is the planned allocation floor. At an
unchanged wraparound gate, keep bits from the previous cycle until readers drain
and the cycle is frozen. An exhausted ACTUAL has no future IDs to check.
This check does not repair stale bits as C TrimCLOG does."
  (unless (or (= actual +max-xid-one-past+)
              (and (= actual next) (first-normal-xid-of-epoch-p actual)))
    (sb-thread:with-mutex (*clog-lock*)
      (file-position *clog-stream* (clog-byte-index actual))
      (let* ((head (read-byte *clog-stream* nil 0))
             (shift (clog-bit-shift actual))
             (bytes (make-array 8192 :element-type '(unsigned-byte 8))))
        (unless (zerop (ash head (- shift)))
          (error "live promotion found settled future CLOG status"))
        (loop for count = (read-sequence bytes *clog-stream*)
              while (plusp count)
              do (unless (loop for index below count always (zerop (aref bytes index)))
                   (error "live promotion found settled future CLOG status")))))))

(defun physical-promotion-preflight-runtime (actual bound fork)
  "Check runtime state and return the canonical child allocation floor.

The floor covers actual next XID, complete source capacity and the local disk
floor.  Use this check before intent and again under the child namespace."
  (let* ((local-floor (load-next-xid))
         (next (canonical-next-full-xid (max actual (1+ bound) local-floor))))
    (unless (physical-promotion-live-xid-floor-valid-p actual next *xid-batch-end* local-floor)
      (error "promotion allocation floor skips its wraparound gate"))
    (physical-promotion-check-live-retention fork)
    (when *clog-stream* (physical-promotion-check-clog-tail actual next))
    next))

(defun physical-promotion-adopt-live-child-runtime ()
  "Adopt a verified committed child in the existing recovery runtime.

Require a clean, quiescent runtime and prove the checkpoint, history, pending
identity and saved catalog plan.  Persist the allocation floor under WAL then
XID lock before installing child scalars.  Keep catalog, CLOG, storage handles,
readers, retention state and the parent replay position.  Any failure fences
recovery; restart must classify durable control."
  (let ((finished nil))
    (unwind-protect
         (progn
           (physical-promotion-check-context 3 :preserve-readers t)
           (unless (and *postmaster-data-lock* (postmaster-pid-owned-p)
                        *clog-stream* (open-stream-p *clog-stream*)
                        (input-stream-p *clog-stream*) (output-stream-p *clog-stream*)
                        (not (and *physical-restartpoint-worker*
                                  (sb-thread:thread-alive-p *physical-restartpoint-worker*)))
                        (not *wal-poisoned*) (not *wal-output-stream*)
                        (not *wal-output-segment*) (null *wal-new-segments*)
                        (zerop (hash-table-count *wal-page-cache*))
                        (null *wal-dirty-store*)
                        (zerop (hash-table-count *wal-page-cache-xids*))
                        (not *wal-page-batch*) (not *wal-page-batch-xids*)
                        (not *wal-page-batch-split*) (not *wal-pending-deletes*)
                        (zerop (hash-table-count *wal-xid-first-lsn*))
                        (not *wal-pending-btree-split*)
                        (not *xid-wrap-pending*) (not *xid-wrap-failure*)
                        (not *xid-wrap-completed-through*)
                        (not (load-xid-wrap-marker)))
             (error "live child adoption requires owned clean recovery runtime"))
           (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline namespace metadata)
               (wal-read-control-fields)
             (multiple-value-bind (checked history)
                 (physical-promotion-check-child-control insertion checkpoint-timeline)
               (unless (equal snapshot checked) (error "child control changed during adoption"))
               (physical-promotion-check-retiring-control
                snapshot history (catalog-system-identifier)))
             (physical-promotion-restore-primary-retained)
             (let* ((next (getf snapshot :next-xid))
                    (proof (multiple-value-list
                            (physical-promotion-current-checkpoint-plan *physical-recovery-state*)))
                    (plan (first proof)))
               (unless (and (= checkpoint durable (getf snapshot :end) (getf plan :end))
                            (equal snapshot (getf plan :snapshot))
                            (equalp (catalog-image-bytes) (second proof))
                            (= *last-replay-lsn* (getf plan :fork))
                            (= *next-xid* (getf *physical-recovery-state* :next-xid)
                               (getf *physical-recovery-state* :observed-next-xid))
                            metadata (= (getf metadata :system-id) (catalog-system-identifier)
                                        (getf *physical-recovery-state* :system-id))
                            (= (getf metadata :db-state) +db-in-crash-recovery+)
                            (= insertion (getf *physical-recovery-state* :promotion-child))
                            (= (getf snapshot :next-oid) (catalog-next-oid *catalog*)))
                 (error "live child control differs from replayed identity or allocation floor"))
               (let ((*wal-segment-namespace* namespace)
                     (*wal-insert-timeline* insertion)
                     (*wal-checkpoint-timeline* checkpoint-timeline)
                     (*wal-checkpoint-snapshot* snapshot)
                     (*wal-checkpoint-lsn* checkpoint)
                     (*wal-durable-lsn* durable))
                 (unless (= next
                            (physical-promotion-preflight-runtime
                             *next-xid*
                             (getf (getf *physical-recovery-state* :source-reservation) :bound)
                             (getf *physical-recovery-state* :applied)))
                   (error "live child allocation proof differs from committed checkpoint")))
               (wal-ensure-directory-durable (wal-replication-slot-directory))
               (sb-thread:with-mutex (*wal-lock*)
                 (sb-thread:with-mutex (*xid-lock*)
                   (unless (null *active-xids*) (error "live child has real active transactions"))
                   (sb-thread:with-mutex (*snapshot-count-lock*)
                     (maphash (lambda (old present)
                                (when (and present (> (snapshot-xmax old) next))
                                  (error "live child floor precedes a retained snapshot")))
                              *active-snapshot-set*))
                   (let ((*xid-batch-end* next)) (save-next-xid))
                   (physical-promotion-test-point :after-live-child-xid-floor)
                   (sb-sys:without-interrupts
                     (setf *next-xid* next *xid-batch-end* next
                           *xid-wrap-completed-through* nil
                           *wal-segment-namespace* namespace
                           *wal-insert-timeline* insertion
                           *wal-checkpoint-timeline* checkpoint-timeline
                           *wal-prev-timeline* (getf snapshot :prev-timeline)
                           *wal-checkpoint-snapshot* (copy-list snapshot)
                           *wal-checkpoint-lsn* checkpoint *wal-durable-lsn* durable
                           *wal-end-lsn* durable *wal-xid-reserved-through* (1- next)
                           *wal-control-metadata* (copy-list metadata)))))
               (physical-promotion-test-point :after-live-child-runtime)
               (setf finished t)
               checkpoint)))
      (unless finished (setf *physical-recovery-failed-p* t)))))

(defun physical-promotion-finish-child-primary ()
  "Retire recovery triggers before clearing the recovery role and declaring readiness."
  (physical-promotion-test-point :after-child-primary-initialization)
  (physical-promotion-retire-recovery)
  (sb-thread:with-mutex (*wal-lock*)
    (wal-refresh-logical-capture-slot-mirror-under-wal-lock))
  (setf *physical-recovery-state* nil *physical-recovery-failed-p* nil
        *physical-replay-seen-relfilenodes* nil)
  (wal-note-primary-ready)
  (physical-promotion-test-point :after-child-primary-ready))

(defun physical-promotion-start-live-child-primary ()
  "Privately adopt the child, close the receive spool and finish promotion.

Keep the existing CLOG stream, storage handles and reader state. The completion
caller must keep admission closed and fence any failure."
  (physical-promotion-adopt-live-child-runtime)
  (when *physical-receive-store* (physical-store-close *physical-receive-store*))
  (setf *physical-receive-store* nil)
  (physical-promotion-finish-child-primary))

(defun physical-promotion-start-child-primary ()
  "Initialize a committed child as an ordinary primary under closed admission.
Register the primary retained root before storage initialization loads slots.
Close standby storage, then hide recovery only in the initializer's dynamic
scope; the outer pending role still excludes writers. Retire recovery triggers
before declaring the primary ready. On failure restart from durable child control."
  (physical-promotion-restore-primary-retained)
  (when *physical-receive-store* (physical-store-close *physical-receive-store*))
  (setf *physical-receive-store* nil)
  (when *clog-stream* (close *clog-stream*) (setf *clog-stream* nil))
  (smgr-close-all)
  (let ((*physical-recovery-state* nil)) (initialize-primary-storage))
  (physical-promotion-finish-child-primary))

(defun physical-promotion-complete-child ()
  "Commit child control, initialize the primary and return the checkpoint end.

Keep admission closed throughout.  The scoped reader-preservation flag selects
live adoption; otherwise initialize cold storage.  Any error or nonlocal exit
fences the process.  A new process must classify durable control before retrying."
  (let ((finished nil))
    (unwind-protect
         (let ((end (physical-promotion-publish-child-control)))
           (if *physical-promotion-preserve-readers-p*
               (physical-promotion-start-live-child-primary)
               (physical-promotion-start-child-primary))
           (setf finished t)
           end)
      (unless finished (setf *physical-recovery-failed-p* t)))))

(declaim (ftype function initialize-primary-storage))
