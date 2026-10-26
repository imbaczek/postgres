;;;; Committed child startup never restores the retired standby generation.
;;;; Startup must choose its authority before it opens ordinary recovery storage.
;;;; Parent control plus a pending request requires the exact saved generation.
;;;; Child control instead requires a matching child checkpoint and history; it
;;;; must not depend on the old generation or receive spool, which may be gone.
;;;;
;;;; Only the owning postmaster, before listeners or under closed admission, may
;;;; finish the transition. Once child control is verified, initialize storage as
;;;; an ordinary primary and retire recovery triggers. Remove pending control last
;;;; so an interrupted cleanup remains distinguishable from an ordinary standby.
;;;; A failure after child control is saved requires restart, not parent rollback.

(in-package #:pg)

(defun physical-promotion-check-child-control (insertion checkpoint-timeline)
  "Verify child control, its checkpoint record and timeline history without writes.
Return the checkpoint snapshot and parsed history. This check must precede WAL
scanning or tail repair. It deliberately needs no old generation, receive spool
or backup manifest: child control is now the recovery authority."
  (multiple-value-bind (checkpoint durable snapshot timeline checkpoint-id namespace) (wal-read-control-fields)
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
      (values snapshot history))))

(defun physical-promotion-check-retiring-control (snapshot history)
  "Check any remaining pending request against the committed child.
An absent request is normal after cleanup. If it remains, require the same
system identity, parent, child and fork, with no larger replay visibility counter.
Do not open its selected generation or receive spool."
  (let ((path (physical-recovery-control-path)))
    (when (wal-namespace-file-stat path t)
      (let* ((control (physical-recovery-read-control *pgdata*))
             (*catalog-identity-needs-persist-p* nil)
             (catalog (load-catalog)))
        (unless (and (eql 3 (getf control :phase))
                     (= (getf control :promotion-child) (getf snapshot :timeline))
                     (= (getf control :timeline) (getf snapshot :prev-timeline))
                     (= (getf control :applied) (timeline-history-entry-begin (first history)))
                     (<= (getf control :next-xid) (getf snapshot :next-xid))
                     (= (getf control :system-id) (catalog-system-identifier catalog)))
          (error "committed child contradicts its pending recovery metadata"))))))

(defun physical-promotion-retire-recovery ()
  "Remove recovery triggers after verifying committed child control.
Require the owning cold postmaster or closed admission under quiescence.
Check every name before deleting any, then unlink and sync the PGDATA directory
for each step. Remove pending control last. Leave old generation and receive
directories inert; they are no longer required for primary startup."
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
  (let ((names '(("recovery.required" :after-retire-required)
                 ("physical-backup.manifest" :after-retire-manifest)
                 ("physical-follow.control" :after-retire-follow)
                 ("physical-restartpoint.control" :after-retire-pointer)
                 ("physical-recovery.control" :after-retire-pending))))
    (dolist (entry names) (wal-namespace-file-stat (merge-pathnames (first entry) *pgdata*) t))
    (dolist (entry names)
      (let ((path (merge-pathnames (first entry) *pgdata*)))
        (when (wal-namespace-file-stat path t) (sb-posix:unlink (namestring path)))
        (physical-promotion-test-point (second entry))
        (fsync-directory *pgdata*))))
  (values))

(defun physical-promotion-startup-primary-p ()
  "Recognize committed child control before ordinary standby preparation.
Finish interrupted trigger cleanup and return true. Return NIL for parent
control. Invalid child control is an error; never fall back to parent recovery."
  (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline) (wal-read-control-fields)
    (declare (ignore checkpoint durable snapshot))
    (when (and insertion (> insertion 1))
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

(defun physical-promotion-start-child-primary ()
  "Initialize a committed child as an ordinary primary under closed admission.
Close standby storage first. Hide the recovery role only in the initializer's
dynamic scope; the outer pending role still excludes writers. Retire recovery
triggers before clearing that outer role and declaring the primary ready.
The caller must fence any failure and restart from durable child control."
  (when *physical-receive-store* (physical-store-close *physical-receive-store*))
  (setf *physical-receive-store* nil)
  (when *clog-stream* (close *clog-stream*) (setf *clog-stream* nil))
  (smgr-close-all)
  (let ((*physical-recovery-state* nil)) (initialize-primary-storage))
  (physical-promotion-test-point :after-child-primary-initialization)
  (physical-promotion-retire-recovery)
  (setf *physical-recovery-state* nil *physical-recovery-failed-p* nil
        *physical-replay-seen-relfilenodes* nil)
  (physical-promotion-test-point :after-child-primary-ready))

(defun physical-promotion-complete-child ()
  "Commit child control, initialize the primary and return the checkpoint end.
Keep admission closed through this entire transition. Any error or nonlocal
exit fences the process, including failures after the irreversible control
replacement. A new process must classify durable control before retrying."
  (let ((finished nil))
    (unwind-protect
         (let ((end (physical-promotion-publish-child-control)))
           (physical-promotion-start-child-primary)
           (setf finished t)
           end)
      (unless finished (setf *physical-recovery-failed-p* t)))))

(declaim (ftype function initialize-primary-storage))
