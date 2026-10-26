;;;; Committed child startup never restores the retired standby generation.
(in-package #:pg)

(defun physical-promotion-check-child-control (insertion checkpoint-timeline)
  "Read-only acceptance of a standalone child checkpoint, before any scan or repair."
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
  "Retired pending metadata may be absent; if present it must agree with commitment."
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
  "Retire triggers only after child control commits; pending control is removed last."
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
  "Classify child control before any standby preparation, including cleanup retries."
  (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline) (wal-read-control-fields)
    (declare (ignore checkpoint durable snapshot))
    (when (and insertion (> insertion 1))
      (wal-require-supported-timelines insertion checkpoint-timeline)
      (physical-promotion-retire-recovery)
      t)))

(defun physical-promotion-startup-pending-p ()
  "Complete a durable pending request in the owning cold postmaster before listeners."
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
  "Use the ordinary primary initializer under closed admission after irreversible control."
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
  "Commit child control and initialize it; any uncertainty requires process restart."
  (let ((finished nil))
    (unwind-protect
         (let ((end (physical-promotion-publish-child-control)))
           (physical-promotion-start-child-primary)
           (setf finished t)
           end)
      (unless finished (setf *physical-recovery-failed-p* t)))))

(declaim (ftype function initialize-primary-storage))
