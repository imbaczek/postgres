;;;; Cold reconstruction of pending promotion; writer admission remains closed.
(in-package #:pg)

(defun physical-promotion-check-cold-context ()
  "Require owned PGDATA and cold storage under command/reload quiescence."
  (unless (and *postmaster-data-lock* (open-stream-p *postmaster-data-lock*)
               (postmaster-pid-owned-p)
               (equal (truename *pgdata*) *postmaster-pid-directory*)
               (zerop *live-operation-depth*) (zerop *n-backends*)
               (sb-thread:holding-mutex-p *physical-follow-command-lock*)
               (sb-thread:holding-mutex-p *reload-run-lock*)
               (not (sb-thread:holding-mutex-p *physical-restartpoint-start-lock*))
               (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
               (not *live-operation-drain-open*)
               (not *wal-ready*) (not *wal-output-stream*) (not *catalog*)
               (not *clog-stream*) (zerop (hash-table-count *relation-streams*))
               (not *physical-recovery-state*) (not *physical-receive-store*)
               (not *physical-recovery-failed-p*) (not *physical-follow-runtime*)
               (not *physical-restartpoint-worker*) (not *physical-shutdown-state*)
               (not (shutdown-in-progress-p)) (not *swank-listener-enabled-p*))
    (error "pending reconstruction requires a cold process owning PGDATA and exclusion")))

;;; Pending reconstruction keeps its own copy of the selected generation's
;;; membership, including unknown coverage when that optional field is absent.

(defun physical-promotion-read-pending-state ()
  "Read provenance and exact selected generation before opening or restoring storage."
  (physical-recovery-check-required-marker)
  (let ((control (physical-recovery-read-control *pgdata*)))
    (unless (and (eql 4 (getf control :control-version)) (eql 3 (getf control :phase)))
      (error "pending reconstruction requires v4 phase 3"))
    (multiple-value-bind (manifest digest) (recovery-read-manifest *pgdata*)
      (recovery-check-manifest-schema manifest)
      (unless (and (= digest (getf control :manifest-digest))
                   (= (getf manifest :system-id) (getf control :system-id)
                      (physical-store-update-digest #xCBF29CE484222325
                                                    (string->bytes (getf manifest :cluster-identity))))
                   (= (getf manifest :timeline) (getf control :timeline))
                   (= (getf manifest :checkpoint) (getf control :checkpoint)))
        (error "pending promotion differs from immutable base provenance"))
      (physical-recovery-check-streaming-base manifest control)
      (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline namespace)
          (wal-read-control-fields)
        (unless (and (= checkpoint (getf manifest :checkpoint)) (= durable (getf manifest :end))
                     (= insertion checkpoint-timeline (getf manifest :timeline))
                     (eq namespace (getf control :promotion-source-namespace))
                     (= (physical-manifest-checkpoint-start manifest)
                        (if snapshot (getf snapshot :start) (- checkpoint +wal-header-size+))))
          (error "pending promotion base WAL control changed"))
        (let ((state (copy-list manifest)))
          (loop for (key value) on control by #'cddr do (setf (getf state key) value))
          (setf (getf state :wal-namespace) namespace
                (getf state :required-applied) (getf control :applied)
                (getf state :required-timeline) (getf control :applied-timeline (getf control :timeline))
                (getf state :replay-timeline) (getf control :applied-timeline (getf control :timeline))
                (getf state :observed-next-xid) (getf control :next-xid))
          (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read state)
            (declare (ignore catalog))
            (unless (and metadata prefix (eql 4 (getf metadata :version))
                         (eql generation (getf control :promotion-generation))
                         (= (getf metadata :lsn) (getf control :applied))
                         (= (getf metadata :next-xid) (getf control :next-xid)))
              (error "pending promotion lost its exact selected generation"))
            (setf (getf state :wal-prefix-lsn) (getf metadata :lsn)
                  (getf state :wal-prefix-bytes) prefix
                  (getf state :source-checkpoint) (copy-list (getf metadata :source-checkpoint))
                  (getf state :source-reservation) (copy-list (getf metadata :source-reservation))
                  (getf state :source-xids) (copy-tree (getf metadata :source-xids)))
            (physical-promotion-check-pending-retained control metadata)
            (values state metadata prefix generation)))))))

(defun physical-promotion-open-pending-store (state)
  "Verify acknowledged files, then own the spool without creating or trimming.

Use the verified pending base identity and the recovery retained archive."

  (let* ((directory (physical-recovery-spool-directory))
         (probe (%make-physical-store :directory directory :system-id (getf state :system-id)
                                      :timeline (getf state :timeline)
                                      :retained-identity (physical-recovery-retained-identity state)
                                      :retained-directory-value (physical-recovery-retained-directory))))
    (wal-namespace-file-stat (physical-store-path probe "receive.control"))
    (wal-namespace-file-stat (physical-store-path probe "receive.lock"))
    (physical-store-load-control probe)
    (unless (= (physical-store-base-lsn probe) (getf state :end))
      (error "pending receive base differs"))
    (physical-recovery-check-history-reference probe (getf state :history-target) (getf state :history-digest))
    (if (physical-store-selection probe)
        (dolist (entry (physical-store-history-segment-entries probe)) (wal-namespace-file-stat (third entry)))
        (let ((floor (physical-store-floor-lsn probe)) (flush (physical-store-flushed-lsn probe)))
          (when (< floor flush)
            (loop with first = (floor floor +wal-segment-size+)
                  for segment from first to (floor (1- flush) +wal-segment-size+)
                  for stat = (wal-namespace-file-stat (physical-store-segment-path probe segment) t)
                  unless (or stat (= segment first)) do (error "missing acknowledged pending segment")))))
    (physical-store-open directory (getf state :system-id) (getf state :timeline)
                         :discard-tail nil :create-lock nil
                         :retained-identity (physical-recovery-retained-identity state)
                         :retained-directory (physical-recovery-retained-directory))))

(defun physical-promotion-validate-cold-state (state metadata prefix generation store &optional (artifact-checker #'physical-promotion-wal-plan))
  "Use the live publisher's proof checker without installing candidate globals."
  (let ((*physical-recovery-state* state) (*physical-receive-store* store)
                                          (*last-replay-lsn* (getf state :applied)) (*next-xid* (getf state :next-xid))
                                          (*wal-segment-namespace* (getf state :wal-namespace))
                                          (*wal-insert-timeline* (getf state :timeline))
                                          (*wal-checkpoint-timeline* (getf state :timeline)))
    (physical-promotion-check-pending state metadata prefix generation :allow-history t)
    (funcall artifact-checker state prefix :allow-uninstalled t)
    (physical-recovery-check-base-forks
     (list :files (getf metadata :forks)) (make-hash-table :test #'eql))))

(defun physical-promotion-restore-cold-state (state store)
  "Restore only the selected restartpoint; never redo or publish ordinary progress."
  (setf *physical-recovery-state* state *physical-receive-store* store
        *wal-segment-namespace* (getf state :wal-namespace)
        *wal-insert-timeline* (getf state :timeline)
        *wal-checkpoint-timeline* (getf state :timeline)
        *wal-checkpoint-lsn* (getf state :checkpoint)
        *wal-durable-lsn* (getf state :end) *wal-end-lsn* (getf state :end)
        *wal-checkpoint-snapshot* (copy-list (getf state :source-checkpoint))
        *last-replay-lsn* (getf state :applied) *next-xid* (getf state :next-xid)
        *xid-batch-end* *next-xid* *active-xids* nil *wal-ready* t *wal-poisoned* nil
        (getf *physical-recovery-state* :receive-lsn) (physical-store-flushed-lsn store))
  (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read state)
    (declare (ignore catalog prefix generation))
    (setf (getf *physical-recovery-state* :timeline-policy)
          (physical-promotion-pending-timeline-policy state metadata store)))
  (physical-restartpoint-prepare-storage store)
  (setf *clog-stream* (open (clog-path) :direction :input :element-type '(unsigned-byte 8)
                                        :if-does-not-exist :error))
  (physical-recovery-check-live-files)
  (catalog-refresh-oldest-relfrozenxid))

(defun physical-promotion-resume-pending (&key (artifact-checker #'physical-promotion-wal-plan)
                                            (publisher #'physical-promotion-publish-wal))
  "Reconstruct a cold phase-3 process from its exact saved recovery proof.
Validate identity, generation, receive progress and child artifacts before catalog
restoration. Restore any selected pending archive owner before restoring the
catalog. A missing root or object stops reconstruction before mutation.
The default callbacks finish only history/EOR publication and retain
recovery exclusion. Normal startup supplies the checkpoint artifact checker and
child completion publisher instead. ARTIFACT-CHECKER receives STATE, PREFIX and
:ALLOW-UNINSTALLED T under restartpoint-start and history locks. The zero-argument
PUBLISHER runs after those locks are released, still under caller-owned command
and reload quiescence. Successful resources remain owned. On failure, fence the
process and close resources opened here. Retry in a fresh process rather than
reuse partially restored state."
  (physical-promotion-check-cold-context)
  (let ((store nil) (finished nil))
    (unwind-protect
         (progn
           (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
             (sb-thread:with-mutex (*timeline-history-lock*)
               (multiple-value-bind (state metadata prefix generation) (physical-promotion-read-pending-state)
                 (setf store (physical-promotion-open-pending-store state))
                 (physical-promotion-validate-cold-state state metadata prefix generation store artifact-checker)
                 (physical-promotion-ensure-pending-retained state metadata store)
                 (physical-promotion-test-point :before-pending-restore)
                 (physical-promotion-restore-cold-state state store)
                 (physical-promotion-test-point :after-pending-restore))))
           (unless (wal-namespace-file-stat
                    (timeline-history-path (getf *physical-recovery-state* :promotion-child) *pgdata*) t)
             (physical-promotion-install-prefix))
           (prog1 (funcall publisher) (setf finished t)))
      (unless finished
        (setf *physical-recovery-failed-p* t)
        (when *clog-stream* (ignore-errors (close *clog-stream*)))
        (when store (physical-store-close store))
        (setf *clog-stream* nil *physical-receive-store* nil)))))
