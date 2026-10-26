;;;; Local pending-promotion publication; EOR and writer admission remain separate.
(in-package #:pg)

(defun physical-promotion-check-context ()
  "The caller owns follow-command -> reload -> quiescence; no worker joins here."
  (unless (and (zerop *live-operation-depth*)
               (sb-thread:holding-mutex-p *physical-follow-command-lock*)
               (sb-thread:holding-mutex-p *reload-run-lock*)
               (not (sb-thread:holding-mutex-p *physical-restartpoint-start-lock*))
               (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
               (not *live-operation-drain-open*))
    (error "pending promotion requires owned command/reload exclusion and quiescence"))
  (check-recovery-available)
  (unless (and (eql 2 (getf *physical-recovery-state* :phase))
               *physical-receive-store* *wal-ready*
               (not *physical-follow-runtime*) (not *physical-shutdown-state*)
               (not (shutdown-in-progress-p)) (not *swank-listener-enabled-p*)
               (not (physical-recovery-reader-conflict-p)))
    (error "pending promotion requires stopped workers and no open reader transactions")))

(defun physical-promotion-select-child (parent)
  "Match C findNewestTimeLine's consecutive local history probe, then select +1."
  (unless (typep parent '(integer 1 4294967295)) (error "invalid promotion parent timeline"))
  (loop for child from (1+ parent) to 4294967295
        unless (wal-namespace-file-stat (timeline-history-path child *pgdata*) t)
          do (return-from physical-promotion-select-child child))
  (error "no representable child timeline remains"))

(defun physical-promotion-check-base-control (state)
  (let ((entry (find "global/pg_wal_control" (getf state :files) :key #'first :test #'equal)))
    (unless entry (error "promotion lost immutable base WAL control provenance"))
    (recovery-check-manifest-files (list :files (list entry)) *pgdata*))
  (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline namespace)
      (wal-read-control-fields)
    (declare (ignore snapshot))
    (unless (and (= checkpoint (getf state :checkpoint)) (= durable (getf state :end))
                 (= insertion checkpoint-timeline *wal-insert-timeline* *wal-checkpoint-timeline* 1)
                 (eq namespace (getf state :wal-namespace))
                 (eq namespace *wal-segment-namespace*))
      (error "promotion base WAL identity or namespace changed"))
    namespace))

(defun physical-promotion-check-progress (state metadata prefix generation)
  (let* ((fork *last-replay-lsn*) (next *next-xid*)
                                  (control (physical-recovery-read-control *pgdata*))
                                  (snapshot (getf metadata :source-checkpoint))
                                  (reservation (getf metadata :source-reservation)))
    (unless (and metadata prefix (eql 4 (getf metadata :version))
                 (typep generation '(unsigned-byte 64))
                 (= fork (getf metadata :lsn) (getf control :applied)
                    (getf state :required-applied))
                 (= next (getf metadata :next-xid) (getf control :next-xid)
                    (getf state :observed-next-xid))
                 (eql 2 (getf control :phase))
                 (every (lambda (key) (eql (getf state key) (getf control key)))
                        '(:system-id :timeline :manifest-digest :checkpoint))
                 snapshot (wal-checkpoint-snapshot-valid-p snapshot)
                 reservation (physical-source-reservation-valid-p reservation)
                 (getf reservation :complete) (= fork (getf reservation :through))
                 (<= next (1+ (getf reservation :bound))))
      (error "promotion lacks exact durable restartpoint, checkpoint or allocation proof"))
    (unless (and (= (physical-store-system-id *physical-receive-store*) (getf state :system-id))
                 (= (physical-store-timeline *physical-receive-store*) (getf state :timeline))
                 (= (physical-store-base-lsn *physical-receive-store*) (getf state :end))
                 (= (physical-store-written-lsn *physical-receive-store*)
                    (physical-store-flushed-lsn *physical-receive-store*)))
      (error "promotion requires matching receive identity and flushed complete writes"))
    (unless (= (getf metadata :prefix-digest)
               (physical-store-prefix-digest *physical-receive-store* fork))
      (error "promotion restartpoint differs from durable receive history"))
    ;; Include all acknowledged receive records. A complete later terminal must
    ;; be applied first; an unfinished native batch does not move the data fork.
    (multiple-value-bind (removed terminal)
        (physical-recovery-proven-drops
         *physical-receive-store* (getf state :cluster-identity) :required-lsn fork)
      (declare (ignore removed))
      (unless (= fork terminal)
        (error "promotion must replay complete durable receive backlog")))
    (values fork next)))

(defun physical-promotion-publish-intent (&optional requested-child)
  "Privately bind a chosen child to verified local recovery; never grant writer authority.
Worker shutdown/join and rejection recovery belong to the future public orchestrator.
The caller owns PGDATA and follow-command/reload/quiescence in that order."
  (physical-promotion-check-context)
  ;; Acquire registration exclusion only AFTER reload/drain, matching streaming
  ;; setup. Do not join workers while holding this mutex.
  (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
    (when (and *physical-restartpoint-worker*
               (sb-thread:thread-alive-p *physical-restartpoint-worker*))
      (error "promotion restartpoint worker has not been joined"))
    (sb-thread:with-mutex (*timeline-history-lock*)
      (let* ((state *physical-recovery-state*)
             (namespace (physical-promotion-check-base-control state))
             (child (physical-promotion-select-child (getf state :timeline))))
        (when (and requested-child (not (eql requested-child child)))
          (error "requested promotion child is not the next local timeline"))
        (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read state)
          (declare (ignore catalog))
          (multiple-value-bind (fork next) (physical-promotion-check-progress state metadata prefix generation)
            (let* ((time (- (get-universal-time) 2208988800))
                   (bytes (physical-recovery-control-image
                           3 (getf state :system-id) (getf state :timeline)
                           (getf state :manifest-digest) (getf state :checkpoint) fork next
                           :state-time time :promotion-child child :promotion-generation generation
                           :promotion-source-namespace namespace))
                   (pending (copy-list state)) (finished nil))
              (loop for (key value) on (physical-recovery-decode-control bytes) by #'cddr
                    do (setf (getf pending key) value))
              (setf (getf pending :required-applied) fork (getf pending :observed-next-xid) next
                    (getf pending :wal-prefix-lsn) fork (getf pending :wal-prefix-bytes) prefix
                    (getf pending :source-checkpoint) (copy-list (getf metadata :source-checkpoint))
                    (getf pending :source-reservation) (copy-list (getf metadata :source-reservation)))
              (unwind-protect
                   (progn
                     (physical-recovery-persist-control-bytes bytes +db-in-archive-recovery+)
                     (setf *physical-recovery-state* pending)
                     (physical-recovery-control-test-point :after-write +db-in-archive-recovery+)
                     (setf finished t)
                     pending)
                (unless finished (setf *physical-recovery-failed-p* t))))))))))
