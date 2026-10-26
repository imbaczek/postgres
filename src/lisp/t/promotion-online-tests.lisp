(in-package #:pg-physical-tests)

(deftest promotion-stops-worker-starts-without-pretending-to-shutdown
    (let ((pg::*physical-promotion-stopping-p* t)
          (pg::*physical-shutdown-state* nil)
          (pg::*physical-recovery-state* '(:phase 2))
          (pg::*physical-recovery-failed-p* nil))
      (ok (intake-fails-p #'pg::check-physical-recovery-worker-start))
      (pg::check-physical-recovery-work)
      (ok (null pg::*physical-shutdown-state*))
      (dolist (name '("pg_wal_replay_pause" "pg_wal_replay_resume"))
        (let ((condition (handler-case (pg::physical-replay-pause-command name)
                           (pg::pg-error (condition) condition))))
          (ok (typep condition 'pg::pg-error))
          (ok (string= "55000" (pg::pg-error-sqlstate condition)))
          (ok (string= "standby promotion is ongoing" (pg::pg-error-message condition)))))))

(deftest promotion-rechecks-read-only-for-idle-connections
    (let* ((pg::*physical-recovery-state* nil)
           (backend (pg::make-backend :state :idle :read-only t)))
      (ok (not (pg::backend-transaction-read-only-p backend)))
      (setf (pg::backend-state backend) :in-progress)
      (ok (pg::backend-transaction-read-only-p backend))
      (setf (pg::backend-read-only backend) nil)
      (ok (not (pg::backend-transaction-read-only-p backend)))
      (let ((pg::*physical-recovery-state* '(:phase 2)))
        (ok (pg::backend-transaction-read-only-p backend)))))

(deftest replay-core-requires-its-own-closed-gate
    (ok (intake-fails-p #'pg::physical-recovery-replay-core)))
