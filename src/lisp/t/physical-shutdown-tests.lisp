;;;; Local standby shutdown preparation; no process exit or coordination.
(in-package #:pg-physical-tests)

(deftest prepared-standby-rejects-new-work-and-worker-starts
    (let ((pg::*physical-shutdown-state* :prepared)
          (pg::*physical-recovery-failed-p* nil))
      (dolist (function (list (lambda () (pg::physical-recovery-receive 0 (empty-octets)))
                              #'pg::physical-recovery-flush #'pg::physical-recovery-replay
                              #'pg::physical-recovery-checkpoint
                              #'pg::physical-recovery-enable-streaming
                              #'pg::physical-restartpoint-start-worker
                              #'pg::physical-follow-stop))
        (ok (equal "55000" (handler-case (progn (funcall function) nil)
                             (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))))))

(deftest process-shutdown-prevents-recovery-worker-creation
    (let ((pg::*physical-shutdown-state* nil)
          (pg::*physical-recovery-failed-p* nil)
          (pg::*shutdown* pg::+smart-shutdown+))
      (dolist (function (list #'pg::physical-restartpoint-start-worker #'pg::physical-follow-autostart))
        (ok (equal "55000" (handler-case (progn (funcall function) nil)
                             (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))))))

(deftest immutable-base-shutdown-is-idempotent-and-preserves-pause
    (let ((pg::*physical-shutdown-state* nil)
          (pg::*physical-recovery-failed-p* nil)
          (pg::*physical-recovery-state* '(:phase 1 :end 42))
          (pg::*physical-receive-store* nil)
          (pg::*physical-follow-runtime* nil)
          (pg::*physical-restartpoint-worker* nil)
          (pg::*swank-listener-enabled-p* nil)
          (pg::*physical-replay-paused-p* t)
          (pg::*last-replay-lsn* 42))
      (ok (= 42 (pg::physical-recovery-prepare-shutdown)))
      (ok (eq :prepared pg::*physical-shutdown-state*))
      (ok pg::*physical-replay-paused-p*)
      (ok (pg::recovery-in-progress-p))
      (ok (= 42 (pg::physical-recovery-prepare-shutdown)))))

(deftest older-maintenance-loop-exits-at-a-safe-step-boundary
    (let ((ready (sb-thread:make-semaphore :count 0)))
      (let ((worker (sb-thread:make-thread
                     (lambda ()
                       (sb-thread:wait-on-semaphore ready)
                       (let ((pg::*physical-shutdown-state* :stopping)
                             (pg::*physical-restartpoint-worker* sb-thread:*current-thread*))
                         ;; Model an old loop that does not inspect the new phase.
                         (loop repeat 3 do (pg::physical-restartpoint-worker-step))
                         :unexpected)))))
        (sb-thread:signal-semaphore ready)
        (ok (eq :stopped (sb-thread:join-thread worker :timeout 5 :default :timeout))))))

(deftest shutdown-preparation-rejects-admitted-callers
    (let ((pg::*live-operation-depth* 1))
      (ok (handler-case (progn (pg::physical-recovery-prepare-shutdown) nil)
            (error () t)))))

(deftest shutdown-publication-between-entry-and-quiescence-rejects-queued-work
    ;; Simulate another caller publishing PREPARED after the outer check passed.
    ;; No file work is needed: absent the inner check replay would return PAUSED,
    ;; and an automatic restartpoint would return NOT-DUE.
    (dolist (operation (list #'pg::physical-recovery-replay
                             (lambda () (pg::physical-recovery-restartpoint :automatic t))))
      (let ((pg::*physical-shutdown-state* nil)
            (pg::*physical-recovery-failed-p* nil)
            (pg::*physical-recovery-state* '(:phase 2))
            (pg::*physical-receive-store*
              (pg::%make-physical-store :written-lsn 0 :flushed-lsn 0 :floor-lsn 0))
            (pg::*last-replay-lsn* 0)
            (pg::*physical-replay-paused-p* t)
            (pg::*swank-listener-enabled-p* nil)
            (original (symbol-function 'pg::check-physical-recovery-work))
            (checks 0))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::check-physical-recovery-work)
                     (lambda ()
                       (incf checks)
                       (prog1 (funcall original)
                         (when (= checks 1) (setf pg::*physical-shutdown-state* :prepared)))))
               (ok (equal "55000" (handler-case (progn (funcall operation) nil)
                                    (pg::pg-error (condition) (pg::pg-error-sqlstate condition)))))
               (ok (= 2 checks))
               (ok (not pg::*physical-recovery-failed-p*)))
          (setf (symbol-function 'pg::check-physical-recovery-work) original)))))
