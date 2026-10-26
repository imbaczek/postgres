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

(deftest last-backend-defers-finalization-outside-admission
    (let ((pg::*shutdown* pg::+smart-shutdown+)
          (pg::*shutdown-finished* nil)
          (pg::*shutdown-finalizing-p* nil)
          (pg::*accept-stop-requested* nil)
          (pg::*n-connections* 1)
          (pg::*live-operation-depth* 1))
      (pg::connection-finished)
      (ok (zerop pg::*n-connections*))
      (ok pg::*shutdown-finalizing-p*)
      (ok pg::*accept-stop-requested*)
      (ok (not pg::*shutdown-finished*))
      ;; Repeated zero-connection observations only retain the request.
      (pg::finish-shutdown)
      (ok (not pg::*shutdown-finished*))))

(deftest postmaster-finalization-prepares-recovery-before-finished
    (let ((pg::*shutdown-finalizing-p* t)
          (pg::*shutdown-finished* nil)
          (pg::*physical-recovery-state* '(:phase 1 :end 42))
          (pg::*live-operation-depth* 0)
          (original (symbol-function 'pg::physical-recovery-prepare-shutdown))
          (calls 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-recovery-prepare-shutdown)
                   (lambda ()
                     (ok (zerop pg::*live-operation-depth*))
                     (ok (not pg::*shutdown-finished*))
                     (incf calls)))
             (pg::prepare-postmaster-shutdown)
             (ok pg::*shutdown-finished*)
             (ok (= calls 1))
             (pg::prepare-postmaster-shutdown)
             (ok (= calls 1)))
        (setf (symbol-function 'pg::physical-recovery-prepare-shutdown) original))))

(deftest postmaster-finalization-failure-never-reports-finished
    (let ((pg::*shutdown-finalizing-p* t)
          (pg::*shutdown-finished* nil)
          (pg::*physical-recovery-state* '(:phase 1 :end 42))
          (pg::*live-operation-depth* 0)
          (original (symbol-function 'pg::physical-recovery-prepare-shutdown)))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-recovery-prepare-shutdown)
                   (lambda () (error "shutdown preparation failpoint")))
             (ok (handler-case (progn (pg::prepare-postmaster-shutdown) nil)
                   (error () t)))
             (ok (not pg::*shutdown-finished*)))
        (setf (symbol-function 'pg::physical-recovery-prepare-shutdown) original))))

(deftest postmaster-finalization-rejects-admitted-or-unrequested-callers
    (let ((pg::*shutdown-finalizing-p* t)
          (pg::*shutdown-finished* nil)
          (pg::*live-operation-depth* 1))
      (ok (handler-case (progn (pg::prepare-postmaster-shutdown) nil)
            (error () t)))
      (ok (not pg::*shutdown-finished*)))
  (let ((pg::*shutdown-finalizing-p* nil)
        (pg::*shutdown-finished* nil)
        (pg::*live-operation-depth* 0))
    (ok (handler-case (progn (pg::prepare-postmaster-shutdown) nil)
          (error () t)))))

(deftest process-shutdown-between-startup-worker-checks-is-normal
    (dolist (boundary '(1 2))
      (let ((pg::*shutdown* pg::+no-shutdown+)
            (pg::*physical-shutdown-state* nil)
            (pg::*physical-recovery-failed-p* nil)
            (follow (symbol-function 'pg::physical-follow-autostart))
            (maintenance (symbol-function 'pg::physical-restartpoint-start-worker))
            (calls 0))
        (flet ((start ()
                 (when (= (incf calls) boundary)
                   (setf pg::*shutdown* pg::+fast-shutdown+))
                 (pg::check-physical-recovery-worker-start)))
          (unwind-protect
               (progn
                 (setf (symbol-function 'pg::physical-follow-autostart) #'start
                       (symbol-function 'pg::physical-restartpoint-start-worker) #'start)
                 (pg::start-postmaster-recovery-workers)
                 (ok (= calls boundary))
                 (ok (pg::shutdown-in-progress-p)))
            (setf (symbol-function 'pg::physical-follow-autostart) follow
                  (symbol-function 'pg::physical-restartpoint-start-worker) maintenance))))))

(deftest startup-handoff-preserves-nonshutdown-errors
    (let ((follow (symbol-function 'pg::physical-follow-autostart))
          (pg::*shutdown* pg::+fast-shutdown+))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-follow-autostart)
                   (lambda () (pg::ereport "55000" "unrelated startup error")))
             (ok (handler-case (progn (pg::start-postmaster-recovery-workers) nil)
                   (pg::pg-error (condition)
                     (string= "unrelated startup error" (pg::pg-error-message condition))))))
        (setf (symbol-function 'pg::physical-follow-autostart) follow)))
  (let ((pg::*shutdown* pg::+no-shutdown+)
        (pg::*physical-shutdown-state* :prepared)
        (pg::*physical-recovery-failed-p* nil))
    (ok (handler-case (progn (pg::start-postmaster-recovery-workers) nil)
          (pg::physical-recovery-worker-shutdown () t)))))
