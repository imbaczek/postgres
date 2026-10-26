(in-package #:pg)

(defun physical-promotion-check-online ()
  (check-recovery-available)
  (when (shutdown-in-progress-p) (error 'physical-promotion-shutdown))
  (unless (and *postmaster-data-lock* (postmaster-pid-owned-p)
               (eql 2 (getf *physical-recovery-state* :phase))
               *physical-receive-store* *wal-ready*
               (not *physical-shutdown-state*) (not (shutdown-in-progress-p))
               (not *swank-listener-enabled-p*))
    (error "online promotion requires an owned healthy streaming standby")))

(defun physical-promotion-try-online ()
  (sb-thread:with-mutex (*reload-run-lock*)
    (with-live-operation-quiescence
      (physical-promotion-check-online)
      (when (physical-recovery-reader-conflict-p)
        (return-from physical-promotion-try-online :deferred))
      (physical-recovery-flush)
      (physical-recovery-replay-core)
      (physical-recovery-restartpoint-core)
      (physical-promotion-test-point :before-online-intent)
      (physical-promotion-publish-intent)
      (physical-promotion-test-point :after-online-intent)
      (physical-promotion-install-prefix)
      (prog1 (physical-promotion-complete-child)
        (setf *physical-promotion-stopping-p* nil)))))

(defun physical-recovery-promote ()
  (when (plusp *live-operation-depth*)
    (error "online promotion must run outside operation admission"))
  (sb-thread:with-mutex (*physical-shutdown-lock*)
    (sb-thread:with-mutex (*physical-follow-command-lock*)
      (with-live-operation (physical-promotion-check-online))
      (let ((finished nil))
        (unwind-protect
             (handler-case
                 (progn
                   (with-live-operation
                     (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
                       (setf *physical-promotion-stopping-p* t))
                     (sb-thread:with-mutex (*physical-replay-pause-lock*)
                       (setf *physical-replay-paused-p* nil)))
                   (physical-follow-quiesce-runtime)
                   (physical-restartpoint-join-for-shutdown)
                   (physical-promotion-test-point :after-online-worker-join)
                   (loop for result = (physical-promotion-try-online)
                         unless (eq result :deferred)
                           do (setf finished t) (return result)
                         do (sleep 0.05)))
               (physical-promotion-shutdown (condition)
                 (unless (and (shutdown-in-progress-p)
                              (eql 2 (getf *physical-recovery-state* :phase))
                              (not *physical-recovery-failed-p*))
                   (error condition))
                 (setf finished t)
                 :shutdown))
          (unless finished (setf *physical-recovery-failed-p* t)))))))

(defun request-physical-promotion (&rest signal-info)
  (declare (ignore signal-info))
  (setf *physical-promotion-requested-p* t))

(defun physical-promotion-consume-signal ()
  (let ((path (merge-pathnames "promote" *pgdata*)))
    (when (wal-namespace-file-stat path t)
      (sb-posix:unlink (namestring path))
      (fsync-directory *pgdata*)
      t)))

(defun physical-promotion-worker-step ()
  (when *physical-promotion-requested-p*
    (setf *physical-promotion-requested-p* nil)
    (handler-case
        (when (physical-promotion-consume-signal)
          (admin-log "received promote request")
          (physical-recovery-promote))
      (error (condition)
        (admin-log "native promotion failed: ~a" condition)))))

(defun physical-promotion-worker-loop ()
  (loop while (and (recovery-in-progress-p) (not (shutdown-in-progress-p))
                   (not *physical-recovery-failed-p*))
        do (physical-promotion-worker-step)
           (sleep 0.05)))

(defun physical-promotion-initialize-requests ()
  (physical-promotion-consume-signal)
  (setf *physical-promotion-requested-p* nil))

(defun physical-promotion-start-worker ()
  (check-physical-recovery-worker-start)
  (when (and (recovery-in-progress-p)
             (not (and *physical-promotion-worker*
                       (sb-thread:thread-alive-p *physical-promotion-worker*))))
    (setf *physical-promotion-worker*
          (sb-thread:make-thread #'physical-promotion-worker-loop
                                 :name "native standby promotion"))))
