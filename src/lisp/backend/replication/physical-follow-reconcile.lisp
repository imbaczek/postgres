(in-package #:pg)

(defvar *physical-follow-desired-applied* nil)
(defvar *physical-follow-manual-generation* nil)
(defvar *physical-follow-desired-worker* nil)
(defvar *physical-follow-desired-stopping-p* nil)
(defvar *physical-follow-desired-last-error* nil)

(defun physical-follow-check-desired-active (desired)
  (unless (and (eq desired *physical-follow-desired*)
               (not *physical-follow-desired-stopping-p*)
               (not (eq desired *physical-follow-manual-generation*))
               (not *physical-follow-manual-reservation*))
    (error 'physical-follow-generation-changed))
  (physical-follow-require-generation desired))

(defun physical-follow-disable-desired-receipt (desired)
  (physical-follow-call-with-controller-blocked
   (lambda ()
     (sb-thread:with-mutex (*physical-follow-command-lock*)
       (physical-follow-check-desired-active desired)
       (check-physical-recovery-worker-start)
       (physical-follow-quiesce-receiver)
       (let ((disabled nil))
         (sb-thread:with-mutex (*reload-run-lock*)
           (with-live-operation-quiescence
             (physical-follow-check-desired-active desired)
             (check-physical-recovery-worker-start)
             (let* ((base (physical-recovery-verify-base-state))
                    (current (physical-follow-read-config))
                    (remembered (physical-follow-with-controller
                                 current (physical-follow-remember-config-timeline current)))
                    (settled (physical-follow-abandon-preparation-config remembered))
                    (state (physical-follow-controller-state settled)))
               (physical-follow-controller-check-current current)
               (physical-follow-persist-management base)
               (setf (getf state :enabled) nil
                     disabled (physical-follow-with-controller settled state))
               (unless (equal disabled current)
                 (physical-follow-controller-save-config disabled))
               (sb-thread:with-mutex (*physical-follow-lock*)
                 (physical-follow-check-desired-active desired)
                 (when *physical-follow-runtime*
                   (setf (physical-follow-runtime-config *physical-follow-runtime*) (copy-tree disabled)))))))
         (unless *physical-follow-runtime*
           (physical-follow-launch-replay-only disabled))
         disabled)))))

(defun physical-follow-mark-desired-applied (desired)
  (sb-thread:with-mutex (*physical-follow-lock*)
    (physical-follow-check-desired-active desired)
    (setf *physical-follow-desired-applied* desired
          *physical-follow-desired-last-error* nil))
  :applied)

(defun physical-follow-reconcile-desired-step ()
  (let ((desired (sb-thread:with-mutex (*physical-follow-lock*) *physical-follow-desired*)))
    (cond
      ((not (physical-follow-desired-claimed-p desired)) :unmanaged)
      ((or *physical-follow-desired-stopping-p* (shutdown-in-progress-p)) :shutdown)
      ((eq desired *physical-follow-manual-generation*) :suspended)
      ((eq desired *physical-follow-desired-applied*) :applied)
      ((not (recovery-in-progress-p)) :inactive)
      (t
       (handler-case
           (progn
             (when (eq :deferred (physical-follow-ensure-attachment-state))
               (return-from physical-follow-reconcile-desired-step :retry))
             (physical-follow-disable-desired-receipt desired)
             (when (zerop (length (%physical-follow-desired-conninfo desired)))
               (return-from physical-follow-reconcile-desired-step
                 (physical-follow-mark-desired-applied desired)))
             (let* ((profile (make-physical-libpq-profile
                              (physical-follow-desired-receiver-conninfo desired)
                              :fallback (physical-follow-desired-fallback desired) :mode :physical))
                    (operation (physical-follow-start-operation
                                "libpq" 5432 "configured" nil profile
                                (physical-follow-desired-slot desired) desired t)))
               (sb-thread:join-thread (physical-follow-operation-thread operation))
               (physical-follow-check-desired-active desired)
               (cond
                 ((or *physical-recovery-failed-p* (physical-follow-operation-error operation)) :retry)
                 ((or (eq :started (physical-follow-operation-result operation))
                      (eq :replay-only
                          (getf (getf (physical-follow-operation-context operation) :local-decision) :action)))
                  (physical-follow-mark-desired-applied desired))
                 (t :retry))))
         (physical-follow-generation-changed () :superseded)
         (physical-recovery-worker-shutdown () :shutdown)
         ((or error sb-ext:timeout) (condition)
           (setf *physical-follow-desired-last-error* (princ-to-string condition))
           (if *physical-recovery-failed-p* :failed :retry)))))))

(defun physical-follow-desired-loop ()
  (let ((last nil) (retry-after 0))
    (loop
      (when (or *physical-follow-desired-stopping-p* (shutdown-in-progress-p)
                *physical-recovery-failed-p* (not (recovery-in-progress-p)))
        (return))
      (let ((desired *physical-follow-desired*) (now (get-internal-real-time)))
        (unless (eq desired last) (setf last desired retry-after 0))
        (when (>= now retry-after)
          (let ((result (physical-follow-reconcile-desired-step)))
            (when (eq result :failed) (return))
            (setf retry-after (+ (get-internal-real-time) internal-time-units-per-second))))
        (sb-thread:wait-on-semaphore *physical-follow-desired-wake* :timeout 0.2)))))

(defun physical-follow-desired-start-worker (&key (reset-applied-p t))
  (unless (and *postmaster-data-lock* (postmaster-pid-owned-p)
               (recovery-in-progress-p)
               (member (getf *physical-recovery-state* :phase) '(1 2)))
    (return-from physical-follow-desired-start-worker :inactive))
  (check-physical-recovery-worker-start)
  (sb-thread:with-mutex (*physical-follow-lock*)
    (unless (and *physical-follow-desired-worker*
                 (sb-thread:thread-alive-p *physical-follow-desired-worker*))
      (let ((ready nil))
        (when reset-applied-p (setf *physical-follow-desired-applied* nil))
        (setf *physical-follow-desired-stopping-p* nil
              *physical-follow-desired-worker*
              (sb-thread:make-thread
               (lambda ()
                 (loop until ready do (sleep 0.001))
                 (physical-follow-desired-loop))
               :name "configured native follow")
              ready t))))
  :started)

(defun physical-follow-desired-stop-worker ()
  (let ((worker nil) (client nil))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (setf *physical-follow-desired-stopping-p* t
            worker *physical-follow-desired-worker*)
      (let ((operation *physical-follow-operation*))
        (when (and operation (getf (physical-follow-operation-context operation) :configured-p))
          (setf (physical-follow-operation-cancelled-p operation) t
                client (physical-follow-operation-client operation)))))
    (sb-thread:signal-semaphore *physical-follow-desired-wake*)
    (when client (ignore-errors (physical-client-stop client)))
    (when worker
      (when (eq worker sb-thread:*current-thread*)
        (error "configured follow worker cannot join itself"))
      (sb-thread:join-thread worker :timeout 30 :default :timeout)
      (when (sb-thread:thread-alive-p worker)
        (setf *physical-recovery-failed-p* t)
        (error "configured follow worker has not stopped"))
      (sb-thread:with-mutex (*physical-follow-lock*)
        (when (eq worker *physical-follow-desired-worker*)
          (setf *physical-follow-desired-worker* nil))))
    (not (null worker))))

(defun physical-follow-plan-settings (settings &optional force-claim)
  (let* ((entry (and settings (gethash "primary_conninfo" settings)))
         (slot (and settings (gethash "primary_slot_name" settings)))
         (cluster (and settings (gethash "cluster_name" settings)))
         (name (and cluster (getf cluster :value))))
    (physical-follow-next-desired
     *physical-follow-desired* (if entry (getf entry :value) "")
     (if slot (getf slot :value) "")
     (if (and (stringp name) (plusp (length name))) name "walreceiver")
     (getf *physical-recovery-state* :timeline-policy)
     (or force-claim (getf *physical-recovery-state* :standard-follow-managed-p)
         (and entry (not (eq :default (getf entry :source))))))))

(defun physical-follow-activate-standard-desired ()
  (let ((desired (physical-follow-plan-settings
                  *startup-server-settings* (getf *physical-recovery-state* :standard-follow-managed-p))))
    (physical-follow-install-desired desired)))

(defun physical-follow-standard-management-p (base signal-mode)
  (let ((marker (physical-follow-managed-marker-p base)))
    (or marker (eq signal-mode :standby)
        (physical-follow-desired-claimed-p *physical-follow-desired*))))

(defun physical-follow-restart-desired-worker-p ()
  (and (recovery-in-progress-p)
       (member (getf *physical-recovery-state* :phase) '(1 2))
       (not *physical-recovery-failed-p*) (not *physical-shutdown-state*)
       (not *physical-promotion-stopping-p*) (not (shutdown-in-progress-p))))

(defun physical-follow-call-with-desired-paused (function)
  (when (plusp *live-operation-depth*)
    (error "configured follow must be joined outside operation admission"))
  (let ((paused (when (and *physical-follow-desired-worker*
                           (not (eq *physical-follow-desired-worker* sb-thread:*current-thread*)))
                  (physical-follow-desired-stop-worker))))
    (unwind-protect (funcall function)
      (when (and paused (physical-follow-restart-desired-worker-p))
        (physical-follow-desired-start-worker)))))

(defun physical-follow-call-with-manual-source (function)
  (when (plusp *live-operation-depth*)
    (error "manual follow requires its own operation admission"))
  (when (sb-thread:holding-mutex-p *physical-follow-command-lock*)
    (error "manual follow must reserve outside command exclusion"))
  (let ((resume-p nil) (generation nil)
                       (reservation (list sb-thread:*current-thread*)))
    ;; Reserve before joining the manager so a competing command cannot
    ;; consume the interval in which the worker pointer is cleared.
    (sb-thread:with-mutex (*physical-follow-command-lock*)
      (sb-thread:with-mutex (*physical-follow-lock*)
        (when *physical-follow-manual-reservation*
          (error "another manual follow command is active"))
        (setf *physical-follow-manual-reservation* reservation
              generation *physical-follow-desired*
              resume-p (not (null *physical-follow-desired-worker*)))))
    (unwind-protect
         (progn
           (when resume-p (physical-follow-desired-stop-worker))
           (let ((result (funcall function generation)))
             (when (member result '(:started :stopped))
               (sb-thread:with-mutex (*physical-follow-lock*)
                 (physical-follow-require-generation generation)
                 (when (physical-follow-desired-claimed-p generation)
                   (setf *physical-follow-manual-generation* generation))))
             result))
      (sb-thread:with-mutex (*physical-follow-lock*)
        (when (eq reservation *physical-follow-manual-reservation*)
          (setf *physical-follow-manual-reservation* nil)))
      (when (and resume-p (physical-follow-restart-desired-worker-p))
        (physical-follow-desired-start-worker :reset-applied-p nil)))))
