;; Reconcile terminal reseed retirement during postmaster ownership startup.

(in-package #:pg)

(defun physical-reseed-follow-check-terminal-startup (observation)
  ;; Main startup admits only terminal retirement evidence.  Generic
  ;; observation also supports inspecting earlier recovery states.  A pending
  ;; cancellation fences retirement startup.
  (case (getf observation :branch)
    (:ordinary observation)
    ((:retirement :retired)
     (let* ((evidence (getf observation :evidence))
            (record (getf (getf evidence :snapshot) :record))
            (activation (getf record :activation))
            (retirement (getf record :retirement)))
       (unless (and (null (getf record :cancellation))
                    (eq :active (getf activation :phase))
                    (eql 2 (getf activation :step))
                    (eq :retired (getf retirement :phase))
                    (eql 2 (getf retirement :step))
                    (eql 2 (getf evidence :prefix))
                    (null (getf evidence :location))
                    (null (getf evidence :temporary))
                    (null (getf evidence :temporary-file)))
         (error "postmaster startup requires terminal reseed retirement without pending journals")))
     observation)
    (otherwise (error "postmaster startup requires a recognized reseed observation"))))

(defun physical-reseed-follow-check-ordinary-startup ()
  ;; Require the ordinary local namespace and adoption checks after handover.
  (physical-reseed-check-before-pid)
  (physical-adoption-check-startup))

(defun physical-reseed-follow-own-startup (host port &key check-ordinary-before-pid)
  ;; Save the before-PID observation and compare it with owned-PID evidence.
  ;; Ordinary startup keeps the existing adoption reconciliation path.
  (let ((before nil) (after nil))
    (postmaster-own-data-directory
     host port
     :startup-observer
     (lambda (lock phase)
       (ecase phase
         (:before-pid
          (setf before
                (physical-reseed-follow-check-terminal-startup
                 (physical-reseed-follow-startup-observation lock phase)))
          (when (and check-ordinary-before-pid (eq :ordinary (getf before :branch)))
            (funcall check-ordinary-before-pid)))
         (:owned-pid
          (setf after (physical-reseed-follow-startup-observation lock phase before)))))
     :startup-reconcile
     (lambda ()
       (ecase (getf after :branch)
         (:ordinary (physical-adoption-reconcile-startup))
         ((:retirement :retired)
          (call-with-physical-reseed-follow-retirement-startup-owner
           after
           (lambda (owner)
             (physical-reseed-follow-retirement-publish-receipt owner)
             (physical-reseed-follow-retirement-consume-receipt owner)))))
       ;; Any retirement owner and its resources have closed.  Check the strict
       ;; namespace while still inside postmaster reconciliation.
       (physical-reseed-follow-check-ordinary-startup)))))

(defun call-with-physical-reseed-offline-startup (target function)
  "Call FUNCTION with fresh startup ownership state for TARGET.
Remove any owned PID file and release the data directory lock on exit."
  (when (or *postmaster-data-lock* *postmaster-pid-stream*)
    (error "offline reseed startup requires an unowned process"))
  (let ((*pgdata* target)
        (*postmaster-start-timestamp* nil)
        (*postmaster-start-time* nil)
        (*postmaster-pid-directory* nil)
        (*postmaster-pid-host* nil)
        (*postmaster-pid-port* nil)
        (*postmaster-pid-stream* nil)
        (*postmaster-data-lock* nil))
    (unwind-protect
         (funcall function)
      (unwind-protect
           (when *postmaster-pid-stream* (postmaster-remove-pid-file))
        (when *postmaster-data-lock* (close *postmaster-data-lock*))))))

(defun physical-reseed-follow-retire-offline (target port)
  "Own the stopped target and archive the install and attempt journals in order."
  (call-with-physical-reseed-offline-startup
   target
   (lambda ()
     (let ((before nil) (after nil))
       (postmaster-own-data-directory
        "127.0.0.1" port
        :startup-observer
        (lambda (lock phase)
          (ecase phase
            (:before-pid (setf before (physical-reseed-follow-startup-observation lock phase)))
            (:owned-pid (setf after (physical-reseed-follow-startup-observation lock phase before)))))
        :startup-reconcile
        (lambda ()
          (call-with-physical-reseed-follow-retirement-startup-owner
           after
           (lambda (owner)
             (physical-reseed-follow-retirement-publish-initial owner)
             (physical-reseed-follow-retirement-archive-install owner)
             (physical-reseed-follow-retirement-archive-attempt owner)
             (physical-reseed-follow-retirement-publish-terminal owner)))))))))

(defun physical-reseed-follow-check-offline-finalization (&key before-pid)
  "Check that ordinary startup and an active installed streaming standby are allowed.
After PID ownership also check the local follow store and startup admission."
  (physical-reseed-follow-check-ordinary-startup)
  (multiple-value-bind (base control) (physical-recovery-verify-base-state)
    (let ((*wal-segment-namespace* (getf base :wal-namespace))
          (*wal-insert-timeline* (getf base :timeline))
          (*wal-checkpoint-timeline* (getf base :timeline)))
      (let ((config (if before-pid
                        (physical-follow-read-config)
                        (physical-follow-startup-owner-check))))
        (unless (and (= 2 (getf control :phase))
                     (eq :active (getf (fifth config) :phase))
                     (not (physical-follow-cleanup-only-p config)))
          (error "offline follow finalization requires an installed streaming standby"))
        (physical-follow-check-unowned-bootstrap-base config base)
        (unless before-pid
          (setf (getf base :follow-startup-config) config)
          (physical-follow-check-startup-admission base)
          (physical-follow-call-with-offline-store
           base
           (lambda (store)
             (let* ((metadata (physical-restartpoint-read base))
                    (policy (physical-recovery-resolve-timeline-policy
                             base store metadata (startup-recovery-timeline-goal))))
               (physical-follow-check-active-state store base config :policy policy))))
          (physical-follow-managed-marker-p base))))))

(defun physical-reseed-follow-finalize-offline (target port)
  "Check the installed streaming standby under startup ownership.
Ensure postgresql.conf exists and sync the target directory."
  (call-with-physical-reseed-offline-startup
   target
   (lambda ()
     (physical-reseed-follow-own-startup
      "127.0.0.1" port
      :check-ordinary-before-pid
      (lambda () (physical-reseed-follow-check-offline-finalization :before-pid t)))
     (physical-reseed-follow-check-offline-finalization)
     (let ((path (merge-pathnames "postgresql.conf" target)))
       (if (physical-adoption-path-present-p path)
           (multiple-value-bind (input size) (open-bounded-regular-file path 16777216)
             (declare (ignore size))
             (close input))
           (wal-atomic-write-bytes path (make-array 0 :element-type '(unsigned-byte 8))))
       (fsync-directory target)))))
