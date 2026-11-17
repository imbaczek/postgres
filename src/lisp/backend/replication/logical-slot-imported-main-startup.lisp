(in-package #:pg)

(defun logical-slot-imported-startup-inert-follow-p (config)
  ;; A checked, source-free config may remain as inert local metadata. It does
  ;; not authorize following or remote cleanup on the imported primary.
  (let ((state (sixth config)))
    (and (physical-follow-no-source-p config)
         (or (physical-follow-cleanup-only-p config)
             (and (physical-follow-unattached-p config) (not (getf state :enabled))))
         (not (getf state :preparing)) (not (getf state :cleanup))
         (not (getf state :source)) (not (getf state :public-slot)))))

(defun logical-slot-imported-startup-check-policy ()
  ;; Historical recovery provenance is evidence, not a new recovery request.
  (physical-directory-check-reseed-intent *pgdata*)
  (physical-adoption-check-startup)
  (when (or (recovery-in-progress-p)
            (getf *physical-recovery-state* :standard-follow-managed-p)
            *physical-promotion-requested-p*)
    (error "imported primary startup conflicts with live recovery or promotion"))
  (dolist (name '("promote" "standby.signal" "recovery.signal" "physical-follow-managed.control"))
    (when (slot-state-lstat (merge-pathnames name *pgdata*))
      (error "imported primary startup conflicts with request ~a" name)))
  (when (slot-state-lstat (physical-follow-config-path))
    (unless (logical-slot-imported-startup-inert-follow-p (physical-follow-read-config))
      (error "imported primary startup requires inactive source-free follow metadata")))
  t)

(defun logical-slot-imported-startup-check-owned (expected)
  ;; Reobserve under published ownership before any storage mutation. The sticky
  ;; continuation fence prevents a later missing proof from selecting empty load.
  (unless (and (eq :owned-pid (getf expected :phase)) (getf expected :inputs))
    (error "imported owned startup requires checked PID-boundary evidence"))
  (let ((expected (logical-slot-copy-clone-facts (getf expected :inputs))))
    (call-with-postmaster-ownership
     (lambda (ownership)
       (call-with-physical-adoption-startup-owner
        (lambda (owner)
          (call-with-physical-directory-root
           owner
           (lambda (root path)
             (declare (ignore path))
             (let ((mount (physical-adoption-install-mount-id nil :directory-fd root)))
               (flet ((check ()
                        (postmaster-check-ownership ownership)
                        (physical-adoption-check-owner owner)
                        (logical-slot-imported-startup-check-policy)
                        (physical-image-check-target *pgdata* root)
                        (unless (= mount
                                   (physical-adoption-install-mount-id *pgdata*)
                                   (physical-adoption-install-mount-id nil :directory-fd root)
                                   (physical-adoption-install-mount-id
                                    nil :directory-fd
                                    (sb-sys:fd-stream-fd *postmaster-data-lock*))
                                   (physical-adoption-install-mount-id
                                    nil :directory-fd
                                    (sb-sys:fd-stream-fd *postmaster-pid-stream*)))
                          (error "imported startup ownership crossed mounts"))
                        t))
                 (check)
                 (let ((inputs (logical-slot-imported-startup-inputs root #'check expected)))
                   (unless (logical-slot-continuation-gc-fenced-p *pgdata*)
                     (error "imported startup lost its continuation fence"))
                   (check)
                   inputs)))))))))))

(defun logical-slot-imported-own-startup (host port)
  (let ((before nil) (after nil))
    (logical-slot-imported-startup-check-policy)
    (postmaster-own-data-directory
     host port
     :startup-observer
     (lambda (lock phase)
       (logical-slot-imported-startup-check-policy)
       (ecase phase
         (:before-pid
          (setf before (logical-slot-imported-startup-observation lock phase)))
         (:owned-pid
          (setf after (logical-slot-imported-startup-observation lock phase before))))
       (logical-slot-imported-startup-check-policy))
     :startup-reconcile
     (lambda () (logical-slot-imported-startup-check-owned after)))
    after))
