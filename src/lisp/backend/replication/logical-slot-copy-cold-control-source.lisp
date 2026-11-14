(in-package #:pg)

(defun logical-slot-copy-cold-control-new-runtime (binding)
  (%make-logical-slot-copy-cold-control-runtime
   :global (getf binding :global) :directory-facts (copy-list (getf binding :global-facts))
   :children (logical-slot-copy-import-state-image (getf binding :global-children))
   :phase (getf binding :control-phase)
   :catalog-bytes (getf binding :old-catalog) :catalog-facts (getf binding :old-catalog-facts)
   :control-bytes (getf binding :canonical-control) :control-facts (getf binding :canonical-control-facts)
   :temporary-kind (getf binding :temporary-kind)
   :temporary-bytes (second (getf binding :temporary))
   :temporary-facts (third (getf binding :temporary))
   :child-complete (or (eq :child (getf binding :control-phase))
                       (eq :control (getf binding :temporary-kind)))))

(defun logical-slot-copy-cold-control-image (attempt)
  (let ((b (logical-slot-copy-cold-control-attempt-binding attempt)))
    (list b (logical-slot-copy-descriptor-encode (getf b :descriptor))
          (logical-slot-copy-cold-control-attempt-owner attempt)
          (logical-slot-copy-cold-control-attempt-context attempt)
          (logical-slot-copy-cold-control-attempt-source-check attempt)
          (logical-slot-copy-cold-control-attempt-operations attempt)
          (logical-slot-copy-cold-control-attempt-failure-operation attempt)
          (logical-slot-copy-cold-control-attempt-mark-started attempt))))

(defun logical-slot-copy-cold-control-make-attempt (owner context binding mark-started store-reader)
  (let ((attempt nil) (runtime (logical-slot-copy-cold-control-new-runtime binding))
                      (seal nil) (failed nil) (active t) (executed nil) (files nil) (close-files nil))
    (labels ((registered (subject)
               (and (eq subject attempt)
                    (eq attempt (cdr (assoc owner *logical-slot-copy-cold-control-attempts* :test #'eq)))))
             (failure (operation subject)
               (if (not (registered subject))
                   (eq operation :failedp)
                   (ecase operation
                     (:failedp (or failed (not active)))
                     (:revoke (setf failed t (physical-adoption-owner-active-p owner) nil)))))
             (check-binding ()
               (unless (and active (not failed)
                            (logical-slot-copy-committed-image-equal-p
                             seal (logical-slot-copy-cold-control-image attempt)))
                 (error "cold control immutable binding ended or changed"))
               (logical-slot-copy-cold-control-check-inputs attempt))
             (source-check ()
               (with-logical-slot-copy-cold-control-guard (attempt)
                 (logical-slot-copy-cold-control-check-source
                  attempt runtime (funcall store-reader)
                  (lambda ()
                    (when files (funcall files :check attempt))
                    (multiple-value-prog1 (logical-slot-copy-cold-control-artifacts attempt runtime)
                      (when files (funcall files :check attempt)))))))
             (operations (operation subject &optional argument)
               (unless (registered subject) (error "cold control operation has an unregistered subject"))
               (let ((complete nil))
                 (unwind-protect
                      (multiple-value-prog1
                          (progn
                            (check-binding)
                            (ecase operation
                              (:check-binding t)
                              (:check-runtime
                               (unless (eq runtime argument) (error "cold control runtime differs")))
                              (:file (funcall files argument attempt))
                              (:execute
                               (when executed (error "cold control attempt already executed"))
                               (setf executed t)
                               (logical-slot-copy-cold-control-execute attempt runtime (funcall store-reader)))))
                        (setf complete t))
                   (unless complete (failure :revoke attempt))))))
      (setf attempt (%make-logical-slot-copy-cold-control-attempt
                     :owner owner :context context :binding binding :source-check #'source-check
                     :operations #'operations :failure-operation #'failure :mark-started mark-started))
      (setf seal (logical-slot-copy-import-state-image (logical-slot-copy-cold-control-image attempt)))
      (multiple-value-setq (files close-files)
        (logical-slot-copy-cold-control-file-operations (lambda () attempt) runtime))
      (values attempt runtime
              (lambda ()
                (unwind-protect (when close-files (funcall close-files))
                  (setf files nil)))
              (lambda ()
                (setf active nil (logical-slot-copy-cold-control-runtime-source-active runtime) nil))))))

(defun logical-slot-copy-cold-control-run-leased (attempt runtime close-files function)
  (let ((complete nil)
        (context (logical-slot-copy-cold-control-attempt-context attempt)))
    (unwind-protect
         (progn
           (logical-slot-copy-cold-control-check-metadata attempt runtime)
           (logical-slot-copy-cold-clean-wal-scratch
            context (lambda () (logical-slot-copy-cold-control-check-metadata attempt runtime))
            (logical-slot-copy-cold-control-attempt-mark-started attempt) 4096)
           (funcall (logical-slot-copy-cold-control-attempt-source-check attempt))
           (multiple-value-prog1 (funcall function attempt)
             (when (logical-slot-copy-cold-control-runtime-source-active runtime)
               (let ((*logical-slot-copy-verification-test-hook* nil))
                 (funcall (logical-slot-copy-cold-control-attempt-source-check attempt))))
             (let ((failure (funcall close-files))) (when failure (error failure)))
             (logical-slot-copy-cold-control-point :after-cold-control-close attempt)
             (logical-slot-copy-cold-control-check-metadata attempt runtime)
             (when (logical-slot-copy-cold-control-runtime-wal-snapshot runtime)
               (logical-slot-copy-cold-wal-check-snapshot
                context (logical-slot-copy-cold-control-runtime-wal-snapshot runtime) 4096))
             (let ((*logical-slot-copy-verification-test-hook* nil))
               (funcall (logical-slot-copy-cold-control-attempt-source-check attempt)))
             (setf (logical-slot-copy-cold-control-runtime-source-active runtime) nil
                   complete t)))
      (setf (logical-slot-copy-cold-control-runtime-source-active runtime) nil)
      (unless complete (logical-slot-copy-cold-control-revoke attempt))
      (let ((failure (funcall close-files))) (when (and complete failure) (error failure))))))

(defun logical-slot-copy-cold-control-call-with-source (owner context proof mark-started function)
  (let* ((binding (logical-slot-copy-cold-control-bind-data
                   owner context (logical-slot-copy-cold-control-read-binding owner context proof)))
         (store nil))
    (multiple-value-bind (attempt runtime close-files expire)
        (logical-slot-copy-cold-control-make-attempt owner context binding mark-started (lambda () store))
      (let ((*logical-slot-copy-cold-control-attempts*
              (acons owner attempt *logical-slot-copy-cold-control-attempts*))
            (complete nil))
        (unwind-protect
             (multiple-value-prog1
                 (call-with-logical-slot-copy-committed-store
                  owner (getf binding :state)
                  (lambda (opened)
                    (setf store opened)
                    (physical-promotion-ensure-pending-retained
                     (getf binding :state) (getf binding :metadata) store)
                    (physical-follow-call-with-receive-root
                     store (lambda ()
                             (logical-slot-copy-cold-control-run-leased
                              attempt runtime close-files function)))))
               (setf complete t))
          (unless complete (logical-slot-copy-cold-control-revoke attempt))
          (funcall expire)
          (let ((failure (funcall close-files))) (when (and complete failure) (error failure))))))))

(defun logical-slot-copy-import-publish-cold-control ()
  (when *logical-slot-copy-cold-control-attempts* (error "cold control publication is already active"))
  (logical-slot-copy-install-check-input-entry)
  (when (or *wal-ready* *physical-recovery-failed-p*)
    (error "cold control publication requires healthy unopened storage"))
  (let ((started nil) (complete nil))
    (unwind-protect
         (multiple-value-prog1
             (call-with-logical-slot-copy-install-exclusion
              (lambda ()
                (call-with-logical-slot-copy-owned-input
                 (lambda (&rest arguments)
                   (apply #'%make-logical-slot-copy-cold-control-owner
                          :mode :cold :state nil :store nil arguments))
                 (lambda (owner context proof)
                   (let ((*pgdata* (physical-image-context-root-path context))
                         (*physical-image-context* context))
                     (logical-slot-copy-cold-control-call-with-source
                      owner context proof (lambda () (setf started t))
                      (lambda (attempt)
                        (funcall (logical-slot-copy-cold-control-attempt-operations attempt)
                                 :execute attempt))))))))
           (setf complete t))
      (when (and started (not complete)) (setf *physical-recovery-failed-p* t)))))
