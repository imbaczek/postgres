(in-package #:pg)

(defun logical-slot-continuation-cold-cleanup-check-owner
    (storage reservation check-directory check-selector)
  (logical-slot-continuation-storage-check storage)
  (funcall check-directory)
  (funcall check-selector)
  (flet ((check ()
           (logical-slot-continuation-cold-reservation-call reservation :check)))
    (if (sb-thread:holding-mutex-p *wal-lock*) (check)
        (sb-thread:with-mutex (*wal-lock*) (check)))))

;; Keep the reservation, selector and slot root checked while building the plan
;; and while its callback runs.  Re-read the complete inventory and root before
;; and after the callback; pass a plan copy and a scoped recheck function.  The
;; scope expires on exit, and observation alone does not perform cleanup.
(defun %call-with-logical-slot-continuation-cold-cleanup-inputs (gate ownership function)
  (unless (functionp function) (error "cold cleanup plan requires a callback"))
  (call-with-logical-slot-continuation-cold-context
   gate ownership
   (lambda (storage reservation archive)
     (declare (ignore archive))
     (let ((facts (logical-slot-continuation-storage-call storage :facts))
           (binding (logical-slot-continuation-storage-call storage :binding))
           (host (logical-slot-continuation-storage-call storage :host)))
       (call-with-logical-slot-continuation-cold-selector-observation
        storage
        (lambda (kind model selected temporary check-selector)
          (logical-slot-continuation-storage-call
           storage :with-cold-slot-root
           (lambda (directory check-directory)
             (let ((active t) (failed nil) (thread sb-thread:*current-thread*)
                              (rows nil) (root nil) (plan nil))
               (macrolet ((in-scope (&body body)
                            `(with-logical-slot-continuation-observation-check
                                 (active failed thread "cold cleanup plan") ,@body)))
                 (labels ((guard ()
                            (in-scope
                             (logical-slot-continuation-cold-cleanup-check-owner
                              storage reservation check-directory check-selector)))
                          (recheck ()
                            (in-scope
                             (guard)
                             (unless (and (equal root (physical-image-file-facts directory))
                                          (equalp rows (logical-slot-continuation-cold-read-raw-inventory
                                                        directory #'guard)))
                               (error "cold cleanup complete inventory changed"))
                             (guard))))
                   (unwind-protect
                        (progn
                          (guard)
                          (setf root (physical-image-file-facts directory)
                                rows (logical-slot-continuation-cold-read-raw-inventory directory #'guard)
                                plan (logical-slot-continuation-cold-build-cleanup-plan
                                      kind model selected temporary rows root binding
                                      (getf facts :certificate) host))
                          (recheck)
                          (multiple-value-prog1
                              (funcall function plan reservation #'recheck)
                            (recheck)))
                     (sb-sys:without-interrupts (setf active nil))))))))))))))

(defun call-with-logical-slot-continuation-cold-cleanup-plan (gate ownership function)
  (unless (functionp function) (error "cold cleanup plan requires a callback"))
  (%call-with-logical-slot-continuation-cold-cleanup-inputs
   gate ownership
   (lambda (plan reservation check)
     (funcall function (logical-slot-continuation-copy plan) reservation check))))

(defun logical-slot-continuation-cold-cleanup-startup (gate ownership)
  (call-with-logical-slot-continuation-cold-context
   gate ownership
   (lambda (storage reservation archive)
     (declare (ignore archive))
     (let ((facts (logical-slot-continuation-storage-call storage :facts))
           (binding (logical-slot-continuation-storage-call storage :binding))
           (host (logical-slot-continuation-storage-call storage :host)))
       (call-with-logical-slot-continuation-cold-selector-observation
        storage
        (lambda (kind model selected temporary check-selector)
          (logical-slot-continuation-storage-call
           storage :with-cold-slot-root
           (lambda (directory check-directory)
             (let ((active t) (failed nil) (thread sb-thread:*current-thread*))
               (labels ((guard ()
                          (with-logical-slot-continuation-observation-check
                              (active failed thread "cold cleanup owner")
                            (logical-slot-continuation-cold-cleanup-check-owner
                             storage reservation check-directory check-selector))))
                 (unwind-protect
                      (progn
                        (guard)
                        (let* ((root (physical-image-file-facts directory))
                               (rows (logical-slot-continuation-cold-read-raw-inventory directory #'guard))
                               (plan (logical-slot-continuation-cold-build-cleanup-plan
                                      kind model selected temporary rows root binding
                                      (getf facts :certificate) host)))
                          (logical-slot-continuation-cold-execute-cleanup-plan directory #'guard plan)
                          (guard)
                          (length (getf plan :actions))))
                   (sb-sys:without-interrupts (setf active nil)))))))))))))
