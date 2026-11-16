(in-package #:pg)

(defun logical-slot-startup-publication-derive-cold (owner plan)
  (let ((token nil))
    (sb-thread:with-mutex (*wal-lock*)
      (let* ((data (logical-slot-continuation-cold-commit-call plan :data owner))
             (registry (getf data :registry)) (image (getf data :image)))
        (logical-slot-publication-check-registry registry image)
        (setf token (%make-logical-slot-startup-publication-token))
        (setf (gethash token *logical-slot-startup-publication-tokens*)
              (vector owner registry image :derived
                      (list (copy-seq (getf data :selected-image)) (copy-seq (getf data :binding)))
                      plan))))
    (logical-slot-startup-publication-test-point :derived owner token)
    token))

(defun logical-slot-startup-publication-finish-cold (owner plan)
  (let* ((token (logical-slot-startup-publication-derive-cold owner plan))
         (binding (logical-slot-startup-publication-token-binding owner token))
         (registry (svref binding 1)))
    (logical-slot-startup-publication-call owner :prepare token)
    (logical-slot-startup-publication-test-point :prepared owner token)
    (sb-thread:with-mutex (*wal-lock*)
      (logical-slot-startup-publication-test-point :before-commit owner token)
      (logical-slot-startup-publication-call owner :commit token))
    (logical-slot-startup-publication-test-point :after-commit owner token)
    registry))

(defun logical-slot-continuation-cold-publish-final
    (owner storage completion check-sidecar check-commit failure-cell)
  (call-with-logical-slot-continuation-cold-publication-files
   storage completion
   (lambda (check-files)
     (flet ((guard ()
              (assert (sb-thread:holding-mutex-p *wal-lock*))
              (funcall check-files)
              (funcall check-sidecar)
              (funcall check-commit)
              (funcall check-files)))
       (call-with-logical-slot-continuation-cold-commit
        owner completion #'guard
        (lambda (plan) (logical-slot-startup-publication-finish-cold owner plan))
        failure-cell)))))

(defun logical-slot-continuation-cold-publish-source (owner completion failure-cell)
  (let* ((data (logical-slot-continuation-cold-completion-call completion :data))
         (observation (getf data :observation)) (proof (getf data :proof)))
    (call-with-logical-slot-startup-publication-storage
     owner
     (lambda (storage facts)
       (declare (ignore facts))
       (logical-slot-continuation-storage-call
        storage :with-origin
        (lambda (archive)
          (logical-slot-continuation-cold-reconciliation-with-tail
           storage observation archive proof
           (lambda (check-tail check-sidecar check-commit)
             (declare (ignore check-tail))
             (logical-slot-continuation-cold-publish-final
              owner storage completion check-sidecar check-commit failure-cell))
           :commit-guard t)))))))

(defun logical-slot-continuation-cold-publish-startup (gate ownership)
  (let ((failure (cons nil nil)) (receipt (vector nil))
                                 (reservation nil) (restored nil) (complete nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (sb-sys:with-local-interrupts
             (setf restored
                   (call-with-logical-slot-startup-publication-owner
                    gate ownership
                    (lambda (owner)
                      (call-with-logical-slot-continuation-cold-completion
                       owner gate ownership
                       (lambda (completion)
                         (setf reservation
                               (getf (logical-slot-continuation-cold-completion-call completion :data)
                                     :reservation))
                         (logical-slot-continuation-cold-publish-source owner completion failure))
                       failure))
                    receipt))
             (logical-slot-startup-publication-notify gate ownership restored)
             (sb-sys:without-interrupts
               (sb-thread:with-mutex (*wal-lock*)
                 (logical-slot-continuation-cold-publication-point
                  :before-old-pin-retirement nil
                  (gethash restored *logical-slot-continuation-protection-registries*) restored)
                 (logical-slot-continuation-cold-reservation-call reservation :retire-handoff)
                 (setf complete t)))
             restored)
        (unless complete
          (logical-slot-continuation-cold-preparation-fail (car failure) (cdr failure)))))))
