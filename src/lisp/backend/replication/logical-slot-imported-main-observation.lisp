(in-package #:pg)

(defun logical-slot-imported-startup-inputs (root root-check &optional expected)
  ;; Keep the pair, history, sidecar and checkpoint scopes nested while copying
  ;; the inputs and comparing EXPECTED.  Their terminal checks run as the scopes
  ;; close, before the detached evidence is returned.  ROOT-CHECK must reject loss
  ;; of the caller's root and PID authority; the copied evidence carries none.
  (unless (logical-slot-copy-imported-startup-candidate-p)
    (error "imported startup requires the complete committed input names"))
  (call-with-logical-slot-imported-startup-pair
   root root-check
   (lambda (image facts pair-check)
     (declare (ignore facts))
     (call-with-logical-slot-imported-startup-history
      root image pair-check
      (lambda (profile history history-check)
        (call-with-logical-slot-imported-startup-sidecar
         root image history-check
         (lambda (pieces sidecar-check)
           (call-with-logical-slot-imported-startup-checkpoint
            root image sidecar-check
            (lambda (checkpoint checkpoint-check)
              (let* ((sidecar (funcall sidecar-check :directory-facts))
                     (identity (logical-slot-copy-source-proof-directory-identity
                                (getf checkpoint :root-facts)))
                     (pair (funcall pair-check :file-facts)))
                (funcall checkpoint-check)
                (setf (getf checkpoint :root-facts) identity)
                (let ((inputs (list :root identity :proof image :pair pair
                                    :recovery-profile profile :history history
                                    :sidecar (list sidecar pieces) :checkpoint checkpoint)))
                  (when (and expected
                             (not (logical-slot-copy-committed-image-equal-p expected inputs)))
                    (error "imported startup inputs changed across PID publication"))
                  inputs)))))))))))

(defun logical-slot-imported-startup-observation (lock phase &optional expected)
  ;; Observe under the held root and the requested PID scope.  The owned-PID
  ;; observation must match the copied before-PID evidence across publication.
  ;; Compare the root by directory identity because its size and timestamps can
  ;; change at publication; compare the other facts in full.  Return evidence
  ;; only after the checks and scope cleanup, without initializing WAL or admitting
  ;; a backend.  The returned copy is not continuing authority over the inputs.
  (ecase phase
    (:before-pid
     (when expected (error "initial imported startup cannot reuse boundary evidence")))
    (:owned-pid
     (unless (and expected (eq :before-pid (getf expected :phase))
                  (getf expected :inputs))
       (error "owned imported startup requires pre-PID evidence"))))
  (let ((expected (logical-slot-copy-clone-facts expected)))
    (call-with-physical-reseed-follow-startup-root
     lock
     (lambda (root root-check)
       (funcall
        (ecase phase
          (:before-pid #'call-with-physical-reseed-follow-retirement-before-pid)
          (:owned-pid #'call-with-physical-reseed-follow-retirement-owned-pid))
        root root-check
        (lambda (pid pid-check)
          (declare (ignore pid))
          (let ((inputs (logical-slot-imported-startup-inputs
                         root pid-check (getf expected :inputs))))
            (funcall pid-check)
            (list :phase phase :inputs inputs))))))))
