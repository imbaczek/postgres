;;; Capture evidence from the owned input and selected promotion plan.

;;; Permanent import evidence checks describe recorded facts, not authority.
(in-package #:pg)

(defun logical-slot-copy-import-build-evidence (context descriptor state plan selected control)
  "Capture raw manifest, D checkpoint, pending C3 control, histories and frame plan.
This does not capture full F metadata or catalog; D through F replay is separate."
  (let ((manifest-bytes (logical-slot-copy-read-file context "physical-backup.manifest"
                                                     (* 16 1024 1024))))
    (multiple-value-bind (manifest digest) (recovery-decode-manifest manifest-bytes)
      (unless (= digest (logical-slot-copy-descriptor-manifest-digest descriptor))
        (error "logical import evidence manifest changed"))
      (logical-slot-copy-import-evidence-encode
       (list :manifest manifest-bytes
             :checkpoint (logical-slot-copy-input-checkpoint context manifest descriptor)
             :control control :selected-history selected
             :child-history (physical-promotion-history-image state) :plan plan)))))
