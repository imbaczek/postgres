(in-package #:pg)

(defun logical-slot-copy-import-build-evidence (context descriptor state plan selected control)
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
