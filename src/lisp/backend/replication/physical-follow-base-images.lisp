(in-package #:pg)

;;; Build recovery and selected-history images from a checked follow binding.
;;; These helpers return bytes without writing or installing them.

;; Encode the binding's phase-2 recovery witness at TIME (Unix seconds).
;; SELECTED uses the history in the after tuple. Return bytes without writing.
(defun physical-follow-binding-recovery-image (binding time &key selected)
  "Encode the required boundary and its witnessed source facts in recovery control."
  (physical-follow-binding-check binding)
  (unless (typep time '(signed-byte 64)) (error "invalid follow recovery image time"))
  (let ((before (getf binding :before)) (after (getf binding :after))
                                        (witness (getf binding :recovery)) (required (getf binding :required)))
    (physical-recovery-control-image
     2 (first before) (second before) (nth 9 before)
     (getf witness :checkpoint) (first required) (getf witness :observed-next-xid)
     :applied-timeline (second required)
     :history-target (if selected (eighth after) (getf witness :history-target))
     :history-digest (if selected (ninth after) (getf witness :history-digest))
     :resource-fact (getf witness :resource-fact) :fpw-fact (getf witness :fpw-fact)
     :logical-fact (getf witness :logical-fact) :state-time time)))

;; Build the named history and selected control byte images from a private empty
;; model.  Check that its resulting selection agrees with the binding.
(defun physical-follow-binding-selected-images (binding model time)
  (physical-follow-binding-check binding)
  (let* ((plain (copy-physical-store model))
         (before (getf binding :before)) (after (getf binding :after))
         (anchor (getf binding :anchor)))
    (setf (physical-store-selection plain) nil)
    (unless (and (not (equal before after))
                 (equal before (physical-store-control-tuple plain))
                 (null (physical-store-directory plain)) (null (physical-store-stream plain))
                 (null (physical-store-lock-stream plain)) (null (physical-store-retained-root plain)))
      (error "selected follow images require their empty base model"))
    (multiple-value-bind (selection bytes)
        (physical-store-history-candidate
         plain (eighth after) (getf binding :child-history)
         (coerce (getf anchor :eor-octets) '(vector (unsigned-byte 8))) (fifth before))
      (setf (physical-store-selection plain) selection)
      (unless (equal after (physical-store-control-tuple plain))
        (error "selected follow images differ from their binding"))
      (list (cons "history.receive" bytes)
            (cons "receive.selected" (physical-store-encode-control plain (fourth after)))
            (cons "history.wal" (string->bytes (getf binding :child-history)))
            (cons "recovery.selected" (physical-follow-binding-recovery-image binding time :selected t))))))
