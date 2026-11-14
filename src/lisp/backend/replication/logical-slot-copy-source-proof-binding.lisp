(in-package #:pg)

(defun logical-slot-copy-source-proof-plan-state (manifest control descriptor metadata prefix)
  ;; Reconstruct the child plan inputs from original R and C3, not the
  ;; separately resolved fact at F.
  (let* ((fields (logical-slot-copy-control-fields
                  (logical-slot-copy-descriptor-base-control descriptor)))
         (namespace (sixth fields))
         (state (copy-list manifest)))
    (unless (and (eql 4 (getf control :control-version)) (eql 3 (getf control :phase))
                 (eq namespace (getf control :promotion-source-namespace)))
      (error "permanent source proof lost its original pending namespace"))
    (loop for (key value) on control by #'cddr do (setf (getf state key) value))
    (setf (getf state :wal-namespace) namespace
          (getf state :required-applied) (getf control :applied)
          (getf state :required-timeline) (getf control :applied-timeline (getf control :timeline))
          (getf state :replay-timeline) (getf control :applied-timeline (getf control :timeline))
          (getf state :observed-next-xid) (getf control :next-xid))
    (physical-restartpoint-check-source-metadata metadata state)
    (unless (and (eql 4 (getf metadata :version))
                 (eql (getf metadata :lsn) (getf control :applied))
                 (eql (getf metadata :next-xid) (getf control :next-xid))
                 (eql (getf metadata :replay-timeline (getf metadata :timeline))
                      (getf state :replay-timeline))
                 (eql (getf metadata :history-target) (getf control :history-target))
                 (eql (getf metadata :history-digest) (getf control :history-digest)))
      (error "permanent source proof differs from its original restartpoint boundary"))
    (physical-restartpoint-check-wal-prefix metadata prefix)
    (setf (getf state :wal-prefix-lsn) (getf metadata :lsn)
          (getf state :wal-prefix-bytes) prefix
          (getf state :source-checkpoint) (copy-tree (getf metadata :source-checkpoint))
          (getf state :source-reservation) (copy-list (getf metadata :source-reservation))
          (getf state :source-xids) (copy-tree (getf metadata :source-xids)))
    (let ((fact (physical-promotion-frozen-resource-fact control metadata)))
      (when fact (setf (getf state :source-resource-fact) fact)))
    (setf (getf state :source-fpw-fact) (physical-promotion-frozen-fpw-fact control metadata))
    (let ((fact (physical-promotion-frozen-logical-fact control metadata)))
      (when fact (setf (getf state :source-logical-fact) fact)))
    state))

(defun logical-slot-copy-source-proof-check-receipt (receipt certificate prefix)
  ;; Check the recorded R observations and supplied prefix against the binding.
  (let* ((metadata (getf receipt :metadata))
         (text (string->bytes (physical-bootstrap-manifest-text metadata)))
         (reference (getf certificate :retained-reference)))
    (unless (and reference
                 (= (getf receipt :generation-size)
                    (+ 32 (length text) (getf receipt :catalog-size) (length prefix)))
                 (<= 1 (length text) (* 16 1024 1024))
                 (= (length prefix) (mod (getf certificate :fork) +wal-segment-size+))
                 (eql (getf metadata :prefix-digest) (getf reference :end-digest))
                 (eql (getf reference :end) (getf certificate :fork)))
      (error "permanent source restartpoint receipt lacks exact archived F coverage"))
    receipt))

(defun logical-slot-copy-source-proof-check-plan (state plan receipt child-control)
  ;; Rebuild the original plan and child control from the receipt's inputs.
  (let ((expected (physical-promotion-checkpoint-plan
                   state (getf receipt :next-oid) (getf receipt :local-floor))))
    (unless (logical-slot-copy-committed-image-equal-p expected plan)
      (error "permanent source proof changed the original child plan"))
    (let ((*physical-recovery-state* state))
      (unless (equalp child-control
                      (physical-promotion-child-control-image
                       expected (getf (getf expected :snapshot) :time)))
        (error "permanent source proof changed the original child control"))))
  plan)

(defun logical-slot-copy-check-source-proof (bytes)
  ;; Bind supplied images without I/O.  This establishes neither ownership
  ;; nor authority to install runtime state, admit clients or retire metadata.
  (let* ((parts (logical-slot-copy-source-proof-decode bytes))
         (journal (logical-slot-copy-install-decode (getf parts :installation)))
         (certificate (logical-slot-copy-import-decode (getf journal :certificate)))
         (descriptor (logical-slot-copy-descriptor-decode (getf parts :descriptor)))
         (evidence (logical-slot-copy-import-evidence-decode (getf parts :evidence)))
         (receipt (logical-slot-copy-source-receipt-decode (getf parts :restartpoint)))
         (fact (physical-logical-decode-fact (getf parts :fact)))
         (prefix (getf parts :prefix)))
    (unless (and (eq :pending (getf certificate :phase))
                 (= (getf journal :evidence-digest)
                    (logical-slot-copy-import-digest (getf parts :evidence)))
                 (equalp (getf parts :fact) (physical-logical-encode-fact fact)))
      (error "permanent source proof changed its original installation binding"))
    (multiple-value-bind (manifest control plan status)
        (logical-slot-copy-import-evidence-check-certificate
         evidence certificate descriptor (getf parts :base-proof))
      (declare (ignore status))
      (let* ((history (logical-slot-copy-evidence-check-history evidence certificate descriptor control))
             (metadata (getf receipt :metadata))
             (state (logical-slot-copy-source-proof-plan-state
                     manifest control descriptor metadata prefix)))
        (logical-slot-copy-source-proof-check-receipt receipt certificate prefix)
        (logical-slot-copy-evidence-check-resolved-fact certificate descriptor control history fact)
        (when (and (getf metadata :source-logical-fact)
                   (not (equal fact (getf metadata :source-logical-fact))))
          (error "permanent source fact differs from original restartpoint evidence"))
        (logical-slot-copy-source-proof-check-plan state plan receipt (getf parts :child-control))
        (list :certificate certificate :journal journal :descriptor descriptor :evidence evidence
              :manifest manifest :control control :plan plan :history history :fact fact
              :receipt receipt :prefix prefix :child-control (getf parts :child-control)
              :plan-state state)))))
