;; The source XID here is the stopped primary's actual next XID copied into
;; the stage, not the remote source's current allocator or XID.

;; Plan the offline transition from the published adoption handoff to follow.
;; The binding keeps the staged base, source pin, source XID and resource
;; witness together with the proved source history.  The child history is a
;; prefix of that history, not a newly chosen lineage.
;;
;; At the fork (E = fork), the initial recovery image still describes the old
;; history.  Separate selected images publish the child history and controls.
;; Before the fork, selection is left for later follow processing.
(in-package #:pg)

;; Copy the source history through the selected child, preserving its ancestry.
(defun physical-adoption-follow-history-prefix (source child text)
  (physical-follow-history-prefix source child text))

(defun physical-adoption-follow-child-history (anchor)
  (physical-follow-anchor-child-history anchor))

(defun physical-adoption-follow-record-anchor (record)
  (physical-follow-anchor-copy
   (physical-adoption-attempt-with-history
    (getf record :anchor) *physical-follow-anchor-keys* (getf record :source-history))))

;; Refuse a binding that changes the saved stage, source or recovery witness.
(defun physical-adoption-follow-check-binding (record binding)
  (physical-follow-binding-check binding)
  (let* ((record (physical-adoption-attempt-copy record))
         (stage (getf record :stage))
         (anchor (physical-adoption-follow-record-anchor record))
         (end (getf stage :end))
         (pair (list end (getf stage :timeline)))
         (seed #xCBF29CE484222325)
         (before (list (getf stage :system-id) (getf stage :timeline) end end seed end seed
                       nil nil (getf stage :manifest-digest) nil))
         (witness (getf binding :recovery))
         (resource (wal-copy-control-resource-fields
                    (getf (physical-adoption-install-image-state record) :snapshot)))
         (fork (nth-value 1 (physical-follow-anchor-check anchor))))
    (unless (and (equal anchor (getf binding :anchor))
                 (equal before (getf binding :before))
                 (equal pair (getf binding :applied)) (equal pair (getf binding :required))
                 (equal (physical-adoption-follow-child-history anchor) (getf binding :child-history))
                 (equal (getf stage :cluster-identity) (getf witness :cluster-identity))
                 (= (getf stage :checkpoint) (getf witness :checkpoint))
                 (= (getf stage :next-xid) (getf witness :observed-next-xid))
                 (null (getf witness :history-target)) (null (getf witness :history-digest))
                 (null (getf witness :restartpoint))
                 (equal resource (getf witness :resource-fact))
                 fork (<= end fork)
                 (eq (< end fork) (equal before (getf binding :after))))
      (error "adoption follow binding rebinds its proved stage or source"))
    binding))

(defun physical-adoption-follow-make-binding (record base control store)
  (let* ((record (physical-adoption-attempt-copy record))
         (stage (getf record :stage))
         (binding (physical-adoption-follow-planned-binding record))
         (anchor (getf binding :anchor))
         (end (getf stage :end))
         (pair (getf binding :required)))
    (unless (and (equal (physical-store-control-tuple store) (getf binding :before))
                 (every (lambda (key) (equal (getf base key) (getf stage key)))
                        '(:system-id :cluster-identity :timeline :checkpoint :end :manifest-digest))
                 (zerop (getf control :phase))
                 (= (getf control :next-xid) (getf stage :next-xid))
                 (null (getf control :history-target)) (null (getf control :retained-reference)))
      (error "adoption follow planning requires the verified empty stage frontier"))
    (multiple-value-bind (required timeline resource) (physical-recovery-control-frontier base end)
      (unless (and (equal pair (list required timeline))
                   (equal resource (getf (getf binding :recovery) :resource-fact)))
        (error "adoption recovery frontier differs from the saved stage witness")))
    (let ((candidate
            (if (equal (getf binding :before) (getf binding :after))
                (physical-follow-copy-checked-store store)
                (physical-store-follow-candidate
                 store (getf binding :before) (getf binding :applied) pair
                 (eighth (getf binding :after)) (getf binding :child-history)
                 (coerce (getf anchor :eor-octets) '(vector (unsigned-byte 8)))))))
      (physical-follow-validate-candidate-replay candidate base binding)
      (values binding candidate))))

(defun physical-adoption-follow-config (record binding)
  (physical-adoption-follow-check-binding record binding)
  (multiple-value-bind (profile owner) (physical-follow-source-profile (getf record :source))
    (declare (ignore profile))
    (multiple-value-bind (host port) (physical-libpq-frozen-display-endpoint owner)
      (let ((config (physical-follow-with-controller
                     (list 1 host port (getf (getf record :stage) :source-pin) binding)
                     (list :version 1 :enabled t :preparing nil :cleanup nil
                           :known-timeline (getf (getf binding :anchor) :source-timeline)
                           :source (physical-follow-source-copy (getf record :source))))))
        (physical-follow-check-config config)
        config))))

(defun physical-adoption-follow-control-image (record binding time &key selected)
  (physical-adoption-follow-check-binding record binding)
  (let* ((stage (getf record :stage))
         (witness (getf binding :recovery))
         (after (getf binding :after)))
    (physical-recovery-control-image
     2 (getf stage :system-id) (getf stage :timeline) (getf stage :manifest-digest)
     (getf stage :checkpoint) (first (getf binding :required)) (getf witness :observed-next-xid)
     :applied-timeline (second (getf binding :required))
     :history-target (if selected (eighth after) (getf witness :history-target))
     :history-digest (if selected (ninth after) (getf witness :history-digest))
     :resource-fact (getf witness :resource-fact) :state-time time)))

(defun physical-adoption-follow-model-store (record)
  (physical-follow-empty-base-model (getf record :stage)))

;; Derive the same binding from the saved attempt when reading the journal.
(defun physical-adoption-follow-planned-binding (record)
  (let* ((record (physical-adoption-attempt-copy record))
         (stage (getf record :stage))
         (anchor (physical-adoption-follow-record-anchor record))
         (base (copy-list stage))
         (snapshot (getf (physical-adoption-install-image-state record) :snapshot))
         (fork (nth-value 1 (physical-follow-anchor-check anchor))))
    (unless (and fork (<= (getf stage :end) fork))
      (error "adoption follow cannot retain a divergent stage frontier"))
    (setf (getf base :observed-next-xid) (getf stage :next-xid)
          (getf base :source-checkpoint) snapshot
          (getf base :source-resource-fact) (wal-copy-control-resource-fields snapshot))
    (let ((binding (physical-follow-empty-base-binding base anchor)))
      (physical-adoption-follow-check-binding record binding)
      binding)))

(defun physical-adoption-follow-selected-images (record binding time)
  (let* ((model (physical-adoption-follow-model-store record))
         (after (getf binding :after))
         (anchor (getf binding :anchor)))
    (multiple-value-bind (selection bytes)
        (physical-store-history-candidate
         model (eighth after) (getf binding :child-history)
         (coerce (getf anchor :eor-octets) '(vector (unsigned-byte 8))) (fifth after))
      (setf (physical-store-selection model) selection)
      (unless (equal after (physical-store-control-tuple model))
        (error "planned history images differ from the proved adoption binding"))
      (list (cons "history.receive" bytes)
            (cons "receive.selected" (physical-store-encode-control model (fourth after)))
            (cons "history.wal" (string->bytes (getf binding :child-history)))
            (cons "recovery.selected"
                  (physical-adoption-follow-control-image record binding time :selected t))))))

;; Build the pending, initial, selected and active images needed by the plan.
(defun physical-adoption-follow-images (record binding time)
  (physical-adoption-follow-check-binding record binding)
  (let* ((pending (physical-adoption-follow-config record binding))
         (active (copy-tree pending)))
    (setf (getf (fifth active) :phase) :active)
    (append (list (cons "follow.pending" (physical-follow-config-image pending))
                  (cons "recovery.initial" (physical-adoption-follow-control-image record binding time)))
            (unless (equal (getf binding :before) (getf binding :after))
              (physical-adoption-follow-selected-images record binding time))
            (list (cons "follow.active" (physical-follow-config-image active))))))
