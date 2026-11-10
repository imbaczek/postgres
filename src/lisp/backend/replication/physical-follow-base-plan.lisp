(in-package #:pg)

;; Copy the history prefix for the selected child from the saved full history.
;; Check the resulting lineage before using it in a local plan.
(defun physical-follow-history-prefix (source child text)
  (unless (and (typep source '(integer 2 4294967295))
               (typep child `(integer 2 ,source))
               (stringp text)
               (<= (length (string->bytes text)) +physical-store-history-maximum-bytes+))
    (error "invalid follow child history request"))
  (let ((history (timeline-check-lineage (timeline-parse-history source text))))
    (unless (member child history :key #'timeline-history-entry-timeline)
      (error "follow child timeline is absent from the saved source history"))
    (when (= source child)
      (return-from physical-follow-history-prefix (copy-seq text)))
    (with-input-from-string (stream text)
      (loop for position = (file-position stream)
            for line = (read-line stream nil nil)
            while line
            for first = (position-if-not #'sql-array-space-p line)
            when (and first (not (char= (char line first) #\#)))
              do (multiple-value-bind (timeline end) (timeline-history-fields line)
                   (declare (ignore end))
                   (when (= timeline child)
                     (unless (and (plusp position) (char= (char text (1- position)) #\Newline))
                       (error "saved source history lacks the copied parent separator"))
                     (let* ((prefix (subseq text 0 (1- position)))
                            (previous (timeline-check-lineage (timeline-parse-history child prefix))))
                       (unless (physical-store-history-extends-p previous history)
                         (error "derived follow child history changes source ancestry"))
                       (return-from physical-follow-history-prefix prefix))))))
    (error "saved source history has no child ancestor record")))

(defun physical-follow-anchor-child-history (anchor)
  (multiple-value-bind (history fork eor) (physical-follow-anchor-check anchor)
    (declare (ignore fork))
    (when (eq :descendant (physical-follow-anchor-kind anchor))
      (let ((child (timeline-check-end-of-recovery eor (getf anchor :receive-timeline) history)))
        (physical-follow-history-prefix
         (getf anchor :source-timeline) child (getf anchor :history-text))))))

(defun physical-follow-empty-base-model (base)
  (let ((end (getf base :end)))
    (%make-physical-store
     :system-id (getf base :system-id) :timeline (getf base :timeline)
     :base-lsn end :floor-lsn end :written-lsn end :flushed-lsn end
     :retained-identity (physical-recovery-retained-identity base))))

;; Build a private empty receive model at base end B.  Floor, write and flush
;; all start at B with seed digests.  At a fork, select the immediate child,
;; while applied and required remain (B, parent timeline).  The phase-2
;; recovery witness is a plan; the installed recovery control remains phase 0.
(defun physical-follow-empty-base-binding (base anchor)
  "Build a pending binding with copied facts at the immutable base frontier."
  (let* ((base (copy-tree base))
         (anchor (physical-follow-anchor-copy anchor))
         (end (getf base :end)) (timeline (getf base :timeline))
         (model (physical-follow-empty-base-model base))
         (before (physical-store-control-tuple model))
         (pair (list end timeline))
         (text (physical-follow-anchor-child-history anchor)))
    (multiple-value-bind (required required-timeline resource fpw) (physical-recovery-control-frontier base end)
      (unless (equal pair (list required required-timeline))
        (error "empty base has a different required recovery frontier"))
      (multiple-value-bind (history fork eor) (physical-follow-anchor-check anchor)
        (when (eq :descendant (physical-follow-anchor-kind anchor))
          (unless (<= end fork) (error "follow fork precedes the immutable base"))
          (when (= end fork)
            (setf (physical-store-selection model)
                  (physical-store-history-candidate
                   model (timeline-check-end-of-recovery eor timeline history)
                   text (coerce (getf anchor :eor-octets) '(vector (unsigned-byte 8)))
                   #xCBF29CE484222325)))))
      (let ((binding
              (list :version 1 :phase :pending :anchor anchor :child-history text
                    :before before :after (physical-store-control-tuple model)
                    :applied pair :required (copy-list pair)
                    :recovery
                    (append (list :phase 2 :cluster-identity (copy-seq (getf base :cluster-identity))
                                  :checkpoint (getf base :checkpoint)
                                  :observed-next-xid (getf base :observed-next-xid)
                                  :history-target nil :history-digest nil :restartpoint nil)
                            (when resource (list :resource-fact (copy-tree resource)))
                            (when fpw (list :fpw-fact (physical-fpw-copy-fact fpw)))))))
        (physical-follow-binding-check binding)
        (values binding model)))))

;; Validate the empty B..B replay using the base's checkpoint, reservation,
;; XID and resource evidence.  This adapter reads no receive files and refuses
;; any request to advance its mapper beyond B.
(defun physical-follow-validate-empty-base-replay (model base binding)
  (physical-follow-binding-check binding)
  (let* ((base (copy-tree base)) (end (getf base :end)) (timeline (getf base :timeline))
                                 (pair (list end timeline)) (observed (getf base :observed-next-xid))
                                 (checkpoint (getf base :source-checkpoint)) (reservation (getf base :source-reservation))
                                 (resource (physical-restartpoint-resource-seed nil base model end))
                                 (fpw (physical-restartpoint-fpw-seed nil base model end))
                                 (seed #xCBF29CE484222325)
                                 (before (list (getf base :system-id) timeline end end seed end seed nil nil
                                               (getf base :manifest-digest) nil)))
    (unless (and (null (physical-store-directory model)) (null (physical-store-stream model))
                 (null (physical-store-lock-stream model)) (null (physical-store-retained-root model))
                 (not (physical-store-closed-p model)) (not (physical-store-failed-p model))
                 (= end (physical-store-base-lsn model) (physical-store-floor-lsn model)
                    (physical-store-written-lsn model) (physical-store-flushed-lsn model))
                 (= seed (physical-store-floor-digest model) (physical-store-written-digest model)
                    (physical-store-flushed-digest model))
                 (equal before (getf binding :before))
                 (equal (physical-store-control-tuple model) (getf binding :after))
                 (equal pair (getf binding :applied)) (equal pair (getf binding :required))
                 (equal (getf base :cluster-identity) (getf (getf binding :recovery) :cluster-identity))
                 (= observed (getf base :next-xid) (getf (getf binding :recovery) :observed-next-xid))
                 (or (null checkpoint)
                     (and (wal-checkpoint-snapshot-valid-p checkpoint)
                          (<= (getf checkpoint :end) end) (= timeline (getf checkpoint :timeline))))
                 (or (null reservation)
                     (and (physical-source-reservation-valid-p reservation)
                          (= end (getf reservation :through))
                          (or (not (getf reservation :complete))
                              (<= observed (1+ (getf reservation :bound)))))))
      (error "empty-base replay model differs from its verified base or seed"))
    (setf (getf base :required-applied) end (getf base :required-timeline) timeline
          (getf base :required-resource-fact) (copy-tree (getf (getf binding :recovery) :resource-fact))
          (getf base :required-fpw-fact) (physical-fpw-copy-fact (getf (getf binding :recovery) :fpw-fact)))
    (physical-follow-scan-replay-from-seed-core
     model base pair pair observed
     (list end timeline observed (copy-tree checkpoint) (copy-tree reservation) nil resource fpw)
     (lambda (function start)
       (declare (ignore function))
       (unless (= start end) (error "empty-base replay cannot skip WAL"))
       nil)
     nil)))
