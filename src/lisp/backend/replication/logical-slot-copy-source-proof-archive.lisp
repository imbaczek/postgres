(in-package #:pg)

(defun logical-slot-copy-source-proof-parent-history (facts)
  ;; Recover the parent lineage and require the certificate's fixed fork.
  (let ((certificate (getf facts :certificate)))
    (multiple-value-bind (parent fork history)
        (physical-promotion-retained-parent-history
         (timeline-parse-history
          (getf certificate :child-timeline)
          (bytes->string (getf (getf facts :evidence) :child-history))))
      (unless (and (= parent (getf certificate :parent-timeline))
                   (= fork (getf certificate :fork)))
        (error "permanent source parent history differs from its certificate"))
      history)))

(defun logical-slot-copy-source-proof-map-archive
    (facts directory reader candidate function start)
  ;; Feed validated archive records from START to exact F, rejecting crossings
  ;; and gaps that do not follow the embedded history.
  (let* ((certificate (getf facts :certificate))
         (reference (getf certificate :retained-reference))
         (history (getf facts :history))
         (parent (getf certificate :parent-timeline))
         (fork (getf certificate :fork))
         (cursor start)
         (archive-history (logical-slot-copy-source-proof-parent-history facts)))
    (physical-promotion-check-retained-reference
     (getf certificate :system-id) reference parent archive-history
     :end fork :directory directory :reader reader
     :on-record
     (lambda (record raw)
       (let ((begin (wal-record-start record)) (end (wal-record-end record)))
         (cond
           ((<= end cursor) nil)
           ((< begin cursor) (error "permanent source archive crosses its replay cursor"))
           ((< begin fork)
            (when (> end fork) (error "permanent source archive crosses F"))
            (when (> begin cursor) (wal-lineage-check-gap cursor record history))
            (when (= (wal-record-type record) +wal-record-end-of-recovery+)
              (let* ((owner (timeline-at-point begin history))
                     (entry (member owner history :key #'timeline-history-entry-timeline)))
                (unless (and (second entry)
                             (= cursor (timeline-history-entry-begin (first entry))))
                  (error "permanent source archive EOR differs from embedded ancestry"))
                (setf (physical-store-selection candidate)
                      (list :history history :target owner
                            :parent (timeline-history-entry-timeline (second entry))
                            :fork cursor :eor-bytes raw
                            :previous (physical-store-selection candidate)))))
            (funcall function record raw)
            (setf cursor end))))))
    (unless (= cursor fork) (error "permanent source archive did not reach exact F"))
    nil))

(defun logical-slot-copy-source-proof-scan-archive (facts directory reader)
  ;; Replay from the embedded D checkpoint seed to clean F and compare the
  ;; resulting fact.  This is a historical observation, not promotion authority.
  (let* ((certificate (getf facts :certificate))
         (manifest (getf facts :manifest))
         (descriptor (getf facts :descriptor))
         (control (getf facts :control))
         (fork (getf certificate :fork))
         (parent (getf certificate :parent-timeline))
         (checkpoint (wal-decode-record
                      (getf (getf facts :evidence) :checkpoint)
                      (physical-manifest-checkpoint-start manifest)))
         (reservation (physical-recovery-checkpoint-reservation
                       manifest checkpoint (getf (getf facts :receipt) :local-floor)))
         (base (logical-slot-copy-import-base-state
                manifest descriptor control :reservation reservation))
         (candidate (%make-physical-store
                     :system-id (getf certificate :system-id)
                     :timeline (getf manifest :timeline) :base-lsn (getf manifest :end)
                     :floor-lsn fork :flushed-lsn fork
                     :selection (list :history (getf facts :history)
                                      :target (timeline-history-entry-timeline
                                               (first (getf facts :history))))))
         (seed (list (getf base :end) (getf base :timeline) (getf base :next-xid)
                     (getf base :source-checkpoint) (getf base :source-reservation) nil
                     (getf base :base-source-resource-fact)
                     (getf base :base-source-fpw-fact) (getf base :base-source-logical-fact)))
         (result
           (physical-follow-scan-replay-from-seed-core
            candidate base (list fork parent) (list fork parent) (getf control :next-xid) seed
            (lambda (function start)
              (logical-slot-copy-source-proof-map-archive
               facts directory reader candidate function start))
            nil)))
    (unless (and (null (getf result :disposition))
                 (= fork (getf result :terminal))
                 (= parent (getf result :timeline))
                 (= (getf control :next-xid) (getf result :next-xid))
                 (equal (getf facts :fact) (getf result :applied-logical-fact)))
      (error "permanent source archive replay differs from its recorded F fact"))
    result))

(defun logical-slot-copy-source-proof-check-archive-prefix (facts directory reader)
  ;; Match the saved F prefix against the selected archived segment.
  (let* ((certificate (getf facts :certificate))
         (reference (getf certificate :retained-reference))
         (fork (getf certificate :fork))
         (size (mod fork +wal-segment-size+))
         (prefix (getf facts :prefix))
         (history (logical-slot-copy-source-proof-parent-history facts)))
    (unless (= size (length prefix)) (error "permanent source prefix has a different F geometry"))
    (when (plusp size)
      (let ((bytes (physical-follow-retained-segment
                    reference
                    (physical-promotion-retained-identity (getf certificate :system-id) reference)
                    history (floor fork +wal-segment-size+) directory reader)))
        (unless (and (<= size (length bytes)) (equalp prefix (subseq bytes 0 size)))
          (error "permanent source prefix differs from its archived bytes"))))
    prefix))
