(in-package #:pg)

(defun logical-slot-imported-check-origin-history (child parent fork original-bytes history)
  ;; Check that the current history preserves the imported origin.
  ;;
  ;; The child's begin point, its parent, and all older intervals must match
  ;; the certified history.  The child's end may now be set if a later
  ;; timeline descended from it.
  ;;
  ;; This only observes history; it grants no admission or WAL retention
  ;; authority.  Parsing is bounded, but this check is not intended for
  ;; per-record guards.
  (unless (and (typep child '(integer 2 4294967295))
               (typep parent '(integer 1 4294967295)) (< parent child)
               (typep fork '(unsigned-byte 64))
               (typep original-bytes '(vector (unsigned-byte 8)))
               (<= (length original-bytes) +physical-store-history-maximum-bytes+))
    (error "invalid imported origin history fields"))
  (logical-slot-copy-descriptor-list-count history (1+ +physical-store-history-maximum-bytes+))
  (dolist (entry history)
    (unless (timeline-history-entry-p entry) (error "invalid imported current history entry")))
  (timeline-check-lineage history)
  (let* ((original (timeline-check-lineage
                    (timeline-parse-history child (bytes->string original-bytes))))
         (observed (member child history :key #'timeline-history-entry-timeline)))
    (unless (and (eql fork (timeline-history-entry-begin (first original)))
                 (second original)
                 (eql parent (timeline-history-entry-timeline (second original)))
                 observed
                 (eql fork (timeline-history-entry-begin (first observed)))
                 (logical-slot-imported-history-equal-p (rest original) (rest observed)))
      (error "current history does not preserve the imported child and its ancestors")))
  t)
