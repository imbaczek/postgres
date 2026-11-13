;;; Validate the child checkpoint and exact initialization frame plan.

;;; Permanent import evidence checks describe recorded facts, not authority.
(in-package #:pg)

(defconstant +logical-slot-copy-evidence-record-maximum+
  (+ +wal-header-size+ +wal-checkpoint-resource-snapshot-size+
     +wal-checkpoint-commit-ts-size+ +wal-checkpoint-logical-interval-size+))

(defun logical-slot-copy-evidence-check-snapshot (snapshot record)
  "Match the checkpoint snapshot and its canonical payload to RECORD."
  (let* ((actual (wal-checkpoint-record-snapshot record))
         (count (logical-slot-copy-descriptor-list-count snapshot 80))
         (keys (loop for key in snapshot by #'cddr collect key)))
    (unless (and actual (= count (length actual))
                 (= (length keys) (length (remove-duplicates keys)))
                 (loop for (key value) on snapshot by #'cddr
                       always (and (get-properties actual (list key))
                                   (equalp value (getf actual key))))
                 (equalp (wal-encode-checkpoint-snapshot snapshot) (wal-record-payload record)))
      (error "import evidence checkpoint snapshot differs from its frame"))))

(defun logical-slot-copy-evidence-check-plan (plan)
  "Check canonical 3, 5 or 7 frame initialization grammar and checkpoint fields.
EOR time converts snapshot Unix seconds to PostgreSQL microseconds."
  (unless (and (= 10 (logical-slot-copy-descriptor-list-count plan 10))
               (equal (loop for key in plan by #'cddr collect key)
                      '(:fork :child :snapshot :end :frames))
               (typep (getf plan :fork) '(unsigned-byte 64))
               (typep (getf plan :child) '(integer 2 4294967295))
               (typep (getf plan :end) '(unsigned-byte 64))
               (evenp (logical-slot-copy-descriptor-list-count (getf plan :snapshot) 80))
               (wal-checkpoint-snapshot-valid-p (getf plan :snapshot))
               (<= 1 (length (string->bytes
                              (physical-bootstrap-manifest-text (getf plan :snapshot)))) 16384))
    (error "invalid import evidence child plan"))
  (let ((cursor (getf plan :fork)) (records nil))
    (unless (member (logical-slot-copy-descriptor-list-count (getf plan :frames) 7) '(3 5 7))
      (error "invalid import evidence frame count"))
    (dolist (frame (getf plan :frames))
      (unless (= 2 (logical-slot-copy-descriptor-list-count frame 2))
        (error "invalid import evidence frame row"))
      (destructuring-bind (start bytes) frame
        (logical-slot-copy-descriptor-blob-size bytes +wal-header-size+
                                                +logical-slot-copy-evidence-record-maximum+)
        (unless (and (typep start '(unsigned-byte 64))
                     (= start (wal-record-start-after cursor (length bytes))))
          (error "invalid import evidence frame placement"))
        (let ((record (wal-decode-record bytes start)))
          (unless (and (zerop (wal-record-xid record))
                       (or (/= (wal-record-type record) +wal-record-batch-start+)
                           (zerop (length (wal-record-payload record))))
                       (equalp bytes (wal-encode-record (wal-record-type record) 0 start
                                                        (wal-record-payload record))))
            (error "noncanonical import evidence frame"))
          (push record records)
          (setf cursor (wal-record-end record)))))
    (setf records (nreverse records))
    (let ((types (mapcar #'wal-record-type records))
          (batch +wal-record-batch-start+) (checkpoint +wal-record-checkpoint+)
          (eor +wal-record-end-of-recovery+) (fpw +wal-record-full-page-writes-change+)
          (logical +wal-record-logical-decoding-status-change+))
      (unless (member types (list (list eor batch checkpoint)
                                  (list eor batch fpw batch checkpoint)
                                  (list eor batch logical batch checkpoint)
                                  (list eor batch fpw batch logical batch checkpoint))
                      :test #'equal)
        (error "invalid import evidence initialization grammar")))
    (let* ((snapshot (getf plan :snapshot))
           (eor (wal-end-of-recovery-record-info (first records))))
      (logical-slot-copy-evidence-check-snapshot snapshot (car (last records)))
      (unless (and (= cursor (getf plan :end) (getf snapshot :end))
                   (= (getf plan :child) (getf snapshot :timeline) (getf eor :timeline))
                   (= (getf snapshot :prev-timeline) (getf eor :prev-timeline))
                   (= (* 1000000 (- (getf snapshot :time) 946684800)) (getf eor :end-time)))
        (error "import evidence child checkpoint or EOR differs"))
      (dolist (record records)
        (cond
          ((= (wal-record-type record) +wal-record-full-page-writes-change+)
           (unless (and (get-properties snapshot '(:full-page-writes))
                        (eql (getf snapshot :full-page-writes)
                             (wal-full-page-writes-change-record-info record)))
             (error "import evidence loses its child FPW policy")))
          ((= (wal-record-type record) +wal-record-logical-decoding-status-change+)
           (unless (and (get-properties snapshot '(:logical-decoding-enabled))
                        (eql (getf snapshot :logical-decoding-enabled)
                             (wal-logical-decoding-status-record-info record)))
             (error "import evidence loses its child logical policy")))))))
  plan)
