;;;; Pending child history/EOR publication, before committed primary control.
(in-package #:pg)

(defvar *physical-promotion-test-hook* nil)

(defun physical-promotion-test-point (point)
  (when *physical-promotion-test-hook* (funcall *physical-promotion-test-hook* point)))

(defun physical-promotion-eor-image (state)
  "Stable pending bytes: intent time in PG microseconds, fixed native logical level."
  (let* ((fork (getf state :applied))
         (remaining (- +wal-segment-size+ (mod fork +wal-segment-size+)))
         (start (if (<= +wal-end-of-recovery-record-size+ remaining) fork (+ fork remaining)))
         (time (* 1000000 (- (getf state :state-time) 946684800))))
    (values (wal-encode-end-of-recovery-record
             start time (getf state :promotion-child) (physical-recovery-replay-timeline state) 2)
            start)))

(defun physical-promotion-valid-tail-frame-p (bytes)
  "Recognize even unsupported CRC-valid native frames before considering repair."
  (when (and (>= (length bytes) +wal-header-size+)
             (equalp (subseq bytes 0 4) #(80 71 87 76)))
    (let ((size (wal-u32 bytes 8)))
      (when (<= +wal-header-size+ size (length bytes))
        (let* ((copy (subseq bytes 0 size)) (crc (wal-u32 copy 28)))
          (wal-set-u32 copy 28 0)
          (= crc (wal-crc32 copy)))))))

(defun physical-promotion-check-eor-file (path prefix eor &key before-history)
  "Preserve verified prefix; only bounded uncommitted partial/torn EOR is repairable."
  (let* ((bytes (physical-promotion-read-file path)) (offset (length prefix)))
    (unless (and (<= offset (length bytes) (+ offset (length eor)))
                 (equalp prefix (subseq bytes 0 offset)))
      (error "pending child prefix or EOR extent differs"))
    (let ((tail (subseq bytes offset)))
      (when (or (and before-history (plusp (length tail)))
                (and (not (equalp tail eor)) (physical-promotion-valid-tail-frame-p tail)))
        (error "pending child contains a different complete record"))
      (equalp tail eor))))

(defun physical-promotion-sync-history (state)
  "History inode and directory must be durable even when retry finds matching bytes."
  (let ((path (timeline-history-path (getf state :promotion-child) *pgdata*))
        (expected (physical-promotion-history-image state)))
    (unless (wal-namespace-file-stat path t) (timeline-publish-history-bytes path expected))
    (multiple-value-bind (stream size) (physical-promotion-open-file path t)
      (unwind-protect
           (let ((bytes (make-array size :element-type '(unsigned-byte 8))))
             (unless (and (= size (read-sequence bytes stream)) (equalp bytes expected))
               (error "pending history changed before durability"))
             (fsync-stream stream))
        (close stream)))
    (fsync-directory (wal-directory))))

(defun physical-promotion-write-eor (path prefix eor)
  "Repair only a reverified owned suffix, then sync WAL before future control changes."
  (physical-promotion-check-eor-file path prefix eor)
  (multiple-value-bind (stream size) (physical-promotion-open-file path t)
    (declare (ignore size))
    (unwind-protect
         (progn
           (physical-promotion-test-point :before-eor-write)
           (file-position stream (length prefix))
           (write-sequence eor stream)
           (finish-output stream)
           (physical-promotion-test-point :after-eor-write)
           (fsync-stream stream)
           (physical-promotion-test-point :after-eor-fsync))
      (close stream)))
  (fsync-directory (wal-directory))
  (physical-promotion-test-point :after-eor-directory-fsync))

(defun physical-promotion-wal-plan (state prefix &key allow-uninstalled)
  "Check child artifacts before mutation, including cold recovery's catalog restore."
  (multiple-value-bind (eor start) (physical-promotion-eor-image state)
    (let* ((fork (getf state :applied)) (child (getf state :promotion-child))
                                        (first (wal-segment-path (floor fork +wal-segment-size+) child :timeline))
                                        (target (wal-segment-path (floor start +wal-segment-size+) child :timeline))
                                        (rotated (/= start fork))
                                        (target-prefix (if rotated (make-array 0 :element-type '(unsigned-byte 8)) prefix))
                                        (history-p (wal-namespace-file-stat (timeline-history-path child *pgdata*) t)))
      (if (and allow-uninstalled (not history-p))
          (progn (wal-namespace-file-stat first t) (wal-namespace-file-stat target t))
          (progn
            (physical-promotion-check-eor-file first prefix (if rotated #() eor)
                                               :before-history (not history-p))
            (if (and rotated (not history-p))
                (wal-namespace-file-stat target t)
                (physical-promotion-check-eor-file target target-prefix eor
                                                   :before-history (not history-p)))))
      (values target target-prefix eor start rotated history-p))))

(defun physical-promotion-publish-wal ()
  "Privately publish history then durable EOR; parent control and admission stay closed.
A failed process remains fenced. Startup reconciliation is a separate transition."
  (with-physical-promotion-exclusion (3)
    (let ((state *physical-recovery-state*))
      (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read state)
        (declare (ignore catalog))
        (physical-promotion-check-pending state metadata prefix generation :allow-history t)
        (physical-promotion-ensure-pending-retained state metadata)
        (multiple-value-bind (target target-prefix eor start rotated history-p)
            (physical-promotion-wal-plan state prefix)
          (let ((finished nil))
            (unwind-protect
                 (progn
                   (when (and rotated (not history-p))
                     (wal-install-timeline-prefix (physical-recovery-replay-timeline state) (getf state :promotion-child)
                                                  start target-prefix
                                                  (physical-promotion-parent-namespace state)))
                   (physical-promotion-test-point :before-history)
                   (physical-promotion-sync-history state)
                   (physical-promotion-test-point :after-history)
                   (physical-promotion-write-eor target target-prefix eor)
                   (setf finished t)
                   (+ start (length eor)))
              (unless finished (setf *physical-recovery-failed-p* t)))))))))
