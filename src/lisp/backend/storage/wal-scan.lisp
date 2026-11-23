;;;; Primary WAL record scanning and unflushed tail repair.
(in-package #:pg)

(defun wal-truncate-tail (path offset)
  (with-open-file (stream path :direction :io
                               :element-type '(unsigned-byte 8)
                               :if-exists :overwrite)
    (sb-posix:ftruncate (sb-sys:fd-stream-fd stream) offset)
    (fsync-stream stream)))

(defun wal-truncate-unflushed-tail (segment last-segment offset lsn reason)
  (unless (and (eql segment last-segment)
               (>= lsn *wal-durable-lsn*))
    (error "~a at LSN ~a before durable LSN ~a"
           reason lsn *wal-durable-lsn*))
  (wal-truncate-tail (wal-segment-path segment) offset)
  (values))

(defun wal-read-scan-record (stream segment last-segment offset size)
  "Read and decode the record at OFFSET, truncating only an unflushed tail."
  (let* ((record-lsn (+ (* segment +wal-segment-size+) offset))
         (remaining (- size offset)))
    (when (< remaining +wal-header-size+)
      (wal-truncate-unflushed-tail segment last-segment offset record-lsn
                                   "incomplete WAL header")
      (return-from wal-read-scan-record (values nil nil)))
    (file-position stream offset)
    (let ((header (make-array +wal-header-size+
                              :element-type '(unsigned-byte 8))))
      (unless (= (read-sequence header stream) +wal-header-size+)
        (error "could not read WAL record header"))
      ;; Recognized timeline-transition bytes must not enter ordinary tail repair,
      ;; even if an interrupted write left their body incomplete or malformed.
      (when (and (equalp (subseq header 0 4) #(80 71 87 76))
                 (>= (wal-u16 header 4) 7))
        (wal-require-ordinary-record-type (wal-u16 header 6)))
      (let ((record-length (wal-u32 header 8)))
        (unless (and (>= record-length +wal-header-size+)
                     (<= record-length +wal-segment-size+)
                     (<= (+ offset record-length) +wal-segment-size+))
          (wal-truncate-unflushed-tail segment last-segment offset record-lsn
                                       "invalid WAL record length")
          (return-from wal-read-scan-record (values nil nil)))
        (when (> record-length remaining)
          (wal-truncate-unflushed-tail segment last-segment offset record-lsn
                                       "incomplete durable WAL record")
          (return-from wal-read-scan-record (values nil nil)))
        (let ((bytes (make-array record-length
                                 :element-type '(unsigned-byte 8))))
          (file-position stream offset)
          (unless (= (read-sequence bytes stream) record-length)
            (error "could not read WAL record at LSN ~a" record-lsn))
          (values
           (handler-case
               (wal-decode-record bytes record-lsn)
             (error (condition)
               (if (and (eql segment last-segment)
                        (>= record-lsn *wal-durable-lsn*))
                   (progn
                     (wal-truncate-tail (wal-segment-path segment) offset)
                     nil)
                   (error condition))))
           record-length))))))

(defun wal-scan-checkpoint-record (record record-lsn scan-from-lsn checkpoint-record-start)
  "Validate the published checkpoint against its independent durable control copy."
  (when (and (not scan-from-lsn) (plusp *wal-checkpoint-lsn*)
             (or (= record-lsn checkpoint-record-start)
                 (= (wal-record-end record) *wal-checkpoint-lsn*)))
    (unless (and (= record-lsn checkpoint-record-start)
                 (= (wal-record-end record) *wal-checkpoint-lsn*)
                 (equal (wal-checkpoint-record-snapshot record) *wal-checkpoint-snapshot*))
      (error "WAL checkpoint differs from its durable control copy"))
    t))

(defun wal-scan-segment (function segment last-segment scan-start-offset
                         scan-end-lsn scanned-end-lsn scan-from-lsn
                         checkpoint-record-start checkpoint-found)
  "Visit one segment, preserving WAL tail-truncation and checkpoint checks."
  (let ((stop-p nil)
        (checkpoint-found checkpoint-found))
    (with-open-file (stream (wal-segment-path segment)
                            :direction :input
                            :element-type '(unsigned-byte 8))
      (let ((size (file-length stream))
            (offset scan-start-offset))
        (loop while (and (< offset size) (not stop-p)
                         (or (null scan-end-lsn)
                             (< (+ (* segment +wal-segment-size+) offset)
                                scan-end-lsn)))
              do (multiple-value-bind (record record-length)
                     (wal-read-scan-record stream segment last-segment offset size)
                   (unless record (return))
                   (wal-require-ordinary-record-type (wal-record-type record))
                   ;; Records stay within a segment; rotate only when this
                   ;; record cannot fit after the preceding scanned record.
                   ;; Check before accepting a checkpoint or invoking the callback.
                   (unless (= (wal-record-start record)
                              (wal-record-start-after scanned-end-lsn record-length))
                     (error "invalid WAL segment rotation gap after ~d" scanned-end-lsn))
                   (when (and scan-end-lsn
                              (> (wal-record-end record) scan-end-lsn))
                     (error "WAL scan end ~a is not a record boundary"
                            scan-end-lsn))
                   (when (wal-scan-checkpoint-record
                          record (wal-record-start record) scan-from-lsn
                          checkpoint-record-start)
                     (setf checkpoint-found t))
                   (setf scanned-end-lsn (wal-record-end record))
                   (if (or (eq (funcall function record) :stop)
                           (and scan-end-lsn
                                (= scanned-end-lsn scan-end-lsn)))
                       (setf stop-p t)
                       (incf offset record-length))))))
    (values scanned-end-lsn stop-p checkpoint-found)))

(defun wal-scan (function &optional scan-from-lsn &rest options)
  "Visit durable records from the checkpoint or an exact retained LSN.
FUNCTION may return :STOP to end the scan after the current record.

Stopping at each decoded record mirrors the XLogReadRecord() consumer loop in
src/backend/access/transam/xlogreader.c."
  (let* ((segments (wal-discover-segments))
         (last-segment (car (last segments)))
         (checkpoint-record-start
           (wal-checkpoint-record-start))
         (scan-start-lsn (or scan-from-lsn checkpoint-record-start))
         (scan-start-segment (floor scan-start-lsn +wal-segment-size+))
         (scan-start-offset (mod scan-start-lsn +wal-segment-size+))
         (scan-segments
           (if (zerop scan-start-lsn)
               segments
               (member scan-start-segment segments)))
         (scan-end-lsn (getf options :end-lsn))
         (scanned-end-lsn scan-start-lsn)
         (checkpoint-found (or scan-from-lsn (zerop *wal-checkpoint-lsn*)))
         (stop-p nil))
    (when (and scan-from-lsn (> scan-from-lsn *wal-durable-lsn*))
      (error "WAL scan start ~a is past durable WAL ~a"
             scan-from-lsn *wal-durable-lsn*))
    (when (and scan-end-lsn
               (not (and (integerp scan-end-lsn)
                         (<= scan-start-lsn scan-end-lsn
                             *wal-durable-lsn*))))
      (error "WAL scan end ~a is outside the durable scan range"
             scan-end-lsn))
    (when (and (not scan-from-lsn) (plusp *wal-checkpoint-lsn*)
               (< *wal-checkpoint-lsn* +wal-header-size+))
      (error "WAL checkpoint LSN ~a is invalid" *wal-checkpoint-lsn*))
    (when (and (not scan-from-lsn) (zerop *wal-checkpoint-lsn*) segments
               (not (zerop (first segments))))
      (error "WAL segment 0 is missing before the first checkpoint"))
    (when (and (< scan-start-lsn *wal-durable-lsn*)
               (null scan-segments))
      (error "WAL scan start segment ~a is missing" scan-start-segment))
    (loop for (left right) on scan-segments
          while right
          unless (= right (1+ left))
            do (error "missing WAL segment between ~a and ~a" left right))
    (loop for segment in scan-segments
          while (not stop-p)
          do (multiple-value-setq (scanned-end-lsn stop-p checkpoint-found)
               (wal-scan-segment
                function segment last-segment
                (if (= segment scan-start-segment) scan-start-offset 0)
                scan-end-lsn scanned-end-lsn scan-from-lsn
                checkpoint-record-start checkpoint-found)))
    (when (and scan-end-lsn (not stop-p)
               (/= scanned-end-lsn scan-end-lsn))
      (error "WAL scan stopped at ~a before requested end ~a"
             scanned-end-lsn scan-end-lsn))
    (unless (or stop-p checkpoint-found)
      (error "WAL checkpoint LSN ~a is not present in retained WAL"
             *wal-checkpoint-lsn*))
    (when (and (not stop-p) (not scan-from-lsn)
               (> *wal-checkpoint-lsn* scanned-end-lsn))
      (error "WAL checkpoint LSN ~a is past the end of WAL ~a"
             *wal-checkpoint-lsn* scanned-end-lsn))
    (when (and (not stop-p) (not scan-end-lsn)
               (> *wal-durable-lsn* scanned-end-lsn))
      (error "WAL durable LSN ~a is past the end of WAL ~a"
             *wal-durable-lsn* scanned-end-lsn))
    (values scanned-end-lsn scan-segments)))
