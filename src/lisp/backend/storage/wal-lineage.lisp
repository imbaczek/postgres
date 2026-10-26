;;;; Bounded, nonmutating native WAL inspection along an explicitly selected history.
(in-package #:pg)

(defun wal-lineage-effective-size (size segment file-timeline history)
  "Exclude a parent file's divergent tail even when its physical length is larger."
  (let* ((entry (find file-timeline history :key #'timeline-history-entry-timeline))
         (end (timeline-history-entry-end entry)))
    (if end (max 0 (min size (- end (* segment +wal-segment-size+)))) size)))

(defun wal-lineage-open-segment (segment history previous-file-timeline namespace)
  "Open the first C-ordered candidate; only ENOENT permits an older candidate."
  (dolist (timeline (timeline-segment-candidates segment history previous-file-timeline))
    (let* ((path (wal-segment-path segment timeline namespace))
           (fd (handler-case
                   (sb-posix:open (namestring path)
                                  (logior sb-posix:o-rdonly sb-posix:o-nonblock
                                          sb-posix:o-nofollow +postmaster-o-cloexec+))
                 (sb-posix:syscall-error (condition)
                   (if (= (sb-posix:syscall-errno condition) sb-posix:enoent)
                       nil (error condition)))))
           (stream nil))
      (when fd
        (unwind-protect
             (let* ((stat (sb-posix:fstat fd)) (size (sb-posix:stat-size stat)))
               (unless (and (= sb-posix:s-ifreg (logand (sb-posix:stat-mode stat) sb-posix:s-ifmt))
                            (<= 0 size +wal-segment-size+))
                 (error "invalid lineage WAL segment: ~a" path))
               (setf stream (sb-sys:make-fd-stream fd :input t :element-type '(unsigned-byte 8)))
               (return-from wal-lineage-open-segment (values stream timeline size)))
          (unless stream (sb-posix:close fd))))))
  (error "missing lineage WAL segment ~d" segment))

(defun wal-lineage-read-record (stream segment offset size)
  "Strict decoder: never repair or truncate any candidate, including a tip."
  (let ((header (make-array +wal-header-size+ :element-type '(unsigned-byte 8)))
        (lsn (+ (* segment +wal-segment-size+) offset)))
    (when (< (- size offset) +wal-header-size+)
      (error "incomplete lineage WAL header at ~d" lsn))
    (file-position stream offset)
    (unless (= (read-sequence header stream) +wal-header-size+)
      (error "short lineage WAL header at ~d" lsn))
    (let ((length (wal-u32 header 8)))
      (unless (and (<= +wal-header-size+ length +wal-segment-size+)
                   (<= (+ offset length) size))
        (error "invalid or incomplete lineage WAL length at ~d" lsn))
      (let ((bytes (make-array length :element-type '(unsigned-byte 8))))
        (replace bytes header)
        (unless (= (read-sequence bytes stream :start +wal-header-size+) length)
          (error "short lineage WAL record at ~d" lsn))
        (wal-decode-record bytes lsn)))))

(defun wal-lineage-check-gap (previous-end record history)
  "Native rotation skips capacity only when the following record cannot fit."
  (when previous-end
    (let ((start (wal-record-start record))
          (length (- (wal-record-end record) (wal-record-start record))))
      (unless (and (= start (* (1+ (floor previous-end +wal-segment-size+))
                               +wal-segment-size+))
                   (> length (- start previous-end))
                   (notany (lambda (entry)
                             (< previous-end (timeline-history-entry-begin entry) start))
                           history))
        (error "invalid lineage WAL rotation gap after ~d" previous-end)))))

(defun wal-lineage-scan-segment (function stream segment size offset end history file-timeline gap)
  "Visit complete records in one immutable segment; return cursor and stop flag."
  (let ((cursor (+ (* segment +wal-segment-size+) offset)))
    (when (> offset size) (error "lineage WAL start is past segment EOF"))
    (loop while (and (< offset size) (< cursor end))
          for record = (wal-lineage-read-record stream segment offset size)
          for owner = (timeline-at-point cursor history)
          for entry = (find owner history :key #'timeline-history-entry-timeline)
          do (wal-lineage-check-gap gap record history)
             (setf gap nil)
             (unless (and (>= file-timeline owner)
                          (<= (wal-record-end record) end)
                          (or (null (timeline-history-entry-end entry))
                              (<= (wal-record-end record) (timeline-history-entry-end entry))))
               (error "lineage WAL record crosses a fork/end or comes from an ancestor file at ~d" cursor))
             (setf cursor (wal-record-end record)
                   offset (mod cursor +wal-segment-size+))
             (when (eq (funcall function record owner file-timeline) :stop)
               (return-from wal-lineage-scan-segment (values cursor t)))
             (when (zerop offset) (return)))
    (when gap (error "lineage WAL rotation destination is empty"))
    (values cursor nil)))

(defun wal-scan-lineage (function history start end &key (namespace :timeline))
  "Inspect [START,END) without mutating storage or asserting it is durable.
The caller owns an immutable segment set, a previously verified START cursor,
and independently verified history and cluster/prefix provenance. WAL7 headers cannot establish those facts. Callback
arguments are RECORD, its history owner TLI, and the opened filename TLI.
Return the actual cursor, used (file-TLI segment) pairs, and whether END was reached;
an empty range checks no record boundary. :STOP also reports whether END was reached.
This is not yet an operational recovery path and grants no writer authority."
  (timeline-check-lineage history)
  (unless (and (typep start '(unsigned-byte 64)) (typep end '(unsigned-byte 64))
               (<= start end) (member namespace '(:legacy :timeline))
               (or (eq namespace :timeline)
                   (every (lambda (entry) (= 1 (timeline-history-entry-timeline entry))) history)))
    (error "invalid lineage WAL scan bounds or namespace"))
  (let ((cursor start) (previous-file-timeline 0) (used nil) (gap nil))
    (loop while (< cursor end)
          do (multiple-value-bind (segment offset) (wal-segment-and-offset cursor)
               (multiple-value-bind (stream timeline size)
                   (wal-lineage-open-segment segment history previous-file-timeline namespace)
                 (unwind-protect
                      (progn
                        (push (list timeline segment) used)
                        (setf previous-file-timeline timeline)
                        (multiple-value-bind (scanned-end stopped)
                            (wal-lineage-scan-segment function stream segment
                                                      (wal-lineage-effective-size size segment timeline history)
                                                      offset end
                                                      history timeline gap)
                          (setf cursor scanned-end gap nil)
                          (when stopped (return-from wal-scan-lineage (values cursor (nreverse used) (= cursor end))))
                          (when (< cursor end)
                            (let ((next (* (1+ segment) +wal-segment-size+)))
                              (when (< cursor next)
                                (when (<= end next)
                                  (error "lineage WAL end is in a rotation gap"))
                                (setf gap cursor cursor next))))))
                   (close stream)))))
    (values cursor (nreverse used) (= cursor end))))
