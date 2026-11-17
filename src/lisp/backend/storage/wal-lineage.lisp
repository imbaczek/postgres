;;;; Bounded, nonmutating native WAL inspection along an explicitly selected history.
(in-package #:pg)

(defun wal-lineage-effective-size (size segment file-timeline history)
  "Exclude a parent file's divergent tail even when its physical length is larger."
  (let* ((entry (find file-timeline history :key #'timeline-history-entry-timeline))
         (end (timeline-history-entry-end entry)))
    (if end (max 0 (min size (- end (* segment +wal-segment-size+)))) size)))

(define-condition wal-lineage-segment-unavailable (error)
  ;; The namespace and segment locate the unavailable carrier.  The reason
  ;; distinguishes exhaustion of all candidates from a missing current carrier.
  ((segment :initarg :segment :reader wal-lineage-unavailable-segment)
   (namespace :initarg :namespace :reader wal-lineage-unavailable-namespace)
   (reason :initarg :reason :initform :missing :reader wal-lineage-unavailable-reason))
  (:report
   (lambda (condition stream)
     (ecase (wal-lineage-unavailable-reason condition)
       (:missing
        (format stream "missing lineage WAL segment ~d"
                (wal-lineage-unavailable-segment condition)))
       (:current-carrier
        (write-string "physical follow requires a complete source WAL extent; reseed is required"
                      stream))))))

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
  (error 'wal-lineage-segment-unavailable :segment segment :namespace namespace))

(defun wal-lineage-read-record (source segment offset size)
  "Decode one record from a bounded file or copied segment prefix without repair.
SIZE bounds both the header and record; a vector must contain that prefix. The
returned record uses SEGMENT and OFFSET for its logical WAL location. The second
value is the original encoded record bytes."
  (let ((header (make-array +wal-header-size+ :element-type '(unsigned-byte 8)))
        (lsn (+ (* segment +wal-segment-size+) offset))
        (memory (typep source '(vector (unsigned-byte 8)))))
    (unless (and (<= 0 offset size +wal-segment-size+)
                 (or (not memory) (<= size (length source)))
                 (<= (+ offset +wal-header-size+) size))
      (error "incomplete lineage WAL header at ~d" lsn))
    (if memory
        (replace header source :start2 offset :end2 (+ offset +wal-header-size+))
        (progn
          (file-position source offset)
          (unless (= (read-sequence header source) +wal-header-size+)
            (error "short lineage WAL header at ~d" lsn))))
    (let ((length (wal-u32 header 8)))
      (unless (and (<= +wal-header-size+ length +wal-segment-size+)
                   (<= (+ offset length) size))
        (error "invalid or incomplete lineage WAL length at ~d" lsn))
      (let ((bytes (make-array length :element-type '(unsigned-byte 8))))
        (replace bytes header)
        (if memory
            (replace bytes source :start1 +wal-header-size+
                                  :start2 (+ offset +wal-header-size+) :end2 (+ offset length))
            (unless (= (read-sequence bytes source :start +wal-header-size+) length)
              (error "short lineage WAL record at ~d" lsn)))
        (values (wal-decode-record bytes lsn) bytes)))))

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

(defun wal-lineage-scan-segment (function stream segment size offset end history file-timeline gap &optional include-raw)
  "Visit complete records in one immutable segment; return cursor and stop flag."
  (let ((cursor (+ (* segment +wal-segment-size+) offset)))
    (when (> offset size) (error "lineage WAL start is past segment EOF"))
    (loop while (and (< offset size) (< cursor end))
          do (multiple-value-bind (record raw)
                 (wal-lineage-read-record stream segment offset size)
               (let* ((owner (timeline-at-point cursor history))
                      (entry (find owner history :key #'timeline-history-entry-timeline)))
                 (wal-lineage-check-gap gap record history)
                 (setf gap nil)
                 (unless (and (>= file-timeline owner)
                              (<= (wal-record-end record) end)
                              (or (null (timeline-history-entry-end entry))
                                  (<= (wal-record-end record) (timeline-history-entry-end entry))))
                   (error "lineage WAL record crosses a fork/end or comes from an ancestor file at ~d" cursor))
                 (setf cursor (wal-record-end record)
                       offset (mod cursor +wal-segment-size+))
                 (when (eq (if include-raw
                               (funcall function record owner file-timeline raw)
                               (funcall function record owner file-timeline)) :stop)
                   (return-from wal-lineage-scan-segment (values cursor t)))
                 (when (zerop offset) (return)))))
    (when gap (error "lineage WAL rotation destination is empty"))
    (values cursor nil)))

(defun wal-scan-lineage (function history start end &key (namespace :timeline) include-raw)
  "Inspect [START,END) without mutating storage or asserting it is durable.
The caller owns an immutable segment set, a previously verified START cursor,
and independently verified history and cluster/prefix provenance. WAL headers
cannot establish those facts. Callback arguments are RECORD, its history owner
TLI, and the opened filename TLI. With INCLUDE-RAW, a fourth argument contains
the original encoded record bytes; by default the callback takes three arguments.
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
                                                      history timeline gap include-raw)
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

(defun wal-check-shared-lineage-prefix
    (left-history right-directory right-history start end
     &key (left-namespace :timeline) (right-namespace :timeline))
  "Verify that two immutable WAL segment sets share the nonempty interval [START, END).
LEFT-HISTORY describes the current PGDATA. RIGHT-HISTORY describes RIGHT-DIRECTORY.
The caller must verify cluster identity, both histories, and the START cursor
and keep the segment sets stable. Compare original record bytes and their
owning timelines. Return T on success or signal an error on a failed check."
  (unless (< start end) (error "shared lineage WAL interval is empty"))
  (let* ((remaining 0)
         (right-files
           (let ((*pgdata* right-directory))
             (multiple-value-bind (cursor files complete)
                 (wal-scan-lineage (lambda (record owner carrier)
                                     (declare (ignore record owner carrier))
                                     (incf remaining)
                                     nil)
                                   right-history start end :namespace right-namespace)
               (unless (and complete (= cursor end))
                 (error "right lineage WAL did not reach ~d" end))
               files)))
         (right-stream nil)
         (right-timeline 0)
         (right-segment nil)
         (right-size 0))
    (unwind-protect
         (multiple-value-bind (cursor files complete)
             (wal-scan-lineage
              (lambda (record owner carrier raw)
                (declare (ignore carrier))
                (let ((lsn (wal-record-start record)))
                  (multiple-value-bind (segment offset) (wal-segment-and-offset lsn)
                    (unless (eql segment right-segment)
                      (when right-stream
                        (close right-stream)
                        (setf right-stream nil))
                      (loop while (and right-files (< (second (first right-files)) segment))
                            do (setf right-timeline (first (pop right-files))))
                      (let ((*pgdata* right-directory))
                        (multiple-value-setq (right-stream right-timeline right-size)
                          (wal-lineage-open-segment segment right-history right-timeline right-namespace)))
                      (unless (equal (pop right-files) (list right-timeline segment))
                        (error "shared lineage WAL selects different segments at ~d" lsn))
                      (setf right-segment segment
                            right-size (wal-lineage-effective-size right-size segment right-timeline right-history)))
                    (unless (and (= owner (timeline-at-point lsn right-history))
                                 (<= (+ offset (length raw)) right-size)
                                 (file-position right-stream offset))
                      (error "shared lineage WAL differs at ~d" lsn))
                    (let ((bytes (make-array (length raw) :element-type '(unsigned-byte 8))))
                      (unless (and (= (read-sequence bytes right-stream) (length raw))
                                   (equalp bytes raw))
                        (error "shared lineage WAL differs at ~d" lsn)))
                    (decf remaining)
                    nil)))
              left-history start end :namespace left-namespace :include-raw t)
           (declare (ignore files))
           (unless (and complete (= cursor end) (zerop remaining) (null right-files))
             (error "shared lineage WAL did not match through ~d" end))
           t)
      (when right-stream (close right-stream)))))
