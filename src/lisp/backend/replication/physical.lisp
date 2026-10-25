;;;; Physical WAL intake; transport positions are never durability acknowledgments.
(in-package #:pg)

(defstruct (physical-wal-decoder (:constructor %make-physical-wal-decoder))
  (next-lsn 0) (record-start 0) (previous-end 0)
  (buffer (make-array +wal-header-size+ :element-type '(unsigned-byte 8)))
  (used 0) (record-length nil) (failed-p nil))

(defun make-physical-wal-decoder (&key (start-lsn 0))
  "Start an intake stream at an exact record boundary supplied by its owner."
  (unless (typep start-lsn '(unsigned-byte 64))
    (error "invalid physical WAL start LSN ~s" start-lsn))
  (%make-physical-wal-decoder :next-lsn start-lsn :record-start start-lsn
                              :previous-end start-lsn))

(defun physical-wal-position-input (decoder start-lsn)
  "Require contiguous input, apart from the native WAL format's segment rotation."
  (let ((expected (physical-wal-decoder-next-lsn decoder)))
    (unless (and (typep start-lsn '(unsigned-byte 64))
                 (or (= start-lsn expected)
                     (and (zerop (physical-wal-decoder-used decoder))
                          (plusp (mod expected +wal-segment-size+))
                          (= start-lsn (* (1+ (floor expected +wal-segment-size+))
                                          +wal-segment-size+)))))
      (error "noncontiguous physical WAL: expected ~a, received ~a" expected start-lsn))
    (when (zerop (physical-wal-decoder-used decoder))
      (setf (physical-wal-decoder-record-start decoder) start-lsn
            (physical-wal-decoder-previous-end decoder) expected))
    (setf (physical-wal-decoder-next-lsn decoder) start-lsn)))

(defun physical-wal-read-header (decoder)
  "Validate bounded allocation and record coordinates before collecting a body."
  ;; C xlogreader.c validates record headers before allocating/reading a body.
  ;; This is the native pg-lisp record format, not C WAL binary compatibility.
  (let* ((header (physical-wal-decoder-buffer decoder))
         (start (physical-wal-decoder-record-start decoder))
         (previous (physical-wal-decoder-previous-end decoder))
         (length (wal-u32 header 8))
         (version (wal-u16 header 4)))
    (unless (and (equalp (subseq header 0 4) #(80 71 87 76))
                 (member version (list +wal-version-v2+ +wal-version-v3+
                                       +wal-version-v4+ +wal-version+))
                 (wal-valid-type-p (wal-u16 header 6) version)
                 (= (wal-u64 header 12) start)
                 (<= +wal-header-size+ length +wal-segment-size+)
                 (<= (+ (mod start +wal-segment-size+) length) +wal-segment-size+)
                 (<= (+ start length) #xffffffffffffffff)
                 (or (= previous start)
                     (> (+ (mod previous +wal-segment-size+) length)
                        +wal-segment-size+)))
      (error "invalid physical WAL header at LSN ~a" start))
    (let ((buffer (make-array length :element-type '(unsigned-byte 8))))
      (replace buffer header)
      (setf (physical-wal-decoder-buffer decoder) buffer
            (physical-wal-decoder-record-length decoder) length))))

(defun physical-wal-finish-record (decoder)
  "Return a complete CRC-validated record, without writing or acknowledging it."
  (let ((bytes (physical-wal-decoder-buffer decoder))
        (record (wal-decode-record (physical-wal-decoder-buffer decoder)
                                   (physical-wal-decoder-record-start decoder)))
        (next (physical-wal-decoder-next-lsn decoder)))
    (setf (physical-wal-decoder-used decoder) 0
          (physical-wal-decoder-record-length decoder) nil
          (physical-wal-decoder-record-start decoder) next
          (physical-wal-decoder-previous-end decoder) next
          (physical-wal-decoder-buffer decoder)
          (make-array +wal-header-size+ :element-type '(unsigned-byte 8)))
    (values record bytes)))

(defun physical-wal-decode-input (decoder start-lsn bytes &key on-record)
  "Collect arbitrarily fragmented native WAL and return only validated records.
NEXT-LSN is intake progress, never write/flush/replay progress. A decoder that
fails is unusable: reconnect must construct a new one at the owner's durable
record boundary. Each input chunk must end before a segment LSN gap; the next
chunk supplies the next segment's start. Segment jumps rely on an ordered,
trusted sender; length checks
cannot prove the absence of omitted records because native WAL has no prev-link."
  (when (physical-wal-decoder-failed-p decoder)
    (error "physical WAL decoder failed; reconnect at a durable boundary"))
  (handler-case
      (progn
        (unless (typep bytes '(vector (unsigned-byte 8)))
          (error "physical WAL input must be an octet vector"))
        (when (zerop (length bytes))
          (unless (= start-lsn (physical-wal-decoder-next-lsn decoder))
            (error "empty physical WAL input cannot advance its position"))
          (return-from physical-wal-decode-input nil))
        (physical-wal-position-input decoder start-lsn)
        (when (> (+ start-lsn (length bytes)) #xffffffffffffffff)
          (error "physical WAL input exceeds the uint64 LSN range"))
        (let ((offset 0) (records '()))
          (loop while (< offset (length bytes))
                do (let* ((used (physical-wal-decoder-used decoder))
                          (target (or (physical-wal-decoder-record-length decoder)
                                      +wal-header-size+))
                          (count (min (- target used) (- (length bytes) offset))))
                     (replace (physical-wal-decoder-buffer decoder) bytes
                              :start1 used :start2 offset :end2 (+ offset count))
                     (incf offset count)
                     (incf (physical-wal-decoder-used decoder) count)
                     (incf (physical-wal-decoder-next-lsn decoder) count)
                     (when (and (null (physical-wal-decoder-record-length decoder))
                                (= (physical-wal-decoder-used decoder) +wal-header-size+))
                       (physical-wal-read-header decoder))
                     (when (and (physical-wal-decoder-record-length decoder)
                                (= (physical-wal-decoder-used decoder)
                                   (physical-wal-decoder-record-length decoder)))
                       (multiple-value-bind (record raw-bytes)
                           (physical-wal-finish-record decoder)
                         (when on-record (funcall on-record record raw-bytes))
                         (push record records)))))
          (nreverse records)))
    (error (condition)
      (setf (physical-wal-decoder-failed-p decoder) t)
      (error condition))))
