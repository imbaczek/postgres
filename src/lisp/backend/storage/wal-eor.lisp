;;;; Native end-of-recovery payload; publication remains separate.
(in-package #:pg)

(defconstant +wal-end-of-recovery-payload-size+ 24)

(defconstant +wal-end-of-recovery-record-size+ (+ +wal-header-size+ +wal-end-of-recovery-payload-size+))

(defun wal-end-of-recovery-fields-valid-p (time timeline previous wal-level)
  (and (typep time '(signed-byte 64))
       (typep timeline '(integer 2 4294967295))
       (typep previous '(integer 1 4294967295)) (< previous timeline)
       (typep wal-level '(integer 0 2))))

(defun wal-encode-end-of-recovery-payload (time timeline previous wal-level)
  "Preserve xl_end_of_recovery's fields with canonical zero native tail bytes."
  (unless (wal-end-of-recovery-fields-valid-p time timeline previous wal-level)
    (error "invalid end-of-recovery fields"))
  (let ((bytes (make-array +wal-end-of-recovery-payload-size+ :element-type '(unsigned-byte 8) :initial-element 0)))
    (wal-set-i64 bytes 0 time)
    (wal-set-u32 bytes 8 timeline)
    (wal-set-u32 bytes 12 previous)
    (wal-set-u32 bytes 16 wal-level)
    bytes))

(defun wal-decode-end-of-recovery-payload (bytes)
  "Decode native fields; C's ABI padding itself is not a portable disk contract."
  (unless (and (= (length bytes) +wal-end-of-recovery-payload-size+) (zerop (wal-u32 bytes 20)))
    (error "invalid end-of-recovery payload"))
  (let ((time (wal-i64 bytes 0)) (timeline (wal-u32 bytes 8))
                                 (previous (wal-u32 bytes 12)) (wal-level (wal-u32 bytes 16)))
    (unless (wal-end-of-recovery-fields-valid-p time timeline previous wal-level)
      (error "invalid end-of-recovery fields"))
    (list :end-time time :timeline timeline :prev-timeline previous :wal-level wal-level)))

(defun wal-end-of-recovery-record-info (record)
  "Validate syntax only; history, minimum recovery and writer authority are separate."
  (unless (and (= (wal-record-type record) +wal-record-end-of-recovery+)
               (>= (wal-record-version record) 7)
               (zerop (wal-record-xid record))
               (typep (wal-record-start record) '(unsigned-byte 64))
               (typep (wal-record-end record) '(unsigned-byte 64))
               (= (- (wal-record-end record) (wal-record-start record)) +wal-end-of-recovery-record-size+)
               (<= (+ (mod (wal-record-start record) +wal-segment-size+) +wal-end-of-recovery-record-size+)
                   +wal-segment-size+))
    (error "invalid end-of-recovery record"))
  (wal-decode-end-of-recovery-payload (wal-record-payload record)))

(defun wal-encode-end-of-recovery-record (start time timeline previous wal-level)
  "Build a private native frame without appending, flushing or publishing a timeline."
  (unless (and (typep start '(unsigned-byte 64))
               (<= (+ start +wal-end-of-recovery-record-size+) #xffffffffffffffff)
               (<= (+ (mod start +wal-segment-size+) +wal-end-of-recovery-record-size+) +wal-segment-size+))
    (error "end-of-recovery record does not fit at the requested LSN"))
  (wal-encode-record +wal-record-end-of-recovery+ 0 start
                     (wal-encode-end-of-recovery-payload time timeline previous wal-level)))
