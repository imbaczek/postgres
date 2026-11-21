;;;; Native WAL record framing, byte fields, and CRC32C.
(in-package #:pg)

(defconstant +wal-segment-size+ (* 16 1024 1024))

(defconstant +wal-header-size+ 32)

(defconstant +wal-record-pages+ 1)

(defconstant +wal-record-commit+ 2)

(defconstant +wal-record-catalog+ 3)

(defconstant +wal-record-checkpoint+ 4)

(defconstant +wal-record-clog-reset+ 5)

(defconstant +wal-record-batch-start+ 6)

(defconstant +wal-record-catalog-chunk+ 7)

(defconstant +wal-record-catalog-commit+ 8)

(defconstant +wal-record-maintenance+ 9)

(defconstant +wal-record-logical-changes+ 10)

(defconstant +wal-record-logical-catalog-event+ 11)

(defconstant +wal-record-xid-reservation+ 12)

(defconstant +wal-record-running-xacts+ 14)

(defconstant +wal-record-parameter-change+ 16
  "Native version 8 empty-batch terminal carrying a resource tuple.")

(defconstant +wal-record-full-page-writes-change+ 17)

(defconstant +wal-record-logical-decoding-status-change+ 18)

(defconstant +wal-record-relation-reset+ 15)

(defconstant +wal-record-end-of-recovery+ 13)

(defconstant +wal-version+ 8)

(declaim (ftype function wal-end-of-recovery-record-info))

(defstruct wal-record
  version type xid start end payload)

(defun wal-u16 (bytes offset)
  (logior (aref bytes offset)
          (ash (aref bytes (+ offset 1)) 8)))

(defun wal-u32 (bytes offset)
  (logior (aref bytes offset)
          (ash (aref bytes (+ offset 1)) 8)
          (ash (aref bytes (+ offset 2)) 16)
          (ash (aref bytes (+ offset 3)) 24)))

(defun wal-u64 (bytes offset)
  (logior (wal-u32 bytes offset)
          (ash (wal-u32 bytes (+ offset 4)) 32)))

(defun wal-i64 (bytes offset)
  (let ((raw (wal-u64 bytes offset)))
    (if (logbitp 63 raw) (- raw #x10000000000000000) raw)))

(defun wal-set-u16 (bytes offset value)
  (setf (aref bytes offset) (logand value #xFF)
        (aref bytes (+ offset 1)) (logand (ash value -8) #xFF))
  value)

(defun wal-set-u32 (bytes offset value)
  (setf (aref bytes offset) (logand value #xFF)
        (aref bytes (+ offset 1)) (logand (ash value -8) #xFF)
        (aref bytes (+ offset 2)) (logand (ash value -16) #xFF)
        (aref bytes (+ offset 3)) (logand (ash value -24) #xFF))
  value)

(defun wal-set-u64 (bytes offset value)
  (wal-set-u32 bytes offset (logand value #xFFFFFFFF))
  (wal-set-u32 bytes (+ offset 4) (logand (ash value -32) #xFFFFFFFF))
  value)

(defun wal-set-i64 (bytes offset value)
  (unless (typep value '(signed-byte 64)) (error "unrepresentable signed WAL integer"))
  (wal-set-u64 bytes offset (ldb (byte 64 0) value))
  value)

(defun make-wal-crc32-table ()
  (let ((table (make-array 256 :element-type '(unsigned-byte 32))))
    (dotimes (i 256 table)
      (let ((crc i))
        (loop repeat 8
              do (setf crc (if (oddp crc)
                               (logxor (ash crc -1) #x82F63B78)
                               (ash crc -1))))
        (setf (aref table i) (logand crc #xFFFFFFFF))))))

(defparameter *wal-crc32-table* (make-wal-crc32-table)
  "Native storage CRC32C table; reinitialized on reload when the format changes.")

#+(or x86-64 arm64)
(defparameter *wal-crc32c-reference-table* (make-wal-crc32-table))

(defun wal-crc32-typed-range (bytes start end table &optional (seed #xffffffff))
  "Return final CRC32C for a validated slice and raw SEED.
Canonical tables permit CPU-qualified native kernels; custom tables retain
their scalar recurrence. Inputs are read only."
  (declare (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type (simple-array (unsigned-byte 32) (256)) table)
           (type fixnum start end)
           (type (unsigned-byte 32) seed))
  #+x86-64
  (when (and *native-crc32c-sse42-available-p*
             (equalp table *wal-crc32c-reference-table*))
    (return-from wal-crc32-typed-range
      (if (and *native-crc32c-pclmul-available-p* (>= (- end start) 64))
          (%wal-crc32c-pclmul-octets bytes start end table
                                     *native-crc32c-constants* seed)
          (%wal-crc32c-sse42-octets bytes start end nil nil seed))))
  #+arm64
  (when (and *native-arm-crc32-available-p*
             (equalp table *wal-crc32c-reference-table*))
    (return-from wal-crc32-typed-range
      (%wal-crc32c-arm-octets bytes start end nil nil seed)))
  (%wal-crc32-octets bytes start end table seed))

(defun wal-crc32 (bytes &optional (start 0) (end (length bytes)))
  "Return final CRC32C (Castagnoli) of BYTES from START inclusive to END exclusive.
The initial raw state and final XOR are #xffffffff, as in PostgreSQL C.
Validated octet slices use available native instructions with a canonical table;
other inputs and modified tables use the scalar recurrence. BYTES is read only."
  (let ((table (and (boundp '*wal-crc32-table*) *wal-crc32-table*)))
    (if (and (typep bytes '(simple-array (unsigned-byte 8) (*)))
             (typep table '(simple-array (unsigned-byte 32) (256)))
             (typep start 'fixnum) (typep end 'fixnum)
             (<= 0 start end (length bytes)))
        (wal-crc32-typed-range bytes start end table)
        (let ((crc #xFFFFFFFF))
          (loop for i from start below end
                for slot = (logand (logxor crc (aref bytes i)) #xFF)
                do (setf crc (logxor (ash crc -8) (aref *wal-crc32-table* slot))))
          (logxor crc #xFFFFFFFF)))))

(defun wal-encode-record (type xid start payload)
  (let* ((length (+ +wal-header-size+ (length payload)))
         (record (make-array length :element-type '(unsigned-byte 8)
                                    :initial-element 0)))
    (setf (aref record 0) (char-code #\P)
          (aref record 1) (char-code #\G)
          (aref record 2) (char-code #\W)
          (aref record 3) (char-code #\L))
    (wal-set-u16 record 4 +wal-version+)
    (wal-set-u16 record 6 type)
    (wal-set-u32 record 8 length)
    (wal-set-u64 record 12 start)
    (wal-set-u64 record 20 xid)
    (replace record payload :start1 +wal-header-size+)
    (wal-set-u32 record 28 (wal-crc32 record))
    record))

(defun wal-valid-type-p (type)
  (member type
          (load-time-value
           (list +wal-record-pages+ +wal-record-commit+ +wal-record-catalog+
                 +wal-record-checkpoint+ +wal-record-clog-reset+ +wal-record-batch-start+
                 +wal-record-catalog-chunk+ +wal-record-catalog-commit+ +wal-record-maintenance+
                 +wal-record-logical-changes+ +wal-record-logical-catalog-event+
                 +wal-record-xid-reservation+ +wal-record-end-of-recovery+ +wal-record-running-xacts+
                 +wal-record-relation-reset+ +wal-record-parameter-change+
                 +wal-record-full-page-writes-change+ +wal-record-logical-decoding-status-change+)
           t)))

(defun wal-record-header-valid-p (bytes version expected-lsn)
  "Check native WAL header fields shared by complete and streaming readers.
The caller supplies a complete header. Record length and checksum checks are
specific to the reader's input boundary."
  (and (= (aref bytes 0) (char-code #\P))
       (= (aref bytes 1) (char-code #\G))
       (= (aref bytes 2) (char-code #\W))
       (= (aref bytes 3) (char-code #\L))
       (= version +wal-version+)
       (= (wal-u64 bytes 12) expected-lsn)
       (wal-valid-type-p (wal-u16 bytes 6))))

(defun wal-record-crc32 (bytes)
  "Return the final CRC of a WAL record with its checksum field treated as zero.
For a simple octet record of at least 32 bytes and a typed table, process bytes
0 through 27, invert that result back to raw state, apply four zero bytes, then
process bytes 32 onward.  This read-only path allocates no heap storage.
Other vectors or tables use the copy, zero and public CRC fallback, preserving
its behavior and allocation.  The record format uses CRC32C (Castagnoli)."
  (let ((table (and (boundp '*wal-crc32-table*) *wal-crc32-table*)))
    (when (and (typep bytes '(simple-array (unsigned-byte 8) (*)))
               (>= (length bytes) +wal-header-size+)
               (typep table '(simple-array (unsigned-byte 32) (256))))
      (let ((crc (logxor #xffffffff (%wal-crc32-octets bytes 0 28 table))))
        (declare (type (unsigned-byte 32) crc))
        (loop repeat 4
              do (setf crc (logxor (ash crc -8) (aref table (logand crc #xff)))))
        (return-from wal-record-crc32
          (wal-crc32-typed-range bytes +wal-header-size+ (length bytes) table crc)))))
  (let ((copy (copy-seq bytes)))
    (wal-set-u32 copy 28 0)
    (wal-crc32 copy)))

(defun wal-terminal-record-type-p (type)
  (member type (list +wal-record-commit+ +wal-record-catalog+
                     +wal-record-catalog-commit+ +wal-record-checkpoint+
                     +wal-record-maintenance+ +wal-record-clog-reset+ +wal-record-xid-reservation+
                     +wal-record-running-xacts+ +wal-record-parameter-change+
                     +wal-record-full-page-writes-change+ +wal-record-logical-decoding-status-change+)
          :test #'=))

(defun wal-decode-record (bytes expected-lsn)
  "Validate the record header and checksum at EXPECTED-LSN and decode it.
The returned record owns a separate payload subsequence of BYTES."
  (let ((version (and (>= (length bytes) +wal-header-size+)
                      (wal-u16 bytes 4))))
    (unless (and (>= (length bytes) +wal-header-size+)
                 (= (wal-u32 bytes 8) (length bytes))
                 (wal-record-header-valid-p bytes version expected-lsn))
      (error "invalid WAL record at LSN ~a" expected-lsn))
    (let ((saved-crc (wal-u32 bytes 28)))
      (unless (= saved-crc (wal-record-crc32 bytes))
        (error "WAL checksum mismatch at LSN ~a" expected-lsn)))
    (let* ((type (wal-u16 bytes 6))
           (xid (wal-u64 bytes 20))
           (payload (subseq bytes +wal-header-size+))
           (end (+ expected-lsn (length bytes))))
      (let ((record (make-wal-record :version version :type type :xid xid
                                     :start expected-lsn :end end :payload payload)))
        (when (= type +wal-record-end-of-recovery+)
          (wal-end-of-recovery-record-info record))
        record))))
