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

(defconstant +wal-record-catalog-transaction-commit+ 19
  "Commit one XID with a chunked catalog image and its deferred file deletions.")

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

(defun wal-crc32 (bytes &optional (start 0) (end (length bytes)) (seed #xffffffff))
  "Return final CRC32C (Castagnoli) of BYTES from START inclusive to END exclusive.
The final XOR is #xffffffff. SEED is the raw initial state before that XOR.
Validated octet slices use available native instructions with a canonical table;
other inputs and modified tables use the scalar recurrence. BYTES is read only."
  (declare (type (unsigned-byte 32) seed))
  (let ((table (and (boundp '*wal-crc32-table*) *wal-crc32-table*)))
    (if (and (typep bytes '(simple-array (unsigned-byte 8) (*)))
             (typep table '(simple-array (unsigned-byte 32) (256)))
             (typep start 'fixnum) (typep end 'fixnum)
             (<= 0 start end (length bytes)))
        (wal-crc32-typed-range bytes start end table seed)
        (let ((crc seed))
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
                 +wal-record-full-page-writes-change+ +wal-record-logical-decoding-status-change+
                 +wal-record-catalog-transaction-commit+)
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

(defconstant +wal-catalog-chunk-header-size+ 8)

(defun wal-encode-catalog-payload (bytes deletes)
  (let* ((payload (make-array (+ 8 (* 4 (length deletes)) (length bytes))
                              :element-type '(unsigned-byte 8)
                              :initial-element 0))
         (offset 0))
    (wal-set-u32 payload offset (length deletes))
    (incf offset 4)
    (dolist (relfilenode deletes)
      (wal-set-u32 payload offset relfilenode)
      (incf offset 4))
    (wal-set-u32 payload offset (length bytes))
    (incf offset 4)
    (replace payload bytes :start1 offset)
    payload))

(defun wal-decode-catalog-payload (payload &key borrow-image-p)
  "Validate PAYLOAD and return deletion IDs and the catalog image.
BORROW-IMAGE-P lets synchronous consumers read the immutable input without
copying the complete image."
  (when (< (length payload) 8)
    (error "WAL catalog record is too short"))
  (let* ((count (wal-u32 payload 0))
         (offset 4))
    (when (> (+ offset (* count 4) 4) (length payload))
      (error "WAL catalog record has an invalid delete list"))
    (let ((deletes '()))
      (loop repeat count
            do (push (wal-u32 payload offset) deletes)
               (incf offset 4))
      (setf deletes (nreverse deletes))
      (let ((catalog-length (wal-u32 payload offset)))
        (incf offset 4)
        (unless (= (+ offset catalog-length) (length payload))
          (error "WAL catalog record has an invalid catalog image length (offset ~a, image ~a, payload ~a)"
                 offset catalog-length (length payload)))
        (values deletes
                (if borrow-image-p
                    (make-array catalog-length :element-type '(unsigned-byte 8)
                                               :displaced-to payload :displaced-index-offset offset)
                    (subseq payload offset)))))))

(defun wal-encode-catalog-chunk-payload (bytes index count)
  (let ((payload (make-array (+ +wal-catalog-chunk-header-size+
                                (length bytes))
                             :element-type '(unsigned-byte 8)
                             :initial-element 0)))
    (wal-set-u32 payload 0 index)
    (wal-set-u32 payload 4 count)
    (replace payload bytes :start1 +wal-catalog-chunk-header-size+)
    payload))

(defun wal-decode-catalog-chunk-payload (payload &optional (copy-bytes-p t))
  (when (< (length payload) (1+ +wal-catalog-chunk-header-size+))
    (error "WAL catalog chunk is too short"))
  (let ((index (wal-u32 payload 0))
        (count (wal-u32 payload 4)))
    (unless (and (plusp count) (< index count))
      (error "WAL catalog chunk has an invalid index or count"))
    (values index count
            (when copy-bytes-p (subseq payload +wal-catalog-chunk-header-size+)))))

(defun wal-catalog-chunk-count (payload)
  (let* ((chunk-size (- +wal-segment-size+ +wal-header-size+
                        +wal-catalog-chunk-header-size+))
         (count (ceiling (length payload) chunk-size)))
    (unless (<= count #xFFFFFFFF)
      (error "WAL catalog image requires too many chunks"))
    count))

(defun wal-encode-catalog-commit-payload (payload chunk-count)
  (let ((commit (make-array 12 :element-type '(unsigned-byte 8)
                               :initial-element 0)))
    (wal-set-u32 commit 0 chunk-count)
    (wal-set-u64 commit 4 (length payload))
    commit))

(defun wal-decode-catalog-commit-payload (payload)
  (unless (= (length payload) 12)
    (error "WAL catalog commit record has an invalid length"))
  (let ((count (wal-u32 payload 0))
        (length (wal-u64 payload 4)))
    (unless (plusp count)
      (error "WAL catalog commit record has an invalid chunk count"))
    (values count length)))

(defun wal-encode-commit-timestamp (timestamp)
  (unless (and (integerp timestamp) (<= (- (ash 1 63)) timestamp (1- (ash 1 63))))
    (error "invalid WAL commit timestamp"))
  (let ((payload (make-array 8 :element-type '(unsigned-byte 8))))
    (dotimes (index 8 payload)
      (setf (aref payload index) (ldb (byte 8 (* index 8)) timestamp)))))

(defun wal-decode-commit-timestamp (payload)
  ;; Earlier commit records have an empty payload. An eight-byte payload adds
  ;; signed microseconds without changing the record header or row-change codec.
  (case (length payload)
    (0 nil)
    (8 (let ((value (loop for index below 8
                          sum (ash (aref payload index) (* index 8)))))
         (if (logbitp 63 value) (- value (ash 1 64)) value)))
    (otherwise (error "WAL commit record has an invalid payload"))))

(defun wal-encode-catalog-transaction-commit (timestamp catalog-payload)
  "Encode the XID terminal payload after WAL catalog chunks have been staged."
  (let ((count (wal-catalog-chunk-count catalog-payload))
        (bytes (make-array 20 :element-type '(unsigned-byte 8))))
    (unless (plusp count) (error "WAL catalog transaction requires a catalog image"))
    (replace bytes (wal-encode-commit-timestamp timestamp))
    (wal-set-u32 bytes 8 count)
    (wal-set-u64 bytes 12 (length catalog-payload))
    bytes))

(defun wal-decode-catalog-transaction-commit (payload)
  "Return catalog chunk count, catalog byte length, and signed commit timestamp."
  (unless (= (length payload) 20)
    (error "WAL catalog transaction commit has an invalid length"))
  (let ((count (wal-u32 payload 8))
        (size (wal-u64 payload 12)))
    (unless (and (plusp count) (plusp size))
      (error "WAL catalog transaction commit has an invalid image size or chunk count"))
    (values count size (wal-decode-commit-timestamp (subseq payload 0 8)))))

(defun wal-normal-transaction-xid-p (xid)
  (and (typep xid '(unsigned-byte 64))
       (>= (ldb (byte 32 0) xid) 3)))

(defun wal-transaction-terminal-type-p (type)
  (or (= type +wal-record-commit+) (= type +wal-record-catalog-transaction-commit+)))

(defun wal-catalog-terminal-type-p (type)
  (or (= type +wal-record-catalog+)
      (= type +wal-record-catalog-commit+)
      (= type +wal-record-catalog-transaction-commit+)))

(defun wal-record-commit-timestamp (record)
  "Read a transaction terminal's timestamp, including the legacy empty payload."
  (let ((payload (wal-record-payload record)))
    (if (= (wal-record-type record) +wal-record-catalog-transaction-commit+)
        (nth-value 2 (wal-decode-catalog-transaction-commit payload))
        (wal-decode-commit-timestamp payload))))

(defstruct wal-catalog-chunk-prefix
  count (next 0) (bytes 0))

(defun wal-note-catalog-chunk-prefix (record prefix)
  "Check a catalog chunk header without retaining or copying the catalog bytes."
  (unless (eql 0 (wal-record-xid record))
    (error "WAL catalog chunks must not carry an XID"))
  (let ((payload (wal-record-payload record)))
    (multiple-value-bind (index count) (wal-decode-catalog-chunk-payload payload nil)
      (let ((state (or prefix (make-wal-catalog-chunk-prefix :count count))))
        (unless (and (= count (wal-catalog-chunk-prefix-count state))
                     (= index (wal-catalog-chunk-prefix-next state)))
          (error "invalid WAL catalog chunk sequence"))
        (incf (wal-catalog-chunk-prefix-next state))
        (incf (wal-catalog-chunk-prefix-bytes state) (- (length payload) +wal-catalog-chunk-header-size+))
        state))))

(defun wal-check-catalog-chunk-prefix (record prefix)
  "Require a terminal to certify the complete catalog chunk prefix."
  (let ((type (wal-record-type record))
        (payload (wal-record-payload record)))
    (multiple-value-bind (count length)
        (cond ((= type +wal-record-catalog-commit+)
               (wal-decode-catalog-commit-payload payload))
              ((= type +wal-record-catalog-transaction-commit+)
               (unless (wal-normal-transaction-xid-p (wal-record-xid record))
                 (error "invalid WAL catalog transaction XID"))
               (wal-decode-catalog-transaction-commit payload)))
      (if count
          (unless (and prefix (= count (wal-catalog-chunk-prefix-count prefix)
                                 (wal-catalog-chunk-prefix-next prefix))
                       (= length (wal-catalog-chunk-prefix-bytes prefix)))
            (error "incomplete WAL catalog chunk prefix"))
          (when prefix (error "WAL catalog chunks lack a catalog commit record")))))
  (values))

(defun wal-terminal-record-type-p (type)
  (member type (list +wal-record-commit+ +wal-record-catalog+
                     +wal-record-catalog-commit+ +wal-record-catalog-transaction-commit+ +wal-record-checkpoint+
                     +wal-record-maintenance+ +wal-record-clog-reset+ +wal-record-xid-reservation+
                     +wal-record-running-xacts+ +wal-record-parameter-change+
                     +wal-record-full-page-writes-change+ +wal-record-logical-decoding-status-change+)
          :test #'=))

(defun wal-decode-record (bytes expected-lsn &key borrow-payload-p)
  "Validate the record header and checksum at EXPECTED-LSN and decode it.
The payload owns its bytes by default. With BORROW-PAYLOAD-P, callers must
keep the octet input unchanged for the lifetime of the read-only payload."
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
           (payload (if borrow-payload-p
                        (make-array (- (length bytes) +wal-header-size+)
                                    :element-type '(unsigned-byte 8)
                                    :displaced-to bytes :displaced-index-offset +wal-header-size+)
                        (subseq bytes +wal-header-size+)))
           (end (+ expected-lsn (length bytes))))
      (let ((record (make-wal-record :version version :type type :xid xid
                                     :start expected-lsn :end end :payload payload)))
        (when (= type +wal-record-end-of-recovery+)
          (wal-end-of-recovery-record-info record))
        record))))
