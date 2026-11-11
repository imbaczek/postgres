(in-package #:pg)

(declaim (inline %wal-crc32-octets))

(defun %wal-crc32-octets (bytes start end table &optional (seed #xffffffff))
  "Return the final inverted CRC for BYTES from START inclusive to END exclusive.
SEED is the raw 32-bit running state, defaulting to #xffffffff.  The caller
supplies a simple octet vector, valid fixnum bounds and a 256-entry unsigned
32-bit table; the recurrence uses its entries even if they are not Castagnoli.
Inputs are read only, and valid calls allocate no heap storage."
  (declare (optimize (speed 3) (safety 1))
           (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type (simple-array (unsigned-byte 32) (256)) table)
           (type fixnum start end)
           (type (unsigned-byte 32) seed))
  (let ((crc seed))
    (declare (type (unsigned-byte 32) crc))
    (loop for index fixnum from start below end
          for slot of-type (unsigned-byte 8) = (logand (logxor crc (aref bytes index)) #xff)
          do (setf crc (logxor (ash crc -8) (aref table slot))))
    (logxor crc #xffffffff)))
