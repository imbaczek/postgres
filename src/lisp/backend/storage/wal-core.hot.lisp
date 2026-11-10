(in-package #:pg)

(declaim (inline %wal-crc32-octets))

(defun %wal-crc32-octets (bytes start end table &optional (seed #xffffffff))
  "Compute IEEE CRC-32 over BYTES from START inclusive to END exclusive.
The caller supplies a simple octet vector, valid fixnum bounds and the
256-entry unsigned 32-bit CRC table.  The inputs are read without mutation.
The polynomial is #xedb88320.  Valid calls allocate no heap storage."
  (declare (optimize (speed 3) (safety 3))
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
