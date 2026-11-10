(in-package #:pg)

(declaim (inline %wal-crc32-octets))

(defun %wal-crc32-octets (bytes start end table)
  (declare (optimize (speed 3) (safety 3))
           (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type (simple-array (unsigned-byte 32) (256)) table)
           (type fixnum start end))
  (let ((crc #xffffffff))
    (declare (type (unsigned-byte 32) crc))
    (loop for index fixnum from start below end
          for slot of-type (unsigned-byte 8) = (logand (logxor crc (aref bytes index)) #xff)
          do (setf crc (logxor (ash crc -8) (aref table slot))))
    (logxor crc #xffffffff)))
