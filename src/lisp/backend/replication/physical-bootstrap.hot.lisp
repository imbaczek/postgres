;;; Allocation-free writing of the native backup manifest envelope.
(in-package #:pg)

(defun %physical-bootstrap-write-manifest-envelope (payload crc output)
  "Write the manifest envelope into OUTPUT and return it.
PAYLOAD is a simple octet vector of UTF-8 bytes and CRC is their checksum.
OUTPUT must be a distinct simple octet vector of exactly
(+ (length PAYLOAD) 28) bytes.
Write the 18-byte magic, eight hexadecimal CRC digits, a newline, PAYLOAD,
and a final newline.  Valid calls allocate no heap memory."
  (declare (optimize (speed 3) (safety 3))
           (type (simple-array (unsigned-byte 8) (*)) payload output)
           (type (unsigned-byte 32) crc))
  (loop for index below 18
        do (setf (aref output index) (char-code (char "PGPHYSICALBACKUP1 " index))))
  (dotimes (offset 8)
    (let ((digit (ldb (byte 4 (* 4 (- 7 offset))) crc)))
      (setf (aref output (+ 18 offset)) (+ digit (if (< digit 10) 48 55)))))
  (setf (aref output 26) 10)
  (replace output payload :start1 27)
  (setf (aref output (+ 27 (length payload))) 10)
  output)
