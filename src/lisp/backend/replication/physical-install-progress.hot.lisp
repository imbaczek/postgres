;; Format expected installation progress lines in reusable scratch space.
;; The decoders compare bounded input spans with the resulting prefix.
(in-package #:pg)

(declaim (inline %physical-install-write-u32-hex %physical-install-write-step))

(defun %physical-install-write-u32-hex (value buffer start)
  "Write eight uppercase hexadecimal digits to BUFFER at START.
Return the first offset after the digits; BUFFER is caller-owned scratch."
  (declare (optimize (speed 3) (safety 3))
           (type (unsigned-byte 32) value)
           (type (simple-array (unsigned-byte 8) (32)) buffer)
           (type (integer 0 24) start))
  (dotimes (offset 8)
    (let ((digit (ldb (byte 4 (* 4 (- 7 offset))) value)))
      (setf (aref buffer (+ start offset)) (+ digit (if (< digit 10) 48 55)))))
  (+ start 8))

(defun %physical-install-write-step (step buffer start)
  "Write STEP in decimal to caller-owned BUFFER at START and return its length.
STEP must be between 1 and 200001 inclusive."
  (declare (optimize (speed 3) (safety 3))
           (type (integer 1 200001) step)
           (type (simple-array (unsigned-byte 8) (32)) buffer)
           (type (integer 0 26) start))
  (let ((divisor 1) (digits 1) (remaining step))
    (declare (type fixnum divisor remaining)
             (type (integer 1 6) digits))
    (loop while (>= step (* divisor 10))
          do (setf divisor (* divisor 10))
             (incf digits))
    (dotimes (offset digits)
      (multiple-value-bind (digit remainder) (truncate remaining divisor)
        (declare (type fixnum digit remainder))
        (setf (aref buffer (+ start offset)) (+ 48 digit)
              remaining remainder
              divisor (truncate divisor 10))))
    digits))

(defun %physical-install-write-progress (body-crc step buffer)
  "Write the canonical progress line into caller-owned 32-byte BUFFER.
Return its length; only that prefix is meaningful and the next call overwrites it.
STEP must be between 1 and 200001 inclusive.  Valid calls allocate no heap
storage.  The journal decoders validate the body checksum before generating progress lines.
The original journal writers remain independent format oracles."
  (declare (optimize (speed 3) (safety 3))
           (type (unsigned-byte 32) body-crc)
           (type (integer 1 200001) step)
           (type (simple-array (unsigned-byte 8) (32)) buffer))
  (%physical-install-write-u32-hex body-crc buffer 0)
  (setf (aref buffer 8) 32)
  (let* ((digits (%physical-install-write-step step buffer 9))
         (crc (wal-crc32 buffer 0 (+ 9 digits))))
    (setf (aref buffer 0) 83
          (aref buffer 1) 84
          (aref buffer 2) 69
          (aref buffer 3) 80
          (aref buffer 4) 32)
    (%physical-install-write-step step buffer 5)
    (setf (aref buffer (+ 5 digits)) 32)
    (%physical-install-write-u32-hex crc buffer (+ 6 digits))
    (setf (aref buffer (+ 14 digits)) 10)
    (+ 15 digits)))
