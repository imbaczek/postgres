(in-package #:pg)

(declaim (inline %physical-store-fnv1a64-parts))

(defun %physical-store-fnv1a64-parts (digest bytes)
  (declare (optimize (speed 3) (safety 3))
           (type (unsigned-byte 64) digest)
           (type (simple-array (unsigned-byte 8) (*)) bytes))
  (let ((value digest))
    (declare (type (unsigned-byte 64) value))
    (loop for index fixnum below (length bytes)
          do (setf value (logand #xffffffffffffffff
                                 (* (logxor value (aref bytes index)) #x100000001b3))))
    ;; The caller joins these fixnum halves into a full-width integer.
    (values (ldb (byte 32 0) value) (ldb (byte 32 32) value))))
