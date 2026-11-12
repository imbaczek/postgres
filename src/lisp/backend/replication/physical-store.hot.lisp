(in-package #:pg)

(declaim (inline %physical-store-fnv1a64-parts %physical-store-fnv1a64-span-parts))

(defun %physical-store-fnv1a64-span-parts (digest bytes start end)
  "Extend DIGEST over the half-open span [START, END) without changing BYTES.
The caller supplies an unsigned 64-bit digest, a simple octet vector and
bounded fixnum indices. FNV-1a arithmetic is modulo 2^64 and is not
cryptographic. Return two 32-bit halves to keep the kernel allocation-free."
  (declare (optimize (speed 3) (safety 3))
           (type (unsigned-byte 64) digest)
           (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type fixnum start end))
  (let ((value digest))
    (declare (type (unsigned-byte 64) value))
    (loop for index fixnum from start below end
          do (setf value (logand #xffffffffffffffff
                                 (* (logxor value (aref bytes index)) #x100000001b3))))
    (values (ldb (byte 32 0) value) (ldb (byte 32 32) value))))

(defun %physical-store-fnv1a64-parts (digest bytes)
  "Extend DIGEST with BYTES using FNV-1a arithmetic modulo 2^64.
The caller supplies an unsigned 64-bit digest and a simple octet vector.
BYTES is read without mutation.  Return the low and high 32-bit halves
as separate values so the full-width result need not be boxed here.
Valid calls allocate no heap storage."
  (declare (optimize (speed 3) (safety 3))
           (type (unsigned-byte 64) digest)
           (type (simple-array (unsigned-byte 8) (*)) bytes))
  (%physical-store-fnv1a64-span-parts digest bytes 0 (length bytes)))
