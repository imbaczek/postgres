;;;; Allocation-free x86-64 Castagnoli CRC32C kernels.
(in-package #:pg)

#+x86-64
(defun %wal-crc32c-sse42-octets (bytes start end table constants seed)
  "Return the final CRC32C for a validated simple octet-vector slice.
SEED is a raw 32-bit running state. The caller establishes SSE4.2 support.
TABLE and CONSTANTS are unused. Valid calls allocate no heap storage."
  (declare (optimize (speed 3) (safety 0))
           (ignore table constants)
           (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type fixnum start end)
           (type (unsigned-byte 32) seed))
  (logxor (the (unsigned-byte 32) (%native-crc32c-serial-raw bytes start end seed))
          #xffffffff))

#+x86-64
(defun %wal-crc32c-pclmul-octets (bytes start end table constants seed)
  "Return final CRC32C using four parallel 128-bit polynomial folds.
The caller supplies a simple octet vector with 0 <= START <= END <= length,
a raw 32-bit SEED and the eight Castagnoli folding constants. SSE4.2 and
PCLMUL are both required. TABLE is unused. Loads do not check bounds.
Valid calls allocate no heap storage."
  (declare (optimize (speed 3) (safety 0))
           (ignore table)
           (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type (simple-array (unsigned-byte 64) (8)) constants)
           (type fixnum start end)
           (type (unsigned-byte 32) seed))
  (when (< (- end start) 64)
    (return-from %wal-crc32c-pclmul-octets
      (logxor (the (unsigned-byte 32)
                   (%native-crc32c-serial-raw bytes start end seed))
              #xffffffff)))
  (let* ((index (+ start 64))
         (multipliers (sb-simd-sse2:u64.2-aref constants 0))
         (mult4 (sb-simd-sse2:u64.2-aref constants 4))
         (mult2 (sb-simd-sse2:u64.2-aref constants 6))
         (state (sb-simd-sse2:u64.2-xor (%native-ieee-crc32-load bytes start)
                                        (sb-simd-sse2:u64.2! seed)))
         (state1 (%native-ieee-crc32-load bytes start 16))
         (state2 (%native-ieee-crc32-load bytes start 32))
         (state3 (%native-ieee-crc32-load bytes start 48)))
    (declare (type fixnum index)
             (type sb-simd-sse2:u64.2 state state1 state2 state3 multipliers mult4 mult2))
    ;; The existing load and fold helpers are polynomial-independent.
    (loop for cursor of-type sb-int:index from index below (- end 63) by 64
          do (setf state (%native-ieee-crc32-fold
                          state (%native-ieee-crc32-load bytes cursor) mult4)
                   state1 (%native-ieee-crc32-fold
                           state1 (%native-ieee-crc32-load bytes cursor 16) mult4)
                   state2 (%native-ieee-crc32-fold
                           state2 (%native-ieee-crc32-load bytes cursor 32) mult4)
                   state3 (%native-ieee-crc32-fold
                           state3 (%native-ieee-crc32-load bytes cursor 48) mult4))
          finally (setf index cursor))
    (setf state (%native-ieee-crc32-fold state state2 mult2)
          state1 (%native-ieee-crc32-fold state1 state3 mult2)
          state (%native-ieee-crc32-fold state state1 multipliers))
    (loop for cursor of-type sb-int:index from index below (- end 15) by 16
          do (setf state (%native-ieee-crc32-fold
                          state (%native-ieee-crc32-load bytes cursor) multipliers))
          finally (setf index cursor))
    ;; Hardware CRC32 reduces the last 128 bits and supplies the x^32 factor.
    (multiple-value-bind (low high) (sb-simd-sse2:u64.2-values state)
      (let ((crc (%native-crc32c-word (%native-crc32c-word 0 low) high)))
        (declare (type (unsigned-byte 32) crc))
        (logxor (the (unsigned-byte 32)
                     (%native-crc32c-serial-raw bytes index end crc))
                #xffffffff)))))
