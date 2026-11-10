;;;; Experimental IEEE CRC folding, adapted from libdeflate's
;;;; lib/x86/crc32_pclmul_template.h (VL=16) and crc32_multipliers.h.
;;;; Copyright 2016 Eric Biggers
;;;;
;;;; Permission is hereby granted, free of charge, to any person obtaining a
;;;; copy of this software and associated documentation files (the "Software"),
;;;; to deal in the Software without restriction, including without limitation
;;;; the rights to use, copy, modify, merge, publish, distribute, sublicense,
;;;; and/or sell copies of the Software, and to permit persons to whom the
;;;; Software is furnished to do so, subject to the following conditions:
;;;;
;;;; The above copyright notice and this permission notice shall be included
;;;; in all copies or substantial portions of the Software.
;;;;
;;;; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
;;;; OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
;;;; FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
;;;; THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
;;;; LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
;;;; FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
;;;; DEALINGS IN THE SOFTWARE.
;;;;
(in-package #:pg)

#+x86-64
(defun %wal-crc32-ieee-octets (bytes start end table constants seed)
  (declare (optimize (speed 3) (safety 3))
           (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type (simple-array (unsigned-byte 32) (256)) table)
           (type fixnum start end)
           (type (simple-array (unsigned-byte 64) (8)) constants)
           (type (unsigned-byte 32) seed))
  (let ((crc seed) (index start))
    (declare (type (unsigned-byte 32) crc) (type fixnum index))
    (when (>= (- end start) 16)
      (let* ((multipliers (sb-simd-sse2:u64.2-aref constants 0))
             (barrett (sb-simd-sse2:u64.2-aref constants 2))
             (state (sb-simd-sse2:u64.2-xor
                     (sb-simd-sse2:u64.2! (sb-simd-sse2:u8.16-aref bytes index))
                     (sb-simd-sse2:u64.2! crc))))
        (declare (type sb-simd-sse2:u64.2 state multipliers barrett))
        (incf index 16)
        (when (>= (- end index) 48)
          (let ((state1 (sb-simd-sse2:u64.2! (sb-simd-sse2:u8.16-aref bytes index)))
                (state2 (sb-simd-sse2:u64.2! (sb-simd-sse2:u8.16-aref bytes (+ index 16))))
                (state3 (sb-simd-sse2:u64.2! (sb-simd-sse2:u8.16-aref bytes (+ index 32))))
                (mult4 (sb-simd-sse2:u64.2-aref constants 4))
                (mult2 (sb-simd-sse2:u64.2-aref constants 6)))
            (declare (type sb-simd-sse2:u64.2 state1 state2 state3 mult4 mult2))
            (incf index 48)
            (loop while (<= (+ index 64) end)
                  do (setf state (%native-ieee-crc32-fold state
                                                          (sb-simd-sse2:u64.2! (sb-simd-sse2:u8.16-aref bytes index))
                                                          mult4)
                           state1 (%native-ieee-crc32-fold state1
                                                           (sb-simd-sse2:u64.2! (sb-simd-sse2:u8.16-aref bytes (+ index 16)))
                                                           mult4)
                           state2 (%native-ieee-crc32-fold state2
                                                           (sb-simd-sse2:u64.2! (sb-simd-sse2:u8.16-aref bytes (+ index 32)))
                                                           mult4)
                           state3 (%native-ieee-crc32-fold state3
                                                           (sb-simd-sse2:u64.2! (sb-simd-sse2:u8.16-aref bytes (+ index 48)))
                                                           mult4))
                     (incf index 64))
            (setf state (%native-ieee-crc32-fold state state2 mult2)
                  state1 (%native-ieee-crc32-fold state1 state3 mult2)
                  state (%native-ieee-crc32-fold state state1 multipliers))))
        (loop while (<= (+ index 16) end)
              do (setf state
                       (%native-ieee-crc32-fold
                        state
                        (sb-simd-sse2:u64.2! (sb-simd-sse2:u8.16-aref bytes index))
                        multipliers))
                 (incf index 16))
        (setf state
              (sb-simd-sse2:u64.2-xor
               (%native-ieee-crc32-clmul state multipliers #x10)
               (sb-simd-sse2:u64.2!
                (nth-value 1 (sb-simd-sse2:u64.2-values state)))))
        (let* ((quotient (%native-ieee-crc32-clmul state barrett #x00))
               (reduced (sb-simd-sse2:u64.2-xor
                         state (%native-ieee-crc32-clmul quotient barrett #x10))))
          (setf crc (nth-value 2
                               (sb-simd-sse2:u32.4-values
                                (sb-simd-sse2:u32.4! reduced)))))))
    (loop while (< index end)
          for slot of-type (unsigned-byte 8) =
                                             (logand (logxor crc (aref bytes index)) #xff)
          do (setf crc (logxor (ash crc -8) (aref table slot)))
             (incf index))
    (logxor crc #xffffffff)))

