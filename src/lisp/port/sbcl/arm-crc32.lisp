;;;; ARM64 IEEE and Castagnoli CRC instructions for SBCL.
;;;; The pinned backend has no CRC emitter; encode the architectural instruction
;;;; here rather than calling a foreign checksum function.
(in-package #:pg)

#+arm64
(sb-c:defknown %native-arm-crc32-word
    ((unsigned-byte 32) (unsigned-byte 64) (integer 0 7))
    (unsigned-byte 32) (sb-c:movable sb-c:flushable)
  :overwrite-fndb-silently t)

#+arm64
(sb-c:define-vop (%native-arm-crc32-word)
  (:translate %native-arm-crc32-word)
  (:args (crc :scs (sb-vm::unsigned-reg))
         (word :scs (sb-vm::unsigned-reg)))
  (:info selector)
  (:arg-types sb-vm::unsigned-num sb-vm::unsigned-num (:constant (integer 0 7)))
  (:results (result :scs (sb-vm::unsigned-reg)))
  (:result-types sb-vm::unsigned-num)
  (:generator 1
              (let ((encoding (logior #x1ac04000
                                      (if (= (logand selector 3) 3) #x80000000 0)
                                      (ash selector 10)
                                      (ash (sb-c:tn-offset word) 16)
                                      (ash (sb-c:tn-offset crc) 5)
                                      (sb-c:tn-offset result))))
                (dotimes (index 4)
                  (sb-assem:inst byte (ldb (byte 8 (* index 8)) encoding))))))

#+arm64
(defun %native-arm-crc32-word (crc word selector)
  "Update an uncomplemented state using a compile-time instruction selector.
Selectors 0..3 process 1, 2, 4 or 8 bytes for IEEE; 4..7 use Castagnoli.
The caller must establish CRC32 instruction support before entering the kernel."
  (declare (ignore crc word selector))
  (error "The ARM64 CRC intrinsic must be compiled."))
