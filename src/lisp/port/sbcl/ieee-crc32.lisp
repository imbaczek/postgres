;;;; Native IEEE CRC-32 support for x86-64 SBCL.
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

(eval-when (:compile-toplevel :load-toplevel :execute)
  (require :sb-simd))

(defvar *native-ieee-crc32-available-p* nil
  "Cached raw CPUID leaf 1 ECX bit 1: whether this CPU supports PCLMUL.")

(defparameter *native-ieee-crc32-constants*
  (make-array 8 :element-type '(unsigned-byte 64)
                :initial-contents '(#xae689191 #xccaa009e
                                    #xb4e5b025f7011641 #x1db710641
                                    #x8f352d95 #x1d9513d7
                                    #xf1da05aa #x81256527))
  "Preallocated IEEE folding coefficients, read only by the native kernel.
Successive pairs fold 16 bytes, perform Barrett reduction, fold 64 bytes,
and fold 32 bytes.  The constants and folding algebra derive from libdeflate.")

(defun refresh-native-ieee-crc32-support ()
  "Refresh the PCLMUL flag from raw CPUID on load and saved-image startup.
Direct CPUID makes this independent of SB-SIMD's initialization hook order.
The stable symbol in SB-EXT:*INIT-HOOKS* resolves the latest function after a
reload and permits idempotent registration."
  (setf *native-ieee-crc32-available-p*
        (logbitp 1 (nth-value 2 (sb-vm::%cpu-identification 1 0)))))

(eval-when (:load-toplevel :execute)
  (refresh-native-ieee-crc32-support)
  (pushnew 'refresh-native-ieee-crc32-support sb-ext:*init-hooks*))

(sb-c:defknown %native-ieee-crc32-clmul
    (sb-simd-sse2:u64.2 sb-simd-sse2:u64.2 (unsigned-byte 8))
    sb-simd-sse2:u64.2
    (sb-c:movable sb-c:flushable)
  :overwrite-fndb-silently t)

(sb-c:define-vop (%native-ieee-crc32-clmul)
  (:translate %native-ieee-crc32-clmul)
  (:args (left :scs (sb-vm::int-sse-reg) :target result)
         (right :scs (sb-vm::int-sse-reg)))
  (:info selector)
  (:arg-types sb-vm::simd-pack-ub64 sb-vm::simd-pack-ub64
              (:constant (unsigned-byte 8)))
  (:temporary (:sc sb-vm::int-sse-reg) scratch)
  (:results (result :scs (sb-vm::int-sse-reg)))
  (:result-types sb-vm::simd-pack-ub64)
  (:generator 4
              (cond ((sb-c:location= left result)
                     (sb-assem:inst sb-x86-64-asm::pclmulqdq result right selector))
                    ((not (sb-c:location= right result))
                     (sb-vm::move result left)
                     (sb-assem:inst sb-x86-64-asm::pclmulqdq result right selector))
                    (t
                     (sb-vm::move scratch left)
                     (sb-assem:inst sb-x86-64-asm::pclmulqdq scratch right selector)
                     (sb-vm::move result scratch)))))

(defun %native-ieee-crc32-clmul (left right selector)
  "Carry-less multiply selected 64-bit lanes using the native VOP.
The caller must have PCLMUL support and SELECTOR must be a compile-time
constant.  The Lisp body rejects calls that bypass the VOP."
  (declare (ignore left right selector))
  (error "The IEEE CRC-32 intrinsic must be compiled."))

(defmacro %native-ieee-crc32-fold (state next multipliers)
  "Fold STATE into NEXT using MULTIPLIERS, evaluating each argument once.
The expansion keeps SIMD packs unboxed within the caller; a standalone
pack-returning function would box its result."
  (let ((current (gensym "STATE")) (following (gensym "NEXT")) (factors (gensym "FACTORS")))
    `(let ((,current ,state) (,following ,next) (,factors ,multipliers))
       (declare (type sb-simd-sse2:u64.2 ,current ,following ,factors))
       (sb-simd-sse2:u64.2-xor
        (%native-ieee-crc32-clmul ,current ,factors #x00)
        (%native-ieee-crc32-clmul ,current ,factors #x11)
        ,following))))
