;;;; Native x86-64 Castagnoli CRC32C support.
;;;; The scalar instruction sequence follows src/port/pg_crc32c_sse42.c.
(in-package #:pg)

(defvar *native-crc32c-sse42-available-p* nil
  "Cached SSE4.2 support for serial CRC32C instructions.")

(defvar *native-crc32c-pclmul-available-p* nil
  "Cached support for the CRC32C folding kernel: SSE4.2 and PCLMUL.")

(defparameter *native-crc32c-constants*
  (make-array 8 :element-type '(unsigned-byte 64)
                :initial-contents '(#xf20c0dfe #x493c7d27 0 0
                                    #x740eef02 #x9e4addf8 #x3da6d0cb #xba4fc28e))
  "Castagnoli residues for folding 16, 64 and 32 bytes.
The two unused slots retain the constant layout used by the IEEE helpers.
The residues agree with PostgreSQL's vector CRC32C implementation.")

(defun refresh-native-crc32c-support ()
  "Refresh optional instruction support on load and saved-image startup.
The folding kernel needs SSE4.2 for its final reduction as well as PCLMUL.
Clear both caches first so an unsuccessful probe cannot retain old host state."
  (setf *native-crc32c-sse42-available-p* nil
        *native-crc32c-pclmul-available-p* nil)
  (handler-case
      (let* ((ecx (nth-value 2 (sb-vm::%cpu-identification 1 0)))
             (sse42 (logbitp 20 ecx)))
        (setf *native-crc32c-sse42-available-p* sse42
              *native-crc32c-pclmul-available-p* (and sse42 (logbitp 1 ecx))))
    (error () nil))
  (values *native-crc32c-sse42-available-p* *native-crc32c-pclmul-available-p*))

(eval-when (:load-toplevel :execute)
  (refresh-native-crc32c-support)
  (pushnew 'refresh-native-crc32c-support sb-ext:*init-hooks*))

(defmacro %native-crc32c-serial-raw (bytes start end seed)
  "Update an uncomplemented CRC32C state using SSE4.2.
The caller supplies a simple octet vector, valid nonnegative fixnum bounds,
a 32-bit raw seed and an SSE4.2 CPU. Loads are unchecked. The descriptor
remains live throughout the VOP, and the result cannot alias its inputs."
  `(sb-c::inline-vop
       (((array sb-vm::descriptor-reg) ,bytes)
        ((start sb-vm::unsigned-reg sb-vm::unsigned-num) ,start)
        ((end sb-vm::unsigned-reg sb-vm::unsigned-num) ,end)
        ((seed sb-vm::unsigned-reg sb-vm::unsigned-num) ,seed)
        ((cursor sb-vm::unsigned-reg))
        ((limit sb-vm::signed-reg)))
       ((result sb-vm::unsigned-reg sb-vm::unsigned-num :from :load))
     (sb-vm::move cursor start)
     (sb-vm::move result seed)
     (sb-assem:inst sb-x86-64-asm::lea limit (sb-x86-64-asm::ea -7 end))
     (let ((words (sb-assem:gen-label))
           (word-test (sb-assem:gen-label))
           (bytes (sb-assem:gen-label))
           (byte-test (sb-assem:gen-label))
           (skip-dword (sb-assem:gen-label)))
       (sb-assem:inst sb-x86-64-asm::jmp word-test)
       (sb-assem:emit-label words)
       (sb-assem:inst sb-x86-64-asm::crc32 :qword result
                      (sb-x86-64-asm::ea 1 array cursor))
       (sb-assem:inst sb-x86-64-asm::add cursor 8)
       (sb-assem:emit-label word-test)
       ;; Signed limits preserve the empty and short-slice cases.
       (sb-assem:inst sb-x86-64-asm::cmp cursor limit)
       (sb-assem:inst sb-x86-64-asm::jmp :l words)
       (sb-assem:inst sb-x86-64-asm::lea limit (sb-x86-64-asm::ea -3 end))
       (sb-assem:inst sb-x86-64-asm::cmp cursor limit)
       (sb-assem:inst sb-x86-64-asm::jmp :ge skip-dword)
       (sb-assem:inst sb-x86-64-asm::crc32 :dword
                      (sb-x86-64-asm::get-gpr :dword (sb-c:tn-offset result))
                      (sb-x86-64-asm::ea 1 array cursor))
       (sb-assem:inst sb-x86-64-asm::add cursor 4)
       (sb-assem:emit-label skip-dword)
       (sb-assem:inst sb-x86-64-asm::jmp byte-test)
       (sb-assem:emit-label bytes)
       (sb-assem:inst sb-x86-64-asm::crc32 :byte result
                      (sb-x86-64-asm::ea 1 array cursor))
       (sb-assem:inst sb-x86-64-asm::inc cursor)
       (sb-assem:emit-label byte-test)
       (sb-assem:inst sb-x86-64-asm::cmp cursor end)
       (sb-assem:inst sb-x86-64-asm::jmp :b bytes))))

(sb-c:defknown %native-crc32c-word ((unsigned-byte 32) (unsigned-byte 64))
    (unsigned-byte 32) (sb-c:movable sb-c:flushable)
  :overwrite-fndb-silently t)

(sb-c:define-vop (%native-crc32c-word)
  (:translate %native-crc32c-word)
  (:args (crc :scs (sb-vm::unsigned-reg) :target result)
         (word :scs (sb-vm::unsigned-reg)))
  (:arg-types sb-vm::unsigned-num sb-vm::unsigned-num)
  (:temporary (:sc sb-vm::unsigned-reg) scratch)
  (:results (result :scs (sb-vm::unsigned-reg)))
  (:result-types sb-vm::unsigned-num)
  (:generator 2
              (cond ((sb-c:location= crc result)
                     (sb-assem:inst sb-x86-64-asm::crc32 :qword result word))
                    ((not (sb-c:location= word result))
                     (sb-vm::move result crc)
                     (sb-assem:inst sb-x86-64-asm::crc32 :qword result word))
                    (t
                     ;; Preserve WORD before moving CRC into an aliased result register.
                     (sb-vm::move scratch word)
                     (sb-vm::move result crc)
                     (sb-assem:inst sb-x86-64-asm::crc32 :qword result scratch)))))

(defun %native-crc32c-word (crc word)
  "Update a raw CRC32C state with one 64-bit word.
The caller establishes SSE4.2 support. Compiled calls use the intrinsic."
  (declare (ignore crc word))
  (error "The CRC32C word intrinsic must be compiled."))
