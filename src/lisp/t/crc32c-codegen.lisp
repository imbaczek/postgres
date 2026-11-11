;;;; The preceding reference to t/crc32-codegen.lisp is obsolete: load pg only.

;;;; CRC32C experiment; not loaded by pg and not a WAL format change.
;;;; Load pg and t/crc32-codegen.lisp, then this file.
;;;; (pg::crc32c-codegen-run #p"/path/to/ram-backed/output/")
(in-package #:pg)

(defparameter *crc32c-codegen-ieee-table*
  (let ((table (make-array 256 :element-type '(unsigned-byte 32))))
    (dotimes (index 256 table)
      (let ((crc index))
        (dotimes (bit 8)
          (setf crc (logxor (ash crc -1) (if (oddp crc) #xedb88320 0))))
        (setf (aref table index) crc))))
  "Independent IEEE table for comparison with CRC32C.")

(defvar *crc32c-codegen-constants* nil)

(defparameter *crc32c-codegen-table*
  (let ((table (make-array 256 :element-type '(unsigned-byte 32))))
    (dotimes (index 256 table)
      (let ((crc index))
        (dotimes (bit 8)
          (setf crc (logxor (ash crc -1) (if (oddp crc) #x82f63b78 0))))
        (setf (aref table index) crc)))))

(defun crc32c-codegen-scalar (bytes start end table constants seed)
  "Compute Castagnoli CRC32C in scalar Lisp, requiring no optional CPU instructions."
  (declare (ignore table constants))
  (%wal-crc32-octets bytes start end *crc32c-codegen-table* seed))

(defun crc32c-codegen-ieee-scalar (bytes start end table constants seed)
  "Adapt the scalar IEEE kernel to the benchmark's six-argument convention."
  (declare (ignore constants))
  (%wal-crc32-octets bytes start end table seed))

(defun crc32c-codegen-sse42-p ()
  "Return SSE4.2 support without assuming that PCLMUL implies it."
  #+x86-64 (logbitp 20 (nth-value 2 (sb-vm::%cpu-identification 1 0)))
  #-x86-64 nil)

#+x86-64
(defmacro crc32c-codegen-serial-raw (bytes start end seed)
  "Emit PostgreSQL's SSE4.2 CRC32C recurrence without a foreign call.
The caller must provide a simple octet vector, 0 <= START <= END <= length,
a 32-bit raw SEED and a CPU with SSE4.2. Loads are unchecked. Return an
uncomplemented state. This experiment uses SBCL's internal INLINE-VOP interface."
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
       ;; END may be less than seven; use a signed comparison with END - 7.
       (sb-assem:inst sb-x86-64-asm::cmp cursor limit)
       (sb-assem:inst sb-x86-64-asm::jmp :l words)
       (sb-assem:inst sb-x86-64-asm::lea limit (sb-x86-64-asm::ea -3 end))
       (sb-assem:inst sb-x86-64-asm::cmp cursor limit)
       (sb-assem:inst sb-x86-64-asm::jmp :ge skip-dword)
       (sb-assem:inst sb-x86-64-asm::crc32 :dword (sb-x86-64-asm::get-gpr :dword (sb-c:tn-offset result))
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

#+x86-64
(sb-c:defknown crc32c-codegen-word ((unsigned-byte 32) (unsigned-byte 64))
    (unsigned-byte 32) (sb-c:movable sb-c:flushable)
  :overwrite-fndb-silently t)

#+x86-64
(sb-c:define-vop (crc32c-codegen-word)
  (:translate crc32c-codegen-word)
  (:args (crc :scs (sb-vm::unsigned-reg) :target result)
         (word :scs (sb-vm::unsigned-reg)))
  (:arg-types sb-vm::unsigned-num sb-vm::unsigned-num)
  (:temporary (:sc sb-vm::unsigned-reg) scratch)
  (:results (result :scs (sb-vm::unsigned-reg)))
  (:result-types sb-vm::unsigned-num)
  (:generator 2
              ;; WORD may share RESULT. Preserve it before moving CRC in that case.
              (cond ((sb-c:location= crc result)
                     (sb-assem:inst sb-x86-64-asm::crc32 :qword result word))
                    ((not (sb-c:location= word result))
                     (sb-vm::move result crc)
                     (sb-assem:inst sb-x86-64-asm::crc32 :qword result word))
                    (t
                     (sb-vm::move scratch word)
                     (sb-vm::move result crc)
                     (sb-assem:inst sb-x86-64-asm::crc32 :qword result scratch)))))

#+x86-64
(defun crc32c-codegen-word (crc word)
  (declare (ignore crc word))
  (error "The CRC32C word intrinsic must be compiled."))

#+x86-64
(defparameter *crc32c-codegen-constants*
  (make-array 8 :element-type '(unsigned-byte 64)
                :initial-contents '(#xf20c0dfe #x493c7d27 0 0
                                    #x740eef02 #x9e4addf8 #x3da6d0cb #xba4fc28e))
  "Castagnoli folding coefficients for 16, 64 and 32 bytes.
These are the same residues used by PostgreSQL's AVX-512 CRC32C code.
The two unused slots preserve the IEEE kernel's constant-array layout.")

#+x86-64
(defun crc32c-codegen-sse (bytes start end table constants seed)
  "Compute final CRC32C using the serial SSE4.2 recurrence.
The caller must validate the simple octet vector slice and 32-bit raw seed,
and establish SSE4.2 support before calling."
  (declare (optimize (speed 3) (safety 0))
           (ignore table constants)
           (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type fixnum start end)
           (type (unsigned-byte 32) seed))
  (logxor (the (unsigned-byte 32) (crc32c-codegen-serial-raw bytes start end seed))
          #xffffffff))

#+x86-64
(defun crc32c-codegen-fold (bytes start end table constants seed)
  "Compute final CRC32C using four 128-bit PCLMUL streams and CRC32 reduction.
The caller must validate the simple octet vector slice, 32-bit raw seed and
folding constants; loads are unchecked. TABLE is unused. SSE4.2 and PCLMUL
are required. The 64-byte cutoff is an experimental crossover assumption,
not a CPU-specific dispatch decision."
  (declare (optimize (speed 3) (safety 0))
           (ignore table)
           (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type (simple-array (unsigned-byte 64) (8)) constants)
           (type fixnum start end)
           (type (unsigned-byte 32) seed))
  (when (< (- end start) 64)
    (return-from crc32c-codegen-fold
      (logxor (the (unsigned-byte 32)
                   (crc32c-codegen-serial-raw bytes start end seed))
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
    ;; The helpers implement polynomial arithmetic, not an IEEE-only operation;
    ;; the Castagnoli coefficients determine which checksum this loop computes.
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
    (multiple-value-bind (low high) (sb-simd-sse2:u64.2-values state)
      (let ((crc (crc32c-codegen-word (crc32c-codegen-word 0 low) high)))
        (declare (type (unsigned-byte 32) crc))
        (logxor (the (unsigned-byte 32)
                     (crc32c-codegen-serial-raw bytes index end crc))
                #xffffffff)))))

(defun crc32c-codegen-engines (&optional force-scalar)
  "Select only kernels whose optional instructions are available.
Always include scalar CRC32C. FORCE-SCALAR suppresses all native kernels,
including IEEE, so fallback correctness can be checked on an accelerated CPU."
  (declare (ignorable force-scalar))
  (let ((ieee (list "ieee-scalar" #'crc32c-codegen-ieee-scalar *crc32c-codegen-ieee-table* nil))
        (native nil))
    #+x86-64
    (unless force-scalar
      (when *native-ieee-crc32-available-p*
        (setf ieee (list "ieee-pclmul" #'%wal-crc32-ieee-octets
                         *crc32c-codegen-ieee-table* *native-ieee-crc32-constants*)))
      (when (crc32c-codegen-sse42-p)
        (push (list "crc32c-sse42" #'crc32c-codegen-sse nil *crc32c-codegen-constants*) native)
        (when *native-ieee-crc32-available-p*
          (push (list "crc32c-pclmul" #'crc32c-codegen-fold nil *crc32c-codegen-constants*)
                native))))
    #+arm64
    (unless force-scalar
      (when *native-arm-crc32-available-p*
        (setf ieee (list "ieee-arm" #'%wal-crc32-ieee-arm-octets *crc32c-codegen-ieee-table* nil))
        (push (list "crc32c-arm" #'%wal-crc32c-arm-octets nil nil) native)))
    (append (list ieee (list "crc32c-scalar" #'crc32c-codegen-scalar nil nil))
            (nreverse native))))

#+sbcl

(defun crc32c-codegen-reference (bytes start end seed)
  "Compute Castagnoli CRC bit by bit, independently of the intrinsics."
  (let ((crc seed))
    (loop for index from start below
          end
          do (setf crc (logxor crc (aref bytes index))) (dotimes (bit 8)
                                                          (setf crc
                                                                (logxor
                                                                 (ash crc -1)
                                                                 (if (oddp
                                                                      crc)
                                                                     2197175160
                                                                     0)))))
    (logxor crc 4294967295)))

#+sbcl

(defun crc32c-codegen-check
    (
     &optional
     (functions (mapcar #'second (rest (crc32c-codegen-engines)))))
  "Compare selected kernels with a bitwise oracle across seeds and exact-ended slices."
  (let ((checks 0) (constants (copy-seq *crc32c-codegen-constants*)))
    (dolist (fn functions)
      (assert
       (= 3808858755
          (funcall fn
                   (make-array 9 :element-type '(unsigned-byte 8)
                                 :initial-contents '(49 50 51 52 53 54 55 56 57))
                   0 9 nil constants 4294967295))))
    (dolist
        (size
         (append
          (loop for size from 0 to 512
                collect size)
          '(1023 1024 1025 8191 8192 8193 65535 65536)))
      (dolist
          (start
           (append
            (loop for offset below 32
                  collect offset)
            '(63)))
        (let* ((end (+ start size))
               (bytes (make-array end :element-type '(unsigned-byte 8))))
          (dotimes (index end)
            (setf (aref bytes index)
                  (mod (+ 17 (* 73 index) (floor index 17)) 256)))
          (let ((before (copy-seq bytes)))
            (dolist (seed '(0 1 305419896 2147483649 4294967295))
              (let ((expected (crc32c-codegen-reference bytes start end seed)))
                (dolist (fn functions)
                  (assert
                   (= expected (funcall fn bytes start end nil constants seed))
                   nil "CRC32C mismatch: size ~d start ~d seed ~x" size start
                   seed)
                  (incf checks))))
            (assert (equalp bytes before))))))
    (let ((bytes (make-array 1024 :element-type '(unsigned-byte 8))))
      (dotimes (index (length bytes))
        (setf (aref bytes index) (mod (+ 31 (* index 109)) 256)))
      (dolist
          (size '(0 1 7 8 15 16 31 32 63 64 65 127 128 129 255 256 257 1000))
        (dolist (seed '(0 4294967295 305419896))
          (let ((expected (crc32c-codegen-reference bytes 3 (+ 3 size) seed)))
            (loop for split from 0 to size
                  do (dolist (fn functions)
                       (let* ((first
                                (funcall fn bytes 3 (+ 3 split) nil constants
                                         seed))
                              (continued
                                (funcall fn bytes (+ 3 split) (+ 3 size) nil
                                         constants (logxor first 4294967295))))
                         (assert (= expected continued) nil
                                 "CRC32C continuation mismatch: size ~d split ~d"
                                 size split)
                         (incf checks))))))))
    (assert (equalp constants *crc32c-codegen-constants*))
    checks))

#+sbcl

(defun crc32c-codegen-measure (fn bytes size table constants iterations)
  "Measure a warmed direct kernel call, with explicit coefficients and no wrapper dispatch."
  (declare (optimize (speed 3) (safety 1))
           (type function fn)
           (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type fixnum size iterations))
  (let ((end (+ 3 size)) (witness 0))
    (declare (type (unsigned-byte 32) witness))
    (dotimes (iteration 100)
      (funcall fn bytes 3 end table constants 4294967295))
    (multiple-value-bind (seconds micros)
        (sb-ext:get-time-of-day)
      (let ((allocated (sb-ext:get-bytes-consed)))
        (dotimes (iteration iterations)
          (setf witness
                (logxor witness
                        (the (unsigned-byte 32)
                             (funcall fn bytes 3 end table constants
                                      4294967295)))))
        (setf allocated (- (sb-ext:get-bytes-consed) allocated))
        (multiple-value-bind (after-seconds after-micros)
            (sb-ext:get-time-of-day)
          (values
           (+ (- after-seconds seconds)
              (/ (- after-micros micros) 1000000.0d0))
           allocated witness))))))

#+sbcl

(defun crc32c-codegen-run (output-directory &key (megabytes 2048) force-scalar)
  "Validate selected CRC32C kernels, then compare them with IEEE CRC.
The checksums are different; each is validated against its own polynomial.
This experiment has no effect on the production WAL checksum or CPU dispatch."
  (let* ((directory (uiop/pathname:ensure-directory-pathname output-directory))
         (engines (crc32c-codegen-engines force-scalar))
         (checks (crc32c-codegen-check (mapcar #'second (rest engines))))
         (bytes
           (make-array (+ 1048576 64) :element-type '(unsigned-byte 8)
                                      :initial-element 173))
         (rows nil))
    (ensure-directories-exist (merge-pathnames "results.sexp" directory))
    (dolist (engine engines)
      (with-open-file
          (output
           (merge-pathnames (format nil "~a.asm" (first engine)) directory)
           :direction :output :if-exists :supersede)
        (let ((*standard-output* output))
          (disassemble (second engine)))))
    (dotimes (round 3)
      (dolist (size '(0 1 8 16 32 64 128 256 512 1024 8192 65536 1048576))
        (let ((iterations
                (max 100
                     (min 10000000 (floor (* megabytes 1048576) (max 1 size))))))
          (dolist
              (engine
               (if (evenp round)
                   engines
                   (reverse engines)))
            (destructuring-bind
                (name fn table constants)
                engine
              (multiple-value-bind (seconds allocated witness)
                  (crc32c-codegen-measure fn bytes size table constants
                                          iterations)
                (push
                 (list :engine name :round round :size size :iterations
                       iterations :seconds seconds :ns-per-call
                       (/ (* seconds 1.0d9) iterations) :allocated allocated
                       :witness witness)
                 rows)))))))
    (setf rows (nreverse rows))
    (with-open-file
        (output (merge-pathnames "results.sexp" directory) :direction :output
                                                           :if-exists :supersede)
      (write
       (list :sbcl (lisp-implementation-version) :checks checks :megabytes
             megabytes :measurements rows)
       :stream output :pretty t))
    (format t "~&CRC32C: ~d bit-reference checks passed.~%" checks)
    (dolist (size '(0 1 8 16 32 64 128 256 512 1024 8192 65536 1048576))
      (format t "~&~d bytes:" size)
      (dolist (engine engines)
        (let ((samples
                (sort
                 (loop for row in rows
                       when (and (= size (getf row :size))
                                 (equal (first engine) (getf row :engine)))
                         collect (getf row :ns-per-call))
                 #'<)))
          (format t " ~a ~,2f ns" (first engine) (second samples)))))
    (terpri)
    (values checks rows)))
