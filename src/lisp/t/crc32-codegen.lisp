;;;; IEEE CRC code generation experiment. Load after the pg system.
;;;; (pg::crc32-codegen-run #p"/path/to/ram-backed/output/"
;;;;                       :baseline-path #p"/path/to/old/wal-crc32.hot.lisp")
;;;; BASELINE-PATH is optional. No production definitions are replaced.
(in-package #:pg)

(defparameter *crc32-codegen-ieee-table*
  (let ((table (make-array 256 :element-type '(unsigned-byte 32))))
    (dotimes (index 256 table)
      (let ((crc index))
        (dotimes (bit 8)
          (setf crc (logxor (ash crc -1) (if (oddp crc) #xedb88320 0))))
        (setf (aref table index) crc))))
  "Independent IEEE table for comparison with CRC32C.")

#+x86-64
(defmacro crc32-codegen-c-loop (bytes index limit state state1 state2 state3 factors)
  "Translate the C four-stream PCLMUL loop into SBCL assembler for comparison.
This experiment depends on SBCL's internal INLINE-VOP interface. LIMIT is
END - 63, not an address. The caller validates the slice and CPU. The descriptor
remains a GC root; no raw pointer escapes this VOP."
  `(sb-c::inline-vop
       (((array sb-vm::descriptor-reg) ,bytes)
        ((input-index sb-vm::unsigned-reg sb-vm::unsigned-num) ,index)
        ((limit sb-vm::unsigned-reg sb-vm::unsigned-num) ,limit)
        ((a sb-vm::int-sse-reg sb-vm::simd-pack-ub64) ,state)
        ((b sb-vm::int-sse-reg sb-vm::simd-pack-ub64) ,state1)
        ((c sb-vm::int-sse-reg sb-vm::simd-pack-ub64) ,state2)
        ((d sb-vm::int-sse-reg sb-vm::simd-pack-ub64) ,state3)
        ((k sb-vm::int-sse-reg sb-vm::simd-pack-ub64) ,factors)
        ((scratch sb-vm::int-sse-reg))
        ((next sb-vm::int-sse-reg)))
       ;; Results are written before all inputs have been consumed. :FROM :LOAD
       ;; makes their lifetimes overlap the inputs, preventing register reuse
       ;; from clobbering a still-needed input, particularly multiplier K.
       ((out-index sb-vm::unsigned-reg sb-vm::unsigned-num :from :load)
        (out-a sb-vm::int-sse-reg sb-vm::simd-pack-ub64 :from :load)
        (out-b sb-vm::int-sse-reg sb-vm::simd-pack-ub64 :from :load)
        (out-c sb-vm::int-sse-reg sb-vm::simd-pack-ub64 :from :load)
        (out-d sb-vm::int-sse-reg sb-vm::simd-pack-ub64 :from :load))
     (sb-vm::move out-index input-index)
     (sb-vm::move out-a a)
     (sb-vm::move out-b b)
     (sb-vm::move out-c c)
     (sb-vm::move out-d d)
     (let ((loop-label (sb-assem:gen-label))
           (done (sb-assem:gen-label)))
       (sb-assem:inst sb-x86-64-asm::cmp out-index limit)
       (sb-assem:inst sb-x86-64-asm::jmp :ae done)
       (sb-assem:emit-label loop-label)
       ,@(loop for reg in '(out-a out-b out-c out-d)
               for displacement in '(1 17 33 49)
               append
               `((sb-vm::move scratch ,reg)
                 (sb-assem:inst sb-x86-64-asm::pclmulqdq ,reg k #x11)
                 (sb-assem:inst sb-x86-64-asm::movdqu next
                                (sb-x86-64-asm::ea ,displacement array out-index))
                 (sb-assem:inst sb-x86-64-asm::pclmulqdq scratch k #x00)
                 (sb-assem:inst sb-x86-64-asm::pxor ,reg scratch)
                 (sb-assem:inst sb-x86-64-asm::pxor ,reg next)))
       (sb-assem:inst sb-x86-64-asm::add out-index 64)
       (sb-assem:inst sb-x86-64-asm::cmp out-index limit)
       (sb-assem:inst sb-x86-64-asm::jmp :b loop-label)
       (sb-assem:emit-label done))))

#+x86-64
(defun crc32-codegen-read-kernel (path)
  "Read the native kernel without evaluating any source forms."
  (let ((*package* (find-package :pg))
        (*read-eval* nil))
    (with-open-file (input path)
      (loop for form = (read input nil nil)
            while form
            when (and (consp form) (eq (first form) 'defun)
                      (eq (second form) '%wal-crc32-ieee-octets))
              return form
            finally (error "No native CRC kernel in ~a" path)))))

#+x86-64
(defun crc32-codegen-direct-form (source)
  "Replace just the four-stream Lisp loop with its assembly translation."
  (let ((replacements 0))
    (labels ((walk (form)
               (cond
                 ((and (consp form) (eq (first form) 'loop)
                       (equal (subseq form 1 (min 6 (length form)))
                              '(for cursor of-type sb-int:index from)))
                  (incf replacements)
                  '(multiple-value-setq (index state state1 state2 state3)
                    (crc32-codegen-c-loop bytes index (- end 63)
                     state state1 state2 state3 mult4)))
                 ((consp form) (cons (walk (car form)) (walk (cdr form))))
                 (t form))))
      (let ((result (walk source)))
        (assert (= replacements 1))
        result))))

#+x86-64
(defun crc32-codegen-reference (bytes start end seed)
  "Compute IEEE CRC bit by bit, independently of the vector folding."
  (let ((crc seed))
    (loop for index from start below end
          do (setf crc (logxor crc (aref bytes index)))
             (dotimes (bit 8)
               (setf crc (logxor (ash crc -1) (if (oddp crc) #xedb88320 0)))))
    (logxor crc #xffffffff)))

#+x86-64
(defun crc32-codegen-check (engines)
  "Check seeds, vector boundaries, offsets, larger buffers and input preservation."
  (let ((bytes (make-array (+ 65536 64) :element-type '(unsigned-byte 8)))
        (checks 0)
        (table (copy-seq *crc32-codegen-ieee-table*))
        (constants (copy-seq *native-ieee-crc32-constants*)))
    (dotimes (index (length bytes))
      (setf (aref bytes index) (mod (+ 17 (* 73 index) (floor index 17)) 256)))
    (let ((before (copy-seq bytes))
          (table-before (copy-seq table))
          (constants-before (copy-seq constants)))
      (dolist (size (append (loop for size from 0 to 512 collect size)
                            '(1023 1024 1025 8191 8192 8193 65535 65536)))
        (dolist (start '(0 1 3 15 16 31 63))
          (dolist (seed '(0 1 #x12345678 #x80000001 #xffffffff))
            (let ((expected (crc32-codegen-reference bytes start (+ start size) seed)))
              (dolist (engine engines)
                (let ((actual (funcall (cdr engine) bytes start (+ start size)
                                       table constants seed)))
                  (assert (= expected actual) ()
                          "~a size ~d start ~d seed ~x: expected ~x, got ~x"
                          (car engine) size start seed expected actual)
                  (incf checks)))))))
      (assert (equalp bytes before))
      (assert (equalp table table-before))
      (assert (equalp constants constants-before)))
    checks))

#+x86-64
(defun crc32-codegen-measure (function bytes size iterations)
  "Measure one warmed sample using a microsecond wall clock."
  (declare (optimize (speed 3) (safety 1))
           (type function function)
           (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type fixnum size iterations))
  (let ((end (+ 3 size))
        (witness 0))
    (declare (type (unsigned-byte 32) witness))
    (dotimes (iteration 100)
      (funcall function bytes 3 end *crc32-codegen-ieee-table* *native-ieee-crc32-constants* #xffffffff))
    (multiple-value-bind (seconds microseconds) (sb-ext:get-time-of-day)
      (let ((allocated (sb-ext:get-bytes-consed)))
        (dotimes (iteration iterations)
          (setf witness
                (logxor witness
                        (funcall function bytes 3 end *crc32-codegen-ieee-table*
                                 *native-ieee-crc32-constants* #xffffffff))))
        (setf allocated (- (sb-ext:get-bytes-consed) allocated))
        (multiple-value-bind (after-seconds after-microseconds) (sb-ext:get-time-of-day)
          (values (+ (- after-seconds seconds)
                     (/ (- after-microseconds microseconds) 1000000d0))
                  allocated witness))))))

#+x86-64
(defun crc32-codegen-run (output-directory &key baseline-path (megabytes 512))
  "Save disassembly, correctness and three-round timing evidence for both loops.
Optionally compile the old source at safety 3 and safety 0 for comparison.
This experiment requires x86-64 SBCL with PCLMUL and calls no foreign CRC."
  (assert *native-ieee-crc32-available-p*)
  (let* ((directory (uiop:ensure-directory-pathname output-directory))
         (source (crc32-codegen-read-kernel
                  (merge-pathnames "backend/storage/wal-crc32.hot.lisp"
                                   (asdf:system-source-directory "pg"))))
         (engines (list (cons "lisp" (compile nil (cons 'lambda (cddr source))))
                        (cons "assembly" (compile nil
                                                  (cons 'lambda
                                                        (cddr (crc32-codegen-direct-form source)))))))
         (measurements nil))
    (when baseline-path
      (let ((baseline (crc32-codegen-read-kernel baseline-path)))
        (setf engines
              (append (list (cons "baseline" (compile nil (cons 'lambda (cddr baseline))))
                            (cons "safety0"
                                  (compile nil
                                           (cons 'lambda
                                                 (cddr (subst '(safety 0) '(safety 3)
                                                              baseline :test #'equal))))))
                      engines))))
    (ensure-directories-exist (merge-pathnames "results.sexp" directory))
    (dolist (engine engines)
      (with-open-file (output (merge-pathnames (format nil "~a.asm" (car engine)) directory)
                              :direction :output :if-exists :supersede)
        (let ((*standard-output* output))
          (disassemble (cdr engine)))))
    (let ((checks (crc32-codegen-check engines))
          (bytes (make-array (+ 1048576 64) :element-type '(unsigned-byte 8)
                                            :initial-element 173)))
      (dotimes (round 3)
        (dolist (size '(16 64 128 256 1024 8192 65536 1048576))
          (let ((iterations (max 100 (min 10000000
                                          (floor (* megabytes 1048576) size)))))
            (dolist (engine (if (evenp round) engines (reverse engines)))
              (multiple-value-bind (seconds allocated witness)
                  (crc32-codegen-measure (cdr engine) bytes size iterations)
                (push (list :engine (car engine) :round round :size size
                            :iterations iterations :seconds seconds
                            :ns-per-call (/ (* seconds 1d9) iterations)
                            :allocated allocated :witness witness)
                      measurements))))))
      (setf measurements (nreverse measurements))
      (with-open-file (output (merge-pathnames "results.sexp" directory)
                              :direction :output :if-exists :supersede)
        (write (list :sbcl (lisp-implementation-version) :checks checks
                     :megabytes megabytes :measurements measurements)
               :stream output :pretty t))
      (format t "~&CRC codegen: ~d differential checks passed.~%" checks)
      (dolist (size '(16 64 128 256 1024 8192 65536 1048576))
        (format t "~&~d bytes:" size)
        (dolist (engine engines)
          (let ((samples
                  (sort (loop for row in measurements
                              when (and (= size (getf row :size))
                                        (equal (car engine) (getf row :engine)))
                                collect (getf row :ns-per-call))
                        #'<)))
            (format t " ~a ~,2f ns" (car engine) (second samples)))))
      (terpri)
      (values checks measurements))))
