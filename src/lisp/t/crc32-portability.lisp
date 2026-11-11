;;;; Standalone CRC qualification for native SBCL or an ARM64 emulator.
;;;; Run with SBCL --script; no Quicklisp or complete server startup is needed.
(require :asdf)
(require :sb-posix)

(load (merge-pathnames "../packages.lisp" *load-truename*))

(let* ((root (truename (merge-pathnames "../" (make-pathname :name nil :type nil :defaults *load-truename*))))
       (output (uiop:ensure-directory-pathname
                (or (uiop:getenv "PG_CRC_PORTABILITY_OUTPUT")
                    (merge-pathnames (format nil "crc-portability-~d/" (sb-posix:getpid))
                                     (uiop:temporary-directory))))))
  (ensure-directories-exist (merge-pathnames "crc-portability.log" output))
  (load (merge-pathnames "packages.lisp" root))
  (flet ((compiled (relative)
           (let* ((source (merge-pathnames relative root))
                  (fasl (merge-pathnames
                         (make-pathname :name (pathname-name source) :type "fasl") output)))
             (multiple-value-bind (path warnings failure) (compile-file source :output-file fasl)
               (assert (not warnings))
               (assert (not failure))
               (load path)))))
    (compiled "port/sbcl/arm-crc32-support.lisp")
    #+arm64 (compiled "port/sbcl/arm-crc32.lisp")
    (compiled "backend/storage/wal-core.hot.lisp")
    #+arm64 (compiled "backend/storage/wal-crc32-arm.hot.lisp")
    #+x86-64 (compiled "port/sbcl/ieee-crc32.lisp")
    #+x86-64 (compiled "backend/storage/wal-crc32.hot.lisp")
    #+x86-64 (compiled "port/sbcl/crc32c.lisp")
    #+x86-64 (compiled "backend/storage/wal-crc32c.hot.lisp"))
  ;; Compile the actual dispatcher, not a copy of its branching logic.
  (let ((*package* (find-package :pg)))
    (with-open-file (input (merge-pathnames "backend/storage/wal-core.lisp" root))
      (loop for form = (read input nil nil)
            while form
            when (and (consp form) (eq (car form) 'defun)
                      (eq (second form) (find-symbol "MAKE-WAL-CRC32-TABLE" :pg)))
              do (eval form)
            when (and (consp form) (eq (car form) 'defun)
                      (eq (second form) (find-symbol "WAL-CRC32-TYPED-RANGE" :pg)))
              do (eval '(defparameter pg::*wal-crc32c-reference-table*
                         (pg::make-wal-crc32-table)))
                 (eval '(defparameter pg::*wal-crc32-table*
                         pg::*wal-crc32c-reference-table*))
                 (eval form)
                 (return)))))

(in-package #:pg)

(defun crc-portability-reference (bytes start end seed polynomial)
  (let ((crc seed))
    (loop for index from start below end
          do (setf crc (logxor crc (aref bytes index)))
             (dotimes (bit 8)
               (setf crc (logxor (ash crc -1) (if (oddp crc) polynomial 0)))))
    (logxor crc #xffffffff)))

(defun crc-portability-table (polynomial)
  (let ((table (make-array 256 :element-type '(unsigned-byte 32))))
    (dotimes (index 256 table)
      (let ((crc index))
        (dotimes (bit 8)
          (setf crc (logxor (ash crc -1) (if (oddp crc) polynomial 0))))
        (setf (aref table index) crc)))))

(defun crc-portability-native-cases ()
  (append
   #+x86-64
   (append
    (when *native-ieee-crc32-available-p*
      (list (list "x86-ieee" #'%wal-crc32-ieee-octets #xedb88320
                  *native-ieee-crc32-constants*)))
    (when *native-crc32c-sse42-available-p*
      (list (list "x86-crc32c-sse42" #'%wal-crc32c-sse42-octets #x82f63b78 nil)))
    (when *native-crc32c-pclmul-available-p*
      (list (list "x86-crc32c-pclmul" #'%wal-crc32c-pclmul-octets #x82f63b78
                  *native-crc32c-constants*))))
   #+arm64
   (when *native-arm-crc32-available-p*
     (list (list "arm-ieee" #'%wal-crc32-ieee-arm-octets #xedb88320 nil)
           (list "arm-crc32c" #'%wal-crc32c-arm-octets #x82f63b78 nil)))))

(defun crc-portability-check ()
  (let ((checks 0)
        (table (make-wal-crc32-table))
        (ieee-table (crc-portability-table #xedb88320))
        (native (crc-portability-native-cases)))
    (format t "~&CRC-PLATFORM ~a ~s CRC32 ~s PMULL ~s~%"
            (lisp-implementation-version) (machine-type)
            *native-arm-crc32-available-p* *native-arm-pmull-available-p*)
    (dolist (size (append (loop for size from 0 to 257 collect size)
                          '(511 512 513 8191 8192 8193)))
      (dolist (start '(0 1 2 3 4 5 6 7 15 16 31))
        (let* ((end (+ start size))
               (bytes (make-array end :element-type '(unsigned-byte 8))))
          (dotimes (index end)
            (setf (aref bytes index) (mod (+ 17 (* 73 index) (floor index 17)) 256)))
          (let ((before (copy-seq bytes)))
            (dolist (seed '(0 #xffffffff #x12345678 #x80000001))
              (let ((expected (crc-portability-reference bytes start end seed #x82f63b78)))
                (assert (= expected (%wal-crc32-octets bytes start end table seed)))
                (assert (= expected (wal-crc32-typed-range bytes start end table seed)))
                (incf checks 2))
              (dolist (case native)
                (destructuring-bind (name fn polynomial constants) case
                  (let* ((expected (crc-portability-reference bytes start end seed polynomial))
                         (actual (funcall fn bytes start end
                                          (if (= polynomial #xedb88320) ieee-table table)
                                          constants seed)))
                    (assert (= expected actual) ()
                            "~a mismatch at size ~d start ~d seed ~x" name size start seed)
                    (incf checks)))))
            (assert (equalp before bytes))))))
    ;; A disabled feature must bypass native code altogether.
    (let* ((bytes (make-array 1024 :element-type '(unsigned-byte 8) :initial-element 173))
           (*native-arm-crc32-available-p* nil)
           #+x86-64 (*native-crc32c-sse42-available-p* nil)
           #+x86-64 (*native-crc32c-pclmul-available-p* nil)
           (expected (crc-portability-reference bytes 3 1024 #xffffffff #x82f63b78)))
      (assert (= expected (wal-crc32-typed-range bytes 3 1024 table)))
      (incf checks))
    ;; Noncanonical tables must keep their scalar recurrence even on a CRC CPU.
    (let* ((bytes (make-array 512 :element-type '(unsigned-byte 8) :initial-element 173))
           (altered (copy-seq table)))
      (setf (aref altered 17) (logxor 1 (aref altered 17)))
      (assert (= (%wal-crc32-octets bytes 0 512 altered)
                 (wal-crc32-typed-range bytes 0 512 altered)))
      (incf checks))
    (dolist (case native)
      (destructuring-bind (name fn polynomial constants) case
        (let ((bytes (make-array 1024 :element-type '(unsigned-byte 8) :initial-element 173)))
          (dotimes (iteration 100)
            (funcall fn bytes 3 1024 (if (= polynomial #xedb88320) ieee-table table)
                     constants #xffffffff))
          (let ((before (sb-ext:get-bytes-consed)))
            (dotimes (iteration 4096)
              (funcall fn bytes 3 1024 (if (= polynomial #xedb88320) ieee-table table)
                       constants #xffffffff))
            (let ((allocated (- (sb-ext:get-bytes-consed) before)))
              (assert (zerop allocated) () "~a allocated ~d" name allocated)
              (format t "~&CRC-ALLOC ~a ~d~%" name allocated))))))
    (format t "~&CRC-PORTABILITY-PASS ~d checks~%" checks)
    checks))

(load (merge-pathnames "crc32c-codegen.lisp" *load-truename*))

(format t "~&CRC-PROTOTYPE-ENGINES ~s~%" (mapcar #'first (crc32c-codegen-engines)))

(format t "~&CRC-PROTOTYPE-SCALAR ~d checks~%"
        (crc32c-codegen-check (list #'crc32c-codegen-scalar)))
#+arm64
(when *native-arm-crc32-available-p*
  (format t "~&CRC-PROTOTYPE-ARM ~d checks~%"
          (crc32c-codegen-check (list #'%wal-crc32c-arm-octets))))

(crc-portability-check)
