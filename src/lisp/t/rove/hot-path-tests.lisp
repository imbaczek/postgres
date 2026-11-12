(in-package #:pg)

(defvar *unit-hot-probes* (make-hash-table :test #'equal))

(defun unit-hot-definitions ()
  (loop for path in (directory (merge-pathnames "**/*.hot.lisp"
                                                (asdf:system-source-directory "pg")))
        append
        (let ((*read-eval* nil) (*package* (find-package :pg)) (eof (list nil)))
          (with-open-file (input path)
            (loop for form = (read input nil eof)
                  until (eq form eof)
                  do (unless (and (consp form)
                                  (member (first form) '(in-package declaim defun)))
                       (error "Unsupported top-level hot-file form in ~a: ~s"
                              path (if (consp form) (first form) form)))
                     (when (eq (first form) 'in-package)
                       (setf *package* (or (find-package (second form))
                                           (error "Unknown hot-file package in ~a" path))))
                  when (and (consp form) (eq (first form) 'defun))
                    collect (list path (second form) form))))))

(defun unit-hot-explicit-allocations (body &optional parameters)
  (let ((found nil))
    (labels ((walk (form)
               (when (consp form)
                 (unless (member (first form) '(quote declare))
                   (let ((name (and (symbolp (first form))
                                    (symbol-name (first form)))))
                     (when (and name
                                (or (member name
                                            '("CONS" "ACONS" "LIST" "LIST*" "APPEND" "REVAPPEND"
                                              "COPY-LIST" "COPY-TREE" "COPY-SEQ" "SUBSEQ"
                                              "BUTLAST" "PAIRLIS" "MULTIPLE-VALUE-LIST"
                                              "CONCATENATE" "MAPCAR" "MAPLIST" "MAPCAN" "MAPCON"
                                              "MAKE-ARRAY" "ADJUST-ARRAY" "MAKE-STRING" "MAKE-SEQUENCE"
                                              "MAKE-LIST" "MAKE-HASH-TABLE" "VECTOR" "VECTOR-PUSH-EXTEND"
                                              "COERCE" "REMOVE" "REMOVE-IF" "REMOVE-IF-NOT" "REVERSE"
                                              "REMOVE-DUPLICATES" "SUBSTITUTE" "SUBSTITUTE-IF"
                                              "SUBSTITUTE-IF-NOT" "MAP" "STRING-UPCASE" "STRING-DOWNCASE"
                                              "STRING-CAPITALIZE" "STRING-TRIM" "STRING-LEFT-TRIM"
                                              "STRING-RIGHT-TRIM" "INTERN" "GENSYM" "GENTEMP"
                                              "QUASIQUOTE" "OPEN" "WITH-OPEN-FILE" "WITH-OPEN-STREAM"
                                              "FORMAT" "FORMATTER" "WRITE-TO-STRING" "PRIN1-TO-STRING"
                                              "PRINC-TO-STRING" "READ-FROM-STRING" "WITH-OUTPUT-TO-STRING"
                                              "STRING->BYTES" "BYTES->STRING" "STRING-TO-OCTETS"
                                              "OCTETS-TO-STRING" "ERROR" "SIGNAL" "WARN" "ASSERT" "CHECK-TYPE")
                                            :test #'string=)
                                    (uiop:string-prefix-p "MAKE-" (string-left-trim "%" name))))
                       (pushnew name found :test #'string=))
                     (when (and (equal name "LOOP")
                                (some (lambda (item)
                                        (and (symbolp item)
                                             (member (symbol-name item)
                                                     '("COLLECT" "COLLECTING" "APPEND" "APPENDING"
                                                       "NCONC" "NCONCING")
                                                     :test #'string=)))
                                      (rest form)))
                       (pushnew "LOOP/COLLECTOR" found :test #'string=)))
                   (mapc #'walk form)))))
      (dolist (parameter parameters)
        (when (consp parameter) (walk (second parameter))))
      (mapc #'walk body))
    (sort found #'string<)))

(defun unit-hot-call-allocated-bytes (function arguments)
  (dotimes (iteration 2) (apply function arguments))
  (let ((before (sb-ext:get-bytes-consed)))
    (dotimes (iteration 4096) (apply function arguments))
    (- (sb-ext:get-bytes-consed) before)))

(cl-spec:defproperty install-progress-scratch-matches-canonical-writers
    ((crc (range integer 0 4294967295)) (step (range integer 1 200001)))
  (:about %physical-install-write-progress)
  (:trials (:normal 200))
  (let* ((scratch (make-array 32 :element-type '(unsigned-byte 8) :initial-element 255))
         (size (%physical-install-write-progress crc step scratch))
         (expected (physical-adoption-install-step-image crc step)))
    (and (= size (length expected))
         (equalp (subseq scratch 0 size) expected)
         (equalp expected (physical-reseed-install-step-image crc step)))))

(rove:deftest install-progress-generated-scratch-encoding-contract
    (unit-check-property 'install-progress-scratch-matches-canonical-writers))

(rove:deftest install-progress-scratch-covers-decimal-and-hex-boundaries
    (dolist (crc '(0 1 2147483648 4294967295))
      (let ((scratch (make-array 32 :element-type '(unsigned-byte 8) :initial-element 255)))
        (dolist (step '(1 9 10 99 100 999 1000 9999 10000 99999 100000 200001))
          (let ((size (%physical-install-write-progress crc step scratch)))
            (rove:ok (equalp (subseq scratch 0 size)
                             (physical-adoption-install-step-image crc step))))))))

(rove:deftest install-progress-scratch-is-allocation-free-with-scalar-crc
    (let* ((*wal-crc32-table* (make-array 256 :element-type t :initial-contents *wal-crc32-table*))
           (scratch (make-array 32 :element-type '(unsigned-byte 8) :initial-element 255))
           (arguments (list 4294967295 200001 scratch)))
      (rove:ok (zerop (unit-hot-call-allocated-bytes #'%physical-install-write-progress arguments)))
      (let ((size (%physical-install-write-progress 4294967295 200001 scratch)))
        (rove:ok (equalp (subseq scratch 0 size)
                         (physical-adoption-install-step-image 4294967295 200001))))))

(setf (gethash '%physical-install-write-u32-hex *unit-hot-probes*)
      (let ((scratch (make-array 32 :element-type '(unsigned-byte 8) :initial-element 255)))
        (list (list 0 scratch 0) (list 4294967295 scratch 24) (list 2147483648 scratch 8)))
      (gethash '%physical-install-write-step *unit-hot-probes*)
      (let ((scratch (make-array 32 :element-type '(unsigned-byte 8) :initial-element 255)))
        (loop for step in '(1 9 10 99 100 999 1000 9999 10000 99999 100000 200001)
              collect (list step scratch 26)))
      (gethash '%physical-install-write-progress *unit-hot-probes*)
      (let ((scratch (make-array 32 :element-type '(unsigned-byte 8) :initial-element 255)))
        (loop for step in '(1 9 10 99 100 999 1000 9999 10000 99999 100000 200001)
              append (list (list 0 step scratch) (list 4294967295 step scratch)))))

(setf (gethash '%wal-crc32-octets *unit-hot-probes*)
      (let ((bytes (make-array 257 :element-type '(unsigned-byte 8) :initial-element 127))
            (table *wal-crc32-table*))
        (list (list bytes 0 0 table)
              (list bytes 0 (length bytes) table)
              (list bytes 1 (1- (length bytes)) table)))
      (gethash '%physical-store-fnv1a64-parts *unit-hot-probes*)
      (let ((bytes (make-array 257 :element-type '(unsigned-byte 8) :initial-element 255)))
        (list (list #xcbf29ce484222325 (make-array 0 :element-type '(unsigned-byte 8)))
              (list 0 bytes)
              (list #xffffffffffffffff bytes)
              (list #x8000000000000000 bytes))))

(setf (gethash '%physical-store-fnv1a64-span-parts *unit-hot-probes*)
      (let ((bytes (make-array 257 :element-type '(unsigned-byte 8) :initial-element 255)))
        (list (list #xcbf29ce484222325 bytes 0 0)
              (list 0 bytes 0 (length bytes))
              (list #xffffffffffffffff bytes 1 (1- (length bytes)))
              (list #x8000000000000000 bytes 2 2)
              (list #xffffffffffffffff bytes (length bytes) (length bytes)))))

(rove:deftest hot-files-reject-explicit-allocation
    (rove:ok (equal '("MAKE-ARRAY") (unit-hot-explicit-allocations '((make-array 8)))))
  (rove:ok (equal '("LOOP/COLLECTOR")
                  (unit-hot-explicit-allocations '((loop for index below 8 collect index)))))
  (rove:ok (null (unit-hot-explicit-allocations
                  '((let ((value 0)) (declare (type fixnum value)) (incf value))))))
  (rove:ok (equal '("MAKE-ARRAY")
                  (unit-hot-explicit-allocations nil '(&optional (scratch (make-array 8))))))
  (rove:ok (equal '("MAKE-STRING")
                  (unit-hot-explicit-allocations nil '(&key ((:scratch supplied) (make-string 8))))))
  (rove:ok (equal '("LIST")
                  (unit-hot-explicit-allocations nil '(&aux (scratch (list 1 2))))))
  (rove:ok (equal '("REMOVE-DUPLICATES")
                  (unit-hot-explicit-allocations '((remove-duplicates input)))))
  (let ((definitions (unit-hot-definitions)))
    (rove:ok definitions)
    (dolist (definition definitions)
      (destructuring-bind (path name form) definition
        (let ((offenders (unit-hot-explicit-allocations (cdddr form) (third form))))
          (rove:ok (null offenders) (format nil "~a ~s ~s" path name offenders)))))))

(rove:deftest hot-files-do-not-allocate
    (let ((witness (list nil)))
      (rove:ok (plusp (unit-hot-call-allocated-bytes
                       (lambda () (setf (car witness) (cons nil nil))) nil))))
  (dolist (definition (unit-hot-definitions))
    (destructuring-bind (path name form) definition
      (declare (ignore form))
      (multiple-value-bind (cases present) (gethash name *unit-hot-probes*)
        (rove:ok (and present cases) (format nil "~a ~s has allocation probes" path name))
        (when (and present cases)
          (dolist (arguments cases)
            (let ((allocated (unit-hot-call-allocated-bytes (fdefinition name) arguments)))
              (rove:ok (zerop allocated)
                       (format nil "~a ~s allocated ~d bytes" path name allocated)))))))))
