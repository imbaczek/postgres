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
