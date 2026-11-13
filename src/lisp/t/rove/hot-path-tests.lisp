(in-package #:pg)

(defvar *unit-hot-probes* (make-hash-table :test #'equal))

(setf (gethash '%string-span-utf8-size *unit-hot-probes*)
      (let ((text (coerce (mapcar #'code-char
                                  '(0 127 128 2047 2048 55295 57344 65535 65536 1114111))
                          'string)))
        (list (list "" 0 0)
              (list text 0 (length text))
              (list text 2 8)
              (list (make-string 4096 :initial-element #\a) 0 4096)
              (list (make-array 3 :element-type 'character :displaced-to text
                                  :displaced-index-offset 2) 0 3)
              (list (make-array 16 :element-type 'character :initial-element #\λ
                                   :fill-pointer 3 :adjustable t) 0 3)
              (list (string (code-char #xd800)) 0 1)
              (list (string (code-char #xdfff)) 0 1))))

(setf (gethash '%logical-sql-quote-into *unit-hot-probes*)
      (loop for value in (list "" "'" "a'b" "a\\b" "🙂λ"
                               (make-string 4096 :initial-element #\')
                               (make-array 8 :element-type 'base-char :initial-element #\'
                                             :fill-pointer 3 :adjustable t)
                               (make-array 2 :element-type 'character
                                             :displaced-to "x'λy" :displaced-index-offset 1))
            collect (list value (make-string (+ 2 (length value) (count #\' value))
                                             :element-type 'character))))

(defun unit-timeline-lineage-fixture (depth stride finite-head-p)
  (loop for index downfrom (1- depth) to 0
        collect (make-timeline-history-entry
                 :timeline (1+ index) :begin (* index stride)
                 :end (when (or finite-head-p (< index (1- depth)))
                        (* (1+ index) stride)))))

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

(setf (gethash '%fdinfo-token= *unit-hot-probes*)
      (list (list "lock:" 0 5 "lock:") (list "lock?" 0 5 "lock:")
            (list "" 0 0 "") (list "x" 0 0 "x"))
      (gethash '%fdinfo-fields *unit-hot-probes*)
      (let ((spans (make-array 18 :element-type '(unsigned-byte 32) :initial-element 0)))
        (list (list "ino: 123" 0 8 spans 2)
              (list (format nil " ~c" #\Tab) 0 2 spans 9)
              (list "a b c" 0 5 spans 2)
              (list "" 0 0 spans 0) (list "a" 0 1 spans 0)))
      (gethash '%fdinfo-unsigned-p *unit-hot-probes*)
      (list (list "0" 0 1 10 nil) (list "0" 0 1 10 t)
            (list "0001" 0 4 10 t) (list "abcdefABCDEF" 0 12 16 nil)
            (list "g" 0 1 16 nil) (list "" 0 0 10 nil)
            (list (make-string 21 :initial-element #\1) 0 21 10 t))
      (gethash '%fdinfo-inode= *unit-hot-probes*)
      (list (list "1" 0 1 1)
            (list "18446744073709551615" 0 20 #xffffffffffffffff)
            (list "18446744073709551616" 0 20 #xffffffffffffffff)
            (list "00000000000000000001" 0 20 1)
            (list "0" 0 1 0) (list "-1" 0 2 1) (list "1x" 0 2 1)
            (list "" 0 0 1) (list "999999999999999999999" 0 21 1)
            (list (string (code-char 1776)) 0 1 0))
      (gethash '%fdinfo-flock-p *unit-hot-probes*)
      (let* ((spans (make-array 18 :element-type '(unsigned-byte 32) :initial-element 0))
             (good (format nil "ino: 1~%lock: 1: FLOCK ADVISORY WRITE 123 08:30:1 0 EOF~%"))
             (large (format nil "ino: 18446744073709551615~%lock: 1: FLOCK ADVISORY WRITE 123 08:30:18446744073709551615 0 EOF~%")))
        (append
         (list (list good 1 spans) (list large #xffffffffffffffff spans)
               (list (format nil "ino: ~c~%lock: 1: FLOCK ADVISORY WRITE 123 08:30:~c 0 EOF~%"
                             (code-char 1776) (code-char 1776)) 0 spans)
               (list (subseq good 0 (1- (length good))) 1 spans)
               (list (concatenate 'string good (make-string (- 4096 (length good)) :initial-element #\x)) 1 spans)
               (list (make-string 4097 :initial-element #\x) 1 spans))
         (loop for text in
                        (list "" (format nil "ino: 1~%ino: 1~%lock: 1: FLOCK ADVISORY WRITE 123 08:30:1 0 EOF~%")
                              (concatenate 'string good "lock: invalid")
                              "lock: 1: FLOCK ADVISORY WRITE 123 08:30:1 0 EOF"
                              (format nil "ino: 2~%lock: 1: FLOCK ADVISORY WRITE 123 08:30:1 0 EOF~%")
                              (format nil "ino: 1~%lock: 1: FLOCK ADVISORY WRITE 123 08:30:1 0~%")
                              (format nil "ino: 1~%lock: 0: FLOCK ADVISORY WRITE 123 08:30:1 0 EOF~%")
                              (format nil "ino: 1~%lock: 1: FLOCK MANDATORY WRITE 123 08:30:1 0 EOF~%")
                              (format nil "ino: 1~%lock: 1: FLOCK ADVISORY READ 123 08:30:1 0 EOF~%")
                              (format nil "ino: 1~%lock: 1: FLOCK ADVISORY WRITE 0 08:30:1 0 EOF~%")
                              (format nil "ino: 1~%lock: 1: FLOCK ADVISORY WRITE 123 08:30:1:1 0 EOF~%")
                              (format nil "ino: 1~%lock: 1: FLOCK ADVISORY WRITE 123 08:1 0 EOF~%")
                              (format nil "ino: 1~%lock: 1: FLOCK ADVISORY WRITE 123 :30:1 0 EOF~%")
                              (format nil "ino: 1~%lock: 1: FLOCK ADVISORY WRITE 123 08:30:2 0 EOF~%"))
               collect (list text 1 spans)))))

(setf (gethash '%timeline-valid-lineage-p *unit-hot-probes*)
      (append
       (list (list nil))
       (loop for depth in '(1 2 8)
             append (loop for stride in '(0 1 2305843009213693951)
                          append (loop for finite-head-p in '(nil t)
                                       collect (list (unit-timeline-lineage-fixture
                                                      depth stride finite-head-p)))))
       (loop for field in '(:timeline :begin :end)
             append (loop for bad in '(-1 18446744073709551616 :invalid)
                          collect (let* ((history (unit-timeline-lineage-fixture 3 10 nil))
                                         (entry (second history)))
                                    (ecase field
                                      (:timeline (setf (timeline-history-entry-timeline entry) bad))
                                      (:begin (setf (timeline-history-entry-begin entry) bad))
                                      (:end (setf (timeline-history-entry-end entry) bad)))
                                    (list history))))
       (loop for field in '(:timeline :begin :end)
             collect (let* ((history (unit-timeline-lineage-fixture 3 10 nil))
                            (entry (second history)))
                       (ecase field
                         (:timeline (setf (timeline-history-entry-timeline entry) 3))
                         (:begin (setf (timeline-history-entry-begin entry) 11))
                         (:end (setf (timeline-history-entry-end entry) nil)))
                       (list history)))))

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
