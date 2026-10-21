;;;; spill-tests.lisp -- executor row-spool and external-sort invariants.

(in-package #:pg)

(defun make-query-spill-test-directory ()
  (let ((directory
          (merge-pathnames
           (format nil "pg-query-spill-test-~a-~a/"
                   (sb-posix:getpid) (random 1000000))
           (uiop:temporary-directory))))
    (ensure-directories-exist (merge-pathnames "sentinel" directory))
    directory))

(defun query-spill-test-directory-empty-p (directory)
  (null (directory (merge-pathnames "*.*" directory))))

(defun test-query-temp-file-macro-cleans-up-after-error ()
  (let* ((directory (make-query-spill-test-directory))
         (pathname nil))
    (unwind-protect
         (let ((*query-temp-directory* directory))
           (handler-case
               (with-query-temp-files ()
                                      (multiple-value-bind (path stream) (open-query-temp-output)
                                        (setf pathname path)
                                        (query-spill-write-record stream #(1))
                                        (close stream))
                                      (error "forced query spill cleanup"))
             (error () nil))
           (check (and pathname (not (probe-file pathname)))
                  "query temporary-file scope removes files after errors")
           (check (query-spill-test-directory-empty-p directory)
                  "query temporary-file scope leaves no spill artifacts"))
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

(defun test-query-temp-open-registration-failure-cleans-up ()
  (let* ((directory (make-query-spill-test-directory))
         (pathname nil)
         (original-register (symbol-function 'register-query-temp-file)))
    (unwind-protect
         (let ((*query-temp-directory* directory))
           (with-query-temp-files ()
                                  (setf (symbol-function 'register-query-temp-file)
                                        (lambda (path)
                                          (setf pathname path)
                                          (error "forced spill registration failure")))
                                  (handler-case (open-query-temp-output)
                                    (error () nil))
                                  (setf (symbol-function 'register-query-temp-file)
                                        original-register)
                                  (check (and pathname (not (probe-file pathname)))
                                         "failed spill registration closes and removes the opened file")
                                  (check (query-spill-test-directory-empty-p directory)
                                         "failed spill registration leaves no untracked artifact")))
      (setf (symbol-function 'register-query-temp-file) original-register)
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

(defun test-query-temp-delete-failure-retains-cleanup-registration ()
  (let* ((directory (make-query-spill-test-directory))
         (pathname nil)
         (original-unlink (symbol-function 'unlink-query-temp-file)))
    (unwind-protect
         (let ((*query-temp-directory* directory))
           (with-query-temp-files ()
                                  (multiple-value-bind (path stream) (open-query-temp-output)
                                    (setf pathname path)
                                    (close stream))
                                  (setf (symbol-function 'unlink-query-temp-file)
                                        (lambda (path)
                                          (declare (ignore path))
                                          (error "forced spill deletion failure")))
                                  (unwind-protect
                                       (delete-query-temp-file pathname)
                                    (setf (symbol-function 'unlink-query-temp-file)
                                          original-unlink))
                                  (check (member pathname *query-temp-files* :test #'equal)
                                         "failed early spill deletion retains scope cleanup tracking"))
           (check (and pathname (not (probe-file pathname)))
                  "scope cleanup retries a failed early spill deletion")
           (check (query-spill-test-directory-empty-p directory)
                  "failed early spill deletion leaves no spill artifact"))
      (setf (symbol-function 'unlink-query-temp-file) original-unlink)
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

(defun test-full-join-spool-spills-typed-bytea-and-match-state ()
  (let* ((directory (make-query-spill-test-directory))
         (bytea-a (make-array 2 :element-type '(unsigned-byte 8)
                                :initial-contents '(10 11)))
         (bytea-b (make-array 3 :element-type '(unsigned-byte 8)
                                :initial-contents '(20 21 22)))
         (original-eval (symbol-function 'eval-expr))
         (emitted '()))
    (unwind-protect
         (let ((*query-work-memory-bytes* 1)
               (*query-temp-directory* directory))
           (setf (symbol-function 'eval-expr)
                 (lambda (expr environment)
                   (if (eq expr :spill-join-equal)
                       (= (aref environment 0) (aref environment 1))
                       (funcall original-eval expr environment))))
           (stream-select-join-environments
            (lambda (visit-left) (funcall visit-left #(1)))
            (lambda (visit-right)
              (dolist (row (list (vector 1 bytea-a)
                                 (vector 2 bytea-b)))
                (when (eq (funcall visit-right row) :stop)
                  (return :stop))))
            (list :type :full :qual :spill-join-equal
                  :left-width 1 :right-width 2)
            (lambda (row) (push row emitted)))
           (setf emitted (nreverse emitted))
           (check (= (length emitted) 2)
                  "spilled FULL JOIN emits one match and one unmatched inner row")
           (check (and (= (aref (first emitted) 0) 1)
                       (= (aref (first emitted) 1) 1)
                       (typep (aref (first emitted) 2)
                              '(vector (unsigned-byte 8)))
                       (equalp (aref (first emitted) 2) bytea-a))
                  "spilled FULL JOIN preserves matched BYTEA element type and bytes")
           (check (and (eq (aref (second emitted) 0) +null+)
                       (= (aref (second emitted) 1) 2)
                       (typep (aref (second emitted) 2)
                              '(vector (unsigned-byte 8)))
                       (equalp (aref (second emitted) 2) bytea-b))
                  "spilled match bitmap preserves the unmatched BYTEA row")
           (check (query-spill-test-directory-empty-p directory)
                  "FULL JOIN removes row-spool and match-map files on completion"))
      (setf (symbol-function 'eval-expr) original-eval)
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

(defun test-query-row-spool-spills-bytea-without-expanded-list ()
  (let* ((directory (make-query-spill-test-directory))
         (byte-length 65536)
         (bytea (make-array byte-length :element-type '(unsigned-byte 8)
                                        :initial-element 255))
         (spilled-size nil)
         (restored nil))
    (unwind-protect
         (let ((*query-temp-directory* directory))
           (with-query-temp-files ()
                                  (with-query-row-spool (spool 1)
                                    (query-row-spool-append spool
                                                            (vector +null+ bytea))
                                    (query-row-spool-seal spool)
                                    (with-open-file (stream
                                                     (query-row-spool-pathname spool)
                                                     :direction :input
                                                     :external-format :utf-8)
                                      (setf spilled-size (file-length stream)))
                                    (map-query-row-spool
                                     spool
                                     (lambda (row index)
                                       (declare (ignore index))
                                       (setf restored row)))))
           (check (and spilled-size (< spilled-size (* 3 byte-length)))
                  "spilled BYTEA uses compact encoding without per-byte list expansion")
           (check (and (eq (aref restored 0) +null+)
                       (typep (aref restored 1) '(vector (unsigned-byte 8)))
                       (= (length (aref restored 1)) byte-length)
                       (equalp (aref restored 1) bytea))
                  "large spilled NULL and BYTEA retain their values and types")
           (check (query-spill-test-directory-empty-p directory)
                  "large BYTEA spool removes its file after reading"))
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))

(defun test-query-plan-external-sort-spills-stably ()
  (let* ((directory (make-query-spill-test-directory))
         (table (make-pg-class :oid 93001 :name "sort_input"
                               :relfilenode 93011 :columns #()))
         (path (make-select-scan-path :kind :seq-scan))
         (target (list :expr '(:col 2 :type :varchar :typmod -1)
                       :name "label" :type :varchar :typmod -1
                       :table-oid 93001 :attnum 2))
         (scan (list :seq-scan 0 93001))
         (order (list (cons '(:col 1 :type :int4 :typmod -1) nil)))
         (plan (%make-query-plan
                :physical (list :sort order
                                (list :project (list target) scan))))
         (registered (list (list :table table :path path)))
         (original-scan (symbol-function 'scan-select-path))
         (input (list #(2 "A") #(1 "B") #(2 "C") #(1 "D")))
         (labels '()))
    (unwind-protect
         (let ((*query-work-memory-bytes* 1)
               (*query-temp-directory* directory))
           (setf (symbol-function 'scan-select-path)
                 (lambda (received-table relfilenode received-path snapshot callback)
                   (declare (ignore relfilenode received-path snapshot))
                   (check (eq received-table table)
                          "external sort reads its registered scan relation")
                   (dolist (values input)
                     (when (eq (funcall callback (make-htup :values values)) :stop)
                       (return :stop)))))
           (interpret-query-plan
            plan registered
            (lambda (row) (push (aref (car row) 0) labels)))
           (setf labels (nreverse labels))
           (check (equal labels '("B" "D" "A" "C"))
                  "external merge sort preserves stable order across equal keys")
           (check (query-spill-test-directory-empty-p directory)
                  "external sort removes run files after completion")
           (setf labels nil)
           (let ((delivered 0))
             (interpret-query-plan
              plan registered
              (lambda (row)
                (incf delivered)
                (push (aref (car row) 0) labels)
                :stop))
             (check (= delivered 1)
                    "external sort stops delivering rows when its consumer stops")
             (check (equal (nreverse labels) '("B"))
                    "external sort stop returns the first stable sorted row"))
           (check (query-spill-test-directory-empty-p directory)
                  "external sort removes merged run files after early stop"))
      (setf (symbol-function 'scan-select-path) original-scan)
      (ignore-errors
       (uiop:delete-directory-tree directory :validate t
                                             :if-does-not-exist :ignore)))))
