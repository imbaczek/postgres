;;;; unit-tests.lisp -- Lisp-level unit tests for the pg server.
;;;;
;;;; Run via t/run-unit-tests.sh, which loads the "pg" system first.

(in-package #:pg)

(defvar *test-failures* 0)
(defvar *test-checks* 0)

(defmacro check (form &optional (label nil))
  `(if ,form
       (incf *test-checks*)
       (progn
         (incf *test-failures*)
         (format t "~&FAIL: ~a~%" (or ,label ',form)))))

(defun test-page-roundtrip ()
  (let ((p (page-init (make-page))))
    (check (= (page-lp-count p) 0) "empty page has no line pointers")
    (check (= (page-free-space p) (- +blcksz+ +size-of-page-header+))
           "empty page free space")
    (let ((item (make-array 8 :element-type '(unsigned-byte 8)
                              :initial-element 7)))
      (check (= (page-add-item p item) 1) "first item gets LP 1")
      (multiple-value-bind (off flags len) (page-get-lp p 1)
        (check (= flags +lp-normal+) "LP is normal")
        (check (= len 8) "LP length")
        (check (equalp (subseq p off (+ off 8)) item) "LP payload"))
      (check (= (page-add-item p item) 2) "second item gets LP 2")
      (check (= (page-lp-count p) 2) "two line pointers")
      (page-mark-dead p 1)
      (check (= (page-lp-count p) 2) "dead slot keeps the LP")
      (check (= (page-add-item p item :allow-reuse t) 1)
             "dead slot is reused when ALLOW-REUSE"))))

(defun test-page-compaction ()
  (let ((p (page-init (make-page))))
    (dotimes (i 64)
      (page-add-item p (make-array 32 :element-type '(unsigned-byte 8)
                                      :initial-element i)))
    (page-mark-dead p 10)
    (page-mark-dead p 20)
    (page-compact p)
    (check (= (page-lp-count p) 64) "compaction preserves LP count")
    (loop for i from 1 to 64
          unless (member i '(10 20))
            do (multiple-value-bind (off flags len) (page-get-lp p i)
                 (declare (ignore off len))
                 (check (= flags +lp-normal+) "compaction keeps live LPs")))))

(defun test-tuple-roundtrip ()
  (let* ((cols (coerce
                (list (make-pg-column :name "a" :type :int4 :typmod -1
                                      :attnum 1 :notnull nil)
                      (make-pg-column :name "b" :type :varchar :typmod 8
                                      :attnum 2 :notnull nil)
                      (make-pg-column :name "c" :type :int8 :typmod -1
                                      :attnum 3 :notnull nil))
                'vector))
         (vals (vector 42 "hello" 1234567890123))
         (bytes (form-heap-tuple vals cols 7 0 1 0 0)))
    (multiple-value-bind (got xmin xmax cid infomask)
        (deform-heap-tuple bytes cols)
      (declare (ignore infomask))
      (check (= xmin 7) "xmin round-trips")
      (check (= xmax 0) "xmax round-trips")
      (check (= cid 1) "cid round-trips")
      (check (= (aref got 0) 42) "int4 round-trips")
      (check (string= (aref got 1) "hello") "varchar round-trips")
      (check (= (aref got 2) 1234567890123) "int8 round-trips")))
  ;; NULLs must survive the round trip too.
  (let* ((cols (coerce
                (list (make-pg-column :name "a" :type :int4 :typmod -1
                                      :attnum 1 :notnull nil)
                      (make-pg-column :name "b" :type :varchar :typmod 8
                                      :attnum 2 :notnull nil))
                'vector))
         (vals (vector +null+ "x"))
         (bytes (form-heap-tuple vals cols 7 0 1 0 0)))
    (multiple-value-bind (got xmin xmax cid infomask)
        (deform-heap-tuple bytes cols)
      (declare (ignore xmin xmax cid infomask))
      (check (eq (aref got 0) +null+) "null int4 round-trips")
      (check (string= (aref got 1) "x") "varchar after null"))))

(defun test-key-order ()
  (flet ((k (v ty) (encode-index-key (list v) (list ty))))
    (check (minusp (key-compare (k -1 :int4) (k 0 :int4))) "int4 order")
    (check (minusp (key-compare (k -2147483648 :int4) (k 2147483647 :int4)))
           "int4 extremes")
    (check (minusp (key-compare (k -1 :int8) (k 0 :int8))) "int8 order")
    (check (minusp (key-compare (k +null+ :int4) (k -2147483648 :int4)))
           "null sorts first")
    (check (minusp (key-compare (k "abc" :varchar) (k "abd" :varchar)))
           "varchar order")
    ;; Index keys carry a length prefix, so ordering is length-first.  This
    ;; is internal only: SQL comparisons and ORDER BY use compare-3way.
    (check (minusp (key-compare (k "b" :varchar) (k "aa" :varchar)))
           "varchar key encoding is length-first")))

(defun test-visibility ()
  (let ((snap (make-snapshot-struct :xmin 10 :xmax 1000 :xip '() :curcid 5)))
    ;; Committed insert, never deleted -> visible.
    (clog-set 100 +clog-committed+)
    (check (heap-tuple-visible-p 100 0 0 snap nil) "committed insert visible")
    ;; Aborted insert -> invisible.
    (clog-set 101 +clog-aborted+)
    (check (not (heap-tuple-visible-p 101 0 0 snap nil)) "aborted insert hidden")
    ;; Committed insert, committed delete -> invisible.
    (clog-set 102 +clog-committed+)
    (clog-set 103 +clog-committed+)
    (check (not (heap-tuple-visible-p 102 103 0 snap nil))
           "committed delete hides tuple")
    ;; Committed insert, in-progress delete still in our snapshot -> visible.
    (clog-set 104 +clog-committed+)
    (clog-set 105 +clog-in-progress+)
    (let ((snap2 (make-snapshot-struct :xmin 10 :xmax 1000
                                       :xip '(105) :curcid 5)))
      (check (heap-tuple-visible-p 104 105 0 snap2 nil)
             "concurrent delete leaves tuple visible"))
    ;; Own-xid rule: visible only if inserted before the current command.
    (let ((snap3 (make-snapshot-struct :xmin 10 :xmax 1000 :xip '() :curcid 5)))
      (check (heap-tuple-visible-p 200 0 3 snap3 200)
             "own insert visible for later command")
      (check (not (heap-tuple-visible-p 200 0 5 snap3 200))
             "own insert invisible in same command"))))

(defun run-unit-tests ()
  (setf *test-failures* 0
        *test-checks* 0)
  (let ((dir (merge-pathnames
              (format nil "pg-unit-~a/" (random 1000000))
              #p"/tmp/opencode/")))
    (ensure-directories-exist dir)
    (let ((*pgdata* dir))
      (clog-init))
    (test-page-roundtrip)
    (test-page-compaction)
    (test-tuple-roundtrip)
    (test-key-order)
    (test-visibility)
    (ignore-errors
      (dolist (f (directory (merge-pathnames "*.*" dir)))
        (delete-file f))
      (sb-posix:rmdir (namestring dir))))
  (format t "~&unit-tests: ~a checks, ~a failures~%"
          *test-checks* *test-failures*)
  (when (plusp *test-failures*)
    (sb-ext:exit :code 1)))

(run-unit-tests)
