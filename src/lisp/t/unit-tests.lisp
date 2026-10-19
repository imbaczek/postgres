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

(defun test-index-catalog-roundtrip ()
  (let* ((idx (make-pg-index :oid 42 :name "nnd" :relfilenode 42
                             :table-oid 41 :columns '(1) :unique-p t
                             :constraint-p t
                             :nulls-not-distinct-p t))
         (copy (index-from-sexp (index-to-sexp idx))))
    (check (pg-index-nulls-not-distinct-p copy)
           "NULLS NOT DISTINCT survives catalog serialization")
    (check (pg-index-constraint-p copy)
           "constraint ownership survives catalog serialization"))
  (let ((legacy-pk (index-from-sexp '(:primary t))))
    (check (not (pg-index-constraint-p legacy-pk))
           "legacy catalog has no constraint ownership flag")
    (check (pg-index-constraint-owned-p legacy-pk)
           "legacy primary index remains constraint-owned")))

(defun test-relation-build-own-xid ()
  (let ((relfilenode 987653)
        (xid 987654))
    (setf (gethash relfilenode *relation-writers*) (list xid))
    (let ((token (acquire-relation-build-lock relfilenode xid)))
      (check (eq token (gethash relfilenode *relation-builders*))
             "relation build ignores its own writer XID")
      (release-relation-build-lock relfilenode token))
    (remhash relfilenode *relation-writers*)))

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

(defun test-btree-duplicate-keys ()
  ;; Insert many entries with one key so the leaves holding them must split.
  ;; Repeatedly updating an indexed value produces exactly this shape: a long
  ;; run of index entries with equal keys spread over several leaves.
  (let* ((rel 987654)
         (key (encode-index-key (list 42) (list :int4)))
         (n 2000))
    (btree-init rel)
    (dotimes (i n)
      (btree-insert rel key 0 i))
    (let ((found (btree-search rel key)))
      (check (= (length found) n)
             "duplicate keys are found across leaf splits")
      (check (= (length (remove-duplicates found :test #'equal)) n)
             "every duplicate-key TID is distinct")
      (check (null (btree-search rel (encode-index-key (list 43) (list :int4))))
             "an absent key is not found"))))

(defun test-btree-mixed-duplicate-keys ()
  ;; Interleave two keys so splits produce equal separators for both, which
  ;; stresses the key-range order that search assumes.
  (let* ((rel 987655)
         (ka (encode-index-key (list 1) (list :int4)))
         (kb (encode-index-key (list 2) (list :int4)))
         (n 3000))
    (btree-init rel)
    (dotimes (i n)
      (btree-insert rel (if (evenp i) ka kb) 0 i))
    (let ((fa (btree-search rel ka))
          (fb (btree-search rel kb)))
      (check (= (length fa) (/ n 2)) "even entries found (mixed keys)")
      (check (= (length fb) (/ n 2)) "odd entries found (mixed keys)")
      (check (every (lambda (tid) (evenp (cdr tid))) fa)
             "key A search returned only key-A TIDs")
      (check (every (lambda (tid) (oddp (cdr tid))) fb)
             "key B search returned only key-B TIDs"))))

(defun test-parse-error-aborts-transaction ()
  (let ((path (merge-pathnames "parse-error.out" *pgdata*)))
    (setf (backend-state *backend*) :in-progress
          (backend-explicit *backend*) t
          (backend-command-id *backend*) 0)
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "select from;"))
    (check (eq (backend-state *backend*) :aborted)
           "a parse error aborts an explicit transaction")
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type '(unsigned-byte 8))
      (exec-simple-query stream "select 1;"))
    (check (eq (backend-state *backend*) :aborted)
           "aborted transaction stays aborted")
    (abort-transaction)
    (check (eq (backend-state *backend*) :idle)
           "abort clears the transaction state")))

(defun run-unit-tests ()
  (setf *test-failures* 0
        *test-checks* 0)
  (let ((dir (merge-pathnames
              (format nil "pg-unit-~a/" (random 1000000))
              #p"/tmp/opencode/")))
    (ensure-directories-exist dir)
    (let ((*pgdata* dir)
          (*backend* (make-backend)))
      (clog-init)
      (test-page-roundtrip)
      (test-page-compaction)
      (test-tuple-roundtrip)
      (test-key-order)
      (test-index-catalog-roundtrip)
      (test-relation-build-own-xid)
      (test-visibility)
      (test-btree-duplicate-keys)
      (test-btree-mixed-duplicate-keys)
      (test-parse-error-aborts-transaction))
    (ignore-errors
     (uiop:delete-directory-tree dir :validate t :if-does-not-exist :ignore)))
  (format t "~&unit-tests: ~a checks, ~a failures~%"
          *test-checks* *test-failures*)
  (when (plusp *test-failures*)
    (sb-ext:exit :code 1)))

(run-unit-tests)
