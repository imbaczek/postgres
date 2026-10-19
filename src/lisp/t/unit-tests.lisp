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

(defun test-live-page-items-stop ()
  (let ((page (page-init (make-page)))
        (page-evaluations 0)
        (seen '()))
    (dotimes (i 3)
      (page-add-item page (make-array 8 :element-type '(unsigned-byte 8)
                                        :initial-element i)))
    (page-mark-dead page 2)
    (do-live-page-items ((progn (incf page-evaluations) page) offnum off len)
      (check (and (plusp off) (plusp len)) "live page item has storage")
      (push offnum seen))
    (check (equal (nreverse seen) '(1 3))
           "live page iteration skips dead slots")
    (check (= page-evaluations 1) "live page iterator evaluates PAGE once")
    (flet ((first-live-item ()
             (do-live-page-items (page offnum off len)
               (when (and (plusp off) (plusp len))
                 (return-from first-live-item offnum)))
             nil))
      (check (equal (first-live-item) 1)
             "live page iteration preserves nonlocal early return"))))

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
         (sexp (index-to-sexp idx))
         (copy (index-from-sexp sexp)))
    (check (equal sexp
                  '(:oid 42 :name "nnd" :relfilenode 42 :table-oid 41
                    :columns (1) :unique t :primary nil :constraint t
                    :nulls-not-distinct t))
           "index catalog encoding is the compatible flat plist")
    (check (pg-index-nulls-not-distinct-p copy)
           "NULLS NOT DISTINCT survives catalog serialization")
    (check (pg-index-constraint-p copy)
           "constraint ownership survives catalog serialization"))
  (let* ((class (make-pg-class
                 :oid 50 :name "codec" :relfilenode 51
                 :columns (vector (make-pg-column :name "value" :type :int4
                                                  :typmod -1 :attnum 1
                                                  :notnull t))
                 :indexes '(52)))
         (sexp (class-to-sexp class))
         (copy (class-from-sexp sexp)))
    (check (equal sexp
                  '(:oid 50 :name "codec" :relfilenode 51
                    :columns ((:name "value" :type :int4 :typmod -1
                               :attnum 1 :notnull t))
                    :indexes (52)))
           "class and column codecs use compatible flat plists")
    (check (vectorp (pg-class-columns copy))
           "class codec restores columns as a vector")
    (check (string= (pg-column-name (aref (pg-class-columns copy) 0)) "value")
           "class codec round-trips column mappings"))
  (let ((legacy-pk (index-from-sexp '(:primary t))))
    (check (not (pg-index-constraint-p legacy-pk))
           "legacy catalog has no constraint ownership flag")
    (check (pg-index-constraint-owned-p legacy-pk)
           "legacy primary index remains constraint-owned")))

(defun test-sql-strict-let-evaluation ()
  (let ((evaluations '()))
    (check (eq +null+
               (sql-strict-let ((left (progn (push :left evaluations) +null+))
                                (right (progn (push :right evaluations) 7)))
                 :body-must-not-run))
           "strict let returns NULL when any binding is NULL")
    (check (equal (nreverse evaluations) '(:left :right))
           "strict let evaluates every binding once before NULL check"))
  (let ((evaluations 0))
    (check (= 9
              (sql-strict-let ((left (progn (incf evaluations) 4))
                               (right (progn (incf evaluations) 5)))
                (+ left right)))
           "strict let returns body result for non-NULL bindings")
    (check (= evaluations 2) "strict let bindings run once each")))

(defun test-error-cleanup-and-fatal-rethrow ()
  (let* ((fatal (make-condition 'pg-error :severity "FATAL"
                                          :sqlstate "XX000" :message "test fatal"))
         (cleanup-count 0)
         (caught nil))
    (handler-case
        (with-error-cleanup (condition (incf cleanup-count))
          (error fatal))
      (pg-error (condition)
        (setf caught condition)))
    (check (= cleanup-count 1) "error cleanup runs before rethrow")
    (check (eq caught fatal) "error cleanup re-signals the original condition"))
  (let* ((fatal (make-condition 'pg-error :severity "FATAL"
                                          :sqlstate "XX000" :message "test fatal"))
         (handler-ran nil)
         (caught nil))
    (handler-case
        (handler-case-nonfatal
         (error fatal)
         (condition
          (setf handler-ran (typep condition 'error))))
      (pg-error (condition)
        (setf caught condition)))
    (check (not handler-ran) "nonfatal handler skips FATAL errors")
    (check (eq caught fatal) "nonfatal handler re-signals the original FATAL"))
  (check (eq :handled
             (handler-case-nonfatal
              (error "test nonfatal")
              (condition
               (if (typep condition 'error) :handled :unexpected))))
         "nonfatal handler handles ordinary errors"))

(defun test-create-table-cleanup-after-fsync-error ()
  (let* ((*catalog* (make-catalog))
         (*backend* (make-backend))
         (db (make-database :name "postgres" :oid 1))
         (original-fsync-index (symbol-function 'fsync-index))
         (caught nil))
    (setf (backend-database *backend*) "postgres"
          (gethash "postgres" (catalog-databases *catalog*)) db)
    (unwind-protect
         (progn
           (setf (symbol-function 'fsync-index)
                 (lambda (index)
                   (declare (ignore index))
                   (error "injected final index fsync failure")))
           (handler-case
               (exec-create-table
                nil
                '(:name "cleanup_test"
                  :columns ((:name "value" :type :int4 :typmod -1 :unique t))))
             (error (condition)
               (setf caught condition)))
           (check (typep caught 'error)
                  "CREATE TABLE re-signals final index fsync failure")
           (check (null (gethash "cleanup_test" (database-tables db)))
                  "CREATE TABLE cleanup removes table after final fsync failure"))
      (setf (symbol-function 'fsync-index) original-fsync-index))))

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
      (test-live-page-items-stop)
      (test-page-compaction)
      (test-tuple-roundtrip)
      (test-key-order)
      (test-index-catalog-roundtrip)
      (test-sql-strict-let-evaluation)
      (test-error-cleanup-and-fatal-rethrow)
      (test-create-table-cleanup-after-fsync-error)
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
