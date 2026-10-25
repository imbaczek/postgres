;;;; SUM shares descriptor execution with other typed aggregates.
(in-package #:pg-interop-tests)

(deftest sum-return-types-and-plan
    (dolist (example '(("SELECT sum(1::smallint)" 20) ("SELECT sum(1)" 20)
                       ("SELECT sum(1::bigint)" 1700)
                       ("SELECT sum(pg_wal_lsn_diff('0/10','0/0'))" 1700)))
      (let* ((plan (pg::plan-select-query (first (pg::parse-statements (first example)))))
             (target (first (getf (pg::query-plan-prepared plan) :targets)))
             (column (pg::target->result-column target)))
        (ok (= (second example) (pg::result-column-type-oid column)))
        (ok (string= "sum" (pg::result-column-name column)))
        (ok (find :sorted-aggregate (pg::collect-query-plan-nodes (pg::query-plan-physical plan))
                  :key #'first)))))

(deftest sum-empty-null-and-wide-values
    (ok (equalp (list (vector pg::+null+ pg::+null+ 0))
                (derived-query-rows "SELECT sum(1),sum(1::bigint),COUNT(*) WHERE false")))
  (ok (equalp (list (vector pg::+null+))
              (derived-query-rows "SELECT sum(NULL::int)")))
  (ok (equalp '(#(0)) (derived-query-rows "SELECT COALESCE(sum(NULL::int),0)")))
  (ok (equalp '(#(18446744073709551614))
              (derived-query-rows
               "SELECT sum(q.x) FROM (SELECT 9223372036854775807::bigint AS x UNION ALL SELECT 9223372036854775807::bigint) q")))
  (ok (= pg::+int8-min+ (pg::sum-aggregate-transition pg::+int8-max+ 1 :int8)))
  (ok (= pg::+int8-max+ (pg::sum-aggregate-transition pg::+int8-min+ -1 :int8))))

(deftest sum-overload-and-context-errors
    (dolist (example '(("SELECT sum(NULL)" "42725") ("SELECT sum('1')" "42725")
                       ("SELECT sum(true)" "42883") ("SELECT sum(1::oid)" "42883")
                       ("SELECT sum('0/0'::pg_lsn)" "42883") ("SELECT sum()" "42883")
                       ("SELECT sum(1,2)" "42883") ("SELECT sum(*)" "42883")
                       ("SELECT sum(sum(1))" "42803")
                       ("SELECT 1 WHERE sum(1)>0" "42803")))
      (ok (equal (second example) (sqlstate-of (derived-query-rows (first example)))))))

(deftest sum-mixed-aggregates-spill-and-cleanup
    (let* ((directory (merge-pathnames (format nil "pg-sum-spill-~a/" (gensym))
                                       (uiop:temporary-directory)))
           (descriptors '((:kind :count :type :int8)
                          (:kind :sum :type :numeric :input (:col 2 :type :int8))
                          (:kind :array-agg :type :text-array :input (:col 3 :type :text))))
           (input (list (vector 1 pg::+int8-max+ "a") (vector 2 pg::+null+ "x")
                        (vector 1 pg::+int8-max+ "b") (vector 2 pg::+null+ "y"))))
      (ensure-directories-exist directory)
      (unwind-protect
           (dolist (memory '(1048576 1))
             (let ((pg::*query-temp-directory* directory)
                   (pg::*query-work-memory-bytes* memory) (output nil))
               (pg::stream-sorted-aggregates
                (lambda (callback) (dolist (row input) (funcall callback row)))
                '((:col 1 :type :int4)) descriptors
                (lambda (row) (push row output)))
               (ok (equalp (list (vector 1 2 18446744073709551614 #("a" "b"))
                                 (vector 2 2 pg::+null+ #("x" "y")))
                           (nreverse output)))
               (ok (null (uiop:directory-files directory)))
               (ok (equalp (list (vector 1 18446744073709551614 18446744073709551614 2)
                                 (vector 2 pg::+null+ pg::+null+ 1))
                           (derived-query-rows
                            "SELECT q.k,sum(q.x),sum(q.x),COUNT(*) FROM (SELECT 1 AS k,9223372036854775807::bigint AS x UNION ALL SELECT 2,NULL::bigint UNION ALL SELECT 1,9223372036854775807::bigint) q GROUP BY q.k ORDER BY sum(q.x)")))
               (ok (null (uiop:directory-files directory)))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(cl-spec:defproperty sum-transition-preserves-exact-total
    ((a (range integer -1000000000000 1000000000000))
     (b (range integer -1000000000000 1000000000000))
     (c (range integer -1000000000000 1000000000000))
     (mask (range integer 0 7)))
  (:about pg::sum-aggregate-transition)
  (:trials (:normal 200))
  (let* ((values (loop for value in (list a b c) for index from 0
                       collect (if (logbitp index mask) pg::+null+ value)))
         (present (remove pg::+null+ values))
         (expected (if present (reduce #'+ present) pg::+null+)))
    (eql expected
         (reduce (lambda (state value) (pg::sum-aggregate-transition state value :numeric))
                 values :initial-value pg::+null+))))

(deftest generated-sum-null-and-integer-inputs
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'sum-transition-preserves-exact-total :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
