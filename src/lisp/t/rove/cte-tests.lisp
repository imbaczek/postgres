;;;; cte-tests.lisp -- shared demand-driven query producers and WITH scope.
(in-package #:pg)

(cl-spec:defproperty cte-replay-readers-share-prefix
    ((size (range integer 0 20)) (schedule (range integer 0 1048575)))
  (:about make-query-replay-reader)
  (:trials (:normal 300))
  (let* ((produced 0)
         (producer (make-sql-iterator
                    (lambda ()
                      (if (< produced size)
                          (values (incf produced) t)
                          (values nil nil)))))
         (store (make-query-replay-store producer))
         (readers (list (make-query-replay-reader store) (make-query-replay-reader store)))
         (positions (vector 0 0)))
    (unwind-protect
         (loop for turn below (* 2 (1+ size))
               for index = (if (logbitp (mod turn 20) schedule) 1 0)
               always
               (multiple-value-bind (value present) (sql-iterator-next (nth index readers))
                 (let ((expected (min size (1+ (aref positions index)))))
                   (prog1 (and (eql present (< (aref positions index) size))
                               (or (not present) (= value expected))
                               (<= produced (max expected (aref positions (- 1 index)))))
                     (when present (incf (aref positions index)))))))
      (mapc #'close-sql-iterator readers)
      (close-query-replay-store store))))

(rove:deftest cte-replay-generated-interleaving
    (unit-check-property 'cte-replay-readers-share-prefix))

(rove:deftest cte-basic-query-results
    (dolist (case '(("WITH q AS (SELECT 7 AS x) SELECT x FROM q" ((7)))
                    ("WITH q(x) AS (SELECT 7) SELECT a.x,b.x FROM q a CROSS JOIN q b" ((7 7)))
                    ("WITH q AS (SELECT 7 AS x),r AS (SELECT x+1 AS y FROM q) SELECT y FROM r" ((8)))
                    ("WITH q AS (SELECT 1 AS x) SELECT (WITH q AS (SELECT 2 AS x) SELECT x FROM q),x FROM q" ((2 1)))
                    ("WITH q AS MATERIALIZED (SELECT 10/(2-d.x) AS x FROM (SELECT 1 AS x UNION ALL SELECT 2) d) SELECT x FROM q LIMIT 1" ((10)))))
      (rove:ok (equal (subquery-test-rows (first case)) (second case)) (first case))))

(rove:deftest cte-cursors-cover-existing-select-operators
    (dolist (sql '("SELECT d.x FROM (SELECT 3 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) d ORDER BY d.x LIMIT 2"
                   "SELECT sum(d.x),count(*) FROM (SELECT 1 AS x UNION ALL SELECT 2) d"
                   "SELECT d.x,count(*) FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) d GROUP BY d.x ORDER BY d.x"
                   "SELECT DISTINCT d.x FROM (SELECT 1 AS x UNION ALL SELECT 1) d"
                   "SELECT a.x,b.y FROM (SELECT 1 AS x UNION ALL SELECT 2) a LEFT JOIN (SELECT 1 AS y) b ON a.x=b.y ORDER BY a.x"
                   "SELECT a.x,b.y FROM (SELECT 1 AS x) a RIGHT JOIN (SELECT 1 AS y UNION ALL SELECT 2) b ON a.x=b.y ORDER BY b.y"
                   "SELECT a.x,b.y FROM (SELECT 1 AS x UNION ALL SELECT 3) a FULL JOIN (SELECT 1 AS y UNION ALL SELECT 2) b ON a.x=b.y ORDER BY a.x,b.y"
                   "SELECT a.x,b.y FROM (SELECT 1 AS x WHERE false) a RIGHT JOIN (SELECT 2 AS y) b ON true"
                   "SELECT d.x,rank() OVER(ORDER BY d.x),sum(d.x) OVER(ORDER BY d.x) FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) d ORDER BY d.x"
                   "SELECT d.x,sum(d.x) OVER(ORDER BY d.x ROWS UNBOUNDED PRECEDING) FROM (SELECT 1 AS x UNION ALL SELECT 2) d ORDER BY d.x LIMIT 1"
                   "SELECT d.x,count(*) OVER() FROM (SELECT 1 AS x UNION ALL SELECT 2) d ORDER BY d.x"
                   "SELECT (SELECT d.x+1) FROM (SELECT 1 AS x UNION ALL SELECT 2) d"))
      (rove:ok (equalp (subquery-test-rows sql)
                       (subquery-test-rows (format nil "WITH q AS MATERIALIZED (~a) SELECT * FROM q" sql)))
               sql)))

(rove:deftest cte-lexical-and-correlated-definitions
    (dolist (case
                '(("WITH a AS (SELECT 1 AS x),b AS (SELECT x FROM a) SELECT (WITH a AS (SELECT 2 AS x) SELECT x FROM b)" ((1)))
                  ("SELECT (WITH q AS MATERIALIZED(SELECT d.x AS x) SELECT a.x+b.x FROM q a CROSS JOIN q b) FROM (SELECT 1 AS x UNION ALL SELECT 2) d" ((2) (4)))
                  ("SELECT (WITH q AS (SELECT d.x AS x) SELECT (SELECT x FROM q)) FROM (SELECT 1 AS x UNION ALL SELECT 2) d" ((1) (2)))
                  ("WITH q AS (SELECT 1 AS x) SELECT x FROM q UNION ALL SELECT x+1 FROM q" ((1) (2)))))
      (rove:ok (equal (second case) (subquery-test-rows (first case))) (first case))))

(rove:deftest cte-correlated-state-lifetime-and-static-reference-plan
    (let* ((source (format nil "~{~a~^ UNION ALL ~}"
                           (loop for value from 1 to 30 collect (format nil "SELECT ~d AS x" value))))
           (plan (plan-select-query
                  (first (parse-statements
                          (format nil "SELECT (WITH q AS NOT MATERIALIZED(SELECT d.x AS x) SELECT x FROM q WHERE x>0) FROM (~a) d" source)))))
           (child (sql-subplan-plan (first (getf (query-plan-prepared plan) :sublinks))))
           (reference (first (query-plan-cte-reference-plans child)))
           (nodes (hash-table-count (query-plan-metadata reference)))
           (*query-plan-instrumentation-p* t)
           (counts nil))
      (dotimes (execution 2)
        (execute-query-plan plan (getf (query-plan-prepared plan) :relations)
                            (lambda (row)
                              (declare (ignore row))
                              (push (list (hash-table-count *cte-open-states*)
                                          (hash-table-count *sql-open-iterators*)) counts))))
      (rove:ok (every (lambda (counts) (equal counts '(0 0))) counts))
      (rove:ok (= nodes (hash-table-count (query-plan-metadata reference))))
      (rove:ok (= 30 (plan-node-metadata-actual-loops
                      (gethash (query-plan-physical reference) (query-plan-metadata reference)))))
      (rove:ok (search "per reference" (format-query-plan plan nil t)))))

(rove:deftest cte-shared-producer-instrumentation
    (let* ((plan (plan-select-query
                  (first (parse-statements
                          "WITH q AS MATERIALIZED(SELECT 1 AS x UNION ALL SELECT 2) SELECT a.x,b.x FROM q a CROSS JOIN q b"))))
           (producer (sql-cte-plan (first (getf (query-plan-prepared plan) :ctes))))
           (*query-plan-instrumentation-p* t))
      (execute-query-plan plan (getf (query-plan-prepared plan) :relations)
                          (lambda (row) (declare (ignore row))))
      (rove:ok (= 1 (plan-node-metadata-actual-loops
                     (gethash (query-plan-physical producer) (query-plan-metadata producer)))))
      (rove:ok (= 2 (plan-node-metadata-actual-row-count
                     (gethash (query-plan-physical producer) (query-plan-metadata producer)))))
      (rove:ok (search "CTE q (shared)" (format-query-plan plan nil t)))))
