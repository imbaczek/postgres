;;;; recursive-cte-tests.lisp -- recursive binding analysis and iterative results.
(in-package #:pg)

(rove:deftest recursive-cte-counting-and-union
    (dolist (case '(("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<4) SELECT n FROM r" ((1) (2) (3) (4)))
                    ("WITH RECURSIVE r(n) AS(SELECT 1 UNION SELECT n FROM r) SELECT n FROM r" ((1)))
                    ("WITH RECURSIVE a AS(SELECT n FROM b),b(n) AS(SELECT 7) SELECT n FROM a" ((7)))
                    ("WITH RECURSIVE r(s,n) AS(SELECT 'a',1 UNION ALL SELECT s||'b',n+1 FROM r WHERE n<3) SELECT s FROM r" (("a") ("ab") ("abb")))
                    ("WITH RECURSIVE r(n) AS(SELECT 1/0 UNION ALL SELECT n+1 FROM r) SELECT 7" ((7)))
                    ("WITH RECURSIVE r(n) AS(WITH r AS(SELECT 7 AS n) SELECT n FROM r) SELECT n FROM r" ((7)))
                    ("WITH RECURSIVE a AS(SELECT n FROM b),b AS(WITH a AS(SELECT 7 AS n) SELECT n FROM a) SELECT n FROM a" ((7)))
                    ("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL (WITH s AS(SELECT n+1 AS n FROM r WHERE n<3) SELECT n FROM s)) SELECT n FROM r" ((1) (2) (3)))
                    ("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT d.n+1 FROM (SELECT n FROM r) d WHERE d.n<3) SELECT n FROM r" ((1) (2) (3)))
                    ("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<100) SELECT n FROM r LIMIT 2" ((1) (2)))))
      (rove:ok (equalp (second case) (subquery-test-rows (first case))) (first case))))

(rove:deftest recursive-cte-analysis-rejections
    (dolist (case '(("WITH RECURSIVE r(n) AS(SELECT n FROM r) SELECT * FROM r" "42P19")
                    ("WITH RECURSIVE r(n) AS(SELECT n FROM r UNION ALL SELECT 1) SELECT * FROM r" "42P19")
                    ("WITH RECURSIVE r(n) AS(WITH s AS(SELECT n FROM r) SELECT 1 UNION ALL SELECT n+1 FROM r) SELECT * FROM r" "42P19")
                    ("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT r.n+1 FROM (SELECT 1 AS x) d LEFT JOIN r ON true) SELECT * FROM r" "42P19")
                    ("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT a.n FROM r a CROSS JOIN r b) SELECT * FROM r" "42P19")
                    ("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM r WHERE EXISTS(SELECT 1 FROM r)) SELECT * FROM r" "42P19")
                    ("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT count(*) FROM r) SELECT * FROM r" "42P19")
                    ("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM r ORDER BY 1) SELECT * FROM r" "0A000")
                    ("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM r LIMIT 3) SELECT * FROM r" "0A000")
                    ("WITH RECURSIVE a AS(SELECT * FROM b),b AS(SELECT * FROM a) SELECT * FROM a" "0A000")
                    ("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT n::int8 FROM r) SELECT * FROM r" "42804")
                    ("WITH RECURSIVE r(n) AS(SELECT 'a' UNION ALL SELECT 1 FROM r) SELECT * FROM r" "42804")
                    ("WITH RECURSIVE r(s) AS(SELECT 'a'::varchar(2) UNION ALL SELECT (s||'b')::varchar(3) FROM r) SELECT * FROM r" "42804")))
      (let ((caught nil))
        (handler-case (plan-select-query (first (parse-statements (first case))))
          (pg-error (error) (setf caught error)))
        (rove:ok (and caught (string= (second case) (pg-error-sqlstate caught))) (first case)))))

(rove:deftest recursive-cte-generation-and-parameter-epochs
    (dolist (case
                '(("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT q.n FROM(SELECT 1 AS i UNION ALL SELECT 2) g CROSS JOIN LATERAL(WITH s AS MATERIALIZED(SELECT n+g.i AS n FROM r WHERE n<2) SELECT n FROM s) q) SELECT n FROM r"
                   ((1) (2) (3)))
                  ("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL(WITH s AS MATERIALIZED(SELECT n FROM r) SELECT n+1 FROM s WHERE n<4)) SELECT n FROM r"
                   ((1) (2) (3) (4)))
                  ("WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<2) SELECT (WITH RECURSIVE r(n) AS(SELECT 7 UNION ALL SELECT n+1 FROM r WHERE n<8) SELECT sum(n) FROM r),n FROM r ORDER BY n"
                   ((15 1) (15 2)))))
      (rove:ok (equalp (subquery-test-rows (first case)) (second case)) (first case))))

(rove:deftest recursive-cte-explain-and-rescan
    (let* ((plan (plan-select-query
                  (first (parse-statements
                          "WITH RECURSIVE t(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<4) SELECT n FROM t"))))
           (definition (first (getf (query-plan-prepared plan) :ctes)))
           (producer (sql-cte-plan definition))
           (nodes (collect-query-plan-nodes (query-plan-physical producer)))
           (recursive (find :recursive-union nodes :key #'first))
           (worktable (find :worktable-scan nodes :key #'first)))
      (rove:ok (and recursive worktable) "The producer exposes recursive and worktable operators")
      (rove:ok (search "Recursive Union" (format-query-plan plan)))
      (dotimes (run 2)
        (let ((*query-plan-instrumentation-p* t) (rows nil))
          (execute-query-plan plan (getf (query-plan-prepared plan) :relations)
                              (lambda (row) (push (coerce (car row) 'list) rows)))
          (rove:ok (equal (nreverse rows) '((1) (2) (3) (4))))
          (rove:ok (= 1 (plan-node-metadata-actual-loops
                         (gethash recursive (query-plan-metadata producer)))))
          (rove:ok (= 4 (plan-node-metadata-actual-loops
                         (gethash worktable (query-plan-metadata producer)))))))))
