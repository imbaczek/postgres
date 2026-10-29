;;;; lateral-tests.lisp -- lexical FROM parameters and per-left rescans.
(in-package #:pg)

(rove:deftest lateral-dependent-query-results
    (dolist (case
                '(("SELECT a.x,b.y FROM (SELECT 1 AS x UNION ALL SELECT 2) a CROSS JOIN LATERAL (SELECT a.x+1 AS y) b ORDER BY a.x" ((1 2) (2 3)))
                  ("SELECT a.x,b.y FROM (SELECT 1 AS x UNION ALL SELECT 2) a LEFT JOIN LATERAL (SELECT a.x AS y WHERE a.x=1) b ON true ORDER BY a.x" ((1 1) (2 :null)))
                  ("SELECT a.x,b.y,c.z FROM (SELECT 1 AS x UNION ALL SELECT 2) a CROSS JOIN LATERAL (SELECT a.x+1 AS y) b CROSS JOIN LATERAL (SELECT a.x+b.y AS z) c ORDER BY a.x" ((1 2 3) (2 3 5)))
                  ("SELECT a.x,b.y FROM (SELECT 1 AS x UNION ALL SELECT 2) a CROSS JOIN LATERAL (SELECT a.x AS y UNION ALL SELECT a.x+10) b ORDER BY a.x,b.y" ((1 1) (1 11) (2 2) (2 12)))
                  ("SELECT a.x,b.y FROM (SELECT 1 AS x UNION ALL SELECT 2) a CROSS JOIN LATERAL (WITH q AS MATERIALIZED (SELECT a.x AS y) SELECT q.y FROM q) b ORDER BY a.x" ((1 1) (2 2)))))
      (let ((expected (mapcar (lambda (row) (mapcar (lambda (value) (if (eq value :null) +null+ value)) row))
                              (second case))))
        (rove:ok (equalp expected (subquery-test-rows (first case))) (first case))
        (rove:ok (equalp expected
                         (subquery-test-rows
                          (format nil "WITH q AS MATERIALIZED (~a) SELECT * FROM q" (first case))))
                 "the demand-driven CTE executor uses the same lateral frames"))))

(rove:deftest lateral-derived-column-aliases
    (dolist (sql '("SELECT d.y FROM (SELECT 7 AS x) d(y)"
                   "SELECT d.y FROM (SELECT 7 AS x) a CROSS JOIN LATERAL (SELECT a.x) d(y)"))
      (rove:ok (equal '((7)) (subquery-test-rows sql)) sql))
  (rove:ok (equal '((7 8))
                  (subquery-test-rows "SELECT d.y,d.z FROM (SELECT 7 AS x,8 AS z) d(y)")))
  (rove:ok
   (handler-case
       (progn (prepare-select (first (parse-statements "SELECT * FROM (SELECT 7) d(x,y)"))) nil)
     (pg-error (condition) (string= "42P10" (pg-error-sqlstate condition))))))

(rove:deftest lateral-scope-and-join-restrictions
    (dolist (case
                '(("SELECT * FROM pg_get_publication_tables(sum(1)::text)" "42803")
                  ("SELECT * FROM pg_get_publication_tables((row_number() OVER())::text)" "42P20")
                  ("SELECT b.y FROM (SELECT 1 AS x) a RIGHT JOIN LATERAL (WITH q AS (SELECT a.x) SELECT 1 AS y) b ON true" "42P10")
                  ("SELECT b.y FROM (SELECT 1 AS x) a CROSS JOIN (SELECT a.x AS y) b" "42P01")
                  ("SELECT b.y FROM (SELECT 1 AS x) a RIGHT JOIN LATERAL (SELECT a.x AS y) b ON true" "42P10")
                  ("SELECT b.y FROM (SELECT 1 AS x) a FULL JOIN LATERAL (SELECT a.x AS y) b ON true" "42P10")
                  ("SELECT b.y FROM LATERAL (SELECT a.x AS y) b CROSS JOIN (SELECT 1 AS x) a" "42P01")))
      (rove:ok (handler-case (progn (prepare-select (first (parse-statements (first case)))) nil)
                 (pg-error (condition) (string= (second case) (pg-error-sqlstate condition))))
               (first case)))
  (rove:ok (equal '((2)) (subquery-test-rows
                          "SELECT b.y FROM (SELECT 1 AS x) a RIGHT JOIN LATERAL (SELECT 2 AS y) b ON true")))
  (rove:ok (equal '((7)) (subquery-test-rows
                          "SELECT (SELECT b.y FROM (SELECT 1 AS x) a CROSS JOIN (SELECT d.x AS y) b) FROM (SELECT 7 AS x) d"))))

(rove:deftest lateral-pull-iterator-closes-each-parameter-epoch
    (let ((opens nil) (closes nil) (next 0))
      (let* ((left (make-sql-iterator
                    (lambda ()
                      (when (< next 3) (values (vector (incf next)) t)))))
             (iterator
               (make-lateral-join-iterator
                '(:type :left :qual (:bool t) :left-width 1 :right-width 1)
                left
                (lambda (row origins)
                  (declare (ignore origins))
                  (let ((value (aref row 0)) (done nil))
                    (push value opens)
                    (make-sql-iterator
                     (lambda () (unless done (setf done t) (values (vector value) t)))
                     (lambda () (push value closes))))))))
        (unwind-protect
             (progn
               (rove:ok (equalp #((1 1) (2 2))
                                (coerce (loop repeat 2 collect
                                                       (coerce (sql-iterator-next iterator) 'list)) 'vector)))
               (rove:ok (equal '(2 1) opens))
               (rove:ok (equal '(1) closes)))
          (close-sql-iterator iterator))
        (rove:ok (equal '(2 1) closes) "early stop closes the current right invocation"))))
