(in-package #:pg)

(rove:deftest basic-aggregate-null-and-type-semantics
    (dolist (case '(("SELECT count(NULL),count(false),count(0),min(NULL),max(NULL)" ((0 1 1 :null :null)))
                    ("SELECT count(d.x),min(d.x),max(d.x) FROM (SELECT 2 AS x UNION ALL SELECT NULL UNION ALL SELECT 1) d" ((2 1 2)))
                    ("SELECT min(d.x),max(d.x) FROM (SELECT 'b'::text AS x UNION ALL SELECT 'a'::text) d" (("a" "b")))
                    ("SELECT count(d.x),min(d.x),max(d.x),avg(d.x) FROM (SELECT 1 AS x WHERE false) d" ((0 :null :null :null)))
                    ("SELECT count(d.x),min(d.x),max(d.x) FROM (SELECT 1 AS x UNION ALL SELECT 2) d HAVING avg(d.x)>1" ((2 1 2)))))
      (rove:ok (equalp (subquery-test-expected (second case)) (subquery-test-rows (first case))) (first case))))

(rove:deftest basic-aggregate-exact-average
    (dolist (case '(("SELECT avg(d.x)::text FROM (SELECT 1 AS x UNION ALL SELECT 0 UNION ALL SELECT 0) d" "0.33333333333333333333")
                    ("SELECT avg(d.x)::text FROM (SELECT -1 AS x UNION ALL SELECT 0 UNION ALL SELECT 0) d" "-0.33333333333333333333")
                    ("SELECT avg(d.x)::text FROM (SELECT 1 AS x UNION ALL SELECT 2) d" "1.5000000000000000")
                    ("SELECT avg(d.x)::text FROM (SELECT 1 AS x UNION ALL SELECT -1) d" "0.00000000000000000000")
                    ("SELECT avg(d.x)::integer FROM (SELECT 0 AS x UNION ALL SELECT 1) d" 1)
                    ("SELECT avg(d.x)::integer FROM (SELECT 0 AS x UNION ALL SELECT -1) d" -1)))
      (rove:ok (equalp (list (list (second case))) (subquery-test-rows (first case))) (first case))))

(rove:deftest basic-aggregate-nested-and-window
    (dolist (case '(("WITH q AS MATERIALIZED (SELECT avg(d.x) AS a FROM (SELECT 1 AS x UNION ALL SELECT 2) d) SELECT a::text,(a+a)::text FROM q" (("1.5000000000000000" "3.0000000000000000")))
                    ("SELECT count(d.x) OVER(), min(d.x) OVER(),max(d.x) OVER(),avg(d.x) OVER()::text FROM (SELECT 1 AS x UNION ALL SELECT 2) d LIMIT 1" ((2 1 2 "1.5000000000000000")))))
      (rove:ok (equalp (second case) (subquery-test-rows (first case))) (first case))))

(cl-spec:defproperty numeric-text-retains-coefficient-and-scale
    ((coefficient (range integer -1000000000 1000000000))
     (scale (range integer 0 12)))
  (:about sql-numeric-input)
  (:trials (:normal 250))
  (let* ((original (make-sql-decimal coefficient scale))
         (restored (sql-numeric-input (sql-numeric-output original))))
    (and (= coefficient (sql-numeric-coefficient restored))
         (= scale (sql-numeric-scale restored)))))

(rove:deftest generated-numeric-text-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'numeric-text-retains-coefficient-and-scale :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))
