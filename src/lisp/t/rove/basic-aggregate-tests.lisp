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
      (rove:ok (equalp (second case) (subquery-test-rows (first case))) (first case)))
  (dolist (sql '("SELECT row_number(x => 1) OVER ()"
                 "SELECT sum(DISTINCT no_such_numeric_argument()) OVER ()"
                 "SELECT sum(DISTINCT 1, 2) OVER ()"
                 "SELECT row_number(DISTINCT 1) OVER ()"))
    (let ((condition (handler-case (progn (subquery-test-rows sql) nil)
                       (pg-error (error) error))))
      (rove:ok (and condition (string= (pg-error-sqlstate condition) "42883")) sql))))

(cl-spec:defproperty numeric-text-retains-coefficient-and-scale
    ((coefficient (range integer -1000000000 1000000000))
     (scale (range integer 0 12)))
  (:about sql-numeric-input)
  (:trials (:normal 250))
  (let* ((original (make-sql-decimal coefficient scale))
         (restored (sql-numeric-input (sql-numeric-output original))))
    (and (= coefficient (sql-numeric-coefficient restored))
         (= scale (sql-numeric-scale restored)))))

(cl-spec:defproperty numeric-literal-roundtrip
    ((coefficient (range integer -1000000000 1000000000))
     (scale (range integer 1 12)))
  (:about analyze-expr)
  (:trials (:normal 250))
  (let* ((original (make-sql-decimal coefficient scale))
         (text (sql-numeric-output original))
         (actual (first (first (subquery-test-rows (format nil "SELECT (~a)::text" text))))))
    (equal text actual)))

(cl-spec:defproperty numeric-typmod-rounds-and-pads
    ((coefficient (range integer -999999 999999))
     (input-scale (range integer 0 5))
     (target-scale (range integer -2 5)))
  (:about sql-numeric-apply-typmod)
  (:trials (:normal 250))
  (let* ((input (make-sql-decimal coefficient input-scale))
         (typmod (+ 4 (logior (ash 12 16) (logand target-scale #x7ff))))
         (output (sql-numeric-apply-typmod input typmod))
         (unit (expt 10 (- target-scale)))
         (scaled (/ (sql-numeric-rational input) unit))
         (rounded (* (signum scaled) (floor (+ (abs scaled) 1/2)))))
    (and (= (sql-numeric-rational output) (* rounded unit))
         (= (sql-numeric-scale output) (max 0 target-scale)))))

(rove:deftest numeric-typmod-casts
    (dolist (case '(("SELECT 1.245::numeric(5,2)::text" "1.25")
                    ("SELECT (-1.245)::numeric(5,2)::text" "-1.25")
                    ("SELECT 1450::numeric(2,-2)::text" "1500")
                    ("SELECT 0.00123::numeric(2,4)::text" "0.0012")
                    ("SELECT 1::numeric(5,3)::text" "1.000")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (let ((data (cl-spec:result-data
               (cl-spec:run-property 'numeric-typmod-rounds-and-pads :seed 42))))
    (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(cl-spec:defproperty numeric-index-key-order
    ((left (range integer -1000000 1000000))
     (right (range integer -1000000 1000000))
     (left-scale (range integer 0 5))
     (right-scale (range integer 0 5)))
  (:about encode-index-key)
  (:trials (:normal 500))
  (let* ((a (make-sql-decimal left left-scale))
         (b (make-sql-decimal right right-scale))
         (expected (signum (compare-3way a b)))
         (actual (signum (key-compare (encode-index-key (list a) '(:numeric))
                                      (encode-index-key (list b) '(:numeric))))))
    (= expected actual)))

(rove:deftest numeric-storage-codecs
    (let* ((columns (vector (make-pg-column :name "n" :type :numeric :typmod -1)))
           (original (make-sql-decimal -120300 4))
           (tuple (form-heap-tuple (vector original) columns 1 0 0 0 1))
           (restored (aref (deform-heap-tuple tuple columns) 0)))
      (rove:ok (string= "-12.0300" (sql-numeric-output restored))))
  (let ((data (cl-spec:result-data (cl-spec:run-property 'numeric-index-key-order :seed 42))))
    (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest smallint-storage-codec
    (let* ((columns (vector (make-pg-column :name "n" :type :int2 :typmod -1)))
           (tuple (form-heap-tuple (vector -32768) columns 1 0 0 0 1)))
      (rove:ok (= -32768 (aref (deform-heap-tuple tuple columns) 0)))
      (rove:ok (key< (encode-index-key (list -32768) '(:int2))
                     (encode-index-key (list 32767) '(:int2))))))

(rove:deftest float-storage-codecs
    (let* ((columns (vector (make-pg-column :type :float4) (make-pg-column :type :float8)))
           (tuple (form-heap-tuple (vector -0.0f0 1.25d0) columns 1 0 0 0 1))
           (values (deform-heap-tuple tuple columns)))
      (rove:ok (= (sb-kernel:single-float-bits (aref values 0)) (sb-kernel:single-float-bits -0.0f0)))
      (rove:ok (= 1.25d0 (aref values 1)))))

(rove:deftest numeric-assignment-and-statistics
    (let* ((ast (first (parse-statements "CREATE TABLE n (v numeric(5,2))")))
           (definition (first (getf ast :columns)))
           (column (make-pg-column :name "v" :type (getf definition :type)
                                   :typmod (getf definition :typmod) :attnum 1))
           (assignment (analyze-subquery-assignment (analyze-expr '(:numeric-literal "1.245") nil) column))
           (one (sql-numeric-input "1.0"))
           (same (sql-numeric-input "1.00")))
      (rove:ok (string= "1.25" (sql-numeric-output (eval-subquery-assignment assignment #()))))
      (rove:ok (string= "\"v\" numeric(5,2)" (catalog-column-definition-sql column)))
      (rove:ok (analyze-statistics-value-equal-p one same))
      (rove:ok (= (analyze-statistics-value-hash one) (analyze-statistics-value-hash same)))
      (rove:ok (equalp (encode-index-key (list one) '(:numeric))
                       (encode-index-key (list same) '(:numeric))))
      (let* ((stats (make-pg-column-statistics :most-common-values (list (cons same 0.5d0))))
             (restored (column-statistics-from-sexp (column-statistics-to-sexp stats))))
        (rove:ok (string= "1.00" (sql-numeric-output (caar (pg-column-statistics-most-common-values restored))))))))

(rove:deftest numeric-logical-row-codec
    (let* ((columns (list (list :attnum 1 :name "n" :type :numeric :typmod -1 :notnull nil)))
           (change (make-logical-change :kind :insert :database-oid 1 :relation-oid 2
                                        :relation-name "n" :columns columns
                                        :new-values (vector (sql-numeric-input "12.0300"))))
           (decoded (logical-decode-change (logical-encode-change change)))
           (value (aref (logical-change-new-values decoded) 0)))
      (rove:ok (equal columns (logical-change-columns decoded)))
      (rove:ok (string= "12.0300" (sql-numeric-output value)))
      (rove:ok (= 1700 (pgoutput-type-oid :numeric)))
      (rove:ok (string= "12.0300" (pgoutput-value-text value :numeric)))))

(cl-spec:defproperty numeric-special-total-order
    ((left (range integer 0 3)) (right (range integer 0 3)))
  (:about sql-numeric-compare)
  (:trials (:normal 250))
  (let ((values (vector :numeric-negative-infinity 0 :numeric-positive-infinity :numeric-nan)))
    (= (signum (- left right))
       (signum (sql-numeric-compare (aref values left) (aref values right))))))

(rove:deftest numeric-special-values
    (dolist (case '(("SELECT 'NaN'::numeric::text" "NaN")
                    ("SELECT '-Inf'::numeric::text" "-Infinity")
                    ("SELECT ('NaN'::numeric/0)::text" "NaN")
                    ("SELECT (1.230/'Infinity'::numeric)::text" "0")
                    ("SELECT (1.230%'Infinity'::numeric)::text" "1.230")
                    ("SELECT ('Infinity'::numeric-'Infinity'::numeric)::text" "NaN")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (rove:ok (equal '((t t t))
                  (subquery-test-rows "SELECT '-Infinity'::numeric<0, 'Infinity'::numeric<'NaN'::numeric, 'NaN'::numeric='NaN'::numeric"))))

(rove:deftest width-bucket-expressions
    (dolist (case '(("SELECT width_bucket(2,0,10,5)::text" "2")
                    ("SELECT width_bucket(8,10,0,5)::text" "2")
                    ("SELECT width_bucket(0.3::numeric,0,1,10)::text" "4")
                    ("SELECT width_bucket(0::float8,'-1e308'::float8,'1e308'::float8,10)::text" "6")
                    ("SELECT width_bucket('b'::text,ARRAY['a','b','b','c'])::text" "3")
                    ("SELECT width_bucket('b'::varchar,ARRAY['a','b'])::text" "2")
                    ("SELECT width_bucket('b'::name,ARRAY['a','b'])::text" "2")
                    ("SELECT width_bucket('b'::text,'{a,b}')::text" "2")
                    ("SELECT width_bucket('b','{a,b}')::text" "2")
                    ("SELECT (width_bucket(NULL,NULL) IS NULL)::text" "true")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (dolist (case '(("SELECT width_bucket(1,0,1,0)" "2201G")
                  ("SELECT width_bucket(1,1,1,2)" "2201G")
                  ("SELECT width_bucket(1,0,1,2147483647)" "22003")
                  ("SELECT width_bucket('a'::text,ARRAY['a',NULL])" "22004")))
    (let ((condition (handler-case (progn (subquery-test-rows (first case)) nil)
                       (pg-error (error) error))))
      (rove:ok (and condition (string= (pg-error-sqlstate condition) (second case))) (first case)))))

(rove:deftest width-bucket-array-numeric-expressions
    (dolist (case '(("SELECT width_bucket(2, ARRAY[1,2,2,4])::text" "3")
                    ("SELECT width_bucket(2.5, ARRAY[1,2,3])::text" "2")
                    ("SELECT width_bucket(1, ARRAY[]::int4[])::text" "0")
                    ("SELECT width_bucket(2, '[0:2]={1,2,3}'::int4[])::text" "2")
                    ("SELECT ARRAY[1,2.5]::text" "{1,2.5}")
                    ("SELECT array_length('{{1,2},{3,4}}'::int4[],2)::text" "2")
                    ("SELECT '{{1,2},{3,4}}'::int4[]::text" "{{1,2},{3,4}}")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (dolist (case '(("SELECT width_bucket(1, ARRAY[1,NULL])" "22004")
                  ("SELECT width_bucket(1, '{{1,2},{3,4}}'::int4[])" "2202E")))
    (let ((condition (handler-case (progn (subquery-test-rows (first case)) nil)
                       (pg-error (error) error))))
      (rove:ok (and condition (string= (pg-error-sqlstate condition) (second case))) (first case)))))

(rove:deftest width-bucket-array-scalar-expressions
    (dolist (case '(("SELECT width_bucket(true, ARRAY[false,true])::text" "2")
                    ("SELECT width_bucket('b'::varchar, '{a,b,c}'::varchar[])::text" "2")
                    ("SELECT width_bucket(NULL::name, ARRAY['a'::name])::text" nil)
                    ("SELECT width_bucket('2020-01-02'::date, ARRAY['2020-01-01'::date,'2020-01-02'::date])::text" "2")
                    ("SELECT width_bucket('12:00'::time, ARRAY['09:00'::time,'12:00'::time])::text" "2")
                    ("SELECT width_bucket('0/10'::pg_lsn, ARRAY['0/01'::pg_lsn,'0/10'::pg_lsn])::text" "2")
                    ("SELECT width_bucket(2::oid, ARRAY[1,2,3])::text" "2")
                    ("SELECT width_bucket(2, '1 2 3'::int2vector)::text" "2")
                    ("SELECT '1 2'::int2vector::int2[]::text" "[0:1]={1,2}")
                    ("SELECT '1 2'::int2vector::int4[]::text" "[0:1]={1,2}")
                    ("SELECT ARRAY[1,2]::int2vector::int2[]::text" "{1,2}")
                    ("SELECT ARRAY[1,2]::int2vector::text" "1 2")
                    ("SELECT (ARRAY[1,2]::int2vector = '1 2'::int2vector)::text" "false")
                    ("SELECT (ARRAY[1,2]::int2vector > '1 2'::int2vector)::text" "true")
                    ("SELECT (ARRAY[]::int2vector = ''::int2vector)::text" "false")
                    ("SELECT width_bucket('12:00+00'::timetz, ARRAY['09:00+00'::timetz,'12:00+00'::timetz])::text" "1")
                    ("SELECT width_bucket('2020-01-02'::timestamp, ARRAY['2020-01-01'::timestamp,'2020-01-02'::timestamp])::text" "2")
                    ("SELECT width_bucket('2020-01-02 00:00+00'::timestamptz, ARRAY['2020-01-01 00:00+00'::timestamptz,'2020-01-02 00:00+00'::timestamptz])::text" "2")
                    ("SELECT width_bucket('2 days'::interval, ARRAY['1 day'::interval,'2 days'::interval])::text" "2")
                    ("SELECT width_bucket(B'10', ARRAY[B'01',B'10'])::text" "2")
                    ("SELECT ARRAY[false,true]::text" "{f,t}")))
      (rove:ok (equal (list (list (or (second case) +null+))) (subquery-test-rows (first case))) (first case))))

(rove:deftest width-bucket-array-polymorphic-types
    (dolist (case '(("SELECT width_bucket(NULL::int2vector,NULL)" :null)
                    ("SELECT width_bucket('1 2'::int2vector,NULL)" :null)
                    ("SELECT width_bucket('1 2'::int2vector,ARRAY['1 2'::int2vector,'3 4'::int2vector])" 1)
                    ("SELECT width_bucket('1 2'::int2vector,'{\"1 2\",\"3 4\"}')" 1)
                    ("SELECT width_bucket('1 2'::int2vector,ARRAY[]::int2vector[])" 0)
                    ("SELECT ARRAY['1 2'::int2vector,'3 4'::int2vector]::text" "{\"1 2\",\"3 4\"}")
                    ("SELECT '{\"1 2\",\"3 4\"}'::int2vector[]::text" "{\"1 2\",\"3 4\"}")))
      (rove:ok (equalp (subquery-test-expected (list (list (second case))))
                       (subquery-test-rows (first case)))
               (first case)))
  (dolist (type +sql-array-types+)
    (let* ((argument (list :col 0 :type (cdr type)))
           (sql (format nil "SELECT width_bucket(NULL::~a,NULL)"
                        (typed-arg-name argument)))
           (condition (handler-case (progn (subquery-test-rows sql) nil)
                        (pg-error (error) error))))
      (rove:ok (and condition
                    (equal (pg-error-sqlstate condition) "42704")
                    (equal (pg-error-message condition)
                           (format nil "could not find array type for data type ~a"
                                   (typed-arg-name argument)))
                    (null (pg-error-detail condition))
                    (null (pg-error-hint condition)))
               sql))))

(rove:deftest width-bucket-compatible-string-order
    (let ((prefix (make-string 63 :initial-element #\a)))
      (dolist (case '(("name" "text" "1")
                      ("name" "varchar" "1")
                      ("text" "name" "1")
                      ("varchar" "name" "1")
                      ("text" "text" "0")
                      ("varchar" "text" "0")))
        (let ((sql (format nil
                           "SELECT width_bucket('~a'::~a, ARRAY['~ab'::~a])::text"
                           prefix (first case) prefix (second case))))
          (rove:ok (equal (list (list (third case))) (subquery-test-rows sql)) sql)))
      (dolist (case '(("name" "1") ("text" "0")))
        (let ((sql (format nil "SELECT width_bucket('~a'::~a, '{~ab}')::text"
                           prefix (first case) prefix)))
          (rove:ok (equal (list (list (second case))) (subquery-test-rows sql)) sql)))))

(rove:deftest temporal-array-sort-and-group-spill
    (let* ((directory (merge-pathnames
                       (format nil "pg-array-spill-~d-~d/" (sb-posix:getpid) (random 1000000))
                       (uiop:temporary-directory)))
           (*query-temp-directory* directory)
           (*query-work-memory-bytes* 128))
      (ensure-directories-exist directory)
      (unwind-protect
           (progn
             (rove:ok (equal (subquery-test-rows
                              "SELECT x::text,count(*)::text FROM (SELECT ARRAY['2 days'::interval] AS x UNION ALL SELECT ARRAY['1 day'::interval] UNION ALL SELECT ARRAY['1 day'::interval]) q GROUP BY x ORDER BY x")
                             '(("{\"1 day\"}" "2") ("{\"2 days\"}" "1"))))
             (rove:ok (equal (subquery-test-rows
                              "SELECT x::text,count(*)::text FROM (SELECT ARRAY['13:00+00'::timetz] AS x UNION ALL SELECT ARRAY['12:00+00'::timetz] UNION ALL SELECT ARRAY['12:00+00'::timetz]) q GROUP BY x ORDER BY x")
                             '(("{12:00:00+00}" "2") ("{13:00:00+00}" "1"))))
             (rove:ok (null (uiop:directory-files directory))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(rove:deftest numeric-basic-functions
    (dolist (case '(("SELECT abs(ALL -3)::text" "3")
                    ("SELECT abs(-12.030)::text" "12.030")
                    ("SELECT numeric_abs('-Infinity'::numeric)::text" "Infinity")
                    ("SELECT sign('-Infinity'::numeric)::text" "-1")
                    ("SELECT round(-1.25::numeric,1)::text" "-1.3")
                    ("SELECT round(1450::numeric,-2)::text" "1500")
                    ("SELECT trunc(-1.29::numeric,1)::text" "-1.2")
                    ("SELECT ceil(-1.2::numeric)::text" "-1")
                    ("SELECT ceiling(1.2::numeric)::text" "2")
                    ("SELECT floor(-1.2::numeric)::text" "-2")
                    ("SELECT round('NaN'::numeric)::text" "NaN")
                    ("SELECT abs((-3)::integer)::text" "3")
                    ("SELECT round(2.5::float8)::text" "2")
                    ("SELECT round(3.5::float8)::text" "4")
                    ("SELECT trunc((-1.9)::float8)::text" "-1")
                    ("SELECT sign('NaN'::float8)::text" "0")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case))))

(rove:deftest float-catalog-aliases
    (dolist (case '(("SELECT float4abs('-2.5')::text" "2.5")
                    ("SELECT float8abs(-2)::text" "2")
                    ("SELECT dround(2.5)::text" "2")
                    ("SELECT dtrunc(-1.9)::text" "-1")
                    ("SELECT dsqrt(9)::text" "3")
                    ("SELECT dexp(0)::text" "1")
                    ("SELECT dlog1(1)::text" "0")
                    ("SELECT dlog10(100)::text" "2")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case)))
               (first case)))
  (rove:ok (equal (list (list +null+)) (subquery-test-rows "SELECT dsqrt(NULL)")))
  (dolist (case '(("SELECT float4abs(1::float8)" "42883")
                  ("SELECT dsqrt(true)" "42883")
                  ("SELECT dsqrt(-1)" "2201F")
                  ("SELECT dlog1(0)" "2201E")
                  ("SELECT DISTINCT dround(x) FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY round(x)" "42P10")
                  ("SELECT DISTINCT round(x) FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY dround(x)" "42P10")))
    (let ((condition (handler-case (progn (subquery-test-rows (first case)) nil)
                       (pg-error (error) error))))
      (rove:ok (and condition (string= (pg-error-sqlstate condition) (second case)))
               (first case)))))

(rove:deftest numeric-basic-function-errors
    (dolist (case '(("SELECT sin(1 ORDER BY 1 USING OPERATOR(<))" "42809" "ORDER BY specified, but sin is not an aggregate function")
                    ("SELECT sin(1 ORDER BY 1 USING OPERATOR(a.b.<))" "42809" "ORDER BY specified, but sin is not an aggregate function")
                    ("SELECT width_bucket(DISTINCT ARRAY[1], NULL)" "42809" "DISTINCT specified, but width_bucket is not an aggregate function")
                    ("SELECT sin(1 ORDER BY 1 USING OPERATOR(pg_catalog.<))" "42809" "ORDER BY specified, but sin is not an aggregate function")
                    ("SELECT sin(1 ORDER BY 1 NULLS FIRST)" "42809" "ORDER BY specified, but sin is not an aggregate function")
                    ("SELECT sin(1 ORDER BY 1 USING <)" "42809" "ORDER BY specified, but sin is not an aggregate function")
                    ("SELECT sin(DISTINCT 'bad')" "42809" "DISTINCT specified, but sin is not an aggregate function")
                    ("SELECT sin('bad' ORDER BY 1)" "42809" "ORDER BY specified, but sin is not an aggregate function")
                    ("SELECT trunc(DISTINCT 'bad')" "42725" "function trunc(unknown) is not unique")
                    ("SELECT sum(DISTINCT 1) OVER ()" "0A000" "DISTINCT is not implemented for window functions")
                    ("SELECT sum(1 ORDER BY 1) OVER ()" "0A000" "aggregate ORDER BY is not implemented for window functions")
                    ("SELECT abs(DISTINCT NULL) OVER ()" "42809" "DISTINCT specified, but abs is not an aggregate function")
                    ("SELECT abs(DISTINCT NULL)" "42809" "DISTINCT specified, but abs is not an aggregate function")
                    ("SELECT abs(NULL ORDER BY NULL)" "42809" "ORDER BY specified, but abs is not an aggregate function")
                    ("SELECT abs(DISTINCT NULL ORDER BY NULL)" "42809" "DISTINCT specified, but abs is not an aggregate function")
                    ("SELECT abs(DISTINCT 1, 2)" "42883" "function abs(integer, integer) does not exist")
                    ("SELECT abs('-9223372036854775808'::bigint)"
                     "22003" "bigint out of range")
                    ("SELECT round(1::float8, 2)"
                     "42883" "function round(double precision, integer) does not exist")))
      (let ((condition (handler-case (progn (subquery-test-rows (first case)) nil)
                         (pg-error (error) error))))
        (rove:ok (and condition
                      (string= (pg-error-sqlstate condition) (second case))
                      (string= (pg-error-message condition) (third case)))
                 (first case)))))

(rove:deftest numeric-named-argument-errors
    (dolist (case '(("SELECT round(x => 1::numeric)" "42883"
                     "function round(x => numeric) does not exist")
                    ("SELECT round(1::numeric, x => 2)" "42883"
                     "function round(numeric, x => integer) does not exist")
                    ("SELECT round(x => NULL, y => '2')" "42883"
                     "function round(x => unknown, y => unknown) does not exist")
                    ("SELECT round(x => 1, 2)" "42601"
                     "positional argument cannot follow named argument")
                    ("SELECT round(x => 1, x => 2)" "42601"
                     "argument name \"x\" used more than once")
                    ("SELECT round(x => no_such_numeric_argument(), 2)" "42883"
                     "function no_such_numeric_argument() does not exist")
                    ("SELECT round(x => 1, x => no_such_numeric_argument())" "42883"
                     "function no_such_numeric_argument() does not exist")
                    ("SELECT numeric_abs(x => 1)" "42883"
                     "function numeric_abs(x => integer) does not exist")
                    ("SELECT numeric_add(1, x => 2)" "42883"
                     "function numeric_add(integer, x => integer) does not exist")
                    ("SELECT int4mod(1, x => 2)" "42883"
                     "function int4mod(integer, x => integer) does not exist")
                    ("SELECT atan2(1, x => 2)" "42883"
                     "function atan2(integer, x => integer) does not exist")
                    ("SELECT pi(x => 1)" "42883"
                     "function pi(x => integer) does not exist")
                    ("SELECT width_bucket(1, x => ARRAY[1,2])" "42883"
                     "function width_bucket(integer, x => integer[]) does not exist")
                    ("SELECT dsqrt(x => '4')" "42883"
                     "function dsqrt(x => unknown) does not exist")
                    ("SELECT pg_catalog.round(x => 1)" "42883"
                     "function pg_catalog.round(x => integer) does not exist")
                    ("SELECT pg_catalog.int4and(1, x => 2)" "42883"
                     "function pg_catalog.int4and(integer, x => integer) does not exist")))
      (let ((condition (handler-case (progn (subquery-test-rows (first case)) nil)
                         (pg-error (error) error))))
        (rove:ok (and condition
                      (string= (pg-error-sqlstate condition) (second case))
                      (string= (pg-error-message condition) (third case)))
                 (first case)))))

(rove:deftest numeric-named-argument-details
    (dolist (case '(("SELECT pi(x => 1)" "count")
                    ("SELECT round(x => 1, y => 2, z => 3)" "count")
                    ("SELECT width_bucket(x => 1)" "count")
                    ("SELECT numeric_add(x => 1)" "count")
                    ("SELECT pg_catalog.round(x => 1)" "names")
                    ("SELECT round(1, x => '2')" "names")
                    ("SELECT width_bucket(1, x => ARRAY[1,2])" "names")
                    ("SELECT atan2(1, x => 2)" "names")
                    ("SELECT int4mod(1, x => 2)" "names")))
      (let ((condition (handler-case (progn (subquery-test-rows (first case)) nil)
                         (pg-error (error) error)))
            (detail (if (string= (second case) "count")
                        "No function of that name accepts the given number of arguments."
                        "No function of that name accepts the given argument names.")))
        (rove:ok (and condition (string= (pg-error-sqlstate condition) "42883")
                      (equal (pg-error-detail condition) detail))
                 (first case)))))

(cl-spec:defproperty numeric-sqrt-perfect-squares
    ((root (range integer 0 1000000))
     (scale (range integer 0 5)))
  (:about sql-numeric-sqrt)
  (:trials (:normal 250))
  (let* ((input (make-sql-decimal (* root root) (* 2 scale)))
         (result (sql-numeric-sqrt input)))
    (= (sql-numeric-rational result) (/ root (expt 10 scale)))))

(cl-spec:defproperty numeric-sqrt-rounding-bound
    ((coefficient (range integer 1 1000000000))
     (scale (range integer 0 9)))
  (:about sql-numeric-sqrt)
  (:trials (:normal 500))
  (let* ((input (make-sql-decimal coefficient scale))
         (result (sql-numeric-sqrt input))
         (value (sql-numeric-rational result))
         (half-unit (/ 1 (* 2 (expt 10 (sql-numeric-scale result)))))
         (original (sql-numeric-rational input)))
    (and (<= (expt (max 0 (- value half-unit)) 2) original)
         (< original (expt (+ value half-unit) 2)))))

(rove:deftest numeric-sqrt-rounding-property
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'numeric-sqrt-rounding-bound :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest float-trigonometric-expressions
    (dolist (case '(("SELECT sind(30)::text" "0.5")
                    ("SELECT cosd(60)::text" "0.5")
                    ("SELECT sind(180)::text" "0")
                    ("SELECT cosd(180)::text" "-1")
                    ("SELECT tand(45)::text" "1")
                    ("SELECT cotd(45)::text" "1")
                    ("SELECT asind(0.5)::text" "30")
                    ("SELECT acosd(-0.5)::text" "120")
                    ("SELECT atand(1)::text" "45")
                    ("SELECT atan2d(1, -1)::text" "135")
                    ("SELECT cbrt(-8)::text" "-2")
                    ("SELECT dcbrt(-8)::text" "-2")
                    ("SELECT tanh('Infinity'::float8)::text" "1")
                    ("SELECT sinh(1000)::text" "Infinity")
                    ("SELECT cot(0)::text" "Infinity")
                    ("SELECT tand(90)::text" "Infinity")
                    ("SELECT sin('NaN'::float8)::text" "NaN")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case)))
               (first case)))
  (rove:ok (equal (list (list +null+)) (subquery-test-rows "SELECT sin(NULL)")))
  (rove:ok (not (analyzed-expression-volatile-p (list :func "sin" nil :type :float8)))))

(rove:deftest integer-bitwise-operators
    (dolist (case '(("SELECT (5 & 3)::text" "1")
                    ("SELECT (5 | 2 # 3)::text" "4")
                    ("SELECT (1 << 31)::text" "-2147483648")
                    ("SELECT (-8 >> 2)::text" "-2")
                    ("SELECT (~ 0::int2)::text" "-1")
                    ("SELECT int8xor(7, 3)::text" "4")
                    ("SELECT int2shl(1::int2, 15)::text" "-32768")
                    ("SELECT (@ -2 + 1)::text" "1")
                    ("SELECT (|/ 9)::text" "3")
                    ("SELECT (||/ 8)::text" "2")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case)))
               (first case))))

(rove:deftest numeric-prefix-operator-errors
    (dolist (operator '("@" "|/" "||/"))
      (let* ((sql (format nil "SELECT ~a NULL" operator))
             (expression (analyze-expr
                          (getf (first (getf (first (parse-statements sql)) :targets)) :expr) nil)))
        (rove:ok (and (eq (expr-type expression) :float8)
                      (eq (eval-expr expression nil) +null+)) sql)))
  (dolist (case '(("SELECT @ (-4)::numeric" :numeric "4")
                  ("SELECT |/ 4::numeric" :float8 "2")
                  ("SELECT ||/ 8::numeric" :float8 "2")))
    (let* ((expression (analyze-expr
                        (getf (first (getf (first (parse-statements (first case))) :targets)) :expr) nil)))
      (rove:ok (and (eq (expr-type expression) (second case))
                    (equal (value->text (eval-expr expression nil) (expr-type expression))
                           (third case)))
               (first case))))
  (let ((condition (handler-case
                       (progn (subquery-test-rows "SELECT |/ (-1)::numeric") nil)
                     (pg-error (error) error))))
    (rove:ok (and condition (equal (pg-error-sqlstate condition) "2201F"))))
  (dolist (operator '("@" "|/" "||/"))
    (dolist (case '(("true" "boolean") ("'x'::text" "text")
                    ("NULL::text" "text") ("ARRAY[1]" "integer[]")
                    ("'1'::oid" "oid")))
      (let* ((sql (format nil "SELECT ~a ~a" operator (first case)))
             (condition (handler-case (progn (subquery-test-rows sql) nil)
                          (pg-error (error) error))))
        (rove:ok (and condition
                      (equal (pg-error-sqlstate condition) "42883")
                      (equal (pg-error-message condition)
                             (format nil "operator does not exist: ~a ~a" operator (second case)))
                      (equal (pg-error-detail condition)
                             "No operator of that name accepts the given argument type.")
                      (equal (pg-error-hint condition)
                             "You might need to add an explicit type cast."))
                 sql)))
    ;; Operand analysis and unknown input coercion retain their own errors.
    (dolist (case '(("missing_prefix_operand" "42703")
                    ("'bad'::integer" "22P02") ("'bad'" "22P02")))
      (let* ((sql (format nil "SELECT ~a ~a" operator (first case)))
             (condition (handler-case (progn (subquery-test-rows sql) nil)
                          (pg-error (error) error))))
        (rove:ok (and condition (equal (pg-error-sqlstate condition) (second case))) sql)))))

(rove:deftest bitwise-operator-resolution-errors
    (dolist (operator '("<<" ">>"))
      (dolist (case '(("1::smallint" "42725" "smallint")
                      ("1::bigint" "42883" "bigint")))
        (let* ((sql (format nil "SELECT '1' ~a ~a" operator (first case)))
               (condition (handler-case (progn (subquery-test-rows sql) nil)
                            (pg-error (error) error))))
          (rove:ok (and condition
                        (equal (pg-error-sqlstate condition) (second case))
                        (equal (pg-error-message condition)
                               (format nil "~a: unknown ~a ~a"
                                       (if (equal (second case) "42725")
                                           "operator is not unique" "operator does not exist")
                                       operator (third case)))) sql)))
      (let* ((sql (format nil "SELECT '1' ~a 1::integer" operator))
             (expression (analyze-expr
                          (getf (first (getf (first (parse-statements sql)) :targets)) :expr) nil)))
        (rove:ok (and (eq (expr-type expression) :int4)
                      (= (eval-expr expression nil) (if (equal operator "<<") 2 0))) sql)))
  (dolist (case '(("SELECT ~ true" "42883" "~ boolean" t)
                  ("SELECT ~ '1'" "42725" "~ unknown" t)
                  ("SELECT ~ NULL" "42725" "~ unknown" t)))
    (let ((condition (handler-case (progn (subquery-test-rows (first case)) nil)
                       (pg-error (error) error))))
      (rove:ok (and condition
                    (equal (pg-error-sqlstate condition) (second case))
                    (equal (pg-error-message condition)
                           (concatenate 'string
                                        (if (equal (second case) "42725")
                                            "operator is not unique: " "operator does not exist: ")
                                        (third case)))
                    (equal (pg-error-detail condition)
                           (if (equal (second case) "42725")
                               "Could not choose a best candidate operator."
                               "No operator of that name accepts the given argument type."))
                    (equal (pg-error-hint condition)
                           (if (equal (second case) "42725")
                               "You might need to add explicit type casts."
                               "You might need to add an explicit type cast.")))
               (first case))))
  (dolist (operator '("&" "|" "#" "<<" ">>"))
    (dolist (case '(("true" "1" "42883" "boolean" "integer")
                    ("'1'" "true" "42883" "unknown" "boolean")
                    ("'1'" "'2'" "42725" "unknown" "unknown")))
      (let* ((sql (format nil "SELECT ~a ~a ~a" (first case) operator (second case)))
             (condition (handler-case (progn (subquery-test-rows sql) nil)
                          (pg-error (error) error))))
        (rove:ok (and condition
                      (equal (pg-error-sqlstate condition) (third case))
                      (equal (pg-error-message condition)
                             (format nil "~a~a ~a ~a"
                                     (if (equal (third case) "42725")
                                         "operator is not unique: " "operator does not exist: ")
                                     (fourth case) operator (fifth case)))
                      (equal (pg-error-detail condition)
                             (if (equal (third case) "42725")
                                 "Could not choose a best candidate operator."
                                 "No operator of that name accepts the given argument types."))
                      (equal (pg-error-hint condition)
                             "You might need to add explicit type casts."))
                 sql)))))

(rove:deftest numeric-sqrt-expressions
    (dolist (case '(("SELECT sqrt(2::numeric)::text" "1.414213562373095")
                    ("SELECT sqrt(4.00::numeric)::text" "2.000000000000000")
                    ("SELECT sqrt('Infinity'::numeric)::text" "Infinity")
                    ("SELECT sqrt('NaN'::numeric)::text" "NaN")
                    ("SELECT sqrt(4::float8)::text" "2")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (let ((data (cl-spec:result-data (cl-spec:run-property 'numeric-sqrt-perfect-squares :seed 42))))
    (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(cl-spec:defproperty numeric-exp-zero-scale
    ((scale (range integer 0 32)))
  (:about sql-numeric-exp)
  (:trials (:normal 100))
  (let ((result (sql-numeric-exp (make-sql-decimal 0 scale))))
    (and (= (sql-numeric-rational result) 1)
         (= (sql-numeric-scale result) (max 16 scale)))))

(cl-spec:defproperty numeric-exp-reciprocal
    ((coefficient (range integer -5000 5000)))
  (:about sql-numeric-exp)
  (:trials (:normal 150))
  (let* ((input (make-sql-decimal coefficient 3))
         (positive (sql-numeric-rational (sql-numeric-exp input)))
         (negative (sql-numeric-rational (sql-numeric-exp (sql-numeric-negate input)))))
    (< (abs (- (* positive negative) 1)) 1/10000000000000)))

(rove:deftest numeric-exp-reciprocal-property
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'numeric-exp-reciprocal :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest numeric-exp-expressions
    (dolist (case '(("SELECT exp(0::numeric)::text" "1.0000000000000000")
                    ("SELECT exp(1::numeric)::text" "2.7182818284590452")
                    ("SELECT exp((-1)::numeric)::text" "0.3678794411714423")
                    ("SELECT exp('-Infinity'::numeric)::text" "0")
                    ("SELECT exp('Infinity'::numeric)::text" "Infinity")
                    ("SELECT exp('NaN'::numeric)::text" "NaN")
                    ("SELECT exp(0::float8)::text" "1")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (let ((data (cl-spec:result-data (cl-spec:run-property 'numeric-exp-zero-scale :seed 42))))
    (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(cl-spec:defproperty numeric-ln-one
    ((scale (range integer 0 32)))
  (:about sql-numeric-ln)
  (:trials (:normal 100))
  (let ((result (sql-numeric-ln (make-sql-decimal (expt 10 scale) scale))))
    (and (zerop (sql-numeric-rational result))
         (= (sql-numeric-scale result) (max 16 scale)))))

(cl-spec:defproperty numeric-ln-exp-inverse
    ((coefficient (range integer -2000 2000)))
  (:about sql-numeric-ln)
  (:trials (:normal 150))
  (let* ((input (make-sql-decimal coefficient 3))
         (result (sql-numeric-ln (sql-numeric-exp input))))
    (< (abs (- (sql-numeric-rational result) (sql-numeric-rational input)))
       1/10000000000000)))

(rove:deftest numeric-ln-inverse-property
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'numeric-ln-exp-inverse :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest numeric-ln-expressions
    (dolist (case '(("SELECT ln(1::numeric)::text" "0.0000000000000000")
                    ("SELECT ln(2::numeric)::text" "0.6931471805599453")
                    ("SELECT ln('Infinity'::numeric)::text" "Infinity")
                    ("SELECT ln('NaN'::numeric)::text" "NaN")
                    ("SELECT ln(1::float8)::text" "0")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (let ((data (cl-spec:result-data (cl-spec:run-property 'numeric-ln-one :seed 42))))
    (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest numeric-log-expressions
    (dolist (case '(("SELECT log(100::numeric)::text" "2.0000000000000000")
                    ("SELECT log(2::numeric,8::numeric)::text" "3.0000000000000000")
                    ("SELECT log10(1000::numeric)::text" "3.0000000000000000")
                    ("SELECT log(100::float8)::text" "2")
                    ("SELECT log(2::numeric,'Infinity'::numeric)::text" "Infinity")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case))))

(cl-spec:defproperty numeric-power-small-integers
    ((base (range integer -10 10))
     (exponent (range integer 0 8)))
  (:about sql-numeric-power)
  (:trials (:normal 250))
  (= (sql-numeric-rational (sql-numeric-power base exponent))
     (expt base exponent)))

(rove:deftest numeric-power-expressions
    (dolist (case '(("SELECT (2^3)::text" "8")
                    ("SELECT (2^3^2)::text" "64")
                    ("SELECT power(2::numeric,3::numeric)::text" "8.0000000000000000")
                    ("SELECT pow(9::numeric,0.5::numeric)::text" "3.0000000000000000")
                    ("SELECT power('NaN'::numeric,0::numeric)::text" "1")
                    ("SELECT power(1::numeric,'NaN'::numeric)::text" "1")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (let ((data (cl-spec:result-data (cl-spec:run-property 'numeric-power-small-integers :seed 42))))
    (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(cl-spec:defproperty power-operator-resolution-diagnostic-fields
    ((invalid (member "true" "'x'::text" "NULL::text" "1::oid" "ARRAY[1]"))
     (invalid-left (member nil t))
     (other (member "2" "'bad'" "NULL")))
  (:about analyze-binop-expression)
  (:trials (:normal 50))
  (let* ((left (if invalid-left invalid other))
         (right (if invalid-left other invalid))
         (sql (format nil "SELECT ~a ^ ~a" left right))
         (statement (first (parse-statements sql)))
         (raw (getf (first (getf statement :targets)) :expr))
         (arguments (list (analyze-expr (third raw) nil)
                          (analyze-expr (fourth raw) nil)))
         (condition (handler-case (progn (subquery-test-rows sql) nil)
                      (pg-error (error) error))))
    (and condition
         (equal (pg-error-sqlstate condition) "42883")
         (equal (pg-error-message condition)
                (format nil "operator does not exist: ~a ^ ~a"
                        (if (in-unknown-constant-p (first arguments))
                            "unknown" (typed-arg-name (first arguments)))
                        (if (in-unknown-constant-p (second arguments))
                            "unknown" (typed-arg-name (second arguments)))))
         (equal (pg-error-detail condition)
                "No operator of that name accepts the given argument types.")
         (equal (pg-error-hint condition)
                "You might need to add explicit type casts."))))

(rove:deftest power-operator-resolution-diagnostics
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'power-operator-resolution-diagnostic-fields :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data)))
  ;; Analyze operands first; coerce unknowns only after a candidate resolves.
  (dolist (case '(("SELECT missing_power_operand ^ true" "42703"
                   "column \"missing_power_operand\" does not exist")
                  ("SELECT 'bad'::numeric ^ true" "22P02"
                   "invalid input syntax for type numeric: \"bad\"")
                  ("SELECT 'bad' ^ '2'" "22P02"
                   "invalid input syntax for type double precision: \"bad\"")
                  ("SELECT 0 ^ (-1)" "2201F"
                   "zero raised to a negative power is undefined")))
    (let ((condition (handler-case (progn (subquery-test-rows (first case)) nil)
                       (pg-error (error) error))))
      (rove:ok (and condition
                    (equal (pg-error-sqlstate condition) (second case))
                    (equal (pg-error-message condition) (third case)))
               (first case))))
  (dolist (sql '("SELECT DISTINCT x ^ 2 FROM (SELECT 4 AS x) s ORDER BY power(x,2)"
                 "SELECT DISTINCT power(x,2) FROM (SELECT 4 AS x) s ORDER BY x ^ 2"))
    (let ((condition (handler-case (progn (subquery-test-rows sql) nil)
                       (pg-error (error) error))))
      (rove:ok (and condition (equal (pg-error-sqlstate condition) "42P10")) sql))))

(cl-spec:defproperty trunc-unknown-resolution-ambiguity
    ((argument (member "'2'" "'bad'" "NULL"))
     (qualified (member nil t)))
  (:about analyze-basic-numeric-function)
  (:trials (:normal 20))
  (let* ((name (if qualified "pg_catalog.trunc" "trunc"))
         (sql (format nil "SELECT ~a(~a)" name argument))
         (condition (handler-case (progn (subquery-test-rows sql) nil)
                      (pg-error (error) error))))
    (and condition
         (equal (pg-error-sqlstate condition) "42725")
         (equal (pg-error-message condition)
                (format nil "function ~a(unknown) is not unique" name))
         (equal (pg-error-detail condition) "Could not choose a best candidate function.")
         (equal (pg-error-hint condition) "You might need to add explicit type casts."))))

(rove:deftest trunc-unknown-resolution
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'trunc-unknown-resolution-ambiguity :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data)))
  (dolist (case '(("SELECT trunc('2',1)::text" "2.0")
                  ("SELECT trunc('2'::float8)::text" "2")
                  ("SELECT trunc('2'::numeric)::text" "2")
                  ("SELECT round('2')::text" "2")))
    (rove:ok (equal (subquery-test-rows (first case)) (list (list (second case))))
             (first case))))

(cl-spec:defproperty numeric-trim-scale-preserves-value
    ((coefficient (range integer -10000000 10000000))
     (scale (range integer 0 10)))
  (:about sql-numeric-trim-scale)
  (:trials (:normal 250))
  (let* ((input (make-sql-decimal coefficient scale))
         (trimmed (sql-numeric-trim-scale input)))
    (and (= (sql-numeric-rational input) (sql-numeric-rational trimmed))
         (<= (sql-numeric-scale trimmed) scale)
         (or (zerop (sql-numeric-scale trimmed))
             (not (zerop (mod (sql-numeric-coefficient trimmed) 10)))))))

(rove:deftest numeric-scale-functions
    (dolist (case '(("SELECT scale(1.2300::numeric)::text" "4")
                    ("SELECT min_scale(1.2300::numeric)::text" "2")
                    ("SELECT trim_scale(1.2300::numeric)::text" "1.23")
                    ("SELECT min_scale(0.0000::numeric)::text" "0")
                    ("SELECT trim_scale(12.000::numeric)::text" "12")
                    ("SELECT trim_scale('NaN'::numeric)::text" "NaN")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (let ((data (cl-spec:result-data
               (cl-spec:run-property 'numeric-trim-scale-preserves-value :seed 42))))
    (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(cl-spec:defproperty numeric-div-rem-identity
    ((left (range integer -1000000 1000000))
     (right (range integer 1 10000)))
  (:about sql-numeric-div-trunc)
  (:trials (:normal 250))
  (let ((quotient (sql-numeric-div-trunc left right))
        (remainder (sql-numeric-arithmetic "%" left right)))
    (= left (+ (* (sql-numeric-rational quotient) right)
               (sql-numeric-rational remainder)))))

(cl-spec:defproperty decimal-div-rem-identity
    ((left (range integer -1000000 1000000))
     (right (range integer -10000 10000))
     (left-scale (range integer 0 5))
     (right-scale (range integer 0 5)))
  (:about sql-numeric-div-trunc)
  (:trials (:normal 250))
  (let* ((a (make-sql-decimal left left-scale))
         (b (make-sql-decimal (if (zerop right) 1 right) right-scale))
         (quotient (sql-numeric-div-trunc a b))
         (remainder (sql-numeric-arithmetic "%" a b)))
    (= (sql-numeric-rational a)
       (+ (* (sql-numeric-rational quotient) (sql-numeric-rational b))
          (sql-numeric-rational remainder)))))

(rove:deftest decimal-div-rem-property
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'decimal-div-rem-identity :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest numeric-catalog-helper-functions
    (dolist (case '(("SELECT numeric_eq('NaN','NaN')::text" "true")
                    ("SELECT numeric_ne(1,2)::text" "true")
                    ("SELECT numeric_gt('Infinity','1')::text" "true")
                    ("SELECT numeric_ge(1,1)::text" "true")
                    ("SELECT numeric_lt('-Infinity','NaN')::text" "true")
                    ("SELECT numeric_le(1,1)::text" "true")
                    ("SELECT numeric_cmp('NaN','Infinity')::text" "1")
                    ("SELECT numeric_inc(1.20)::text" "2.20")
                    ("SELECT numeric_smaller(1.0,1.000)::text" "1.000")
                    ("SELECT numeric_larger(1.0,1.000)::text" "1.000")
                    ("SELECT numeric_uminus('-Infinity')::text" "Infinity")
                    ("SELECT numeric_uplus(1.00)::text" "1.00")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case)))
               (first case)))
  (rove:ok (eq +null+ (eval-numeric-helper-function "numeric_eq" (list +null+ 1))))
  (rove:ok (not (analyzed-expression-volatile-p '(:func "numeric_inc" nil :type :numeric))))
  (let* ((call '(:func "numeric_inc" ((:const 1 :type :numeric :typmod -1))
                 :type :numeric :typmod -1))
         (folded (fold-column-default call)))
    (rove:ok (eq :const (first folded)))
    (rove:ok (= 2 (second folded)))))

(rove:deftest numeric-div-mod-functions
    (dolist (case '(("SELECT div(-7::numeric,2::numeric)::text" "-3")
                    ("SELECT numeric_div_trunc(7,2)::text" "3")
                    ("SELECT mod(-7,3)::text" "-1")
                    ("SELECT mod(7.50::numeric,2::numeric)::text" "1.50")
                    ("SELECT numeric_add(1.20::numeric,2::numeric)::text" "3.20")
                    ("SELECT numeric_sub(1::numeric,2::numeric)::text" "-1")
                    ("SELECT numeric_mul(1.20::numeric,2::numeric)::text" "2.40")
                    ("SELECT numeric_div(1::numeric,2::numeric)::text" "0.50000000000000000000")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (let ((data (cl-spec:result-data (cl-spec:run-property 'numeric-div-rem-identity :seed 42))))
    (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(cl-spec:defproperty integer-mod-alias-remainder
    ((left (range integer -32768 32767))
     (right (range integer -32768 32767))
     (width (range integer 0 2)))
  (:about eval-analyzed-function)
  (:trials (:normal 250))
  (let* ((name (nth width '("int2mod" "int4mod" "int8mod")))
         (type (nth width '(:int2 :int4 :int8)))
         (divisor (if (zerop right) 1 right))
         (result (eval-analyzed-function name (list left divisor) type)))
    (and (= left (+ (* (truncate left divisor) divisor) result))
         (< (abs result) (abs divisor))
         (or (zerop result) (= (signum result) (signum left))))))

(rove:deftest remainder-operator-resolution
    (dolist (case '(("SELECT 5 % '2'" :int4 "1")
                    ("SELECT '5' % 2::int2" :int2 "1")
                    ("SELECT 5::int8 % '2'" :int8 "1")
                    ("SELECT 5::int2 % 2::int8" :int8 "1")
                    ("SELECT '5.5' % 2::numeric" :numeric "1.5")
                    ("SELECT NULL % 2" :int4 nil)
                    ("SELECT '-9223372036854775808'::int8 % '-1'" :int8 "0")))
      (let* ((statement (first (parse-statements (first case))))
             (expression (analyze-expr (getf (first (getf statement :targets)) :expr) nil)))
        (rove:ok (and (eq (expr-type expression) (second case))
                      (equal (value->text (eval-expr expression nil) (expr-type expression))
                             (third case)))
                 (first case))))
  (dolist (case '(("SELECT '3' % '2'" "42725" "operator is not unique: unknown % unknown")
                  ("SELECT NULL % NULL" "42725" "operator is not unique: unknown % unknown")
                  ("SELECT NULL::boolean % 2" "42883" "operator does not exist: boolean % integer")
                  ("SELECT 2 % NULL::text" "42883" "operator does not exist: integer % text")
                  ("SELECT 2::float8 % 1" "42883" "operator does not exist: double precision % integer")
                  ("SELECT 'bad' % 2" "22P02" nil)))
    (let ((condition (handler-case
                         (progn (analyze-expr
                                 (getf (first (getf (first (parse-statements (first case))) :targets)) :expr)
                                 nil)
                                nil)
                       (pg-error (error) error))))
      (rove:ok (and condition (string= (pg-error-sqlstate condition) (second case))
                    (or (null (third case))
                        (and (equal (pg-error-message condition) (third case))
                             (equal (pg-error-detail condition)
                                    (if (string= (second case) "42725")
                                        "Could not choose a best candidate operator."
                                        "No operator of that name accepts the given argument types."))
                             (equal (pg-error-hint condition)
                                    "You might need to add explicit type casts."))))
               (first case)))))

(rove:deftest integer-catalog-resolution-details
    (dolist (case '(("SELECT int4mod('x',true)" "int4mod(unknown, boolean)")
                    ("SELECT pg_catalog.int4mod('x',true)" "pg_catalog.int4mod(unknown, boolean)")
                    ("SELECT int2mod(7,2)" "int2mod(integer, integer)")
                    ("SELECT int8and('x',true)" "int8and(unknown, boolean)")
                    ("SELECT int4mod()" "int4mod()")))
      (let ((condition (handler-case
                           (progn (subquery-test-rows (first case)) nil)
                         (pg-error (error) error))))
        (rove:ok (and condition (equal (pg-error-sqlstate condition) "42883")
                      (equal (pg-error-message condition)
                             (format nil "function ~a does not exist" (second case)))
                      (equal (pg-error-detail condition)
                             (if (equal (second case) "int4mod()")
                                 "No function of that name accepts the given number of arguments."
                                 "No function of that name accepts the given argument types."))
                      (equal (pg-error-hint condition)
                             (unless (equal (second case) "int4mod()")
                               "You might need to add explicit type casts.")))
                 (first case)))))

(rove:deftest numeric-resolution-unknown-signatures
    (dolist (case '(("SELECT mod('1','1')" "42725" "function mod(unknown, unknown) is not unique")
                    ("SELECT gcd(NULL,1::int2)" "42725" "function gcd(unknown, smallint) is not unique")
                    ("SELECT mod('x'::text,1)" "42883" "function mod(text, integer) does not exist")))
      (let ((condition (handler-case (progn (subquery-test-rows (first case)) nil)
                         (pg-error (error) error))))
        (rove:ok (and condition (equal (pg-error-sqlstate condition) (second case))
                      (equal (pg-error-message condition) (third case)))
                 (first case)))))

(rove:deftest numeric-function-resolution-diagnostics
    ;; C ParseFuncOrColumn: SQLSTATE, primary message, detail and hint.
    (dolist (case '(("SELECT gcd('2','4')" "42725" "gcd(unknown, unknown)" "ambiguous")
                    ("SELECT pg_catalog.lcm(NULL,1::int2)" "42725" "pg_catalog.lcm(unknown, smallint)" "ambiguous")
                    ("SELECT mod('x'::text,1)" "42883" "mod(text, integer)" "types")
                    ("SELECT scale('x'::text)" "42883" "scale(text)" "types")
                    ("SELECT pg_catalog.sin('x'::text)" "42883" "pg_catalog.sin(text)" "types")
                    ("SELECT atan2('x',true)" "42883" "atan2(unknown, boolean)" "types")
                    ("SELECT factorial(1.0)" "42883" "factorial(numeric)" "types")
                    ("SELECT div('x',true)" "42883" "div(unknown, boolean)" "types")
                    ("SELECT pg_catalog.numeric_eq('x',true)" "42883" "pg_catalog.numeric_eq(unknown, boolean)" "types")
                    ("SELECT width_bucket('x',true)" "42883" "width_bucket(unknown, boolean)" "types")
                    ("SELECT width_bucket(1,true,3,4)" "42883" "width_bucket(integer, boolean, integer, integer)" "types")
                    ("SELECT dsqrt(true)" "42883" "dsqrt(boolean)" "types")
                    ("SELECT pg_catalog.int4and('x',true)" "42883" "pg_catalog.int4and(unknown, boolean)" "types")
                    ("SELECT pi(1)" "42883" "pi(integer)" "count")
                    ("SELECT numeric_eq(1)" "42883" "numeric_eq(integer)" "count")
                    ("SELECT width_bucket(1)" "42883" "width_bucket(integer)" "count")
                    ("SELECT pg_catalog.cbrt()" "42883" "pg_catalog.cbrt()" "count")
                    ("SELECT round(x=>1)" "42883" "round(x => integer)" "names")
                    ("SELECT pg_catalog.random(min=>'x',max=>true)" "42883" "pg_catalog.random(min => unknown, max => boolean)" "types")
                    ("SELECT random(min=>'2',max=>'4')" "42725" "random(min => unknown, max => unknown)" "ambiguous")
                    ("SELECT pg_catalog.random(x=>1)" "42883" "pg_catalog.random(x => integer)" "count")
                    ("SELECT random(x=>1,y=>2)" "42883" "random(x => integer, y => integer)" "names")
                    ("SELECT random(1,min=>2)" "42883" "random(integer, min => integer)" "overlap")
                    ("SELECT pg_catalog.random(1,min=>2)" "42883" "pg_catalog.random(integer, min => integer)" "overlap")
                    ("SELECT random(1,min=>2,max=>3)" "42883" "random(integer, min => integer, max => integer)" "count")
                    ("SELECT setseed(x=>1)" "42883" "setseed(x => integer)" "names")
                    ("SELECT pg_catalog.setseed('x'::text)" "42883" "pg_catalog.setseed(text)" "types")))
      (destructuring-bind (sql state signature reason) case
        (let* ((condition (handler-case (progn (subquery-test-rows sql) nil)
                            (pg-error (error) error)))
               (ambiguous (equal reason "ambiguous"))
               (detail (cond (ambiguous "Could not choose a best candidate function.")
                             ((equal reason "count") "No function of that name accepts the given number of arguments.")
                             ((equal reason "names") "No function of that name accepts the given argument names.")
                             ((equal reason "overlap") "In the closest available match, an argument was specified both positionally and by name.")
                             (t "No function of that name accepts the given argument types.")))
               (hint (when (or ambiguous (equal reason "types"))
                       "You might need to add explicit type casts.")))
          (rove:ok (and condition
                        (equal (pg-error-sqlstate condition) state)
                        (equal (pg-error-message condition)
                               (format nil "function ~a ~a" signature
                                       (if ambiguous "is not unique" "does not exist")))
                        (equal (pg-error-detail condition) detail)
                        (equal (pg-error-hint condition) hint))
                   sql)))))

(rove:deftest numeric-resolution-preserves-argument-errors
    (dolist (case '(("SELECT pg_catalog.sin(no_such_numeric_argument())"
                     "42883" "function no_such_numeric_argument() does not exist")
                    ("SELECT pg_catalog.sin('x')" "22P02"
                     "invalid input syntax for type double precision: \"x\"")
                    ("SELECT pg_catalog.sin(sqrt(-1))" "2201F"
                     "cannot take square root of a negative number")
                    ("SELECT pg_catalog.random(min=>1,max=>no_such_numeric_argument())"
                     "42883" "function no_such_numeric_argument() does not exist")
                    ("SELECT pg_catalog.random(min=>no_such_numeric_argument(),1)"
                     "42883" "function no_such_numeric_argument() does not exist")))
      (let ((condition (handler-case (progn (subquery-test-rows (first case)) nil)
                         (pg-error (error) error))))
        (rove:ok (and condition
                      (equal (pg-error-sqlstate condition) (second case))
                      (equal (pg-error-message condition) (third case)))
                 (first case)))))

(rove:deftest integer-input-diagnostic-fields
    (dolist (width '(("int2" "smallint" -32768 32767)
                     ("int4" "integer" -2147483648 2147483647)
                     ("int8" "bigint" -9223372036854775808 9223372036854775807)))
      (destructuring-bind (type type-name minimum maximum) width
        (let* ((overflow (write-to-string (1+ maximum)))
               (inputs (append
                        (loop for input in (list "bad" "  bad  " "a\"b\\c"
                                                 (format nil "bad~%value") "" "1_"
                                                 (concatenate 'string overflow "x"))
                              collect (list input "22P02"))
                        (loop for input in (list overflow (write-to-string (1- minimum))
                                                 (format nil "  ~a  " overflow)
                                                 (concatenate 'string overflow "0x"))
                              collect (list input "22003")))))
          (dolist (input inputs)
            (destructuring-bind (text state) input
              (dolist (sql (list (format nil "SELECT '~a'::~a" text type)
                                 (format nil "SELECT ~aand('~a', 1::int2)" type text)
                                 (format nil "SELECT 1::~a & '~a'" type text)))
                (let ((condition (handler-case (progn (subquery-test-rows sql) nil)
                                   (pg-error (error) error))))
                  (rove:ok
                   (and condition
                        (equal (pg-error-sqlstate condition) state)
                        (equal (pg-error-message condition)
                               (if (equal state "22P02")
                                   (format nil "invalid input syntax for type ~a: \"~a\"" type-name text)
                                   (format nil "value \"~a\" is out of range for type ~a" text type-name)))
                        (null (pg-error-detail condition))
                        (null (pg-error-hint condition)))
                   sql)))))
          (rove:ok (equal (list (list (write-to-string minimum) (write-to-string maximum)))
                          (subquery-test-rows
                           (format nil "SELECT '~a'::~a::text, '~a'::~a::text"
                                   minimum type maximum type)))))))
  ;; Shift counts always use integer input, including smallint and bigint shifts.
  (dolist (sql '("SELECT 1::int2 << 'bad'" "SELECT int8shr(1, 'bad')"))
    (let ((condition (handler-case (progn (subquery-test-rows sql) nil)
                       (pg-error (error) error))))
      (rove:ok (and condition
                    (equal (pg-error-sqlstate condition) "22P02")
                    (equal (pg-error-message condition)
                           "invalid input syntax for type integer: \"bad\""))
               sql))))

(rove:deftest numeric-input-overflow-diagnostics
    ;; Storage bounds follow trailing-junk validation; exponent accumulation
    ;; overflows immediately, as set_var_from_str does in numeric.c.
    (dolist (input '("1e131072" "1e-16384" "0e-16384" "-1e-16384"
                     "1e1073741824" "0e1073741824" "1e-1073741824"
                     "1e1073741824junk" "1e1073741824_"))
      (let* ((sql (format nil "SELECT '~a'::numeric" input))
             (condition (handler-case (progn (subquery-test-rows sql) nil)
                          (pg-error (error) error))))
        (rove:ok (and condition
                      (equal (pg-error-sqlstate condition) "22003")
                      (equal (pg-error-message condition) "value overflows numeric format")
                      (null (pg-error-detail condition)) (null (pg-error-hint condition)))
                 sql)))
  (dolist (input '("1e131072junk" "1e-16384junk" "0e-16384junk"
                   "1e1073741823junk" "1e131072_"))
    (let* ((sql (format nil "SELECT '~a'::numeric" input))
           (condition (handler-case (progn (subquery-test-rows sql) nil)
                        (pg-error (error) error))))
      (rove:ok (and condition
                    (equal (pg-error-sqlstate condition) "22P02")
                    (equal (pg-error-message condition)
                           (format nil "invalid input syntax for type numeric: \"~a\"" input))
                    (null (pg-error-detail condition)) (null (pg-error-hint condition)))
               sql)))
  (rove:ok (equal '(("0" "16383"))
                  (subquery-test-rows
                   "SELECT '0e1073741823'::numeric::text, scale('1e-16383'::numeric)::text"))))

(rove:deftest numeric-float-input-diagnostics
    ;; float.c reports range errors for the recognized prefix before checking
    ;; trailing junk; syntax errors instead retain the complete original input.
    (dolist (case '(("SELECT float4abs('x')" "22P02" "real" "x")
                    ("SELECT float8abs('x')" "22P02" "double precision" "x")
                    ("SELECT float4abs('')" "22P02" "real" "")
                    ("SELECT float4abs(' 0x? ')" "22P02" "real" " 0x? ")
                    ("SELECT sin(' 1_2 ')" "22P02" "double precision" " 1_2 ")
                    ("SELECT sin('  x  ')" "22P02" "double precision" "  x  ")
                    ("SELECT float4abs('1e1000')" "22003" "real" "1e1000")
                    ("SELECT float8abs('1e1000')" "22003" "double precision" "1e1000")
                    ("SELECT sin('1e-1000')" "22003" "double precision" "1e-1000")
                    ("SELECT sin(' 1e1000 ')" "22003" "double precision" "1e1000")
                    ("SELECT sin(' 1e-1000 ')" "22003" "double precision" "1e-1000")
                    ("SELECT float4abs(' 1e1000 ')" "22003" "real" "1e1000")
                    ("SELECT float4abs(' 1e-1000 ')" "22003" "real" "1e-1000")
                    ("SELECT sin('a\"b')" "22P02" "double precision" "a\"b")
                    ("SELECT sin('1e1000junk')" "22003" "double precision" "1e1000")
                    ("SELECT float4abs('1e1000_')" "22003" "real" "1e1000")
                    ("SELECT sin('0x1p99999?')" "22003" "double precision" "0x1p99999")
                    ("SELECT float4abs(' 0x1p-99999? ')" "22003" "real" "0x1p-99999")
                    ("SELECT sin(' 1e-1000junk ')" "22003" "double precision" "1e-1000")
                    ("SELECT sin('12junk')" "22P02" "double precision" "12junk")
                    ("SELECT sin('1e+')" "22P02" "double precision" "1e+")
                    ("SELECT float4abs('0x1p-')" "22P02" "real" "0x1p-")
                    ("SELECT float4abs('0x1p2junk')" "22P02" "real" "0x1p2junk")
                    ("SELECT sin('1_2')" "22P02" "double precision" "1_2")
                    ("SELECT float4abs(' 1e40junk ')" "22003" "real" "1e40")))
      (destructuring-bind (sql state type input) case
        (let ((condition (handler-case (progn (subquery-test-rows sql) nil)
                           (pg-error (error) error))))
          (rove:ok (and condition
                        (equal (pg-error-sqlstate condition) state)
                        (equal (pg-error-message condition)
                               (if (equal state "22P02")
                                   (format nil "invalid input syntax for type ~a: \"~a\"" type input)
                                   (format nil "\"~a\" is out of range for type ~a" input type)))
                        (null (pg-error-detail condition)) (null (pg-error-hint condition)))
                   sql)))))

(rove:deftest numeric-float-incomplete-exponent-range-errors
    ;; strtod checks an overflowing mantissa even when the exponent is incomplete.
    (dolist (prefix '("" "0x"))
      (dolist (function '("sin" "float4abs"))
        (let* ((mantissa (concatenate 'string prefix "1"
                                      (make-string 400 :initial-element #\0)))
               (input (concatenate 'string mantissa (if (equal prefix "") "e+" "p+")))
               (sql (format nil "SELECT ~a('~a')" function input))
               (condition (handler-case (progn (subquery-test-rows sql) nil)
                            (pg-error (error) error))))
          (rove:ok (and condition
                        (equal (pg-error-sqlstate condition) "22003")
                        (equal (pg-error-message condition)
                               (format nil "\"~a\" is out of range for type ~a"
                                       mantissa (if (equal function "sin") "double precision" "real")))
                        (null (pg-error-detail condition)) (null (pg-error-hint condition)))
                   sql)))))

(cl-spec:defproperty numeric-resolution-diagnostic-fields
    ((qualified (member nil t))
     (reason (member :types :count :names :ambiguous)))
  (:about report-numeric-function-resolution-error)
  (:trials (:normal 50))
  (let* ((count-p (eq reason :count))
         (ambiguous-p (eq reason :ambiguous))
         (args (unless count-p (list (list :const "x" :type :text :typmod -1 :explicit-cast t))))
         (condition (handler-case
                        (report-numeric-function-resolution-error
                         "sin" args :schema (when qualified "pg_catalog")
                         :ambiguous-p ambiguous-p :named-p (eq reason :names))
                      (pg-error (error) error))))
    (and condition
         (equal (pg-error-sqlstate condition) (if ambiguous-p "42725" "42883"))
         (equal (pg-error-message condition)
                (format nil "function ~asin(~a) ~a"
                        (if qualified "pg_catalog." "") (if count-p "" "text")
                        (if ambiguous-p "is not unique" "does not exist")))
         (equal (pg-error-detail condition)
                (case reason
                  (:ambiguous "Could not choose a best candidate function.")
                  (:count "No function of that name accepts the given number of arguments.")
                  (:names "No function of that name accepts the given argument names.")
                  (t "No function of that name accepts the given argument types.")))
         (equal (pg-error-hint condition)
                (when (member reason '(:types :ambiguous))
                  "You might need to add explicit type casts.")))))

(rove:deftest integer-mod-catalog-aliases
    (dolist (case '(("int2mod" :int2 "-32768")
                    ("int4mod" :int4 "-2147483648")
                    ("int8mod" :int8 "-9223372036854775808")))
      (destructuring-bind (name type minimum) case
        (let* ((sql (format nil
                            "SELECT ~a('-7','3')::text, ~a('7','-3')::text, ~a('~a','-1')::text, ~a(NULL,'0')::text"
                            name name name minimum name))
               (statement (first (parse-statements (format nil "SELECT ~a('7','2')" name))))
               (call (analyze-expr (getf (first (getf statement :targets)) :expr) nil)))
          (rove:ok (equal (list (list "-1" "1" "0" +null+)) (subquery-test-rows sql)) name)
          (rove:ok (string= name (second call)) name)
          (rove:ok (eq type (expr-type call)) name)
          (rove:ok (not (analyzed-expression-volatile-p call)) name)
          (rove:ok (having-immutable-function-p name) name)
          (rove:ok (equal (list :const 1 :type type :typmod -1) (fold-column-default call)) name))
        (let ((condition (handler-case
                             (progn (subquery-test-rows (format nil "SELECT ~a('1','0')" name)) nil)
                           (pg-error (error) error))))
          (rove:ok (and condition (string= (pg-error-sqlstate condition) "22012")) name))))
  (dolist (case '(("SELECT int2mod(7,2)" "42883")
                  ("SELECT int4mod(7::int8,2)" "42883")
                  ("SELECT int8mod(7.0,2)" "42883")
                  ("SELECT int4mod(1)" "42883")
                  ("SELECT DISTINCT int4mod(x,2) FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY mod(x,2)" "42P10")
                  ("SELECT DISTINCT mod(x,2) FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY int4mod(x,2)" "42P10")))
    (let ((condition (handler-case (progn (subquery-test-rows (first case)) nil)
                       (pg-error (error) error))))
      (rove:ok (and condition (string= (pg-error-sqlstate condition) (second case))) (first case))))
  (let ((data (cl-spec:result-data
               (cl-spec:run-property 'integer-mod-alias-remainder :seed 42))))
    (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(cl-spec:defproperty numeric-gcd-divides-inputs
    ((left (range integer -1000000 1000000))
     (right (range integer -1000000 1000000))
     (scale (range integer 0 5)))
  (:about sql-numeric-gcd-lcm)
  (:trials (:normal 250))
  (let* ((a (make-sql-decimal left scale))
         (b (make-sql-decimal right scale))
         (result (sql-numeric-gcd-lcm a b nil))
         (multiple (sql-numeric-gcd-lcm a b t))
         (divisor (sql-numeric-coefficient result)))
    (and (= (sql-numeric-scale result) scale)
         (= (sql-numeric-scale multiple) scale)
         (not (minusp divisor))
         (= divisor (gcd left right))
         (= (* divisor (sql-numeric-coefficient multiple)) (abs (* left right)))
         (if (zerop divisor) (and (zerop left) (zerop right))
             (and (zerop (rem left divisor)) (zerop (rem right divisor)))))))

(rove:deftest numeric-gcd-lcm-factorial
    (dolist (case '(("SELECT gcd(24,18)::text" "6")
                    ("SELECT lcm(-24,18)::text" "72")
                    ("SELECT gcd(1.20::numeric,0.45::numeric)::text" "0.15")
                    ("SELECT lcm(1.20::numeric,0.45::numeric)::text" "3.60")
                    ("SELECT factorial(10)::text" "3628800")
                    ("SELECT factorial(0)::text" "1")
                    ("SELECT gcd('Infinity'::numeric,1)::text" "NaN")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (let ((data (cl-spec:result-data (cl-spec:run-property 'numeric-gcd-divides-inputs :seed 42))))
    (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest numeric-gcd-lcm-cancellation
    (dolist (lcm-p '(nil t))
      (let ((*backend* (make-backend)))
        (setf (backend-cancel-pending *backend*) t)
        (let ((condition (handler-case
                             (progn (sql-numeric-gcd-lcm (make-sql-decimal 55 0)
                                                         (make-sql-decimal 34 0) lcm-p)
                                    nil)
                           (pg-error (error) error))))
          (rove:ok (and condition (string= (pg-error-sqlstate condition) "57014")))
          (rove:ok (null (backend-cancel-pending *backend*)))))))

(rove:deftest numeric-factorial-cancellation
    ;; numeric_fac's loop has no iterations for 0, 1, or 2.
    (dolist (case '((0 "1") (1 "1") (2 "2")))
      (let ((*backend* (make-backend)))
        (setf (backend-cancel-pending *backend*) t)
        (rove:ok (string= (second case) (sql-numeric-output (sql-factorial (first case)))))
        (rove:ok (backend-cancel-pending *backend*))))
  ;; The first multiplication at 3 must notice and consume cancellation.
  (let ((*backend* (make-backend)))
    (setf (backend-cancel-pending *backend*) t)
    (let ((condition (handler-case (progn (sql-factorial 3) nil)
                       (pg-error (error) error))))
      (rove:ok (and condition (string= (pg-error-sqlstate condition) "57014")))
      (rove:ok (null (backend-cancel-pending *backend*))))))

(cl-spec:defproperty numeric-rounding-is-symmetric
    ((coefficient (range integer -10000000 10000000))
     (input-scale (range integer 0 6))
     (target-scale (range integer -4 6)))
  (:about sql-numeric-round-or-trunc)
  (:trials (:normal 250))
  (let* ((positive (make-sql-decimal (abs coefficient) input-scale))
         (negative (sql-numeric-negate positive)))
    (every (lambda (round-p)
             (let ((a (sql-numeric-round-or-trunc positive target-scale round-p))
                   (b (sql-numeric-round-or-trunc negative target-scale round-p)))
               (and (= (sql-numeric-rational a) (- (sql-numeric-rational b)))
                    (= (sql-numeric-scale a) (max 0 target-scale)))))
           '(t nil))))

(rove:deftest numeric-rounding-property
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'numeric-rounding-is-symmetric :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest numeric-special-properties-and-indexes
    (let ((data (cl-spec:result-data (cl-spec:run-property 'numeric-special-total-order :seed 42)))
          (values (list :numeric-negative-infinity -1 0 1 :numeric-positive-infinity :numeric-nan)))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (loop for rest on values while (rest rest)
            do (rove:ok (key< (encode-index-key (list (first rest)) '(:numeric))
                              (encode-index-key (list (second rest)) '(:numeric)))))))

(cl-spec:defproperty float4-text-roundtrip
    ((coefficient (range integer -1000000 1000000))
     (scale (range integer 0 5)))
  (:about sql-float-input)
  (:trials (:normal 250))
  (let* ((text (sql-numeric-output (make-sql-decimal coefficient scale)))
         (value (sql-float-input text :float4))
         (restored (sql-float-input (sql-float-output value :float4) :float4)))
    (= (sb-kernel:single-float-bits value) (sb-kernel:single-float-bits restored))))

(cl-spec:defproperty julian-calendar-roundtrip
    ((julian (range integer 0 2147483493)))
  (:about sql-julian-to-date)
  (:trials (:normal 500))
  (multiple-value-bind (year month day) (sql-julian-to-date julian)
    (= julian (sql-date-to-julian year month day))))

(rove:deftest julian-calendar-known-dates
    (dolist (case '((2000 1 1 2451545) (1970 1 1 2440588) (0 1 1 1721060)
                    (-4713 11 24 0)))
      (destructuring-bind (year month day julian) case
        (rove:ok (= julian (sql-date-to-julian year month day)))
        (rove:ok (equal (list year month day) (multiple-value-list (sql-julian-to-date julian))))))
  (let ((data (cl-spec:result-data (cl-spec:run-property 'julian-calendar-roundtrip :seed 42))))
    (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest date-input-output
    (dolist (case '(("2000-01-01" 0 "2000-01-01")
                    ("1970-01-01" -10957 "1970-01-01")
                    ("0001-01-01 BC" -730485 "0001-01-01 BC")
                    ("infinity" 2147483647 "infinity")))
      (let ((value (sql-date-input (first case))))
        (rove:ok (= (second case) value))
        (rove:ok (string= (third case) (sql-date-output value))))))

(rove:deftest timetz-default-zone-uses-reference-date
    (let ((zone (sql-load-timezone "Europe/Warsaw")))
      (rove:ok (string= "12:00:00+01" (sql-timetz-output (sql-timetz-input "12:00" -1 zone 1704067200))))
      (rove:ok (string= "12:00:00+02" (sql-timetz-output (sql-timetz-input "12:00" -1 zone 1719835200))))))

(rove:deftest relative-datetime-uses-transaction-instant
    (let* ((zone (sql-load-timezone "Europe/Warsaw"))
           (unix (+ (* (+ (* (sql-tz-days-from-civil 2024 3 30) 86400) 84600) 1000000) 123456))
           (today (sql-timestamp-input "today" -1 t zone :mdy unix))
           (tomorrow (sql-timestamp-input "tomorrow" -1 t zone :mdy unix))
           (now (sql-timestamp-input "now" -1 t zone :mdy unix))
           (local-now (sql-timestamp-input "now" -1 nil zone :mdy unix)))
      (rove:ok (= now (- unix 946684800000000)))
      (rove:ok (string= "2024-03-31 00:30:00.123456" (sql-timestamp-output local-now nil)))
      (rove:ok (= (- tomorrow today) (* 23 3600000000)))
      (rove:ok (string= "00:30:00.123456+01"
                        (sql-timetz-output (sql-timetz-input "now" -1 zone nil unix))))))

(rove:deftest interval-generic-typmod-ddl-roundtrip
    (let* ((typmod (logior (ash 4 16) 3))
           (column (make-pg-column :name "i" :type :interval :typmod typmod))
           (sql (format nil "CREATE TABLE x (~a)" (catalog-column-definition-sql column)))
           (statement (first (parse-statements sql))))
      (rove:ok (= typmod (getf (first (getf statement :columns)) :typmod)))))

(rove:deftest temporal-check-canonical-replay
    (let* ((*backend* (make-backend))
           (column (make-pg-column :name "d" :type :date :typmod -1 :attnum 1))
           (table (make-pg-class :name "t" :columns (vector column)))
           (source (first (parse-statements "SELECT d = ANY('{01/02/2024,04/03/2024}')")))
           (raw (getf (first (getf source :targets)) :expr)))
      (setf (gethash "datestyle" (backend-settings *backend*)) "ISO, DMY")
      (let* ((cooked (analyze-expr raw table))
             (sql (catalog-canonical-expression-sql cooked table))
             (parsed (getf (first (getf (first (parse-statements (concatenate 'string "SELECT " sql))) :targets)) :expr)))
        (rove:ok (catalog-temporal-constant-p cooked))
        (setf (gethash "datestyle" (backend-settings *backend*)) "ISO, MDY")
        (let ((replayed (analyze-expr parsed table)))
          (rove:ok (eval-expr replayed (vector (sql-date-input "2024-02-01"))))
          (rove:ok (not (eval-expr replayed (vector (sql-date-input "2024-01-02")))))))))

(rove:deftest temporal-default-contract-distinguishes-cooked-values
    (let* ((*backend* (make-backend))
           (column (make-pg-column :name "d" :type :date :typmod -1 :attnum 1))
           (raw '(:const "01/02/2024")))
      (setf (pg-column-default-sql column) "'01/02/2024'"
            (gethash "datestyle" (backend-settings *backend*)) "ISO, DMY"
            (pg-column-default-expression column) (cook-column-default raw column))
      (let ((dmy-definition (catalog-column-definition-sql column)))
        (setf (gethash "datestyle" (backend-settings *backend*)) "ISO, MDY"
              (pg-column-default-expression column) (cook-column-default raw column))
        (rove:ok (not (string= dmy-definition (catalog-column-definition-sql column)))))))

(rove:deftest temporal-replay-retains-runtime-text-cast
    (let* ((*backend* (make-backend))
           (column (make-pg-column :name "d" :type :date :typmod -1 :attnum 1))
           (statement (first (parse-statements "SELECT COALESCE(('01/02/2024'::text)::date, DATE '2024-03-04')")))
           (raw (getf (first (getf statement :targets)) :expr)))
      (setf (gethash "datestyle" (backend-settings *backend*)) "ISO, DMY")
      (let* ((cooked (cook-column-default raw column))
             (sql (catalog-canonical-expression-sql cooked))
             (parsed (getf (first (getf (first (parse-statements (concatenate 'string "SELECT " sql))) :targets)) :expr)))
        (setf (gethash "datestyle" (backend-settings *backend*)) "ISO, MDY")
        (let ((replayed (cook-column-default parsed column)))
          (rove:ok (= (sql-date-input "2024-01-02") (eval-expr replayed #())))))))

(rove:deftest timestamptz-default-replay-retains-instant
    (let* ((*backend* (make-backend))
           (column (make-pg-column :name "z" :type :timestamptz :typmod -1 :attnum 1))
           (raw '(:const "01/02/2024 12:00:00")))
      (setf (gethash "datestyle" (backend-settings *backend*)) "ISO, DMY"
            (gethash "timezone" (backend-settings *backend*)) "Europe/Warsaw")
      (let* ((cooked (cook-column-default raw column))
             (original (eval-expr cooked #()))
             (sql (catalog-canonical-expression-sql cooked))
             (parsed (getf (first (getf (first (parse-statements (concatenate 'string "SELECT " sql))) :targets)) :expr)))
        (setf (gethash "datestyle" (backend-settings *backend*)) "ISO, MDY"
              (gethash "timezone" (backend-settings *backend*)) "UTC")
        (rove:ok (= original (eval-expr (cook-column-default parsed column) #()))))))

(rove:deftest timestamp-calendar-roundtrip
    (dolist (case '(("2000-01-01 00:00:00" 0 "2000-01-01 00:00:00")
                    ("1999-12-31 23:59:59.25" -750000 "1999-12-31 23:59:59.25")
                    ("0001-01-01 00:00:00 BC" -63113904000000000 "0001-01-01 00:00:00 BC")))
      (let ((value (sql-timestamp-input (first case) -1 nil)))
        (rove:ok (= (second case) value))
        (rove:ok (string= (third case) (sql-timestamp-output value nil))))))

(cl-spec:defproperty interval-month-equals-thirty-days
    ((months (range integer -1000000 1000000))
     (days (range integer -1000000 1000000))
     (micros (range integer -1000000000 1000000000)))
  (:about sql-interval-comparison-value)
  (:trials (:normal 500))
  (= (sql-interval-comparison-value (make-sql-interval months days micros))
     (sql-interval-comparison-value (make-sql-interval 0 (+ (* 30 months) days) micros))))

(cl-spec:defproperty timetz-key-compares-utc-then-zone
    ((offset (range integer -36000 36000)) (micros (range integer 0 50000000000)))
  (:about sql-timetz-comparison-values)
  (:trials (:normal 250))
  (multiple-value-bind (utc zone) (sql-timetz-comparison-values (make-sql-timetz micros offset))
    (and (= utc (+ micros (* offset 1000000))) (= zone offset))))

(cl-spec:defproperty timezone-utc-is-fixed
    ((second (range integer -100000000000 100000000000)))
  (:about sql-timezone-offset-at)
  (:trials (:normal 250))
  (= 0 (sql-timezone-offset-at (sql-load-timezone "UTC") second)))

(rove:deftest timezone-abbreviations-follow-transitions
    (let ((zone (sql-load-timezone "Europe/Warsaw")))
      (dolist (case '((2024 1 "CET") (2024 7 "CEST")
                      (2050 1 "CET") (2050 7 "CEST")))
        (destructuring-bind (year month abbreviation) case
          (rove:ok (string= abbreviation
                            (sql-timezone-abbreviation-at
                             zone (* 86400 (sql-tz-days-from-civil year month 15)))))))))

(cl-spec:defproperty iso-interval-hours-retain-value
    ((hours (range integer -100000 100000)))
  (:about sql-iso8601-interval-input)
  (:trials (:normal 250))
  (let ((interval (sql-iso8601-interval-input (format nil "PT~dH" hours))))
    (and (zerop (sql-interval-months interval))
         (zerop (sql-interval-days interval))
         (= (sql-interval-microseconds interval) (* hours 3600000000)))))

(cl-spec:defproperty interval-year-typmod-truncates
    ((months (range integer -100000 100000)) (days (range integer -1000 1000)))
  (:about sql-interval-adjust-typmod)
  (:trials (:normal 250))
  (let ((value (sql-interval-adjust-typmod
                (make-sql-interval months days 1234567) (logior (ash 4 16) #xffff))))
    (and (= (sql-interval-months value) (* 12 (truncate months 12)))
         (zerop (sql-interval-days value))
         (zerop (sql-interval-microseconds value)))))

(cl-spec:defproperty interval-scaling-identity
    ((months (range integer -10000 10000)) (days (range integer -10000 10000))
                                           (micros (range integer -100000000000 100000000000)))
  (:about sql-interval-scale)
  (:trials (:normal 250))
  (let ((result (sql-interval-scale (make-sql-interval months days micros) 1d0 nil)))
    (and (= months (sql-interval-months result))
         (= days (sql-interval-days result))
         (= micros (sql-interval-microseconds result)))))

(cl-spec:defproperty date-two-digit-year-window
    ((year (range integer 0 99)))
  (:about sql-date-input)
  (:trials (:normal 250))
  (let* ((expanded (+ year (if (< year 70) 2000 1900)))
         (short (format nil "01/02/~2,'0d" year))
         (long (format nil "~4,'0d-01-02" expanded)))
    (= (sql-date-input short nil :mdy) (sql-date-input long))))

(rove:deftest float-expression-casts
    (dolist (case '(("SELECT '1.25'::real::text" "1.25")
                    ("SELECT '-0'::double precision::text" "-0")
                    ("SELECT 'Infinity'::float8::text" "Infinity")
                    ("SELECT 'NaN'::float4::text" "NaN")
                    ("SELECT 2.5::float8::int" 2)))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case))))

(rove:deftest float-expression-arithmetic
    (dolist (case '(("SELECT ('1.25'::real + '2.5'::real)::text" "3.75")
                    ("SELECT ('NaN'::float8/0::float8)::text" "NaN")
                    ("SELECT ('Infinity'::float8-'Infinity'::float8)::text" "NaN")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (rove:ok (equal '((t t)) (subquery-test-rows "SELECT 'NaN'::float8='NaN'::float8, 'NaN'::float8>'Infinity'::float8"))))

(cl-spec:defproperty numeric-operator-unknown-coercion
    ((n (range integer -100 100)))
  (:about analyze-binop-expression)
  (:trials (:normal 100))
  (equal (subquery-test-rows (format nil "SELECT (1.20 + '~d')::text" n))
         (subquery-test-rows (format nil "SELECT (1.20 + ~d::numeric)::text" n))))

(rove:deftest numeric-operator-unknown-literals
    (dolist (case '(("SELECT (1.2 + '3')::text" "4.2")
                    ("SELECT ('3' - 1.2)::text" "1.8")
                    ("SELECT (1.2 * '3')::text" "3.6")
                    ("SELECT (1.2 / '3')::text" "0.40000000000000000000")
                    ("SELECT (1.2 % '3')::text" "1.2")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case)))
  (rove:ok (equal (list (list +null+ +null+)) (subquery-test-rows "SELECT NULL + 1.2, 1.2 + NULL")))
  (dolist (sql '("SELECT 1.2 + 'bad'" "SELECT 'bad' + 1.2"))
    (let ((condition (handler-case (progn (subquery-test-rows sql) nil)
                       (pg-error (error) error))))
      (rove:ok (and condition (string= (pg-error-sqlstate condition) "22P02")) sql)))
  (rove:ok (equal '((t)) (subquery-test-rows "SELECT 1.2 = '1.2'")))
  (let ((data (cl-spec:result-data (cl-spec:run-property 'numeric-operator-unknown-coercion :seed 42))))
    (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest numeric-operator-function-identity
    (dolist (sql '("SELECT DISTINCT @ x FROM (SELECT 4 AS x) s ORDER BY abs(x)"
                   "SELECT DISTINCT abs(x) FROM (SELECT 4 AS x) s ORDER BY @ x"
                   "SELECT DISTINCT |/ x FROM (SELECT 4 AS x) s ORDER BY sqrt(x)"
                   "SELECT DISTINCT sqrt(x) FROM (SELECT 4 AS x) s ORDER BY |/ x"
                   "SELECT DISTINCT ||/ x FROM (SELECT 8 AS x) s ORDER BY cbrt(x)"
                   "SELECT DISTINCT cbrt(x) FROM (SELECT 8 AS x) s ORDER BY ||/ x"
                   "SELECT DISTINCT int4and(x, 1) FROM (SELECT 4 AS x) s ORDER BY x & 1"
                   "SELECT DISTINCT x & 1 FROM (SELECT 4 AS x) s ORDER BY int4and(x, 1)"))
      (let ((condition (handler-case (progn (subquery-test-rows sql) nil)
                         (pg-error (error) error))))
        (rove:ok (and condition (string= (pg-error-sqlstate condition) "42P10")) sql)))
  (dolist (case '(("SELECT (@ -4)::text" "4")
                  ("SELECT (|/ 4)::text" "2")
                  ("SELECT (||/ 8)::text" "2")
                  ("SELECT int4and(7, 3)::text" "3")
                  ("SELECT int4not(1)::text" "-2")
                  ("SELECT int4abs(-4)::text" "4")))
    (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case))))

(rove:deftest numeric-literal-expressions
    (dolist (case '(("SELECT 1.50::text" "1.50")
                    ("SELECT (-1.25 + 2.00)::text" "0.75")
                    ("SELECT 1e-2::text" "0.01")))
      (rove:ok (equal (list (list (second case))) (subquery-test-rows (first case))) (first case))))

(rove:deftest generated-numeric-literal-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'numeric-literal-roundtrip :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))

(rove:deftest generated-numeric-text-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'numeric-text-retains-coefficient-and-scale :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))))
