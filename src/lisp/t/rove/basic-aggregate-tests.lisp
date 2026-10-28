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
                    ("SELECT width_bucket('12:00+00'::timetz, ARRAY['09:00+00'::timetz,'12:00+00'::timetz])::text" "1")
                    ("SELECT width_bucket('2020-01-02'::timestamp, ARRAY['2020-01-01'::timestamp,'2020-01-02'::timestamp])::text" "2")
                    ("SELECT width_bucket('2020-01-02 00:00+00'::timestamptz, ARRAY['2020-01-01 00:00+00'::timestamptz,'2020-01-02 00:00+00'::timestamptz])::text" "2")
                    ("SELECT width_bucket('2 days'::interval, ARRAY['1 day'::interval,'2 days'::interval])::text" "2")
                    ("SELECT width_bucket(B'10', ARRAY[B'01',B'10'])::text" "2")
                    ("SELECT ARRAY[false,true]::text" "{f,t}")))
      (rove:ok (equal (list (list (or (second case) +null+))) (subquery-test-rows (first case))) (first case))))

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
    (dolist (case '(("SELECT abs(-12.030)::text" "12.030")
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
    (dolist (case '(("SELECT abs('-9223372036854775808'::bigint)"
                     "22003" "bigint out of range")
                    ("SELECT round(1::float8, 2)"
                     "42883" "function round(double precision, integer) does not exist")))
      (let ((condition (handler-case (progn (subquery-test-rows (first case)) nil)
                         (pg-error (error) error))))
        (rove:ok (and condition
                      (string= (pg-error-sqlstate condition) (second case))
                      (string= (pg-error-message condition) (third case)))
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
