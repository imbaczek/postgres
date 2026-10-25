;;;; Fixed bit-string expression casts used by Patroni timeline extraction.
(in-package #:pg-interop-tests)

(deftest bit-input-and-explicit-resize
    (dolist (example '(("SELECT '101'::bit" "1")
                       ("SELECT '101'::\"bit\"" "101")
                       ("SELECT '101'::pg_catalog.bit" "101")
                       ("SELECT 'b101'::bit(5)" "10100")
                       ("SELECT 'xF'::bit(3)" "111")
                       ("SELECT ''::bit" "0")
                       ("SELECT ''::\"bit\"" "")
                       ("SELECT (B'1')::bit(3)" "100")
                       ("SELECT X'ff'" "11111111")
                       ("SELECT 1::bit(3)" "001")
                       ("SELECT (-1)::bit(40)" "1111111111111111111111111111111111111111")))
      (ok (equalp (list (vector (second example))) (derived-query-rows (first example))))))

(deftest bit-integer-casts-use-destination-signed-width
    (dolist (example '(("SELECT B'1'::int" 1)
                       ("SELECT 'x80000000'::bit(32)::int" -2147483648)
                       ("SELECT 'xFFFFFFFF'::bit(32)::int" -1)
                       ("SELECT 'xFFFFFFFF'::bit(32)::bigint" 4294967295)
                       ("SELECT 'x8000000000000000'::bit(64)::bigint" -9223372036854775808)
                       ("SELECT ''::\"bit\"::int" 0)))
      (ok (equalp (list (vector (second example))) (derived-query-rows (first example)))))
  (ok (equalp '(#(2147483647))
              (derived-query-rows
               "SELECT ('x'||substr('7FFFFFFF0000000000000001',1,8))::bit(32)::int"))))

(deftest integer-literal-width-does-not-bypass-bit-cast-overloads
    (dolist (example '(("SELECT 2147483648" 20)
                       ("SELECT 9223372036854775808" 1700)
                       ("SELECT -9223372036854775809" 1700)))
      (let* ((plan (pg::plan-select-query (first (pg::parse-statements (first example)))))
             (column (pg::target->result-column
                      (first (getf (pg::query-plan-prepared plan) :targets)))))
        (ok (= (second example) (pg::result-column-type-oid column)))))
  (ok (equal "42846" (sqlstate-of
                      (derived-query-rows "SELECT 9223372036854775808::bit(64)"))))
  (ok (equal "22026" (sqlstate-of (pg::sql-bit-input "2" 3))))
  (ok (equal "22P02" (sqlstate-of (pg::sql-bit-input "120" 3)))))

(deftest bit-modifier-numeric-token-validation
    (dolist (expression '("1.5" "-1.5" ".5" "1." "1e1" "1_0.5"))
      (ok (equal "22P02"
                 (sqlstate-of (derived-query-rows (format nil "SELECT NULL::bit(~a)" expression))))))
  (dolist (expression '("+8" "+1.5" "1.5,1+1"))
    (ok (equal "42601"
               (sqlstate-of (derived-query-rows (format nil "SELECT NULL::bit(~a)" expression))))))
  (ok (equalp (list (vector pg::+null+ pg::+null+))
              (derived-query-rows "SELECT NULL::bit(0x20),NULL::bit(1_0)")))
  (dolist (sql '("SELECT 1.5" "SELECT 1e1" "SELECT .5" "SELECT 1."
                 "SELECT 1 LIMIT 1.5" "SET statement_timeout TO 1.5"))
    (ok (equal "0A000" (sqlstate-of (derived-query-rows sql)))))
  (ok (equalp '(#(32 10 63 3))
              (derived-query-rows "SELECT 0x20,1_0,0o7_7,0b1_1")))
  (dolist (sql '("SELECT 1__0" "SELECT 123abc" "SELECT 0xG" "SELECT 1e+"))
    (ok (equal "42601" (sqlstate-of (derived-query-rows sql))))))

(deftest bit-cast-errors-and-null
    (dolist (example '(("SELECT '120'::bit(1)" "22P02")
                       ("SELECT 'xG'::bit(1)" "22P02")
                       ("SELECT NULL::bit(0)" "22023")
                       ("SELECT NULL::bit(-1)" "22023")
                       ("SELECT NULL::bit(83886081)" "22023")
                       ("SELECT NULL::bit(1,2)" "22023")
                       ("SELECT NULL::bit(foo)" "22P02")
                       ("SELECT '1'::bit(33)::int" "22003")
                       ("SELECT '1'::bit(65)::bigint" "22003")
                       ("SELECT 1::smallint::bit(8)" "42846")
                       ("SELECT B'1'::smallint" "42846")
                       ("SELECT B'1'+B'0'" "42883")
                       ("SELECT B'1'||B'0'" "0A000")))
      (ok (equal (second example) (sqlstate-of (derived-query-rows (first example))))))
  (ok (equalp (list (vector pg::+null+))
              (derived-query-rows "SELECT NULL::bit(83886080)"))))

(deftest bit-result-type-and-typmod
    (dolist (example '(("SELECT '101'::bit" 1) ("SELECT '101'::bit(32)" 32)
                       ("SELECT '101'::\"bit\"" -1)
                       ("SELECT ('101'::bit(3))::\"bit\"" -1)))
      (let* ((plan (pg::plan-select-query (first (pg::parse-statements (first example)))))
             (target (first (getf (pg::query-plan-prepared plan) :targets)))
             (column (pg::target->result-column target)))
        (ok (= 1560 (pg::result-column-type-oid column)))
        (ok (= -1 (pg::result-column-typlen column)))
        (ok (= (second example) (pg::expr-typmod (getf target :expr)))))))

(deftest bit-values-survive-comparison-and-spill
    (let ((directory (merge-pathnames (format nil "pg-bit-spill-~a/" (gensym))
                                      (uiop:temporary-directory))))
      (ensure-directories-exist directory)
      (unwind-protect
           (let ((pg::*query-temp-directory* directory) (pg::*query-work-memory-bytes* 1))
             (ok (equalp '(#("1") #("10") #("11"))
                         (derived-query-rows
                          "SELECT DISTINCT q.v FROM (SELECT B'10' AS v UNION ALL SELECT B'1' UNION ALL SELECT B'10' UNION ALL SELECT B'11') q ORDER BY v")))
             (ok (null (uiop:directory-files directory))))
        (uiop:delete-directory-tree directory :validate t :if-does-not-exist :ignore))))

(cl-spec:defproperty bit-casts-roundtrip-signed-int32
    ((value (range integer -2147483648 2147483647)) (width (range integer 1 31)))
  (:about pg::sql-bit-to-integer)
  (:trials (:normal 200))
  (and (= value (pg::sql-bit-to-integer (pg::sql-integer-to-bit value 32) 32))
       (= value (pg::sql-bit-to-integer (pg::sql-integer-to-bit value 64) 64))
       (= (logand value (1- (ash 1 width)))
          (pg::sql-bit-to-integer (pg::sql-integer-to-bit value width) 32))))

(deftest generated-bit-cast-roundtrips
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'bit-casts-roundtrip-signed-int32 :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
