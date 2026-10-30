;;;; String operations used by Patroni's WAL status expression.
(in-package #:pg-interop-tests)

(deftest text-concat-precedence-types-and-null
    (dolist (example '(("SELECT 'x'||1+2" "x3")
                       ("SELECT 1+2||'x'" "3x")
                       ("SELECT 'a'||'b'||'c'" "abc")
                       ("SELECT 'x'||true" "xtrue")
                       ("SELECT 'x'||'0/1'::pg_lsn" "x0/00000001")
                       ("SELECT 'λ'||'🙂'" "λ🙂")
                       ("SELECT 'x'||'λ'::name" "xλ")))
      (ok (test-row-equal-p (list (vector (second example))) (derived-query-rows (first example)))))
  (ok (test-row-equal-p (list (vector pg::+null+)) (derived-query-rows "SELECT NULL||'x'")))
  (ok (test-row-equal-p '(#(t)) (derived-query-rows "SELECT 'x'||1+2 = 'x3'")))
  (ok (equal "42883" (sqlstate-of (derived-query-rows "SELECT 1||2")))))

(deftest substr-adjusts-negative-start-and-counts-characters
    (dolist (example '(("SELECT substr('abc',1,2)" "ab")
                       ("SELECT substr('abc',0,2)" "a")
                       ("SELECT substr('abc',-2,4)" "a")
                       ("SELECT substr('abc',-2,2)" "")
                       ("SELECT substr('abc',2)" "bc")
                       ("SELECT substr('abc',-2)" "abc")
                       ("SELECT substr('aλ🙂é',2,2)" "λ🙂")
                       ("SELECT substr('abc',2,2147483647)" "bc")
                       ("SELECT substr('abc',2147483647,2147483647)" "")
                       ("SELECT substr('abc',-2147483648,2147483647)" "")
                       ("SELECT substr('abc','2','1')" "b")
                       ("SELECT substr('abc',2::smallint,1::smallint)" "b")))
      (ok (test-row-equal-p (list (vector (second example))) (derived-query-rows (first example))))))

(deftest string-expressions-preserve-integer-arithmetic-errors
    (dolist (sql '("SELECT substr('abc',2147483647+1)"
                   "SELECT 'x'||(2147483647+1)"
                   "SELECT 9223372036854775807::bigint+1"
                   "SELECT -('-2147483648'::int)"
                   "SELECT -2147483648 / -1"))
      (ok (equal "22003" (sqlstate-of (derived-query-rows sql)))))
  (ok (test-row-equal-p '(#("x-1")) (derived-query-rows "SELECT 'x'||(-5 % 2)")))
  (ok (test-row-equal-p '(#(1 -1)) (derived-query-rows "SELECT 5 % -2, -5 % -2")))
  (ok (equal "0A000" (sqlstate-of (derived-query-rows "SELECT '{}'::text[]||'x'")))))

(deftest substr-strict-null-overload-and-error-contract
    (dolist (sql '("SELECT substr(NULL,1,-1)" "SELECT substr('a',NULL)"
                   "SELECT substr('a',1,NULL)"))
      (ok (test-row-equal-p (list (vector pg::+null+)) (derived-query-rows sql))))
  (dolist (example '(("SELECT substr('a',1,-1)" "22011")
                     ("SELECT substr(1,1)" "42883")
                     ("SELECT substr('a',1::bigint)" "42883")
                     ("SELECT substr('a','bad')" "22P02")))
    (ok (equal (second example) (sqlstate-of (derived-query-rows (first example)))))))

(deftest substr-result-metadata-and-patroni-prefix
    (let* ((plan (pg::plan-select-query
                  (first (pg::parse-statements "SELECT substr('abcdef'::varchar(6),1,2)"))))
           (column (pg::target->result-column
                    (first (getf (pg::query-plan-prepared plan) :targets)))))
      (ok (= 25 (pg::result-column-type-oid column)))
      (ok (= -1 (pg::result-column-typlen column)))
      (ok (string= "substr" (pg::result-column-name column))))
  (ok (test-row-equal-p '(#("x00000001"))
                        (derived-query-rows "SELECT 'x'||pg_catalog.substr('000000010000000000000001',1,8)"))))

(cl-spec:defproperty substr-partitions-characters-without-loss
    ((size (range integer 0 128)) (position (range integer 0 256)))
  (:about pg::eval-substr-function)
  (:trials (:normal 200))
  (let* ((text (coerce (loop for index below size collect (char "aλ🙂é" (mod index 4))) 'string))
         (cut (min size position)))
    (string= text
             (concatenate 'string (pg::eval-substr-function (list text 1 cut))
                          (pg::eval-substr-function (list text (1+ cut)))))))

(deftest generated-substr-character-partitions
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'substr-partitions-characters-without-loss :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
