;;;; SQL JSONB field and array extraction.
(in-package #:pg)

(defun json-test-evaluate (sql)
  (let* ((statement (first (parse-statements sql)))
         (target (first (getf statement :targets)))
         (expr (analyze-expr (getf target :expr) nil)))
    (values (eval-expr expr nil) (expr-type expr))))

(rove:deftest test-jsonb-extraction
    (flet ((check (sql expected type)
             (multiple-value-bind (value actual-type) (json-test-evaluate sql)
               (rove:ok (eq actual-type type))
               (rove:ok (equal (if (sql-jsonb-p value) (sql-jsonb-output value) value)
                               expected)))))
      (check "select '{\"a\": [1, \"two\", null]}'::jsonb -> 'a' -> -2" "\"two\"" :jsonb)
      (check "select '{\"a\": [1, \"two\", null]}'::jsonb -> 'a' ->> -2" "two" :text)
      (check "select '{\"a\":null}'::jsonb -> 'a'" "null" :jsonb)
      (check "select '{\"a\":null}'::jsonb ->> 'a'" +null+ :text)
      (check "select '{\"a\":1}'::jsonb -> 'missing'" +null+ :jsonb)
      (check "select '[1,2]'::jsonb -> -3" +null+ :jsonb)
      (check "select '1'::jsonb -> 0" "1" :jsonb)
      (check "select '1'::jsonb -> -1" "1" :jsonb)
      (check "select '\"abc\"'::jsonb ->> 0" "abc" :text)
      (check "select '{\"a\":1}'::jsonb -> 'a'::name" "1" :jsonb)
      (check "select '{\"a\":1}'::jsonb -> 'a'::\"char\"" "1" :jsonb)
      (check "select null::jsonb -> 'a'" +null+ :jsonb)
      (check "select '{}'::jsonb -> null" +null+ :jsonb)
      (check "select '{}'::jsonb -> null::int" +null+ :jsonb)
      (check "select '{\"a\": true}'::jsonb ->> 'a'" "true" :text)
      (check "select '{\"a\": {\"b\":1}}'::jsonb ->> 'a'" "{\"b\": 1}" :text)))

(rove:deftest test-jsonb-extraction-invalid-operator-types
    (dolist (case '(("select '{}'::jsonb -> 1::bigint" "42883" "operator does not exist: jsonb -> bigint")
                    ("select '{}'::jsonb -> true" "42883" "operator does not exist: jsonb -> boolean")
                    ("select '{}' -> 0" "42725" "operator is not unique: unknown -> integer")
                    ("select '{}' -> 'a'" "42725" "operator is not unique: unknown -> unknown")))
      (let ((condition (handler-case (progn (json-test-evaluate (first case)) nil)
                         (pg-error (error) error))))
        (rove:ok (and condition
                      (string= (pg-error-sqlstate condition) (second case))
                      (string= (pg-error-message condition) (third case)))))))

(rove:deftest test-json-source-preserving-extraction
    (flet ((check (sql expected type)
             (multiple-value-bind (value actual-type) (json-test-evaluate sql)
               (rove:ok (eq actual-type type))
               (rove:ok (equal value expected)))))
      (check "select '{ \"a\" : 1, \"a\" : [ 2, 3 ] }'::json -> 'a'" "[ 2, 3 ]" :json)
      (check "select '{ \"a\" : 1, \"a\" : [ 2, 3 ] }'::json -> 'a' ->> -1" "3" :text)
      (check "select '{\"a\":1.00e+2}'::json ->> 'a'" "1.00e+2" :text)
      (check "select '{\"a\":\"hi\\nthere\"}'::json -> 'a'" "\"hi\\nthere\"" :json)
      (check "select '{\"a\":\"hi\\nthere\"}'::json ->> 'a'" (format nil "hi~%there") :text)
      (check "select '{\"a\":null}'::json -> 'a'" "null" :json)
      (check "select '{\"a\":null}'::json ->> 'a'" +null+ :text)
      (check "select '[1,2]'::json -> -3" +null+ :json)
      (check "select '1'::json -> 0" +null+ :json)
      (check "select '{} '::json -> null" +null+ :json)))

(rove:deftest test-json-extraction-escapes-and-large-numbers
    (let ((slash (string (code-char 92))))
      (let ((json (format nil "{\"~au0061\": \"~au0062\", \"n\":1e999999}" slash slash)))
        (rove:ok (equal (sql-json-extract json "a" t) "b"))
        (rove:ok (equal (sql-json-extract json "n" t) "1e999999")))
      (dolist (bad '("u0000" "ud800" "udc00"))
        (let* ((json (format nil "{\"other\":\"~a~a\"}" slash bad))
               (condition (handler-case (progn (sql-json-extract json "missing" nil) nil)
                            (pg-error (error) error))))
          (rove:ok (and condition
                        (string= (pg-error-sqlstate condition)
                                 (if (string= bad "u0000") "22P05" "22P02"))))))))

(rove:deftest test-json-extraction-unicode-error-details
    (let ((slash (string (code-char 92))))
      (dolist (case '(("u0000" "22P05" "unsupported Unicode escape sequence"
                       "\\u0000 cannot be converted to text.")
                      ("ud800" "22P02" "invalid input syntax for type json"
                       "Unicode low surrogate must follow a high surrogate.")
                      ("udc00" "22P02" "invalid input syntax for type json"
                       "Unicode low surrogate must follow a high surrogate.")
                      ("ud800\\ud800" "22P02" "invalid input syntax for type json"
                       "Unicode high surrogate must not follow a high surrogate.")))
        (let* ((json (format nil "{\"a\":\"~a~a\"}" slash (first case)))
               (condition (handler-case (progn (sql-json-extract json "x" nil) nil)
                            (pg-error (error) error))))
          (rove:ok (and condition
                        (string= (pg-error-sqlstate condition) (second case))
                        (string= (pg-error-message condition) (third case))
                        (string= (pg-error-detail condition) (fourth case))))))))

(rove:deftest test-json-path-extraction
    (flet ((check (sql expected type)
             (multiple-value-bind (value actual-type) (json-test-evaluate sql)
               (rove:ok (eq actual-type type))
               (rove:ok (equal (if (sql-jsonb-p value) (sql-jsonb-output value) value)
                               expected)))))
      (dolist (kind '("json" "jsonb"))
        (check (format nil "select '{\"a\":[1, {\"b\":2}]}'::~a #>> '{a,-1,b}'" kind) "2" :text)
        (check (format nil "select '[10,20]'::~a #>> array['  +01']" kind) "20" :text)
        (check (format nil "select '[10,20]'::~a #>> array['1 ']" kind) +null+ :text)
        (check (format nil "select '1'::~a #> '{0}'" kind) +null+ (if (string= kind "json") :json :jsonb))
        (check (format nil "select 'null'::~a #>> '{}'" kind) +null+ :text)
        (check (format nil "select '\"abc\"'::~a #>> '{}'" kind) "abc" :text)
        (check (format nil "select '{\"a\":1}'::~a #> array['a',null]" kind) +null+
               (if (string= kind "json") :json :jsonb))
        (check (format nil "select '{\"a\":{\"b\":3}}'::~a #>> array[['a','b']]" kind) "3" :text))
      (check "select ' { \"a\" : [1, 2] } '::json #> '{a}'" "[1, 2]" :json)
      (check "select '[1,2]'::jsonb #> '{}'" "[1, 2]" :jsonb)
      (check "select '  1.00e+2  '::json #>> '{}'" "  1.00e+2" :text)
      (check "select '  null  '::json #>> '{}'" +null+ :text)
      (check "select '  null  '::json #> '{}'" "  null" :json)
      (check "select '{\"a\":1}'::json #>> array['a'::varchar]" "1" :text)
      (check "select '{\"a\":1}'::jsonb #>> array['a'::name]" "1" :text)))

(rove:deftest test-json-path-operator-resolution
    (dolist (case '(("select '{}' #> '{a}'" "42725")
                    ("select '{}'::jsonb #> array[1]" "42883")
                    ("select '{}'::json #> 'not an array'" "22P02")))
      (let ((condition (handler-case (progn (json-test-evaluate (first case)) nil)
                         (pg-error (error) error))))
        (rove:ok (and condition (string= (pg-error-sqlstate condition) (second case)))))))

(rove:deftest test-json-path-duplicate-prefixes
    (rove:ok (equal (json-test-evaluate
                     "select '{\"a\":{\"b\":1},\"a\":{}}'::json #> '{a,b}'")
                    "1"))
  (rove:ok (equal (json-test-evaluate
                   "select '{\"a\":[1,2],\"a\":[3,4,5]}'::json #>> '{a,-1}'")
                  "4"))
  (rove:ok (eq +null+ (json-test-evaluate
                       "select '{\"x\":[1,2,3],\"a\":[4,5]}'::json #>> '{a,-1}'")))
  (rove:ok (equal "5" (json-test-evaluate
                       "select '{\"x\":[1,2],\"a\":[4,5,6]}'::json #>> '{a,-1}'"))))

(rove:deftest test-json-path-deep-document
    (let* ((depth 10000)
           (json (concatenate 'string (make-string depth :initial-element #\[)
                              "1" (make-string depth :initial-element #\])))
           (path (make-array depth :initial-element "0")))
      (rove:ok (equal (sql-json-path-extract json path nil t) "1"))))
