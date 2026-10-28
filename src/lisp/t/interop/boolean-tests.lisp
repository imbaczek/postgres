;;;; Boolean analysis and executor short circuit, matching parse_coerce/execExprInterp.
(in-package #:pg-interop-tests)

(deftest boolean-three-valued-truth-tables
    (dolist (example '(("true" "true" t t) ("true" "false" nil t)
                       ("true" "NULL" :sql-null t) ("false" "true" nil t)
                       ("false" "false" nil nil) ("false" "NULL" nil :sql-null)
                       ("NULL" "true" :sql-null t) ("NULL" "false" nil :sql-null)
                       ("NULL" "NULL" :sql-null :sql-null)))
      (destructuring-bind (left right conjunction disjunction) example
        (ok (equalp (list (vector conjunction disjunction))
                    (derived-query-rows
                     (format nil "SELECT ~a AND ~a, ~a OR ~a" left right left right)))))))

(deftest boolean-unknown-input-and-analysis-errors
    (ok (equalp '(#(t nil t))
                (derived-query-rows "SELECT 'ye' AND true, 'of' OR false, NOT '0'")))
  (dolist (sql '("SELECT false AND 1" "SELECT true OR 1"
                 "SELECT NOT 1" "SELECT false AND 'true'::text"))
    (ok (equal "42804" (sqlstate-of (derived-query-rows sql)))))
  (ok (equal "22P02" (sqlstate-of (derived-query-rows "SELECT false AND 'bad'")))))

(deftest boolean-type-error-names-the-original-type
    (dolist (example '(("SELECT true AND pg_wal_replay_pause()" "void")
                       ("SELECT true OR 1::oid" "oid")))
      (let ((condition (handler-case (derived-query-rows (first example))
                         (pg::pg-error (condition) condition))))
        (ok (typep condition 'pg::pg-error))
        (when (typep condition 'pg::pg-error)
          (ok (equal "42804" (pg::pg-error-sqlstate condition)))
          (ok (search (format nil "not type ~a" (second example))
                      (pg::pg-error-message condition))))))
  (ok (equal "record" (pg::typed-arg-name '(:func "record_probe" nil :type :record)))))

(deftest smallint-casts-check-destination-range
    (dolist (example '(("SELECT 32767::bigint::smallint" 32767)
                       ("SELECT (-32768)::bigint::smallint" -32768)
                       ("SELECT 32766.5::float8::smallint" 32766)
                       ("SELECT 32766.5::numeric::smallint" 32767)
                       ("SELECT (-32768.5)::float8::smallint" -32768)))
      (ok (equalp (list (vector (second example))) (derived-query-rows (first example)))))
  (dolist (example '(("SELECT 9223372036854775807::bigint::smallint" "22003" "smallint out of range")
                     ("SELECT (-9223372036854775808)::bigint::smallint" "22003" "smallint out of range")
                     ("SELECT 1e30::numeric::smallint" "22003" "smallint out of range")
                     ("SELECT 32767.5::numeric::smallint" "22003" "smallint out of range")
                     ("SELECT (-32768.5)::numeric::smallint" "22003" "smallint out of range")
                     ("SELECT 32767.5::float8::smallint" "22003" "smallint out of range")
                     ("SELECT (-32769)::float4::smallint" "22003" "smallint out of range")
                     ("SELECT 1e30::float8::smallint" "22003" "smallint out of range")
                     ("SELECT 1e30::float4::smallint" "22003" "smallint out of range")
                     ("SELECT 'NaN'::numeric::smallint" "0A000" "cannot convert NaN to smallint")
                     ("SELECT '-Infinity'::numeric::smallint" "0A000" "cannot convert infinity to smallint")
                     ("SELECT 'NaN'::float8::smallint" "22003" "smallint out of range")
                     ("SELECT 'Infinity'::float4::smallint" "22003" "smallint out of range")))
    (let ((condition (handler-case (derived-query-rows (first example))
                       (pg::pg-error (condition) condition))))
      (ok (typep condition 'pg::pg-error))
      (when (typep condition 'pg::pg-error)
        (ok (equal (second example) (pg::pg-error-sqlstate condition)))
        (ok (equal (third example) (pg::pg-error-message condition)))))))

(deftest boolean-short-circuit-skips-recovery-only-function
    ;; On a primary this function raises 55000 if evaluated.
    (let ((pg::*physical-recovery-state* nil))
      (ok (equalp '(#(nil t))
                  (derived-query-rows
                   "SELECT pg_is_in_recovery() AND pg_is_wal_replay_paused(),
                         NOT pg_is_in_recovery() OR pg_is_wal_replay_paused()")))
      (dolist (sql '("SELECT true AND pg_is_wal_replay_paused()"
                     "SELECT false OR pg_is_wal_replay_paused()"
                     "SELECT NULL AND pg_is_wal_replay_paused()"
                     "SELECT NULL OR pg_is_wal_replay_paused()"))
        (ok (equal "55000" (sqlstate-of (derived-query-rows sql)))))))
