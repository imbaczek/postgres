;;;; C xlogfuncs.c / XLogFileName SQL contracts.
(in-package #:pg-interop-tests)

(deftest wal-filename-segment-boundaries
    (dolist (example '(("0/0" "000000010000000000000000")
                       ("0/FFFFFF" "000000010000000000000000")
                       ("0/1000000" "000000010000000000000001")
                       ("0/FFFFFFFF" "0000000100000000000000FF")
                       ("1/0" "000000010000000100000000")
                       ("FFFFFFFF/FFFFFFFF" "00000001FFFFFFFF000000FF")))
      (ok (equalp (list (vector (second example)))
                  (derived-query-rows
                   (format nil "SELECT pg_catalog.pg_walfile_name('~a')" (first example)))))))

(deftest wal-filename-strict-input-and-recovery
    (ok (equalp (list (vector pg::+null+))
                (derived-query-rows "SELECT pg_walfile_name(NULL)")))
  (dolist (sql '("SELECT pg_walfile_name(1)"
                 "SELECT pg_walfile_name('0/0'::text)"
                 "SELECT pg_walfile_name()"))
    (ok (equal "42883" (sqlstate-of (derived-query-rows sql)))))
  (ok (equal "22P02" (sqlstate-of (derived-query-rows "SELECT pg_walfile_name('bad')"))))
  (let ((pg::*physical-recovery-state* '(:timeline 1)))
    (let ((condition (handler-case
                         (derived-query-rows "SELECT pg_walfile_name('0/0')")
                       (pg::pg-error (condition) condition))))
      (ok (typep condition 'pg::pg-error))
      (when (typep condition 'pg::pg-error)
        (ok (equal "55000" (pg::pg-error-sqlstate condition)))
        (ok (equal "recovery is in progress" (pg::pg-error-message condition)))
        (ok (equal "pg_walfile_name() cannot be executed during recovery."
                   (pg::pg-error-hint condition)))))
    (ok (equalp (list (vector pg::+null+))
                (derived-query-rows "SELECT pg_walfile_name(NULL)")))))
