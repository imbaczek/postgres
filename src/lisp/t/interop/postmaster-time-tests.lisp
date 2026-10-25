;;;; C pg_postmaster_start_time returns one immutable TimestampTz per postmaster.
(in-package #:pg-interop-tests)

(deftest postmaster-time-is-typed-and-does-not-sample-the-query-clock
    (let ((pg::*postmaster-start-timestamp* 1230000))
      (ok (equalp '(#(1230000 1230000))
                  (derived-query-rows
                   "SELECT pg_postmaster_start_time(), pg_catalog.pg_postmaster_start_time()")))
      (let* ((plan (pg::plan-select-query
                    (first (pg::parse-statements "SELECT pg_postmaster_start_time()"))))
             (column (pg::target->result-column
                      (first (getf (pg::query-plan-prepared plan) :targets)))))
        (ok (= 1184 (pg::result-column-type-oid column))))
      (ok (equal "42883" (sqlstate-of
                          (derived-query-rows "SELECT pg_postmaster_start_time(NULL)"))))))

(deftest uninitialized-postmaster-time-fails-closed
    (let ((pg::*postmaster-start-timestamp* nil))
      (ok (equal "55000" (sqlstate-of
                          (derived-query-rows "SELECT pg_postmaster_start_time()"))))))
