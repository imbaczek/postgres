;;;; protocol-tests.lisp -- migrated unit regressions; run with pg-unit-tests.
(in-package #:pg)

(rove:deftest test-swank-arguments
    (multiple-value-bind (host port pgdata control-port control-socket
                          swank-port swank-bind-address)
        (parse-args '("--swank-port=4512" "--swank-bind-address=0.0.0.0"))
      (declare (ignore pgdata control-port control-socket))
      (rove:ok (string= host "127.0.0.1") "default SQL host is unchanged")
      (rove:ok (= port 5433) "default SQL port is unchanged")
      (rove:ok (= swank-port 4512) "Swank port accepts --option=value")
      (rove:ok (string= swank-bind-address "0.0.0.0")
               "Swank bind address accepts --option=value"))
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port swank-bind-address)
      (parse-args '("--swank-bind-address" "0.0.0.0"))
    (declare (ignore host port pgdata control-port control-socket))
    (rove:ok (null swank-port) "Swank bind address alone does not enable listener")
    (rove:ok (string= swank-bind-address "0.0.0.0")
             "Swank bind address is preserved without a listener"))
  (rove:ok (handler-case
               (progn (parse-args '("--swank-port=")) nil)
             (error () t))
           "empty Swank port is rejected")
  (rove:ok (handler-case
               (progn (parse-args '("--swank-port")) nil)
             (error () t))
           "missing Swank port is rejected")
  (rove:ok (handler-case
               (progn (parse-args '("--swank-port" "65536")) nil)
             (error () t))
           "out-of-range Swank port is rejected")
  (rove:ok (handler-case
               (progn (parse-args '("--swank-bind-address=")) nil)
             (error () t))
           "empty Swank bind address is rejected")
  (rove:ok (handler-case
               (progn (parse-args '("--swank-bind-address")) nil)
             (error () t))
           "missing Swank bind address is rejected"))

(rove:deftest test-replication-slot-retention-option-parsing
    (multiple-value-bind (host port pgdata control-port control-socket
                          swank-port bind-address temp-limit arena-limit slot-limit)
        (parse-args '("--max-slot-wal-keep-bytes=4096"))
      (declare (ignore host port pgdata control-port control-socket swank-port
                       bind-address temp-limit arena-limit))
      (rove:ok (= slot-limit 4096)
               "slot retention option accepts a finite byte cap"))
  (multiple-value-bind (host port pgdata control-port control-socket
                        swank-port bind-address temp-limit arena-limit slot-limit)
      (parse-args '("--max-slot-wal-keep-bytes" "-1"))
    (declare (ignore host port pgdata control-port control-socket swank-port
                     bind-address temp-limit arena-limit))
    (rove:ok (null slot-limit)
             "slot retention option uses -1 to disable the cap"))
  (rove:ok (handler-case
               (progn (parse-args '("--max-slot-wal-keep-bytes" "-2")) nil)
             (error () t))
           "slot retention option rejects negative values other than -1"))
