;;;; Native receiver telemetry and C scalar-composite SQL behavior.
(in-package #:pg-interop-tests)

(defun call-with-receiver-monitor (function)
  (let* ((pg::*physical-follow-runtime*
           (pg::make-physical-follow-runtime :host "127.0.0.1" :port 5432 :pin "private_pin"))
         (pg::*physical-follow-statistics* nil)
         (pg::*physical-follow-last-end-lsn* 0)
         (pg::*physical-receive-store*
           (pg::%make-physical-store :timeline 1 :written-lsn 300 :flushed-lsn 200)))
    (pg::physical-follow-monitor-start pg::*physical-follow-runtime* 100)
    (pg::physical-follow-monitor-connected pg::*physical-follow-runtime*)
    (funcall function)))

(deftest receiver-absent-is-one-null-composite-row
    (let ((pg::*physical-follow-runtime* nil))
      (ok (equalp (list (vector pg::+null+))
                  (derived-query-rows "SELECT pg_stat_get_wal_receiver()")))
      (ok (equalp (list (make-array 15 :initial-element pg::+null+))
                  (derived-query-rows "SELECT * FROM pg_catalog.pg_stat_get_wal_receiver()")))
      (ok (equalp '(#(0)) (derived-query-rows
                           "SELECT count(*) FROM pg_stat_get_wal_receiver() WHERE pid IS NOT NULL")))))

(deftest receiver-snapshot-keeps-progress-and-source-end-distinct
    (call-with-receiver-monitor
     (lambda ()
       (pg::physical-follow-monitor-message pg::*physical-follow-runtime* 500 10)
       (ok (equalp '(#("streaming" 100 300 200 500 1 :sql-null))
                   (derived-query-rows
                    "SELECT status,receive_start_lsn,written_lsn,flushed_lsn,latest_end_lsn,
                          received_tli,slot_name FROM pg_stat_get_wal_receiver()")))
       (pg::physical-follow-monitor-message pg::*physical-follow-runtime* 400 20)
       (let ((snapshot (pg::physical-follow-statistics)))
         (ok (= 400 (getf snapshot :latest-end-lsn)))
         (ok (= 10 (getf snapshot :latest-end-time)))
         (ok (= 20 (getf snapshot :last-send-time))))
       (pg::physical-follow-monitor-stop pg::*physical-follow-runtime*)
       (ok (null (pg::physical-follow-statistics)))
       (ok (equalp (list (vector pg::+null+))
                   (derived-query-rows "SELECT pg_stat_get_wal_receiver()"))))))

(deftest receiver-from-materializes-one-snapshot
    (call-with-receiver-monitor
     (lambda ()
       (let ((original (symbol-function 'pg::wal-receiver-result)) (calls 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::wal-receiver-result)
                      (lambda () (incf calls) (funcall original)))
                (ok (= 15 (length (first (derived-query-rows
                                          "SELECT * FROM pg_stat_get_wal_receiver()")))))
                (ok (= 1 calls)))
           (setf (symbol-function 'pg::wal-receiver-result) original))))))

(deftest receiver-timestamp-output-and-disabled-archive-restore
    (dolist (example '((0 "2000-01-01 00:00:00+00")
                       (1 "2000-01-01 00:00:00.000001+00")
                       (-1 "1999-12-31 23:59:59.999999+00")
                       (1230000 "2000-01-01 00:00:01.23+00")))
      (ok (equal (second example) (pg::sql-timestamptz-output (first example)))))
  (ok (equal "" (pg::startup-normalize-setting "restore_command" "")))
  (ok (handler-case (progn (pg::startup-normalize-setting "restore_command" "cp %f %p") nil)
        (error () t))))

(deftest receiver-timestamps-use-signed-wire-values
    (ok (= -1 (pg::pg-client-i64 (octets '(255 255 255 255 255 255 255 255)) 0)))
  (ok (= 1 (pg::pg-client-i64 (octets '(0 0 0 0 0 0 0 1)) 0)))
  (ok (equal "42883" (sqlstate-of (derived-query-rows "SELECT pg_stat_get_wal_receiver(1)")))))

(deftest receiver-connection-descriptor-quotes-host-values
    (let* ((host (coerce (list #\a (code-char 39) (code-char 92) #\b #\Space #\c) 'string))
           (runtime (pg::make-physical-follow-runtime :host host :port 5432))
           (description (pg::physical-follow-connection-description runtime))
           (expected-prefix (coerce (list #\h #\o #\s #\t #\=
                                          (code-char 39) #\a (code-char 92) (code-char 39)
                                          (code-char 92) (code-char 92) #\b #\Space #\c
                                          (code-char 39) #\Space) 'string)))
      (ok (uiop:string-prefix-p expected-prefix description))
      (ok (search "replication=database sslmode=disable" description))))

(deftest receiver-reconnect-clears-previous-sender-details
    (call-with-receiver-monitor
     (lambda ()
       (pg::physical-follow-monitor-message pg::*physical-follow-runtime* 500 10)
       (pg::physical-follow-monitor-stop pg::*physical-follow-runtime*)
       (pg::physical-follow-monitor-start pg::*physical-follow-runtime* 200)
       (ok (equalp '(#("connecting" :sql-null :sql-null :sql-null 500))
                   (derived-query-rows
                    "SELECT status,sender_host,sender_port,conninfo,latest_end_lsn FROM pg_stat_get_wal_receiver()")))
       (pg::physical-follow-monitor-connected pg::*physical-follow-runtime*)
       (ok (equalp '(#("streaming" "127.0.0.1" 5432))
                   (derived-query-rows
                    "SELECT status,sender_host,sender_port FROM pg_stat_get_wal_receiver()"))))))
