(in-package #:pg-interop-apply-tests)

(defun call-with-standard-wire-fixture (function)
  (let* ((port (parse-integer (or (uiop:getenv "PG_INTEROP_TARGET_PORT")
                                  (error "isolated target port is required"))))
         (sql (pg::pg-client-connect "127.0.0.1" port "postgres"))
         (replication (pg::pg-client-connect "127.0.0.1" port "postgres" :replication-p t)))
    (unwind-protect
         (progn
           (pg::pg-client-query sql "CREATE TABLE standard_wire_rows (id integer PRIMARY KEY)")
           (pg::pg-client-query sql "CREATE PUBLICATION standard_wire_pub FOR TABLE standard_wire_rows")
           (pg::pg-client-query replication
                                "CREATE_REPLICATION_SLOT standard_wire_slot LOGICAL pgoutput (SNAPSHOT 'nothing')")
           (pg::pg-client-send-query
            replication "START_REPLICATION SLOT standard_wire_slot LOGICAL 0/0 (proto_version '4', publication_names 'standard_wire_pub')")
           (multiple-value-bind (kind payload) (pg::pg-client-frame (pg::pg-client-stream replication))
             (ok (char= kind #\W))
             (ok (equalp payload #(0 0 0))))
           (funcall function replication))
      (pg::pg-client-close replication)
      (let ((cleanup (pg::pg-client-connect "127.0.0.1" port "postgres" :replication-p t)))
        (unwind-protect
             (ignore-errors
              (pg::pg-client-query cleanup "DROP_REPLICATION_SLOT standard_wire_slot WAIT"))
          (pg::pg-client-close cleanup)))
      (ignore-errors (pg::pg-client-query sql "DROP PUBLICATION standard_wire_pub"))
      (ignore-errors (pg::pg-client-query sql "DROP TABLE standard_wire_rows"))
      (pg::pg-client-close sql))))

(deftest standard-copydone-keeps-pipelined-query
    (call-with-standard-wire-fixture
     (lambda (client)
       (let ((payload (pg::make-payload))
             (stream (pg::pg-client-stream client))
             (tags nil) (copydone 0))
         ;; One write makes the next command available to fd-stream read-ahead.
         (pg::payload-push-byte payload (char-code #\c))
         (pg::payload-push-u32 payload 4)
         (pg::payload-push-byte payload (char-code #\Q))
         (pg::payload-push-u32 payload 14)
         (pg::payload-push payload (pg::string->bytes "SELECT 42"))
         (pg::payload-push-byte payload 0)
         (write-sequence payload stream)
         (finish-output stream)
         (loop
           (multiple-value-bind (kind bytes) (pg::pg-client-frame stream)
             (case kind
               (#\c (incf copydone))
               (#\C (push (pg::pg-client-cstring bytes 0) tags))
               (#\d nil)
               (#\Z (ok (= (char-code #\I) (aref bytes 0))) (return))
               (otherwise (error "unexpected CopyDone response ~a" kind)))))
         (ok (= 1 copydone))
         (ok (equal '("COPY 0" "START_REPLICATION") (nreverse tags)))
         (let ((columns nil) (rows nil))
           (loop
             (multiple-value-bind (kind bytes) (pg::pg-client-frame stream)
               (case kind
                 (#\T (setf columns (pg::pg-client-read-column-description bytes)))
                 (#\D (push (pg::pg-client-read-data-row bytes columns) rows))
                 (#\C nil)
                 (#\Z (return))
                 (otherwise (error "unexpected pipelined query response ~a" kind)))))
           (ok (equalp '(#("42")) (nreverse rows))))))))

(deftest standard-copy-error-allows-next-query
    (call-with-standard-wire-fixture
     (lambda (client)
       (let ((stream (pg::pg-client-stream client))
             (sqlstate nil))
         (pg::write-message
          stream #\d (pg-interop-tests::standard-test-feedback 0 #xFFFFFFFFFFFFFFFF 0))
         (loop
           (multiple-value-bind (kind bytes) (pg::pg-client-frame stream)
             (case kind
               (#\d nil)
               (#\E (setf sqlstate (pg::pg-client-error-sqlstate
                                    (pg::pg-client-error-from-payload bytes))))
               (#\Z (ok (= (char-code #\I) (aref bytes 0))) (return))
               (otherwise (error "unexpected CopyBoth error response ~a" kind)))))
         (ok (equal "22023" sqlstate))
         (ok (equalp '(#("43")) (pg::pg-client-query client "SELECT 43")))))))
