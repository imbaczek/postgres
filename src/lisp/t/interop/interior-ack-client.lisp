;;;; Scripted wire peer for logical-interior-ack.sh; no subscriber-side repair.
(require :asdf)
(let ((root (merge-pathnames "../../" (uiop:pathname-directory-pathname *load-truename*))))
  (asdf:load-asd (merge-pathnames "pg.asd" root))
  (asdf:load-system "pg"))
(in-package #:pg)

(defun interior-ack-transaction (client expected-id)
  "Group pgoutput frames under a deadline, excluding transport keepalives.
PG-CLIENT-FRAME reads individual transport frames; this groups B/R/I/C payloads
for exact comparison across publisher processes without applying or rewriting them."
  (sb-ext:with-timeout 10
    (let ((frames nil))
      (loop
        (multiple-value-bind (kind payload) (pg-client-frame (pg-client-stream client))
          (when (char= kind #\E) (error (pg-client-error-from-payload payload)))
          (assert (char= kind #\d))
          (when (= (aref payload 0) (char-code #\w))
            (let ((frame (subseq payload 25)))
              (push frame frames)
              (when (= (aref frame 0) (char-code #\C))
                (setf frames (nreverse frames))
                (assert (equal '(66 82 73 67) (mapcar (lambda (bytes) (aref bytes 0)) frames)))
                (assert (equalp (vector expected-id) (logical-pgoutput-read-tuple (third frames) 6)))
                (return frames)))))))))

(defun interior-ack-wait-confirmation (sql expected)
  "Wait for exact durable confirmation before the harness cuts publisher power.
PG-CLIENT-SEND-STANDBY-STATUS only flushes the socket, not the server slot image."
  (loop repeat 100
        for rows = (pg-client-query sql
                                    "SELECT confirmed_flush_lsn,restart_lsn FROM pg_replication_slots WHERE slot_name='interior_ack'")
        when (and rows (= expected (parse-replication-lsn (aref (first rows) 0))))
          return (parse-replication-lsn (aref (first rows) 1))
        do (sleep 0.02)
        finally (error "source did not persist confirmation ~a" expected)))

(defun interior-ack-start (client floor)
  (pg-client-send-query client
                        (format nil "START_REPLICATION SLOT interior_ack LOGICAL ~a (proto_version '1', publication_names 'interior_ack_pub')"
                                (wal-format-lsn floor)))
  (multiple-value-bind (kind payload) (pg-client-frame (pg-client-stream client))
    (assert (and (char= kind #\W) (equalp payload #(0 0 0))))))

(defun interior-ack-phase (phase port evidence-path)
  (let ((sql (pg-client-connect "127.0.0.1" port "postgres"))
        (replication nil))
    (unwind-protect
         (progn
           (setf replication (pg-client-connect "127.0.0.1" port "postgres" :replication-p t))
           (if (string= phase "capture")
               (progn
                 (pg-client-query replication
                                  "CREATE_REPLICATION_SLOT interior_ack LOGICAL pgoutput (SNAPSHOT 'nothing')")
                 (interior-ack-start replication 0)
                 (pg-client-query sql "INSERT INTO interior_ack_rows VALUES (7)")
                 (let* ((frames (interior-ack-transaction replication "7"))
                        (commit (fourth frames))
                        (start (pg-client-u64 commit 2))
                        (end (pg-client-u64 commit 10))
                        (interior (1+ start)))
                   (assert (< start interior end))
                   (pg-client-send-standby-status replication interior)
                   (assert (<= (interior-ack-wait-confirmation sql interior) start))
                   (with-open-file (output evidence-path :direction :output :if-exists :error)
                     (write (list :frames frames :start start :end end :interior interior)
                            :stream output :readably t))
                   (format t "~&interior-ack: captured start=~a interior=~a end=~a~%"
                           (wal-format-lsn start) (wal-format-lsn interior) (wal-format-lsn end))))
               (let* ((evidence (with-open-file (input evidence-path) (read input)))
                      (end (getf evidence :end))
                      (floor (if (string= phase "replay") (getf evidence :interior) end)))
                 (let ((restart (interior-ack-wait-confirmation sql floor)))
                   (assert (<= restart (if (string= phase "replay")
                                           (getf evidence :start) end))))
                 (interior-ack-start replication floor)
                 (cond
                   ((string= phase "replay")
                    (assert (equalp (getf evidence :frames) (interior-ack-transaction replication "7")))
                    (pg-client-send-standby-status replication end)
                    (interior-ack-wait-confirmation sql end)
                    (format t "~&interior-ack: exact transaction replayed after publisher SIGKILL~%"))
                   ((string= phase "confirmed")
                    (pg-client-query sql "INSERT INTO interior_ack_rows VALUES (8)")
                    (let ((frames (interior-ack-transaction replication "8")))
                      (assert (> (pg-client-u64 (fourth frames) 10) end)))
                    (format t "~&interior-ack: durable end acknowledgment suppressed replay~%"))
                   (t (error "unknown test phase ~s" phase))))))
      (when replication (pg-client-close replication))
      (pg-client-close sql))))

(destructuring-bind (phase port evidence) (uiop:command-line-arguments)
  (interior-ack-phase phase (parse-integer port) evidence))
