(in-package #:pg-physical-tests)

(defun native-test-start-query (&optional (cursor 64) (base 32) (identity 123))
  (format nil "START_REPLICATION SLOT copy PHYSICAL ~a (native_format 'pg_lisp_wal_5', system_id '~d', timeline '1', base_lsn '~a', prefix_digest '14695981039346656037')"
          (pg::wal-format-lsn cursor) identity (pg::wal-format-lsn base)))

(deftest native-physical-start-requires-format-and-base
    (let ((command (pg::parse-replication-command (native-test-start-query))))
      (ok (eq :physical-start (pg::logical-replication-command-type command)))
      (ok (= 64 (pg::logical-replication-command-requested-lsn command)))
      (ok (equal "pg_lisp_wal_5" (pg::physical-stream-option command "native_format"))))
  (dolist (query '("START_REPLICATION PHYSICAL 0/0"
                   "START_REPLICATION SLOT copy PHYSICAL 0/0"
                   "START_REPLICATION SLOT copy PHYSICAL 0/0 (native_format 'other')"
                   "START_REPLICATION SLOT copy PHYSICAL 0/0 (native_format 'pg_lisp_wal_5', system_id '123', timeline '2', base_lsn '0/0')"
                   "START_REPLICATION SLOT copy PHYSICAL 0/0 (native_format 'pg_lisp_wal_5', system_id '123', timeline '1', timeline '1')"))
    (ok (intake-fails-p (lambda () (pg::parse-replication-command query))))))

(deftest native-physical-start-rejects-transaction-blocks
    (let ((pg::*physical-recovery-state* nil)
          (pg::*physical-recovery-failed-p* nil)
          (pg::*backend* (pg::make-backend :explicit t :state :in-progress)))
      (ok (equal "25001"
                 (recovery-state-of
                  (lambda ()
                    (pg::validate-replication-command-context
                     (pg::parse-replication-command (native-test-start-query)))))))))

(deftest native-physical-feedback-separates-positions
    (let ((previous (list :write 64 :flush 64 :apply 32))
          (next (list :write 96 :flush 80 :apply 64)))
      (ok (eq next (pg::physical-stream-validate-status next previous 96 32)))
      (dolist (bad (list (list :write 97 :flush 80 :apply 64)
                         (list :write 96 :flush 97 :apply 64)
                         (list :write 96 :flush 80 :apply 81)
                         (list :write 96 :flush 63 :apply 32)
                         (list :write 96 :flush 80 :apply 31)))
        (ok (intake-fails-p
             (lambda () (pg::physical-stream-validate-status bad previous 96 32)))))))

(deftest native-receiver-batches-write-but-flushes-on-keepalive
    (with-physical-store-directory (root)
      (let* ((store (pg::physical-store-open root 123 1 :create t))
             (pg::*physical-receive-store* store)
             (pg::*physical-recovery-state* (list :end 0))
             (pg::*physical-recovery-failed-p* nil)
             (pg::*physical-receive-flush-bytes* 1024))
        (unwind-protect
             (labels ((frame (start)
                        (let ((payload (pg::make-payload)) (raw (sample-physical-record start)))
                          (pg::payload-push-byte payload (char-code #\w))
                          (pg::pg-client-push-u64 payload start)
                          (pg::pg-client-push-u64 payload (+ start (length raw)))
                          (pg::pg-client-push-u64 payload 0)
                          (pg::payload-push payload raw)
                          payload)))
               (pg::physical-follow-process-frame (frame 0))
               (ok (= 32 (pg::physical-store-written-lsn store)))
               (ok (zerop (pg::physical-store-flushed-lsn store)))
               (let ((keepalive (pg::make-payload)))
                 (pg::payload-push-byte keepalive (char-code #\k))
                 (pg::pg-client-push-u64 keepalive 999999)
                 (pg::pg-client-push-u64 keepalive 0)
                 (pg::payload-push-byte keepalive 1)
                 (pg::physical-follow-process-frame keepalive))
               (ok (= 32 (pg::physical-store-flushed-lsn store)))
               (ok (= 32 (getf pg::*physical-recovery-state* :receive-lsn)))
               (let ((pg::*physical-receive-flush-bytes* 16))
                 (pg::physical-follow-process-frame (frame 32)))
               (ok (= 64 (pg::physical-store-flushed-lsn store))))
          (pg::physical-store-close store)))))

(deftest native-receiver-retries-sbcl-timeout-without-escaping-thread
    (with-physical-store-directory (root)
      (let* ((store (pg::physical-store-open root 123 1 :create t))
             (pg::*physical-receive-store* store)
             (pg::*physical-recovery-state* (list :end 0))
             (pg::*physical-recovery-failed-p* nil)
             (pg::*shutdown* pg::+no-shutdown+)
             (runtime (pg::make-physical-follow-runtime))
             (original (symbol-function 'pg::physical-follow-session))
             (attempts 0) (escaped nil))
        (unwind-protect
             (progn
               (setf (symbol-function 'pg::physical-follow-session)
                     (lambda (state cursor)
                       (declare (ignore cursor))
                       (if (= (incf attempts) 1)
                           (error 'sb-ext:timeout :seconds 10)
                           (setf (pg::physical-follow-runtime-stopped-p state) t))))
               (handler-case (pg::physical-follow-receiver-loop runtime)
                 (sb-ext:timeout () (setf escaped t)))
               (ok (not escaped))
               (ok (= attempts 2))
               (ok (pg::physical-follow-runtime-last-error runtime)))
          (setf (symbol-function 'pg::physical-follow-session) original)
          (pg::physical-store-close store)))))

(deftest native-follow-config-roundtrip-and-damage
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root) (config (list 1 "127.0.0.1" 5874 "copy")))
        (pg::physical-follow-save-config config)
        (ok (equal config (pg::physical-follow-read-config)))
        (with-open-file (stream (pg::physical-follow-config-path) :direction :io :if-exists :overwrite)
          (file-position stream 5)
          (write-char #\X stream))
        (ok (intake-fails-p #'pg::physical-follow-read-config))))
  (dolist (config '((1 "" 5874 "copy") (1 "host" 0 "copy")
                    (1 "host" 70000 "copy") (1 "host" 5874 "bad pin")))
    (ok (intake-fails-p (lambda () (pg::physical-follow-check-config config))))))

(deftest native-physical-frame-shape-is-bounded
    (dolist (payload (list (empty-octets) #(119) #(107 0) #(120)))
      (ok (intake-fails-p (lambda () (pg::physical-follow-process-frame payload))))))

(deftest native-source-proves-resume-prefix-and-preserves-corrupt-wal
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*catalog* (pg::make-catalog))
             (pg::*physical-recovery-state* nil)
             (pg::*physical-source-pins* (make-hash-table :test #'equal))
             (pg::*physical-source-senders* (make-hash-table :test #'equal))
             (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
             (pg::*wal-checkpoint-lsn* 64) (pg::*wal-durable-lsn* 128)
             (pg::*wal-poisoned* nil)
             (records (list (sample-physical-record 0)
                            (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 32 (empty-octets))
                            (sample-physical-record 64)
                            (pg::wal-encode-record pg::+wal-record-maintenance+ 0 96 (empty-octets)))))
        (pg::wal-ensure-directory-durable (pg::wal-directory))
        (with-open-file (stream (pg::wal-segment-path 0) :direction :output
                                                         :element-type '(unsigned-byte 8))
          (dolist (raw records) (write-sequence raw stream)))
        (pg::physical-source-pin-create "copy")
        (let* ((command (pg::parse-replication-command
                         (native-test-start-query 128 64 (pg::catalog-system-identifier))))
               (digest (reduce #'pg::physical-store-update-digest (nthcdr 2 records)
                               :initial-value #xCBF29CE484222325))
               (owner (pg::physical-source-acquire-sender command)))
          (unwind-protect
               (progn
                 (setf (cdr (assoc "prefix_digest" (pg::logical-replication-command-options command)
                                   :test #'equal)) (format nil "~d" digest))
                 (ok (= 128 (pg::physical-source-validate-resume command owner)))
                 (setf (pg::logical-replication-command-requested-lsn command) 65)
                 (ok (intake-fails-p (lambda () (pg::physical-source-validate-resume command owner))))
                 (setf (pg::logical-replication-command-requested-lsn command) 128
                       (cdr (assoc "prefix_digest" (pg::logical-replication-command-options command)
                                   :test #'equal)) "0")
                 (ok (intake-fails-p (lambda () (pg::physical-source-validate-resume command owner))))
                 (ok (intake-fails-p (lambda () (pg::physical-source-pin-release "copy"))))
                 (ok (not pg::*wal-poisoned*))
                 (with-open-file (stream (pg::wal-segment-path 0) :direction :io :if-exists :overwrite
                                                                  :element-type '(unsigned-byte 8))
                   (file-position stream 92)
                   (write-byte 0 stream))
                 (ok (intake-fails-p (lambda () (pg::physical-source-read-next "copy" owner 64))))
                 (with-open-file (stream (pg::wal-segment-path 0) :element-type '(unsigned-byte 8))
                   (ok (= 128 (file-length stream)))))
            (pg::physical-source-release-sender "copy" owner)))
        (pg::physical-source-pin-release "copy"))))
