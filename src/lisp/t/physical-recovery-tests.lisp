;;;; Recovery admission is cluster-owned, independent of mutable backend flags.
(in-package #:pg-physical-tests)

(defun recovery-state-of (function)
  (handler-case (progn (funcall function) nil)
    (pg::pg-error (condition) (pg::pg-error-sqlstate condition))))

(defun sample-recovery-manifest ()
  (list :version 1 :system-id 123 :cluster-identity "fixture#identity" :timeline 1
        :checkpoint 32 :end 32 :next-xid 3 :source-pin "copy"
        :files (list (list "PG_VERSION" 3 9))))

(defun write-recovery-manifest-text (root text)
  (pg::wal-atomic-write-bytes
   (merge-pathnames "physical-backup.manifest" root)
   (pg::string->bytes
    (format nil "PGPHYSICALBACKUP1 ~8,'0X~%~a~%" (pg::wal-crc32 (pg::string->bytes text)) text))))

(deftest recovery-manifest-is-literal-and-checksummed
    (with-physical-store-directory (root)
      (let* ((manifest (sample-recovery-manifest))
             (text (with-standard-io-syntax (prin1-to-string manifest))))
        (write-recovery-manifest-text root text)
        (ok (equal manifest (pg::recovery-read-manifest root)))
        (ok (equal manifest (pg::recovery-check-manifest-schema manifest))))
      (dolist (text '("#.(error \"reader evaluation\")" "#1=(#1#)" "'(:version 1)"
                      "(A . B)" "(((((((((((((((((1)))))))))))))))))"))
        (write-recovery-manifest-text root text)
        (ok (intake-fails-p (lambda () (pg::recovery-read-manifest root)))))
      (write-recovery-manifest-text root "(:version 1)")
      (with-open-file (stream (merge-pathnames "physical-backup.manifest" root)
                              :direction :io :if-exists :overwrite :element-type '(unsigned-byte 8))
        (file-position stream 18)
        (write-byte (char-code #\Z) stream))
      (ok (intake-fails-p (lambda () (pg::recovery-read-manifest root))))))

(cl-spec:defproperty recovery-manifest-dynamic-strings-roundtrip
    ((value (range integer 0 1000000)) (kind (range integer 0 3)))
  (:about pg::physical-bootstrap-manifest-text pg::recovery-parse-manifest-text)
  (:trials (:normal 200))
  (let ((manifest (sample-recovery-manifest)))
    (setf (getf manifest :cluster-identity)
          (format nil "~a-~d" (nth kind '("identity" "ą€😀" "quoted\"identity" "#.literal")) value))
    (equal manifest
           (pg::recovery-parse-manifest-text
            (pg::physical-bootstrap-manifest-text manifest)))))

(deftest generated-recovery-manifest-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'recovery-manifest-dynamic-strings-roundtrip :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest recovery-file-entries-reject-traversal-and-duplicates
    (dolist (name '("../pg_control" "/tmp/file" "base/../pg_control" "base/1.tmp"
                    "base/*" "global/unknown" "pg_wal/../../file"))
      (ok (not (pg::recovery-manifest-path-p name))))
  (dolist (name '("base/16384" "base/16384.vm" "PG_VERSION" "pg_wal/0000000000000000.wal"))
    (ok (pg::recovery-manifest-path-p name)))
  (let ((manifest (sample-recovery-manifest)))
    (setf (getf manifest :files) '(("PG_VERSION" 3 9) ("PG_VERSION" 3 9)))
    (ok (intake-fails-p (lambda () (pg::recovery-check-manifest-schema manifest)))))
  (let ((manifest (sample-recovery-manifest)))
    (setf (getf manifest :end) 64)
    (ok (intake-fails-p (lambda () (pg::recovery-check-manifest-schema manifest))))))

(deftest recovery-write-guards-ignore-session-read-write
    (let* ((pg::*physical-recovery-state* (sample-recovery-manifest))
           (pg::*backend* (pg::make-backend)))
      (setf (pg::backend-read-only pg::*backend*) nil)
      (dolist (function (list (lambda () (pg::assign-xid))
                              (lambda () (pg::save-catalog))
                              (lambda () (pg::sequence-nextval "s"))
                              (lambda () (pg::sequence-setval "s" 5))))
        (ok (equal "25006" (recovery-state-of function))))
      (dolist (type '(:insert :update :delete :truncate :create-table :create-sequence
                      :alter-sequence :drop-sequence :create-publication :alter-publication
                      :drop-publication :vacuum :analyze :apply-logical-event))
        (ok (equal "25006"
                   (recovery-state-of (lambda () (pg::recovery-check-statement (list :type type)))))))
      (ok (null (recovery-state-of
                 (lambda () (pg::recovery-check-statement (list :type :select))))))
      (ok (equal "0A000"
                 (recovery-state-of (lambda () (pg::command-begin nil '(:type :begin :read-only nil))))))))

(deftest recovery-failed-transactions-keep-error-precedence
    (let* ((pg::*physical-recovery-state* (sample-recovery-manifest))
           (pg::*backend* (pg::make-backend :state :aborted)))
      (ok (equal "25P02" (recovery-state-of
                          (lambda () (pg::execute-statement nil '(:type :insert))))))
      (ok (equal "25P02"
                 (recovery-state-of
                  (lambda () (pg::validate-replication-command-context
                              (pg::make-logical-replication-command :type :create))))))))

(deftest recovery-wal-functions-distinguish-base-and-receiver
    (let ((pg::*physical-recovery-state* (sample-recovery-manifest))
          (pg::*last-replay-lsn* 32))
      (ok (pg::eval-recovery-wal-function "pg_is_in_recovery"))
      (ok (= 32 (pg::eval-recovery-wal-function "pg_last_wal_replay_lsn")))
      (ok (eq pg::+null+ (pg::eval-recovery-wal-function "pg_last_wal_receive_lsn")))
      (ok (equal "55000" (recovery-state-of
                          (lambda () (pg::eval-recovery-wal-function "pg_current_wal_lsn")))))))

(deftest replay-pause-controls-require-recovery
    (let ((pg::*physical-recovery-state* nil)
          (pg::*physical-recovery-failed-p* nil))
      (dolist (name '("pg_wal_replay_pause" "pg_wal_replay_resume"
                      "pg_is_wal_replay_paused" "pg_get_wal_replay_pause_state"))
        (ok (equal "55000" (recovery-state-of
                            (lambda () (pg::eval-func name nil))))))))

(deftest replay-pause-controls-preserve-progress-and-void-type
    (let ((pg::*physical-recovery-state* (sample-recovery-manifest))
          (pg::*physical-recovery-failed-p* nil)
          (pg::*physical-replay-paused-p* nil)
          (pg::*last-replay-lsn* 32))
      (ok (equal "not paused" (pg::eval-func "pg_get_wal_replay_pause_state" nil)))
      (ok (equal "" (pg::eval-func "pg_wal_replay_pause" nil)))
      (ok (pg::eval-func "pg_is_wal_replay_paused" nil))
      (ok (equal "paused" (pg::eval-func "pg_get_wal_replay_pause_state" nil)))
      (ok (= 32 pg::*last-replay-lsn*))
      (ok (equal "" (pg::eval-func "pg_wal_replay_pause" nil)))
      (ok (equal "" (pg::eval-func "pg_wal_replay_resume" nil)))
      (ok (not (pg::eval-func "pg_is_wal_replay_paused" nil)))
      (ok (= 2278 (pg::type-oid :void)))
      (ok (equal "" (pg::value->text "" :void)))
      (dolist (name '("pg_wal_replay_pause" "pg_wal_replay_resume"))
        (ok (eq :void (pg::expr-type
                       (pg::analyze-function-expression (list :func name nil) nil)))))
      (setf pg::*physical-recovery-failed-p* t)
      (ok (equal "57P03" (recovery-state-of
                          (lambda () (pg::eval-func "pg_wal_replay_resume" nil)))))))

(deftest recovery-control-persists-role-and-detects-corruption
    (with-physical-store-directory (root)
      (let ((bytes (pg::physical-recovery-control-image 1 123 1 456 32 32 1003))
            (pg::*pgdata* root)
            (pg::*physical-recovery-state* nil))
        (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path root) bytes)
        (let ((control (pg::physical-recovery-read-control root)))
          (ok (= 1 (getf control :phase)))
          (ok (= 456 (getf control :manifest-digest)))
          (ok (= 1003 (getf control :next-xid))))
        ;; The durable role alone must prevent fallback to writable startup.
        (ok (intake-fails-p #'pg::physical-bootstrap-check-startup))
        (setf (aref bytes 16) (logxor 1 (aref bytes 16)))
        (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path root) bytes)
        (ok (intake-fails-p (lambda () (pg::physical-recovery-read-control root)))))))

(deftest streaming-recovery-control-keeps-base-and-applied-distinct
    (with-physical-store-directory (root)
      (pg::wal-atomic-write-bytes
       (pg::physical-recovery-control-path root)
       (pg::physical-recovery-control-image 2 123 1 456 32 96 1004))
      (let ((control (pg::physical-recovery-read-control root)))
        (ok (= 2 (getf control :phase)))
        (ok (= 32 (getf control :checkpoint)))
        (ok (= 96 (getf control :applied)))
        (ok (= 1004 (getf control :next-xid))))
      (ok (intake-fails-p
           (lambda () (pg::physical-recovery-control-image 1 123 1 456 32 96 1004))))
      (ok (intake-fails-p
           (lambda () (pg::physical-recovery-control-image 2 123 1 456 96 32 1004))))))

(deftest failed-streaming-recovery-does-not-admit-sql
    (let ((pg::*physical-recovery-state* (sample-recovery-manifest))
          (pg::*physical-recovery-failed-p* t))
      (ok (equal "57P03" (recovery-state-of
                          (lambda () (pg::recovery-check-statement '(:type :select))))))))

(deftest streaming-recovery-defers-for-idle-transactions
    (let ((pg::*active-snapshots* 0)
          (pg::*backend-registry* (make-hash-table)))
      (let ((backend (pg::make-backend :state :in-progress)))
        (setf (gethash 1 pg::*backend-registry*) backend)
        (ok (pg::physical-recovery-reader-conflict-p))
        (setf (pg::backend-state backend) :idle)
        (ok (not (pg::physical-recovery-reader-conflict-p)))
        (let ((pg::*active-snapshots* 1))
          (ok (pg::physical-recovery-reader-conflict-p))))))

(deftest streaming-replay-observes-full-xids-and-reset-boundaries
    (let* ((cycle pg::+xid-cycle+)
           (record (pg::make-wal-record :type pg::+wal-record-commit+ :xid (1- cycle))))
      (ok (= (+ cycle 3) (pg::physical-replay-observed-next-xid record 3)))
      (setf (pg::wal-record-xid record) (+ cycle 42))
      (ok (= (+ cycle 43) (pg::physical-replay-observed-next-xid record 3)))
      (ok (= (+ cycle 1003) (pg::physical-replay-observed-next-xid record (+ cycle 1003))))
      (let ((payload (make-array 8 :element-type '(unsigned-byte 8))))
        (pg::wal-set-u64 payload 0 (+ cycle 3))
        (setf (pg::wal-record-type record) pg::+wal-record-clog-reset+
              (pg::wal-record-xid record) 0
              (pg::wal-record-payload record) payload)
        ;; A reset boundary is already NEXT, not an allocated transaction.
        (ok (= (+ cycle 3) (pg::physical-replay-observed-next-xid record 3))))
      (setf (pg::wal-record-type record) pg::+wal-record-pages+
            (pg::wal-record-version record) pg::+wal-version-v3+)
      (ok (intake-fails-p (lambda () (pg::physical-replay-observed-next-xid record 3))))))

(deftest streaming-catalog-creates-new-empty-heaps-without-masking-loss
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*physical-recovery-state* (list :files nil))
             (pg::*physical-replay-seen-relfilenodes* (make-hash-table :test #'eql))
             (pg::*physical-replay-restarting-p* nil)
             (catalog (pg::make-catalog))
             (database (pg::make-database :name "postgres"))
             (table (pg::make-pg-class :oid 900 :relfilenode 900 :name "empty")))
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (setf (gethash "postgres" (pg::catalog-databases catalog)) database
              (gethash "empty" (pg::database-tables database)) table)
        (unwind-protect
             (progn
               (pg::physical-replay-create-heap-forks catalog)
               (ok (probe-file (pg::relation-file-path 900)))
               (with-open-file (stream (pg::relation-file-path 900))
                 (ok (zerop (file-length stream))))
               (pg::smgr-forget 900 :delete t)
               (ok (intake-fails-p
                    (lambda () (pg::physical-replay-create-heap-forks catalog))))
               (ok (not (probe-file (pg::relation-file-path 900)))))
          (pg::smgr-close-all)))))

(deftest recovery-readers-use-virtual-lock-identities
    (let* ((pg::*physical-recovery-state* (sample-recovery-manifest))
           (pg::*backend* (pg::make-backend))
           (pg::*next-xid* 50) (pg::*active-xids* nil)
           (pg::*recovery-reader-xid-counter* 0)
           (pg::*relation-readers* (make-hash-table :test #'eql))
           (pg::*relation-writers* (make-hash-table :test #'eql))
           (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
           (token (pg::get-current-reader-xid)))
      (ok (minusp token))
      (ok (null (pg::backend-xid pg::*backend*)))
      (ok (= 50 pg::*next-xid*))
      (ok (null pg::*active-xids*))
      (pg::register-relation-readers '(16384) token)
      (ok (equal (list token) (gethash 16384 pg::*relation-readers*)))
      (pg::reset-backend-transaction pg::*backend*)
      (ok (null (pg::backend-virtual-reader-xid pg::*backend*)))
      (ok (zerop (hash-table-count pg::*relation-readers*)))
      (ok (= 50 pg::*next-xid*))))

(deftest primary-replay-progress-excludes-incomplete-batch
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (pg::*wal-checkpoint-lsn* 0)
                                 (pg::*wal-durable-lsn* 104) (pg::*last-replay-lsn* nil)
                                 (start (sample-physical-record))
                                 (commit (pg::wal-encode-record pg::+wal-record-commit+ 3 32
                                                                (pg::wal-encode-commit-timestamp 1)))
                                 (tail (sample-physical-record 72)))
        (pg::wal-ensure-directory-durable (pg::wal-directory))
        (pg::wal-atomic-write-bytes (pg::wal-segment-path 0)
                                    (concatenate '(vector (unsigned-byte 8)) start commit tail))
        (pg::note-primary-startup-replay)
        (ok (= 72 pg::*last-replay-lsn*))
        (ok (= 104 pg::*wal-durable-lsn*)))))
