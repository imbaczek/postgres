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

(defun pglz-corruption-rejected-p (function)
  (handler-case (progn (funcall function) nil)
    (pg::pg-error (condition) (string= (pg::pg-error-sqlstate condition) "XX001"))))

(cl-spec:defproperty pglz-compressed-roundtrip-and-streamed-slices
    ((size (range integer 32 8192))
     (period (range integer 1 4))
     (seed-byte (range integer 0 255))
     (start-seed (range integer 0 8192))
     (length-seed (range integer 0 8192)))
  (:about pg::pglz-compress pg::pglz-decompress pg::pglz-decompress-slice)
  (:trials (:normal 200))
  (let* ((data (make-array size :element-type '(unsigned-byte 8)))
         (start (mod start-seed (1+ size)))
         (length (mod length-seed (1+ (- size start)))))
    (dotimes (i size)
      (setf (aref data i) (mod (+ seed-byte (mod i period)) 256)))
    (let* ((compressed (pg::pglz-compress data))
           (decode (lambda (stream)
                     (pg::pglz-decompress-slice size start length (length stream)
                                                (lambda (consume)
                                                  (loop for byte across stream do (funcall consume byte)))))))
      (and compressed
           (equalp data (pg::pglz-decompress compressed size))
           (equalp (subseq data start (+ start length)) (funcall decode compressed))
           (pglz-corruption-rejected-p (lambda () (funcall decode (subseq compressed 0 (1- (length compressed))))))
           (pglz-corruption-rejected-p (lambda () (funcall decode
                                                           (concatenate '(vector (unsigned-byte 8)) compressed #(0)))))))))

(deftest generated-pglz-roundtrip-and-slices
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'pglz-compressed-roundtrip-and-streamed-slices :seed 42))))
      (ok (eq :passed (getf result :status)) (format nil "~s" result))))

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

(deftest recovery-manifest-v3-preserves-exhausted-actual-next-xid
    (with-physical-store-directory (root)
      (let* ((manifest (list :version 3 :system-id 123 :cluster-identity "fixture#identity"
                             :timeline 1 :checkpoint 152 :checkpoint-start 32 :end 152
                             :next-xid 18446744073709551616 :source-pin "copy"
                             :files '(("PG_VERSION" 3 9))))
             (text (pg::physical-bootstrap-manifest-text manifest)))
        (write-recovery-manifest-text root text)
        (ok (equal manifest (pg::recovery-check-manifest-schema
                             (pg::recovery-read-manifest root))))
        (setf (getf manifest :version) 2)
        (ok (intake-fails-p (lambda () (pg::recovery-check-manifest-schema manifest)))))))

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

(deftest wal-lsn-difference-preserves-unsigned-range-and-numeric-type
    (let* ((expression (pg::analyze-function-expression
                        '(:func "pg_wal_lsn_diff"
                          ((:const "FFFFFFFF/FFFFFFFF" :type :text :typmod -1)
                           (:const "0/0" :type :text :typmod -1))) nil))
           (value (pg::eval-expr expression nil)))
      (ok (eq :numeric (pg::expr-type expression)))
      (ok (= 1700 (pg::type-oid :numeric)))
      (ok (equal "numeric" (pg::typed-arg-name expression)))
      (ok (equal "pg_lsn" (pg::typed-arg-name '(:const 0 :type :pg-lsn))))
      (ok (= #xffffffffffffffff value))
      (ok (equal "18446744073709551615" (pg::value->text value :numeric)))
      (ok (equal "22003" (recovery-state-of
                          (lambda () (pg::sql-cast-value value :numeric :int8 -1)))))
      (ok (= pg::+int8-max+ (pg::sql-cast-value pg::+int8-max+ :numeric :int8 -1)))
      (ok (= pg::+int8-min+ (pg::sql-cast-value pg::+int8-min+ :numeric :int8 -1)))
      (ok (equal "22003" (recovery-state-of
                          (lambda () (pg::sql-cast-value (1- pg::+int8-min+) :numeric :int8 -1)))))
      (ok (eq pg::+null+ (pg::eval-func "pg_wal_lsn_diff" (list pg::+null+ 1))))))

(cl-spec:defproperty wal-lsn-difference-is-exact-and-antisymmetric
    ((left (range integer 0 18446744073709551615))
     (right (range integer 0 18446744073709551615)))
  (:about pg::eval-wal-lsn-diff)
  (:trials (:normal 200))
  (let ((difference (pg::eval-wal-lsn-diff (list left right))))
    (and (= left (+ right difference))
         (= difference (- (pg::eval-wal-lsn-diff (list right left)))))))

(deftest generated-wal-lsn-difference
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'wal-lsn-difference-is-exact-and-antisymmetric :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

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

(deftest streaming-catalog-replays-toast-heap-without-masking-loss
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*physical-recovery-state* (list :files nil))
             (pg::*physical-replay-seen-relfilenodes* (make-hash-table :test #'eql))
             (pg::*physical-replay-restarting-p* nil)
             (catalog (pg::make-catalog))
             (database (pg::make-database :name "postgres"))
             (toast (pg::make-pg-toast :oid 901 :relfilenode 901 :index-oid 902
                                       :index-relfilenode 902 :relfrozenxid 3))
             (table (pg::make-pg-class :oid 900 :relfilenode 900 :name "parent" :toast toast)))
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (setf (gethash "postgres" (pg::catalog-databases catalog)) database
              (gethash "parent" (pg::database-tables database)) table)
        (unwind-protect
             (progn
               (pg::physical-replay-create-heap-forks catalog)
               (ok (probe-file (pg::relation-file-path 900)))
               (ok (probe-file (pg::relation-file-path 901)))
               (ok (not (probe-file (pg::relation-file-path 902))))
               (pg::smgr-forget 901 :delete t)
               (ok (intake-fails-p
                    (lambda () (pg::physical-replay-create-heap-forks catalog))))
               (ok (not (probe-file (pg::relation-file-path 901)))))
          (pg::smgr-close-all)))))

(deftest catalog-orphan-cleanup-retains-toast-storage
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*catalog* (pg::make-catalog))
             (database (pg::make-database :name "postgres"))
             (toast (pg::make-pg-toast :oid 901 :relfilenode 901 :index-oid 902
                                       :index-relfilenode 902 :relfrozenxid 3))
             (table (pg::make-pg-class :oid 900 :relfilenode 900 :name "parent" :toast toast)))
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database
              (gethash "parent" (pg::database-tables database)) table)
        (unwind-protect
             (progn
               (dolist (node '(900 901 902 903)) (pg::smgr-create node))
               (dolist (node '(901 903))
                 (with-open-file (stream (pg::relation-file-path node :visibility)
                                         :direction :output :if-does-not-exist :create)
                   (declare (ignore stream))))
               (pg::catalog-remove-orphan-relations)
               (dolist (node '(900 901 902))
                 (ok (probe-file (pg::relation-file-path node))))
               (ok (probe-file (pg::relation-file-path 901 :visibility)))
               (ok (not (probe-file (pg::relation-file-path 903))))
               (ok (not (probe-file (pg::relation-file-path 903 :visibility)))))
          (pg::smgr-close-all)))))

(deftest create-table-initializes-owned-toast-storage
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*wal-page-cache* (make-hash-table :test #'equal))
             (pg::*wal-ready* nil)
             (pg::*catalog* (pg::make-catalog))
             (pg::*backend* (pg::make-backend))
             (database (pg::make-database :name "postgres")))
        (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database)
        (ensure-directories-exist (pg::catalog-path))
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (unwind-protect
             (let* ((table (pg::create-table* database "big"
                                              (list (list :name "n" :type :numeric :typmod -1))))
                    (toast (pg::pg-class-toast table))
                    (small (pg::create-table* database "small"
                                              (list (list :name "n" :type :int4 :typmod -1)))))
               (ok toast)
               (ok (not (pg::pg-class-toast small)))
               (ok (probe-file (pg::relation-file-path (pg::pg-toast-relfilenode toast))))
               (ok (= 2 (pg::buf-nblocks (pg::pg-toast-index-relfilenode toast))))
               (pg::save-catalog)
               (let* ((restored (pg::load-catalog))
                      (db (gethash "postgres" (pg::catalog-databases restored)))
                      (saved (gethash "big" (pg::database-tables db))))
                 (ok (equalp toast (pg::pg-class-toast saved)))))
          (pg::smgr-close-all)))))

(deftest toast-fetch-slices-validate-selected-chunks
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*wal-page-cache* (make-hash-table :test #'equal))
             (pg::*wal-ready* nil)
             (pg::*clog-stream* nil)
             (pg::*active-xids* nil)
             (pg::*catalog* (pg::make-catalog))
             (pg::*backend* (pg::make-backend))
             (database (pg::make-database :name "postgres"))
             (snapshot (pg::make-snapshot-struct :xmin 3 :xmax 50 :xip nil :curcid 0))
             (registration (pg::make-snapshot-registration))
             (bytes (make-array 5000 :element-type '(unsigned-byte 8)
                                     :initial-contents (loop for i below 5000 collect (mod i 251))))
             (compressible (make-array 6300 :element-type '(unsigned-byte 8))))
        (let ((state 1))
          (dotimes (i 2100)
            (setf state (logand #xffffffff (+ (* state 1664525) 1013904223))
                  (aref compressible i) (if (< i 200) 42 (ldb (byte 8 24) state))
                  (aref compressible (+ i 2100)) (aref compressible i)
                  (aref compressible (+ i 4200)) (aref compressible i))))
        (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database)
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (unwind-protect
             (let* ((table (pg::create-table* database "slices" (list (list :name "v" :type :bytea :typmod -1))))
                    (toast (pg::pg-class-toast table))
                    (pointer nil) (compressed nil))
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (pg::with-write-lock
                   (setf pointer (pg::toast-store-value-unlocked toast bytes nil 42 0)
                         compressed (pg::toast-store-value-unlocked toast compressible nil 42 0 :compress t))))
               (ok (intake-fails-p (lambda () (pg::toast-fetch-value-slice toast pointer snapshot 0 1))))
               (pg::register-active-snapshot snapshot registration)
               (dolist (bounds '((0 1) (1990 30) (4990 30) (5000 20) (9999 2) (0 0)))
                 (let ((start (min (first bounds) (length bytes))))
                   (ok (equalp (subseq bytes start (min (length bytes) (+ start (second bounds))))
                               (pg::toast-fetch-value-slice toast pointer snapshot (first bounds) (second bounds))))))
               (let ((old (symbol-function 'pg::btree-prefix-entries-unlocked)))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::btree-prefix-entries-unlocked)
                              (lambda (&rest args) (declare (ignore args))
                                (error "full fetch must use the visitor")))
                        (ok (equalp bytes (pg::toast-fetch-value toast pointer snapshot)))
                        (ok (equalp compressible (pg::toast-fetch-value toast compressed snapshot))))
                   (setf (symbol-function 'pg::btree-prefix-entries-unlocked) old)))
               (let ((old (symbol-function 'pg::btree-map-prefix-entries-unlocked)))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::btree-map-prefix-entries-unlocked)
                              (lambda (node prefix callback)
                                (funcall old node prefix
                                         (lambda (entry)
                                           (funcall callback entry)
                                           (error "interrupted fetch")))))
                        (ok (intake-fails-p (lambda () (pg::toast-fetch-value toast pointer snapshot)))))
                   (setf (symbol-function 'pg::btree-map-prefix-entries-unlocked) old)))
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (ok t))
               (ok (equalp bytes (pg::toast-fetch-value toast pointer snapshot)))
               (ok (nth-value 2 (pg::toast-pointer-storage-size toast compressed)))
               (ok (> (pg::toast-pointer-storage-size toast compressed) pg::+toast-oid-chunk-size+))
               (let ((old (symbol-function 'pg::toast-fetch-value)))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::toast-fetch-value)
                              (lambda (&rest args) (declare (ignore args))
                                (error "compressed slice must stream")))
                        (ok (equalp (subseq compressible 1900 2100)
                                    (pg::toast-fetch-value-slice toast compressed snapshot 1900 200))))
                   (setf (symbol-function 'pg::toast-fetch-value) old)))
               ;; Cancellation during decompression must release the index lease.
               (let* ((long (make-array 200000 :element-type '(unsigned-byte 8) :initial-element 42))
                      (long-pointer
                        (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                          (pg::with-write-lock
                            (pg::toast-store-value-unlocked toast long nil 42 0 :compress t))))
                      (old (symbol-function 'pg::check-for-interrupts))
                      (polls 0))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::check-for-interrupts)
                              (lambda () (incf polls) (error "cancel slice")))
                        (ok (intake-fails-p
                             (lambda () (pg::toast-fetch-value-slice toast long-pointer snapshot 0 1))))
                        (ok (= polls 1)))
                   (setf (symbol-function 'pg::check-for-interrupts) old))
                 (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                   (ok t))
                 (ok (equalp #(42) (pg::toast-fetch-value-slice toast long-pointer snapshot 0 1))))
               ;; Even after the selected output is complete, an extra indexed
               ;; chunk is corrupt; an empty slice preserves the old fast path.
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (pg::with-write-lock
                   (multiple-value-bind (block offset)
                       (pg::heapam-insert (pg::pg-toast-relfilenode toast) pg::*toast-chunk-columns*
                                          (vector (pg::heap-external-varlena-value-id compressed)
                                                  (ceiling (pg::toast-pointer-storage-size toast compressed) pg::+toast-oid-chunk-size+)
                                                  (make-array 1 :element-type '(unsigned-byte 8) :initial-element 0)) 42 0)
                     (pg::btree-insert-unlocked (pg::pg-toast-index-relfilenode toast)
                                                (pg::toast-chunk-key (pg::heap-external-varlena-value-id compressed)
                                                                     (ceiling (pg::toast-pointer-storage-size toast compressed) pg::+toast-oid-chunk-size+))
                                                block offset))))
               (ok (equal "XX001" (recovery-state-of
                                   (lambda () (pg::toast-fetch-value-slice toast compressed snapshot 0 1)))))
               (ok (equalp #() (pg::toast-fetch-value-slice toast compressed snapshot 0 0)))
               (let ((original (symbol-function 'pg::btree-search-unlocked))
                     (calls 0))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::btree-search-unlocked)
                              (lambda (&rest args) (incf calls) (apply original args)))
                        (ok (equalp (subseq bytes 2000 2020)
                                    (pg::toast-fetch-value-slice toast pointer snapshot 2000 20)))
                        (ok (= calls 1)))
                   (setf (symbol-function 'pg::btree-search-unlocked) original)))
               (let ((leases 0))
                 (ok (equalp (subseq bytes 1900 2100)
                             (pg::toast-fetch-value-slice
                              toast pointer snapshot 1900 200
                              :index-reader (lambda (node thunk)
                                              (incf leases)
                                              (pg::call-with-btree-relation-read-lock node thunk)))))
                 (ok (= leases 1)))
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (pg::with-write-lock (pg::toast-delete-value-unlocked toast pointer 44 0)))
               (ok (equalp (subseq bytes 1900 2100)
                           (pg::toast-fetch-value-slice toast pointer snapshot 1900 200)))
               (let ((missing (pg::copy-heap-external-varlena pointer)))
                 (setf (pg::heap-external-varlena-value-id missing) 999999)
                 (ok (equal "XX001" (recovery-state-of
                                     (lambda () (pg::toast-fetch-value-slice toast missing snapshot 0 1)))))
                 (ok (equal "XX001" (recovery-state-of
                                     (lambda () (pg::toast-fetch-value toast missing snapshot))))))
               (ok (intake-fails-p (lambda () (pg::toast-fetch-value-slice toast pointer snapshot -1 1))))
               (let ((bad (pg::copy-heap-external-varlena pointer)))
                 (incf (pg::heap-external-varlena-relation-id bad))
                 (ok (equal "XX001" (recovery-state-of (lambda () (pg::toast-fetch-value-slice toast bad snapshot 0 1))))))
               ;; A selected chunk with the right key but wrong length is corrupt.
               (let ((malformed (pg::copy-heap-external-varlena pointer)))
                 (setf (pg::heap-external-varlena-value-id malformed) 999998)
                 (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                   (pg::with-write-lock
                     (multiple-value-bind (block offset)
                         (pg::heapam-insert (pg::pg-toast-relfilenode toast) pg::*toast-chunk-columns*
                                            (vector 999998 0 (subseq bytes 0 1)) 42 0)
                       (pg::btree-insert-unlocked (pg::pg-toast-index-relfilenode toast)
                                                  (pg::toast-chunk-key 999998 0) block offset))))
                 (ok (equal "XX001" (recovery-state-of
                                     (lambda () (pg::toast-fetch-value-slice toast malformed snapshot 0 1)))))
                 (ok (equal "XX001" (recovery-state-of
                                     (lambda () (pg::toast-fetch-value toast malformed snapshot))))))
               ;; A matching index key must point to the same chunk identity.
               (let ((wrong (pg::copy-heap-external-varlena pointer)))
                 (setf (pg::heap-external-varlena-value-id wrong) 999996)
                 (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                   (pg::with-write-lock
                     (multiple-value-bind (block offset)
                         (pg::heapam-insert (pg::pg-toast-relfilenode toast) pg::*toast-chunk-columns*
                                            (vector 999997 0 (subseq bytes 0 pg::+toast-oid-chunk-size+)) 42 0)
                       (pg::btree-insert-unlocked (pg::pg-toast-index-relfilenode toast)
                                                  (pg::toast-chunk-key 999996 0) block offset))))
                 (ok (equal "XX001" (recovery-state-of
                                     (lambda () (pg::toast-fetch-value toast wrong snapshot))))))
               ;; An aborted insertion may leave a stale index entry beside the real chunk.
               (pg::clog-set 43 pg::+clog-aborted+)
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (pg::with-write-lock
                   (multiple-value-bind (block offset)
                       (pg::heapam-insert (pg::pg-toast-relfilenode toast) pg::*toast-chunk-columns*
                                          (vector (pg::heap-external-varlena-value-id pointer) 0
                                                  (subseq bytes 0 pg::+toast-oid-chunk-size+)) 43 0)
                     (pg::btree-insert-unlocked (pg::pg-toast-index-relfilenode toast)
                                                (pg::toast-chunk-key (pg::heap-external-varlena-value-id pointer) 0)
                                                block offset)
                     (pg::with-heap-tuple-edit (page at (pg::pg-toast-relfilenode toast) block offset)
                       (pg::page-set-u16 page (+ at pg::+heap-tuple-infomask-off+)
                                         pg::+heap-xmin-invalid+)))))
               (ok (equalp (subseq bytes 0 1)
                           (pg::toast-fetch-value-slice toast pointer snapshot 0 1)))
               (ok (equalp bytes (pg::toast-fetch-value toast pointer snapshot)))
               ;; Duplicate an indexed selected chunk; exact lookup must reject it.
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (pg::with-write-lock
                   (multiple-value-bind (block offset)
                       (pg::heapam-insert (pg::pg-toast-relfilenode toast) pg::*toast-chunk-columns*
                                          (vector (pg::heap-external-varlena-value-id pointer) 0
                                                  (subseq bytes 0 pg::+toast-oid-chunk-size+)) 42 0)
                     (pg::btree-insert-unlocked (pg::pg-toast-index-relfilenode toast)
                                                (pg::toast-chunk-key (pg::heap-external-varlena-value-id pointer) 0)
                                                block offset))))
               (ok (equal "XX001" (recovery-state-of (lambda () (pg::toast-fetch-value-slice toast pointer snapshot 0 1)))))
               (ok (equal "XX001" (recovery-state-of (lambda () (pg::toast-fetch-value toast pointer snapshot)))))
               (ok (equalp (subseq bytes 4500 4510)
                           (pg::toast-fetch-value-slice toast pointer snapshot 4500 10))))
          (pg::release-snapshot-registration registration)
          (when pg::*clog-stream* (close pg::*clog-stream*))
          (pg::smgr-close-all)))))

(deftest toast-text-slices-stream-utf8-and-validate-suffix
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*wal-page-cache* (make-hash-table :test #'equal))
             (pg::*wal-ready* nil) (pg::*clog-stream* nil) (pg::*active-xids* nil)
             (pg::*catalog* (pg::make-catalog)) (pg::*backend* (pg::make-backend))
             (database (pg::make-database :name "postgres"))
             (snapshot (pg::make-snapshot-struct :xmin 3 :xmax 50 :xip nil :curcid 0))
             (registration (pg::make-snapshot-registration))
             (text (concatenate 'string (make-string 1995 :initial-element #\a)
                                "é🙂é€" (make-string 5000 :initial-element #\z)))
             (raw (pg::string->bytes text)))
        (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database)
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (unwind-protect
             (let* ((table (pg::create-table* database "text_slice"
                                              (list (list :name "v" :type :text :typmod -1)
                                                    (list :name "b" :type :bytea :typmod -1))))
                    (toast (pg::pg-class-toast table))
                    (plain nil) (compressed nil) (bad nil))
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (pg::with-write-lock
                   (setf plain (pg::toast-store-value-unlocked toast raw nil 42 0)
                         compressed (pg::toast-store-value-unlocked toast raw nil 42 0 :compress t)
                         bad (pg::toast-store-value-unlocked
                              toast (concatenate '(vector (unsigned-byte 8)) raw
                                                 (make-array 1 :element-type '(unsigned-byte 8) :initial-element #xff))
                              nil 42 0))))
               (pg::register-active-snapshot snapshot registration)
               (ok (nth-value 2 (pg::toast-pointer-storage-size toast compressed)))
               ;; A full-value projection of the same column must retain the
               ;; ordinary fetch path while its substring still returns SQL text.
               (pg::clog-set 42 pg::+clog-committed+)
               (pg::with-write-lock
                 (pg::heapam-insert (pg::pg-class-relfilenode table)
                                    (pg::pg-class-columns table) (vector plain plain) 42 0))
               (let ((old (symbol-function 'pg::toast-fetch-value)) (calls 0) (rows nil)
                                                                    (pg::*current-snapshot* snapshot))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::toast-fetch-value)
                              (lambda (&rest args) (incf calls) (apply old args)))
                        (let* ((ast (first (pg::parse-statements
                                            "SELECT substring(v,1996,3), octet_length(v), v FROM text_slice")))
                               (plan (pg::plan-select-query ast)))
                          (pg::interpret-query-plan
                           plan (getf (pg::query-plan-prepared plan) :relations)
                           (lambda (row) (push (car row) rows))))
                        (ok (= calls 1))
                        (ok (and (= (length rows) 1)
                                 (string= (aref (first rows) 0) (subseq text 1995 1998))
                                 (= (aref (first rows) 1) (length raw))
                                 (string= (aref (first rows) 2) text))))
                   (setf (symbol-function 'pg::toast-fetch-value) old)))
               (let ((old (symbol-function 'pg::toast-fetch-value)))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::toast-fetch-value)
                              (lambda (&rest args) (declare (ignore args))
                                (error "text slice must not fetch full value")))
                        (let* ((pg::*current-snapshot* snapshot)
                               (ast (first (pg::parse-statements "SELECT substring(v,1996,3) FROM text_slice")))
                               (plan (pg::plan-select-query ast))
                               (rows nil))
                          (pg::interpret-query-plan
                           plan (getf (pg::query-plan-prepared plan) :relations)
                           (lambda (row) (push (car row) rows)))
                          (ok (and (= (length rows) 1)
                                   (string= (aref (first rows) 0) (subseq text 1995 1998)))))
                        (let* ((pg::*current-snapshot* snapshot)
                               (ast (first (pg::parse-statements
                                            "SELECT octet_length(v), length(v), char_length(v), character_length(v) FROM text_slice")))
                               (plan (pg::plan-select-query ast)) (rows nil))
                          (pg::interpret-query-plan
                           plan (getf (pg::query-plan-prepared plan) :relations)
                           (lambda (row) (push (car row) rows)))
                          (ok (and (= (length rows) 1)
                                   (= (aref (first rows) 0) (length raw))
                                   (every (lambda (i) (= (aref (first rows) i) (length text))) '(1 2 3)))))
                        (let ((old-map (symbol-function 'pg::toast-map-value-octets)))
                          (unwind-protect
                               (progn
                                 (setf (symbol-function 'pg::toast-map-value-octets)
                                       (lambda (&rest args) (declare (ignore args))
                                         (error "byte count must not scan chunks")))
                                 (ok (= (pg::toast-raw-value-size toast compressed snapshot) (length raw)))
                                 (let* ((pg::*current-snapshot* snapshot)
                                        (ast (first (pg::parse-statements "SELECT octet_length(v), length(b), octet_length(b) FROM text_slice")))
                                        (plan (pg::plan-select-query ast)) (rows nil))
                                   (pg::interpret-query-plan
                                    plan (getf (pg::query-plan-prepared plan) :relations)
                                    (lambda (row) (push (car row) rows)))
                                   (ok (every (lambda (i) (= (aref (first rows) i) (length raw))) '(0 1 2)))))
                            (setf (symbol-function 'pg::toast-map-value-octets) old-map)))
                        (dolist (pointer (list plain compressed))
                          (dolist (bounds '((0 0) (1994 6) (1995 1) (1996 1) (1997 2) (7000 100) (0 nil)))
                            (let* ((start (min (first bounds) (length text)))
                                   (count (second bounds))
                                   (end (if count (min (length text) (+ start count)) (length text))))
                              (ok (string= (subseq text start end)
                                           (pg::toast-fetch-text-slice toast pointer snapshot
                                                                       (first bounds) count))))))
                        (let* ((expression '(:func "substr" ((:col 1 :type :text)
                                                             (:const 1996 :type :int4)
                                                             (:const 3 :type :int4)) :type :text))
                               (environment (vector pg::+null+))
                               (pg::*toast-slice-current*
                                 (list (cons expression (list toast plain snapshot environment)))))
                          (ok (string= (subseq text 1995 1998)
                                       (pg::eval-expr expression environment))))
                        ;; Byte length follows pointer metadata even if contents
                        ;; are corrupt; character count validates the full UTF-8.
                        (ok (= (pg::toast-raw-value-size toast bad snapshot) (1+ (length raw))))
                        (ok (intake-fails-p (lambda () (pg::toast-map-text-characters toast bad snapshot nil))))
                        (ok (intake-fails-p (lambda () (pg::toast-raw-value-size toast bad nil))))
                        (let ((wrong (pg::copy-heap-external-varlena plain)))
                          (incf (pg::heap-external-varlena-relation-id wrong))
                          (ok (equal "XX001" (recovery-state-of
                                              (lambda () (pg::toast-raw-value-size toast wrong snapshot))))))
                        (ok (intake-fails-p (lambda () (pg::toast-fetch-text-slice toast bad snapshot 0 1))))
                        (ok (intake-fails-p (lambda () (pg::toast-fetch-text-slice toast bad snapshot 0 0))))
                        (dolist (suffix '((#x80) (#xc0 #x80) (#xe0 #x80 #x80)
                                          (#xed #xa0 #x80) (#xf4 #x90 #x80 #x80) (#xe2 #x82)))
                          (let ((invalid (concatenate '(vector (unsigned-byte 8)) raw
                                                      (coerce suffix '(vector (unsigned-byte 8))))))
                            (dolist (compression '(nil t))
                              (let ((pointer
                                      (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                                        (pg::with-write-lock
                                          (pg::toast-store-value-unlocked toast invalid nil 42 0 :compress compression)))))
                                (when compression
                                  (ok (nth-value 2 (pg::toast-pointer-storage-size toast pointer))))
                                (ok (intake-fails-p
                                     (lambda () (pg::toast-fetch-text-slice toast pointer snapshot 0 1))))))))
                        (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                          (ok t)))
                   (setf (symbol-function 'pg::toast-fetch-value) old))))
          (pg::release-snapshot-registration registration)
          (when pg::*clog-stream* (close pg::*clog-stream*))
          (pg::smgr-close-all)))))

(deftest update-retains-unreferenced-toast-without-fetch
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*wal-page-cache* (make-hash-table :test #'equal))
             (pg::*wal-ready* nil) (pg::*clog-stream* nil)
             (pg::*active-xids* (list 43)) (pg::*next-xid* 50)
             (pg::*logical-capture-xid-modes* (make-hash-table :test #'eql))
             (pg::*catalog* (pg::make-catalog))
             (pg::*backend* (pg::make-backend :database "postgres" :state :in-progress
                                              :explicit t :xid 43))
             (database (pg::make-database :name "postgres"))
             (snapshot (pg::make-snapshot-struct :xmin 3 :xmax 50 :xip '(43) :curcid 0))
             (pg::*current-snapshot* snapshot)
             (registration (pg::make-snapshot-registration))
             (raw (make-array 10000 :element-type '(unsigned-byte 8) :initial-element 97))
             (out-path (merge-pathnames "update-response" root))
             (old-fetch (symbol-function 'pg::toast-fetch-value)))
        (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database
              (gethash 43 pg::*logical-capture-xid-modes*) :skip-written)
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (unwind-protect
             (let* ((table (pg::create-table* database "update_slice"
                                              (list (list :name "id" :type :int4 :typmod -1)
                                                    (list :name "n" :type :int4 :typmod -1)
                                                    (list :name "payload" :type :text :typmod -1 :notnull t))))
                    (toast (pg::pg-class-toast table))
                    (pointer nil))
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (pg::with-write-lock
                   (setf pointer (pg::toast-store-value-unlocked toast raw nil 42 0))
                   (pg::heapam-insert (pg::pg-class-relfilenode table)
                                      (pg::pg-class-columns table) (vector 1 2 pointer) 42 0)))
               (pg::clog-set 42 pg::+clog-committed+)
               (pg::register-active-snapshot snapshot registration)
               (setf (pg::pg-class-checks table)
                     (list (list :name "negative" :validated nil :expression
                                 '(:binop "<" (:col 2 :type :int4) (:const 0 :type :int4)))))
               (setf (symbol-function 'pg::toast-fetch-value)
                     (lambda (&rest args) (declare (ignore args))
                       (error "unchanged payload was fetched")))
               (with-open-file (out out-path :direction :output :if-exists :supersede
                                             :element-type '(unsigned-byte 8))
                 (ok (handler-case
                         (progn (pg::exec-update-subqueries
                                 out (first (pg::parse-statements "UPDATE update_slice SET n=n+1 WHERE id=1")))
                                nil)
                       (pg::pg-error (condition)
                         (string= "23514" (pg::pg-error-sqlstate condition))))))
               (setf (pg::pg-class-checks table)
                     (list (list :name "positive" :validated nil :expression
                                 '(:binop ">" (:col 2 :type :int4) (:const 0 :type :int4)))))
               (with-open-file (out out-path :direction :output :if-exists :supersede
                                             :element-type '(unsigned-byte 8))
                 (pg::exec-update-subqueries
                  out (first (pg::parse-statements "UPDATE update_slice SET n=n+1 WHERE id=1 RETURNING old.n,new.n"))))
               (with-open-file (in out-path :direction :input :element-type '(unsigned-byte 8))
                 (multiple-value-bind (type size payload) (pg::read-message-frame in)
                   (declare (ignore size payload))
                   (ok (= type (char-code #\T))))
                 (multiple-value-bind (type size payload) (pg::read-message-frame in)
                   (declare (ignore size))
                   (ok (= type (char-code #\D)))
                   (ok (equalp payload #(0 2 0 0 0 1 50 0 0 0 1 51))))
                 (multiple-value-bind (type size payload) (pg::read-message-frame in)
                   (declare (ignore size))
                   (ok (= type (char-code #\C)))
                   (ok (equalp payload (pg::string->bytes (format nil "UPDATE 1~c" (code-char 0)))))))
               (setf (symbol-function 'pg::toast-fetch-value) old-fetch)
               (pg::clog-set 43 pg::+clog-committed+)
               (let ((new-snapshot (pg::make-snapshot-struct :xmin 3 :xmax 50 :xip nil :curcid 0))
                     (found nil))
                 (let ((pg::*backend* nil))
                   (pg::heapam-scan
                    (pg::pg-class-relfilenode table) (pg::pg-class-columns table) new-snapshot
                    (lambda (tuple) (push (pg::htup-values tuple) found))))
                 (ok (= 1 (length found)))
                 (ok (= 3 (aref (first found) 1)))
                 (ok (pg::heap-external-varlena-p (aref (first found) 2)))
                 (ok (= (pg::heap-external-varlena-value-id pointer)
                        (pg::heap-external-varlena-value-id (aref (first found) 2))))
                 (ok (equalp raw (pg::toast-fetch-value toast (aref (first found) 2) snapshot)))))
          (setf (symbol-function 'pg::toast-fetch-value) old-fetch)
          (pg::release-snapshot-registration registration)
          (pg::release-relation-writer-xid 43)
          (when pg::*clog-stream* (close pg::*clog-stream*))
          (pg::smgr-close-all)))))

(deftest array-element-update-from-and-repeated-targets
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*wal-page-cache* (make-hash-table :test #'equal))
             (pg::*wal-ready* nil) (pg::*clog-stream* nil)
             (pg::*active-xids* (list 43)) (pg::*next-xid* 50)
             (pg::*logical-capture-xid-modes* (make-hash-table :test #'eql))
             (pg::*catalog* (pg::make-catalog))
             (pg::*backend* (pg::make-backend :database "postgres" :state :in-progress :explicit t :xid 43))
             (database (pg::make-database :name "postgres"))
             (snapshot (pg::make-snapshot-struct :xmin 3 :xmax 50 :xip '(43) :curcid 0))
             (pg::*current-snapshot* snapshot)
             (registration (pg::make-snapshot-registration))
             (out-path (merge-pathnames "array-update-response" root)))
        (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database
              (gethash 43 pg::*logical-capture-xid-modes*) :skip-written)
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (unwind-protect
             (let* ((columns (list (list :name "id" :type :int4 :typmod -1)
                                   (list :name "a" :type :jsonb-array :typmod -1)))
                    (target (pg::create-table* database "array_target" columns))
                    (source (pg::create-table* database "array_source" columns))
                    (array (pg::make-sql-array :element-type-id :jsonb
                                               :elements (vector (pg::sql-jsonb-input "1") (pg::sql-jsonb-input "2"))
                                               :dimensions '(2) :lower-bounds '(1))))
               (pg::with-write-lock
                 (pg::heapam-insert (pg::pg-class-relfilenode target) (pg::pg-class-columns target) (vector 1 array) 42 0)
                 (pg::heapam-insert (pg::pg-class-relfilenode source) (pg::pg-class-columns source) (vector 1 array) 42 0))
               (pg::clog-set 42 pg::+clog-committed+)
               (pg::register-active-snapshot snapshot registration)
               (with-open-file (out out-path :direction :output :if-exists :supersede :element-type '(unsigned-byte 8))
                 (pg::exec-update-subqueries
                  out (first (pg::parse-statements
                              "UPDATE array_target AS t SET a[1:1]=ARRAY[t.a[2]],a[2]=t.a[1] FROM array_source AS s WHERE t.id=s.id"))))
               (let ((sql (format nil "UPDATE array_target SET ~{~a~^,~}"
                                  (loop for i from 1 to 512 collect (format nil "a[1]='~a'::jsonb" i)))))
                 (incf (pg::backend-command-id pg::*backend*))
                 (with-open-file (out out-path :direction :output :if-exists :supersede :element-type '(unsigned-byte 8))
                   (pg::exec-update-subqueries out (first (pg::parse-statements sql)))))
               (let ((sql (format nil "UPDATE array_target SET ~{~a~^,~}"
                                  (loop for i from 1 to 513 collect (format nil "a[1]='~a'::jsonb" i)))))
                 (ok (handler-case (progn (pg::prepare-dml-subqueries (first (pg::parse-statements sql))) nil)
                       (pg::pg-error (condition) (string= "54000" (pg::pg-error-sqlstate condition))))))
               (pg::clog-set 43 pg::+clog-committed+)
               (let ((found nil) (pg::*backend* nil)
                                 (fresh (pg::make-snapshot-struct :xmin 3 :xmax 50 :xip nil :curcid 0)))
                 (pg::heapam-scan (pg::pg-class-relfilenode target) (pg::pg-class-columns target) fresh
                                  (lambda (tuple) (push (pg::htup-values tuple) found)))
                 (ok (= 2 (length found)))
                 (ok (some (lambda (row)
                             (let ((elements (pg::sql-array-elements (aref row 1))))
                               (and (string= "2" (pg::sql-jsonb-output (aref elements 0)))
                                    (string= "1" (pg::sql-jsonb-output (aref elements 1)))))) found))
                 (ok (some (lambda (row)
                             (let ((elements (pg::sql-array-elements (aref row 1))))
                               (string= "512" (pg::sql-jsonb-output (aref elements 0))))) found))))
          (pg::release-snapshot-registration registration)
          (pg::release-relation-writer-xid 43)
          (when pg::*clog-stream* (close pg::*clog-stream*))
          (pg::smgr-close-all)))))

(deftest internal-varlena-array-external-toast-roundtrip
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*wal-page-cache* (make-hash-table :test #'equal))
             (pg::*wal-ready* nil) (pg::*clog-stream* nil)
             (pg::*active-xids* nil) (pg::*catalog* (pg::make-catalog))
             (pg::*backend* (pg::make-backend :database "postgres"))
             (database (pg::make-database :name "postgres"))
             (snapshot (pg::make-snapshot-struct :xmin 3 :xmax 50 :xip nil :curcid 0))
             (registration (pg::make-snapshot-registration))
             (bytes (make-array 10000 :element-type '(unsigned-byte 8)))
             (state 12345))
        (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database)
        (dotimes (i (length bytes))
          (setf state (logand #xffffffff (+ (* state 1664525) 1013904223))
                (aref bytes i) (ldb (byte 8 24) state)))
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (unwind-protect
             (let* ((table (pg::create-table* database "array_toast"
                                              (list (list :name "v" :type :bytea :typmod -1))))
                    (columns (pg::pg-class-columns table))
                    (toast (pg::pg-class-toast table))
                    (array (pg::make-sql-array :element-type-id :bytea :elements (vector bytes pg::+null+)
                                               :dimensions '(2) :lower-bounds '(1)))
                    (physical nil))
               ;; Exercise the internal storage path without enabling SQL array DDL.
               (setf (pg::pg-column-type (aref columns 0)) :bytea-array)
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (pg::with-write-lock
                   (setf physical (pg::toast-externalize-tuple-unlocked toast (vector array) columns 42 0))
                   (pg::heapam-insert (pg::pg-class-relfilenode table) columns physical 42 0)))
               (ok (pg::heap-external-varlena-p (aref physical 0)))
               (pg::clog-set 42 pg::+clog-committed+)
               (pg::register-active-snapshot snapshot registration)
               (let ((found nil))
                 (pg::heapam-scan (pg::pg-class-relfilenode table) columns snapshot
                                  (lambda (tuple) (push tuple found)))
                 (ok (= 1 (length found)))
                 (pg::materialize-select-tuple table (first found) snapshot)
                 (let ((result (aref (pg::htup-values (first found)) 0)))
                   (ok (equalp bytes (aref (pg::sql-array-elements result) 0)))
                   (ok (eq pg::+null+ (aref (pg::sql-array-elements result) 1))))))
          (pg::release-snapshot-registration registration)
          (when pg::*clog-stream* (close pg::*clog-stream*))
          (pg::smgr-close-all)))))

(deftest toast-stores-and-fetches-binary-chunks
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*wal-page-cache* (make-hash-table :test #'equal))
             (pg::*wal-ready* nil)
             (pg::*clog-stream* nil)
             (pg::*active-xids* nil)
             (pg::*catalog* (pg::make-catalog))
             (pg::*backend* (pg::make-backend))
             (database (pg::make-database :name "postgres"))
             (snapshot (pg::make-snapshot-struct :xmin 3 :xmax 50 :xip nil :curcid 0))
             (registration (pg::make-snapshot-registration))
             (bytes (make-array 5000 :element-type '(unsigned-byte 8)
                                     :initial-contents (loop for i below 5000 collect (mod i 256)))))
        (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database)
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (unwind-protect
             (let* ((table (pg::create-table* database "big"
                                              (list (list :name "v" :type :bytea :typmod -1))))
                    (toast (pg::pg-class-toast table))
                    (pointer nil)
                    (parent-block nil)
                    (parent-offset nil))
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (pg::with-write-lock
                   (setf pointer (pg::toast-store-value-unlocked toast bytes #xffffffff 42 0))))
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (pg::with-write-lock
                   (dotimes (i 600)
                     (pg::toast-store-value-unlocked
                      toast (make-array 1 :element-type '(unsigned-byte 8) :initial-element 17)
                      (+ i 1000) 42 0))))
               (ok (> (nth-value 0 (pg::btree-read-meta (pg::pg-toast-index-relfilenode toast))) 0))
               (let ((pg::*toast-next-value-id* #xffffffff)
                     (one-byte (make-array 1 :element-type '(unsigned-byte 8) :initial-element 42)))
                 (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                   (pg::with-write-lock
                     (let ((first (pg::toast-store-value-unlocked toast one-byte nil 43 0)))
                       (ok (= 1 (pg::heap-external-varlena-value-id first))))
                     ;; Aborted chunks remain indexed and reserve their ID.
                     (pg::clog-set 43 pg::+clog-aborted+)
                     ;; Simulate restart: the hint resets but the index retains IDs.
                     (setf pg::*toast-next-value-id* #xffffffff)
                     (let ((second (pg::toast-store-value-unlocked toast one-byte nil 42 0)))
                       (ok (= 2 (pg::heap-external-varlena-value-id second)))))))
               (ok (intake-fails-p (lambda () (pg::toast-fetch-value toast pointer snapshot))))
               (pg::register-active-snapshot snapshot registration)
               (ok (equalp bytes (pg::toast-fetch-value toast pointer snapshot)))
               ;; One value spans many index leaves; a duplicate in its middle
               ;; must remain visible to the prefix visitor after page splits.
               (let* ((large (make-array 1000000 :element-type '(unsigned-byte 8)
                                                 :initial-element 123))
                      (large-pointer
                        (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                          (pg::with-write-lock
                            (pg::toast-store-value-unlocked toast large 999000 42 0)))))
                 (ok (equalp large (pg::toast-fetch-value toast large-pointer snapshot)))
                 (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                   (pg::with-write-lock
                     (multiple-value-bind (block offset)
                         (pg::heapam-insert (pg::pg-toast-relfilenode toast) pg::*toast-chunk-columns*
                                            (vector 999000 200 (subseq large 0 pg::+toast-oid-chunk-size+)) 42 0)
                       (pg::btree-insert-unlocked (pg::pg-toast-index-relfilenode toast)
                                                  (pg::toast-chunk-key 999000 200) block offset))))
                 (ok (equal "XX001" (recovery-state-of
                                     (lambda () (pg::toast-fetch-value toast large-pointer snapshot))))))
               (let* ((logical (vector bytes))
                      (physical (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                                  (pg::with-write-lock
                                    (pg::toast-externalize-tuple-unlocked
                                     toast logical (pg::pg-class-columns table) 42 0)))))
                 (ok (eq (aref logical 0) bytes))
                 (ok (pg::heap-compressed-varlena-p (aref physical 0)))
                 (ok (equalp bytes (aref (pg::deform-heap-tuple
                                             (pg::form-heap-tuple physical (pg::pg-class-columns table)
                                                                  42 0 0 0 1)
                                             (pg::pg-class-columns table)) 0)))
                 (let ((compressed-pointer
                         (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                           (pg::with-write-lock
                             (pg::toast-store-value-unlocked toast bytes nil 42 0 :compress t)))))
                   (ok (< (pg::heap-external-varlena-external-info compressed-pointer)
                          (length bytes)))
                   (ok (equalp bytes (pg::toast-fetch-value toast compressed-pointer snapshot)))
                   (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                     (pg::with-write-lock
                       (pg::toast-delete-value-unlocked toast compressed-pointer 44 0)))
                   (ok (equalp bytes (pg::toast-fetch-value toast compressed-pointer snapshot)))))
               (pg::clog-set 42 pg::+clog-committed+)
               (pg::with-write-lock
                 (multiple-value-setq (parent-block parent-offset)
                   (pg::heapam-insert (pg::pg-class-relfilenode table)
                                      (pg::pg-class-columns table) (vector pointer) 42 0))
                 (let ((invisible-pointer (pg::copy-heap-external-varlena pointer)))
                   (incf (pg::heap-external-varlena-relation-id invisible-pointer))
                   (pg::heapam-insert (pg::pg-class-relfilenode table)
                                      (pg::pg-class-columns table)
                                      (vector invisible-pointer) 60 0)))
               (let ((result nil))
                 (pg::call-with-select-index-lease-domain
                  (list (pg::pg-toast-index-relfilenode toast))
                  (lambda ()
                    (pg::scan-select-path
                     table (pg::pg-class-relfilenode table)
                     (pg::make-select-scan-path :kind :seq-scan) snapshot
                     (lambda (tuple) (push (aref (pg::htup-values tuple) 0) result)))))
                 (ok (equalp (list bytes) result)))
               (let* ((pg::*current-snapshot* snapshot)
                      (relation (list :table table :path (pg::make-select-scan-path :kind :seq-scan)))
                      (node (list :seq-scan 0 (pg::pg-class-oid table))))
                 (pg::call-with-select-index-lease-domain
                  (list (pg::pg-toast-index-relfilenode toast))
                  (lambda ()
                    (let ((iterator (pg::make-select-scan-iterator node (list relation))))
                      (pg::detach-select-index-scans)
                      (unwind-protect
                           (multiple-value-bind (row present) (pg::sql-iterator-next iterator)
                             (ok present)
                             (ok (equalp bytes (aref row 0)))
                             (ok (not (nth-value 1 (pg::sql-iterator-next iterator)))))
                        (pg::close-sql-iterator iterator))))))
               (let* ((index-node 99999)
                      (key (make-array 1 :element-type '(unsigned-byte 8) :initial-element 7))
                      (index (pg::make-pg-index :relfilenode index-node))
                      (path (pg::make-select-scan-path :kind :index-scan :index index :key key))
                      (result nil))
                 (pg::btree-init index-node)
                 (pg::btree-insert index-node key parent-block parent-offset)
                 (pg::call-with-select-index-lease-domain
                  (list index-node (pg::pg-toast-index-relfilenode toast))
                  (lambda ()
                    (pg::scan-select-index table path snapshot
                                           (lambda (tuple)
                                             (push (aref (pg::htup-values tuple) 0) result)))))
                 (ok (equalp (list bytes) result)))
               (let* ((text-value "Zażółć gęślą jaźń")
                      (text-pointer
                        (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                          (pg::with-write-lock
                            (pg::toast-store-value-unlocked toast (pg::string->bytes text-value) 77 42 0))))
                      (text-table (pg::make-pg-class :toast toast
                                                     :columns (vector (pg::make-pg-column :type :varchar))))
                      (tuple (pg::make-htup :values (vector text-pointer))))
                 (pg::call-with-select-index-lease-domain
                  (list (pg::pg-toast-index-relfilenode toast))
                  (lambda ()
                    (pg::materialize-select-tuple text-table tuple snapshot)))
                 (ok (equal text-value (aref (pg::htup-values tuple) 0))))
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (pg::with-write-lock
                   (pg::toast-delete-value-unlocked toast pointer 43 1)
                   (pg::toast-delete-value-unlocked toast pointer 43 1)
                   (dolist (entry (pg::btree-prefix-entries-unlocked
                                   (pg::pg-toast-index-relfilenode toast)
                                   (pg::encode-index-key (list #xffffffff) '(:oid))))
                     (let ((tuple (pg::heapam-get-tuple
                                   (pg::pg-toast-relfilenode toast) (second entry) (third entry)
                                   pg::*toast-chunk-columns* t)))
                       (ok (= 43 (pg::htup-xmax tuple)))))))
               (ok (equalp bytes (pg::toast-fetch-value toast pointer snapshot)))
               (pg::clog-set 43 pg::+clog-aborted+)
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (pg::with-write-lock
                   (pg::toast-delete-value-unlocked toast pointer 44 2)))
               (ok (equalp bytes (pg::toast-fetch-value toast pointer snapshot)))
               (ok (intake-fails-p
                    (lambda ()
                      (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                        (pg::with-write-lock
                          (pg::toast-store-value-unlocked toast bytes #xffffffff 43 0))))))
               (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                 (pg::with-write-lock
                   (dolist (entry (list (list #xffffffff 99) (list 44 7)))
                     (multiple-value-bind (block offset)
                         (pg::heapam-insert
                          (pg::pg-toast-relfilenode toast) pg::*toast-chunk-columns*
                          (vector (first entry) (second entry)
                                  (make-array 1 :element-type '(unsigned-byte 8) :initial-element 1))
                          42 0)
                       (pg::btree-insert-unlocked
                        (pg::pg-toast-index-relfilenode toast)
                        (pg::toast-chunk-key (first entry) (second entry)) block offset)))))
               (ok (equal "XX001" (recovery-state-of (lambda () (pg::toast-fetch-value toast pointer snapshot)))))
               (ok (intake-fails-p
                    (lambda ()
                      (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode toast)))
                        (pg::with-write-lock
                          (pg::toast-store-value-unlocked toast bytes 44 43 0)))))))
          (pg::release-snapshot-registration registration)
          (when pg::*clog-stream* (close pg::*clog-stream*))
          (pg::smgr-close-all)))))

(deftest vacuum-full-rewrites-toast-with-parent-visibility
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*wal-page-cache* (make-hash-table :test #'equal))
             (pg::*wal-ready* nil)
             (pg::*clog-stream* nil)
             (pg::*active-xids* nil)
             (pg::*catalog* (pg::make-catalog))
             (pg::*backend* (pg::make-backend))
             (database (pg::make-database :name "postgres"))
             (snapshot (pg::make-snapshot-struct :xmin 3 :xmax 50 :xip nil :curcid 0))
             (pg::*current-snapshot* snapshot)
             (registration (pg::make-snapshot-registration))
             (bytes (make-array 3000 :element-type '(unsigned-byte 8) :initial-element 42)))
        (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database)
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (unwind-protect
             (let* ((source (pg::create-table* database "source"
                                               (list (list :name "v" :type :bytea :typmod -1))))
                    (target (pg::create-table* database "target"
                                               (list (list :name "v" :type :bytea :typmod -1))))
                    (source-toast (pg::pg-class-toast source))
                    (target-toast (pg::pg-class-toast target))
                    (pointer (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode source-toast)))
                               (pg::with-write-lock
                                 (pg::toast-store-value-unlocked source-toast bytes nil 42 0)))))
               (pg::register-active-snapshot snapshot registration)
               (setf (pg::pg-column-storage (aref (pg::pg-class-columns target) 0)) #\e)
               (let* ((tuple (pg::make-htup :values (vector pointer) :cid 0))
                      (rewritten (pg::vacuum-full-rewrite-values source target tuple 42 44))
                      (new-pointer (aref rewritten 0)))
                 (ok (pg::heap-external-varlena-p new-pointer))
                 (ok (= (pg::heap-external-varlena-relation-id new-pointer)
                        (pg::pg-toast-oid target-toast)))
                 (ok (= (pg::heap-external-varlena-external-info new-pointer) (length bytes)))
                 (ok (equalp bytes (pg::toast-fetch-value target-toast new-pointer snapshot)))
                 (pg::with-btree-relation-locks ((list (pg::pg-toast-index-relfilenode target-toast)))
                   (dolist (entry (pg::btree-prefix-entries-unlocked
                                   (pg::pg-toast-index-relfilenode target-toast)
                                   (pg::encode-index-key
                                    (list (pg::heap-external-varlena-value-id new-pointer)) '(:oid))))
                     (let ((chunk (pg::heapam-get-tuple (pg::pg-toast-relfilenode target-toast)
                                                        (second entry) (third entry)
                                                        pg::*toast-chunk-columns*)))
                       (ok (= (pg::htup-xmin chunk) 42))
                       (ok (= (pg::htup-xmax chunk) 44))))))
               (setf (pg::pg-column-storage (aref (pg::pg-class-columns target) 0)) #\p)
               (let ((rewritten (pg::vacuum-full-rewrite-values
                                 source target (pg::make-htup :values (vector pointer) :cid 0) 42 0)))
                 (ok (equalp (aref rewritten 0) bytes))
                 (ok (not (pg::heap-external-varlena-p (aref rewritten 0))))))
          (pg::release-snapshot-registration registration)
          (when pg::*clog-stream* (close pg::*clog-stream*))
          (pg::smgr-close-all)))))

(deftest toast-fetch-skips-invalid-tuple-before-decoding
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*wal-page-cache* (make-hash-table :test #'equal))
             (pg::*wal-ready* nil)
             (pg::*backend* (pg::make-backend))
             (tuple (pg::form-heap-tuple
                     (vector 42 0 (make-array 1 :element-type '(unsigned-byte 8) :initial-element 5))
                     pg::*toast-chunk-columns* 42 0 0 0 0))
             (page (pg::page-init (pg::make-page))))
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (pg::page-set-u16 tuple pg::+heap-tuple-infomask-off+ pg::+heap-xmin-invalid+)
        (setf (aref tuple (+ (aref tuple pg::+heap-tuple-hoff-off+) 8)) 1)
        (pg::page-add-item page tuple)
        (unwind-protect
             (progn
               (pg::buf-extend 991 page)
               (ok (not (pg::heapam-get-tuple 991 0 1 pg::*toast-chunk-columns* t)))
               (ok (intake-fails-p (lambda ()
                                     (pg::heapam-get-tuple 991 0 1 pg::*toast-chunk-columns*)))))
          (pg::smgr-close-all)))))

(deftest vacuum-toast-advances-owned-horizon
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*wal-ready* nil)
             (pg::*clog-stream* nil)
             (pg::*wal-page-cache* (make-hash-table :test #'equal))
             (pg::*backend* (pg::make-backend))
             (pg::*catalog* (pg::make-catalog))
             (pg::*catalog-oldest-relfrozenxid* nil)
             (pg::*next-xid* 100)
             (columns (vector (pg::make-pg-column :name "chunk" :type :int4 :typmod -1 :attnum 1)))
             (tid nil)
             (database (pg::make-database :name "postgres"))
             (toast (pg::make-pg-toast :oid 901 :relfilenode 901 :index-oid 902
                                       :index-relfilenode 902 :relfrozenxid 3))
             (table (pg::make-pg-class :oid 900 :relfilenode 900 :name "parent"
                                       :relfrozenxid 60 :columns #() :toast toast)))
        (setf (gethash "postgres" (pg::catalog-databases pg::*catalog*)) database
              (gethash "parent" (pg::database-tables database)) table)
        (ensure-directories-exist (pg::catalog-path))
        (pg::wal-ensure-directory-durable (merge-pathnames "base/" root))
        (unwind-protect
             (progn
               (pg::smgr-create 900)
               (pg::smgr-create 901)
               (pg::btree-init 902)
               (pg::clog-set 10 pg::+clog-committed+)
               (multiple-value-bind (block offset)
                   (pg::heapam-insert 901 columns #(7) 10 0)
                 (setf tid (cons block offset))
                 (pg::btree-insert 902 (pg::encode-index-key '(7) '(:int4)) block offset))
               (let ((pg::*catalog-rename-function*
                       (lambda (&rest arguments)
                         (declare (ignore arguments))
                         (error "injected catalog rename failure"))))
                 (ok (intake-fails-p
                      (lambda ()
                        (pg::vacuum-toast-relation
                         table (lambda () t) :forced-oldest-xmin 30
                         :forced-freeze-cutoff 30))))
                 (ok (= 3 (pg::pg-toast-relfrozenxid toast))))
               (pg::vacuum-toast-relation table (lambda () t)
                                          :forced-oldest-xmin 50 :forced-freeze-cutoff 50)
               (ok (= 50 (pg::pg-toast-relfrozenxid toast)))
               (ok (= pg::+frozen-xid+
                      (pg::htup-xmin (pg::heapam-get-tuple 901 (car tid) (cdr tid) columns))))
               (ok (equal (list tid) (pg::btree-search 902 (pg::encode-index-key '(7) '(:int4)))))
               (ok (= 50 (pg::catalog-oldest-relfrozenxid)))
               (let ((synced nil))
                 (pg::vacuum-all-relations-for-xid-wraparound
                  70 :fsync-function (lambda (node) (push node synced)))
                 (ok (and (member 900 synced) (member 901 synced)))
                 (ok (= 70 (pg::pg-class-relfrozenxid table)))
                 (ok (= 70 (pg::pg-toast-relfrozenxid toast))))
               (let* ((restored (pg::load-catalog))
                      (db (gethash "postgres" (pg::catalog-databases restored)))
                      (saved (gethash "parent" (pg::database-tables db))))
                 (ok (= 70 (pg::pg-toast-relfrozenxid (pg::pg-class-toast saved))))))
          (when pg::*clog-stream* (close pg::*clog-stream*))
          (pg::smgr-close-all)))))

(deftest recovery-readers-use-virtual-lock-identities
    (let* ((pg::*physical-recovery-state* (sample-recovery-manifest))
           (pg::*backend* (pg::make-backend))
           (pg::*next-xid* 50) (pg::*active-xids* nil)
           (pg::*recovery-reader-xid-counter* 0)
           (pg::*recovery-reader-xids* (make-hash-table :test #'eql))
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

(deftest recovery-reader-keeps-lock-identity-after-role-change
    (let* ((pg::*physical-recovery-state* (sample-recovery-manifest))
           (pg::*backend* (pg::make-backend :state :in-progress :explicit t
                                            :read-only t))
           (pg::*next-xid* 50)
           (pg::*active-xids* nil)
           (pg::*recovery-reader-xid-counter* 0)
           (pg::*recovery-reader-xids* (make-hash-table :test #'eql))
           (pg::*relation-readers* (make-hash-table :test #'eql))
           (pg::*relation-writers* (make-hash-table :test #'eql))
           (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
           (token (pg::get-current-reader-xid)))
      (unwind-protect
           (progn
             (pg::register-relation-readers '(16384) token)
             (setf pg::*physical-recovery-state* nil)
             (ok (= token (pg::get-current-reader-xid)))
             (pg::register-relation-readers '(16385) (pg::get-current-reader-xid))
             (ok (equal (list token) (gethash 16384 pg::*relation-readers*)))
             (ok (equal (list token) (gethash 16385 pg::*relation-readers*)))
             (ok (null (pg::backend-xid pg::*backend*)))
             (ok (null pg::*active-xids*))
             (ok (= 50 pg::*next-xid*))
             (ok (pg::backend-transaction-read-only-p))
             (pg::reset-backend-transaction pg::*backend*)
             (ok (null (pg::backend-virtual-reader-xid pg::*backend*)))
             (ok (zerop (hash-table-count pg::*relation-readers*)))
             (ok (not (pg::backend-transaction-read-only-p))))
        (pg::reset-backend-transaction pg::*backend*))))

(deftest promoted-reader-registration-keeps-virtual-identity-at-wrap-gate
    (let* ((pg::*physical-recovery-state* nil)
           (pg::*backend* (pg::make-backend :state :in-progress :read-only t
                                            :virtual-reader-xid -7))
           (pg::*next-xid* (+ pg::+xid-cycle+ pg::+first-normal-xid+))
           (pg::*wal-ready* nil)
           (pg::*active-xids* nil)
           (pg::*recovery-reader-xids* (make-hash-table :test #'eql))
           (pg::*relation-readers* (make-hash-table :test #'eql))
           (pg::*relation-writers* (make-hash-table :test #'eql))
           (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql)))
      (unwind-protect
           (progn
             (ok (= -7
                    (pg::with-registered-relation-access (nodes xid :read)
                        '(900) (values)
                      xid)))
             (ok (null (pg::backend-xid pg::*backend*)))
             (ok (equal '(-7) (gethash 900 pg::*relation-readers*))))
        (pg::reset-backend-transaction pg::*backend*))))

(deftest virtual-reader-can-finish-during-pending-wrap
    (let* ((pg::*physical-recovery-state* nil)
           (pg::*backend* (pg::make-backend :state :in-progress :explicit t
                                            :read-only t :virtual-reader-xid -7))
           (pg::*xid-wrap-pending* t)
           (pg::*xid-wrap-failure* nil))
      (ok (handler-case
              (sb-ext:with-timeout 1
                (pg::prepare-xid-wraparound-for-statement pg::*backend*)
                t)
            (sb-ext:timeout () nil)))
      (ok (not (pg::backend-wraparound-waiting pg::*backend*)))))

(deftest virtual-reader-registry-protects-wrap-and-relation-waits
    (let* ((pg::*physical-recovery-state* (sample-recovery-manifest))
           (pg::*backend* (pg::make-backend :state :in-progress :read-only t))
           (pg::*active-xids* nil)
           (pg::*recovery-reader-xids* (make-hash-table :test #'eql))
           (pg::*relation-readers* (make-hash-table :test #'eql))
           (pg::*relation-writers* (make-hash-table :test #'eql))
           (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
           (token (pg::get-current-reader-xid)))
      (unwind-protect
           (progn
             (ok (pg::active-xids-empty-p))
             (ok (not (pg::xid-wrap-users-empty-p)))
             (ok (not (pg::xid-active-p token)))
             (ok (pg::relation-user-active-p token))
             (clrhash pg::*recovery-reader-xids*)
             (ok (= token (pg::get-current-reader-xid)))
             (ok (pg::relation-user-active-p token))
             (setf (pg::backend-cancel-pending pg::*backend*) t)
             (ok (equal "57014" (recovery-state-of (lambda () (pg::wait-for-xid token)))))
             (ok (null (pg::backend-waiting-xid pg::*backend*)))
             (pg::reset-backend-transaction pg::*backend*)
             (ok (pg::xid-wrap-users-empty-p))
             (ok (not (pg::relation-user-active-p token)))
             (pg::wait-for-xid token)
             (ok (null (pg::backend-virtual-reader-xid pg::*backend*))))
        (pg::reset-backend-transaction pg::*backend*))))

(deftest standby-snapshot-without-relation-access-owns-a-virtual-identity
    (let* ((pg::*physical-recovery-state* (sample-recovery-manifest))
           (pg::*backend* (pg::make-backend :state :in-progress :explicit t
                                            :read-only t :isolation-level :repeatable-read))
           (pg::*next-xid* 50) (pg::*active-xids* nil)
           (pg::*active-snapshots* 0)
           (pg::*active-snapshot-set* (make-hash-table :test #'eq))
           (pg::*recovery-reader-xids* (make-hash-table :test #'eql))
           (pg::*relation-readers* (make-hash-table :test #'eql))
           (pg::*relation-writers* (make-hash-table :test #'eql))
           (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql)))
      (unwind-protect
           (let ((snapshot (pg::ensure-transaction-snapshot pg::*backend* 1)))
             (ok (minusp (pg::backend-virtual-reader-xid pg::*backend*)))
             (ok (zerop (hash-table-count pg::*relation-readers*)))
             (ok (not (pg::xid-wrap-users-empty-p)))
             (setf pg::*physical-recovery-state* nil)
             (ok (eq snapshot (pg::ensure-transaction-snapshot pg::*backend* 2)))
             (ok (minusp (pg::get-current-reader-xid)))
             (ok (null (pg::backend-xid pg::*backend*)))
             (ok (= 50 pg::*next-xid*)))
        (pg::reset-backend-transaction pg::*backend*))
      (ok (pg::xid-wrap-users-empty-p))
      (ok (zerop pg::*active-snapshots*))))

(deftest wrap-coordinator-drains-virtual-readers-before-freeze
    (let* ((pg::*physical-recovery-state* (sample-recovery-manifest))
           (pg::*backend* (pg::make-backend :state :in-progress :read-only t))
           (pg::*active-xids* nil)
           (pg::*recovery-reader-xids* (make-hash-table :test #'eql))
           (pg::*relation-readers* (make-hash-table :test #'eql))
           (pg::*relation-writers* (make-hash-table :test #'eql))
           (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
           (registry pg::*recovery-reader-xids*)
           (waiting (sb-thread:make-semaphore))
           (frozen (sb-thread:make-semaphore))
           (worker nil))
      (pg::get-current-reader-xid)
      (unwind-protect
           (progn
             (setf worker
                   (sb-thread:make-thread
                    (lambda ()
                      (let ((pg::*backend* (pg::make-backend))
                            (pg::*next-xid* (+ pg::+xid-cycle+ pg::+first-normal-xid+))
                            (pg::*xid-batch-end* 0)
                            (pg::*active-xids* nil)
                            (pg::*recovery-reader-xids* registry)
                            (pg::*xid-wrap-pending* nil)
                            (pg::*xid-wrap-failure* nil)
                            (pg::*xid-wrap-completed-through* nil))
                        (pg::complete-xid-wraparound
                         :load-marker-function (constantly nil)
                         :save-next-xid-function (constantly nil)
                         :save-marker-function (constantly nil)
                         :wait-for-xids-function
                         (lambda ()
                           (sb-thread:signal-semaphore waiting)
                           (pg::wait-for-no-active-xids))
                         :wait-for-snapshots-function (constantly nil)
                         :freeze-function (lambda (epoch)
                                            (declare (ignore epoch))
                                            (sb-thread:signal-semaphore frozen))
                         :reset-clog-function (constantly nil)
                         :delete-marker-function (constantly nil)
                         :shutdown-function (constantly nil))
                        :completed))
                    :name "virtual reader wrap regression"))
             (ok (sb-thread:wait-on-semaphore waiting :timeout 5))
             (ok (not (sb-thread:wait-on-semaphore frozen :timeout 0.1)))
             (pg::reset-backend-transaction pg::*backend*)
             (ok (eq :completed
                     (sb-thread:join-thread worker :timeout 5 :default :timed-out)))
             (ok (sb-thread:try-semaphore frozen)))
        (pg::reset-backend-transaction pg::*backend*)
        (when worker
          (sb-thread:join-thread worker :timeout 5 :default :timed-out)))))

(deftest virtual-reader-cleanup-releases-identity-on-every-transaction-exit
    (dolist (exit (list #'pg::commit-transaction #'pg::abort-transaction
                        #'pg::abort-transaction-block #'pg::end-backend-session))
      (let* ((pg::*physical-recovery-state* (sample-recovery-manifest))
             (pg::*backend* (pg::make-backend :state :in-progress :explicit t :read-only t))
             (pg::*active-xids* nil)
             (pg::*recovery-reader-xids* (make-hash-table :test #'eql))
             (pg::*relation-readers* (make-hash-table :test #'eql))
             (pg::*relation-writers* (make-hash-table :test #'eql))
             (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
             (token (pg::get-current-reader-xid)))
        (unwind-protect
             (progn
               (pg::register-relation-readers '(16384) token)
               (funcall exit)
               (ok (pg::xid-wrap-users-empty-p))
               (ok (zerop (hash-table-count pg::*relation-readers*)))
               (ok (null (pg::backend-virtual-reader-xid pg::*backend*)))
               (pg::reset-backend-transaction pg::*backend*)
               (ok (pg::xid-wrap-users-empty-p)))
          (pg::reset-backend-transaction pg::*backend*)))))

(deftest virtual-reader-is-not-its-own-wrap-coordinator
    (let ((pg::*backend* (pg::make-backend :virtual-reader-xid -7))
          (pg::*xid-wrap-pending* nil)
          (pg::*xid-wrap-failure* nil)
          (original (symbol-function 'pg::complete-xid-wraparound))
          (called nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::complete-xid-wraparound)
                   (lambda () (setf called t)))
             (pg::prepare-xid-wraparound-for-statement pg::*backend*)
             (ok (not called)))
        (setf (symbol-function 'pg::complete-xid-wraparound) original))))

(deftest virtual-only-commit-defers-interrupt-until-resource-cleanup
    (let* ((pg::*physical-recovery-state* (sample-recovery-manifest))
           (pg::*backend* (pg::make-backend :state :in-progress :explicit t :read-only t))
           (pg::*active-xids* nil)
           (pg::*recovery-reader-xids* (make-hash-table :test #'eql))
           (pg::*relation-readers* (make-hash-table :test #'eql))
           (pg::*relation-writers* (make-hash-table :test #'eql))
           (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
           (original (symbol-function 'pg::wal-forget-xid-under-xid-lock))
           (token (pg::get-current-reader-xid))
           (interrupted nil))
      (pg::register-relation-readers '(16384) token)
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::wal-forget-xid-under-xid-lock)
                   (lambda (xid)
                     (funcall original xid)
                     (sb-thread:interrupt-thread
                      sb-thread:*current-thread*
                      (lambda () (setf interrupted t) (throw 'reader-interrupt :interrupted)))))
             (ok (eq :interrupted
                     (catch 'reader-interrupt (pg::commit-transaction) :not-interrupted)))
             (ok interrupted)
             (ok (pg::xid-wrap-users-empty-p))
             (ok (zerop (hash-table-count pg::*relation-readers*)))
             (ok (null (pg::backend-virtual-reader-xid pg::*backend*)))
             (ok (eq :idle (pg::backend-state pg::*backend*))))
        (setf (symbol-function 'pg::wal-forget-xid-under-xid-lock) original)
        (pg::release-xid token)
        (pg::reset-backend-transaction pg::*backend*))))

(deftest relation-builder-waits-for-abandoned-virtual-reader-cleanup
    (let* ((pg::*physical-recovery-state* (sample-recovery-manifest))
           (pg::*backend* (pg::make-backend :state :in-progress :explicit t
                                            :read-only t :isolation-level :repeatable-read))
           (pg::*next-xid* 50) (pg::*active-xids* nil)
           (pg::*active-snapshots* 0)
           (pg::*active-snapshot-set* (make-hash-table :test #'eq))
           (pg::*recovery-reader-xids* (make-hash-table :test #'eql))
           (pg::*relation-readers* (make-hash-table :test #'eql))
           (pg::*relation-writers* (make-hash-table :test #'eql))
           (pg::*relation-builders* (make-hash-table :test #'eql))
           (pg::*relation-reader-blockers* (make-hash-table :test #'eql))
           (pg::*relation-build-groups* (make-hash-table :test #'eq))
           (pg::*relation-maintainers* (make-hash-table :test #'eql))
           (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
           (pg::*n-connections* 0) (pg::*n-backends* 0)
           (pg::*shutdown-abandoned-backends* nil)
           (registry pg::*recovery-reader-xids*)
           (readers pg::*relation-readers*) (writers pg::*relation-writers*)
           (builders pg::*relation-builders*) (blockers pg::*relation-reader-blockers*)
           (groups pg::*relation-build-groups*) (maintainers pg::*relation-maintainers*)
           (published (sb-thread:make-semaphore))
           (completed (sb-thread:make-semaphore))
           (worker nil)
           (identity (pg::get-current-reader-xid)))
      (pg::ensure-transaction-snapshot pg::*backend* 1)
      (pg::register-relation-readers '(16384) identity)
      (unwind-protect
           (progn
             (setf pg::*physical-recovery-state* nil)
             (setf worker
                   (sb-thread:make-thread
                    (lambda ()
                      (let ((pg::*backend* (pg::make-backend))
                            (pg::*active-xids* nil)
                            (pg::*recovery-reader-xids* registry)
                            (pg::*relation-readers* readers)
                            (pg::*relation-writers* writers)
                            (pg::*relation-builders* builders)
                            (pg::*relation-reader-blockers* blockers)
                            (pg::*relation-build-groups* groups)
                            (pg::*relation-maintainers* maintainers))
                        (let ((token
                                (pg::acquire-relation-build-locks
                                 '(16384) nil t
                                 (lambda (relations token)
                                   (declare (ignore relations token))
                                   (sb-thread:signal-semaphore published)))))
                          (pg::release-relation-build-locks '(16384) token))
                        (sb-thread:signal-semaphore completed)
                        :completed))
                    :name "virtual reader DDL regression"))
             (ok (sb-thread:wait-on-semaphore published :timeout 5))
             (ok (not (sb-thread:wait-on-semaphore completed :timeout 0.1)))
             (ok (= identity (pg::get-current-reader-xid)))
             (pg::register-relation-readers '(16384) identity)
             (pg::postmaster-remember-abandoned-backend pg::*backend*)
             (ok (not (pg::xid-wrap-users-empty-p)))
             (pg::postmaster-retire-abandoned-backends)
             (ok (pg::xid-wrap-users-empty-p))
             (ok (pg::active-snapshots-empty-p))
             (ok (null pg::*shutdown-abandoned-backends*))
             (ok (eq :completed
                     (sb-thread:join-thread worker :timeout 5 :default :timed-out)))
             (ok (zerop (hash-table-count readers)))
             (ok (zerop (hash-table-count builders))))
        (pg::reset-backend-transaction pg::*backend*)
        (when worker
          (sb-thread:join-thread worker :timeout 5 :default :timed-out)))))

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
