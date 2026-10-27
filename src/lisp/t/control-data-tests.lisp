(in-package #:pg-physical-tests)

(deftest legacy-control-data-accepts-a-catalog-larger-than-a-wal-segment
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (identity "large-legacy-control-catalog")
             (expected (pg::catalog-system-identifier
                        (pg::make-catalog :cluster-identity identity)))
             (padding (make-string (* 1024 1024) :initial-element #\Space))
             (path (pg::catalog-path)))
        (ensure-directories-exist path)
        (with-open-file (stream path :direction :output :external-format :utf-8)
          (dotimes (index 17)
            (write-sequence padding stream))
          (prin1 (list :cluster-identity identity) stream))
        (ok (= expected (pg::native-control-catalog-system-id))))))

(deftest wal-control-reader-rejects-nonregular-and-unbounded-input
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (path (pg::wal-control-path))
             (target (merge-pathnames "control-target" root))
             (bytes (pg::wal-encode-control-image 0 0 nil)))
        (ensure-directories-exist path)
        (ok (null (pg::wal-read-control-fields)))
        (with-open-file (stream path :direction :output :element-type '(unsigned-byte 8))
          (write-sequence bytes stream))
        (ok (= 0 (pg::wal-read-control-fields)))
        (with-open-file (stream path :direction :output :if-exists :append)
          (write-line "unexpected second line" stream))
        (ok (intake-fails-p #'pg::wal-read-control-fields))
        (with-open-file (stream path :direction :output :if-exists :supersede)
          (write-string (make-string 514 :initial-element #\x) stream))
        (ok (intake-fails-p #'pg::wal-read-control-fields))
        (delete-file path)
        (with-open-file (stream target :direction :output :element-type '(unsigned-byte 8))
          (write-sequence bytes stream))
        (sb-posix:symlink (namestring target) (namestring path))
        (ok (intake-fails-p #'pg::wal-read-control-fields))
        (delete-file path)
        (sb-posix:mkfifo (namestring path) #o600)
        (ok (intake-fails-p #'pg::wal-read-control-fields))
        (delete-file path))))

(deftest wal-control-preserves-optional-primary-lifecycle-metadata
    (let* ((metadata '(:system-id 18446744073709551615 :db-state 6 :state-time -1))
           (snapshot (sample-checkpoint-snapshot))
           (bytes (pg::wal-encode-control-image 152 300 snapshot :metadata metadata))
           (values (multiple-value-list (pg::wal-decode-control-line (pg::bytes->string bytes)))))
      (ok (= 152 (first values)))
      (ok (equal metadata (seventh values)))
      (ok (= 11 (length (pg::wal-split-fields (pg::bytes->string bytes))))))
  (let* ((bytes (pg::wal-encode-control-image 0 0 nil))
         (values (multiple-value-list (pg::wal-decode-control-line (pg::bytes->string bytes)))))
    (ok (= 8 (length (pg::wal-split-fields (pg::bytes->string bytes)))))
    (ok (null (seventh values)))))

(deftest wal-control-retained-reference-roundtrips-and-is-crc-bound
    (dolist (root-id '(0 18446744073709551615))
      (let* ((reference (list :root-id root-id :base-timeline 1 :base 32
                              :manifest-digest 18446744073709551615 :end 96
                              :end-digest 18446744073709551615))
             (metadata (list :system-id 18446744073709551615 :db-state 6 :state-time -1
                             :retained-reference reference))
             (bytes (pg::wal-encode-control-image 152 300 (sample-checkpoint-snapshot)
                                                  :timeline 2 :namespace :timeline :metadata metadata))
             (loaded (nth-value 6 (pg::wal-decode-control-line (pg::bytes->string bytes))))
             (fields (pg::wal-split-fields (pg::bytes->string bytes))))
        (ok (equal metadata loaded))
        (ok (= 17 (length fields)))
        (ok (<= (length bytes) 513))
        (setf (nth 10 fields) (if (zerop root-id) "1" "0"))
        (ok (signals (pg::wal-decode-control-line (format nil "~{~a~^ ~}~%" fields)) 'error)))))

(deftest wal-control-refresh-preserves-retained-reference
    (let* ((reference '(:root-id 0 :base-timeline 1 :base 32 :manifest-digest 7 :end 96 :end-digest 8))
           (metadata (list :system-id 42 :db-state 6 :state-time 0 :retained-reference reference))
           (refreshed (pg::wal-refresh-control-metadata metadata 4 17)))
      (ok (= 4 (getf refreshed :db-state)))
      (ok (= 17 (getf refreshed :state-time)))
      (ok (equal reference (getf refreshed :retained-reference)))
      (ok (not (eq reference (getf refreshed :retained-reference))))
      (ok (equal metadata
                 (list :system-id 42 :db-state 6 :state-time 0 :retained-reference reference)))))

(deftest wal-control-retained-reference-rejects-invalid-shapes
    (let ((reference '(:root-id 0 :base-timeline 1 :base 32 :manifest-digest 7 :end 96 :end-digest 8)))
      (dolist (bad (list nil (append reference '(:root-id 1))
                         (append reference '(:extra 1))
                         (list* :root-id -1 (cddr reference))
                         (list* :base-timeline 0 (cddr reference))))
        (ok (signals (pg::wal-encode-control-image
                      0 0 nil :metadata
                      (list :system-id 42 :db-state 6 :state-time 0 :retained-reference bad))
                     'error)))
      (dolist (replacement '(:root-id :unknown))
        (let ((bad (copy-list reference)))
          (setf (nth 2 bad) replacement)
          (ok (not (pg::wal-retained-reference-valid-p bad)))))
      (let ((bad (copy-list reference)))
        (setf (getf bad :end) 31)
        (ok (not (pg::wal-retained-reference-valid-p bad))))))

(deftest wal-control-position-rewrites-retain-reference-and-startup-fails-closed
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (reference '(:root-id 0 :base-timeline 1 :base 32 :manifest-digest 7 :end 96 :end-digest 8))
             (pg::*wal-control-metadata*
               (list :system-id 42 :db-state 6 :state-time 0 :retained-reference reference))
             (pg::*wal-checkpoint-snapshot* nil)
             (pg::*wal-insert-timeline* 1)
             (pg::*wal-checkpoint-timeline* 1)
             (pg::*wal-segment-namespace* :legacy))
        (ensure-directories-exist (pg::wal-control-path))
        (pg::wal-save-control-fields 0 96)
        (ok (equal reference (getf (nth-value 6 (pg::wal-read-control-fields)) :retained-reference)))
        (pg::wal-save-control-fields 0 128)
        (ok (= 128 (nth-value 1 (pg::wal-read-control-fields))))
        (ok (equal reference (getf (nth-value 6 (pg::wal-read-control-fields)) :retained-reference)))
        (let ((before (pg::read-bounded-regular-file (pg::wal-control-path) 513)))
          (ok (signals (pg::wal-mark-primary-startup) 'error))
          (ok (equalp before (pg::read-bounded-regular-file (pg::wal-control-path) 513)))))))

(deftest wal-control-state-writer-retains-reference
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*catalog* (pg::make-catalog :cluster-identity "retained-state-writer"))
             (reference '(:root-id 0 :base-timeline 1 :base 32 :manifest-digest 7 :end 96 :end-digest 8))
             (pg::*wal-control-metadata*
               (list :system-id (pg::catalog-system-identifier) :db-state 6 :state-time 0
                     :retained-reference reference))
             (pg::*physical-recovery-state* nil)
             (pg::*physical-recovery-failed-p* nil)
             (pg::*wal-ready* t)
             (pg::*wal-poisoned* nil)
             (pg::*wal-checkpoint-lsn* 0)
             (pg::*wal-durable-lsn* 96)
             (pg::*wal-checkpoint-snapshot* nil)
             (pg::*wal-insert-timeline* 1)
             (pg::*wal-checkpoint-timeline* 1)
             (pg::*wal-segment-namespace* :legacy))
        (ensure-directories-exist (pg::wal-control-path))
        (pg::wal-set-primary-state 4)
        (let ((loaded (nth-value 6 (pg::wal-read-control-fields))))
          (ok (= 4 (getf loaded :db-state)))
          (ok (equal reference (getf loaded :retained-reference)))))))

(deftest wal-control-reference-validation-survives-recomputed-crc
    (let* ((reference '(:root-id 0 :base-timeline 1 :base 32 :manifest-digest 7 :end 96 :end-digest 8))
           (bytes (pg::wal-encode-control-image
                   0 96 nil :metadata
                   (list :system-id 42 :db-state 6 :state-time 0 :retained-reference reference)))
           (fields (butlast (pg::wal-split-fields (pg::bytes->string bytes)))))
      (flet ((rejected (words)
               (let ((prefix (format nil "~{~a~^ ~}" words)))
                 (signals (pg::wal-decode-control-line
                           (format nil "~a ~8,'0X~%" prefix (pg::wal-crc32 (pg::string->bytes prefix))))
                          'error))))
        (dolist (change '((10 "-1") (11 "0") (11 "4294967296") (12 "97")
                          (13 "18446744073709551616") (14 "31") (15 "-1")))
          (let ((bad (copy-list fields)))
            (setf (nth (first change) bad) (second change))
            (ok (rejected bad))))
        (ok (rejected (butlast fields)))
        (ok (rejected (append fields '("0")))))))

(deftest wal-control-rejects-invalid-or-damaged-primary-state
    (dolist (metadata '((:system-id -1 :db-state 6 :state-time 0)
                        (:system-id 1 :db-state 7 :state-time 0)
                        (:system-id 1 :db-state -1 :state-time 0)
                        (:system-id 1 :db-state 6 :state-time 9223372036854775808)
                        (:system-id 1 :db-state 6 :state-time 0 :db-state 1)))
      (ok (intake-fails-p (lambda () (pg::wal-encode-control-image 0 0 nil :metadata metadata)))))
  (let ((bytes (pg::wal-encode-control-image
                0 0 nil :metadata '(:system-id 42 :db-state 6 :state-time 1))))
    (setf (aref bytes (- (length bytes) 3)) (logxor 1 (aref bytes (- (length bytes) 3))))
    (ok (intake-fails-p (lambda () (pg::wal-decode-control-line (pg::bytes->string bytes)))))))

(cl-spec:defproperty wal-control-metadata-retains-state-and-identity
    ((system-id (range integer 0 18446744073709551615))
     (state (range integer 0 6))
     (time (range integer -9223372036854775808 9223372036854775807)))
  (:about pg::wal-encode-control-image)
  (:trials (:normal 100))
  (let ((metadata (list :system-id system-id :db-state state :state-time time)))
    (equal metadata
           (nth-value 6 (pg::wal-decode-control-line
                         (pg::bytes->string
                          (pg::wal-encode-control-image 0 0 nil :metadata metadata)))))))

(deftest generated-wal-control-lifecycle-roundtrips
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'wal-control-metadata-retains-state-and-identity :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest control-data-formatter-uses-checkpoint-start-and-retains-timeline-roles
    (let* ((snapshot (sample-checkpoint-snapshot :timeline 7 :prev-timeline 3
                                                 :next-xid #x100000005 :xid-reserved-through #x100000100))
           (facts (list :system-id 18446744073709551615 :db-state 6 :state-time 0
                        :checkpoint 32 :checkpoint-timeline 7 :snapshot snapshot
                        :recovery-lsn 0 :recovery-timeline 0))
           (rows (pg::native-control-data-rows facts)))
      (flet ((value (name) (cdr (assoc name rows :test #'string=))))
        (ok (string= "0/00000020" (value "Latest checkpoint location")))
        (ok (string= "7" (value "Latest checkpoint's TimeLineID")))
        (ok (string= "3" (value "Latest checkpoint's PrevTimeLineID")))
        (ok (string= "1:5" (value "Latest checkpoint's NextXID")))
        (ok (string= "000000070000000000000000" (value "Latest checkpoint's REDO WAL file")))
        (ok (string= "18446744073709551615" (value "Database system identifier")))
        (ok (string= "in production" (value "Database cluster state"))))
      (setf (getf facts :db-state) nil)
      (ok (string= "unrecognized status code"
                   (cdr (assoc "Database cluster state" (pg::native-control-data-rows facts) :test #'string=))))))

(deftest control-data-directory-options-match-the-supported-c-cli
    (dolist (arguments '(("/tmp/data") ("-D" "/tmp/data") ("--pgdata=/tmp/data") ("-D/tmp/data")))
      (ok (string= "/tmp/data" (pg::native-controldata-directory arguments))))
  (ok (string= "-directory" (pg::native-controldata-directory '("--" "-directory"))))
  (dolist (arguments '(("-D") ("--unknown") ("a" "b") ("a" "-D" "b") ("-D" "a" "b")))
    (ok (intake-fails-p (lambda () (pg::native-controldata-directory arguments))))))

(deftest terminal-shutdown-fence-cannot-be-reopened-by-a-quiescence-owner
    (let* ((owner (gensym))
           (pg::*live-operation-depth* 0)
           (pg::*live-operation-count* 0)
           (pg::*live-operation-terminal-shutdown* nil)
           (pg::*live-operation-admission-open* nil)
           (pg::*live-operation-drain-open* nil)
           (pg::*live-operation-quiescence-owner* owner)
           (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
           (pg::*live-operation-before-open-hook* nil)
           (pg::*live-operation-pending-cancel-requests* nil))
      (pg::fence-live-operation-shutdown)
      (pg::open-live-operation-admission owner)
      (ok pg::*live-operation-terminal-shutdown*)
      (ok (not pg::*live-operation-admission-open*))
      (ok (not (pg::live-operation-admitted-p t)))))
