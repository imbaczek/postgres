(in-package #:pg-physical-tests)

(defun call-with-slot-standby-fixture (function &key compact (replay 96) (system-id 123))
  (with-retained-root-registry
      (call-with-restartpoint-fixture
       (lambda (root store)
         (let ((pg::*wal-insert-timeline* 1)
               (pg::*wal-segment-namespace* :legacy))
           (setf (getf pg::*physical-recovery-state* :phase) 2
                 (getf pg::*physical-recovery-state* :required-applied) replay
                 pg::*last-replay-lsn* replay
                 (pg::physical-store-retained-identity store) (pg::physical-recovery-retained-identity))
           (pg::wal-atomic-write-bytes
            (pg::physical-recovery-control-path)
            (pg::physical-recovery-control-image 2 system-id 1 77 32 replay 50))
           (when compact
             (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish))
           (funcall function root store)))
       :system-id system-id)))

(cl-spec:defproperty physical-slot-positions-follow-prefix-history
    ((restart (range integer 1 128))
     (frontier (range integer 1 128))
     (timeline (range integer 1 2))
     (mode (range integer 0 2)))
  (:about pg::physical-slot-check-position)
  (:trials (:normal 200))
  (let* ((history (pg::timeline-parse-history
                   2 (format nil "1~c0/00000040~cpromoted~%" #\Tab #\Tab)))
         (slot (physical-slot-test-object
                :restart (unless (zerop mode) restart)
                :timeline (unless (zerop mode) timeline)))
         (reserved (= mode 1))
         (allowed (or (not reserved)
                      (and (<= restart frontier)
                           (if (= timeline 1) (<= restart 64) (>= restart 64))))))
    (when (= mode 2) (setf (pg::wal-replication-slot-wal-status slot) :lost))
    (handler-case
        (let ((result (pg::physical-slot-check-position slot 42 history frontier)))
          (and allowed (eql result (and reserved restart))))
      (error () (not allowed)))))

(deftest generated-physical-slot-prefix-positions
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'physical-slot-positions-follow-prefix-history :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest standby-slot-validation-rejects-lifecycle-and-replay-timeline-mismatches
    (call-with-slot-standby-fixture
     (lambda (root store)
       (declare (ignore root))
       (let ((slot (physical-slot-test-object :system-id 123)))
         (let ((pg::*physical-receive-store* nil))
           (ok (signals (pg::physical-slot-check-standby-slots (list slot) 123) 'error)))
         (setf (getf pg::*physical-recovery-state* :replay-timeline) 2)
         (ok (signals (pg::physical-slot-check-standby-slots (list slot) 123) 'error))
         (setf (getf pg::*physical-recovery-state* :replay-timeline) 1)
         (let ((pg::*last-replay-lsn* (1+ (pg::physical-store-flushed-lsn store))))
           (ok (signals (pg::physical-slot-check-standby-slots (list slot) 123) 'error)))
         (setf (getf pg::*physical-recovery-state* :phase) 1
               pg::*last-replay-lsn* 32)
         (pg::wal-atomic-write-bytes
          (pg::physical-recovery-control-path)
          (pg::physical-recovery-control-image 1 123 1 77 32 32 50))
         (ok (signals (pg::physical-slot-check-standby-slots (list slot) 123) 'error))))))

(deftest base-only-standby-slots-use-the-verified-base-frontier
    (call-with-slot-standby-fixture
     (lambda (root store)
       (declare (ignore root store))
       (let ((pg::*physical-receive-store* nil)
             (pg::*last-replay-lsn* 32)
             (slot (physical-slot-test-object :restart 1 :system-id 123)))
         (setf (getf pg::*physical-recovery-state* :phase) 1
               (getf pg::*physical-recovery-state* :required-applied) 32)
         (pg::wal-atomic-write-bytes
          (pg::physical-recovery-control-path)
          (pg::physical-recovery-control-image 1 123 1 77 32 32 50))
         (ok (pg::physical-slot-check-standby-slots (list slot) 123))
         (setf (pg::wal-replication-slot-restart-lsn slot) 33)
         (ok (signals (pg::physical-slot-check-standby-slots (list slot) 123) 'error))))))

(deftest standby-slots-distinguish-the-parent-fork-from-child-eor-replay
    (call-with-slot-standby-fixture
     (lambda (root store)
       (declare (ignore root))
       (let ((slot (physical-slot-test-object :restart 33 :system-id 123)))
         (selected-replay-test-prepare store)
         (ok (= 152 (pg::physical-store-flushed-lsn store)))
         (ok (= 96 pg::*last-replay-lsn*))
         (ok (pg::physical-slot-check-standby-slots (list slot) 123))
         (setf (pg::wal-replication-slot-restart-lsn slot) 97
               (pg::wal-replication-slot-restart-timeline slot) 2)
         (ok (signals (pg::physical-slot-check-standby-slots (list slot) 123) 'error))
         (pg::clog-init)
         (unwind-protect
              (progn
                (ok (= 152 (pg::physical-replay-from 96)))
                (ok (= 2 (pg::physical-recovery-replay-timeline)))
                (ok (pg::physical-slot-check-standby-slots (list slot) 123))
                (setf (getf pg::*physical-recovery-state* :replay-timeline) 1)
                (ok (signals (pg::physical-slot-check-standby-slots (list slot) 123) 'error)))
           (close pg::*clog-stream*)
           (setf pg::*clog-stream* nil))))))

(deftest standby-slot-loader-restores-physical-state-after-replay
    (let ((system-id (pg::catalog-system-identifier
                      (pg::make-catalog :cluster-identity "history-source"))))
      (call-with-slot-standby-fixture
       (lambda (root store)
         (declare (ignore root store))
         (let* ((pg::*catalog* (pg::make-catalog :cluster-identity "stale-warm-image"))
                (pg::*wal-replication-slots* (make-hash-table :test #'equal))
                (slot (physical-slot-test-object :restart 33 :system-id system-id))
                (temporary (physical-slot-test-object :name "dead_temporary" :temporary t)))
           (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
           (pg::wal-save-replication-slot slot)
           (pg::wal-save-replication-slot temporary)
           (pg::wal-load-replication-slots)
           (let ((restored (gethash "public_physical" pg::*wal-replication-slots*)))
             (ok (pg::wal-physical-slot-p restored))
             (ok (= 33 (pg::wal-replication-slot-restart-lsn restored)))
             (ok (plusp (pg::wal-replication-slot-inactive-since restored))))
           (ok (not (probe-file (pg::wal-replication-slot-path "dead_temporary"))))
           (setf (pg::wal-replication-slot-restart-lsn slot) 97)
           (pg::wal-save-replication-slot slot)
           (ok (signals (pg::wal-load-replication-slots) 'error))))
       :system-id system-id)))

(deftest standby-slot-positions-are-bounded-by-replay-not-receipt
    (call-with-slot-standby-fixture
     (lambda (root store)
       (declare (ignore root store))
       (let ((slot (physical-slot-test-object :restart 33 :system-id 123)))
         (ok (pg::physical-slot-check-standby-slots (list slot) 123))
         (setf (pg::wal-replication-slot-restart-lsn slot) 65)
         (ok (signals (pg::physical-slot-check-standby-slots (list slot) 123) 'error))
         (setf (pg::wal-replication-slot-restart-lsn slot) 64)
         (ok (pg::physical-slot-check-standby-slots (list slot) 123))
         (ok (signals (pg::physical-slot-check-standby-slots (list slot) 124) 'error))
         (setf (pg::wal-replication-slot-restart-timeline slot) 2)
         (ok (signals (pg::physical-slot-check-standby-slots (list slot) 123) 'error))))
     :replay 64))

(deftest standby-slots-use-retained-bytes-instead-of-stale-spool-prefix
    (call-with-slot-standby-fixture
     (lambda (root store)
       (declare (ignore root))
       (let* ((slot (physical-slot-test-object :restart 33 :system-id 123))
              (archive (pg::physical-store-retained-directory store))
              (index (pg::physical-follow-controller-index store))
              (entry (first (getf index :entries)))
              (path (pg::physical-retained-object-path archive :extent (first entry)))
              (hidden (make-pathname :type "held" :defaults path)))
         (with-open-file (stream (pg::physical-store-segment-path store 0)
                                 :direction :io :element-type '(unsigned-byte 8)
                                 :if-exists :overwrite)
           (file-position stream 40)
           (write-byte 255 stream))
         (ok (pg::physical-slot-check-standby-slots (list slot) 123))
         (rename-file path hidden)
         (unwind-protect
              (ok (signals (pg::physical-slot-check-standby-slots (list slot) 123) 'error))
           (rename-file hidden path))
         (ok (not (pg::physical-store-failed-p store)))))
     :compact t))

(deftest standby-slot-prebase-coverage-is-bound-to-immutable-overlap
    (call-with-slot-standby-fixture
     (lambda (root store)
       (declare (ignore store))
       (let ((slot (physical-slot-test-object :restart 1 :system-id 123))
             (path (merge-pathnames "pg_wal/0000000000000000.wal" root)))
         (ok (pg::physical-slot-check-standby-slots (list slot) 123))
         (with-open-file (stream path :direction :io :element-type '(unsigned-byte 8)
                                      :if-exists :overwrite)
           (write-byte 255 stream))
         (ok (signals (pg::physical-slot-check-standby-slots (list slot) 123) 'error))))))

(deftest standby-null-and-lost-slots-do-not-demand-wal-bytes
    (call-with-slot-standby-fixture
     (lambda (root store)
       (let ((unreserved (physical-slot-test-object :system-id 123))
             (lost (physical-slot-test-object :name "lost_physical" :restart 33 :system-id 123)))
         (setf (pg::wal-replication-slot-wal-status lost) :lost)
         (delete-file (merge-pathnames "pg_wal/0000000000000000.wal" root))
         (delete-file (pg::physical-store-segment-path store 0))
         (ok (pg::physical-slot-check-standby-slots (list unreserved lost) 123))))))
