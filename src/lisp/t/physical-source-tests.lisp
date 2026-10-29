;;;; Source pin tests isolate retention decisions; placeholder segments are not replay fixtures.
(in-package #:pg-physical-tests)

(defun call-with-physical-source-fixture (function)
  (with-physical-store-directory (root)
    (let* ((pg::*pgdata* root)
           (pg::*catalog* (pg::make-catalog))
           (pg::*physical-source-pins* (make-hash-table :test #'equal))
           (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
           (pg::*wal-replication-slots* (make-hash-table :test #'equal))
           (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
           (pg::*wal-output-stream* nil) (pg::*wal-output-segment* nil)
           (pg::*wal-poisoned* nil)
           (pg::*pm-requested-mode* pg::+no-shutdown+)
           (pg::*wal-checkpoint-lsn* (* 2 pg::+wal-segment-size+))
           (pg::*wal-durable-lsn* (+ (* 3 pg::+wal-segment-size+) 64)))
      (pg::wal-ensure-directory-durable (pg::wal-directory))
      (loop for segment below 4
            do (with-open-file (stream (pg::wal-segment-path segment)
                                       :direction :output :element-type '(unsigned-byte 8))
                 (write-byte 0 stream)))
      (let* ((start (- pg::*wal-checkpoint-lsn* pg::+wal-header-size+))
             (record (legacy-checkpoint-record start)))
        (with-open-file (stream (pg::wal-segment-path 1) :direction :io :if-exists :overwrite
                                                         :element-type '(unsigned-byte 8))
          (file-position stream (mod start pg::+wal-segment-size+))
          (write-sequence record stream)
          (pg::fsync-stream stream)))
      (funcall function))))

(deftest physical-source-legacy-pin-is-readable-before-handoff
    (call-with-physical-source-fixture
     (lambda ()
       (let* ((pin (pg::physical-source-pin-create "legacy"))
              (prefix (format nil "PGPHYSPIN1 legacy ~d 1 ~d ~d"
                              (pg::physical-source-pin-system-id pin)
                              (pg::physical-source-pin-restart-lsn pin)
                              (pg::physical-source-pin-consistent-lsn pin)))
              (crc (pg::wal-crc32 (pg::string->bytes prefix))))
         (pg::wal-atomic-write-bytes
          (pg::physical-source-pin-path "legacy")
          (pg::string->bytes (format nil "~a ~8,'0X~%" prefix crc)))
         (let ((loaded (pg::physical-source-pin-read (pg::physical-source-pin-path "legacy"))))
           (ok (null (pg::physical-source-pin-confirmed-lsn loaded)))
           (ok (= #xCBF29CE484222325 (pg::physical-source-pin-prefix-digest loaded)))
           (ok (not (pg::physical-source-pin-lost-p loaded))))))))

(deftest physical-source-confirmation-persists-before-recycling
    (call-with-physical-source-fixture
     (lambda ()
       (let* ((pg::*physical-source-senders* (make-hash-table :test #'equal))
              (pg::*max-slot-wal-keep-bytes* nil)
              (pin (pg::physical-source-pin-create "advance"))
              (owner (gensym "SENDER"))
              (received (+ (* 3 pg::+wal-segment-size+) 32)))
         (setf (gethash "advance" pg::*physical-source-senders*) owner)
         ;; This fixture isolates retention; the protocol tests prove record/digest membership.
         (pg::physical-source-confirm-receipt "advance" owner received 456)
         (ok (= received (pg::physical-source-pin-confirmed-lsn pin)))
         (ok (= received (gethash pin pg::*wal-snapshot-retention-pins*)))
         (pg::wal-remove-checkpointed-segments pg::*wal-durable-lsn*)
         (ok (equal '(3) (pg::wal-discover-segments)))
         (pg::physical-source-pin-load)
         (let ((loaded (gethash "advance" pg::*physical-source-pins*)))
           (ok (= received (pg::physical-source-pin-confirmed-lsn loaded)))
           (ok (= 456 (pg::physical-source-pin-prefix-digest loaded)))
           (ok (not (pg::physical-source-pin-lost-p loaded))))
         (ok (intake-fails-p
              (lambda () (pg::physical-source-confirm-receipt "advance" owner (1- received) 456))))
         (ok (not pg::*wal-poisoned*))))))

(deftest physical-source-failed-save-keeps-conservative-runtime-floor
    (dolist (point '(:before-save :after-save))
      (call-with-physical-source-fixture
       (lambda ()
         (let* ((pg::*physical-source-senders* (make-hash-table :test #'equal))
                (pin (pg::physical-source-pin-create "advance"))
                (old (pg::physical-source-pin-restart-lsn pin))
                (owner (gensym "SENDER"))
                (received (+ (* 3 pg::+wal-segment-size+) 32))
                (pg::*physical-source-test-hook*
                  (lambda (stage candidate)
                    (declare (ignore candidate))
                    (when (eq stage point) (error "injected source pin publication failure")))))
           (setf (gethash "advance" pg::*physical-source-senders*) owner)
           (ok (intake-fails-p
                (lambda () (pg::physical-source-confirm-receipt "advance" owner received 456))))
           (ok (= old (gethash pin pg::*wal-snapshot-retention-pins*)))
           (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
           (let ((pg::*physical-source-test-hook* nil)
                 (loaded (pg::physical-source-pin-read (pg::physical-source-pin-path "advance"))))
             (ok (if (eq point :before-save)
                     (null (pg::physical-source-pin-confirmed-lsn loaded))
                     (= received (pg::physical-source-pin-confirmed-lsn loaded))))))))))

(deftest physical-source-retention-cap-loss-is-durable-and-terminal
    (call-with-physical-source-fixture
     (lambda ()
       (let* ((pg::*max-slot-wal-keep-bytes* 64)
              (pin (pg::physical-source-pin-create "lagged"))
              (snapshot (list :unrelated-snapshot)))
         (setf (gethash snapshot pg::*wal-snapshot-retention-pins*) 0)
         (pg::wal-remove-checkpointed-segments pg::*wal-durable-lsn*)
         (ok (pg::physical-source-pin-lost-p pin))
         (ok (not (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (eql 0 (gethash snapshot pg::*wal-snapshot-retention-pins*)))
         (ok (equal '(0 1 2 3) (pg::wal-discover-segments)))
         (remhash snapshot pg::*wal-snapshot-retention-pins*)
         (pg::wal-remove-checkpointed-segments pg::*wal-durable-lsn*)
         (ok (equal '(3) (pg::wal-discover-segments)))
         ;; A lost marker survives startup without blocking the entire primary.
         (pg::physical-source-pin-load)
         (ok (pg::physical-source-pin-lost-p (gethash "lagged" pg::*physical-source-pins*)))
         (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*)))))))

(deftest private-proof-startup-stabilizes-before-replacing-retention
    (dolist (failure '(nil :file :directory))
      (call-with-physical-source-fixture
       (lambda ()
         (let* ((pin (pg::physical-source-pin-create "startup_sync"))
                (old-table pg::*physical-source-pins*)
                (floor (pg::physical-source-pin-restart-lsn pin)))
           (call-with-metadata-sync-observer
            (lambda (events streams)
              (pg::physical-source-pin-read (pg::physical-source-pin-path "startup_sync"))
              (ok (null (car events)))
              (if failure
                  (progn
                    (ok (intake-fails-p #'pg::physical-source-pin-load))
                    (ok pg::*wal-poisoned*)
                    (ok (eq old-table pg::*physical-source-pins*))
                    (ok (= floor (gethash pin pg::*wal-snapshot-retention-pins*))))
                  (progn
                    (pg::physical-source-pin-load)
                    (ok (not (eq old-table pg::*physical-source-pins*)))
                    (ok (equal '(:directory :file :directory) (mapcar #'first (reverse (car events)))))
                    (let ((loaded (gethash "startup_sync" pg::*physical-source-pins*)))
                      (ok (not (eq pin loaded)))
                      (ok (= floor (gethash loaded pg::*wal-snapshot-retention-pins*))))
                    (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))))
              (ok (every (complement #'open-stream-p) (car streams))))
            :fail-at failure
            :before-sync (lambda (stage)
                           (declare (ignore stage))
                           (ok (eq old-table pg::*physical-source-pins*))
                           (ok (= floor (gethash pin pg::*wal-snapshot-retention-pins*))))))))))

(deftest empty-private-proof-inventory-stabilizes-deletion-before-dropping-floor
    (dolist (failure '(nil :directory))
      (call-with-physical-source-fixture
       (lambda ()
         (let* ((pin (pg::physical-source-pin-create "removed_sync"))
                (old-table pg::*physical-source-pins*)
                (floor (pg::physical-source-pin-restart-lsn pin)))
           (delete-file (pg::physical-source-pin-path "removed_sync"))
           (call-with-metadata-sync-observer
            (lambda (events streams)
              (if failure
                  (progn
                    (ok (intake-fails-p #'pg::physical-source-pin-load))
                    (ok pg::*wal-poisoned*)
                    (ok (eq old-table pg::*physical-source-pins*))
                    (ok (= floor (gethash pin pg::*wal-snapshot-retention-pins*))))
                  (progn
                    (pg::physical-source-pin-load)
                    (ok (zerop (hash-table-count pg::*physical-source-pins*)))
                    (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))))
              (ok (equal '(:directory) (mapcar #'first (reverse (car events)))))
              (ok (null (car streams))))
            :fail-at failure
            :before-sync (lambda (stage)
                           (declare (ignore stage))
                           (ok (eq old-table pg::*physical-source-pins*))
                           (ok (= floor (gethash pin pg::*wal-snapshot-retention-pins*))))))))))

(deftest physical-source-retention-survives-reload
    (call-with-physical-source-fixture
     (lambda ()
       (let ((pin (pg::physical-source-pin-create "bootstrap")))
         (ok (= (- (* 2 pg::+wal-segment-size+) 32) (pg::physical-source-pin-restart-lsn pin)))
         (ok (= (* 2 pg::+wal-segment-size+) (pg::physical-source-pin-consistent-lsn pin)))
         (ok (probe-file (pg::physical-source-pin-path "bootstrap"))))
       (clrhash pg::*physical-source-pins*)
       (clrhash pg::*wal-snapshot-retention-pins*)
       (pg::physical-source-pin-load)
       (ok (= 1 (hash-table-count pg::*physical-source-pins*)))
       (pg::wal-remove-checkpointed-segments pg::*wal-durable-lsn*)
       ;; The older checkpoint record ends exactly at the next segment's start:
       ;; retaining only its end would incorrectly recycle segment 1.
       (ok (equal '(1 2 3) (pg::wal-discover-segments)))
       (pg::physical-source-pin-release "bootstrap")
       (ok (not (probe-file (pg::physical-source-pin-path "bootstrap"))))
       (pg::wal-remove-checkpointed-segments pg::*wal-durable-lsn*)
       (ok (equal '(3) (pg::wal-discover-segments))))))

(deftest physical-source-pin-before-first-checkpoint
    (call-with-physical-source-fixture
     (lambda ()
       (let ((pg::*wal-checkpoint-lsn* 0))
         (let ((pin (pg::physical-source-pin-create "initial")))
           (ok (zerop (pg::physical-source-pin-restart-lsn pin)))
           (ok (zerop (pg::physical-source-pin-consistent-lsn pin)))))
       (pg::physical-source-pin-load)
       (pg::wal-remove-checkpointed-segments pg::*wal-durable-lsn*)
       (ok (equal '(0 1 2 3) (pg::wal-discover-segments)))
       (pg::physical-source-pin-release "initial")
       (pg::wal-remove-checkpointed-segments pg::*wal-durable-lsn*)
       (ok (equal '(3) (pg::wal-discover-segments))))))

(deftest physical-source-pins-preserve-unrelated-snapshot-pins
    (call-with-physical-source-fixture
     (lambda ()
       (let ((snapshot (list :snapshot)))
         (setf (gethash snapshot pg::*wal-snapshot-retention-pins*) 0)
         (pg::physical-source-pin-create "bootstrap")
         (pg::physical-source-pin-load)
         (ok (= 2 (hash-table-count pg::*wal-snapshot-retention-pins*)))
         (pg::physical-source-pin-release "bootstrap")
         (ok (= 1 (hash-table-count pg::*wal-snapshot-retention-pins*)))
         (ok (eql 0 (gethash snapshot pg::*wal-snapshot-retention-pins*)))))))

(deftest physical-source-pin-identity-and-checkpoint-validation
    (call-with-physical-source-fixture
     (lambda ()
       (pg::physical-source-pin-create "bootstrap")
       (ok (intake-fails-p (lambda () (pg::physical-source-pin-create "bootstrap"))))
       (ok (not pg::*wal-poisoned*))
       (let ((pg::*catalog* (pg::make-catalog)))
         (ok (intake-fails-p
              (lambda () (pg::physical-source-pin-read (pg::physical-source-pin-path "bootstrap"))))))
       (with-open-file (stream (pg::wal-segment-path 1) :direction :io :if-exists :overwrite
                                                        :element-type '(unsigned-byte 8))
         (file-position stream (- pg::+wal-segment-size+ 32))
         (write-byte 0 stream)
         (pg::fsync-stream stream))
       (ok (intake-fails-p
            (lambda () (pg::physical-source-pin-read (pg::physical-source-pin-path "bootstrap")))))
       (ok (= 1 (hash-table-count pg::*physical-source-pins*))))))
