;;;; Cold pending reconstruction preserves write exclusion and original progress.
(in-package #:pg-physical-tests)

(deftest pending-store-validation-preserves-unacknowledged-tail-and-requires-existing-lock
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (pg::physical-store-input store 0 (sample-physical-record 0))
        (pg::physical-store-flush store)
        (pg::physical-store-close store))
      (let* ((path (merge-pathnames "0000000000000000.wal" root))
             (bytes (concatenate '(vector (unsigned-byte 8)) (lineage-test-bytes path) #(9 8 7))))
        (pg::wal-atomic-write-bytes path bytes)
        (let ((store (pg::physical-store-open root 123 1 :discard-tail nil :create-lock nil)))
          (unwind-protect
               (progn (ok (= 32 (pg::physical-store-flushed-lsn store)))
                      (ok (equalp bytes (lineage-test-bytes path))))
            (pg::physical-store-close store)))
        (delete-file (merge-pathnames "receive.lock" root))
        (ok (intake-fails-p (lambda () (pg::physical-store-open root 123 1 :discard-tail nil :create-lock nil))))
        (ok (not (probe-file (merge-pathnames "receive.lock" root))))
        (ok (equalp bytes (lineage-test-bytes path))))))

(deftest pending-store-validation-rejects-lock-symlinks
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t)))
        (pg::physical-store-close store))
      (delete-file (merge-pathnames "receive.lock" root))
      (sb-posix:symlink "/missing-pending-receive-lock" (namestring (merge-pathnames "receive.lock" root)))
      (ok (intake-fails-p (lambda () (pg::physical-store-open root 123 1 :discard-tail nil :create-lock nil))))
      (ok (not (probe-file #P"/missing-pending-receive-lock")))))

(deftest pending-resume-requires-cold-owned-exclusion
    (ok (intake-fails-p #'pg::physical-promotion-resume-pending)))

(deftest shared-recovery-marker-preserves-bytes-and-rejects-special-files
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (path (merge-pathnames "recovery.required" root)))
        (pg::physical-bootstrap-mark-target root)
        (ok (equalp (pg::string->bytes (format nil "PGPHYSICAL-RECOVERY-REQUIRED-1~%"))
                    (lineage-test-bytes path)))
        (ok (progn (pg::physical-recovery-check-required-marker) t))
        (pg::wal-atomic-write-bytes path #(1 2 3))
        (ok (intake-fails-p #'pg::physical-recovery-check-required-marker))
        (delete-file path)
        (sb-posix:mkfifo (namestring path) #o600)
        (ok (intake-fails-p #'pg::physical-recovery-check-required-marker))
        (sb-posix:unlink (namestring path))
        (sb-posix:symlink "/missing-pending-marker" (namestring path))
        (unwind-protect (ok (intake-fails-p #'pg::physical-recovery-check-required-marker))
          (sb-posix:unlink (namestring path))))))

(cl-spec:defproperty pending-open-never-trims-generated-unacknowledged-bytes
    ((length (range integer 0 256)) (byte (range integer 0 255)))
  (:about pg::physical-store-open)
  (:trials (:normal 50))
  (with-physical-store-directory (root)
    (let ((store (pg::physical-store-open root 123 1 :create t)))
      (pg::physical-store-input store 0 (sample-physical-record 0))
      (pg::physical-store-flush store)
      (pg::physical-store-close store))
    (let* ((path (merge-pathnames "0000000000000000.wal" root))
           (bytes (concatenate '(vector (unsigned-byte 8)) (sample-physical-record 0)
                               (make-array length :element-type '(unsigned-byte 8) :initial-element byte))))
      (pg::wal-atomic-write-bytes path bytes)
      (let ((store (pg::physical-store-open root 123 1 :discard-tail nil :create-lock nil)))
        (unwind-protect
             (and (= 32 (pg::physical-store-flushed-lsn store))
                  (equalp bytes (lineage-test-bytes path)))
          (pg::physical-store-close store))))))

(deftest generated-pending-store-open
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'pending-open-never-trims-generated-unacknowledged-bytes :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
