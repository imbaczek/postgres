(in-package #:pg-physical-tests)

(defun call-with-reseed-original-base-fixture (function)
  (call-with-reseed-follow-installed-fixture
   (lambda (root stage incoming quarantine plan)
     (declare (ignore stage incoming quarantine plan))
     (let ((fd nil))
       (unwind-protect
            (progn
              (setf fd (pg::physical-backup-open-directory root))
              (pg::call-with-physical-backup-image-directory
               fd
               (lambda (directory)
                 (let ((pg::*pgdata* directory))
                   (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline namespace)
                       (pg::wal-read-control-fields)
                     (declare (ignore checkpoint durable snapshot))
                     (let ((pg::*wal-segment-namespace* namespace)
                           (pg::*wal-insert-timeline* insertion)
                           (pg::*wal-checkpoint-timeline* checkpoint-timeline))
                       (funcall function root directory (pg::physical-recovery-read-control directory))))))))
         (when fd (sb-posix:close fd)))))))

(deftest reseed-original-base-control-core-matches-public-verification
    (call-with-reseed-original-base-fixture
     (lambda (root directory control)
       (declare (ignore root))
       (let* ((public (multiple-value-list (pg::physical-recovery-verify-base-with-namespace)))
              (before (copy-tree control))
              (core (multiple-value-list (pg::physical-recovery-verify-base-control-with-namespace control))))
         (let ((same (equalp public core))) (ok same))
         (ok (equalp before control))
         (ok (pg::recovery-check-image-directories (first core) directory))
         (ok (zerop (getf (second core) :phase)))))))

(deftest reseed-original-base-control-core-retains-immutable-file-checks
    (dolist (name '("PG_VERSION" "pg_xact" "global/pg_catalog.dat" "global/pg_wal_control" :checkpoint-wal))
      (call-with-reseed-original-base-fixture
       (lambda (root directory control)
         (let* ((manifest (pg::recovery-read-manifest directory))
                (relative (if (eq name :checkpoint-wal)
                              (first (find-if (lambda (entry) (and (uiop:string-prefix-p "pg_wal/" (first entry))
                                                                   (uiop:string-suffix-p (first entry) ".wal")))
                                              (getf manifest :files)))
                              name)))
           (ok relative)
           (with-open-file (stream (merge-pathnames relative root) :direction :io :if-exists :overwrite
                                                                   :element-type '(unsigned-byte 8))
             (let ((byte (read-byte stream nil 0)))
               (file-position stream 0)
               (write-byte (logxor byte 255) stream)
               (finish-output stream)))
           (ok (intake-fails-p
                (lambda () (pg::physical-recovery-verify-base-control-with-namespace control))))
           (ok (intake-fails-p (lambda () (pg::physical-recovery-verify-base-with-namespace)))))))))

(deftest reseed-original-base-public-verifier-still-rejects-extra-child-files
    (call-with-reseed-original-base-fixture
     (lambda (root directory control)
       (let ((path (merge-pathnames "pg_wal/unrelated" root)))
         (reseed-follow-owner-test-write path #(1 2 3))
         (let ((core (pg::physical-recovery-verify-base-control-with-namespace control)))
           (ok (equal (getf control :system-id) (getf core :system-id)))
           (ok (intake-fails-p (lambda () (pg::recovery-check-image-directories core directory))))
           (ok (intake-fails-p (lambda () (pg::recovery-check-image-metadata core directory))))
           (ok (intake-fails-p (lambda () (pg::physical-recovery-verify-base-with-namespace))))
           (ok (equalp #(1 2 3) (lineage-test-bytes path))))))))
