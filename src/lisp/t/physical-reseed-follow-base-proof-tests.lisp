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

(deftest reseed-original-base-proof-kernels-use-the-saved-original-control
    (call-with-reseed-follow-building-fixture
     :later-writer
     (lambda (owner root workspace building)
       (declare (ignore root))
       (let ((core (pg::physical-reseed-follow-core-projection building)))
         (pg::call-with-physical-reseed-follow-image
          owner
          (lambda (root-fd check)
            (pg::call-with-physical-backup-image-directory
             root-fd
             (lambda (directory)
               (let ((pg::*pgdata* directory))
                 (multiple-value-bind (checked base control)
                     (pg::physical-reseed-follow-check-local-intent owner root-fd core)
                   (declare (ignore checked control))
                   (multiple-value-bind (binding model readiness)
                       (pg::physical-reseed-follow-proof-at owner root-fd core)
                     (let ((expected (pg::physical-reseed-follow-configuration-images core binding model 12345))
                           (original (pg::physical-recovery-control-path directory))
                           (saved (merge-pathnames "saved.original" workspace)))
                       (sb-posix:rename (namestring original) (namestring saved))
                       (unwind-protect
                            (let ((actual-control
                                    (pg::physical-recovery-decode-control
                                     (pg::recovery-read-bytes saved :maximum 196))))
                              (ok (not (probe-file original)))
                              (ok (pg::physical-reseed-follow-check-base-intent core base directory))
                              (multiple-value-bind (actual-binding actual-model actual-readiness)
                                  (pg::physical-reseed-follow-proof-for-base core base actual-control)
                                (let ((same (equalp expected
                                                    (pg::physical-reseed-follow-configuration-images
                                                     core actual-binding actual-model 12345))))
                                  (ok same))
                                (ok (equalp readiness actual-readiness))))
                         (sb-posix:rename (namestring saved) (namestring original)))))))))
            (funcall check)))))))
