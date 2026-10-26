;;;; Timeline prefix staging never publishes a child or changes authority.
(in-package #:pg-physical-tests)

(defun timeline-test-staging-files ()
  (directory (merge-pathnames ".timeline-*" (pg::wal-directory))))

(deftest timeline-prefix-staging-copies-only-parent-prefix-in-both-namespaces
    (dolist (namespace '(:legacy :timeline))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root) (pg::*wal-segment-namespace* namespace)
                                   (first (lineage-test-record 0)) (tail (lineage-test-record 33 33 9))
                                   (parent (pg::wal-segment-path 0 1 namespace))
                                   (before (list pg::*wal-insert-timeline* pg::*wal-checkpoint-timeline*
                                                 pg::*wal-end-lsn* pg::*wal-durable-lsn* pg::*physical-recovery-state*)))
          (pg::wal-atomic-write-bytes parent (concatenate '(vector (unsigned-byte 8)) first tail))
          (let ((temporary (pg::wal-stage-timeline-prefix 1 33)))
            (unwind-protect
                 (progn
                   (ok (equalp first (lineage-test-bytes temporary)))
                   (ok (equalp (concatenate '(vector (unsigned-byte 8)) first tail)
                               (lineage-test-bytes parent)))
                   (ok (equal before (list pg::*wal-insert-timeline* pg::*wal-checkpoint-timeline*
                                           pg::*wal-end-lsn* pg::*wal-durable-lsn* pg::*physical-recovery-state*)))
                   (ok (not (probe-file (pg::wal-segment-path 0 2 :timeline))))
                   (ok (equal '(0) (pg::wal-discover-segments 1 namespace))))
              (delete-file temporary)))))))

(deftest timeline-prefix-staging-boundary-needs-no-parent-file
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root))
        (dolist (fork (list 0 pg::+wal-segment-size+ (* 2 pg::+wal-segment-size+)))
          (let ((temporary (pg::wal-stage-timeline-prefix 4 fork :namespace :timeline)))
            (unwind-protect
                 (ok (zerop (length (lineage-test-bytes temporary))))
              (delete-file temporary))))
        (ok (null (timeline-test-staging-files))))))

(deftest timeline-prefix-staging-accepts-exact-authenticated-bytes-including-boundaries
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root))
        (dolist (fork (list 0 33 pg::+wal-segment-size+ (+ pg::+wal-segment-size+ 65)))
          (let* ((bytes (make-array (mod fork pg::+wal-segment-size+)
                                    :element-type '(unsigned-byte 8) :initial-element 42))
                 (temporary (pg::wal-stage-timeline-prefix 1 fork :prefix bytes)))
            (unwind-protect (ok (equalp bytes (lineage-test-bytes temporary)))
              (delete-file temporary))))
        (dolist (bad (list nil #(1) (vector "x")))
          (ok (intake-fails-p (lambda () (pg::wal-stage-timeline-prefix 1 33 :prefix bad)))))
        (ok (null (timeline-test-staging-files))))))

(deftest timeline-prefix-staging-copies-multiple-chunks-from-a-later-segment
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (start (* 2 pg::+wal-segment-size+))
                                 (length (+ 65536 17)) (prefix (lineage-test-record start length))
                                 (parent (lineage-test-file 4 2 prefix (lineage-test-record (+ start length) 33 9)))
                                 (temporary (pg::wal-stage-timeline-prefix 4 (+ start length) :namespace :timeline)))
        (unwind-protect
             (progn
               (ok (equalp prefix (lineage-test-bytes temporary)))
               (ok (= (+ length 33) (length (lineage-test-bytes parent)))))
          (delete-file temporary)))))

(deftest timeline-prefix-staging-rejects-short-or-special-parent-before-temporary-creation
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (parent (lineage-test-file 1 0 (lineage-test-record 0))))
        (ok (intake-fails-p (lambda () (pg::wal-stage-timeline-prefix 1 34 :namespace :timeline))))
        (ok (null (timeline-test-staging-files)))
        (delete-file parent)
        (sb-posix:mkfifo (namestring parent) #o600)
        (ok (intake-fails-p (lambda () (pg::wal-stage-timeline-prefix 1 33 :namespace :timeline))))
        (sb-posix:unlink (namestring parent))
        (sb-posix:symlink "/missing-owned-timeline-parent" (namestring parent))
        (ok (intake-fails-p (lambda () (pg::wal-stage-timeline-prefix 1 33 :namespace :timeline))))
        (sb-posix:unlink (namestring parent))
        (ok (null (timeline-test-staging-files))))))

(deftest timeline-prefix-staging-cleans-aborted-preparation-and-preserves-source
    (dolist (point '(:before-prefix-fsync :after-prefix-fsync :after-prefix-directory-fsync))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root) (parent (lineage-test-file 1 0 (lineage-test-record 0)))
                                   (bytes (lineage-test-bytes parent))
                                   (pg::*wal-timeline-test-hook*
                                     (lambda (observed) (when (eq observed point) (throw 'abort-prefix :aborted)))))
          (ok (eq :aborted (catch 'abort-prefix
                             (pg::wal-stage-timeline-prefix 1 33 :namespace :timeline))))
          (ok (equalp bytes (lineage-test-bytes parent)))
          (ok (null (timeline-test-staging-files)))))))

(cl-spec:defproperty timeline-prefix-staging-preserves-generated-prefixes
    ((length (range integer 32 1024)) (timeline (range integer 1 4294967295)))
  (:about pg::wal-stage-timeline-prefix)
  (:trials (:normal 100))
  (with-physical-store-directory (root)
    (let* ((pg::*pgdata* root)
           (prefix (lineage-test-record 0 length))
           (parent (lineage-test-file timeline 0 prefix (lineage-test-record length 33 9)))
           (temporary (pg::wal-stage-timeline-prefix timeline length :namespace :timeline)))
      (unwind-protect
           (and (equalp prefix (lineage-test-bytes temporary))
                (= (+ length 33) (length (lineage-test-bytes parent))))
        (delete-file temporary)))))

(deftest generated-timeline-prefix-staging
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'timeline-prefix-staging-preserves-generated-prefixes :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
