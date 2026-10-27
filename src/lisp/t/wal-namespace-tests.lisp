;;;; Explicit timeline/segment identity; legacy names remain timeline one.
(in-package #:pg-physical-tests)

(deftest raw-directory-inventory-preserves-hidden-and-dangling-entries
    (with-physical-store-directory (root)
      (let ((directory (merge-pathnames "inventory/" root))
            (link (merge-pathnames "inventory-link" root))
            (expected '(".pending.tmp" "broken.wal" "valid.slot")))
        (pg::wal-ensure-directory-durable directory)
        (dolist (name '(".pending.tmp" "valid.slot"))
          (with-open-file (stream (merge-pathnames name directory)
                                  :direction :output :element-type '(unsigned-byte 8))
            (finish-output stream)))
        (sb-posix:symlink "missing-target" (namestring (merge-pathnames "broken.wal" directory)))
        (ok (equal expected (sort (pg::directory-entry-names directory) #'string<)))
        (sb-posix:symlink (namestring directory) (namestring link))
        (ok (intake-fails-p (lambda () (pg::directory-entry-names link))))
        (ok (equal expected (sort (pg::directory-entry-names link :follow-symlinks t) #'string<)))
        (ok (intake-fails-p
             (lambda () (pg::directory-entry-names (merge-pathnames "valid.slot" directory)))))
        (ok (null (pg::directory-entry-names (merge-pathnames "absent/" root)))))))

(deftest timeline-qualified-segment-names-use-c-log-and-segment-fields
    (ok (equal "000000030000000100000002.wal" (pg::wal-segment-filename 258 3 :timeline)))
  (ok (equal "0000000000000102.wal" (pg::wal-segment-filename 258 1 :legacy)))
  (multiple-value-bind (timeline segment)
      (pg::wal-parse-segment-name "000000030000000100000002" :timeline)
    (ok (= timeline 3)) (ok (= segment 258)))
  (multiple-value-bind (timeline segment)
      (pg::wal-parse-segment-name "0000000000000102" :legacy)
    (ok (= timeline 1)) (ok (= segment 258)))
  (ok (intake-fails-p (lambda () (pg::wal-segment-filename 258 3 :legacy))))
  (dolist (name '("000000000000000000000000" "000000010000000000000100"
                  "00000001000000000000000a" "../000000010000000000000001"))
    (ok (null (pg::wal-parse-segment-name name :timeline)))))

(cl-spec:defproperty wal-segment-identity-roundtrips-without-branch-aliasing
    ((timeline (range integer 1 4294967295)) (segment (range integer 0 1099511627775)))
  (:about pg::wal-segment-filename)
  (:trials (:normal 200))
  (let ((name (subseq (pg::wal-segment-filename segment timeline :timeline) 0 24)))
    (multiple-value-bind (decoded-timeline decoded-segment)
        (pg::wal-parse-segment-name name :timeline)
      (and (= timeline decoded-timeline) (= segment decoded-segment)
           (null (pg::wal-parse-segment-name name :legacy))))))

(deftest generated-timeline-qualified-segment-identities
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'wal-segment-identity-roundtrips-without-branch-aliasing :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest wal-discovery-selects-explicit-namespace-and-timeline
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root))
        (dolist (name '("0000000000000001.wal" "000000010000000000000002.wal"
                        "000000020000000000000002.wal" "000000020000000000000003.wal"))
          (pg::wal-atomic-write-bytes (merge-pathnames (concatenate 'string "pg_wal/" name) root)
                                      (make-array 0 :element-type '(unsigned-byte 8))))
        (ok (equal '(1) (pg::wal-discover-segments 1 :legacy)))
        (ok (equal '(2) (pg::wal-discover-segments 1 :timeline)))
        (ok (equal '(2 3) (pg::wal-discover-segments 2 :timeline)))
        (ok (null (pg::wal-discover-segments 3 :timeline))))))

(deftest wal-control-binds-namespace-independently-of-timeline-identities
    (dolist (namespace '(:legacy :timeline))
      (let ((line (string-right-trim '(#\Newline)
                                     (pg::bytes->string
                                      (pg::wal-encode-control-image 0 0 nil :namespace namespace)))))
        (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline decoded)
            (pg::wal-decode-control-line line)
          (ok (= checkpoint durable 0)) (ok (null snapshot))
          (ok (= insertion checkpoint-timeline 1)) (ok (eq namespace decoded)))))
  (let* ((prefix "PGWAL5 0 0 1 1 -")
         (line (format nil "~a ~8,'0X" prefix (pg::wal-crc32 (pg::string->bytes prefix)))))
    (ok (eq :legacy (nth-value 5 (pg::wal-decode-control-line line))))))

(defun call-with-wal-namespace-fixture (function)
  (let ((pg::*wal-segment-namespace* :legacy) (pg::*wal-insert-timeline* 1)
                                              (pg::*wal-checkpoint-timeline* 1) (pg::*wal-namespace-test-hook* nil)
                                              (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
                                              (pg::*live-operation-drain-open* nil))
    (call-with-wal-allocation-fixture
     (lambda ()
       (pg::assign-xid)
       (funcall function)))))

(defun migrate-test-wal-namespace ()
  (pg::with-write-lock
    (pg::with-wal-critical (pg::wal-migrate-namespace-under-lock))))

(deftest wal-namespace-migration-preserves-bytes-and-restarts-in-committed-grammar
    (call-with-wal-namespace-fixture
     (lambda ()
       (let* ((segment (first (pg::wal-discover-segments)))
              (old (pg::wal-segment-path segment 1 :legacy))
              (new (pg::wal-segment-path segment 1 :timeline))
              (bytes (pg::recovery-read-bytes old))
              (durable pg::*wal-durable-lsn*))
         (ok (eq :timeline (migrate-test-wal-namespace)))
         (ok (not (probe-file old)))
         (ok (equalp bytes (pg::recovery-read-bytes new)))
         (ok (= durable pg::*wal-durable-lsn*))
         (ok (eq :timeline (nth-value 5 (pg::wal-read-control-fields))))
         (when pg::*wal-output-stream* (close pg::*wal-output-stream*) (setf pg::*wal-output-stream* nil))
         (pg::wal-init)
         (ok (eq :timeline pg::*wal-segment-namespace*))
         (ok (= durable pg::*wal-durable-lsn* pg::*wal-end-lsn*))))))

(deftest wal-namespace-retry-cleans-only-reverified-legacy-aliases
    (call-with-wal-namespace-fixture
     (lambda ()
       (let* ((segment (first (pg::wal-discover-segments)))
              (old (pg::wal-segment-path segment 1 :legacy))
              (new (pg::wal-segment-path segment 1 :timeline)))
         (migrate-test-wal-namespace)
         (sb-posix:link (namestring new) (namestring old))
         (let ((before (pg::recovery-read-bytes (pg::wal-control-path))))
           (ok (eq :timeline (migrate-test-wal-namespace)))
           (ok (null (pg::wal-discover-segments 1 :legacy)))
           (ok (probe-file new))
           (ok (equalp before (pg::recovery-read-bytes (pg::wal-control-path)))))
         (pg::wal-atomic-write-bytes old #(1 2 3))
         (ok (intake-fails-p #'migrate-test-wal-namespace))
         (ok (not pg::*wal-poisoned*))
         (ok (eq :timeline pg::*wal-segment-namespace*))
         (ok (equalp #(1 2 3) (pg::recovery-read-bytes old)))))))

(deftest wal-namespace-cleanup-helper-refuses-the-active-name
    (call-with-wal-namespace-fixture
     (lambda ()
       (let* ((segment (first (pg::wal-discover-segments)))
              (old (pg::wal-segment-path segment 1 :legacy))
              (new (pg::wal-segment-path segment 1 :timeline)))
         (sb-posix:link (namestring old) (namestring new))
         (let ((entry (first (pg::wal-namespace-migration-plan))))
           (ok (intake-fails-p (lambda () (pg::wal-namespace-remove-inactive-alias entry old))))
           (ok (probe-file old))
           (ok (probe-file new)))))))

(deftest wal-startup-reconciles-migration-before-writes-or-recycling
    (dolist (point '(:after-link :after-control-before-memory))
      (call-with-wal-namespace-fixture
       (lambda ()
         (setf pg::*wal-namespace-test-hook*
               (lambda (current) (when (eq current point) (throw 'namespace-crash :crashed))))
         (ok (eq :crashed (catch 'namespace-crash (migrate-test-wal-namespace))))
         (ok pg::*wal-poisoned*)
         (when pg::*wal-output-stream* (close pg::*wal-output-stream*) (setf pg::*wal-output-stream* nil))
         (pg::wal-init)
         (let ((active (if (eq point :after-link) :legacy :timeline))
               (inactive (if (eq point :after-link) :timeline :legacy)))
           (ok (eq active pg::*wal-segment-namespace*))
           (ok (null (pg::wal-discover-segments 1 inactive)))
           (ok (not pg::*wal-poisoned*))
           (ok pg::*wal-ready*))))))

(deftest wal-startup-refuses-unmatched-opposite-alias-before-recycling
    (call-with-wal-namespace-fixture
     (lambda ()
       (let ((extra (pg::wal-segment-path 99 1 :timeline))
             (control (pg::recovery-read-bytes (pg::wal-control-path))))
         (pg::wal-atomic-write-bytes extra #(1 2 3))
         (when pg::*wal-output-stream* (close pg::*wal-output-stream*) (setf pg::*wal-output-stream* nil))
         (let ((pg::*wal-ready* nil))
           (ok (intake-fails-p #'pg::wal-init))
           (ok (not pg::*wal-ready*)))
         (ok (equalp control (pg::recovery-read-bytes (pg::wal-control-path))))
         (ok (equalp #(1 2 3) (pg::recovery-read-bytes extra)))))))

(deftest wal-namespace-migration-rejects-unmatched-aliases-without-poisoning
    (call-with-wal-namespace-fixture
     (lambda ()
       (let ((extra (pg::wal-segment-path 99 1 :timeline)))
         (pg::wal-atomic-write-bytes extra #(1 2 3))
         (ok (intake-fails-p #'migrate-test-wal-namespace))
         (ok (not pg::*wal-poisoned*))
         (ok (eq :legacy pg::*wal-segment-namespace*))
         (ok (equalp #(1 2 3) (pg::recovery-read-bytes extra)))))))

(deftest wal-namespace-migration-rejects-unmatched-dangling-symlink-aliases
    (call-with-wal-namespace-fixture
     (lambda ()
       (let ((extra (pg::wal-segment-path 99 1 :timeline)))
         (sb-posix:symlink "missing-namespace-target" (namestring extra))
         (ok (intake-fails-p #'migrate-test-wal-namespace))
         (ok (not pg::*wal-poisoned*))
         (ok (eq :legacy pg::*wal-segment-namespace*))))))

(deftest missing-control-never-ignores-a-dangling-wal-entry-in-either-namespace
    (dolist (namespace '(:legacy :timeline))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root) (pg::*wal-ready* nil) (pg::*wal-segment-namespace* :legacy)
                                   (path (pg::wal-segment-path 99 1 namespace)))
          (ensure-directories-exist path)
          (sb-posix:symlink "missing-wal-target" (namestring path))
          (ok (intake-fails-p #'pg::wal-init))
          (ok (not pg::*wal-ready*))
          (ok (not (probe-file (pg::wal-control-path))))))))

(deftest wal-namespace-migration-requires-the-same-inode-not-equal-content
    (dolist (kind '(:copy :symlink :same-inode))
      (call-with-wal-namespace-fixture
       (lambda ()
         (let* ((segment (first (pg::wal-discover-segments)))
                (old (pg::wal-segment-path segment 1 :legacy))
                (new (pg::wal-segment-path segment 1 :timeline)))
           (ecase kind
             (:copy (pg::wal-atomic-write-bytes new (pg::recovery-read-bytes old)))
             (:symlink (sb-posix:symlink (namestring old) (namestring new)))
             (:same-inode (sb-posix:link (namestring old) (namestring new))))
           (if (eq kind :same-inode)
               (ok (eq :timeline (migrate-test-wal-namespace)))
               (progn
                 (ok (intake-fails-p #'migrate-test-wal-namespace))
                 (ok (not pg::*wal-poisoned*))
                 (ok (eq :legacy pg::*wal-segment-namespace*)))))))))

(deftest wal-namespace-uncertain-control-publication-fences-old-memory-state
    (call-with-wal-namespace-fixture
     (lambda ()
       (let ((original (symbol-function 'pg::fsync-directory)))
         (unwind-protect
              (progn
                (setf pg::*wal-namespace-test-hook*
                      (lambda (point)
                        (when (eq point :after-links-sync)
                          (setf (symbol-function 'pg::fsync-directory)
                                (lambda (directory)
                                  (if (equal (namestring directory)
                                             (namestring (merge-pathnames "global/" pg::*pgdata*)))
                                      (error "namespace control directory fsync failed after rename")
                                      (funcall original directory)))))))
                (ok (intake-fails-p #'migrate-test-wal-namespace))
                (ok pg::*wal-poisoned*)
                (ok (eq :legacy pg::*wal-segment-namespace*))
                (ok (eq :timeline (nth-value 5 (pg::wal-read-control-fields))))
                (ok (intake-fails-p #'pg::assign-xid)))
           (setf (symbol-function 'pg::fsync-directory) original))))))

(deftest wal-namespace-nonlocal-exit-fences-before-releasing-wal
    (call-with-wal-namespace-fixture
     (lambda ()
       (setf pg::*wal-namespace-test-hook*
             (lambda (point) (when (eq point :after-control-before-memory) (throw 'namespace-abort :aborted))))
       (ok (eq :aborted (catch 'namespace-abort (migrate-test-wal-namespace))))
       (ok pg::*wal-poisoned*)
       (ok (eq :legacy pg::*wal-segment-namespace*))
       (ok (eq :timeline (nth-value 5 (pg::wal-read-control-fields)))))))
