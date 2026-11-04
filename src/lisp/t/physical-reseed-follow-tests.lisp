(in-package #:pg-physical-tests)

(cl-spec:defproperty reseed-follow-fence-preserves-untrusted-entries
    ((name-choice (range integer 0 1)) (kind-choice (range integer 0 3)) (install-only boolean))
  (:about pg::physical-directory-check-reseed-intent)
  (:trials (:normal 80))
  (call-with-reseed-owner-directory
   (lambda (root)
     (let* ((name (nth name-choice '("physical-reseed.follow" "physical-reseed.follow.tmp")))
            (path (merge-pathnames name root))
            (kind (nth kind-choice '(:file :symlink :directory :fifo))))
       (reseed-install-test-entry path kind)
       (let ((before (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
         (and (intake-fails-p
               (lambda () (pg::physical-directory-check-reseed-intent root :install-only install-only)))
              (equal before (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path))))))))))

(deftest generated-reseed-follow-fence-entry-preservation
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-fence-preserves-untrusted-entries
                                       :seed 114 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-fences-startup-before-reading-or-creating-pid
    (dolist (name '("physical-reseed.follow" "physical-reseed.follow.tmp"))
      (dolist (kind '(:file :symlink :directory :fifo))
        (dolist (old-pid '(nil t))
          (with-physical-store-directory (root)
            (call-with-adoption-postmaster-fixture
             root
             (lambda ()
               (let* ((pid (merge-pathnames "postmaster.pid" root))
                      (entered nil))
                 (when old-pid
                   (with-open-file (out pid :direction :output)
                     (write-string "opaque old PID must remain unread" out)))
                 (reseed-install-test-entry (merge-pathnames name root) kind)
                 (let ((before (when old-pid (lineage-test-bytes pid)))
                       (inode (when old-pid (sb-posix:stat-ino (sb-posix:lstat (namestring pid))))))
                   (ok (intake-fails-p
                        (lambda ()
                          (pg::postmaster-own-data-directory
                           "127.0.0.1" 5439 :startup-before-pid #'pg::physical-reseed-check-before-pid
                           :startup-check (lambda () (setf entered t))))))
                   (ok (not entered))
                   (if old-pid
                       (progn
                         (ok (equalp before (lineage-test-bytes pid)))
                         (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid))))))
                       (ok (not (probe-file pid))))
                   (ok (null pg::*postmaster-data-lock*))
                   (ok (null pg::*postmaster-pid-stream*)))))))))))

(deftest reseed-follow-fences-refuse-ordinary-owner-factories
    (dolist (name '("physical-reseed.follow" "physical-reseed.follow.tmp"))
      (dolist (kind '(:file :symlink :directory :fifo))
        (dolist (factory '(pg::call-with-physical-reseed-owner pg::call-with-physical-adoption-owner))
          (call-with-reseed-owner-directory
           (lambda (root)
             (let ((entered nil))
               (reseed-install-test-entry (merge-pathnames name root) kind)
               (ok (intake-fails-p
                    (lambda () (funcall factory root (lambda (owner) (declare (ignore owner)) (setf entered t))))))
               (ok (not entered))
               (ok (not (probe-file (merge-pathnames "postmaster.pid" root)))))))))))

(deftest reseed-follow-fences-recheck-existing-ordinary-owners
    (dolist (name '("physical-reseed.follow" "physical-reseed.follow.tmp"))
      (dolist (factory '(pg::call-with-physical-reseed-owner pg::call-with-physical-adoption-owner))
        (call-with-reseed-owner-directory
         (lambda (root)
           (funcall factory root
                    (lambda (owner)
                      (ok (eq owner (pg::physical-directory-check-owner owner)))
                      (reseed-install-test-entry (merge-pathnames name root) :file)
                      (ok (intake-fails-p (lambda () (pg::physical-directory-check-owner owner)))))))))))

(deftest reseed-follow-fences-revoke-cleanup-ownership
    (dolist (name '("physical-reseed.follow" "physical-reseed.follow.tmp"))
      (call-with-reseed-cleanup-fixture
       (lambda (root owner record)
         (declare (ignore record))
         (reseed-install-test-entry (merge-pathnames name root) :file)
         (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner))))
         (ok (intake-fails-p (lambda () (pg::physical-reseed-cleanup-initial-arguments root))))))))

(deftest reseed-follow-fences-revoke-install-ownership
    (dolist (name '("physical-reseed.follow" "physical-reseed.follow.tmp"))
      (call-with-reseed-install-owner-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine plan))
         (pg::call-with-physical-reseed-install-owner
          root
          (lambda (owner)
            (ok (eq owner (pg::physical-reseed-check-owner owner)))
            (reseed-install-test-entry (merge-pathnames name root) :file)
            (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner))))))
         (let ((entered nil))
           (ok (intake-fails-p
                (lambda ()
                  (pg::call-with-physical-reseed-install-owner
                   root (lambda (owner) (declare (ignore owner)) (setf entered t))))))
           (ok (not entered)))))))

(deftest reseed-follow-fences-block-late-startup-retirement
    (dolist (name '("physical-reseed.follow" "physical-reseed.follow.tmp"))
      (dolist (point '(:preflight :reconcile))
        (call-with-adoption-startup-entry-fixture
         (lambda (root state directory)
           (declare (ignore state directory))
           (let ((before (reseed-install-test-adoption-fences root)))
             (ok (intake-fails-p
                  (lambda ()
                    (pg::postmaster-own-data-directory
                     "127.0.0.1" 5439
                     :startup-before-pid
                     (lambda ()
                       (pg::physical-reseed-check-before-pid)
                       (when (eq point :preflight)
                         (reseed-install-test-entry (merge-pathnames name root) :file)))
                     :startup-check
                     (lambda ()
                       (pg::physical-adoption-startup-preflight)
                       (when (eq point :reconcile)
                         (reseed-install-test-entry (merge-pathnames name root) :file)))
                     :startup-reconcile #'pg::physical-adoption-reconcile-startup))))
             (reseed-install-test-preserved-adoption-fences before)
             (ok (null pg::*postmaster-data-lock*))
             (ok (null pg::*postmaster-pid-stream*))))))))

(deftest reseed-follow-fences-recheck-the-special-startup-owner
    (dolist (name '("physical-reseed.follow" "physical-reseed.follow.tmp"))
      (call-with-adoption-startup-entry-fixture
       (lambda (root state directory)
         (declare (ignore state directory))
         (let ((before (reseed-install-test-adoption-fences root)))
           (pg::postmaster-own-data-directory "127.0.0.1" 5439)
           (pg::call-with-physical-adoption-startup-owner
            (lambda (owner)
              (reseed-install-test-entry (merge-pathnames name root) :file)
              (ok (intake-fails-p (lambda () (pg::physical-adoption-check-owner owner))))
              (reseed-install-test-preserved-adoption-fences before)
              (ok (open-stream-p pg::*postmaster-data-lock*))
              (ok (open-stream-p pg::*postmaster-pid-stream*)))))))))