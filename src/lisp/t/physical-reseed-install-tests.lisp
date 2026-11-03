(in-package #:pg-physical-tests)

(defun reseed-install-test-entry (path kind)
  (ecase kind
    (:file (with-open-file (output path :direction :output :if-exists :error)
             (write-string "untrusted or incomplete install intent" output)))
    (:symlink (sb-posix:symlink "missing" (namestring path)))
    (:directory (sb-posix:mkdir (namestring path) #o700))
    (:fifo (sb-posix:mkfifo (namestring path) #o600))))

(deftest reseed-install-fences-real-startup-before-pid-inspection-or-creation
    (dolist (name '("physical-reseed.install" "physical-reseed.install.tmp"))
      (dolist (kind '(:file :symlink :directory :fifo))
        (dolist (old-pid '(nil t))
          (with-physical-store-directory (root)
            (call-with-adoption-postmaster-fixture
             root
             (lambda ()
               (let* ((path (merge-pathnames name root))
                      (pid (merge-pathnames "postmaster.pid" root))
                      (accepted nil))
                 (when old-pid
                   (with-open-file (output pid :direction :output :if-exists :error)
                     (write-string "opaque PID bytes must not be parsed or repaired" output)))
                 (reseed-install-test-entry path kind)
                 (let* ((before (when old-pid (lineage-test-bytes pid)))
                        (inode (when old-pid (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))
                        (message
                          (handler-case
                              (progn
                                (pg::postmaster-own-data-directory
                                 "127.0.0.1" 5439
                                 :startup-before-pid #'pg::physical-reseed-check-before-pid
                                 :startup-check (lambda () (setf accepted t)))
                                "startup unexpectedly accepted")
                            (error (condition) (princ-to-string condition)))))
                   (ok (search "pending offline reseed installation" message)
                       (format nil "~a ~s old-pid=~s" name kind old-pid))
                   (ok (not accepted))
                   (if old-pid
                       (progn
                         (ok (equalp before (lineage-test-bytes pid)))
                         (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid))))))
                       (ok (not (probe-file pid))))
                   (ok (null pg::*postmaster-data-lock*))
                   (ok (null pg::*postmaster-pid-stream*)))))))))))

(deftest reseed-install-intent-refuses-ordinary-owner-before-reading-opaque-pid
    (dolist (name '("physical-reseed.install" "physical-reseed.install.tmp"))
      (dolist (kind '(:file :symlink :directory :fifo))
        (call-with-reseed-owner-directory
         (lambda (root)
           (let ((pid (merge-pathnames "postmaster.pid" root))
                 (entered nil))
             (with-open-file (output pid :direction :output)
               (write-string "opaque old PID metadata" output))
             (reseed-install-test-entry (merge-pathnames name root) kind)
             (let ((before (lineage-test-bytes pid))
                   (message
                     (handler-case
                         (progn
                           (pg::call-with-physical-reseed-owner
                            root (lambda (owner) (declare (ignore owner)) (setf entered t)))
                           "ordinary owner unexpectedly accepted")
                       (error (condition) (princ-to-string condition)))))
               (ok (search "pending offline reseed installation" message))
               (ok (not entered))
               (ok (equalp before (lineage-test-bytes pid))))))))))

(deftest reseed-install-intent-stops-an-existing-ordinary-owner-without-moving-pid
    (dolist (name '("physical-reseed.install" "physical-reseed.install.tmp"))
      (dolist (kind '(:file :symlink :directory :fifo))
        (call-with-reaped-reseed-pid
         (lambda (dead)
           (call-with-reseed-owner-directory
            (lambda (root)
              (let* ((pid (reseed-test-write-pid root (- dead)))
                     (before (lineage-test-bytes pid))
                     (inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))
                (pg::call-with-physical-reseed-owner
                 root
                 (lambda (owner)
                   (ok (eq owner (pg::physical-reseed-check-owner owner)))
                   (reseed-install-test-entry (merge-pathnames name root) kind)
                   (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner))))
                   (ok (equalp before (lineage-test-bytes pid)))
                   (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring pid)))))))))))))))

(deftest reseed-intent-refuses-new-clean-adoption-ownership-without-a-pid
    (dolist (name '("physical-reseed.attempt" "physical-reseed.attempt.tmp"
                    "physical-reseed.install" "physical-reseed.install.tmp"))
      (dolist (kind '(:file :symlink :directory :fifo))
        (call-with-reseed-owner-directory
         (lambda (root)
           (let ((entered nil))
             (reseed-install-test-entry (merge-pathnames name root) kind)
             (let ((message
                     (handler-case
                         (progn
                           (pg::call-with-physical-adoption-owner
                            root (lambda (owner) (declare (ignore owner)) (setf entered t)))
                           "clean adoption unexpectedly accepted")
                       (error (condition) (princ-to-string condition)))))
               (ok (search "pending offline reseed" message))
               (ok (not entered))
               (ok (not (probe-file (merge-pathnames "postmaster.pid" root)))))))))))

(deftest reseed-intent-stops-existing-clean-adoption-authority
    (dolist (name '("physical-reseed.attempt" "physical-reseed.attempt.tmp"
                    "physical-reseed.install" "physical-reseed.install.tmp"))
      (dolist (kind '(:file :symlink :directory :fifo))
        (call-with-reseed-owner-directory
         (lambda (root)
           (pg::call-with-physical-adoption-owner
            root
            (lambda (owner)
              (ok (eq owner (pg::physical-adoption-check-owner owner)))
              (reseed-install-test-entry (merge-pathnames name root) kind)
              (ok (intake-fails-p (lambda () (pg::physical-adoption-check-owner owner))))
              (let ((message
                      (handler-case
                          (progn (pg::physical-adoption-inspect-primary owner) "inspection unexpectedly accepted")
                        (error (condition) (princ-to-string condition)))))
                (ok (search "pending offline reseed" message)))
              (ok (not (probe-file (merge-pathnames "postmaster.pid" root)))))))))))

(deftest reseed-intent-fences-adoption-startup-entrypoints-without-follow-state
    (dolist (name '("physical-reseed.attempt" "physical-reseed.attempt.tmp"
                    "physical-reseed.install" "physical-reseed.install.tmp"))
      (with-physical-store-directory (root)
        (let ((pg::*pgdata* root))
          (reseed-install-test-entry (merge-pathnames name root) :file)
          (dolist (entry '(pg::physical-adoption-startup-preflight pg::physical-adoption-reconcile-startup))
            (let ((message (handler-case (progn (funcall entry) "startup unexpectedly accepted")
                             (error (condition) (princ-to-string condition)))))
              (ok (search "pending offline reseed" message))
              (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))))))))

(defun reseed-install-test-adoption-fences (root)
  (loop for path in (list (pg::physical-adoption-attempt-path root)
                          (pg::physical-adoption-install-path root)
                          (pg::physical-adoption-handoff-path root)
                          (pg::physical-adoption-follow-path root))
        collect (list path (sb-posix:stat-ino (sb-posix:lstat (namestring path)))
                      (lineage-test-bytes path))))

(defun reseed-install-test-preserved-adoption-fences (before)
  (dolist (entry before)
    (destructuring-bind (path inode bytes) entry
      (ok (probe-file path))
      (when (probe-file path)
        (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
        (ok (equalp bytes (lineage-test-bytes path)))))))

(deftest reseed-intent-arriving-during-startup-does-not-retire-adoption-fences
    (dolist (point '(:preflight :reconcile))
      (dolist (name '("physical-reseed.attempt" "physical-reseed.attempt.tmp"
                      "physical-reseed.install" "physical-reseed.install.tmp"))
        (call-with-adoption-startup-entry-fixture
         (lambda (root state directory)
           (declare (ignore state directory))
           (let* ((before (reseed-install-test-adoption-fences root))
                  (fence (merge-pathnames name root))
                  (message
                    (handler-case
                        (progn
                          (pg::postmaster-own-data-directory
                           "127.0.0.1" 5439
                           :startup-before-pid
                           (lambda ()
                             (pg::physical-reseed-check-before-pid)
                             (when (eq point :preflight) (reseed-install-test-entry fence :file)))
                           :startup-check
                           (lambda ()
                             (pg::physical-adoption-startup-preflight)
                             (when (eq point :reconcile) (reseed-install-test-entry fence :file)))
                           :startup-reconcile #'pg::physical-adoption-reconcile-startup)
                          "startup unexpectedly accepted")
                      (error (condition) (princ-to-string condition)))))
             (ok (search "pending offline reseed" message))
             (ok (probe-file fence))
             (reseed-install-test-preserved-adoption-fences before)
             (ok (null pg::*postmaster-data-lock*))
             (ok (null pg::*postmaster-pid-stream*))))))))

(deftest reseed-intent-stops-an-existing-startup-adoption-owner
    (dolist (name '("physical-reseed.attempt" "physical-reseed.attempt.tmp"
                    "physical-reseed.install" "physical-reseed.install.tmp"))
      (call-with-adoption-startup-entry-fixture
       (lambda (root state directory)
         (declare (ignore state directory))
         (let ((before (reseed-install-test-adoption-fences root)))
           (pg::postmaster-own-data-directory "127.0.0.1" 5439)
           (pg::call-with-physical-adoption-startup-owner
            (lambda (owner)
              (ok (eq owner (pg::physical-adoption-check-owner owner)))
              (reseed-install-test-entry (merge-pathnames name root) :file)
              (ok (intake-fails-p (lambda () (pg::physical-adoption-check-owner owner))))
              (let ((message
                      (handler-case (progn (pg::physical-adoption-retire-resume owner) "retirement accepted")
                        (error (condition) (princ-to-string condition)))))
                (ok (search "pending offline reseed" message)))
              (reseed-install-test-preserved-adoption-fences before)
              (ok (open-stream-p pg::*postmaster-data-lock*))
              (ok (open-stream-p pg::*postmaster-pid-stream*)))))))))
