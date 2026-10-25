;;;; Local data ownership precedes all standby/primary storage initialization.
(in-package #:pg-physical-tests)

(defun call-with-pid-owner (function)
  (with-physical-store-directory (root)
    (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                              (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                              (pg::*postmaster-start-time* nil) (pg::*postmaster-pid-host* nil)
                              (pg::*postmaster-pid-port* nil) (pg::*postmaster-pid-inode* nil)
                              (saved-exit-hooks sb-ext:*exit-hooks*))
      (sb-posix:chmod (namestring root) #o700)
      (unwind-protect
           (progn (pg::postmaster-own-data-directory "127.0.0.1" 5439)
                  (funcall function root))
        (pg::postmaster-remove-pid-file)
        (setf sb-ext:*exit-hooks* saved-exit-hooks)
        (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))))))

(deftest pid-metadata-eight-lines-and-stable-status-width
    (call-with-pid-owner
     (lambda (root)
       (let* ((path (merge-pathnames "postmaster.pid" root))
              (before (pg::recovery-read-bytes path))
              (fields (uiop:split-string (pg::bytes->string before) :separator '(#\Newline))))
         (dolist (stream (list pg::*postmaster-data-lock* pg::*postmaster-pid-stream*))
           (ok (logbitp 0 (sb-posix:fcntl (sb-sys:fd-stream-fd stream) sb-posix:f-getfd))))
         (ok (= 9 (length fields)))
         (ok (= (sb-posix:getpid) (pg::postmaster-existing-pid)))
         (ok (= (sb-posix:getpid) (parse-integer (first fields))))
         (ok (= 5439 (parse-integer (fourth fields))))
         (ok (equal "" (fifth fields)))
         (ok (equal "" (seventh fields)))
         (ok (equal "starting" (eighth fields)))
         (ok (= #o600 (logand #o777 (sb-posix:stat-mode (sb-posix:stat path)))))
         (pg::postmaster-update-pid-status "ready   ")
         (ok (= (length before) (length (pg::recovery-read-bytes path))))
         (ok (pg::postmaster-pid-owned-p))
         (pg::postmaster-remove-pid-file)
         (ok (not (probe-file path)))
         (ok (probe-file (merge-pathnames ".pg_lisp.lock" root)))))))

(deftest pid-cleanup-never-unlinks-a-replacement-inode
    (call-with-pid-owner
     (lambda (root)
       (let ((path (merge-pathnames "postmaster.pid" root)))
         (pg::wal-atomic-write-bytes path (pg::string->bytes "replacement\n"))
         (ok (not (pg::postmaster-pid-owned-p)))
         (ok (intake-fails-p (lambda () (pg::postmaster-update-pid-status "ready   "))))
         (pg::postmaster-remove-pid-file)
         (ok (equalp (pg::string->bytes "replacement\n") (pg::recovery-read-bytes path)))))))

(deftest pid-stable-lock-rejects-a-second-open-in-the-same-process
    (call-with-pid-owner
     (lambda (root)
       (with-open-file (stream (merge-pathnames ".pg_lisp.lock" root)
                               :direction :io :if-exists :overwrite)
         (ok (not (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd stream) 6))))))))

(deftest pid-directory-permissions-are-not-silently-repaired
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root))
        (sb-posix:chmod (namestring root) #o755)
        (ok (intake-fails-p (lambda () (pg::postmaster-check-data-directory))))
        (ok (= #o755 (logand #o777 (sb-posix:stat-mode (sb-posix:stat root)))))
        (ok (not (probe-file (merge-pathnames ".pg_lisp.lock" root)))))))

(deftest pid-data-lock-never-follows-a-symlink
    (with-physical-store-directory (root)
      (let* ((pg::*postmaster-pid-directory* root)
             (outside (merge-pathnames "other-file" root))
             (lock (merge-pathnames ".pg_lisp.lock" root)))
        (pg::wal-atomic-write-bytes outside #(1 2 3))
        (sb-posix:chmod (namestring outside) #o644)
        (sb-posix:symlink (namestring outside) (namestring lock))
        (ok (intake-fails-p (lambda () (pg::postmaster-open-data-lock))))
        (ok (= #o644 (logand #o777 (sb-posix:stat-mode (sb-posix:stat outside)))))
        (ok (equalp #(1 2 3) (pg::recovery-read-bytes outside))))))
