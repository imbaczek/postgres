(in-package #:pg-physical-tests)

(defun call-with-adoption-postmaster-fixture (root function)
  (let ((pg::*pgdata* root) (pg::*postmaster-data-lock* nil)
                            (pg::*postmaster-pid-stream* nil) (pg::*postmaster-pid-directory* nil)
                            (pg::*postmaster-start-time* nil) (pg::*postmaster-start-timestamp* nil)
                            (pg::*postmaster-pid-host* nil) (pg::*postmaster-pid-port* nil)
                            (pg::*postmaster-pid-inode* nil)
                            (saved-exit-hooks sb-ext:*exit-hooks*)
                            (pg::*wal-ready* nil) (pg::*physical-receive-store* nil)
                            (pg::*physical-follow-runtime* nil) (pg::*physical-restartpoint-worker* nil)
                            (pg::*live-operation-depth* 0))
    (sb-posix:chmod (namestring root) #o700)
    (unwind-protect
         (funcall function)
      (pg::postmaster-remove-pid-file)
      (when pg::*postmaster-data-lock* (close pg::*postmaster-data-lock*))
      (setf sb-ext:*exit-hooks* saved-exit-hooks))))

(deftest adoption-startup-reconciliation-runs-under-real-published-ownership
    (with-physical-store-directory (root)
      (call-with-adoption-postmaster-fixture
       root
       (lambda ()
         (let ((events nil))
           (pg::postmaster-own-data-directory
            "127.0.0.1" 5439
            :startup-check
            (lambda ()
              (push :check events)
              (ok (pg::postmaster-pid-owned-p))
              (ok (null pg::*postmaster-data-lock*)))
            :startup-reconcile
            (lambda ()
              (push :reconcile events)
              (ok (pg::postmaster-pid-owned-p))
              (ok (open-stream-p pg::*postmaster-data-lock*))
              (ok (= (sb-posix:getpid) (pg::postmaster-existing-pid)))))
           (ok (equal '(:reconcile :check) events))
           (ok (pg::postmaster-pid-owned-p)))))))

(deftest adoption-startup-reconciliation-refusal-releases-real-ownership
    (with-physical-store-directory (root)
      (call-with-adoption-postmaster-fixture
       root
       (lambda ()
         (dolist (mode '(:error :nonlocal-exit))
           (let ((entered nil) (owned-lock nil))
             (ok (eq :refused
                     (catch 'adoption-startup-refused
                       (handler-case
                           (progn
                             (pg::postmaster-own-data-directory
                              "127.0.0.1" 5439
                              :startup-reconcile
                              (lambda ()
                                (setf entered t owned-lock pg::*postmaster-data-lock*)
                                (ok (pg::postmaster-pid-owned-p))
                                (if (eq mode :error)
                                    (error "reconciliation refusal")
                                    (throw 'adoption-startup-refused :refused))))
                             :accepted)
                         (error () :refused)))))
             (ok entered)
             (ok (not (open-stream-p owned-lock)))
             (ok (null pg::*postmaster-data-lock*))
             (ok (null pg::*postmaster-pid-stream*))
             (ok (null pg::*postmaster-start-timestamp*))
             (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
             (multiple-value-bind (lock size)
                 (pg::open-bounded-regular-file (merge-pathnames ".pg_lisp.lock" root) 0)
               (declare (ignore size))
               (unwind-protect
                    (ok (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd lock) 6)))
                 (close lock)))))))))

(deftest adoption-startup-refusal-closes-pid-even-when-cleanup-sync-fails
    (with-physical-store-directory (root)
      (call-with-adoption-postmaster-fixture
       root
       (lambda ()
         (let ((original (symbol-function 'pg::fsync-directory))
               (cleanup nil) (pid nil) (lock nil) (faulted nil))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::fsync-directory)
                        (lambda (directory)
                          (when (and cleanup (equal directory root))
                            (setf faulted t)
                            (error "PID cleanup directory sync failed"))
                          (funcall original directory)))
                  (ok (intake-fails-p
                       (lambda ()
                         (pg::postmaster-own-data-directory
                          "127.0.0.1" 5439 :startup-reconcile
                          (lambda ()
                            (setf pid pg::*postmaster-pid-stream*
                                  lock pg::*postmaster-data-lock* cleanup t)
                            (error "startup reconciliation refused")))))))
             (setf (symbol-function 'pg::fsync-directory) original))
           (ok faulted)
           (ok (not (open-stream-p pid)))
           (ok (not (open-stream-p lock)))
           (ok (null pg::*postmaster-pid-stream*))
           (ok (null pg::*postmaster-data-lock*))
           (ok (null pg::*postmaster-start-timestamp*))
           (ok (not (probe-file (merge-pathnames "postmaster.pid" root)))))))))

(deftest adoption-startup-owner-validates-terminal-follow-under-the-real-pid
    (call-with-adoption-follow-transaction-fixture
     (lambda (root owner handoff directory)
       (declare (ignore handoff))
       (pg::physical-adoption-follow-start owner directory)
       (sb-posix:chmod (namestring (merge-pathnames ".pg_lisp.lock" root)) #o600)
       (let ((before (pg::physical-adoption-install-snapshot root))
             (borrowed nil))
         (setf (pg::physical-adoption-owner-active-p owner) nil)
         (close (pg::physical-adoption-owner-lock owner))
         (call-with-adoption-postmaster-fixture
          root
          (lambda ()
            (flet ((validate-owner (startup-owner)
                     (setf borrowed startup-owner)
                     (ok (pg::physical-adoption-startup-owner-p startup-owner))
                     (ok (= 11 (pg::physical-adoption-follow-validate
                                startup-owner (pg::physical-adoption-follow-read startup-owner))))
                     (ok (equal before (remove "postmaster.pid" (pg::physical-adoption-install-snapshot root)
                                               :key #'first :test #'equal)))))
              (pg::postmaster-own-data-directory
               "127.0.0.1" 5439 :startup-reconcile
               (lambda () (pg::call-with-physical-adoption-startup-owner #'validate-owner))))
            (ok (not (pg::physical-adoption-owner-active-p borrowed)))
            (ok (intake-fails-p (lambda () (pg::physical-adoption-check-owner borrowed))))
            (ok (open-stream-p pg::*postmaster-data-lock*))
            (ok (pg::postmaster-pid-owned-p)))))) 
     :fork-distance 0))

(deftest adoption-startup-owner-rejects-unowned-and-replaced-pid-context
    (with-physical-store-directory (root)
      (call-with-adoption-postmaster-fixture
       root
       (lambda ()
         (ok (intake-fails-p
              (lambda () (pg::call-with-physical-adoption-startup-owner #'identity))))
         (pg::postmaster-own-data-directory "127.0.0.1" 5439)
         (pg::call-with-physical-adoption-startup-owner
          (lambda (owner)
            (ok (eq owner (pg::physical-adoption-check-owner owner)))
            (rename-file (merge-pathnames "postmaster.pid" root)
                         (merge-pathnames "postmaster.saved" root))
            (with-open-file (stream (merge-pathnames "postmaster.pid" root)
                                    :direction :output :if-exists :error)
              (write-line (format nil "~d" (sb-posix:getpid)) stream))
            (ok (intake-fails-p (lambda () (pg::physical-adoption-check-owner owner))))
            (delete-file (merge-pathnames "postmaster.pid" root))
            (rename-file (merge-pathnames "postmaster.saved" root)
                         (merge-pathnames "postmaster.pid" root))
            (ok (eq owner (pg::physical-adoption-check-owner owner)))))))))
