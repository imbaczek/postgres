(in-package #:pg-physical-tests)

(defun adoption-retire-test-marker (owner)
  (let* ((state (pg::physical-adoption-follow-read owner))
         (marker (adoption-retire-expected-marker (getf state :body-crc))))
    (with-open-file (stream (pg::physical-adoption-follow-path
                             (pg::physical-adoption-owner-directory owner))
                            :direction :output :element-type '(unsigned-byte 8)
                            :if-exists :append :if-does-not-exist :error)
      (write-sequence marker stream)
      (finish-output stream)
      (sb-posix:fsync (sb-sys:fd-stream-fd stream)))
    (pg::physical-adoption-follow-read owner)))

(defun call-with-adoption-retirement-fixture (function &key (distance 0) (generations 1) (marker t))
  (call-with-adoption-follow-transaction-fixture
   (lambda (root owner handoff directory)
     (declare (ignore handoff))
     (pg::physical-adoption-follow-start owner directory)
     (setf (pg::physical-adoption-owner-active-p owner) nil)
     (close (pg::physical-adoption-owner-lock owner))
     (call-with-adoption-postmaster-fixture
      root
      (lambda ()
        (pg::postmaster-own-data-directory
         "127.0.0.1" 5439 :startup-reconcile
         (lambda ()
           (pg::call-with-physical-adoption-startup-owner
            (lambda (startup-owner)
              (let ((state (if marker (adoption-retire-test-marker startup-owner)
                               (pg::physical-adoption-follow-read startup-owner))))
                (funcall function root startup-owner state directory)))))))))
   :fork-distance distance :generations generations))

(deftest adoption-retirement-validates-each-ordered-authority-prefix
    (dolist (distance '(0 64))
      (dolist (generations '(1 3))
        (loop for prefix from 0 to 3 do
          (call-with-adoption-retirement-fixture
           (lambda (root owner state directory)
             (declare (ignore directory))
             (dolist (name (subseq '("physical-adoption.handoff" "physical-adoption.install"
                                     "physical-adoption.attempt") 0 prefix))
               (delete-file (merge-pathnames name root)))
             (ok (= prefix (pg::physical-adoption-retire-validate owner state)))
             (ok (pg::postmaster-pid-owned-p)))
           :distance distance :generations generations)))))

(deftest adoption-retirement-rejects-holes-and-mutated-files
    (dolist (mutation '(:hole :attempt :root-name :base-file :workspace-name))
      (call-with-adoption-retirement-fixture
       (lambda (root owner state directory)
         (ecase mutation
           (:hole (delete-file (pg::physical-adoption-install-path root)))
           (:attempt
            (with-open-file (stream (pg::physical-adoption-attempt-path root)
                                    :direction :output :if-exists :append)
              (write-char #\Space stream)))
           (:root-name
            (with-open-file (stream (merge-pathnames "unexplained" root)
                                    :direction :output :if-exists :error)
              (write-line "unexpected" stream)))
           (:base-file
            (with-open-file (stream (merge-pathnames "PG_VERSION" root)
                                    :direction :output :if-exists :append)
              (write-char #\Space stream)))
           (:workspace-name
            (with-open-file (stream (merge-pathnames "unexplained" directory)
                                    :direction :output :if-exists :error)
              (write-line "unexpected" stream))))
         (ok (intake-fails-p (lambda () (pg::physical-adoption-retire-validate owner state)))
             (format nil "~s" mutation))))))

(deftest adoption-retirement-requires-a-complete-marker
    (call-with-adoption-retirement-fixture
     (lambda (root owner state directory)
       (declare (ignore directory))
       (ok (intake-fails-p (lambda () (pg::physical-adoption-retire-validate owner state))))
       (delete-file (pg::physical-adoption-handoff-path root))
       (ok (intake-fails-p (lambda () (pg::physical-adoption-retire-validate owner state)))))
     :marker nil))

(cl-spec:defproperty adoption-retirement-accepts-only-an-ordered-missing-prefix
    ((bits (range integer 0 7)))
  (:about pg::physical-adoption-retire-presence-prefix)
  (:trials (:normal 100))
  (let ((presence (loop for index below 3 collect (logbitp index bits)))
        (expected (case bits (7 0) (6 1) (4 2) (0 3) (otherwise :invalid))))
    (equal expected
           (handler-case (pg::physical-adoption-retire-presence-prefix presence)
             (error () :invalid)))))

(deftest generated-adoption-retirement-authority-prefixes
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'adoption-retirement-accepts-only-an-ordered-missing-prefix
                                         :seed 86 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(defun adoption-retire-test-protected-image (root)
  (let ((names (list "physical-adoption.attempt" "physical-adoption.install"
                     "physical-adoption.handoff" "physical-adoption.follow")))
    (remove-if (lambda (entry) (member (first entry) names :test #'equal))
               (pg::physical-adoption-install-snapshot root))))

(deftest adoption-retirement-removes-only-authority-after-the-marker-is-durable
    (dolist (distance '(0 64))
      (call-with-adoption-retirement-fixture
       (lambda (root owner state directory)
         (declare (ignore state))
         (let ((before (adoption-retire-test-protected-image root))
               (workspace (pg::physical-adoption-install-snapshot directory))
               (events nil))
           (let ((pg::*physical-adoption-install-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (push point events))))
             (ok (eq :retired (pg::physical-adoption-retire-resume owner))))
           (ok (equal before (adoption-retire-test-protected-image root)))
           (ok (equal workspace (pg::physical-adoption-install-snapshot directory)))
           (ok (not (intake-fails-p #'pg::physical-adoption-check-startup)))
           (setf events (reverse events))
           (ok (< (position :after-retire-marker-fsync events)
                  (position :before-retire-authority-unlink events)))
           (ok (= 3 (count :after-retire-authority-fsync events)))
           (ok (eq :after-retire-follow-fsync (car (last events))))))
       :distance distance :generations 3 :marker nil)))

(deftest adoption-retirement-resumes-error-cuts-and-every-marker-byte
    (dolist (scenario
             (append '((:before-retire-marker-write 1) (:after-retire-marker-fsync 1)
                       (:before-retire-follow-unlink 1) (:after-retire-follow-unlink 1)
                       (:after-retire-follow-fsync 1))
                     (loop for point in '(:before-retire-authority-unlink
                                          :after-retire-authority-unlink
                                          :after-retire-authority-fsync)
                           append (loop for occurrence from 1 to 3 collect (list point occurrence)))
                     (loop for occurrence from 1 to 16 collect
                                                       (list :after-retire-marker-write-chunk occurrence))))
      (call-with-adoption-retirement-fixture
       (lambda (root owner state directory)
         (declare (ignore state directory))
         (let ((seen 0) (before (adoption-retire-test-protected-image root)))
           (let ((pg::*physical-adoption-retire-write-limit* 1)
                 (pg::*physical-adoption-install-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (and (eq point (first scenario)) (= (second scenario) (incf seen)))
                       (error "retirement cut ~s" scenario)))))
             (ok (intake-fails-p (lambda () (pg::physical-adoption-retire-resume owner)))
                 (format nil "~s" scenario)))
           (ok (= seen (second scenario)))
           (when (probe-file (pg::physical-adoption-follow-path root))
             (ok (eq :retired (pg::physical-adoption-retire-resume owner))))
           (ok (not (intake-fails-p #'pg::physical-adoption-check-startup)))
           (ok (equal before (adoption-retire-test-protected-image root)))))
       :marker nil)))

(deftest adoption-retirement-never-borrows-an-offline-owner
    (call-with-adoption-follow-transaction-fixture
     (lambda (root owner handoff directory)
       (declare (ignore handoff))
       (pg::physical-adoption-follow-start owner directory)
       (let ((before (lineage-test-bytes (pg::physical-adoption-follow-path root))))
         (ok (intake-fails-p (lambda () (pg::physical-adoption-retire-resume owner))))
         (ok (intake-fails-p
              (lambda ()
                (pg::physical-adoption-retire-validate owner (pg::physical-adoption-follow-read owner)))))
         (ok (equalp before (lineage-test-bytes (pg::physical-adoption-follow-path root))))))))

(deftest adoption-retirement-rechecks-ownership-after-removing-the-last-fence
    (dolist (mutation '(:pid :capability))
      (call-with-adoption-retirement-fixture
       (lambda (root owner state directory)
         (declare (ignore state directory))
         (let ((seen nil)
               (pid (merge-pathnames "postmaster.pid" root))
               (saved (merge-pathnames "postmaster.saved" root)))
           (unwind-protect
                (progn
                  (let ((pg::*physical-adoption-install-test-hook*
                          (lambda (point path)
                            (declare (ignore path))
                            (when (eq point :after-retire-follow-fsync)
                              (setf seen t)
                              (ecase mutation
                                (:pid
                                 (rename-file pid saved)
                                 (with-open-file (stream pid :direction :output :if-exists :error)
                                   (write-line (format nil "~d" (sb-posix:getpid)) stream)))
                                (:capability (setf (pg::physical-adoption-owner-active-p owner) nil)))))))
                    (ok (intake-fails-p (lambda () (pg::physical-adoption-retire-resume owner)))
                        (format nil "~s" mutation)))
                  (ok seen)
                  (ok (not (probe-file (pg::physical-adoption-follow-path root)))))
             (when (probe-file saved)
               (delete-file pid)
               (rename-file saved pid)))))
       :marker nil)))

(defun adoption-retire-run-child (root log &key cut (occurrence 1) partial)
  (let* ((source (asdf:system-source-directory "pg"))
         (code (format nil
                       "(progn
                         (setf pg::*pgdata* #P~s)
                         (let ((seen 0))
                           (setf pg::*physical-adoption-retire-write-limit* ~s
                                 pg::*physical-adoption-install-test-hook*
                                 (lambda (point path)
                                   (declare (ignore path))
                                   (when (and (eq point ~s) (= ~d (incf seen)))
                                     (sb-posix:kill (sb-posix:getpid) 9))))
                           (pg::postmaster-own-data-directory
                            \"127.0.0.1\" 5439
                            :startup-check #'pg::physical-adoption-startup-preflight
                            :startup-reconcile #'pg::physical-adoption-reconcile-startup)
                           (pg::physical-adoption-check-startup)
                           (assert (pg::postmaster-pid-owned-p))
                           (pg::physical-recovery-verify-base-state)
                           (pg::postmaster-remove-pid-file)
                           (close pg::*postmaster-data-lock*)
                           (setf pg::*postmaster-data-lock* nil)))"
                       (namestring root) (when partial 1) cut occurrence))
         (process (sb-ext:run-program
                   "/usr/bin/env"
                   (list (format nil "XDG_CACHE_HOME=/tmp/pg-adoption-startup-crash-cache-~d" (sb-posix:getpid))
                         (format nil "CL_SOURCE_REGISTRY=~a//:" (namestring source))
                         "sbcl" "--noinform" "--no-sysinit" "--no-userinit" "--non-interactive"
                         "--eval" "(require :asdf)" "--eval" "(asdf:load-system \"pg\")"
                         "--eval" code)
                   :wait t :output log :error :output :if-output-exists :supersede)))
    (unwind-protect
         (list (sb-ext:process-status process) (sb-ext:process-exit-code process))
      (sb-ext:process-close process))))

(deftest adoption-retirement-recovers-real-postmaster-sigkill
    (loop for scenario in '((:before-retire-marker-write 1 nil)
                            (:after-retire-marker-write-chunk 7 t)
                            (:after-retire-marker-fsync 1 nil)
                            (:after-retire-authority-unlink 1 nil)
                            (:after-retire-authority-unlink 2 nil)
                            (:after-retire-authority-unlink 3 nil)
                            (:after-retire-authority-fsync 1 nil)
                            (:after-retire-authority-fsync 2 nil)
                            (:after-retire-authority-fsync 3 nil)
                            (:before-retire-follow-unlink 1 nil)
                            (:after-retire-follow-unlink 1 nil)
                            (:after-retire-follow-fsync 1 nil))
          for index from 0 do
            (call-with-adoption-follow-transaction-fixture
             (lambda (root owner handoff directory)
               (declare (ignore handoff))
               (pg::physical-adoption-follow-start owner directory)
               (sb-posix:chmod (namestring (merge-pathnames ".pg_lisp.lock" root)) #o600)
               (let ((before (adoption-retire-test-protected-image root))
                     (root-inode (pg::physical-adoption-owner-inode owner))
                     (lock-inode (pg::physical-adoption-owner-lock-inode owner)))
                 (setf (pg::physical-adoption-owner-active-p owner) nil)
                 (close (pg::physical-adoption-owner-lock owner))
                 (with-physical-store-directory (logs)
                   (let ((log (merge-pathnames "child.log" logs)))
                     (ok (equal '(:signaled 9)
                                (adoption-retire-run-child root log :cut (first scenario)
                                                                    :occurrence (second scenario)
                                                                    :partial (third scenario)))
                         (format nil "~s" scenario))
                     (ok (equal '(:exited 0) (adoption-retire-run-child root log))
                         (format nil "~s" scenario))))
                 (ok (equal before (adoption-retire-test-protected-image root)))
                 (pg::call-with-physical-adoption-owner
                  root
                  (lambda (recovered)
                    (ok (= root-inode (pg::physical-adoption-owner-inode recovered)))
                    (ok (= lock-inode (pg::physical-adoption-owner-lock-inode recovered)))))))
             :fork-distance (if (oddp index) 64 0) :generations 3)))
