(in-package #:pg-physical-tests)

(defun call-with-adoption-install-prepared-fixture (function &key (fork-distance 64) (generations 1))
  (call-with-adoption-proof-driver-fixture
   (lambda (root state stage client profile record counts)
     (declare (ignore record counts))
     (with-physical-store-directory (workspace)
       (sb-posix:chmod (namestring workspace) #o700)
       (pg::physical-adoption-attempt-start-proof state stage client profile)
       (let ((prepared (pg::physical-adoption-install-prepare
                        (getf state :owner) workspace :client client)))
         (funcall function root (getf state :owner) stage client workspace prepared))))
   :fork-distance fork-distance :generations generations))

(deftest adoption-install-preparation-is-durable-without-moving-original-files
    (call-with-adoption-install-prepared-fixture
     (lambda (root owner stage client workspace prepared)
       (declare (ignore client workspace))
       (let* ((plan (getf prepared :plan))
              (record (getf plan :attempt)))
         (ok (zerop (getf prepared :step)))
         (ok (eq :proved (getf (pg::physical-adoption-attempt-read owner) :phase)))
         (ok (equal prepared (pg::physical-adoption-install-read owner)))
         (ok (zerop (pg::physical-adoption-install-validate owner prepared)))
         (ok (equal (getf plan :old)
                    (pg::physical-adoption-install-snapshot root :exclude pg::*physical-adoption-install-retained-names*)))
         (ok (equal (getf plan :stage-tree)
                    (pg::physical-adoption-install-snapshot (getf stage :directory))))
         (ok (intake-fails-p #'pg::physical-adoption-check-startup))
         (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-resume-proof owner))))
         (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-cancel owner))))
         (ok (equal record (pg::physical-adoption-attempt-read owner)))))))

(deftest adoption-install-completes-the-image-and-preserves-original-quarantine
    (call-with-adoption-install-prepared-fixture
     (lambda (root owner stage client workspace prepared)
       (declare (ignore client))
       (let* ((plan (getf prepared :plan))
              (root-inode (pg::physical-adoption-owner-inode owner))
              (lock-inode (pg::physical-adoption-owner-lock-inode owner))
              (quarantine (merge-pathnames "quarantine/" workspace))
              (installed (pg::physical-adoption-install-resume owner)))
         (ok (= (getf installed :step) (pg::physical-adoption-install-last-step plan)))
         (ok (eq :installed (getf (pg::physical-adoption-attempt-read owner) :phase)))
         (ok (= root-inode (sb-posix:stat-ino (sb-posix:lstat (namestring root)))))
         (ok (= lock-inode (sb-posix:stat-ino (sb-posix:lstat (namestring (merge-pathnames ".pg_lisp.lock" root))))))
         (ok (equal (getf plan :old)
                    (pg::physical-adoption-install-snapshot quarantine :exclude pg::*physical-adoption-install-retained-names*)))
         (ok (equal (getf plan :stage-tree)
                    (pg::physical-adoption-install-snapshot (getf stage :directory))))
         (ok (pg::physical-adoption-install-check-image root (getf plan :attempt) :installed t))
         (ok (equal installed (pg::physical-adoption-install-resume owner)))
         (ok (intake-fails-p #'pg::physical-adoption-check-startup))
         (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-cancel owner))))))))

(deftest adoption-install-resumes-every-local-publication-and-rename-cut
    (dolist (cut '(:before-attempt-file-fsync :after-attempt-file-fsync :before-attempt-rename
                   :after-attempt-rename :after-attempt-workspace-fsync :after-attempt-pgdata-fsync
                   :after-attempt-readback :before-evacuation-rename :after-evacuation-rename
                   :before-installation-rename :after-installation-rename :after-move-source-fsync
                   :after-move-target-fsync :before-progress-write :after-progress-write-chunk
                   :before-progress-fsync :after-progress-fsync :after-progress-readback
                   :before-installed-verification))
      (call-with-adoption-install-prepared-fixture
       (lambda (root owner stage client workspace prepared)
         (declare (ignore root stage client workspace))
         (let* ((fired nil)
                (pg::*physical-adoption-install-test-hook*
                  (lambda (point path)
                    (declare (ignore path))
                    (when (and (not fired) (eq point cut))
                      (setf fired t)
                      (error "installation cut")))))
           (ok (intake-fails-p (lambda () (pg::physical-adoption-install-resume owner))) (format nil "~a" cut))
           (ok fired)
           (let ((after (pg::physical-adoption-install-read owner)))
             (ok (equal (getf prepared :plan) (getf after :plan)))
             (ok (integerp (pg::physical-adoption-install-validate owner after))))
           (setf pg::*physical-adoption-install-test-hook* nil)
           (ok (= (pg::physical-adoption-install-last-step (getf prepared :plan))
                  (getf (pg::physical-adoption-install-resume owner) :step)))
           (ok (eq :installed (getf (pg::physical-adoption-attempt-read owner) :phase))))))))

(deftest adoption-install-truncates-only-a-verified-partial-progress-prefix
    (call-with-adoption-install-prepared-fixture
     (lambda (root owner stage client workspace prepared)
       (declare (ignore root stage client workspace))
       (let* ((fired nil)
              (pg::*physical-adoption-install-progress-write-limit* 1)
              (pg::*physical-adoption-install-test-hook*
                (lambda (point path)
                  (declare (ignore path))
                  (when (and (not fired) (eq point :after-progress-write-chunk))
                    (setf fired t)
                    (error "torn progress append")))))
         (ok (intake-fails-p (lambda () (pg::physical-adoption-install-resume owner))))
         (let ((partial (pg::physical-adoption-install-read owner)))
           (ok (getf partial :partial))
           (ok (zerop (getf partial :step)))
           (ok (= 1 (pg::physical-adoption-install-validate owner partial))))
         (setf pg::*physical-adoption-install-test-hook* nil
               pg::*physical-adoption-install-progress-write-limit* nil)
         (ok (= (pg::physical-adoption-install-last-step (getf prepared :plan))
                (getf (pg::physical-adoption-install-resume owner) :step)))))))

(deftest adoption-install-refuses-two-unrecorded-moves-without-further-mutation
    (call-with-adoption-install-prepared-fixture
     (lambda (root owner stage client workspace prepared)
       (declare (ignore stage client workspace))
       (let ((plan (getf prepared :plan)))
         (pg::physical-adoption-install-publish-phase owner prepared :installing)
         (loop for index below 2
               do (multiple-value-bind (from to entry kind) (pg::physical-adoption-install-move plan index)
                    (declare (ignore kind))
                    (pg::physical-adoption-install-rename
                     (pg::physical-adoption-install-entry-path from (first entry))
                     (pg::physical-adoption-install-entry-path to (first entry)))))
         (let* ((quarantine (pg::physical-adoption-install-directory plan :quarantine))
                (root-before (pg::physical-adoption-install-snapshot root :exclude pg::*physical-adoption-install-retained-names*))
                (quarantine-before (pg::physical-adoption-install-snapshot
                                    quarantine :exclude pg::*physical-adoption-install-retained-names*)))
           (ok (intake-fails-p (lambda () (pg::physical-adoption-install-resume owner))))
           (ok (equal root-before
                      (pg::physical-adoption-install-snapshot root :exclude pg::*physical-adoption-install-retained-names*)))
           (ok (equal quarantine-before
                      (pg::physical-adoption-install-snapshot quarantine :exclude pg::*physical-adoption-install-retained-names*)))
           (ok (equal prepared (pg::physical-adoption-install-read owner))))))))

(deftest adoption-install-preserves-an-unexpected-destination-occupant
    (call-with-adoption-install-prepared-fixture
     (lambda (root owner stage client workspace prepared)
       (declare (ignore stage client workspace))
       (let* ((plan (getf prepared :plan))
              (entry (first (getf plan :old)))
              (destination (pg::physical-adoption-install-entry-path
                            (pg::physical-adoption-install-directory plan :quarantine) (first entry)))
              (pg::*physical-adoption-install-test-hook*
                (lambda (point path)
                  (declare (ignore path))
                  (when (eq point :before-evacuation-rename)
                    (pg::wal-atomic-write-bytes destination (pg::string->bytes "unexpected occupant"))))))
         (ok (intake-fails-p (lambda () (pg::physical-adoption-install-resume owner))))
         (ok (equalp (pg::string->bytes "unexpected occupant") (lineage-test-bytes destination)))
         (ok (equal (getf plan :old)
                    (pg::physical-adoption-install-snapshot root :exclude pg::*physical-adoption-install-retained-names*)))
         (ok (zerop (getf (pg::physical-adoption-install-read owner) :step)))))))

(deftest adoption-install-missing-journal-does-not-reopen-cancellation
    (call-with-adoption-install-prepared-fixture
     (lambda (root owner stage client workspace prepared)
       (declare (ignore stage workspace))
       (pg::physical-adoption-install-publish-phase owner prepared :installing)
       (delete-file (pg::physical-adoption-install-path root))
       (ok (intake-fails-p (lambda () (pg::physical-adoption-install-resume owner))))
       (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-cancel owner :client client))))
       (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-resume-proof owner :client client))))
       (ok (eq :installing (getf (pg::physical-adoption-attempt-read owner) :phase)))
       (ok (intake-fails-p #'pg::physical-adoption-check-startup)))))

(deftest adoption-install-resumes-both-attempt-publication-windows-and-a-torn-final-marker
    (dolist (scenario '(:initial-attempt :final-attempt :final-marker))
      (call-with-adoption-install-prepared-fixture
       (lambda (root owner stage client workspace prepared)
         (declare (ignore root stage client workspace))
         (let* ((last (pg::physical-adoption-install-last-step (getf prepared :plan)))
                (finished 0)
                (fired nil)
                (pg::*physical-adoption-install-attempt-write-limit* 1)
                (pg::*physical-adoption-install-progress-write-limit* 1)
                (pg::*physical-adoption-install-test-hook*
                  (lambda (point path)
                    (declare (ignore path))
                    (when (and (not fired)
                               (or (and (eq point :after-attempt-write-chunk)
                                        (= finished (if (eq scenario :initial-attempt) 0 1))
                                        (not (eq scenario :final-marker)))
                                   (and (eq scenario :final-marker) (eq point :after-progress-write-chunk)
                                        (= (1- last) (getf (pg::physical-adoption-install-read owner) :step)))))
                      (setf fired t)
                      (error "torn installation publication"))
                    (when (eq point :after-attempt-readback) (incf finished)))))
           (ok (intake-fails-p (lambda () (pg::physical-adoption-install-resume owner))))
           (ok fired)
           (ok (integerp (pg::physical-adoption-install-validate owner (pg::physical-adoption-install-read owner))))
           (setf pg::*physical-adoption-install-test-hook* nil
                 pg::*physical-adoption-install-attempt-write-limit* nil
                 pg::*physical-adoption-install-progress-write-limit* nil)
           (ok (= last (getf (pg::physical-adoption-install-resume owner) :step))))))))

(defun adoption-install-run-child (root log &key cut (occurrence 1) publication partial)
  (let* ((source (asdf:system-source-directory "pg"))
         (code
           (format nil
                   "(pg::call-with-physical-adoption-owner #P~s
                     (lambda (owner)
                       (let ((seen 0) (finished 0))
                         (setf pg::*physical-adoption-install-progress-write-limit* ~s
                               pg::*physical-adoption-install-attempt-write-limit* ~s
                               pg::*physical-adoption-install-test-hook*
                               (lambda (point path)
                                 (declare (ignore path))
                                 (when (and (eq point ~s)
                                            (or (null ~s) (= ~s (1+ finished)))
                                            (= ~d (incf seen)))
                                   (sb-posix:kill (sb-posix:getpid) 9))
                                 (when (eq point :after-attempt-readback) (incf finished))))
                         (pg::physical-adoption-install-resume owner))))"
                   (namestring root) (when partial 1) (when partial 1) cut publication publication occurrence))
         (process
           (sb-ext:run-program
            "/usr/bin/env"
            (list (format nil "XDG_CACHE_HOME=/tmp/pg-adoption-install-crash-cache-~d" (sb-posix:getpid))
                  (format nil "CL_SOURCE_REGISTRY=~a//:" (namestring source))
                  "sbcl" "--noinform" "--disable-debugger" "--eval" "(require :asdf)"
                  "--eval" (format nil "(asdf:load-asd #P~s)" (namestring (merge-pathnames "pg.asd" source)))
                  "--eval" "(asdf:load-system \"pg\")"
                  "--eval" code "--eval" "(uiop:quit 0)")
            :wait t :output log :error :output :if-output-exists :supersede)))
    (unwind-protect
         (list (sb-ext:process-status process) (sb-ext:process-exit-code process))
      (sb-ext:process-close process))))

(deftest adoption-install-recovers-after-sigkill-in-a-fresh-lisp
    (dolist (scenario '((:after-attempt-write-chunk 1 t)
                        (:after-attempt-rename 1 nil)
                        (:after-evacuation-rename nil nil)
                        (:after-move-source-fsync nil nil)
                        (:after-progress-write-chunk nil t)
                        (:after-installation-rename nil nil)
                        (:after-attempt-write-chunk 2 t)
                        (:after-attempt-rename 2 nil)))
      (call-with-adoption-install-prepared-fixture
       (lambda (root owner stage client workspace prepared)
         (declare (ignore stage client workspace))
         (with-physical-store-directory (logs)
           (let* ((plan (getf prepared :plan))
                  (log (merge-pathnames "child.log" logs))
                  (root-inode (pg::physical-adoption-owner-inode owner))
                  (lock-inode (pg::physical-adoption-owner-lock-inode owner)))
             (setf (pg::physical-adoption-owner-active-p owner) nil)
             (close (pg::physical-adoption-owner-lock owner))
             (ok (equal '(:signaled 9)
                        (adoption-install-run-child root log :cut (first scenario)
                                                             :publication (second scenario) :partial (third scenario)))
                 (format nil "~s" scenario))
             (ok (equal '(:exited 0) (adoption-install-run-child root log)) (format nil "~s" scenario))
             (pg::call-with-physical-adoption-owner
              root
              (lambda (recovered)
                (let ((state (pg::physical-adoption-install-read recovered)))
                  (ok (= (pg::physical-adoption-install-last-step plan) (getf state :step)))
                  (ok (eq :installed (getf (pg::physical-adoption-attempt-read recovered) :phase)))
                  (ok (= root-inode (pg::physical-adoption-owner-inode recovered)))
                  (ok (= lock-inode (pg::physical-adoption-owner-lock-inode recovered)))
                  (ok (integerp (pg::physical-adoption-install-validate recovered state))))))))))))

(deftest adoption-install-makes-original-bytes-and-all-path-ancestors-durable-before-authority
    (call-with-adoption-proof-driver-fixture
     (lambda (root state stage client profile record counts)
       (declare (ignore record counts))
       (let ((note (merge-pathnames "operator-note" root)))
         (with-open-file (stream note :direction :output :if-exists :error)
           (write-string "unsynced operator data" stream))
         (pg::physical-adoption-attempt-start-proof state stage client profile)
         (with-physical-store-directory (parent)
           (let* ((workspace (merge-pathnames "new-parent/workspace/" parent))
                  (synced nil)
                  (objects nil)
                  (checked nil))
             (ensure-directories-exist workspace)
             (sb-posix:chmod (namestring workspace) #o700)
             (let ((pg::*physical-adoption-install-test-hook*
                     (lambda (point path)
                       (case point
                         (:after-directory-name-fsync (push path synced))
                         (:after-snapshot-object-fsync (push path objects))
                         (:before-journal-rename
                          (assert (member (uiop:pathname-parent-directory-pathname workspace) synced :test #'equal))
                          (assert (member parent synced :test #'equal))
                          (assert (member (uiop:pathname-parent-directory-pathname (getf stage :directory))
                                          synced :test #'equal))
                          (assert (member note objects :test #'equal))
                          (assert (member (merge-pathnames "PG_VERSION" (getf stage :directory))
                                          objects :test #'equal))
                          (setf checked t))))))
               (ok (pg::physical-adoption-install-prepare (getf state :owner) workspace :client client)))
             (ok checked)))))))

(deftest adoption-install-durability-preparation-error-never-moves-an-original
    (call-with-adoption-proof-driver-fixture
     (lambda (root state stage client profile record counts)
       (declare (ignore record counts))
       (let ((proved (pg::physical-adoption-attempt-start-proof state stage client profile)))
         (with-physical-store-directory (workspace)
           (sb-posix:chmod (namestring workspace) #o700)
           (let ((before (pg::physical-adoption-install-snapshot root :exclude pg::*physical-adoption-install-retained-names*))
                 (pg::*physical-adoption-install-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (eq point :after-directory-name-fsync) (error "ancestor sync failed")))))
             (ok (intake-fails-p
                  (lambda () (pg::physical-adoption-install-prepare (getf state :owner) workspace :client client))))
             (ok (not (probe-file (pg::physical-adoption-install-path root))))
             (ok (equal proved (pg::physical-adoption-attempt-read (getf state :owner))))
             (ok (equal before
                        (pg::physical-adoption-install-snapshot root :exclude pg::*physical-adoption-install-retained-names*)))))))))
