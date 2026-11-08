(in-package #:pg-physical-tests)

(defun call-with-adoption-handoff-fixture (function &key (fork-distance 64) (generations 1))
  (call-with-adoption-install-prepared-fixture
   (lambda (root owner stage client workspace prepared)
     (declare (ignore client prepared))
     (let ((installed (pg::physical-adoption-install-resume owner)))
       (with-physical-store-directory (handoff)
         (sb-posix:chmod (namestring handoff) #o700)
         (funcall function root owner stage workspace installed handoff))))
   :fork-distance fork-distance :generations generations))

(deftest adoption-handoff-prepares-a-durable-intent-without-changing-the-image
    (call-with-adoption-handoff-fixture
     (lambda (root owner stage workspace installed handoff)
       (declare (ignore stage workspace installed))
       (let* ((before (pg::physical-adoption-install-snapshot
                       root :exclude pg::*physical-adoption-install-retained-names*))
              (value (pg::physical-adoption-handoff-prepare owner handoff)))
         (ok (eq :prepared (pg::physical-adoption-handoff-validate owner value)))
         (ok (not (probe-file (merge-pathnames "pg_physical_receive/" root))))
         (ok (probe-file (merge-pathnames "spool/receive.control" handoff)))
         (dolist (entry before)
           (ok (equal (second entry)
                      (pg::physical-adoption-install-tree
                       root (first entry) (pg::physical-adoption-owner-device owner)
                       (pg::physical-adoption-install-mount-id root)))))
         (ok (intake-fails-p #'pg::physical-adoption-check-startup))))))

(deftest adoption-handoff-installs-an-empty-spool-and-stays-fenced
    (call-with-adoption-handoff-fixture
     (lambda (root owner stage workspace installed handoff)
       (declare (ignore stage workspace))
       (let* ((root-inode (pg::physical-adoption-owner-inode owner))
              (lock-inode (pg::physical-adoption-owner-lock-inode owner))
              (attempt (pg::physical-adoption-handoff-file-fact
                        (pg::physical-adoption-attempt-path root) pg::+physical-adoption-attempt-maximum-bytes+))
              (journal (pg::physical-adoption-handoff-file-fact
                        (pg::physical-adoption-install-path root) pg::+physical-adoption-install-maximum-bytes+))
              (control (lineage-test-bytes (merge-pathnames "physical-recovery.control" root))))
         (multiple-value-bind (value location) (pg::physical-adoption-handoff-start owner handoff)
           (ok (eq :ready location))
           (ok (eq :ready (pg::physical-adoption-handoff-validate owner value)))
           (ok (equal value (pg::physical-adoption-handoff-resume owner)))
           (ok (= root-inode (pg::physical-adoption-owner-inode owner)))
           (ok (= lock-inode (pg::physical-adoption-owner-lock-inode owner)))
           (ok (equal attempt (getf value :attempt-file)))
           (ok (equal journal (getf value :journal-file)))
           (ok (equal installed (pg::physical-adoption-install-read owner)))
           (ok (eq :installed (getf (pg::physical-adoption-attempt-read owner) :phase)))
           (ok (equalp control (lineage-test-bytes (merge-pathnames "physical-recovery.control" root))))
           (ok (let ((pg::*pgdata* root)) (not (probe-file (pg::physical-follow-config-path)))))
           (ok (not (probe-file (merge-pathnames "spool/" handoff))))
           (ok (intake-fails-p #'pg::physical-adoption-check-startup)))))))

(deftest adoption-handoff-resumes-published-cuts-and-preserves-preauthority-images
    (dolist (scenario '((:after-handoff-spool-create 1 nil)
                        (:after-handoff-intent-write-chunk 1 nil)
                        (:after-handoff-intent-file-fsync 1 nil)
                        (:after-directory-name-fsync 1 nil)
                        (:before-handoff-intent-rename 1 nil)
                        (:after-handoff-intent-rename 1 t)
                        (:after-handoff-workspace-fsync 1 t)
                        (:after-handoff-root-fsync 1 t)
                        (:after-handoff-workspace-fsync 2 t)
                        (:before-handoff-spool-rename 1 t)
                        (:after-handoff-spool-rename 1 t)
                        (:after-handoff-workspace-fsync 3 t)
                        (:after-handoff-root-fsync 3 t)))
      (call-with-adoption-handoff-fixture
       (lambda (root owner stage workspace installed handoff)
         (declare (ignore stage workspace))
         (let ((seen 0)
               (before (pg::physical-adoption-install-snapshot
                        root :exclude pg::*physical-adoption-install-retained-names*))
               (control (lineage-test-bytes (merge-pathnames "physical-recovery.control" root))))
           (let ((pg::*physical-adoption-handoff-write-limit* 1)
                 (pg::*physical-adoption-install-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (and (eq point (first scenario))
                                (= (second scenario) (incf seen)))
                       (error "handoff cut ~s" scenario)))))
             (ok (intake-fails-p (lambda () (pg::physical-adoption-handoff-start owner handoff)))
                 (format nil "~s" scenario)))
           (ok (= seen (second scenario)))
           (if (third scenario)
               (progn
                 (pg::physical-adoption-handoff-resume owner)
                 (ok (eq :ready (pg::physical-adoption-handoff-validate
                                 owner (pg::physical-adoption-handoff-read owner)))))
               (progn
                 (ok (not (probe-file (pg::physical-adoption-handoff-path root))))
                 (ok (integerp (pg::physical-adoption-install-validate owner installed)))
                 (ok (equal before (pg::physical-adoption-install-snapshot
                                    root :exclude pg::*physical-adoption-install-retained-names*)))))
           (ok (equalp control (lineage-test-bytes (merge-pathnames "physical-recovery.control" root))))
           (ok (intake-fails-p #'pg::physical-adoption-check-startup)))))))

(deftest adoption-handoff-rejects-changed-authority-images-and-extra-names
    (dolist (mode '(:attempt-inode :journal-inode :intent-inode :image :incoming-extra
                    :quarantine-extra :spool-inode :destination :both-spools :guard-inode))
      (call-with-adoption-handoff-fixture
       (lambda (root owner stage workspace installed handoff)
         (declare (ignore stage installed))
         (let* ((value (pg::physical-adoption-handoff-prepare owner handoff))
                (source (merge-pathnames "spool/" handoff)))
           (flet ((replace-same (path)
                    (pg::wal-atomic-write-bytes path (lineage-test-bytes path))))
             (ecase mode
               (:attempt-inode (replace-same (pg::physical-adoption-attempt-path root)))
               (:journal-inode (replace-same (pg::physical-adoption-install-path root)))
               (:intent-inode (replace-same (pg::physical-adoption-handoff-path root)))
               (:image (pg::wal-atomic-write-bytes (merge-pathnames "PG_VERSION" root) #(88)))
               (:incoming-extra
                (pg::wal-atomic-write-bytes (merge-pathnames "incoming/physical-adoption.attempt" workspace) #(1)))
               (:quarantine-extra
                (pg::wal-atomic-write-bytes (merge-pathnames "quarantine/physical-adoption.attempt" workspace) #(1)))
               (:spool-inode (replace-same (merge-pathnames "receive.lock" source)))
               (:destination (pg::wal-atomic-write-bytes (merge-pathnames "pg_physical_receive" root) #(99)))
               (:both-spools
                (let ((copy (merge-pathnames "pg_physical_receive/" root)))
                  (sb-posix:mkdir (namestring copy) #o700)
                  (dolist (name '("receive.control" "receive.lock"))
                    (pg::wal-atomic-write-bytes (merge-pathnames name copy)
                                                (lineage-test-bytes (merge-pathnames name source))))))
               (:guard-inode (replace-same (merge-pathnames "physical-adoption.install" handoff)))))
           (let ((before (pg::physical-adoption-install-snapshot root))
                 (work-before (pg::physical-adoption-install-snapshot handoff)))
             (ok (intake-fails-p (lambda () (pg::physical-adoption-handoff-resume owner)))
                 (symbol-name mode))
             (ok (equal before (pg::physical-adoption-install-snapshot root)))
             (ok (equal work-before (pg::physical-adoption-install-snapshot handoff))))
           (ok (probe-file source))
           (ok (intake-fails-p #'pg::physical-adoption-check-startup))
           (ok value))))))

(deftest adoption-handoff-presence-fences-startup-without-other-adoption-records
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root))
        (pg::wal-atomic-write-bytes (pg::physical-adoption-handoff-path root) #(1))
        (ok (intake-fails-p #'pg::physical-adoption-check-startup))
        (delete-file (pg::physical-adoption-handoff-path root))
        (sb-posix:symlink "missing" (namestring (pg::physical-adoption-handoff-path root)))
        (ok (intake-fails-p #'pg::physical-adoption-check-startup)))))

(defun adoption-handoff-run-child (root handoff log &key start cut (occurrence 1) partial)
  (let* ((source (asdf:system-source-directory "pg"))
         (command (if start
                      (format nil "(pg::physical-adoption-handoff-start owner #P~s)" (namestring handoff))
                      "(pg::physical-adoption-handoff-resume owner)"))
         (code (format nil
                       "(pg::call-with-physical-adoption-owner #P~s
                         (lambda (owner)
                           (let ((seen 0))
                             (setf pg::*physical-adoption-handoff-write-limit* ~s
                                   pg::*physical-adoption-install-test-hook*
                                   (lambda (point path)
                                     (declare (ignore path))
                                     (when (and (eq point ~s) (= ~d (incf seen)))
                                       (sb-posix:kill (sb-posix:getpid) 9))))
                             ~a)))"
                       (namestring root) (when partial 1) cut occurrence command))
         (process (sb-ext:run-program
                   "/usr/bin/env"
                   (list (format nil "XDG_CACHE_HOME=/tmp/pg-adoption-handoff-crash-cache-~d" (sb-posix:getpid))
                         (format nil "CL_SOURCE_REGISTRY=~a//:" (namestring source))
                         "sbcl" "--noinform" "--disable-debugger" "--non-interactive"
                         "--eval" "(require :asdf)" "--eval" "(asdf:load-system \"pg\")"
                         "--eval" code)
                   :wait t :output log :error :output :if-output-exists :supersede)))
    (unwind-protect
         (list (sb-ext:process-status process) (sb-ext:process-exit-code process))
      (sb-ext:process-close process))))

(deftest adoption-handoff-recovers-sigkill-with-fresh-ownership
    (dolist (scenario '((:after-handoff-spool-create 1 nil nil)
                        (:after-handoff-intent-write-chunk 1 nil t)
                        (:after-handoff-intent-rename 1 t nil)
                        (:after-handoff-workspace-fsync 1 t nil)
                        (:after-handoff-spool-rename 1 t nil)
                        (:after-handoff-workspace-fsync 3 t nil)
                        (:after-handoff-root-fsync 3 t nil)))
      (call-with-adoption-handoff-fixture
       (lambda (root owner stage workspace installed handoff)
         (declare (ignore stage workspace))
         (with-physical-store-directory (logs)
           (let ((root-inode (pg::physical-adoption-owner-inode owner))
                 (lock-inode (pg::physical-adoption-owner-lock-inode owner))
                 (log (merge-pathnames "child.log" logs)))
             (setf (pg::physical-adoption-owner-active-p owner) nil)
             (close (pg::physical-adoption-owner-lock owner))
             (ok (equal '(:signaled 9)
                        (adoption-handoff-run-child
                         root handoff log :start t :cut (first scenario)
                         :occurrence (second scenario) :partial (fourth scenario)))
                 (format nil "~s" scenario))
             (when (third scenario)
               (ok (equal '(:exited 0) (adoption-handoff-run-child root handoff log))
                   (format nil "~s" scenario)))
             (pg::call-with-physical-adoption-owner
              root
              (lambda (recovered)
                (ok (= root-inode (pg::physical-adoption-owner-inode recovered)))
                (ok (= lock-inode (pg::physical-adoption-owner-lock-inode recovered)))
                (if (third scenario)
                    (ok (eq :ready (pg::physical-adoption-handoff-validate
                                    recovered (pg::physical-adoption-handoff-read recovered))))
                    (progn
                      (ok (not (probe-file (pg::physical-adoption-handoff-path root))))
                      (ok (integerp (pg::physical-adoption-install-validate recovered installed)))))))))))))

(deftest adoption-handoff-syncs-spool-and-workspace-ancestors-before-authority
    (call-with-adoption-handoff-fixture
     (lambda (root owner stage workspace installed handoff)
       (declare (ignore root stage workspace installed))
       (let ((objects nil) (ancestors nil) (checked nil))
         (let ((pg::*physical-adoption-install-test-hook*
                 (lambda (point path)
                   (case point
                     (:after-snapshot-object-fsync (push (namestring path) objects))
                     (:after-directory-name-fsync (push path ancestors))
                     (:before-handoff-intent-rename
                      (dolist (name '("intent.pending" "spool/receive.control" "spool/receive.lock"))
                        (assert (member (namestring (merge-pathnames name handoff)) objects :test #'string=)))
                      (assert (member handoff ancestors :test #'equal))
                      (assert (member (uiop:pathname-parent-directory-pathname handoff) ancestors :test #'equal))
                      (setf checked t))))))
           (pg::physical-adoption-handoff-prepare owner handoff))
         (ok checked)))))

(deftest adoption-handoff-rechecks-a-late-destination-occupant
    (call-with-adoption-handoff-fixture
     (lambda (root owner stage workspace installed handoff)
       (declare (ignore stage workspace installed))
       (pg::physical-adoption-handoff-prepare owner handoff)
       (let ((target (merge-pathnames "pg_physical_receive" root)))
         (let ((pg::*physical-adoption-install-test-hook*
                 (lambda (point path)
                   (declare (ignore path))
                   (when (eq point :before-handoff-spool-rename)
                     (pg::wal-atomic-write-bytes target #(111 99 99))))))
           (ok (intake-fails-p (lambda () (pg::physical-adoption-handoff-resume owner)))))
         (ok (equalp #(111 99 99) (lineage-test-bytes target)))
         (ok (probe-file (merge-pathnames "spool/receive.control" handoff)))))))

(deftest adoption-handoff-workspace-closes-interrupted-store-creation
    (dolist (kind '(:normal :error :throw :interrupt))
      (testing (symbol-name kind)
               (call-with-adoption-handoff-fixture
                (lambda (root owner stage workspace installed handoff)
                  (declare (ignore stage workspace))
                  (let ((original (symbol-function 'pg::physical-store-open))
                        (spool (merge-pathnames "spool/" handoff))
                        (opened nil) (calls 0) (control nil) (outcome nil)
                        (tag (gensym "HANDOFF-STORE-CREATE"))
                        (inventory (pg::physical-adoption-install-snapshot root)))
                    (unwind-protect
                         (progn
                           (setf (symbol-function 'pg::physical-store-open)
                                 (lambda (&rest arguments)
                                   (setf opened (apply original arguments))
                                   (incf calls)
                                   (setf control (lineage-test-bytes (merge-pathnames "receive.control" spool)))
                                   (case kind
                                     (:error (error "injected handoff store creation"))
                                     (:throw (throw tag :cut))
                                     (:interrupt
                                      (sb-thread:interrupt-thread
                                       sb-thread:*current-thread*
                                       (lambda () (error "injected handoff store creation")))))
                                   opened))
                           (setf outcome
                                 (catch tag
                                   (handler-case
                                       (multiple-value-list
                                        (pg::physical-adoption-handoff-create-workspace owner installed handoff))
                                     (error (condition)
                                       (when (search "injected handoff store creation"
                                                     (princ-to-string condition))
                                         :cut)))))
                           (setf (symbol-function 'pg::physical-store-open) original)
                           (if (eq kind :normal)
                               (progn
                                 (ok (= 3 (length outcome)))
                                 (ok (equal (first outcome)
                                            (pg::physical-adoption-install-persistent-fact
                                             (pg::physical-adoption-install-auxiliary-fact handoff) :directory t)))
                                 (let ((stat (sb-posix:stat (namestring spool))))
                                   (ok (equal (second outcome)
                                              (list :device (sb-posix:stat-dev stat) :inode (sb-posix:stat-ino stat)))))
                                 (ok (equal (third outcome) (pg::physical-adoption-install-snapshot spool))))
                               (ok (eq outcome :cut)))
                           (ok (= calls 1))
                           (ok (and opened (pg::physical-store-closed-p opened)))
                           (ok (and opened (not (open-stream-p (pg::physical-store-lock-stream opened)))))
                           (ok (equal inventory (pg::physical-adoption-install-snapshot root)))
                           (ok (equalp control (lineage-test-bytes (merge-pathnames "receive.control" spool))))
                           (ok (and opened
                                    (handler-case
                                        (let ((fresh
                                                (funcall original spool
                                                         (pg::physical-store-system-id opened)
                                                         (pg::physical-store-timeline opened)
                                                         :start-lsn (pg::physical-store-base-lsn opened)
                                                         :discard-tail nil :create-lock nil)))
                                          (unwind-protect (pg::physical-store-check-held-lock fresh)
                                            (pg::physical-store-close fresh)))
                                      (error () nil))))
                           (ok (equalp control (lineage-test-bytes (merge-pathnames "receive.control" spool)))))
                      (setf (symbol-function 'pg::physical-store-open) original)
                      (when opened (pg::physical-store-close opened)))))))))
