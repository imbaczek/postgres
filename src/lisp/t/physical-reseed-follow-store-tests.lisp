(in-package #:pg-physical-tests)

(cl-spec:defproperty reseed-follow-intent-never-certifies-a-remote-outcome
    ((kind-choice (range integer 0 2)) (phase-choice (range integer 0 3)) (empty boolean))
  (:about pg::physical-reseed-follow-intent-copy)
  (:trials (:normal 100))
  (let* ((phase (nth phase-choice '(:preparing :proved :canceling :canceled)))
         (record (reseed-follow-codec-fixture
                  (nth kind-choice '(:same-timeline :descendant :retained-timeline)) phase)))
    (when empty (setf (getf record :generations) nil))
    (let* ((before (pg::physical-bootstrap-manifest-text record))
           (copy (handler-case (pg::physical-reseed-follow-intent-copy record) (error () nil))))
      (and (eq (not (null copy)) (or empty (eq phase :preparing)))
           (equal before (pg::physical-bootstrap-manifest-text record))
           (or (null copy)
               (not (eq (getf (getf copy :attempt) :source)
                        (getf (getf record :attempt) :source))))))))

(deftest generated-reseed-follow-intent-authority-boundary
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-intent-never-certifies-a-remote-outcome
                                       :seed 117 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-intent-publication-is-durable-and-keeps-predecessors
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming))
       (let ((install (lineage-test-bytes (pg::physical-reseed-install-path root)))
             (pid (lineage-test-bytes (merge-pathnames "postmaster.pid" quarantine)))
             (attempt (lineage-test-bytes (pg::physical-reseed-attempt-path root))))
         (pg::call-with-physical-reseed-follow-owner
          root
          (lambda (owner)
            (let* ((initial (pg::physical-reseed-follow-initial-record owner))
                   (saved (pg::physical-reseed-follow-publish-intent owner initial)))
              (ok (equal initial saved))
              (ok (equal saved (pg::physical-reseed-follow-read owner)))
              (ok (equal saved (pg::physical-reseed-follow-recover-intent owner)))
              (ok (equal saved (pg::physical-reseed-follow-publish-intent owner initial :previous saved)))
              (ok (= (getf (getf saved :install) :step) (pg::physical-reseed-install-last-step plan))))))
         (ok (equalp install (lineage-test-bytes (pg::physical-reseed-install-path root))))
         (ok (equalp pid (lineage-test-bytes (merge-pathnames "postmaster.pid" quarantine))))
         (ok (equalp attempt (lineage-test-bytes (pg::physical-reseed-attempt-path root))))))))

(deftest reseed-follow-intent-recovery-refuses-an-incomplete-temporary
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (let ((path (merge-pathnames "physical-reseed.follow.tmp" root))
             (bytes (pg::string->bytes "partial")))
         (reseed-follow-owner-test-write path bytes)
         (pg::call-with-physical-reseed-follow-owner
          root
          (lambda (owner)
            (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-recover-intent owner))))
            (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-read owner))))))
         (ok (equalp bytes (lineage-test-bytes path)))))))

(deftest reseed-follow-intent-recovery-promotes-a-complete-genesis
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (let ((record nil) (path (merge-pathnames "physical-reseed.follow.tmp" root)))
         (pg::call-with-physical-reseed-follow-owner
          root (lambda (owner) (setf record (pg::physical-reseed-follow-initial-record owner))))
         (reseed-follow-owner-test-write path (pg::physical-reseed-follow-image record))
         (pg::call-with-physical-reseed-follow-owner
          root
          (lambda (owner)
            (ok (equal record (pg::physical-reseed-follow-recover-intent owner)))
            (ok (equal record (pg::physical-reseed-follow-read owner)))))
         (ok (not (pg::physical-adoption-path-present-p path)))))))

(defun reseed-follow-intent-test-record (initial root)
  (let ((record (pg::physical-reseed-follow-copy initial))
        (pg::*pgdata* root))
    (let* ((base (pg::physical-recovery-verify-base-state))
           (timeline (getf base :timeline)))
      (multiple-value-bind (bytes entry) (pg::physical-recovery-read-base-overlap base root)
        (setf (getf record :generations)
              (list
               (list :generation 1 :name "target_follow_1"
                     :coordinates
                     (list :version 1 :system-id (getf base :system-id)
                           :base-timeline timeline :base (getf base :end)
                           :manifest-digest (getf base :manifest-digest)
                           :source-timeline timeline :receive-timeline timeline
                           :receive-lsn (getf base :end) :receive-digest #xCBF29CE484222325
                           :overlap-start (- (getf base :end) (length bytes)) :overlap-digest (third entry))
                     :source-history (if (= timeline 1) "" (pg::bytes->string (pg::timeline-history-bytes timeline root)))
                     :phase :preparing :anchor nil)))))
    (pg::physical-reseed-follow-copy record)))

(deftest reseed-follow-intent-rejects-invented-local-overlap-before-creating-intent
    (dolist (key '(:overlap-start :overlap-digest))
      (call-with-reseed-follow-installed-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine plan))
         (pg::call-with-physical-reseed-follow-owner
          root
          (lambda (owner)
            (let* ((record (reseed-follow-intent-test-record (pg::physical-reseed-follow-initial-record owner) root))
                   (coordinates (getf (first (getf record :generations)) :coordinates)))
              (incf (getf coordinates key))
              (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-publish-intent owner record))))
              (ok (not (pg::physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow" root))))
              (ok (not (pg::physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow.tmp" root)))))))))))

(deftest reseed-follow-intent-publishes-preparing-after-empty-genesis
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((initial (pg::physical-reseed-follow-initial-record owner))
                 (record (reseed-follow-intent-test-record initial root))
                 (previous (pg::physical-reseed-follow-publish-intent owner initial)))
            (ok (equal record (pg::physical-reseed-follow-publish-intent owner record :previous previous)))
            (ok (equal record (pg::physical-reseed-follow-read owner)))
            (ok (equal record (pg::physical-reseed-follow-recover-intent owner)))
            (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-publish-intent owner initial :previous record))))))))))

(deftest reseed-follow-intent-reentrant-public-operations-cannot-admit-state
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((record (reseed-follow-intent-test-record (pg::physical-reseed-follow-initial-record owner) root))
                 (calls 0)
                 (pg::*physical-reseed-follow-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (member point '(:after-follow-temporary-create :after-follow-write-chunk
                                           :after-follow-rename :after-follow-readback))
                       (incf calls)
                       (dolist (function (list (lambda () (pg::physical-reseed-follow-read owner))
                                               (lambda () (pg::physical-reseed-follow-initial-record owner))
                                               (lambda () (pg::physical-reseed-follow-recover-intent owner))
                                               (lambda () (pg::physical-reseed-follow-publish-intent owner record))))
                         (ok (intake-fails-p function)))))))
            (ok (equal record (pg::physical-reseed-follow-publish-intent owner record)))
            (ok (= 4 calls))))))))

(deftest reseed-follow-intent-failure-cuts-reopen-as-durable-intent-or-remain-fenced
    (dolist (cut '(:before-follow-temporary-create :after-follow-temporary-create
                   :after-follow-write-chunk :before-follow-temporary-fsync :after-follow-temporary-fsync
                   :before-follow-temporary-readback :after-follow-temporary-readback
                   :before-follow-rename :after-follow-rename :after-follow-directory-fsync
                   :before-follow-readback :after-follow-readback))
      (call-with-reseed-follow-installed-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine plan))
         (let ((record nil) (fired 0))
           (pg::call-with-physical-reseed-follow-owner
            root
            (lambda (owner)
              (setf record (reseed-follow-intent-test-record (pg::physical-reseed-follow-initial-record owner) root))
              (let ((pg::*physical-reseed-follow-write-limit* 257)
                    (pg::*physical-reseed-follow-test-hook*
                      (lambda (point path)
                        (declare (ignore path))
                        (when (and (eq point cut) (zerop fired))
                          (incf fired)
                          (error "intent publication cut")))))
                (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-publish-intent owner record)))))
              (ok (= 1 fired))
              (if (eq cut :before-follow-temporary-create)
                  (ok (null (pg::physical-reseed-follow-read owner)))
                  (progn
                    (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-read owner))))
                    (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-recover-intent owner))))
                    (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-publish-intent owner record))))))))
           (let* ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                  (bytes (when (pg::physical-adoption-path-present-p temporary) (lineage-test-bytes temporary)))
                  (complete (and bytes (not (intake-fails-p (lambda () (pg::physical-reseed-follow-decode bytes)))))))
             (pg::call-with-physical-reseed-follow-owner
              root
              (lambda (owner)
                (cond
                  ((and bytes (not complete))
                   (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-recover-intent owner))))
                   (ok (equalp bytes (lineage-test-bytes temporary))))
                  ((eq cut :before-follow-temporary-create)
                   (ok (null (pg::physical-reseed-follow-recover-intent owner))))
                  (t
                   (ok (equal record (pg::physical-reseed-follow-recover-intent owner)))
                   (ok (equal record (pg::physical-reseed-follow-read owner)))))))))))))

(deftest reseed-follow-intent-freezes-callers-record-before-filesystem-checks
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((record (reseed-follow-intent-test-record (pg::physical-reseed-follow-initial-record owner) root))
                 (expected (pg::physical-reseed-follow-copy record))
                 (original (symbol-function 'pg::physical-directory-check-owner-common))
                 (fired 0))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-directory-check-owner-common)
                         (lambda (value)
                           (when (zerop fired)
                             (incf fired)
                             (setf (char (getf (getf (getf record :attempt) :request) :token) 0) #\b
                                   (char (getf (first (getf record :generations)) :name) 0) #\x))
                           (funcall original value)))
                   (ok (equal expected (pg::physical-reseed-follow-publish-intent owner record)))
                   (ok (= 1 fired)))
              (setf (symbol-function 'pg::physical-directory-check-owner-common) original))))))))

(deftest reseed-follow-intent-never-overwrites-a-temporary-found-before-create
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((record (pg::physical-reseed-follow-initial-record owner))
                 (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                 (bytes (pg::string->bytes "preserve foreign intent"))
                 (fired 0)
                 (pg::*physical-reseed-follow-test-hook*
                   (lambda (point path)
                     (when (eq point :before-follow-temporary-create)
                       (incf fired)
                       (reseed-follow-owner-test-write path bytes)))))
            (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-publish-intent owner record))))
            (ok (= 1 fired))
            (ok (equalp bytes (lineage-test-bytes temporary)))
            (ok (not (pg::physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow" root))))))))))

(deftest reseed-follow-intent-refuses-a-replaced-temporary-before-rename
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((record (pg::physical-reseed-follow-initial-record owner))
                 (bytes (pg::physical-reseed-follow-image record))
                 (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                 (fired 0) (inode nil)
                 (pg::*physical-reseed-follow-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (eq point :before-follow-rename)
                       (incf fired)
                       (sb-posix:unlink (namestring temporary))
                       (reseed-follow-owner-test-write temporary bytes)
                       (setf inode (sb-posix:stat-ino (sb-posix:lstat (namestring temporary))))))))
            (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-publish-intent owner record))))
            (ok (= 1 fired))
            (ok (equalp bytes (lineage-test-bytes temporary)))
            (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring temporary)))))
            (ok (not (pg::physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow" root))))))))))

(deftest reseed-follow-intent-does-not-clear-uncertainty-before-the-image-postcheck
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((record (pg::physical-reseed-follow-initial-record owner))
                 (returned nil) (fired 0)
                 (original (symbol-function 'pg::physical-reseed-follow-write-intent))
                 (pg::*physical-reseed-install-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (and returned (eq point :after-install-image-validation) (zerop fired))
                       (incf fired)
                       (error "image postcheck cut")))))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-reseed-follow-write-intent)
                         (lambda (value root-fd candidate bytes previous check)
                           (multiple-value-prog1 (funcall original value root-fd candidate bytes previous check)
                             (setf returned t))))
                   (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-publish-intent owner record))))
                   (ok (= 1 fired))
                   (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                   (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                   (ok (pg::physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow" root)))
                   (ok (not (pg::physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow.tmp" root))))
                   (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-read owner))))
                   (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-recover-intent owner)))))
              (setf (symbol-function 'pg::physical-reseed-follow-write-intent) original))))))))

(deftest reseed-follow-intent-keeps-attempt-identity-through-the-image-postcheck
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((record (pg::physical-reseed-follow-initial-record owner))
                 (attempt (pg::physical-reseed-attempt-copy (getf plan :attempt)))
                 (original (symbol-function 'pg::physical-reseed-follow-write-intent))
                 (returned nil) (fired 0)
                 (pg::*physical-reseed-install-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (and returned (eq point :after-install-image-validation) (zerop fired))
                       (incf fired)
                       (let ((temporary (merge-pathnames "replacement-attempt" root)))
                         (setf (getf attempt :phase) :aborting)
                         (reseed-follow-owner-test-write temporary (pg::physical-reseed-attempt-image attempt))
                         (sb-posix:rename (namestring temporary) (namestring (pg::physical-reseed-attempt-path root)))
                         (pg::fsync-directory root))))))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-reseed-follow-write-intent)
                         (lambda (&rest arguments)
                           (multiple-value-prog1 (apply original arguments) (setf returned t))))
                   (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-publish-intent owner record))))
                   (ok (= 1 fired))
                   (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                   (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                   (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-read owner)))))
              (setf (symbol-function 'pg::physical-reseed-follow-write-intent) original))))))))

(defun reseed-follow-intent-test-outcome (record phase)
  (let* ((copy (pg::physical-reseed-follow-copy record))
         (generation (first (getf copy :generations))))
    (setf (getf generation :phase) phase)
    (when (eq phase :proved)
      (let ((anchor (pg::physical-follow-anchor-copy
                     (append (copy-list (getf generation :coordinates))
                             (list :kind :same-timeline :history-text (getf generation :source-history))))))
        (setf (getf generation :anchor)
              (loop for (key value) on anchor by #'cddr
                    unless (eq key :history-text) append (list key value)))))
    (pg::physical-reseed-follow-copy copy)))

(deftest reseed-follow-intent-store-does-not-certify-remote-outcomes
    (dolist (phase '(:proved :canceling :canceled))
      (call-with-reseed-follow-installed-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine plan))
         (let ((record nil))
           (pg::call-with-physical-reseed-follow-owner
            root
            (lambda (owner)
              (setf record (reseed-follow-intent-test-outcome
                            (reseed-follow-intent-test-record (pg::physical-reseed-follow-initial-record owner) root)
                            phase))
              (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-publish-intent owner record))))
              (ok (not (pg::physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow.tmp" root))))))
           (let* ((path (merge-pathnames "physical-reseed.follow.tmp" root))
                  (bytes (pg::physical-reseed-follow-image record)))
             (reseed-follow-owner-test-write path bytes)
             (pg::call-with-physical-reseed-follow-owner
              root (lambda (owner)
                     (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-recover-intent owner))))))
             (ok (equalp bytes (lineage-test-bytes path)))
             (ok (not (pg::physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow" root))))))))))

(defun reseed-follow-store-run-child (root log body cache)
  (let* ((source (asdf:system-source-directory "pg"))
         (code (format nil "(let ((pg::*pgdata* #P~s)) ~a)" (namestring root) body))
         (process
           (sb-ext:run-program
            "/usr/bin/env"
            (list (format nil "XDG_CACHE_HOME=~a" (namestring cache))
                  (format nil "CL_SOURCE_REGISTRY=~a//:" (namestring source))
                  (or (uiop:getenv "SBCL") (namestring sb-ext:*runtime-pathname*))
                  "--noinform" "--no-sysinit" "--no-userinit" "--non-interactive"
                  "--eval" "(require :asdf)" "--eval" "(asdf:load-system \"pg\")" "--eval" code)
            :wait nil :output log :error :output :if-output-exists :supersede)))
    (unwind-protect
         (progn
           (sb-ext:process-wait process)
           (list (sb-ext:process-status process) (sb-ext:process-exit-code process)))
      (when (sb-ext:process-alive-p process)
        (sb-ext:process-kill process 9)
        (sb-ext:process-wait process))
      (sb-ext:process-close process))))

(deftest reseed-follow-intent-recovers-actual-sigkill-cuts-in-fresh-images
    (with-physical-store-directory (cache-root)
      (let ((cache (uiop:ensure-directory-pathname
                    (sb-posix:mkdtemp (namestring (merge-pathnames "intent-cache.XXXXXX" cache-root))))))
        (dolist (cut '(:before-follow-temporary-create :after-follow-temporary-create :after-follow-write-chunk
                       :after-follow-temporary-fsync :before-follow-rename :after-follow-rename
                       :after-follow-directory-fsync :after-follow-readback))
          (call-with-reseed-follow-installed-fixture
           (lambda (root stage incoming quarantine plan)
             (declare (ignore stage incoming plan))
             (let* ((log (merge-pathnames "intent-child.log" cache-root))
                    (paths (list (pg::physical-reseed-attempt-path root)
                                 (pg::physical-reseed-install-path root)
                                 (merge-pathnames "postmaster.pid" quarantine)
                                 (merge-pathnames "physical-recovery.control" root)))
                    (before (mapcar (lambda (path)
                                      (list path (lineage-test-bytes path)
                                            (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
                                    paths))
                    (generation nil))
               (pg::call-with-physical-reseed-follow-owner
                root (lambda (owner)
                       (setf generation (getf (reseed-follow-intent-test-record
                                               (pg::physical-reseed-follow-initial-record owner) root)
                                              :generations))))
               (let ((status
                       (reseed-follow-store-run-child
                        root log
                        (format nil
                                "(pg::call-with-physical-reseed-follow-owner #P~s
                                 (lambda (owner)
                                   (let ((record (pg::physical-reseed-follow-initial-record owner))
                                         (pg::*physical-reseed-follow-write-limit* 257)
                                         (pg::*physical-reseed-follow-test-hook*
                                           (lambda (point path)
                                             (declare (ignore path))
                                             (when (eq point ~s) (sb-posix:kill (sb-posix:getpid) 9)))))
                                     (setf (getf record :generations) '~a)
                                     (pg::physical-reseed-follow-publish-intent owner record))))"
                                (namestring root) cut (pg::physical-bootstrap-manifest-text generation))
                        cache)))
                 (ok (equal '(:signaled 9) status) (format nil "intent SIGKILL ~s ~a" cut log)))
               (let* ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                      (bytes (when (pg::physical-adoption-path-present-p temporary) (lineage-test-bytes temporary)))
                      (status
                        (reseed-follow-store-run-child
                         root log
                         (format nil
                                 "(assert (handler-case (progn (pg::physical-reseed-check-before-pid) nil) (error () t)))
                                (pg::call-with-physical-reseed-follow-owner #P~s
                                  (lambda (owner)
                                    (cond
                                      ((member ~s '(:after-follow-temporary-create :after-follow-write-chunk))
                                       (assert (handler-case (progn (pg::physical-reseed-follow-recover-intent owner) nil)
                                                 (error () t)))
                                       (assert (handler-case (progn (pg::physical-reseed-follow-read owner) nil) (error () t))))
                                      ((eq ~s :before-follow-temporary-create)
                                       (assert (null (pg::physical-reseed-follow-recover-intent owner))))
                                      (t
                                       (let ((record (pg::physical-reseed-follow-recover-intent owner)))
                                         (assert (eq :preparing (getf (first (getf record :generations)) :phase)))
                                         (assert (equal record (pg::physical-reseed-follow-read owner))))))))
                                (assert (handler-case (progn (pg::physical-reseed-check-before-pid) nil) (error () t)))"
                                 (namestring root) cut cut)
                         cache)))
                 (ok (equal '(:exited 0) status) (format nil "fresh intent recovery ~s ~a" cut log))
                 (when (member cut '(:after-follow-temporary-create :after-follow-write-chunk))
                   (let ((unchanged (equalp bytes (lineage-test-bytes temporary))))
                     (ok unchanged))))
               (let ((unchanged
                       (every (lambda (entry)
                                (and (equalp (second entry) (lineage-test-bytes (first entry)))
                                     (equal (third entry)
                                            (pg::physical-reseed-file-facts
                                             (sb-posix:lstat (namestring (first entry)))))))
                              before)))
                 (ok unchanged)))))))))

(deftest reseed-follow-intent-refuses-an-equal-but-replaced-final-before-rename
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((initial (pg::physical-reseed-follow-initial-record owner))
                 (record (reseed-follow-intent-test-record initial root))
                 (previous (pg::physical-reseed-follow-publish-intent owner initial))
                 (bytes (pg::physical-reseed-follow-image previous))
                 (replacement (merge-pathnames "replacement-follow" root))
                 (final (merge-pathnames "physical-reseed.follow" root))
                 (fired 0) (inode nil)
                 (pg::*physical-reseed-follow-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (eq point :before-follow-rename)
                       (incf fired)
                       (reseed-follow-owner-test-write replacement bytes)
                       (sb-posix:rename (namestring replacement) (namestring final))
                       (pg::fsync-directory root)
                       (setf inode (sb-posix:stat-ino (sb-posix:lstat (namestring final))))))))
            (let ((message
                    (handler-case
                        (progn (pg::physical-reseed-follow-publish-intent owner record :previous previous) nil)
                      (error (condition) (princ-to-string condition)))))
              (ok (and message (search "final generation changed" message))))
            (ok (= 1 fired))
            (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring final)))))
            (let ((unchanged (equalp bytes (lineage-test-bytes final))))
              (ok unchanged))
            (ok (pg::physical-reseed-follow-owner-uncertain-p owner))))))))

(deftest reseed-follow-intent-creates-under-the-held-root-after-path-replacement
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((record (pg::physical-reseed-follow-initial-record owner))
                 (saved (pathname (concatenate 'string (string-right-trim "/" (namestring root)) "-saved/")))
                 (original (symbol-function 'pg::physical-image-openat))
                 (fired 0) (swapped nil) (foreign nil))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-image-openat)
                         (lambda (fd name flags mode)
                           (when (and (zerop fired) (equal name "physical-reseed.follow.tmp")
                                      (logtest sb-posix:o-creat flags))
                             (incf fired)
                             (sb-posix:rename (namestring root) (namestring saved))
                             (setf swapped t)
                             (sb-posix:mkdir (namestring root) #o700)
                             (setf foreign (sb-posix:stat-ino (sb-posix:lstat (namestring root))))
                             (reseed-follow-owner-test-write (merge-pathnames ".pg_lisp.lock" root)
                                                             (make-array 0 :element-type '(unsigned-byte 8)))
                             (reseed-follow-owner-test-write (merge-pathnames "foreign-marker" root)
                                                             (pg::string->bytes "preserve foreign root")))
                           (funcall original fd name flags mode)))
                   (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-publish-intent owner record))))
                   (ok (= 1 fired))
                   (ok (equal '(".pg_lisp.lock" "foreign-marker")
                              (sort (pg::directory-entry-names root :follow-symlinks t) #'string<)))
                   (ok (pg::physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow.tmp" saved))))
              (setf (symbol-function 'pg::physical-image-openat) original)
              (when swapped
                (when (probe-file root)
                  (assert (or (null foreign) (= foreign (sb-posix:stat-ino (sb-posix:lstat (namestring root))))))
                  (uiop:delete-directory-tree root :validate t))
                (sb-posix:rename (namestring saved) (namestring root))))))))))
