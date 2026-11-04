(in-package #:pg-physical-tests)

(deftest reseed-follow-configuration-building-intent-precedes-workspace-output
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (with-physical-store-directory (workspace)
         (sb-posix:chmod (namestring workspace) #o700)
         (let ((core nil) (result nil) (before (pg::directory-entry-names root :follow-symlinks t)))
           (pg::call-with-physical-reseed-follow-owner
            root (lambda (owner)
                   (setf core (reseed-follow-proof-test-record
                               (pg::physical-reseed-follow-initial-record owner) root :same-timeline))))
           (reseed-follow-owner-test-write
            (merge-pathnames "physical-reseed.follow" root) (pg::physical-reseed-follow-image core))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (setf result (pg::physical-reseed-follow-begin-configuration owner workspace 12345))
              (ok (eq :building (getf (getf result :configuration) :phase)))
              (ok (equal core (pg::physical-reseed-follow-core-projection result)))
              (ok (equal result (pg::physical-reseed-follow-read owner)))
              (ok (null (pg::directory-entry-names workspace :follow-symlinks t)))
              (let ((same (equal result (pg::physical-reseed-follow-begin-configuration owner workspace 12345))))
                (ok same))
              (ok (intake-fails-p
                   (lambda () (pg::physical-reseed-follow-begin-configuration owner workspace 12346))))))
           (let ((names (pg::directory-entry-names root :follow-symlinks t)))
             (ok (equal (sort (cons "physical-reseed.follow" before) #'string<) (sort names #'string<))))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root (lambda (owner)
                   (let ((same (equal result (pg::physical-reseed-follow-read owner)))) (ok same)))))
         (ok (null (pg::directory-entry-names workspace :follow-symlinks t)))))))

(deftest reseed-follow-configuration-building-rejects-protected-and-nonempty-workspaces
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore plan))
       (with-physical-store-directory (workspace)
         (sb-posix:chmod (namestring workspace) #o700)
         (let ((core nil))
           (pg::call-with-physical-reseed-follow-owner
            root (lambda (owner)
                   (setf core (reseed-follow-proof-test-record
                               (pg::physical-reseed-follow-initial-record owner) root :same-timeline))))
           (reseed-follow-owner-test-write
            (merge-pathnames "physical-reseed.follow" root) (pg::physical-reseed-follow-image core))
           (reseed-follow-owner-test-write (merge-pathnames "unrelated" workspace) #(1 2 3))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root (lambda (owner)
                   (dolist (directory (list root stage incoming quarantine workspace))
                     (ok (intake-fails-p
                          (lambda () (pg::physical-reseed-follow-begin-configuration owner directory 12345))))
                     (let ((same (equal core (pg::physical-reseed-follow-read owner)))) (ok same))
                     (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner))))
                   (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))
           (ok (equalp #(1 2 3) (lineage-test-bytes (merge-pathnames "unrelated" workspace))))
           (ok (equal '("unrelated") (pg::directory-entry-names workspace :follow-symlinks t))))))))

(deftest reseed-follow-configuration-building-recovers-complete-publication-cuts
    (dolist (cut '(:before-follow-temporary-fsync :after-follow-temporary-fsync
                   :before-follow-temporary-readback :after-follow-temporary-readback
                   :before-follow-rename :after-follow-rename :after-follow-directory-fsync
                   :before-follow-readback :after-follow-readback))
      (call-with-reseed-follow-installed-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine plan))
         (with-physical-store-directory (workspace)
           (sb-posix:chmod (namestring workspace) #o700)
           (let ((core nil) (fired 0))
             (pg::call-with-physical-reseed-follow-owner
              root (lambda (owner)
                     (setf core (reseed-follow-proof-test-record
                                 (pg::physical-reseed-follow-initial-record owner) root :same-timeline))))
             (reseed-follow-owner-test-write
              (merge-pathnames "physical-reseed.follow" root) (pg::physical-reseed-follow-image core))
             (pg::call-with-physical-reseed-follow-configuration-owner
              root
              (lambda (owner)
                (let ((pg::*physical-reseed-follow-test-hook*
                        (lambda (point path)
                          (declare (ignore path))
                          (when (and (eq point cut) (zerop fired))
                            (incf fired)
                            (error "configuration publication cut")))))
                  (ok (intake-fails-p
                       (lambda () (pg::physical-reseed-follow-begin-configuration owner workspace 12345)))))
                (ok (= 1 fired))
                (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-read owner))))))
             (ok (null (pg::directory-entry-names workspace :follow-symlinks t)))
             (pg::call-with-physical-reseed-follow-configuration-owner
              root
              (lambda (owner)
                (let ((record (pg::physical-reseed-follow-recover-configuration owner)))
                  (ok (eq :building (getf (getf record :configuration) :phase)))
                  (let ((same (equal record (pg::physical-reseed-follow-read owner)))) (ok same))
                  (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))))
             (ok (null (pg::directory-entry-names workspace :follow-symlinks t)))))))))

(deftest reseed-follow-configuration-building-keeps-partial-temporaries-fenced
    (dolist (cut '(:after-follow-temporary-create :after-follow-write-chunk))
      (call-with-reseed-follow-installed-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine plan))
         (with-physical-store-directory (workspace)
           (sb-posix:chmod (namestring workspace) #o700)
           (let ((core nil) (fired 0))
             (pg::call-with-physical-reseed-follow-owner
              root (lambda (owner)
                     (setf core (reseed-follow-proof-test-record
                                 (pg::physical-reseed-follow-initial-record owner) root :same-timeline))))
             (reseed-follow-owner-test-write
              (merge-pathnames "physical-reseed.follow" root) (pg::physical-reseed-follow-image core))
             (pg::call-with-physical-reseed-follow-configuration-owner
              root (lambda (owner)
                     (let ((pg::*physical-reseed-follow-write-limit* 257)
                           (pg::*physical-reseed-follow-test-hook*
                             (lambda (point path)
                               (declare (ignore path))
                               (when (and (eq point cut) (zerop fired))
                                 (incf fired) (error "partial configuration cut")))))
                       (ok (intake-fails-p
                            (lambda () (pg::physical-reseed-follow-begin-configuration owner workspace 12345)))))
                     (ok (= 1 fired))))
             (let* ((path (merge-pathnames "physical-reseed.follow.tmp" root))
                    (bytes (lineage-test-bytes path))
                    (facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
               (pg::call-with-physical-reseed-follow-configuration-owner
                root (lambda (owner)
                       (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-recover-configuration owner))))
                       (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))
               (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
               (ok (equalp bytes (lineage-test-bytes path)))
               (let ((same (equal core (pg::physical-reseed-follow-decode
                                        (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))
                 (ok same)))
             (ok (null (pg::directory-entry-names workspace :follow-symlinks t)))))))))

(deftest reseed-follow-configuration-building-refuses-an-aborted-attempt
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (with-physical-store-directory (workspace)
         (sb-posix:chmod (namestring workspace) #o700)
         (let ((core nil))
           (pg::call-with-physical-reseed-follow-owner
            root (lambda (owner)
                   (setf core (reseed-follow-proof-test-record
                               (pg::physical-reseed-follow-initial-record owner) root :same-timeline))))
           (reseed-follow-owner-test-write
            (merge-pathnames "physical-reseed.follow" root) (pg::physical-reseed-follow-image core))
           (reseed-follow-prepare-test-abort root core)
           (pg::call-with-physical-reseed-follow-configuration-owner
            root (lambda (owner)
                   (let ((same (equal core (pg::physical-reseed-follow-read owner)))) (ok same))
                   (ok (intake-fails-p
                        (lambda () (pg::physical-reseed-follow-begin-configuration owner workspace 12345))))
                   (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                   (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))
           (ok (null (pg::directory-entry-names workspace :follow-symlinks t))))))))

(deftest reseed-follow-configuration-building-rejects-a-changed-named-mount
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (with-physical-store-directory (workspace)
         (sb-posix:chmod (namestring workspace) #o700)
         (let ((core nil) (armed nil) (observed 0)
                          (original (symbol-function 'pg::physical-adoption-install-mount-id)))
           (pg::call-with-physical-reseed-follow-owner
            root (lambda (owner)
                   (setf core (reseed-follow-proof-test-record
                               (pg::physical-reseed-follow-initial-record owner) root :same-timeline))))
           (reseed-follow-owner-test-write
            (merge-pathnames "physical-reseed.follow" root) (pg::physical-reseed-follow-image core))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-adoption-install-mount-id)
                        (lambda (path &key directory-fd)
                          (let ((actual (funcall original path :directory-fd directory-fd)))
                            (if (and armed (null directory-fd) path (equal (pathname path) workspace))
                                (progn (incf observed) (1+ actual))
                                actual))))
                  (pg::call-with-physical-reseed-follow-configuration-owner
                   root
                   (lambda (owner)
                     (let ((pg::*physical-reseed-follow-test-hook*
                             (lambda (point path)
                               (declare (ignore path))
                               (when (eq point :before-follow-temporary-create) (setf armed t)))))
                       (ok (intake-fails-p
                            (lambda () (pg::physical-reseed-follow-begin-configuration owner workspace 12345)))))
                     (setf armed nil)
                     (ok (plusp observed))
                     (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                     (let ((same (equal core (pg::physical-reseed-follow-read owner)))) (ok same))
                     (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))))
             (setf (symbol-function 'pg::physical-adoption-install-mount-id) original))
           (ok (null (pg::directory-entry-names workspace :follow-symlinks t))))))))
