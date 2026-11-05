(in-package #:pg-physical-tests)

(defvar *reseed-follow-fixture-owner-exit* nil)

(defun call-with-reseed-follow-building-fixture (kind function &optional after)
  (call-with-reseed-follow-installed-fixture
   (lambda (root stage incoming quarantine plan)
     (declare (ignore stage incoming quarantine plan))
     (with-physical-store-directory (workspace)
       (sb-posix:chmod (namestring workspace) #o700)
       (let ((core nil) (*reseed-follow-fixture-owner-exit* (gensym "RESEED-OWNER-EXIT")))
         (pg::call-with-physical-reseed-follow-owner
          root (lambda (owner)
                 (setf core (reseed-follow-proof-test-record
                             (pg::physical-reseed-follow-initial-record owner) root kind))))
         (reseed-follow-owner-test-write
          (merge-pathnames "physical-reseed.follow" root) (pg::physical-reseed-follow-image core))
         ;; Fault tests use this tag for an intentional nonlocal exit.  Normal
         ;; callbacks still run the owner's final proof.
         (catch *reseed-follow-fixture-owner-exit*
           (pg::call-with-physical-reseed-follow-configuration-owner
            root (lambda (owner)
                   (let ((record (pg::physical-reseed-follow-begin-configuration owner workspace 12345)))
                     (funcall function owner root workspace record)))))
         (when after (funcall after root workspace)))))))

(deftest reseed-follow-configuration-stages-exact-inert-files-and-keeps-the-installed-base
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-follow-building-fixture
       kind
       (lambda (owner root workspace building)
         (let* ((paths (mapcar (lambda (name) (merge-pathnames name root))
                               '("physical-recovery.control" "global/pg_wal_control" "global/pg_control")))
                (before (mapcar #'lineage-test-bytes paths))
                (names (sort (pg::directory-entry-names root :follow-symlinks t) #'string<))
                (staged (pg::physical-reseed-follow-stage-configuration owner))
                (configuration (getf staged :configuration))
                (pending (pg::physical-follow-decode-config
                          (lineage-test-bytes (merge-pathnames "staged/follow.pending" workspace)))))
           (ok (eq :staged (getf configuration :phase)))
           (ok (zerop (getf configuration :step)))
           (ok (not (getf (sixth pending) :enabled)))
           (ok (eq :pending (getf (fifth pending) :phase)))
           (ok (string= (fourth pending) (getf (first (getf building :generations)) :name)))
           (ok (= (length (getf configuration :images)) (length (getf configuration :entries))))
           (ok (= (if (member kind '(:at-fork :later-writer)) 8 4)
                  (length (getf configuration :entries))))
           (ok (equalp before (mapcar #'lineage-test-bytes paths)))
           (ok (equal names (sort (pg::directory-entry-names root :follow-symlinks t) #'string<)))
           (let ((same (equal staged (pg::physical-reseed-follow-stage-configuration owner)))) (ok same))
           (let ((same (equal staged (pg::physical-reseed-follow-read owner)))) (ok same))
           (ok (not (probe-file (merge-pathnames "physical-follow.control" root))))
           (ok (not (probe-file (merge-pathnames "pg_physical_receive/" root)))))))))

(deftest reseed-follow-configuration-staging-recovers-complete-journal-cuts
    (dolist (cut '(:before-follow-temporary-fsync :before-follow-rename :after-follow-rename))
      (let ((fired 0))
        (call-with-reseed-follow-building-fixture
         :same-timeline
         (lambda (owner root workspace building)
           (declare (ignore root workspace building))
           (let ((pg::*physical-reseed-follow-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (and (eq point cut) (zerop fired))
                       (incf fired) (error "staged journal publication cut")))))
             (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-stage-configuration owner)))))
           (ok (= 1 fired))
           (ok (pg::physical-reseed-follow-owner-uncertain-p owner)))
         (lambda (root workspace)
           (declare (ignore workspace))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (let ((record (pg::physical-reseed-follow-recover-configuration owner)))
                (ok (eq :staged (getf (getf record :configuration) :phase)))
                (let ((same (equal record (pg::physical-reseed-follow-read owner)))) (ok same))
                (let ((same (equal record (pg::physical-reseed-follow-stage-configuration owner)))) (ok same))
                (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))))))))

(deftest reseed-follow-configuration-staging-resumes-recognized-workspace-prefixes
    (dolist (cut '((:after-configuration-guard-sync 1) (:after-configuration-guard-sync 2)
                   (:after-configuration-directory-create 1) (:after-configuration-directory-create 2)
                   (:after-configuration-directory-create 3) (:after-configuration-file-fsync 1)))
      (let ((seen 0) (fired nil))
        (call-with-reseed-follow-building-fixture
         :same-timeline
         (lambda (owner root workspace building)
           (declare (ignore root workspace building))
           (let ((pg::*physical-reseed-follow-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (and (eq point (first cut)) (= (incf seen) (second cut)) (not fired))
                       (setf fired t) (error "configuration workspace prefix cut")))))
             (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-stage-configuration owner)))))
           (ok fired)
           (ok (pg::physical-reseed-follow-owner-uncertain-p owner)))
         (lambda (root workspace)
           (declare (ignore workspace))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (let ((record (pg::physical-reseed-follow-recover-configuration owner)))
                (ok (eq :building (getf (getf record :configuration) :phase))))
              (let ((record (pg::physical-reseed-follow-stage-configuration owner)))
                (ok (eq :staged (getf (getf record :configuration) :phase))))
              (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))))))))

(deftest reseed-follow-configuration-staging-preserves-partial-image-files
    (dolist (cut '(:after-configuration-file-create :after-configuration-write-chunk))
      (let ((fired nil))
        (call-with-reseed-follow-building-fixture
         :same-timeline
         (lambda (owner root workspace building)
           (declare (ignore root workspace building))
           (let ((pg::*physical-reseed-follow-write-limit* 257)
                 (pg::*physical-reseed-follow-test-hook*
                   (lambda (point path)
                     (declare (ignore path))
                     (when (and (eq point cut) (not fired))
                       (setf fired t) (error "partial configuration image cut")))))
             (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-stage-configuration owner)))))
           (ok fired))
         (lambda (root workspace)
           (let* ((path (merge-pathnames "staged/follow.pending" workspace))
                  (bytes (lineage-test-bytes path))
                  (facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
             (pg::call-with-physical-reseed-follow-configuration-owner
              root
              (lambda (owner)
                (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-recover-configuration owner))))
                (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-stage-configuration owner))))
                (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))
             (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
             (ok (equalp bytes (lineage-test-bytes path)))
             (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))))))

(deftest reseed-follow-configuration-staging-rejects-unexplained-workspace-entries
    (call-with-reseed-follow-building-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (declare (ignore root))
       (reseed-follow-owner-test-write (merge-pathnames "unrelated" workspace) #(1 2 3))
       (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-stage-configuration owner))))
       (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
       (ok (equal '("unrelated") (pg::directory-entry-names workspace :follow-symlinks t)))
       (ok (equalp #(1 2 3) (lineage-test-bytes (merge-pathnames "unrelated" workspace))))
       (let ((same (equal building (pg::physical-reseed-follow-read owner)))) (ok same)))))

(deftest reseed-follow-configuration-staged-evidence-refuses-changed-or-missing-files
    (dolist (damage '(:image :guard :extra))
      (call-with-reseed-follow-building-fixture
       :same-timeline
       (lambda (owner root workspace building)
         (declare (ignore building))
         (let* ((record (pg::physical-reseed-follow-stage-configuration owner))
                (journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
                (path (merge-pathnames "staged/follow.pending" workspace)))
           (ecase damage
             (:image
              (with-open-file (stream path :direction :io :if-exists :overwrite :element-type '(unsigned-byte 8))
                (let ((byte (read-byte stream)))
                  (file-position stream 0)
                  (write-byte (logxor byte 255) stream))
                (finish-output stream)))
             (:guard (delete-file (merge-pathnames "physical-reseed.install" workspace)))
             (:extra (reseed-follow-owner-test-write (merge-pathnames "unrelated" workspace) #(7 8 9))))
           (let ((bytes (lineage-test-bytes path))
                 (facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
             (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-stage-configuration owner))))
             (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-recover-configuration owner))))
             (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
             (ok (equalp bytes (lineage-test-bytes path)))
             (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
             (ok (equalp journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
           (let ((same (equal record (pg::physical-reseed-follow-read owner)))) (ok same))
           (when (eq damage :guard)
             (ok (not (probe-file (merge-pathnames "physical-reseed.install" workspace)))))
           (when (eq damage :extra)
             (ok (equalp #(7 8 9) (lineage-test-bytes (merge-pathnames "unrelated" workspace))))))))))

(deftest reseed-follow-configuration-staging-refuses-a-prior-journal-temporary-before-workspace-mutation
    (call-with-reseed-follow-building-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (let* ((path (merge-pathnames "physical-reseed.follow.tmp" root))
              (bytes (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(1 2))))
         (reseed-follow-owner-test-write path bytes)
         (let ((facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
           (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-stage-configuration owner))))
           (ok (null (pg::directory-entry-names workspace :follow-symlinks t)))
           (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
           (ok (equal facts (pg::physical-reseed-file-facts (sb-posix:lstat (namestring path)))))
           (ok (equalp bytes (lineage-test-bytes path)))
           (let ((same (equal building (pg::physical-reseed-follow-decode
                                        (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))
             (ok same)))))))

(deftest reseed-follow-configuration-file-readback-rechecks-the-named-mount
    (with-physical-store-directory (directory)
      (sb-posix:chmod (namestring directory) #o700)
      (let ((fd nil) (observed 0)
                     (bytes (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)))
                     (original (symbol-function 'pg::physical-adoption-install-mount-id)))
        (reseed-follow-owner-test-write (merge-pathnames "payload" directory) bytes)
        (unwind-protect
             (progn
               (setf fd (pg::physical-backup-open-directory directory))
               (let ((path (merge-pathnames "payload" (pg::physical-image-fd-path fd))))
                 (setf (symbol-function 'pg::physical-adoption-install-mount-id)
                       (lambda (where &key directory-fd)
                         (let ((actual (funcall original where :directory-fd directory-fd)))
                           (if (and (null directory-fd) where (equal (pathname where) path)
                                    (>= (incf observed) 2))
                               (1+ actual)
                               actual))))
                 (ok (intake-fails-p
                      (lambda () (pg::physical-reseed-follow-configuration-file-row fd "payload" "payload" bytes))))
                 (ok (>= observed 2))))
          (setf (symbol-function 'pg::physical-adoption-install-mount-id) original)
          (when fd (sb-posix:close fd))))))

(deftest reseed-follow-configuration-guard-readback-rechecks-the-named-mount
    (with-physical-store-directory (directory)
      (sb-posix:chmod (namestring directory) #o700)
      (let ((fd nil) (observed 0)
                     (original (symbol-function 'pg::physical-adoption-install-mount-id))
                     (name "physical-reseed.install"))
        (reseed-follow-owner-test-write (merge-pathnames name directory) (pg::physical-reseed-install-guard-bytes name))
        (unwind-protect
             (progn
               (setf fd (pg::physical-backup-open-directory directory))
               (let ((path (merge-pathnames name (pg::physical-image-fd-path fd))))
                 (setf (symbol-function 'pg::physical-adoption-install-mount-id)
                       (lambda (where &key directory-fd)
                         (let ((actual (funcall original where :directory-fd directory-fd)))
                           (if (and (null directory-fd) where (equal (pathname where) path)
                                    (>= (incf observed) 2))
                               (1+ actual)
                               actual))))
                 (pg::call-with-physical-image-input-at
                  fd name
                  (lambda (stream)
                    (ok (intake-fails-p
                         (lambda () (pg::physical-reseed-follow-configuration-guard-facts fd stream name)))))
                  :element-type '(unsigned-byte 8))
                 (ok (>= observed 2))))
          (setf (symbol-function 'pg::physical-adoption-install-mount-id) original)
          (when fd (sb-posix:close fd))))))

(deftest reseed-follow-configuration-staged-retry-never-recreates-a-vanished-file
    (call-with-reseed-follow-building-fixture
     :same-timeline
     (lambda (owner root workspace building)
       (declare (ignore building))
       (let* ((record (pg::physical-reseed-follow-stage-configuration owner))
              (journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
              (path (merge-pathnames "staged/recovery.initial" workspace))
              (fired 0)
              (original (symbol-function 'pg::physical-reseed-follow-configuration-staged-record)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-reseed-follow-configuration-staged-record)
                      (lambda (&rest arguments)
                        (multiple-value-prog1 (apply original arguments)
                          (when (zerop fired)
                            (incf fired)
                            (delete-file path)))))
                (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-stage-configuration owner)))))
           (setf (symbol-function 'pg::physical-reseed-follow-configuration-staged-record) original))
         (ok (= 1 fired))
         (ok (not (probe-file path)))
         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
         (ok (equalp journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (let ((same (equal record (pg::physical-reseed-follow-read owner)))) (ok same))))))
