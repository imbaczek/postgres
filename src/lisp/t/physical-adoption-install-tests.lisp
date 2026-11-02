(in-package #:pg-physical-tests)

(deftest adoption-install-journal-presence-fences-startup-without-an-attempt
    (dolist (kind '(:empty :corrupt :directory :symlink :fifo))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root)
               (path (merge-pathnames "physical-adoption.install" root)))
          (ecase kind
            (:empty (pg::wal-atomic-write-bytes path (empty-octets)))
            (:corrupt (pg::wal-atomic-write-bytes path (pg::string->bytes "unknown journal")))
            (:directory (ensure-directories-exist (merge-pathnames "physical-adoption.install/child" root)))
            (:symlink (sb-posix:symlink "missing-journal" (namestring path)))
            (:fifo (sb-posix:mkfifo (namestring path) #o600)))
          (ok (not (probe-file (pg::physical-adoption-attempt-path root))))
          (ok (intake-fails-p #'pg::physical-adoption-check-startup))))))

(deftest adoption-install-authority-excludes-proof-cancellation-and-attempt-publication
    (call-with-adoption-proof-driver-fixture
     (lambda (root state stage client profile record counts)
       (let ((owner (getf state :owner)))
         (pg::physical-adoption-attempt-publish owner record)
         (pg::wal-atomic-write-bytes (merge-pathnames "physical-adoption.install" root)
                                     (pg::string->bytes "installation authority"))
         (ok (intake-fails-p
              (lambda () (pg::physical-adoption-attempt-resume-proof owner :client client))))
         (ok (intake-fails-p
              (lambda () (pg::physical-adoption-attempt-start-proof state stage client profile))))
         (ok (intake-fails-p
              (lambda () (pg::physical-adoption-attempt-cancel owner :client client))))
         (ok (intake-fails-p
              (lambda () (pg::physical-adoption-attempt-publish owner record :previous record))))
         (ok (equal record (pg::physical-adoption-attempt-read owner)))
         (ok (equal '(0 0) (funcall counts)))
         (delete-file (pg::physical-adoption-attempt-path root))
         (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-cancel owner))))
         (ok (intake-fails-p #'pg::physical-adoption-check-startup))))))

(deftest adoption-install-authority-is-rechecked-before-remote-abort
    (call-with-adoption-cancel-fixture
     (lambda (root state stage client record calls)
       (declare (ignore stage record))
       (let ((*adoption-context-observation-hook*
               (lambda (point source image)
                 (declare (ignore source image))
                 (when (eq point :identity)
                   (pg::wal-atomic-write-bytes (merge-pathnames "physical-adoption.install" root)
                                               (pg::string->bytes "installation began"))))))
         (ok (intake-fails-p
              (lambda () (pg::physical-adoption-attempt-cancel (getf state :owner) :client client))))
         (ok (zerop (funcall calls)))
         (ok (probe-file (pg::physical-adoption-attempt-path root)))))))

(deftest adoption-install-authority-is-rechecked-before-prepare
    (call-with-adoption-proof-driver-fixture
     (lambda (root state stage client profile record counts)
       (declare (ignore stage profile))
       (let ((owner (getf state :owner))
             (*adoption-context-observation-hook*
               (lambda (point source image)
                 (declare (ignore source image))
                 (when (eq point :history)
                   (pg::wal-atomic-write-bytes (merge-pathnames "physical-adoption.install" root)
                                               (pg::string->bytes "installation began"))))))
         (pg::physical-adoption-attempt-publish owner record)
         (ok (intake-fails-p
              (lambda () (pg::physical-adoption-attempt-resume-proof owner :client client))))
         (ok (equal '(0 0) (funcall counts)))
         (ok (equal record (pg::physical-adoption-attempt-read owner)))))))

(deftest adoption-install-rename-never-replaces-an-existing-object
    (dolist (kind '(:file :directory))
      (with-physical-store-directory (root)
        (let ((from (merge-pathnames "from" root))
              (to (merge-pathnames "to" root)))
          (ecase kind
            (:file
             (pg::wal-atomic-write-bytes from (pg::string->bytes "original"))
             (pg::wal-atomic-write-bytes to (pg::string->bytes "occupant")))
            (:directory
             (ensure-directories-exist (merge-pathnames "from/child" root))
             (ensure-directories-exist (merge-pathnames "to/child" root))))
          (let ((original (sb-posix:stat-ino (sb-posix:lstat (namestring from))))
                (occupant (sb-posix:stat-ino (sb-posix:lstat (namestring to)))))
            (ok (intake-fails-p (lambda () (pg::physical-adoption-install-rename from to))))
            (ok (= original (sb-posix:stat-ino (sb-posix:lstat (namestring from)))))
            (ok (= occupant (sb-posix:stat-ino (sb-posix:lstat (namestring to)))))
            (if (eq kind :file)
                (delete-file to)
                (uiop:delete-directory-tree (uiop:ensure-directory-pathname to) :validate t))
            (pg::physical-adoption-install-rename from to)
            (ok (not (probe-file from)))
            (ok (= original (sb-posix:stat-ino (sb-posix:lstat (namestring to))))))))))

(deftest adoption-install-tree-keeps-object-identity-across-parent-directory-renames
    (with-physical-store-directory (root)
      (with-physical-store-directory (target)
        (sb-posix:chmod (namestring root) #o700)
        (sb-posix:chmod (namestring target) #o700)
        (ensure-directories-exist (merge-pathnames "base/child" root))
        (pg::wal-atomic-write-bytes (merge-pathnames "base/child" root) (pg::string->bytes "original bytes"))
        (let ((before (pg::physical-adoption-install-snapshot root)))
          (pg::physical-adoption-install-rename (merge-pathnames "base/" root) (merge-pathnames "base/" target))
          (ok (equal before (pg::physical-adoption-install-snapshot target)))
          (ok (null (pg::physical-adoption-install-entry-names root)))))))

(deftest adoption-install-tree-refuses-links-special-files-and-cross-mount-entries
    (dolist (kind '(:symlink :hardlink :fifo :mount))
      (with-physical-store-directory (root)
        (sb-posix:chmod (namestring root) #o700)
        (let ((path (merge-pathnames "entry" root))
              (original (symbol-function 'pg::physical-adoption-install-mount-id)))
          (unwind-protect
               (progn
                 (ecase kind
                   (:symlink (sb-posix:symlink "missing" (namestring path)))
                   (:hardlink
                    (pg::wal-atomic-write-bytes (merge-pathnames "original" root) (empty-octets))
                    (sb-posix:link (namestring (merge-pathnames "original" root)) (namestring path)))
                   (:fifo (sb-posix:mkfifo (namestring path) #o600))
                   (:mount
                    (pg::wal-atomic-write-bytes path (empty-octets))
                    (setf (symbol-function 'pg::physical-adoption-install-mount-id)
                          (lambda (actual)
                            (+ (funcall original actual) (if (equal actual path) 1 0))))))
                 (ok (intake-fails-p (lambda () (pg::physical-adoption-install-snapshot root)))))
            (setf (symbol-function 'pg::physical-adoption-install-mount-id) original))))))

(deftest adoption-install-rejects-nonliteral-and-escaping-relative-paths
    (dolist (name (list "" ".." "." "../file" "base/../global" "base//file"
                        "/absolute" "base/" "base/*" (string (code-char 0))))
      (ok (not (pg::physical-adoption-install-relative-p name)))))

(deftest adoption-install-phases-preserve-the-proved-operation-and-never-abort
    (let* ((proved (pg::physical-adoption-attempt-copy (adoption-attempt-codec-fixture :phase :proved)))
           (installing (copy-tree proved))
           (installed (copy-tree proved))
           (aborting (copy-tree proved)))
      (setf (getf installing :phase) :installing
            (getf installed :phase) :installed
            (getf aborting :phase) :aborting)
      (dolist (record (list installing installed))
        (ok (equal record (pg::physical-adoption-attempt-decode (pg::physical-adoption-attempt-image record)))))
      (ok (pg::physical-adoption-attempt-check-transition proved installing))
      (ok (pg::physical-adoption-attempt-check-transition installing installed))
      (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-check-transition installing aborting))))
      (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-check-transition installed proved))))
      (setf (getf installing :anchor) nil (getf installing :proof) nil)
      (ok (intake-fails-p (lambda () (pg::physical-adoption-attempt-copy installing))))))

(deftest adoption-install-phases-require-journal-authority-even-without-a-journal-file
    (call-with-adoption-proof-driver-fixture
     (lambda (root state stage client profile record counts)
       (declare (ignore root record counts))
       (let* ((owner (getf state :owner))
              (proved (pg::physical-adoption-attempt-start-proof state stage client profile))
              (installing (copy-tree proved)))
         (setf (getf installing :phase) :installing)
         (ok (intake-fails-p
              (lambda () (pg::physical-adoption-attempt-publish owner installing :previous proved))))
         (ok (equal proved (pg::physical-adoption-attempt-read owner)))
         (pg::wal-atomic-write-bytes
          (pg::physical-adoption-attempt-path (pg::physical-adoption-owner-directory owner))
          (pg::physical-adoption-attempt-image installing))
         (sb-posix:chmod (namestring (pg::physical-adoption-attempt-path (pg::physical-adoption-owner-directory owner))) #o600)
         (ok (intake-fails-p
              (lambda () (pg::physical-adoption-attempt-cancel owner :client client))))
         (ok (intake-fails-p
              (lambda () (pg::physical-adoption-attempt-resume-proof owner :client client))))
         (ok (equal installing (pg::physical-adoption-attempt-read owner)))
         (ok (intake-fails-p #'pg::physical-adoption-check-startup))))))

(deftest adoption-install-clones-a-fenced-image-with-independent-inodes
    (call-with-adoption-proof-driver-fixture
     (lambda (root state stage client profile record counts)
       (declare (ignore root state client profile counts))
       (with-physical-store-directory (target)
         (sb-posix:chmod (namestring target) #o700)
         (let* ((source (getf stage :directory))
                (before (pg::physical-adoption-install-snapshot source))
                (aux (pg::physical-adoption-install-prepare-auxiliary target))
                (checks 0)
                (pg::*physical-adoption-install-test-hook*
                  (lambda (point path)
                    (declare (ignore path))
                    (when (eq point :after-copy-file)
                      (incf checks)
                      (let ((pg::*pgdata* target))
                        (assert (intake-fails-p #'pg::physical-adoption-check-startup)))))))
           (let ((copied (pg::physical-adoption-install-clone source target before)))
             (ok (equal before (pg::physical-adoption-install-snapshot source)))
             (ok (equal aux (pg::physical-adoption-install-auxiliary-fact target)))
             (ok (plusp checks))
             (ok (equal (pg::physical-adoption-install-tree-content before)
                        (pg::physical-adoption-install-tree-content copied)))
             (ok (null (intersection
                        (loop for entry in before append (mapcar #'fifth (second entry)))
                        (loop for entry in copied append (mapcar #'fifth (second entry))))))
             (ok (pg::physical-adoption-install-check-image target record :auxiliary t))
             (with-physical-store-directory (second-target)
               (sb-posix:chmod (namestring second-target) #o700)
               (pg::physical-adoption-install-prepare-auxiliary second-target)
               (ok (pg::physical-adoption-install-clone source second-target before)))))))))

(deftest adoption-install-short-or-corrupt-copy-leaves-its-guard-and-stage-intact
    (call-with-adoption-proof-driver-fixture
     (lambda (root state stage client profile record counts)
       (declare (ignore root state client profile record counts))
       (with-physical-store-directory (target)
         (sb-posix:chmod (namestring target) #o700)
         (pg::physical-adoption-install-prepare-auxiliary target)
         (let* ((source (getf stage :directory))
                (before (pg::physical-adoption-install-snapshot source))
                (changed nil)
                (pg::*physical-adoption-install-test-hook*
                  (lambda (point path)
                    (when (and (not changed) (eq point :after-copy-file))
                      (setf changed t)
                      (pg::wal-atomic-write-bytes path (pg::string->bytes "short copy"))))))
           (ok (intake-fails-p (lambda () (pg::physical-adoption-install-clone source target before))))
           (ok changed)
           (ok (equal before (pg::physical-adoption-install-snapshot source)))
           (let ((pg::*pgdata* target))
             (ok (intake-fails-p #'pg::physical-adoption-check-startup))))))))

(deftest adoption-install-snapshot-rechecks-root-and-forbids-hidden-exclusions
    (with-physical-store-directory (root)
      (sb-posix:chmod (namestring root) #o700)
      (pg::wal-atomic-write-bytes (merge-pathnames "original" root) (empty-octets))
      (ok (intake-fails-p (lambda () (pg::physical-adoption-install-snapshot root :exclude '("original")))))
      (let ((pg::*physical-adoption-install-test-hook*
              (lambda (point actual)
                (when (eq point :before-snapshot-recheck)
                  (pg::wal-atomic-write-bytes (merge-pathnames "late-entry" actual) (empty-octets))))))
        (ok (intake-fails-p (lambda () (pg::physical-adoption-install-snapshot root)))))))

(deftest adoption-install-fence-is-rechecked-after-the-last-preparation-scan
    (call-with-adoption-proof-driver-fixture
     (lambda (root state stage client profile record counts)
       (declare (ignore stage profile))
       (let ((owner (getf state :owner))
             (original (symbol-function 'pg::physical-adoption-attempt-validate-context))
             (scans 0))
         (pg::physical-adoption-attempt-publish owner record)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-adoption-attempt-validate-context)
                      (lambda (actual saved)
                        (multiple-value-prog1 (funcall original actual saved)
                          (when (= 2 (incf scans))
                            (pg::wal-atomic-write-bytes (merge-pathnames "physical-adoption.install" root)
                                                        (pg::string->bytes "installation began"))))))
                (ok (intake-fails-p
                     (lambda () (pg::physical-adoption-attempt-resume-proof owner :client client))))
                (ok (equal '(0 0) (funcall counts))))
           (setf (symbol-function 'pg::physical-adoption-attempt-validate-context) original))))))
