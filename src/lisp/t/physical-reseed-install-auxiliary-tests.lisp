(in-package #:pg-physical-tests)

(defun call-with-reseed-auxiliary-fixture (function)
  (call-with-reseed-inventory-fixture
   (lambda (root owner record)
     (declare (ignore record))
     (with-physical-store-directory (auxiliary)
       (sb-posix:chmod (namestring auxiliary) #o700)
       (funcall function root owner auxiliary)))))

(deftest reseed-install-auxiliary-holds-startup-and-directory-locks-and-reopens
    (call-with-reseed-auxiliary-fixture
     (lambda (root owner auxiliary)
       (declare (ignore root))
       (let ((saved nil) (escaped nil) (borrowed nil))
         (pg::call-with-physical-reseed-install-auxiliary
          owner auxiliary
          (lambda (directory facts fd check)
            (setf saved (pg::physical-reseed-install-auxiliary-copy
                         facts (pg::physical-reseed-attempt-local-evidence owner) (make-hash-table :test #'equal))
                  escaped check borrowed fd)
            (ok (funcall check))
            (ok (equal (namestring auxiliary) (getf facts :directory)))
            (ok (= (sb-posix:stat-ino (sb-posix:fstat fd)) (getf facts :inode)))
            (setf (getf facts :lock-inode) 0 (char (getf facts :directory) 1) #\X)
            (let ((observed (funcall check)))
              (setf (getf observed :lock-inode) 0 (char (getf observed :directory) 1) #\X))
            (ok (equal saved (funcall check)))
            (dolist (path (list directory (merge-pathnames ".pg_lisp.lock" directory)))
              (let ((other (if (equal path directory)
                               (pg::physical-backup-open-directory path)
                               (sb-posix:open (namestring path) sb-posix:o-rdonly 0))))
                (unwind-protect (ok (not (zerop (pg::physical-store-flock other 6))))
                  (sb-posix:close other))))
            (ok (sb-thread:join-thread
                 (sb-thread:make-thread (lambda () (intake-fails-p check))))))
          :prepare t :empty t)
         (ok (intake-fails-p (lambda () (sb-posix:fstat borrowed))))
         (ok (intake-fails-p escaped))
         (ok (equal '(".pg_lisp.lock" "physical-reseed.install")
                    (sort (pg::directory-entry-names auxiliary) #'string<)))
         (pg::call-with-physical-reseed-install-auxiliary
          owner auxiliary
          (lambda (directory facts fd check)
            (declare (ignore directory fd))
            (ok (equal saved facts))
            (ok (funcall check)))
          :expected saved)
         (let ((pg::*pgdata* auxiliary))
           (ok (intake-fails-p #'pg::physical-reseed-check-before-pid)))))))

(deftest reseed-install-auxiliary-revokes-checks-and-retains-guards-on-nonlocal-exit
    (call-with-reseed-auxiliary-fixture
     (lambda (root owner auxiliary)
       (declare (ignore root))
       (let ((saved nil) (escaped nil))
         (ok (eq :cut
                 (catch 'cut
                   (pg::call-with-physical-reseed-install-auxiliary
                    owner auxiliary
                    (lambda (directory facts fd check)
                      (declare (ignore directory fd))
                      (setf saved (copy-tree facts) escaped check)
                      (throw 'cut :cut))
                    :prepare t :empty t))))
         (ok (intake-fails-p escaped))
         (pg::call-with-physical-reseed-install-auxiliary
          owner auxiliary (lambda (&rest values) (declare (ignore values)) (ok t))
          :expected saved)))))

(deftest reseed-install-auxiliary-checks-survive-superseded-ordinary-role
    (call-with-reseed-auxiliary-fixture
     (lambda (root owner auxiliary)
       (let ((saved nil))
         (pg::call-with-physical-reseed-install-auxiliary
          owner auxiliary
          (lambda (directory facts fd check)
            (declare (ignore directory fd))
            (setf saved (copy-tree facts))
            (with-open-file (out (pg::physical-reseed-install-temporary-path root) :direction :output)
              (write-string "interrupted initial installation" out))
            (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner))))
            (ok (funcall check)))
          :prepare t :empty t)
         (setf (pg::physical-adoption-owner-active-p owner) nil)
         (close (pg::physical-adoption-owner-lock owner))
         (pg::call-with-physical-reseed-cleanup-owner
          root
          (lambda (cleanup)
            (pg::call-with-physical-reseed-install-auxiliary
             cleanup auxiliary
             (lambda (directory facts fd check)
               (declare (ignore directory fd))
               (ok (equal saved facts)) (ok (funcall check)))
             :expected saved)
            (with-physical-store-directory (fresh)
              (sb-posix:chmod (namestring fresh) #o700)
              (ok (intake-fails-p
                   (lambda ()
                     (pg::call-with-physical-reseed-install-auxiliary
                      cleanup fresh (lambda (&rest values) (declare (ignore values)))
                      :prepare t :empty t))))
              (ok (null (pg::directory-entry-names fresh))))))))))

(deftest reseed-install-auxiliary-rejects-changed-private-guards
    (dolist (kind '(:fence-mode :fence-bytes :fence-link :fence-replacement :fence-symlink :lock-bytes))
      (call-with-reseed-auxiliary-fixture
       (lambda (root owner auxiliary)
         (declare (ignore root))
         (ok (eq :checked
                 (catch 'guard-damaged
                   (pg::call-with-physical-reseed-install-auxiliary
                    owner auxiliary
                    (lambda (directory facts fd check)
                      (declare (ignore facts fd))
                      (let ((fence (merge-pathnames "physical-reseed.install" directory))
                            (lock (merge-pathnames ".pg_lisp.lock" directory)))
                        (ecase kind
                          (:fence-mode (sb-posix:chmod (namestring fence) #o644))
                          (:fence-link (sb-posix:link (namestring fence) (namestring (merge-pathnames "alias" directory))))
                          (:fence-bytes
                           (with-open-file (out fence :direction :output :if-exists :overwrite)
                             (write-char #\X out)))
                          (:lock-bytes
                           (with-open-file (out lock :direction :output :if-exists :overwrite)
                             (write-char #\X out)))
                          ((:fence-replacement :fence-symlink)
                           (let ((saved (merge-pathnames "saved-fence" directory)))
                             (sb-posix:rename (namestring fence) (namestring saved))
                             (if (eq kind :fence-symlink)
                                 (sb-posix:symlink (namestring saved) (namestring fence))
                                 (progn
                                   (with-open-file (out fence :direction :output)
                                     (write-string pg::*physical-reseed-install-guard-text* out))
                                   (sb-posix:chmod (namestring fence) #o600))))))
                        (ok (intake-fails-p check) (symbol-name kind))
                        (throw 'guard-damaged :checked)))
                    :prepare t :empty t))))))))

(deftest reseed-install-auxiliary-refuses-existing-intent-and-nonempty-quarantine
    (dolist (name '(".pg_lisp.lock" "physical-reseed.install" "old-data"))
      (call-with-reseed-auxiliary-fixture
       (lambda (root owner auxiliary)
         (declare (ignore root))
         (let ((path (merge-pathnames name auxiliary)))
           (with-open-file (out path :direction :output) (write-string "preserve" out))
           (ok (intake-fails-p
                (lambda ()
                  (pg::call-with-physical-reseed-install-auxiliary
                   owner auxiliary (lambda (&rest values) (declare (ignore values)))
                   :prepare t :empty (equal name "old-data")))))
           (ok (equalp (pg::string->bytes "preserve") (lineage-test-bytes path)))
           (ok (equal (list name) (pg::directory-entry-names auxiliary))))))))

(deftest reseed-install-guarded-image-inventory-keeps-root-and-child-boundaries
    (dolist (kind '(:valid :root-extra :child-extra :missing-guard :public-guard))
      (with-physical-store-directory (directory)
        (sb-posix:chmod (namestring directory) #o700)
        (let ((fd (pg::physical-backup-open-directory directory)))
          (unwind-protect
               (pg::call-with-physical-backup-receive-directory
                fd
                (lambda (anchored)
                  (let* ((manifest (list :files '(("PG_VERSION" 1 42) ("global/pg_control" 1 42))))
                         (names (append (mapcar #'first (pg::physical-backup-metadata-bounds))
                                        (mapcar #'first (getf manifest :files))
                                        '(".pg_lisp.lock" "physical-reseed.install"))))
                    (dolist (name names)
                      (pg::call-with-physical-image-output anchored name (lambda (out) (write-byte 42 out))))
                    (ok (intake-fails-p (lambda () (pg::physical-backup-check-received-inventory anchored manifest))))
                    (ecase kind
                      (:valid nil)
                      (:root-extra (pg::call-with-physical-image-output anchored "unexpected" (lambda (out) (write-byte 42 out))))
                      (:child-extra (pg::call-with-physical-image-output anchored "global/physical-reseed.install" (lambda (out) (write-byte 42 out))))
                      (:missing-guard (delete-file (merge-pathnames "physical-reseed.install" anchored)))
                      (:public-guard (sb-posix:chmod (namestring (merge-pathnames "physical-reseed.install" anchored)) #o644)))
                    (if (eq kind :valid)
                        (ok (pg::physical-backup-check-received-inventory anchored manifest :reseed-installation t))
                        (ok (intake-fails-p
                             (lambda () (pg::physical-backup-check-received-inventory anchored manifest :reseed-installation t))))))))
            (sb-posix:close fd))))))

(deftest reseed-install-auxiliary-refuses-intent-arriving-before-prepare-callback
    (dolist (cut '(:after-auxiliary-open :after-auxiliary-lock :after-auxiliary-sync))
      (call-with-reseed-auxiliary-fixture
       (lambda (root owner auxiliary)
         (let ((called nil)
               (pg::*physical-reseed-install-test-hook*
                 (lambda (point directory)
                   (declare (ignore directory))
                   (when (eq cut point)
                     (with-open-file (out (pg::physical-reseed-install-temporary-path root) :direction :output)
                       (write-string "new installation intent" out))))))
           (ok (intake-fails-p
                (lambda ()
                  (pg::call-with-physical-reseed-install-auxiliary
                   owner auxiliary
                   (lambda (&rest values) (declare (ignore values)) (setf called t))
                   :prepare t :empty t))))
           (ok (not called))
           (ok (equal (case cut
                        (:after-auxiliary-open nil)
                        (:after-auxiliary-lock '(".pg_lisp.lock"))
                        (:after-auxiliary-sync '(".pg_lisp.lock" "physical-reseed.install")))
                      (sort (pg::directory-entry-names auxiliary) #'string<))))))))

(deftest reseed-install-auxiliary-refuses-replaced-directory-without-writing-foreign-tree
    (call-with-reseed-auxiliary-fixture
     (lambda (root owner auxiliary)
       (declare (ignore root))
       (let ((moved (pathname (concatenate 'string (string-right-trim "/" (namestring auxiliary)) ".saved/")))
             (fired nil) (called nil))
         (unwind-protect
              (let ((pg::*physical-reseed-install-test-hook*
                      (lambda (point directory)
                        (when (eq point :after-auxiliary-open)
                          (setf fired t)
                          (sb-posix:rename (namestring directory) (namestring moved))
                          (sb-posix:mkdir (namestring directory) #o700)))))
                (ok (intake-fails-p
                     (lambda ()
                       (pg::call-with-physical-reseed-install-auxiliary
                        owner auxiliary
                        (lambda (&rest values) (declare (ignore values)) (setf called t))
                        :prepare t :empty t))))
                (ok fired) (ok (not called))
                (ok (null (pg::directory-entry-names auxiliary)))
                (ok (null (pg::directory-entry-names moved))))
           (when fired
             (sb-posix:rmdir (namestring auxiliary))
             (sb-posix:rename (namestring moved) (namestring auxiliary))))))))

(deftest reseed-install-guarded-inventory-requires-descriptor-scope-and-boolean-mode
    (let ((pg::*physical-image-context* nil))
      (ok (intake-fails-p
           (lambda () (pg::physical-backup-check-received-inventory #p"/unused/" nil :reseed-installation t))))
      (ok (intake-fails-p
           (lambda () (pg::physical-backup-check-received-inventory #p"/unused/" nil :reseed-installation :unknown))))
      (ok (null (pg::physical-backup-check-received-inventory #p"/unused/" nil)))))

(deftest reseed-install-auxiliary-reopen-rejects-unsafe-guard-entries-without-following
    (dolist (name '(".pg_lisp.lock" "physical-reseed.install"))
      (dolist (kind '(:symlink :fifo :directory :missing))
        (call-with-reseed-auxiliary-fixture
         (lambda (root owner auxiliary)
           (declare (ignore root))
           (let ((saved nil) (called nil)
                             (path (merge-pathnames name auxiliary))
                             (original (merge-pathnames "saved-guard" auxiliary)))
             (pg::call-with-physical-reseed-install-auxiliary
              owner auxiliary
              (lambda (directory facts fd check)
                (declare (ignore directory fd check))
                (setf saved (copy-tree facts)))
              :prepare t :empty t)
             (sb-posix:rename (namestring path) (namestring original))
             (ecase kind
               (:symlink (sb-posix:symlink (namestring original) (namestring path)))
               (:fifo (sb-posix:mkfifo (namestring path) #o600))
               (:directory (sb-posix:mkdir (namestring path) #o700))
               (:missing nil))
             (ok (intake-fails-p
                  (lambda ()
                    (pg::call-with-physical-reseed-install-auxiliary
                     owner auxiliary
                     (lambda (&rest values) (declare (ignore values)) (setf called t))
                     :expected saved))))
             (ok (not called))
             (ok (equalp (pg::physical-reseed-install-guard-bytes name) (lineage-test-bytes original)))
             (ok (if (eq kind :missing)
                     (not (pg::physical-adoption-path-present-p path))
                     (let ((mode (sb-posix:stat-mode (sb-posix:lstat (namestring path)))))
                       (ecase kind
                         (:symlink (sb-posix:s-islnk mode))
                         (:fifo (sb-posix:s-isfifo mode))
                         (:directory (sb-posix:s-isdir mode))))))))))))
