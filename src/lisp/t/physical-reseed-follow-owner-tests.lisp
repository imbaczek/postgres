(in-package #:pg-physical-tests)

(defun call-with-reseed-follow-installed-fixture (function)
  (call-with-adoption-stage-fixture
   (lambda (source state proof)
     (declare (ignore source))
     (with-physical-store-directory (stage)
       (sb-posix:chmod (namestring stage) #o700)
       (let* ((name (getf (backup-lease-test-request) :name))
              (result (pg::physical-adoption-stage-base state proof stage name))
              (manifest (getf result :base-state))
              (digest (getf result :manifest-digest)))
         (dolist (child '("base/" "global/" "pg_wal/"))
           (sb-posix:chmod (namestring (merge-pathnames child stage)) #o700))
         (dolist (entry (append (pg::physical-backup-metadata-bounds) (getf manifest :files)))
           (sb-posix:chmod (namestring (merge-pathnames (first entry) stage)) #o600))
         (call-with-reseed-owner-directory
          (lambda (root)
            (reseed-test-write-pid root (- (call-with-reaped-reseed-pid #'identity)))
            (with-physical-store-directory (incoming)
              (with-physical-store-directory (quarantine)
                (dolist (directory (list incoming quarantine))
                  (sb-posix:chmod (namestring directory) #o700))
                (let ((plan nil))
                  (pg::call-with-physical-reseed-owner
                   root
                   (lambda (owner)
                     (let* ((record (reseed-attempt-test-record owner))
                            (request (getf record :request))
                            (stat (sb-posix:lstat (namestring stage))))
                       (dolist (key '(:cluster-identity :system-id :timeline))
                         (setf (getf request key) (getf manifest key)))
                       (setf (getf record :stage)
                             (list :directory (namestring stage)
                                   :device (sb-posix:stat-dev stat) :inode (sb-posix:stat-ino stat))
                             record (pg::physical-reseed-attempt-publish owner record))
                       (call-with-reseed-step-source
                        (list :attempt record :manifest manifest :manifest-digest digest)
                        (lambda (remote)
                          (declare (ignore remote))
                          (pg::physical-reseed-prepare-image owner incoming :helper "/unused/helper")
                          (pg::physical-reseed-publish-install
                           owner incoming quarantine :helper "/unused/helper")
                          (setf plan (getf (pg::physical-reseed-install-initial-arguments root) :plan)))))))
                  (call-with-reseed-step-source
                   plan
                   (lambda (remote)
                     (declare (ignore remote))
                     (pg::call-with-physical-reseed-install-owner
                      root
                      (lambda (owner)
                        (loop repeat (pg::physical-reseed-install-last-step plan)
                              do (pg::physical-reseed-install-advance owner :helper "/unused/helper"))))))
                  (funcall function root stage incoming quarantine plan)))))))))))

(deftest reseed-follow-owner-fixture-builds-a-real-installed-base
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (multiple-value-bind (manifest digest) (pg::physical-reseed-image-base-state root)
         (ok (= digest (getf plan :manifest-digest)))
         (ok (= (getf manifest :end) (getf (getf plan :manifest) :end))))
       )))

(deftest reseed-follow-owner-opens-only-terminal-installed-base
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (let ((saved nil))
         (pg::call-with-physical-reseed-follow-owner
          root
          (lambda (owner)
            (setf saved owner)
            (ok (eq owner (pg::physical-reseed-check-owner owner)))
            (ok (null (pg::physical-reseed-follow-read owner)))
            (let* ((record (pg::physical-reseed-follow-initial-record owner))
                   (reference (getf record :install)))
              (ok (= (getf reference :step) (pg::physical-reseed-install-last-step plan)))
              (ok (null (getf record :generations)))
              (ok (plusp (getf (getf record :base) :manifest-digest)))
              (ok (not (search "journal-secret" (princ-to-string owner))))
              (setf (char (getf (getf (getf record :attempt) :request) :token) 0) #\x)
              (ok (char/= #\x (char (getf (getf (getf (pg::physical-reseed-follow-initial-record owner) :attempt) :request) :token) 0))))))
         (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner saved))))))))

(defun reseed-follow-owner-test-write (path bytes)
  (with-open-file (out path :direction :output :if-exists :error :element-type '(unsigned-byte 8))
    (sb-posix:fchmod (sb-sys:fd-stream-fd out) #o600)
    (write-sequence bytes out)
    (pg::fsync-stream out))
  (pg::fsync-directory (uiop:pathname-directory-pathname path)))

(deftest reseed-follow-owner-rejects-nonterminal-and-torn-predecessors
    (dolist (kind '(:nonterminal :partial :temporary))
      (call-with-reseed-follow-installed-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine))
         (case kind
           (:nonterminal (reseed-install-owner-test-write root plan (1- (pg::physical-reseed-install-last-step plan))))
           (:partial
            (with-open-file (out (pg::physical-reseed-install-path root) :direction :io
                                                                         :if-exists :overwrite :element-type '(unsigned-byte 8))
              (file-position out (file-length out))
              (write-byte 83 out)))
           (:temporary (reseed-follow-owner-test-write
                        (pg::physical-reseed-install-temporary-path root)
                        (make-array 0 :element-type '(unsigned-byte 8)))))
         (let ((entered nil))
           (ok (intake-fails-p
                (lambda () (pg::call-with-physical-reseed-follow-owner root (lambda (owner) (declare (ignore owner)) (setf entered t))))))
           (ok (not entered)))))))

(deftest reseed-follow-owner-does-not-inherit-legacy-operation-authority
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (dolist (entry '((pg::physical-reseed-attempt-check-owner 1)
                           (pg::physical-reseed-attempt-read-owned 1)
                           (pg::physical-reseed-attempt-read-file 1)
                           (pg::physical-reseed-attempt-read 0)
                           (pg::physical-reseed-attempt-recover 0)
                           (pg::physical-reseed-attempt-recover-for-cancel 0)
                           (pg::call-with-physical-reseed-cancel-temporary 1)
                           (pg::physical-reseed-cancel-normalize-temporary 6)
                           (pg::call-with-physical-reseed-client 2)
                           (pg::physical-reseed-status 0)
                           (pg::physical-reseed-cancel 0)
                           (pg::physical-reseed-install-advance 0)
                           (pg::call-with-physical-reseed-install-progress 7)
                           (pg::physical-reseed-install-publish-prepared 14)
                           (pg::physical-reseed-install-publish-workspaces 9)))
            (let ((message (handler-case
                               (progn (apply (first entry) owner (make-list (second entry))) nil)
                             (error (condition) (princ-to-string condition)))))
              (ok (and message (search "does not authorize legacy reseed attempt operations" message))
                  (symbol-name (first entry)))))
          (ok (intake-fails-p
               (lambda () (pg::physical-reseed-attempt-publish owner (getf plan :attempt)))))
          (ok (intake-fails-p (lambda () (pg::physical-adoption-check-owner owner))))
          (ok (pg::physical-reseed-check-owner owner)))))))

(deftest reseed-follow-owner-validates-full-image-and-namespace
    (dolist (kind '(:extra :control))
      (call-with-reseed-follow-installed-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine plan))
         (if (eq kind :extra)
             (pg::wal-atomic-write-bytes (merge-pathnames "unexpected" root) (pg::string->bytes "x"))
             (with-open-file (out (merge-pathnames "global/pg_control" root) :direction :io
                                                                             :if-exists :overwrite :element-type '(unsigned-byte 8))
               (write-byte 0 out)))
         (let ((entered nil))
           (ok (intake-fails-p
                (lambda () (pg::call-with-physical-reseed-follow-owner root (lambda (owner) (declare (ignore owner)) (setf entered t))))))
           (ok (not entered)))))))

(deftest reseed-follow-owner-binds-canonical-final-and-fences-partial-temporary
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (let ((record nil))
         (pg::call-with-physical-reseed-follow-owner
          root (lambda (owner) (setf record (pg::physical-reseed-follow-initial-record owner))))
         (reseed-follow-owner-test-write (merge-pathnames "physical-reseed.follow" root)
                                         (pg::physical-reseed-follow-image record))
         (pg::call-with-physical-reseed-follow-owner
          root (lambda (owner)
                 (ok (equal record (pg::physical-reseed-follow-read owner)))
                 (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-initial-record owner))))))
         (reseed-follow-owner-test-write (merge-pathnames "physical-reseed.follow.tmp" root)
                                         (make-array 0 :element-type '(unsigned-byte 8)))
         (pg::call-with-physical-reseed-follow-owner
          root (lambda (owner)
                 (ok (pg::physical-reseed-check-owner owner))
                 (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-read owner))))))
         (ok (zerop (length (lineage-test-bytes (merge-pathnames "physical-reseed.follow.tmp" root)))))))))

(deftest reseed-follow-owner-is-thread-bound-and-retains-stopped-pid-evidence
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let ((thread (sb-thread:make-thread
                         (lambda () (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner)))))))
            (ok (sb-thread:join-thread thread)))
          (ok (probe-file (merge-pathnames "postmaster.pid" quarantine)))
          (ok (not (probe-file (merge-pathnames "postmaster.pid" root))))
          (ok (pg::physical-reseed-check-owner owner)))))))

(deftest reseed-follow-owner-rejects-unsafe-final-and-temporary-entries
    (dolist (name '("physical-reseed.follow" "physical-reseed.follow.tmp"))
      (dolist (kind '(:malformed :directory :symlink :hardlink :fifo :public))
        (call-with-reseed-follow-installed-fixture
         (lambda (root stage incoming quarantine plan)
           (declare (ignore stage incoming quarantine plan))
           (let ((path (merge-pathnames name root)) (entered nil))
             (case kind
               (:directory (sb-posix:mkdir (namestring path) #o700))
               (:symlink (sb-posix:symlink "missing" (namestring path)))
               (:hardlink (sb-posix:link (namestring (merge-pathnames "physical-reseed.attempt" root)) (namestring path)))
               (:fifo (sb-posix:mkfifo (namestring path) #o600))
               (otherwise
                (reseed-follow-owner-test-write path (pg::string->bytes "not a journal"))
                (when (eq kind :public) (sb-posix:chmod (namestring path) #o644))))
             (if (and (eq kind :malformed) (equal name "physical-reseed.follow.tmp"))
                 (progn
                   (pg::call-with-physical-reseed-follow-owner
                    root (lambda (owner)
                           (setf entered t)
                           (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-read owner))))))
                   (ok entered))
                 (progn
                   (ok (intake-fails-p
                        (lambda () (pg::call-with-physical-reseed-follow-owner
                                    root (lambda (owner) (declare (ignore owner)) (setf entered t))))))
                   (ok (not entered))))
             (ok (pg::physical-adoption-path-present-p path))))))))

(deftest reseed-follow-owner-refuses-a-canonical-journal-for-another-base
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (let ((record nil) (entered nil))
         (pg::call-with-physical-reseed-follow-owner
          root (lambda (owner) (setf record (pg::physical-reseed-follow-initial-record owner))))
         (incf (getf (getf record :base) :manifest-digest))
         (reseed-follow-owner-test-write (merge-pathnames "physical-reseed.follow" root)
                                         (pg::physical-reseed-follow-image record))
         (ok (intake-fails-p
              (lambda () (pg::call-with-physical-reseed-follow-owner
                          root (lambda (owner) (declare (ignore owner)) (setf entered t))))))
         (ok (not entered))))))

(deftest reseed-follow-reader-rechecks-the-name-after-opening-its-descriptor
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (let ((record nil) (fired 0))
         (pg::call-with-physical-reseed-follow-owner
          root (lambda (owner) (setf record (pg::physical-reseed-follow-initial-record owner))))
         (let* ((path (merge-pathnames "physical-reseed.follow" root))
                (bytes (pg::physical-reseed-follow-image record)))
           (reseed-follow-owner-test-write path bytes)
           (let ((fd (pg::physical-backup-open-directory root)))
             (unwind-protect
                  (let ((pg::*physical-reseed-follow-test-hook*
                          (lambda (point where)
                            (when (eq point :after-follow-journal-open)
                              (incf fired)
                              (sb-posix:rename (namestring where) (namestring (merge-pathnames "saved-follow" root)))
                              (reseed-follow-owner-test-write path bytes)))))
                    (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-read-at fd "physical-reseed.follow"))))
                    (ok (= 1 fired))
                    (ok (equalp bytes (lineage-test-bytes path))))
               (sb-posix:close fd))))))))

(deftest reseed-follow-owner-keeps-original-attempt-through-abort-inode-replacement
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine))
       (let* ((original (getf plan :attempt))
              (aborting (pg::physical-reseed-attempt-copy original))
              (path (pg::physical-reseed-attempt-path root))
              (temporary (merge-pathnames "replacement-attempt" root))
              (inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
         (setf (getf aborting :phase) :aborting)
         (flet ((replace-attempt (record)
                  (reseed-follow-owner-test-write temporary (pg::physical-reseed-attempt-image record))
                  (sb-posix:rename (namestring temporary) (namestring path))
                  (pg::fsync-directory root)))
           (replace-attempt aborting)
           (ok (/= inode (sb-posix:stat-ino (sb-posix:lstat (namestring path)))))
           (pg::call-with-physical-reseed-follow-owner
            root
            (lambda (owner)
              (ok (eq :aborting (getf (pg::physical-reseed-cleanup-owner-attempt owner) :phase)))
              (ok (eq :premanifest
                      (getf (getf (pg::physical-reseed-follow-initial-record owner) :attempt) :phase)))
              (replace-attempt original)
              (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner))))
              (replace-attempt aborting)
              (ok (pg::physical-reseed-check-owner owner)))))))))
