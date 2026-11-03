(in-package #:pg-physical-tests)

(defun reseed-publication-test-image (request)
  (let* ((manifest (copy-tree (getf (reseed-install-codec-fixture 0) :manifest)))
         (files (list (list "PG_VERSION" (pg::string->bytes (format nil "16~%")))
                      (list "global/pg_control" (make-array 196 :element-type '(unsigned-byte 8) :initial-element 42))
                      (list (concatenate 'string "pg_wal/" (pg::timeline-history-filename (getf request :timeline)))
                            (pg::string->bytes "history")))))
    (setf (getf manifest :source-pin) (copy-seq (getf request :name))
          (getf manifest :cluster-identity) (copy-seq (getf request :cluster-identity))
          (getf manifest :system-id) (getf request :system-id)
          (getf manifest :timeline) (getf request :timeline)
          (getf manifest :files)
          (loop for (name bytes) in files collect (list name (length bytes) (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
    (let* ((encoded (pg::physical-bootstrap-manifest-image manifest))
           (payloads (append (loop for entry in (pg::physical-backup-metadata-bounds) for name = (first entry)
                                   collect (list name (if (equal name "physical-backup.manifest") encoded
                                                          (make-array 1 :element-type '(unsigned-byte 8) :initial-element 42))))
                             files)))
      (values (list :manifest manifest :manifest-digest (pg::physical-store-update-digest #xCBF29CE484222325 encoded)
                    :files (loop for (name bytes) in payloads
                                 collect (list name (length bytes) (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
              payloads))))

(defun reseed-publication-test-write-image (directory payloads)
  (let ((fd (pg::physical-backup-open-directory directory)))
    (unwind-protect
         (pg::call-with-physical-backup-receive-directory
          fd
          (lambda (anchored)
            (dolist (entry payloads)
              (pg::call-with-physical-image-output
               anchored (first entry) (lambda (output) (write-sequence (second entry) output))))))
      (sb-posix:close fd))))

(defun reseed-publication-test-check-image (directory &optional expected guarded)
  (multiple-value-bind (manifest digest) (pg::recovery-read-manifest directory)
    (pg::recovery-check-manifest-schema manifest)
    (pg::recovery-check-manifest-files manifest directory)
    (pg::physical-backup-check-received-inventory directory manifest :reseed-installation guarded)
    (let ((actual (list :manifest manifest :manifest-digest digest
                        :files (pg::physical-backup-bundle-entries directory manifest))))
      (when (and expected (not (equal expected actual))) (error "test image changed"))
      actual)))

(defun call-with-reseed-publication-fixture (function)
  (call-with-reseed-remote-fixture
   (lambda (root stage owner request profile state)
     (pg::physical-reseed-start-remote owner stage request profile :helper "/unused/helper")
     (delete-file (merge-pathnames "received.witness" stage))
     (multiple-value-bind (image payloads) (reseed-publication-test-image request)
       (reseed-publication-test-write-image stage payloads)
       (setf (getf (getf (getf state :status-result) :base) :manifest-digest) (getf image :manifest-digest))
       (let ((original (symbol-function 'pg::physical-reseed-image-check)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-reseed-image-check) #'reseed-publication-test-check-image)
                (with-physical-store-directory (incoming)
                  (with-physical-store-directory (quarantine)
                    (dolist (directory (list incoming quarantine)) (sb-posix:chmod (namestring directory) #o700))
                    (pg::physical-reseed-prepare-image owner incoming :helper "/unused/helper")
                    (funcall function root stage incoming quarantine owner image state))))
           (setf (symbol-function 'pg::physical-reseed-image-check) original)))))))

(defun call-with-reseed-publication-reopen (root old-owner state function)
  (setf (pg::physical-adoption-owner-active-p old-owner) nil)
  (close (pg::physical-adoption-owner-lock old-owner))
  (pg::call-with-physical-reseed-operation-owner
   root
   (lambda (owner)
     (setf (getf state :reader-owner) owner)
     (funcall function owner))))

(deftest reseed-install-publication-nonlocal-exits-release-scopes-and-keep-cleanup
    (dolist (cut '(:before-install-create :after-install-create :after-install-write :after-install-file-fsync
                   :before-install-rename :after-install-rename :after-install-directory-fsync
                   :before-install-readback :after-install-readback))
      (call-with-reseed-publication-fixture
       (lambda (root stage incoming quarantine owner image state)
         (declare (ignore image))
         (let ((pg::*physical-reseed-install-test-hook*
                 (lambda (point path)
                   (declare (ignore path))
                   (when (eq point cut) (throw 'publication-cut :cut)))))
           (ok (eq :cut (catch 'publication-cut
                          (pg::physical-reseed-publish-install owner incoming quarantine :helper "/unused/helper")))))
         (dolist (directory (list stage incoming)) (uiop:delete-directory-tree directory :validate t))
         (call-with-reseed-publication-reopen
          root owner state
          (lambda (selected)
            (ok (eq :ready (getf (pg::physical-reseed-status selected :helper "/unused/helper") :phase)))
            (ok (eq :aborted (getf (pg::physical-reseed-cancel selected :helper "/unused/helper") :phase)))))
         (ok (every #'pg::physical-libpq-channel-closed (getf state :channels)))))))

(deftest reseed-install-publication-checks-canonical-temporary-after-pre-rename-hook
    (call-with-reseed-publication-fixture
     (lambda (root stage incoming quarantine owner image state)
       (declare (ignore stage image state))
       (let* ((fired nil)
              (pg::*physical-reseed-install-test-hook*
                (lambda (point path)
                  (when (eq point :before-install-rename)
                    (setf fired t)
                    (with-open-file (out path :direction :output :element-type '(unsigned-byte 8)
                                              :if-exists :append)
                      (write-byte 99 out))))))
         (ok (intake-fails-p
              (lambda () (pg::physical-reseed-publish-install owner incoming quarantine :helper "/unused/helper"))))
         (ok fired)
         (ok (not (pg::physical-adoption-path-present-p (pg::physical-reseed-install-path root))))
         (ok (pg::physical-adoption-path-present-p (pg::physical-reseed-install-temporary-path root)))))))

(deftest reseed-install-publication-refuses-journal-mutation-after-readback
    (call-with-reseed-publication-fixture
     (lambda (root stage incoming quarantine owner image state)
       (declare (ignore stage image state))
       (let* ((fired nil)
              (pg::*physical-reseed-install-test-hook*
                (lambda (point path)
                  (declare (ignore path))
                  (when (eq point :after-install-readback)
                    (setf fired t)
                    (with-open-file (out (pg::physical-reseed-install-path root)
                                         :direction :output :element-type '(unsigned-byte 8) :if-exists :append)
                      (write-byte 99 out))))))
         (ok (intake-fails-p
              (lambda () (pg::physical-reseed-publish-install owner incoming quarantine :helper "/unused/helper"))))
         (ok fired)))))

(deftest reseed-install-publication-refuses-changed-proofs-at-intent-boundaries
    (dolist (point '(:before-install-create :after-install-create :before-install-rename
                     :after-install-rename :before-install-readback :after-install-readback))
      (dolist (mutation '(:old-extra :incoming-bytes :stage-bytes :quarantine-extra :attempt-generation :source-not-ready))
        (call-with-reseed-publication-fixture
         (lambda (root stage incoming quarantine owner image state)
           (declare (ignore image))
           (with-open-file (original-attempt (pg::physical-reseed-attempt-path root))
             (let* ((fired nil)
                    (pg::*physical-reseed-install-test-hook*
                      (lambda (actual path)
                        (declare (ignore path))
                        (when (and (eq point actual) (not fired))
                          (setf fired t)
                          (case mutation
                            (:source-not-ready (setf (getf state :status-result) nil))
                            (:attempt-generation
                             (let* ((path (pg::physical-reseed-attempt-path root))
                                    (bytes (lineage-test-bytes path)))
                               (delete-file path)
                               (with-open-file (stream path :direction :output :element-type '(unsigned-byte 8)
                                                            :if-exists :error)
                                 (sb-posix:fchmod (sb-sys:fd-stream-fd stream) #o600)
                                 (write-sequence bytes stream))))
                            (otherwise
                             (let* ((directory (case mutation
                                                 (:old-extra root) (:incoming-bytes incoming)
                                                 (:stage-bytes stage) (:quarantine-extra quarantine)))
                                    (name (if (member mutation '(:incoming-bytes :stage-bytes))
                                              "global/pg_control" "unexpected"))
                                    (path (merge-pathnames name directory)))
                               (with-open-file (stream path :direction :output :element-type '(unsigned-byte 8)
                                                            :if-exists :overwrite)
                                 (write-byte 99 stream)))))))))
               (ok (intake-fails-p
                    (lambda () (pg::physical-reseed-publish-install owner incoming quarantine :helper "/unused/helper"))))
               (ok fired)
               (ok (open-stream-p original-attempt))
               (ok (= 1 (getf state :captures)))
               (ok (every #'pg::physical-libpq-channel-closed (getf state :channels))))))))))

(deftest reseed-install-initial-journal-keeps-descriptor-bound-fences-at-cuts
    (dolist (cut '(nil :before-install-create :after-install-create :after-install-write
                   :after-install-file-fsync :before-install-rename :after-install-rename
                   :after-install-directory-fsync :before-install-readback :after-install-readback))
      (call-with-reseed-install-owner-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine))
         (delete-file (pg::physical-reseed-install-path root))
         (pg::call-with-physical-reseed-owner
          root
          (lambda (owner)
            (pg::call-with-physical-directory-root
             owner
             (lambda (fd directory)
               (declare (ignore directory))
               (let* ((fired nil) (checks 0)
                                  (pg::*physical-reseed-install-test-hook*
                                    (lambda (point path)
                                      (declare (ignore path))
                                      (when (eq point cut) (setf fired t) (error "journal cut"))))
                                  (publish
                                    (lambda ()
                                      (pg::physical-reseed-install-publish-journal
                                       fd plan
                                       (lambda (name)
                                         (pg::physical-directory-check-owner-common owner)
                                         (incf checks)
                                         (when name
                                           (ok (member name '("physical-reseed.install.tmp" "physical-reseed.install")
                                                       :test #'equal))))))))
                 (if cut
                     (progn (ok (intake-fails-p publish)) (ok fired))
                     (ok (funcall publish)))
                 (ok (plusp checks))
                 (unless (eq cut :before-install-create)
                   (let* ((final (probe-file (pg::physical-reseed-install-path root)))
                          (name (if final "physical-reseed.install" "physical-reseed.install.tmp")))
                     (when (or (null cut)
                               (member cut '(:after-install-write :after-install-file-fsync :before-install-rename
                                             :after-install-rename :after-install-directory-fsync
                                             :before-install-readback :after-install-readback)))
                       (let ((state (pg::physical-reseed-install-read-at fd name :stabilize t)))
                         (ok (zerop (getf state :step)))
                         (ok (not (getf state :partial)))
                         (ok (equal (getf plan :old) (getf (getf state :plan) :old))))))))))))))))

(deftest reseed-install-publication-inventory-binds-real-image-bytes
    (call-with-reseed-publication-fixture
     (lambda (root stage incoming quarantine owner image state)
       (declare (ignore root stage quarantine owner state))
       (let ((fd (pg::physical-backup-open-directory incoming)))
         (unwind-protect
              (pg::call-with-physical-backup-image-directory
               fd
               (lambda (directory)
                 (declare (ignore directory))
                 (let* ((context pg::*physical-image-context*)
                        (rows (pg::physical-reseed-install-image-rows context image :synchronize t))
                        (device (sb-posix:stat-dev (sb-posix:fstat fd))))
                   (ok (= 12 (length rows)))
                   (ok (equal rows (pg::physical-reseed-install-new-copy
                                    rows (getf image :manifest) (getf image :manifest-digest)
                                    device (make-hash-table :test #'equal))))
                   (let ((path (merge-pathnames "global/pg_control" incoming)))
                     (with-open-file (stream path :direction :output :element-type '(unsigned-byte 8)
                                                  :if-exists :overwrite)
                       (write-byte 99 stream)))
                   (ok (intake-fails-p
                        (lambda () (pg::physical-reseed-install-image-rows context image)))))))
           (sb-posix:close fd))))))

(deftest reseed-install-publication-old-scan-excludes-only-fixed-own-intent
    (call-with-reseed-publication-fixture
     (lambda (root stage incoming quarantine owner image state)
       (declare (ignore stage incoming quarantine owner image state))
       (let* ((fd (pg::physical-backup-open-directory root))
              (mount (pg::physical-adoption-install-mount-id nil :directory-fd fd))
              (device (sb-posix:stat-dev (sb-posix:fstat fd))))
         (unwind-protect
              (let ((rows (pg::physical-reseed-install-old-scan fd mount device nil)))
                (with-open-file (stream (merge-pathnames "physical-reseed.install.tmp" root)
                                        :direction :output :if-exists :error)
                  (write-char #\x stream))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-install-old-scan fd mount device nil))))
                (ok (equal rows (pg::physical-reseed-install-old-scan
                                 fd mount device nil :install-entry "physical-reseed.install.tmp")))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-install-old-scan
                                 fd mount device nil :install-entry "old-dirty-data"))))
                (with-open-file (stream (merge-pathnames "physical-reseed.install" root)
                                        :direction :output :if-exists :error)
                  (write-char #\x stream))
                (ok (intake-fails-p
                     (lambda () (pg::physical-reseed-install-old-scan
                                 fd mount device nil :install-entry "physical-reseed.install.tmp")))))
           (sb-posix:close fd))))))

(deftest reseed-install-publication-preserves-old-data-and-keeps-status-and-abort
    (call-with-reseed-publication-fixture
     (lambda (root stage incoming quarantine owner image state)
       (declare (ignore image))
       (with-open-file (held-attempt (pg::physical-reseed-attempt-path root) :element-type '(unsigned-byte 8))
         (let* ((attempt-path (pg::physical-reseed-attempt-path root))
                (before (lineage-test-bytes attempt-path))
                (inode (sb-posix:stat-ino (sb-posix:fstat (sb-sys:fd-stream-fd held-attempt)))))
           (ok (eq t (pg::physical-reseed-publish-install owner incoming quarantine :helper "/unused/helper")))
           (ok (equalp before (lineage-test-bytes attempt-path)))
           (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring attempt-path)))))
           (ok (intake-fails-p (lambda () (pg::physical-reseed-check-owner owner))))
           (ok (equal '(".pg_lisp.lock" "physical-reseed.install") (sort (pg::directory-entry-names quarantine) #'string<)))
           (dolist (directory (list stage incoming)) (uiop:delete-directory-tree directory :validate t))
           (call-with-reseed-publication-reopen
            root owner state
            (lambda (installed)
              (ok (pg::physical-reseed-install-owner-p installed))
              (ok (eq :ready (getf (pg::physical-reseed-status installed :helper "/unused/helper") :phase)))
              (ok (eq :aborted (getf (pg::physical-reseed-cancel installed :helper "/unused/helper") :phase)))
              (ok (/= inode (sb-posix:stat-ino (sb-posix:lstat (namestring attempt-path)))))
              (ok (pg::physical-reseed-check-owner installed))))
           (ok (= 1 (getf state :captures)))
           (ok (= 1 (getf state :aborts)))
           (ok (every #'pg::physical-libpq-channel-closed (getf state :channels))))))))

(deftest reseed-install-publication-cuts-keep-a-source-cleanup-owner
    (dolist (point '(:before-install-create :after-install-create :after-install-write :after-install-file-fsync
                     :before-install-rename :after-install-rename :after-install-directory-fsync
                     :before-install-readback :after-install-readback))
      (call-with-reseed-publication-fixture
       (lambda (root stage incoming quarantine owner image state)
         (declare (ignore image))
         (let* ((fired nil)
                (pg::*physical-reseed-install-test-hook*
                  (lambda (actual path)
                    (declare (ignore path))
                    (when (eq point actual) (setf fired t) (error "publication cut")))))
           (ok (intake-fails-p
                (lambda () (pg::physical-reseed-publish-install owner incoming quarantine :helper "/unused/helper"))))
           (ok fired))
         (dolist (directory (list stage incoming)) (uiop:delete-directory-tree directory :validate t))
         (call-with-reseed-publication-reopen
          root owner state
          (lambda (selected)
            (ok (eq :ready (getf (pg::physical-reseed-status selected :helper "/unused/helper") :phase)))
            (ok (eq :aborted (getf (pg::physical-reseed-cancel selected :helper "/unused/helper") :phase)))))
         (ok (= 1 (getf state :captures)))
         (ok (= 1 (getf state :aborts)))))))

(deftest reseed-install-publication-requires-original-source-ready-before-local-intent
    (call-with-reseed-publication-fixture
     (lambda (root stage incoming quarantine owner image state)
       (declare (ignore stage image))
       (setf (getf state :status-result) nil)
       (let ((condition (handler-case
                            (progn (pg::physical-reseed-publish-install owner incoming quarantine :helper "/unused/helper") nil)
                          (error (condition) condition))))
         (ok (typep condition 'pg::pg-client-error)))
       (ok (not (pg::physical-adoption-path-present-p (pg::physical-reseed-install-path root))))
       (ok (not (pg::physical-adoption-path-present-p (pg::physical-reseed-install-temporary-path root))))
       (ok (equal (sort (copy-list '("base" "global" "pg_wal" "PG_VERSION" "authentication.initialized" "pg_hba.conf" "pg_ident.conf"
                                     "physical-backup.manifest" "physical-recovery.control" "recovery.required")) #'string<)
                  (sort (pg::directory-entry-names incoming) #'string<)))
       (ok (null (pg::directory-entry-names quarantine))))))