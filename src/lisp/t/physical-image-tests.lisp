(in-package #:pg-physical-tests)

(deftest reseed-receive-keeps-child-descriptors-through-writes-reads-and-failure
    (with-physical-store-directory (stage)
      (with-physical-store-directory (old)
        (sb-posix:chmod (namestring stage) #o700)
        (sb-posix:chmod (namestring old) #o700)
        (let* ((borrowed (pg::physical-backup-open-directory stage))
               (global (merge-pathnames "global/" stage))
               (saved (merge-pathnames "original-global/" stage))
               (redirected (merge-pathnames "pg_control" old))
               (owned-fds nil)
               (escaped-context nil))
          (unwind-protect
               (progn
                 (ok (intake-fails-p
                      (lambda ()
                        (pg::call-with-physical-backup-receive-directory
                         borrowed
                         (lambda (anchored)
                           (setf escaped-context pg::*physical-image-context*
                                 owned-fds
                                 (cons (pg::physical-image-context-root-fd escaped-context)
                                       (mapcar #'cdr (pg::physical-image-context-children escaped-context))))
                           (rename-file global saved)
                           (sb-posix:symlink
                            (namestring old) (string-right-trim "/" (namestring global)))
                           (pg::call-with-physical-image-output
                            anchored "global/pg_control" (lambda (output) (write-byte 42 output)))
                           (pg::with-physical-image-input
                               (input (pg::pgdata-resolve-path "global/pg_control" anchored)
                                      :element-type '(unsigned-byte 8))
                             (ok (= 1 (pg::physical-image-stream-length input)))
                             (ok (= 42 (read-byte input)))
                             (ok (eq :eof (read-byte input nil :eof))))
                           (ok (sb-posix:fstat borrowed)))))))
                 (ok (not (probe-file redirected)))
                 (ok (equalp #(42) (lineage-test-bytes (merge-pathnames "pg_control" saved))))
                 (ok (sb-posix:fstat borrowed))
                 (ok (= 4 (length owned-fds)))
                 (dolist (fd owned-fds)
                   (ok (intake-fails-p (lambda () (sb-posix:fstat fd)))))
                 (ok (intake-fails-p (lambda () (pg::physical-image-check-context escaped-context)))))
            (sb-posix:close borrowed)
            (when (probe-file saved)
              (sb-posix:unlink (string-right-trim "/" (namestring global)))
              (rename-file saved global)))))))

(deftest reseed-receive-rejects-input-symlinks-and-special-files-without-following
    (dolist (kind '(:symlink :fifo :hardlink))
      (with-physical-store-directory (stage)
        (with-physical-store-directory (old)
          (sb-posix:chmod (namestring stage) #o700)
          (sb-posix:chmod (namestring old) #o700)
          (let ((witness (merge-pathnames "old-data" old))
                (borrowed (pg::physical-backup-open-directory stage)))
            (with-open-file (output witness :direction :output)
              (write-string "unchanged" output))
            (unwind-protect
                 (pg::call-with-physical-backup-receive-directory
                  borrowed
                  (lambda (anchored)
                    (let ((path (pg::pgdata-resolve-path "global/pg_control" anchored)))
                      (ecase kind
                        (:symlink (sb-posix:symlink (namestring witness) (namestring path)))
                        (:fifo (sb-posix:mkfifo (namestring path) #o600))
                        (:hardlink (sb-posix:link (namestring witness) (namestring path))))
                      (ok (intake-fails-p
                           (lambda ()
                             (pg::with-physical-image-input (input path)
                               (read-line input nil nil)))))
                      (ok (equalp (pg::string->bytes "unchanged") (lineage-test-bytes witness))))))
              (sb-posix:close borrowed)))))))

(deftest reseed-receive-backup-authentication-refuses-replaced-policy-symlinks
    (dolist (name '("pg_hba.conf" "pg_ident.conf"))
      (with-physical-store-directory (stage)
        (with-physical-store-directory (outside)
          (sb-posix:chmod (namestring stage) #o700)
          (let ((borrowed (pg::physical-backup-open-directory stage))
                (witness (merge-pathnames name outside)))
            (unwind-protect
                 (pg::call-with-physical-backup-receive-directory
                  borrowed
                  (lambda (anchored)
                    (let* ((hba (format nil "host all all 127.0.0.1/32 trust~%# policy żółw~%"))
                           (ident (format nil "# mapping żółw~%"))
                           (text (if (equal name "pg_hba.conf") hba ident)))
                      (flet ((write-bytes (filename bytes)
                               (pg::call-with-physical-image-output
                                anchored filename (lambda (output) (write-sequence bytes output)))))
                        (write-bytes "physical-backup.manifest"
                                     (pg::physical-bootstrap-manifest-image
                                      (append (authentication-test-manifest) '(:auth-policy-required 1))))
                        (write-bytes "authentication.initialized" (pg::string->bytes pg::*authentication-marker-text*))
                        (write-bytes "pg_hba.conf" (pg::string->bytes hba))
                        (write-bytes "pg_ident.conf" (pg::string->bytes ident)))
                      (ok (pg::authentication-check-backup-policy anchored))
                      (ok (equal hba (pg::authentication-read-text (merge-pathnames "pg_hba.conf" anchored))))
                      (with-open-file (output witness :direction :output :element-type '(unsigned-byte 8))
                        (write-sequence (pg::string->bytes text) output))
                      (let ((path (merge-pathnames name anchored)))
                        (delete-file path)
                        (sb-posix:symlink (namestring witness) (namestring path))
                        (ok (intake-fails-p (lambda () (pg::authentication-check-backup-policy anchored)))))
                      (ok (equalp (pg::string->bytes text) (lineage-test-bytes witness))))))
              (sb-posix:close borrowed)))))))

(deftest reseed-receive-rejects-nonempty-child-replacement-during-construction
    (with-physical-store-directory (stage)
      (with-physical-store-directory (donor)
        (sb-posix:chmod (namestring stage) #o700)
        (sb-posix:chmod (namestring donor) #o700)
        (let* ((global (merge-pathnames "global/" stage))
               (saved (merge-pathnames "new-global/" stage))
               (witness (merge-pathnames "existing-data" donor))
               (replaced nil)
               (entered nil)
               (borrowed (pg::physical-backup-open-directory stage))
               (pg::*physical-image-test-hook*
                 (lambda (point name)
                   (when (and (eq point :after-mkdir) (equal name "global"))
                     (rename-file global saved)
                     (rename-file donor global)
                     (setf replaced t)))))
          (with-open-file (output witness :direction :output) (write-string "preexisting" output))
          (let ((inode (sb-posix:stat-ino (sb-posix:lstat (namestring witness)))))
            (unwind-protect
                 (progn
                   (ok (intake-fails-p
                        (lambda ()
                          (pg::call-with-physical-backup-receive-directory
                           borrowed (lambda (anchored) (declare (ignore anchored)) (setf entered t))))))
                   (ok replaced)
                   (ok (not entered))
                   (ok (sb-posix:fstat borrowed))
                   (let ((moved (merge-pathnames "existing-data" global)))
                     (ok (= inode (sb-posix:stat-ino (sb-posix:lstat (namestring moved)))))
                     (ok (equalp (pg::string->bytes "preexisting") (lineage-test-bytes moved)))
                     (ok (equal '("existing-data") (pg::directory-entry-names global)))))
              (sb-posix:close borrowed)
              (when replaced
                (rename-file global donor)
                (rename-file saved global))))))))

(deftest reseed-receive-requires-exact-descriptor-inventory-before-acceptance
    (dolist (kind '(:valid :root-extra :child-extra :dangling :fifo :mode :hardlink))
      (with-physical-store-directory (stage)
        (with-physical-store-directory (outside)
          (sb-posix:chmod (namestring stage) #o700)
          (let ((borrowed (pg::physical-backup-open-directory stage)))
            (unwind-protect
                 (pg::call-with-physical-backup-receive-directory
                  borrowed
                  (lambda (anchored)
                    (let* ((names '("PG_VERSION" "global/pg_control"))
                           (root-file (merge-pathnames "PG_VERSION" anchored))
                           (child-file (pg::pgdata-resolve-path "global/pg_control" anchored)))
                      (dolist (name names)
                        (pg::call-with-physical-image-output
                         anchored name (lambda (output) (write-byte 42 output))))
                      (ecase kind
                        (:valid nil)
                        (:root-extra (with-open-file (out (merge-pathnames "extra" anchored) :direction :output)
                                       (write-string "extra" out)))
                        (:child-extra
                         (with-open-file (out (pg::pgdata-resolve-path "global/extra" anchored) :direction :output)
                           (write-string "extra" out)))
                        (:dangling (delete-file child-file) (sb-posix:symlink "missing" (namestring child-file)))
                        (:fifo (delete-file child-file) (sb-posix:mkfifo (namestring child-file) #o600))
                        (:mode (sb-posix:chmod (namestring root-file) #o644))
                        (:hardlink (sb-posix:link (namestring root-file)
                                                  (namestring (merge-pathnames "alias" outside)))))
                      (if (eq kind :valid)
                          (ok (pg::physical-image-check-inventory anchored names))
                          (ok (intake-fails-p
                               (lambda () (pg::physical-image-check-inventory anchored names))))))))
              (sb-posix:close borrowed)))))))

(deftest reseed-receive-rejects-mismatched-stage-descriptor-before-network-dispatch
    (with-physical-store-directory (source)
      (with-physical-store-directory (stage)
        (with-physical-store-directory (other)
          (sb-posix:chmod (namestring stage) #o700)
          (sb-posix:chmod (namestring other) #o700)
          (let ((fd (pg::physical-backup-open-directory other))
                (pg::*pgdata* source)
                (old (symbol-function 'pg::physical-libpq-start-copy-out))
                (dispatches 0))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-libpq-start-copy-out)
                         (lambda (&rest arguments)
                           (declare (ignore arguments))
                           (incf dispatches)
                           (error "unexpected network dispatch")))
                   (ok (intake-fails-p
                        (lambda ()
                          (pg::physical-backup-lease-capture-remote
                           stage nil (backup-lease-test-request) :directory-fd fd))))
                   (ok (zerop dispatches))
                   (ok (null (pg::directory-entry-names stage)))
                   (ok (null (pg::directory-entry-names other))))
              (setf (symbol-function 'pg::physical-libpq-start-copy-out) old)
              (sb-posix:close fd)))))))
