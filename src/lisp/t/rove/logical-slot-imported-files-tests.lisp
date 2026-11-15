(in-package #:pg)

(rove:deftest imported-held-file-checks-canonical-bytes-and-named-inode
    (dolist (failure '(nil :bytes :name :size))
      (let ((rejected nil) (checked nil))
        (call-with-imported-scan-postmaster-test
         (lambda (scan source)
           (let* ((path (merge-pathnames "imported-file-probe" *pgdata*))
                  (moved (merge-pathnames "imported-file-probe.saved" *pgdata*))
                  (bytes (string->bytes "original image")))
             (labels ((write-image ()
                        (with-open-file (out path :direction :output :element-type '(unsigned-byte 8)
                                                  :if-exists :supersede :if-does-not-exist :create)
                          (write-sequence bytes out))
                        (sb-posix:chmod (namestring path) #o600)))
               (write-image)
               (unwind-protect
                    (handler-case
                        (call-with-logical-slot-imported-scan-owner
                         scan source
                         (lambda (owner)
                           (call-with-physical-directory-root
                            owner
                            (lambda (root root-path)
                              (declare (ignore root-path))
                              (call-with-logical-slot-copy-source-proof-fd
                               root "imported-file-probe" nil
                               (lambda (fd facts)
                                 (rove:ok (logical-slot-imported-check-held-file root "imported-file-probe" fd facts bytes))
                                 (case failure
                                   (:bytes
                                    (with-open-file (out path :direction :output :element-type '(unsigned-byte 8)
                                                              :if-exists :overwrite)
                                      (write-byte 0 out)))
                                   (:name (rename-file path moved) (write-image))
                                   (:size
                                    (with-open-file (out path :direction :output :element-type '(unsigned-byte 8)
                                                              :if-exists :append)
                                      (write-byte 0 out))))
                                 (if failure
                                     (rove:ok (rove:signals
                                               (logical-slot-imported-check-held-file
                                                root "imported-file-probe" fd facts bytes) 'error))
                                     (rove:ok (logical-slot-imported-check-held-file
                                               root "imported-file-probe" fd facts bytes)))
                                 (setf checked t)))))))
                      (error () (setf rejected t)))
                 (when (probe-file path) (delete-file path))
                 (when (probe-file moved) (delete-file moved)))))))
        (rove:ok checked)
        (rove:ok (eq (not (null failure)) rejected)))))

(rove:deftest imported-proof-files-refuse-missing-or-invalid-proof-before-callback
    (dolist (present '(nil t))
      (call-with-imported-scan-postmaster-test
       (lambda (scan source)
         (let ((path (merge-pathnames "logical-slot-copy.source-proof" *pgdata*))
               (entered nil))
           (when present
             (with-open-file (out path :direction :output :element-type '(unsigned-byte 8)
                                       :if-exists :error :if-does-not-exist :create)
               (write-sequence (string->bytes "invalid source proof") out))
             (sb-posix:chmod (namestring path) #o600))
           (unwind-protect
                (rove:ok
                 (rove:signals
                  (call-with-logical-slot-imported-scan-owner
                   scan source
                   (lambda (owner)
                     (call-with-physical-directory-root
                      owner
                      (lambda (root root-path)
                        (declare (ignore root-path))
                        (call-with-physical-image-owned-context
                         root t
                         (lambda (context)
                           (rove:ok
                            (rove:signals
                             (call-with-logical-slot-imported-proof-files
                              owner context
                              (lambda (facts check anchors)
                                (declare (ignore facts check anchors))
                                (setf entered t)))
                             'error))))))
                     (setf (physical-adoption-owner-active-p owner) t)
                     (sb-thread:with-mutex (*wal-lock*)
                       (rove:ok (rove:signals
                                 (logical-slot-imported-scan-check-under-lock owner) 'error)))))
                  'error))
             (when (probe-file path) (delete-file path)))
           (rove:ok (not entered)))))))

(rove:deftest imported-install-directory-requires-exact-terminal-pair
    (call-with-imported-scan-postmaster-test
     (lambda (scan source)
       (declare (ignore scan source))
       (let* ((directory (merge-pathnames "imported-install-probe/" *pgdata*))
              (active (merge-pathnames "active" directory))
              (journal (merge-pathnames "journal" directory))
              (extra (merge-pathnames "pending" directory)))
         (ensure-directories-exist active)
         (dolist (path (list active journal))
           (with-open-file (out path :direction :output :if-exists :error)
             (write-string "test" out)))
         (unwind-protect
              (let ((fd (sb-posix:open (namestring directory) sb-posix:o-rdonly)))
                (unwind-protect
                     (progn
                       (rove:ok (logical-slot-imported-check-install-directory fd))
                       (with-open-file (out extra :direction :output :if-exists :error)
                         (write-string "unfinished" out))
                       (rove:ok (rove:signals (logical-slot-imported-check-install-directory fd) 'error))
                       (delete-file extra)
                       (delete-file active)
                       (rove:ok (rove:signals (logical-slot-imported-check-install-directory fd) 'error)))
                  (sb-posix:close fd)))
           (dolist (path (list active journal extra))
             (when (probe-file path) (delete-file path)))
           (sb-posix:rmdir (namestring directory)))))))

(rove:deftest imported-light-anchors-reject-file-change-under-wal-lock
    (dolist (failure '(:bytes :name))
      (call-with-imported-scan-postmaster-test
       (lambda (scan source)
         (declare (ignore scan source))
         (let ((path (merge-pathnames "imported-anchor-probe" *pgdata*))
               (saved (merge-pathnames "imported-anchor-probe.saved" *pgdata*))
               (bytes (string->bytes "original")))
           (flet ((write-image ()
                    (with-open-file (out path :direction :output :element-type '(unsigned-byte 8)
                                              :if-exists :error)
                      (write-sequence bytes out))))
             (write-image)
             (unwind-protect
                  (let ((root (sb-posix:open (namestring *pgdata*) sb-posix:o-rdonly))
                        (fd (sb-posix:open (namestring path) sb-posix:o-rdonly)))
                    (unwind-protect
                         (let* ((facts (physical-image-file-facts fd))
                                (root-facts (physical-image-file-facts root))
                                (held (list (list root "imported-anchor-probe" fd facts bytes))))
                           (sb-thread:with-mutex (*wal-lock*)
                             (rove:ok (logical-slot-imported-check-file-anchors
                                       root (namestring *pgdata*) root-facts held)))
                           (ecase failure
                             (:bytes
                              (with-open-file (out path :direction :output :element-type '(unsigned-byte 8)
                                                        :if-exists :overwrite)
                                (write-byte 0 out)))
                             (:name (rename-file path saved) (write-image)))
                           (sb-thread:with-mutex (*wal-lock*)
                             (rove:ok (rove:signals
                                       (logical-slot-imported-check-file-anchors
                                        root (namestring *pgdata*) root-facts held) 'error))))
                      (sb-posix:close fd)
                      (sb-posix:close root)))
               (dolist (file (list path saved))
                 (when (probe-file file) (delete-file file))))))))))
