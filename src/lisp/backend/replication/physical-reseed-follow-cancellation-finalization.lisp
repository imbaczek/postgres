(in-package #:pg)

(defun physical-reseed-follow-cancellation-cleanup-stamp (owner root-fd check)
  (funcall check)
  (let* ((plan (physical-reseed-install-owner-plan owner))
         (prefix (1- (physical-reseed-install-last-step plan)))
         (names (physical-reseed-install-expected-names
                 plan prefix (physical-reseed-install-extra-entries owner root-fd)))
         (root (physical-adoption-owner-directory owner))
         (roles nil) (paths nil))
    (dolist (role '(:root :incoming :quarantine))
      (let* ((directory (namestring
                         (if (eq role :root) root
                             (pathname (getf (getf plan role) :directory)))))
             (mount (physical-adoption-install-mount-id directory))
             (entries (append (getf names role)
                              (when (eq role :root)
                                (loop for row in (getf plan :new)
                                      when (find #\/ (first row)) collect (first row))))))
        (dolist (binding (physical-reseed-follow-retirement-path-facts directory))
          (pushnew binding paths :test #'equal))
        (push (list directory mount
                    (physical-reseed-follow-retirement-named-row directory "." mount)
                    (mapcar (lambda (name)
                              (physical-reseed-follow-retirement-named-row directory name mount))
                            entries))
              roles)))
    (dolist (name '("base/" "global/" "pg_wal/"))
      (dolist (binding (physical-reseed-follow-retirement-path-facts
                        (namestring (merge-pathnames name root))))
        (pushnew binding paths :test #'equal)))
    (funcall check)
    (list :paths paths :roles (nreverse roles))))

(defun physical-reseed-follow-cancellation-check-cleanup-stamp (stamp)
  (dolist (binding (getf stamp :paths))
    (unless (equal binding (physical-reseed-follow-retirement-path-fact (first binding)))
      (error "cancellation ancestor changed during resource cleanup")))
  (dolist (entry (getf stamp :roles))
    (destructuring-bind (directory mount row entries) entry
      (unless (and (equal row (physical-reseed-follow-retirement-named-row directory "." mount))
                   (every (lambda (file)
                            (equal file (physical-reseed-follow-retirement-named-row
                                         directory (first file) mount)))
                          entries)
                   (equal row (physical-reseed-follow-retirement-named-row directory "." mount)))
        (error "cancellation image changed during resource cleanup"))))
  t)

(defun call-with-physical-reseed-follow-cancellation-finalization (owner function)
  (let ((fd nil) (stream nil) (snapshot nil) (stamp nil) (bytes nil) (mount nil)
                 (complete nil))
    (labels ((source-check ()
               (unless (and (equal (getf snapshot :identity)
                                   (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
                            (equal (rest (getf snapshot :row))
                                   (physical-reseed-follow-retirement-held-file-facts fd mount)))
                 (error "cancellation held journal changed during cleanup"))
               (file-position stream 0)
               (let ((actual (make-array (length bytes) :element-type '(unsigned-byte 8))))
                 (unless (and (= (length actual) (read-sequence actual stream))
                              (eq :eof (read-byte stream nil :eof)) (equalp bytes actual))
                   (error "cancellation held journal bytes changed during cleanup")))))
      (unwind-protect
           (multiple-value-prog1
               (unwind-protect
                    (let ((result
                            (call-with-physical-reseed-follow-held-store
                             owner
                             (lambda (root-fd check)
                               (multiple-value-bind (record final) (funcall function root-fd check)
                                 (setf snapshot (physical-adoption-attempt-value-copy final)
                                       bytes (physical-reseed-follow-image record)
                                       mount (physical-adoption-install-mount-id nil :directory-fd root-fd))
                                 (unless (equal record (getf snapshot :record))
                                   (error "cancellation finalization changed its published record"))
                                 (sb-sys:without-interrupts
                                   (setf fd (physical-image-openat
                                             root-fd "physical-reseed.follow"
                                             (logior sb-posix:o-rdonly sb-posix:o-nofollow
                                                     sb-posix:o-nonblock +postmaster-o-cloexec+) 0))
                                   (when (minusp fd) (setf fd nil) (error "cannot hold cancellation journal"))
                                   (setf stream (sb-sys:make-fd-stream
                                                 fd :input t :element-type '(unsigned-byte 8))))
                                 (source-check)
                                 (setf stamp (physical-reseed-follow-cancellation-cleanup-stamp owner root-fd check))
                                 (physical-reseed-follow-check-snapshot root-fd snapshot)
                                 (source-check)
                                 (values record final))))))
                      (physical-directory-check-owner-common owner)
                      (physical-reseed-follow-cancellation-check-cleanup-stamp stamp)
                      (source-check)
                      result)
                 (if stream (close stream) (when fd (sb-posix:close fd))))
             (physical-directory-check-owner-common owner)
             (physical-reseed-follow-cancellation-check-cleanup-stamp stamp)
             (sb-sys:without-interrupts
               (setf (physical-reseed-follow-owner-uncertain-p owner) nil complete t)))
        (sb-sys:without-interrupts
          (unless complete
            (when snapshot (setf (physical-reseed-follow-owner-uncertain-p owner) t))))))))
