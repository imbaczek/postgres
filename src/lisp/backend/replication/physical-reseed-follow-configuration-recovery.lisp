(in-package #:pg)

(defun physical-reseed-follow-installation-recover-temporary (context check)
  (let* ((owner (physical-reseed-follow-installation-context-owner context))
         (root (physical-reseed-follow-configuration-role-fd
                (physical-reseed-follow-installation-context-descriptors context) :root))
         (previous (physical-reseed-follow-installation-context-snapshot context))
         (old (getf (getf previous :record) :configuration))
         (fd nil))
    (physical-reseed-follow-check-active-attempt owner)
    (multiple-value-bind (value identity) (physical-reseed-follow-read-at root "physical-reseed.follow.tmp")
      (let* ((candidate (physical-reseed-follow-installation-candidate context value))
             (*physical-reseed-follow-installation-candidate* candidate))
        (when (and (eq :installing (getf old :phase))
                   (eq :installing (getf (getf candidate :configuration) :phase)))
          (physical-reseed-follow-installation-next-move context check))
        (funcall check)
        (setf (physical-reseed-follow-owner-uncertain-p owner) t)
        (unwind-protect
             (progn
               (sb-sys:without-interrupts
                 (setf fd (physical-image-openat
                           root "physical-reseed.follow.tmp"
                           (logior sb-posix:o-rdwr sb-posix:o-nofollow sb-posix:o-nonblock
                                   +postmaster-o-cloexec+) 0)))
               (when (minusp fd) (setf fd nil) (error "cannot open installation recovery temporary"))
               (unless (equal identity (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
                 (error "installation recovery temporary changed before ownership"))
               (physical-reseed-follow-promote-intent
                owner root fd (subseq identity 0 5) candidate previous check))
          (when fd (sb-posix:close fd)))))))

(defun physical-reseed-follow-installation-recover-stable (context check)
  (let* ((owner (physical-reseed-follow-installation-context-owner context))
         (record (physical-reseed-follow-copy (physical-reseed-follow-installation-context-record context)))
         (configuration (getf record :configuration))
         (snapshot (physical-reseed-follow-installation-context-snapshot context))
         (root (physical-reseed-follow-configuration-role-fd
                (physical-reseed-follow-installation-context-descriptors context) :root)))
    (if (and (eq :installing (getf configuration :phase))
             (= (1+ (getf configuration :step)) (physical-reseed-follow-installation-context-prefix context)))
        (progn
          (physical-reseed-follow-check-active-attempt owner)
          (setf (getf configuration :step) (physical-reseed-follow-installation-next-move context check))
          (physical-reseed-follow-installation-publish context record check))
        (progn
          (funcall check)
          (setf (physical-reseed-follow-owner-uncertain-p owner) t)
          (physical-reseed-follow-read-at root "physical-reseed.follow" :stabilize t)
          (physical-reseed-follow-check-snapshot root snapshot)
          (funcall check)
          (values record snapshot)))))

(defun physical-reseed-follow-recover-installed-configuration (owner)
  (call-with-physical-reseed-follow-installation-operation
   owner
   (lambda (root check)
     (let ((context (physical-reseed-follow-installation-context owner)))
       (funcall check)
       (if (physical-reseed-follow-entry root "physical-reseed.follow.tmp" t)
           (physical-reseed-follow-installation-recover-temporary context check)
           (physical-reseed-follow-installation-recover-stable context check))))))

(defmethod physical-reseed-follow-configuration-recovery-dispatch
    ((owner physical-reseed-follow-configuration-owner))
  (let ((installed
          (call-with-physical-directory-root
           owner
           (lambda (fd directory)
             (declare (ignore directory))
             (let* ((record (physical-reseed-follow-read-at fd "physical-reseed.follow"))
                    (phase (getf (getf record :configuration) :phase)))
               (or (member phase '(:installing :inert))
                   (and (eq :staged phase)
                        (physical-reseed-follow-entry fd "physical-reseed.follow.tmp" t)
                        (member (getf (getf (physical-reseed-follow-read-at
                                             fd "physical-reseed.follow.tmp") :configuration) :phase)
                                '(:installing :inert)))))))))
    (if installed
        (physical-reseed-follow-recover-installed-configuration owner)
        (call-next-method))))
