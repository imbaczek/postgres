(in-package #:pg)

(defun physical-reseed-follow-recover-settlement-temporary (context check)
  (let* ((owner (physical-reseed-follow-installation-context-owner context))
         (root (physical-reseed-follow-configuration-role-fd
                (physical-reseed-follow-installation-context-descriptors context) :root))
         (previous (physical-reseed-follow-installation-context-snapshot context))
         (fd nil))
    (physical-reseed-follow-check-active-attempt owner)
    (multiple-value-bind (value identity)
        (physical-reseed-follow-read-at root "physical-reseed.follow.tmp")
      (let* ((candidate (physical-reseed-follow-pending-settlement-candidate context value))
             (*physical-reseed-follow-settlement-candidate* candidate))
        (funcall check)
        (setf (physical-reseed-follow-owner-uncertain-p owner) t)
        (unwind-protect
             (progn
               (sb-sys:without-interrupts
                 (setf fd (physical-image-openat
                           root "physical-reseed.follow.tmp"
                           (logior sb-posix:o-rdwr sb-posix:o-nofollow sb-posix:o-nonblock
                                   +postmaster-o-cloexec+) 0)))
               (when (minusp fd) (setf fd nil) (error "cannot open settlement recovery temporary"))
               (unless (equal identity (physical-reseed-follow-file-identity (sb-posix:fstat fd)))
                 (error "settlement temporary changed before ownership"))
               (physical-reseed-follow-promote-intent
                owner root fd (subseq identity 0 5) candidate previous check))
          (when fd (sb-posix:close fd)))))))

(defun physical-reseed-follow-recover-settlement (owner)
  (call-with-physical-reseed-follow-installation-operation
   owner
   (lambda (root check)
     (let* ((context (physical-reseed-follow-installation-context owner))
            (record (physical-reseed-follow-installation-context-record context)))
       (funcall check)
       (unless (eq :inert (getf (getf record :configuration) :phase))
         (error "settlement recovery requires inert installed configuration"))
       (if (physical-reseed-follow-entry root "physical-reseed.follow.tmp" t)
           (physical-reseed-follow-recover-settlement-temporary context check)
           (physical-reseed-follow-installation-recover-stable context check))))))

(defmethod physical-reseed-follow-configuration-recovery-dispatch :around
    ((owner physical-reseed-follow-configuration-owner))
  (let ((inert
          (call-with-physical-directory-root
           owner
           (lambda (root directory)
             (declare (ignore directory))
             (eq :inert
                 (getf (getf (physical-reseed-follow-read-at
                              root "physical-reseed.follow") :configuration) :phase))))))
    (if inert
        (physical-reseed-follow-recover-settlement owner)
        (call-next-method))))
