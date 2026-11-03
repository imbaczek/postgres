(in-package #:pg)

;; Durable publication of the initial install intent.  The held descriptor
;; identifies the actual journal generation throughout creation and promotion.
;;
;; Match the retained descriptor to its name, generation, and root mount.
(defun physical-reseed-install-check-held-journal (root-fd fd name generation)
  (physical-reseed-install-check-journal-name name)
  (let* ((facts (physical-reseed-install-file-identity (sb-posix:fstat fd) t))
         (path (merge-pathnames name (physical-image-fd-path root-fd))))
    (unless (and (equal generation (subseq facts 0 2))
                 (equal facts (physical-reseed-install-file-identity (sb-posix:lstat (namestring path)) t))
                 (= (physical-adoption-install-mount-id nil :directory-fd root-fd)
                    (physical-adoption-install-mount-id nil :directory-fd fd)))
      (error "reseed retained journal no longer matches its named generation"))))

;; Restrict publication checks to the two fixed journal names.
(defun physical-reseed-install-check-journal-name (name)
  (unless (member name '("physical-reseed.install.tmp" "physical-reseed.install") :test #'equal)
    (error "unexpected reseed publication journal name")))

;; Sync and decode the supplied name while retaining the original descriptor.
;; Require complete bytes, the exact initial plan, and zero progress.
(defun physical-reseed-install-check-initial-readback (root-fd fd name generation plan)
  (physical-reseed-install-check-held-journal root-fd fd name generation)
  (let ((state (physical-reseed-install-read-at root-fd name :stabilize t)))
    (unless (and (equal plan (getf state :plan)) (zerop (getf state :step)) (not (getf state :partial)))
      (error "reseed initial journal readback differs"))
    (physical-reseed-install-check-held-journal root-fd fd name generation)))

;; Publish one initial intent with exclusive creation and NOREPLACE promotion.
;; ROOT-FD is borrowed.  Retain the new journal descriptor and derive its
;; device and inode before encoding SEED.  CHECK receives NIL before creation.
;; After creation it receives the fixed entry name only after the held journal
;; descriptor is matched to that name.  CHECK must signal if its ownership or
;; namespace proof fails, including before excluding that entry from a scan.
;;
;; Sync and read back before rename and after directory sync, retaining the
;; descriptor and repeating checks at each boundary.  Failure leaves any
;; created intent and existing startup fences for reconciliation.
(defun physical-reseed-install-publish-journal (root-fd seed check)
  (let ((fd nil) (stream nil) (generation nil)
                 (name "physical-reseed.install.tmp") (plan nil))
    (labels ((path () (merge-pathnames name (physical-image-fd-path root-fd)))
             (verify ()
               (when fd (physical-reseed-install-check-held-journal root-fd fd name generation))
               (funcall check (when fd name)))
             (point (event)
               (physical-reseed-install-test-point event (path))
               (verify)))
      (unwind-protect
           (progn
             (verify)
             (point :before-install-create)
             (sb-sys:without-interrupts
               (setf fd (physical-image-openat
                         root-fd name
                         (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-excl
                                 sb-posix:o-nofollow sb-posix:o-nonblock +postmaster-o-cloexec+)
                         #o600)))
             (when (minusp fd)
               (setf fd nil)
               (error "cannot create exclusive reseed journal: errno ~d" (sb-alien:get-errno)))
             (sb-posix:fchmod fd #o600)
             (setf generation (subseq (physical-reseed-install-file-identity (sb-posix:fstat fd) t) 0 2))
             (let ((candidate (copy-list seed)))
               (setf (getf candidate :journal)
                     (list :device (first generation) :inode (second generation)))
               (setf plan (physical-reseed-install-copy candidate)))
             (point :after-install-create)
             (sb-sys:without-interrupts
               (setf stream (sb-sys:make-fd-stream fd :input t :output t :element-type '(unsigned-byte 8))))
             (write-sequence (physical-reseed-install-image plan 0) stream)
             (finish-output stream)
             (point :after-install-write)
             (fsync-stream stream)
             (point :after-install-file-fsync)
             (physical-reseed-install-check-initial-readback root-fd fd name generation plan)
             (point :before-install-rename)
             (physical-reseed-install-check-initial-readback root-fd fd name generation plan)
             (verify)
             (unless (zerop (physical-adoption-renameat2
                             root-fd name root-fd "physical-reseed.install" +physical-adoption-rename-noreplace+))
               (error "cannot promote exclusive reseed journal: errno ~d" (sb-alien:get-errno)))
             (setf name "physical-reseed.install")
             (point :after-install-rename)
             (fsync-stream stream)
             (sb-posix:fsync root-fd)
             (point :after-install-directory-fsync)
             (point :before-install-readback)
             (physical-reseed-install-check-initial-readback root-fd fd name generation plan)
             (verify)
             (point :after-install-readback)
             (physical-reseed-install-check-initial-readback root-fd fd name generation plan)
             (verify)
             t)
        (if stream (close stream) (when fd (sb-posix:close fd)))))))
