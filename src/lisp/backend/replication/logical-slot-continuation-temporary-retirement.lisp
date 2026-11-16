(in-package #:pg)

(defvar *logical-slot-continuation-retirement-test-hook* nil)

(defun logical-slot-continuation-retirement-point (point purpose)
  (when *logical-slot-continuation-retirement-test-hook*
    (funcall *logical-slot-continuation-retirement-test-hook* point purpose)))

(defun logical-slot-continuation-retirement-check-file (directory name fd facts image removed)
  (let ((named (slot-state-lstat (slot-state-fd-path directory name))))
    (unless (if removed (null named) (if facts named (null named)))
      (error "continuation retirement temporary binding changed"))
    (when facts
      (unless fd (error "continuation retirement lost its held file"))
      (let ((now (physical-image-file-facts fd)))
        (unless (if removed
                    (and (zerop (fourth now))
                         (loop for old in facts for current in now for index from 0
                               always (or (member index '(3 7)) (eql old current))))
                    (and (equal facts now)
                         (equal facts (physical-image-file-facts directory name))))
          (error "continuation retirement temporary inode changed"))
        (unless (equalp image (logical-slot-copy-import-control-read-fd fd (length image)))
          (error "continuation retirement temporary bytes changed"))
        (unless (equal now (physical-image-file-facts fd))
          (error "continuation retirement temporary changed while reading"))))))

(defun logical-slot-continuation-retirement-close (fd facts complete)
  (declare (notinline sb-posix:close))
  (when fd
    (handler-case
        (progn
          (when (and facts
                     (not (equal (list (first facts) (second facts) (ninth facts))
                                 (logical-slot-copy-install-file-identity
                                  (physical-image-file-facts fd)))))
            (error "continuation retirement descriptor was reused"))
          (sb-posix:close fd))
      (error (condition)
        (if complete (error condition)
            (report-query-temp-cleanup-failure "continuation retirement" condition))))))

(defun call-with-logical-slot-continuation-temporary-retirement
    (directory purpose expected guard function)
  (declare (notinline logical-slot-copy-unlinkat sb-posix:fsync))
  (multiple-value-bind (name maximum)
      (ecase purpose
        (:slot-state (values "state.tmp" +slot-state-maximum-bytes+))
        (:selector (values "selector.tmp" (logical-slot-continuation-prepared-maximum-bytes))))
    (unless (and (functionp guard) (functionp function)
                 (or (null expected)
                     (and (listp expected) (= 2 (length expected))
                          (listp (first expected))
                          (typep (second expected) '(vector (unsigned-byte 8)))
                          (<= (length (second expected)) maximum))))
      (error "invalid continuation retirement inputs"))
    (let* ((facts (when expected (copy-list (first expected))))
           (image (when expected (copy-seq (second expected))))
           (parent-facts (physical-image-file-facts directory))
           (parent (logical-slot-copy-source-proof-directory-identity parent-facts))
           (fd nil) (opened-facts nil) (removed nil) (synced nil)
           (active t) (failed nil) (busy nil) (complete nil)
           (thread sb-thread:*current-thread*))
      (unless (and (sb-posix:s-isdir (third parent-facts))
                   (= #o700 (logand #o7777 (third parent-facts)))
                   (= (fifth parent-facts) (sb-posix:geteuid)))
        (error "continuation retirement requires a private directory"))
      (labels ((check ()
                 (funcall guard removed)
                 (unless (equal parent
                                (logical-slot-copy-source-proof-directory-identity
                                 (physical-image-file-facts directory)))
                   (error "continuation retirement directory changed"))
                 (logical-slot-continuation-retirement-check-file
                  directory name fd facts image removed)
                 (funcall guard removed))
               (operation (operation)
                 (unless (and active (eq thread sb-thread:*current-thread*))
                   (error "continuation retirement ended or changed thread"))
                 (sb-sys:without-interrupts
                   (when (or failed busy)
                     (setf failed t)
                     (error "continuation retirement failed or is busy"))
                   (let ((done nil))
                     (setf busy t)
                     (unwind-protect
                          (multiple-value-prog1
                              (sb-sys:with-local-interrupts
                                (check)
                                (ecase operation
                                  (:check t)
                                  (:retire
                                   (when removed (error "continuation temporary was already retired"))
                                   (logical-slot-continuation-retirement-point :before-unlink purpose)
                                   (check)
                                   (sb-sys:without-interrupts
                                     (when facts
                                       (unless (zerop (logical-slot-copy-unlinkat directory name 0))
                                         (error "cannot retire continuation temporary")))
                                     (setf removed t))
                                   (logical-slot-continuation-retirement-point :after-unlink purpose))
                                  (:sync
                                   (unless (and removed (not synced))
                                     (error "continuation retirement cannot sync in this phase"))
                                   (logical-slot-continuation-retirement-point :before-directory-sync purpose)
                                   (check)
                                   (sb-posix:fsync directory)
                                   (setf synced t)
                                   (logical-slot-continuation-retirement-point :after-directory-sync purpose))
                                  (:receipt
                                   (unless synced (error "continuation retirement has no durable receipt"))))
                                (check)
                                (when failed (error "continuation retirement failed during its operation"))
                                (if (eq operation :receipt)
                                    (list name (copy-list facts) (when image (copy-seq image)) removed synced)
                                    t))
                            (setf done t))
                       (setf busy nil)
                       (unless done (setf failed t)))))))
        (sb-sys:without-interrupts
          (unwind-protect
               (sb-sys:with-local-interrupts
                 (funcall guard nil)
                 (when facts
                   (sb-sys:without-interrupts
                     (setf fd (physical-image-openat
                               directory name
                               (logior sb-posix:o-rdonly sb-posix:o-nofollow sb-posix:o-nonblock
                                       +postmaster-o-cloexec+) 0))
                     (when (minusp fd) (setf fd nil) (error "cannot open continuation temporary"))
                     (setf opened-facts (physical-image-file-facts fd)))
                   (unless (and (equal facts opened-facts)
                                (logical-slot-copy-import-control-private-facts-p
                                 facts (physical-image-file-facts directory)))
                     (error "continuation temporary lost its private inode")))
                 (check)
                 (logical-slot-continuation-retirement-point :after-open purpose)
                 (multiple-value-prog1 (funcall function #'operation)
                   (operation :check)
                   (when (and removed (not synced))
                     (error "continuation retirement returned before directory synchronization"))
                   (setf complete t)))
            (setf active nil)
            (logical-slot-continuation-retirement-close fd opened-facts complete)))))))
