(in-package #:pg)

(defvar *logical-slot-continuation-fenced-roots*
  (make-hash-table :test #'equal :synchronized t))

(defun logical-slot-continuation-fd-fenced-p (fd)
  "Remember continuation evidence for the held host, including through bind aliases."
  (let* ((facts (physical-image-file-facts fd))
         (device (first facts))
         (inode (second facts))
         (key (list device inode)))
    (sb-ext:with-locked-hash-table (*logical-slot-continuation-fenced-roots*)
      (or (gethash key *logical-slot-continuation-fenced-roots*)
          (when (or (slot-state-lstat (slot-state-fd-path fd "logical-slot-copy.source-proof"))
                    (slot-state-lstat (slot-state-fd-path fd "pg_logical_continuation"))
                    ;; Adopt sticky device/inode/mount keys from a live reload.
                    (loop for old being the hash-keys of *logical-slot-continuation-fenced-roots*
                            thereis (and (eql device (first old)) (eql inode (second old)))))
            (setf (gethash key *logical-slot-continuation-fenced-roots*) t))))))

(defun logical-slot-continuation-gc-fenced-p (directory)
  "Observe continuation evidence on one held host directory."
  (let ((root (uiop:ensure-directory-pathname directory))
        (fd nil))
    (when (slot-state-lstat root)
      (unwind-protect
           (progn
             (sb-sys:without-interrupts
               (setf fd (sb-posix:open
                         (let ((name (namestring (truename root))))
                           (if (string= name "/") name (string-right-trim "/" name)))
                         (logior sb-posix:o-rdonly sb-posix:o-directory
                                 sb-posix:o-nofollow +postmaster-o-cloexec+))))
             (logical-slot-continuation-fd-fenced-p fd))
        (when fd (sb-posix:close fd))))))
