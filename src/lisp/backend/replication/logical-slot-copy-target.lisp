(in-package #:pg)

(defun logical-slot-copy-check-target (owner context)
  (physical-directory-check-owner owner)
  (physical-image-check-context context)
  (let ((stat (physical-image-check-directory (physical-image-context-root-fd context))))
    (unless (and (not (physical-image-read-context-p context))
                 (= (physical-adoption-owner-device owner) (sb-posix:stat-dev stat))
                 (= (physical-adoption-owner-inode owner) (sb-posix:stat-ino stat)))
      (error "logical slot copy target differs from its held root")))
  (physical-image-check-children context)
  t)

(defun logical-slot-copy-mark-in-progress (owner context)
  (logical-slot-copy-check-target owner context)
  (let* ((copy-id (logical-slot-generation-token (logical-slot-new-generation)))
         (bytes (string->bytes (format nil "PGLOGICALCOPY ~a~%" copy-id)))
         (*physical-image-context* context))
    (call-with-physical-image-output
     (physical-image-context-root-path context) "logical-slot-copy.in-progress"
     (lambda (stream) (write-sequence bytes stream)))
    copy-id))

(defun call-with-logical-slot-copy-target-descriptors (owner function)
  (call-with-physical-directory-root
   owner
   (lambda (borrowed-fd root)
     (declare (ignore root))
     (let ((context (%make-physical-image-context)))
       (sb-sys:without-interrupts
         (unwind-protect
              (progn
                (setf (physical-image-context-root-fd context) (sb-posix:fcntl borrowed-fd 1030 0))
                (setf (physical-image-context-root-path context)
                      (physical-image-fd-path (physical-image-context-root-fd context)))
                (logical-slot-copy-check-target owner context)
                (unless (equal '(".pg_lisp.lock")
                               (directory-entry-names (physical-image-context-root-path context)
                                                      :follow-symlinks t :maximum-entries 2))
                  (error "logical slot copy target is not empty under ownership"))
                (sb-sys:with-local-interrupts
                  (let ((copy-id (logical-slot-copy-mark-in-progress owner context)))
                    (dolist (name '("base" "global" "pg_wal"))
                      (physical-image-open-child context name))
                    (multiple-value-prog1 (funcall function owner context copy-id)
                      (logical-slot-copy-check-target owner context)
                      (dolist (entry (physical-image-context-children context))
                        (sb-posix:fsync (cdr entry)))
                      (sb-posix:fsync (physical-image-context-root-fd context))))))
           (setf (physical-image-context-active context) nil)
           (dolist (entry (physical-image-context-children context)) (sb-posix:close (cdr entry)))
           (when (physical-image-context-root-fd context)
             (sb-posix:close (physical-image-context-root-fd context)))))))))

(defun call-with-logical-slot-copy-target (directory function)
  (let ((target (physical-bootstrap-target directory)) (fd nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf fd (physical-backup-open-directory target))
             (let ((initial (physical-image-check-directory fd)))
               (sb-sys:with-local-interrupts
                 (let ((*postmaster-pid-directory* (physical-image-fd-path fd)))
                   (with-open-stream (lock (postmaster-open-data-lock))
                     (fsync-stream lock)
                     (sb-posix:fsync fd)))
                 (call-with-physical-adoption-owner
                  target
                  (lambda (owner)
                    (unless (and (= (sb-posix:stat-dev initial) (physical-adoption-owner-device owner))
                                 (= (sb-posix:stat-ino initial) (physical-adoption-owner-inode owner)))
                      (error "logical slot copy target changed before ownership"))
                    (call-with-logical-slot-copy-target-descriptors owner function))))))
        (when fd (sb-posix:close fd))))))
