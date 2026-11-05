(in-package #:pg)

(defun physical-reseed-follow-startup-projection (descriptors state)
  (flet ((file-row (entry)
           (let ((roles (loop for (role . fd) in descriptors
                              when (= fd (second entry)) collect role)))
             (unless (= 1 (length roles))
               (error "startup evidence requires one parent directory role"))
             (list (first entry) (first roles) (third entry) (fourth entry)))))
    (let* ((layout (getf state :layout))
           (projection
             (append
              (loop for key in '(:prefix :snapshot :install :attempt :temporary :temporary-file)
                    append (list key (getf state key)))
              (list :layout
                    (list :new (getf layout :new)
                          :images (mapcar #'file-row (getf layout :images))
                          :active (file-row (getf layout :active))
                          :old (getf layout :old)
                          :directories
                          (loop for (role . row) in (getf layout :directories)
                                collect (cons role (if (eq role :root) (subseq row 0 6) row)))
                          :names
                          (loop for (role names) on (getf layout :names) by #'cddr
                                append (list role
                                             (if (eq role :root)
                                                 (remove "postmaster.pid" names :test #'equal)
                                                 names))))))))
      (physical-adoption-attempt-value-copy projection))))

(defun call-with-physical-reseed-follow-startup-root (lock function)
  (unless (and lock (open-stream-p lock) (null *postmaster-data-lock*))
    (error "reseed startup observation requires the unpublished local lock"))
  (let* ((directory (uiop:ensure-directory-pathname (truename *pgdata*)))
         (root-stat (sb-posix:lstat (namestring directory)))
         (lock-fd (sb-sys:fd-stream-fd lock))
         (lock-stat (sb-posix:fstat lock-fd))
         (lock-facts (physical-reseed-file-facts lock-stat))
         (owner (make-physical-adoption-owner
                 :directory directory :device (sb-posix:stat-dev root-stat)
                 :inode (sb-posix:stat-ino root-stat)
                 :lock lock :lock-device (sb-posix:stat-dev lock-stat)
                 :lock-inode (sb-posix:stat-ino lock-stat)
                 :thread sb-thread:*current-thread* :active-p t)))
    (unwind-protect
         (flet ((check-lock ()
                  (physical-directory-check-owner-common owner)
                  (physical-adoption-check-startup-quiescence)
                  (unless (and (null *postmaster-data-lock*)
                               (equal directory *postmaster-pid-directory*)
                               (equal directory (uiop:ensure-directory-pathname (truename *pgdata*)))
                               (= lock-fd (sb-sys:fd-stream-fd lock))
                               (equal lock-facts (physical-reseed-file-facts (sb-posix:fstat lock-fd)))
                               (equal lock-facts
                                      (physical-reseed-file-facts
                                       (sb-posix:lstat (namestring (merge-pathnames ".pg_lisp.lock" directory)))))
                               (zerop (physical-store-flock lock-fd 6)))
                    (error "reseed startup observation lost its local lock context"))))
           (check-lock)
           (call-with-physical-directory-root
            owner
            (lambda (root path)
              (declare (ignore path))
              (let ((mount (physical-adoption-install-mount-id nil :directory-fd root)))
                (flet ((verify ()
                         (check-lock)
                         (physical-image-check-target directory root)
                         (unless (= mount
                                    (physical-adoption-install-mount-id directory)
                                    (physical-adoption-install-mount-id nil :directory-fd root)
                                    (physical-adoption-install-mount-id nil :directory-fd lock-fd))
                           (error "reseed startup root or lock crossed mounts"))
                         t))
                  (verify)
                  (multiple-value-prog1 (funcall function root #'verify)
                    (verify)))))))
      (setf (physical-adoption-owner-active-p owner) nil))))

(defun physical-reseed-follow-startup-observation (lock phase &optional expected)
  (ecase phase
    (:before-pid
     (when expected (error "initial startup observation cannot reuse boundary evidence")))
    (:owned-pid
     (unless expected (error "owned PID observation requires pre-PID evidence"))))
  (call-with-physical-reseed-follow-startup-root
   lock
   (lambda (root check)
     (let* ((present (physical-adoption-path-present-p
                      (merge-pathnames "physical-reseed.follow" (physical-image-fd-path root))))
            (branch (if present :retirement :ordinary)))
       (when (and expected (not (eq branch (getf expected :branch))))
         (error "startup branch changed across PID creation"))
       (if present
           (let ((snapshot (physical-reseed-follow-snapshot-at root))
                 (reader (ecase phase
                           (:before-pid #'call-with-physical-reseed-follow-retirement-before-pid-observation)
                           (:owned-pid #'call-with-physical-reseed-follow-retirement-owned-pid-observation))))
             (funcall
              reader root snapshot check
              (lambda (descriptors state verify)
                (let ((evidence (physical-reseed-follow-startup-projection descriptors state)))
                  (when (and expected (not (equal evidence (getf expected :evidence))))
                    (error "retirement evidence changed across PID creation"))
                  (funcall verify)
                  (list :branch :retirement :evidence evidence)))))
           (progn
             (ecase phase
               (:before-pid (physical-reseed-check-before-pid))
               (:owned-pid (physical-adoption-startup-preflight)))
             (funcall check)
             (physical-reseed-check-before-pid)
             (list :branch :ordinary)))))))
