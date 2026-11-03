(in-package #:pg)

(defun physical-reseed-attempt-path (&optional (directory *pgdata*))
  (merge-pathnames "physical-reseed.attempt" directory))

(defun physical-reseed-check-before-pid ()
  (physical-adoption-require-absent
   (physical-reseed-attempt-path)
   "pending offline reseed requires reconciliation before PID changes"))

(defstruct (physical-reseed-owner (:include physical-adoption-owner))
  old-pid)

(defmethod physical-adoption-check-owner ((owner physical-reseed-owner))
  (declare (ignore owner))
  (error "stopped reseed ownership does not authorize clean adoption"))

(defun physical-reseed-check-owner (owner)
  (unless (physical-reseed-owner-p owner)
    (error "stopped reseed ownership is required"))
  (physical-directory-check-owner owner))

(defun physical-reseed-process-state (pid &optional (probe #'sb-posix:kill))
  (unless (typep pid '(integer 1 2147483647))
    (error "invalid process ID for stopped reseed ownership"))
  (handler-case
      (progn (funcall probe pid 0) :alive)
    (sb-posix:syscall-error (condition)
      (if (= sb-posix:esrch (sb-posix:syscall-errno condition)) :dead :unknown))))

(defun physical-reseed-pid-identity (stat)
  (list (sb-posix:stat-dev stat) (sb-posix:stat-ino stat)
        (sb-posix:stat-mode stat) (sb-posix:stat-nlink stat)
        (sb-posix:stat-uid stat) (sb-posix:stat-size stat)
        (sb-posix:stat-mtime stat) (sb-posix:stat-ctime stat)))

(defun physical-reseed-read-pid (directory)
  (let ((path (merge-pathnames "postmaster.pid" directory)))
    (when (physical-adoption-path-present-p path)
      (let* ((stat (sb-posix:lstat (namestring path)))
             (before (physical-reseed-pid-identity stat)))
        (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
                     (= 1 (sb-posix:stat-nlink stat))
                     (= (sb-posix:geteuid) (sb-posix:stat-uid stat))
                     (<= 1 (sb-posix:stat-size stat) 16384))
          (error "reseed PID metadata is not a bounded owned single-link regular file"))
        (multiple-value-bind (stream size) (open-bounded-regular-file path 16384)
          (unwind-protect
               (let ((bytes (make-array size :element-type '(unsigned-byte 8))))
                 (flet ((descriptor-identity ()
                          (physical-reseed-pid-identity
                           (sb-posix:fstat (sb-sys:fd-stream-fd stream)))))
                   (unless (equal before (descriptor-identity))
                     (error "reseed PID metadata changed before reading"))
                   (unless (and (= size (read-sequence bytes stream))
                                (equal before (descriptor-identity))
                                (equal before (physical-reseed-pid-identity
                                               (sb-posix:lstat (namestring path)))))
                     (error "reseed PID metadata changed while reading")))
                 (let* ((line (bytes->string (subseq bytes 0 (position 10 bytes))))
                        (pid (ignore-errors (parse-integer line))))
                   (unless (and pid (not (zerop pid)) (<= (abs pid) 2147483647))
                     (error "reseed PID metadata has an invalid process ID"))
                   (list :pid (abs pid) :identity before :bytes bytes)))
            (close stream)))))))

(defmethod physical-adoption-check-owner-role ((owner physical-reseed-owner))
  (let* ((old (physical-reseed-owner-old-pid owner))
         (current (physical-reseed-read-pid (physical-adoption-owner-directory owner))))
    (unless (equalp old current)
      (error "reseed target PID metadata changed"))
    (when (and old (not (eq :dead (physical-reseed-process-state (getf old :pid)))))
      (error "reseed target process is live or its state is unknown")))
  owner)

(defun call-with-physical-reseed-owner (directory function)
  (call-with-physical-adoption-directory-owner
   directory function :constructor #'make-physical-reseed-owner
   :initial-arguments (lambda (root) (list :old-pid (physical-reseed-read-pid root)))))
