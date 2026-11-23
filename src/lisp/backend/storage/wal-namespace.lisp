;;;; Primary-only durable filename migration; never changes timeline or role.
(in-package #:pg)

(defun wal-namespace-file-stat (path &optional missing-ok)
  "Return the status of a regular WAL artifact without following symlinks."
  (let ((stat (handler-case (sb-posix:lstat (namestring path))
                (sb-posix:syscall-error (condition)
                  (if (and missing-ok (= (sb-posix:syscall-errno condition) sb-posix:enoent))
                      nil (error condition))))))
    (when (and stat (/= (logand (sb-posix:stat-mode stat) sb-posix:s-ifmt) sb-posix:s-ifreg))
      (wal-invalid-input "WAL namespace alias is not a regular file: ~a" path))
    stat))
