;;;; primitives-tests.lisp -- page, output, and backend helper tests.

(in-package #:pg)

(defun test-atomic-output ()
  (let* ((directory (merge-pathnames "atomic-output/" *pgdata*))
         (target (merge-pathnames "control.dat" directory))
         (temporary (merge-pathnames "control.tmp" directory))
         (bytes #(0 1 127 255))
         (events '()))
    (with-atomic-output (stream target temporary directory
                                :element-type '(unsigned-byte 8)
                                :fsync-function
                                (lambda (fd)
                                  (push :file-fsync events)
                                  (sb-posix:fsync fd))
                                :rename-function
                                (lambda (from to)
                                  (push :rename events)
                                  (sb-posix:rename from to))
                                :fsync-directory-function
                                (lambda (dir)
                                  (push :directory-fsync events)
                                  (fsync-directory dir)))
      (write-sequence bytes stream))
    (check (equal (nreverse events)
                  '(:file-fsync :rename :directory-fsync))
           "atomic output fsyncs the file before rename and directory fsync")
    (with-open-file (stream target :direction :input
                                   :element-type '(unsigned-byte 8))
      (let ((written (make-array (file-length stream)
                                 :element-type '(unsigned-byte 8))))
        (read-sequence written stream)
        (check (equalp written bytes) "atomic output writes binary bytes")))
    (let ((failed-target (merge-pathnames "failed.dat" directory))
          (failed-temp (merge-pathnames "failed.tmp" directory)))
      (check (handler-case
                 (progn
                   (with-atomic-output (stream failed-target failed-temp directory
                                               :element-type '(unsigned-byte 8)
                                               :rename-function
                                               (lambda (from to)
                                                 (declare (ignore from to))
                                                 (error "injected rename failure")))
                     (write-sequence bytes stream))
                   nil)
               (error () t))
             "atomic output reports rename failures")
      (check (not (probe-file failed-temp))
             "atomic output removes its temporary file after failure"))))
