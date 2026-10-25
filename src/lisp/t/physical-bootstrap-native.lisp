;;;; Hooks installed only into the task-owned acceptance source image.
(in-package #:pg)

(defun install-physical-bootstrap-pause (root)
  (setf *physical-bootstrap-test-hook*
        (lambda (point target)
          (declare (ignore target))
          (when (eq point :after-checkpoint)
            (setf *physical-bootstrap-test-hook* nil)
            (wal-atomic-write-bytes (merge-pathnames "copy.paused" root) #(1))
            (loop repeat 300
                  until (probe-file (merge-pathnames "copy.release" root))
                  do (sleep 0.1))
            (unless (probe-file (merge-pathnames "copy.release" root))
              (error "bootstrap pause timed out"))))
        *live-operation-admission-wait-hook*
        (lambda ()
          (setf *live-operation-admission-wait-hook* nil)
          (wal-atomic-write-bytes (merge-pathnames "writer.blocked" root) #(1))))
  t)

(defun install-physical-bootstrap-crash ()
  (setf *physical-bootstrap-test-hook*
        (lambda (point target)
          (declare (ignore target))
          (when (eq point :after-file)
            (sb-posix:kill (sb-posix:getpid) 9))))
  t)
