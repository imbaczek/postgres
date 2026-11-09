(in-package #:pg)

(defun native-patroni-bootstrap-pause (ready release)
  (setf *physical-bootstrap-test-hook*
        (lambda (point target)
          (declare (ignore target))
          (when (eq point :before-stream)
            (with-open-file (stream ready :direction :output :if-exists :error)
              (write-line "ready" stream))
            (loop until (probe-file release) do (sleep 0.02)))))
  (values))
