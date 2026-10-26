;;;; Bounded hooks for the task-owned native namespace acceptance peers.
(in-package #:pg)

(defun namespace-test-barrier (marker release)
  (with-open-file (stream marker :direction :output :if-exists :error)
    (format stream "~d~%" (sb-posix:getpid)))
  (loop repeat 600 until (probe-file release) do (sleep 0.05))
  (unless (probe-file release) (error "namespace migration barrier timed out")))

(defun install-namespace-test-hook (name marker release)
  (let ((point (find name '(:after-link :after-links-sync :after-control-before-memory
                            :after-memory :after-cleanup-link :after-control-rename)
                     :key #'symbol-name :test #'string-equal)))
    (unless point (error "unknown namespace migration test point"))
    (setf *wal-namespace-test-hook*
          (lambda (current)
            (cond
              ((and (eq point :after-control-rename) (eq current :after-links-sync))
               (let ((original (symbol-function 'fsync-directory)))
                 ;; The next global-directory sync follows WAL-control rename.
                 (setf (symbol-function 'fsync-directory)
                       (lambda (directory)
                         (when (equal (namestring directory)
                                      (namestring (merge-pathnames "global/" *pgdata*)))
                           (namespace-test-barrier marker release))
                         (funcall original directory)))))
              ((eq current point) (namespace-test-barrier marker release))))))
  (values))
