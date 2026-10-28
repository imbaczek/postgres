(in-package #:pg)

(defvar *child-bootstrap-crash-root* nil)
(defvar *child-bootstrap-crash-point* nil)

(defun child-bootstrap-promotion-crash-hook (point)
  (when (eq point *child-bootstrap-crash-point*)
    (let* ((path (merge-pathnames "promotion-crash-held" *child-bootstrap-crash-root*))
           (temporary (pathname (concatenate 'string (namestring path) ".tmp"))))
      (with-open-file (stream temporary :direction :output :if-exists :error)
        (format stream "~d~%" (sb-posix:getpid)))
      (sb-posix:rename (namestring temporary) (namestring path)))
    (loop repeat 1200
          until (probe-file (merge-pathnames "promotion-crash-release" *child-bootstrap-crash-root*))
          do (sleep 0.025))))

(defun child-bootstrap-install-promotion-crash (root point)
  (assert (member point '(:after-online-intent :after-child-control-directory-fsync :after-retire-follow-transfer)))
  (setf *child-bootstrap-crash-root* root
        *child-bootstrap-crash-point* point
        *physical-promotion-test-hook* #'child-bootstrap-promotion-crash-hook)
  t)

(defun child-bootstrap-install-backup-fault (point)
  (assert (member point '(:after-pin-binding :after-manifest)))
  (setf *physical-bootstrap-test-hook*
        (lambda (actual target)
          (declare (ignore target))
          (when (eq actual point) (error "injected child backup publication failure"))))
  t)

(defvar *child-bootstrap-query-original* nil)

(defvar *child-bootstrap-query-wrapper* nil)

(defun child-bootstrap-install-wrong-manifest ()
  (assert (null *child-bootstrap-query-original*))
  (let ((original (symbol-function 'physical-follow-start-query)))
    (setf *child-bootstrap-query-original* original
          *child-bootstrap-query-wrapper*
          (lambda (runtime cursor)
            (let ((*physical-recovery-state* (copy-list *physical-recovery-state*)))
              (setf (getf *physical-recovery-state* :manifest-digest)
                    (logxor 1 (getf *physical-recovery-state* :manifest-digest)))
              (funcall original runtime cursor)))
          (symbol-function 'physical-follow-start-query) *child-bootstrap-query-wrapper*))
  t)

(defun child-bootstrap-restore-query ()
  (when *child-bootstrap-query-original*
    (assert (eq (symbol-function 'physical-follow-start-query) *child-bootstrap-query-wrapper*))
    (setf (symbol-function 'physical-follow-start-query) *child-bootstrap-query-original*
          *child-bootstrap-query-original* nil
          *child-bootstrap-query-wrapper* nil))
  t)
