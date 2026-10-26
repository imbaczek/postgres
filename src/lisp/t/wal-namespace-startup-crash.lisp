;;;; Pause actual startup while reconciling an interrupted namespace migration.
(require :asdf)
(let ((base (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))))
  (pushnew base asdf:*central-registry* :test #'equal)
  (pushnew (merge-pathnames "vendor/swank/" base) asdf:*central-registry* :test #'equal)
  (let ((*compile-verbose* nil) (*load-verbose* nil) (*standard-output* *error-output*))
    (asdf:load-system "pg")
    (asdf:load-system "pg-swank")
    (load (merge-pathnames "t/wal-namespace-failpoints.lisp" base))))
(in-package #:pg)

(setf *wal-namespace-test-hook*
      (lambda (point)
        (when (eq point :after-startup-alias-cleanup)
          (namespace-test-barrier (uiop:getenv "PG_NAMESPACE_STARTUP_MARKER")
                                  (uiop:getenv "PG_NAMESPACE_STARTUP_RELEASE")))))

(handler-case (main)
  (error (condition)
    (format *error-output* "~&namespace startup rejected: ~a~%" condition)
    (sb-ext:exit :code 1)))
