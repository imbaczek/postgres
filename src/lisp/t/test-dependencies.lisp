;;;; Discover installed test dependencies for the plain SBCL shell entry point.
;;;; Prefer configured ASDF systems; Quicklisp is only a fallback for missing ones.
(in-package #:cl-user)

(defvar *pg-test-load-wrapper* #'funcall
  "Call the dependency loader in the current image's test cache context.")

(defun load-pg-test-system (system)
  ;; Keep an active Quicklisp installation; only initialize one if ASDF misses.
  (handler-case
      (funcall *pg-test-load-wrapper* (lambda () (asdf:load-system system)))
    (asdf:missing-component (condition)
      (unless (find-package :ql)
        (let* ((quicklisp-home (uiop:getenv "QUICKLISP_HOME"))
               (setup
                 (find-if #'probe-file
                          (remove nil
                                  (list
                                   (when (and quicklisp-home (plusp (length quicklisp-home)))
                                     (merge-pathnames #p"setup.lisp"
                                                      (uiop:ensure-directory-pathname quicklisp-home)))
                                   (merge-pathnames #p"quicklisp/setup.lisp"
                                                    (user-homedir-pathname)))))))
          (unless setup
            (error "Tests for ~a need Rove and cl-spec/check-it. Register their ASDF systems ~
                    or set QUICKLISP_HOME to an installed Quicklisp directory. Original error: ~a"
                   system condition))
          (load setup)))
      (uiop:symbol-call :ql :register-local-projects)
      (unless (asdf:find-system "cl-spec/check-it" nil)
        (error "Cannot find cl-spec/check-it. Install cl-spec in the active Quicklisp ~
                local-projects directory or register its checkout with ASDF using ~
                CL_SOURCE_REGISTRY. See src/lisp/README.md, Testing."))
      (funcall *pg-test-load-wrapper*
               (lambda () (uiop:symbol-call :ql :quickload system :silent t))))))

(defvar *pg-test-system* "pg-unit-tests")

(load-pg-test-system *pg-test-system*)
