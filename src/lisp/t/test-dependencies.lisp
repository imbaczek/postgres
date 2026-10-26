;;;; Discover installed test dependencies for the plain SBCL shell entry point.
;;;; Prefer configured ASDF systems; Quicklisp is only a fallback for missing ones.
(in-package #:cl-user)

(handler-case
    (asdf:load-system "pg-unit-tests")
  (asdf:missing-component (condition)
    (unless (find-package :ql)
      (let* ((quicklisp-home (uiop:getenv "QUICKLISP_HOME"))
             (setup
               (find-if
                #'probe-file
                (remove
                 nil
                 (list
                  (when (and quicklisp-home (plusp (length quicklisp-home)))
                    (merge-pathnames #p"setup.lisp"
                                     (uiop:ensure-directory-pathname quicklisp-home)))
                  (merge-pathnames #p"quicklisp/setup.lisp" (user-homedir-pathname)))))))
        (unless setup
          (error "Unit tests need Rove and cl-spec/check-it. Register their ASDF systems ~
                  or set QUICKLISP_HOME to an installed Quicklisp directory. Original error: ~a"
                 condition))
        (load setup)))
    (uiop:symbol-call :ql :register-local-projects)
    (unless (asdf:find-system "cl-spec/check-it" nil)
      (error "Cannot find cl-spec/check-it. Install cl-spec in the active Quicklisp ~
              local-projects directory or register its checkout with ASDF using ~
              CL_SOURCE_REGISTRY. See src/lisp/README.md, Testing."))
    (uiop:symbol-call :ql :quickload "pg-unit-tests" :silent t)))
