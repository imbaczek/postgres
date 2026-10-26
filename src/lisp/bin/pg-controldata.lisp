(require :asdf)

;;;; Load the control-data formatter without starting the SQL server.
;;;; Compilation output goes to stderr so stdout remains machine-readable.

(in-package #:cl-user)

(let ((base (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))))
  (pushnew base asdf:*central-registry* :test #'equal)
  (let ((*compile-verbose* nil) (*load-verbose* nil) (*standard-output* *error-output*))
    (asdf:load-system "pg")))

(sb-ext:exit :code (pg::native-controldata-main (uiop:command-line-arguments)))
