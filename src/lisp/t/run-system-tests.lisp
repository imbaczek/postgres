;; Resolve the requested checkout from this script and return failure to the shell.

(require :asdf)

(let* ((directory (uiop:pathname-directory-pathname *load-truename*))
       (source (truename (merge-pathnames "../" directory))))
  (load (merge-pathnames "system-test-runner.lisp" directory))
  (unless (uiop:symbol-call :cl-user :run-pg-system-tests (uiop:command-line-arguments) source)
    (sb-ext:exit :code 1)))
