;;;; boot.lisp -- load the pg system and run the server.
;;;; Invoked by pg-lisp.sh.

(require :asdf)

(let ((here (make-pathname :name nil :type nil :defaults *load-truename*)))
  (pushnew here asdf:*central-registry* :test #'equal)
  (pushnew (merge-pathnames "vendor/swank/" here)
           asdf:*central-registry* :test #'equal))

(asdf:load-system "pg")
(asdf:load-system "pg-swank")

(handler-case
    (pg:main)
  (error (e)
    (format *error-output* "~&pg-lisp: fatal: ~a~%" e)
    (finish-output *error-output*)
    (sb-ext:exit :code 1)))
