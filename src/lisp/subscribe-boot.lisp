;;;; subscribe-boot.lisp -- load the pg system and run a logical subscriber.
;;;; Invoked by pg-lisp-subscribe.

(require :asdf)

(let ((here (make-pathname :name nil :type nil :defaults *load-truename*)))
  (pushnew here asdf:*central-registry* :test #'equal))

(asdf:load-system "pg")

(handler-case
    (pg::logical-subscription-main)
  (error (condition)
    (format *error-output* "~&pg-lisp-subscribe: fatal: ~a~%" condition)
    (finish-output *error-output*)
    (sb-ext:exit :code 1)))
