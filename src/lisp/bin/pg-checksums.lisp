(require :asdf)

(in-package #:cl-user)

(let ((base (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))))
  (pushnew base asdf:*central-registry* :test #'equal)
  (let ((*compile-verbose* nil) (*load-verbose* nil) (*standard-output* *error-output*))
    (asdf:load-system "pg")))

(in-package #:pg)

(defun native-checksums-main (arguments)
  (handler-case
      (progn
        (unless (equal (first arguments) "--enable")
          (error "expected --enable"))
        (native-control-enable-page-checksums (native-controldata-directory (rest arguments)))
        (write-line "Native page checksum format verified and enabled.")
        0)
    (error (condition)
      (format *error-output* "pg_checksums: ~a~%" condition)
      1)))

(sb-ext:exit :code (native-checksums-main (uiop:command-line-arguments)))
