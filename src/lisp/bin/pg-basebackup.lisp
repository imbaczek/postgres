(require :asdf)

(in-package #:cl-user)

(let ((base (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))))
  (pushnew base asdf:*central-registry* :test #'equal)
  (let ((*compile-verbose* nil) (*load-verbose* nil) (*standard-output* *error-output*))
    (asdf:load-system "pg")))

(let ((main sb-thread:*current-thread*) (cancelled nil))
  (flet ((cancel (&rest arguments)
           (declare (ignore arguments))
           (unless cancelled
             (setf cancelled t)
             (sb-thread:interrupt-thread main (lambda () (error "native base backup cancelled"))))))
    (dolist (signal (list sb-unix:sighup sb-unix:sigint sb-unix:sigterm))
      (sb-sys:enable-interrupt signal #'cancel))
    (sb-ext:exit :code (pg::native-basebackup-main (uiop:command-line-arguments)))))
