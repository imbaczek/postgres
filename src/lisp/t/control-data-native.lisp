(require :asdf)
(in-package #:cl-user)

(let ((base (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))))
  (pushnew base asdf:*central-registry* :test #'equal)
  (let ((*compile-verbose* nil) (*load-verbose* nil))
    (asdf:load-system "pg")))

(setf pg::*native-control-test-hook*
      (lambda (point)
        (when (string-equal (symbol-name point) (uiop:getenv "PG_CONTROL_PAUSE"))
          (with-open-file (stream (uiop:getenv "PG_CONTROL_MARKER") :direction :output
                                                                    :if-exists :error :if-does-not-exist :create)
            (format stream "~d~%" (sb-posix:getpid))
            (finish-output stream))
          (loop (sleep 0.05)))))

(setf sb-ext:*posix-argv*
      (list "pg-lisp" "-p" (uiop:getenv "PG_CONTROL_PORT")
            "-D" (uiop:getenv "PG_CONTROL_DATA")
            "--control-socket" (uiop:getenv "PG_CONTROL_SOCKET")))

(pg::main)
