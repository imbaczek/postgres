(require :asdf)
(in-package #:cl-user)

(let ((base (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))))
  (pushnew base asdf:*central-registry* :test #'equal)
  (let ((*compile-verbose* nil) (*load-verbose* nil))
    (asdf:load-system "pg")))

(setf pg::*physical-promotion-test-hook*
      (lambda (point)
        (when (string-equal (symbol-name point) (uiop:getenv "PG_PROMOTION_PAUSE"))
          (with-open-file (stream (uiop:getenv "PG_PROMOTION_MARKER") :direction :output
                                                                      :if-exists :error :if-does-not-exist :create)
            (format stream "~d~%" (sb-posix:getpid))
            (finish-output stream))
          (loop until (and (uiop:getenv "PG_PROMOTION_RELEASE")
                           (probe-file (uiop:getenv "PG_PROMOTION_RELEASE")))
                do (sleep 0.05)))))

(setf sb-ext:*posix-argv*
      (list "pg-lisp" "-p" (uiop:getenv "PG_PROMOTION_PORT")
            "-D" (uiop:getenv "PG_PROMOTION_DATA")
            "--control-socket" (uiop:getenv "PG_PROMOTION_SOCKET")))

(pg::main)
