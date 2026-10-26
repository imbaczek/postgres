;;;; Standalone task-owned history publisher: never opens cluster storage or listeners.
(require :asdf)
(let ((base (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))))
  (pushnew base asdf:*central-registry* :test #'equal))
(let ((*compile-verbose* nil) (*load-verbose* nil) (*standard-output* *error-output*))
  (asdf:load-system "pg"))
(in-package #:pg)

(with-open-file (stream (concatenate 'string (uiop:getenv "PG_HISTORY_MARKER") ".pid")
                        :direction :output :if-exists :supersede)
  (format stream "~d~%" (sb-posix:getpid)))

(let* ((point (uiop:getenv "PG_HISTORY_POINT"))
       (*timeline-history-test-hook*
         (when point
           (lambda (current)
             (when (string-equal point (symbol-name current))
               (with-open-file (stream (uiop:getenv "PG_HISTORY_MARKER")
                                       :direction :output :if-exists :error)
                 (write-line "publisher paused" stream))
               (loop repeat 600
                     until (probe-file (uiop:getenv "PG_HISTORY_RELEASE"))
                     do (sleep 0.05))
               (unless (probe-file (uiop:getenv "PG_HISTORY_RELEASE"))
                 (error "history publication barrier timed out")))))))
  (handler-case
      (timeline-write-history
       (parse-integer (uiop:getenv "PG_HISTORY_CHILD"))
       (parse-integer (uiop:getenv "PG_HISTORY_PARENT"))
       (parse-integer (uiop:getenv "PG_HISTORY_FORK"))
       (uiop:getenv "PG_HISTORY_REASON")
       (uiop:ensure-directory-pathname (uiop:getenv "PG_HISTORY_DATA")))
    (error (condition)
      (format *error-output* "~&history publisher rejected: ~a~%" condition)
      (sb-ext:exit :code 1))))
