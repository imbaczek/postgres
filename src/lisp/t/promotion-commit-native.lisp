;;;; Complete promotion in an owned process, including committed cleanup retries.
(require :asdf)
(let ((base (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))))
  (pushnew base asdf:*central-registry* :test #'equal)
  (let ((*compile-verbose* nil) (*load-verbose* nil) (*standard-output* *error-output*))
    (asdf:load-system "pg")))
(in-package #:pg)

(defun promotion-commit-native ()
  (setf *pgdata* (uiop:ensure-directory-pathname (uiop:getenv "PG_PROMOTION_DATA")))
  (postmaster-own-data-directory "127.0.0.1" (parse-integer (uiop:getenv "PG_PROMOTION_PORT")))
  (unwind-protect
       (let ((*physical-promotion-test-hook*
               (lambda (point)
                 (when (string-equal (symbol-name point) (uiop:getenv "PG_PROMOTION_PAUSE"))
                   (wal-atomic-write-bytes (pathname (uiop:getenv "PG_PROMOTION_MARKER"))
                                           (string->bytes (format nil "~d~%" (sb-posix:getpid))))
                   (loop (sleep 1))))))
         (if (> (nth-value 3 (wal-read-control-fields)) 1)
             (progn (physical-bootstrap-check-startup) (initialize-primary-storage))
             (sb-thread:with-mutex (*physical-follow-command-lock*)
               (sb-thread:with-mutex (*reload-run-lock*)
                 (with-live-operation-quiescence
                   (physical-promotion-resume-pending
                    :artifact-checker #'physical-promotion-checkpoint-artifacts
                    :publisher #'physical-promotion-complete-child)))))
         (assert (not (recovery-in-progress-p)))
         (assert (> *wal-insert-timeline* 1))
         (assert (= *wal-insert-timeline* *wal-checkpoint-timeline*))
         (assert (not (probe-file (physical-recovery-control-path))))
         (check-recovery-available)
         (format t "promotion-commit-native: primary timeline=~d checkpoint=~d next=~d PASS~%"
                 *wal-insert-timeline* *wal-checkpoint-lsn* *next-xid*))
    (when *physical-receive-store* (physical-store-close *physical-receive-store*))
    (when *clog-stream* (close *clog-stream*))
    (when *wal-output-stream* (close *wal-output-stream*))
    (smgr-close-all)
    (postmaster-remove-pid-file)))

(handler-case (promotion-commit-native)
  (error (condition)
    (format *error-output* "promotion-commit-native: failed=~s ~a~%" *physical-recovery-failed-p* condition)
    (sb-ext:exit :code 1)))
