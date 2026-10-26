;;;; Fresh process reconstructs verified pending state without granting writes.
(require :asdf)
(let ((base (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))))
  (pushnew base asdf:*central-registry* :test #'equal)
  (let ((*compile-verbose* nil) (*load-verbose* nil) (*standard-output* *error-output*))
    (asdf:load-system "pg")))
(in-package #:pg)

(defun promotion-resume-native ()
  (setf *pgdata* (uiop:ensure-directory-pathname (uiop:getenv "PG_PROMOTION_DATA")))
  (postmaster-own-data-directory "127.0.0.1" (parse-integer (uiop:getenv "PG_PROMOTION_PORT")))
  (unwind-protect
       (let ((intent (recovery-read-bytes (physical-recovery-control-path)))
             (control (recovery-read-bytes (wal-control-path)))
             (end nil))
         (sb-thread:with-mutex (*physical-follow-command-lock*)
           (sb-thread:with-mutex (*reload-run-lock*)
             (with-live-operation-quiescence
               (let ((*physical-promotion-test-hook*
                       (lambda (point)
                         (when (string-equal (symbol-name point) (uiop:getenv "PG_PROMOTION_PAUSE"))
                           (wal-atomic-write-bytes
                            (pathname (uiop:getenv "PG_PROMOTION_MARKER"))
                            (string->bytes (format nil "~d~%" (sb-posix:getpid))))
                           (loop (sleep 1))))))
                 (setf end (physical-promotion-resume-pending))))))
         (let* ((metadata (physical-restartpoint-read *physical-recovery-state*))
                (expected (getf metadata :source-xids))
                (restored (getf *physical-recovery-state* :source-xids)))
           (assert (equal expected restored))
           (when restored
             (running-xacts-check-state restored)
             (assert (= *last-replay-lsn* (getf restored :through)))
             (assert (= *next-xid* (getf restored :next-xid))))
           (format t "promotion-resume-native: source-membership ~a PASS~%"
                   (if restored "restored" "unknown")))
         (assert (= 3 (getf *physical-recovery-state* :phase)))
         (assert (= *next-xid* (getf *physical-recovery-state* :observed-next-xid)))
         (assert (= *last-replay-lsn* (getf *physical-recovery-state* :applied)))
         (assert (equalp intent (recovery-read-bytes (physical-recovery-control-path))))
         (assert (equalp control (recovery-read-bytes (wal-control-path))))
         (assert (handler-case (progn (check-recovery-available) nil)
                   (pg-error (condition) (equal "57P03" (pg-error-sqlstate condition)))))
         (format t "promotion-resume-native: restored fork=~d next=~d durable-child-end=~d PASS~%"
                 *last-replay-lsn* *next-xid* end))
    (when *physical-receive-store* (physical-store-close *physical-receive-store*))
    (when *clog-stream* (close *clog-stream*))
    (smgr-close-all)
    (postmaster-remove-pid-file)))

(handler-case (promotion-resume-native)
  (error (condition)
    (format *error-output* "promotion-resume-native: failed=~s ~a~%" *physical-recovery-failed-p* condition)
    (sb-ext:exit :code 1)))
