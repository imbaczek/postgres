;;;; Own a stopped test clone, publish real pending state, and pause for SIGKILL.
(require :asdf)
(let ((base (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))))
  (pushnew base asdf:*central-registry* :test #'equal)
  (let ((*compile-verbose* nil) (*load-verbose* nil) (*standard-output* *error-output*))
    (asdf:load-system "pg")))
(in-package #:pg)

(defun promotion-native-barrier (point &optional ignored)
  (declare (ignore ignored))
  (when (string-equal (symbol-name point) (uiop:getenv "PG_PROMOTION_PAUSE"))
    (let ((marker (pathname (uiop:getenv "PG_PROMOTION_MARKER"))))
      (wal-atomic-write-bytes marker (string->bytes (format nil "~d~%" (sb-posix:getpid))))
      (loop (sleep 1)))))

(setf *physical-promotion-test-hook* #'promotion-native-barrier)

(handler-case
    (progn
      (setf *pgdata* (uiop:ensure-directory-pathname (uiop:getenv "PG_PROMOTION_DATA")))
      (postmaster-own-data-directory "127.0.0.1" (parse-integer (uiop:getenv "PG_PROMOTION_PORT")))
      (unwind-protect
           (progn
             (when (probe-file (physical-follow-config-path))
               (error "native fixture must have no persisted follow endpoint"))
             (physical-bootstrap-check-startup)
             (physical-recovery-open-storage)
             (sb-thread:with-mutex (*physical-follow-command-lock*)
               (sb-thread:with-mutex (*reload-run-lock*)
                 (with-live-operation-quiescence
                   (physical-store-compact *physical-receive-store* *last-replay-lsn*
                                           #'physical-restartpoint-publish)
                   (let ((*physical-recovery-control-test-hook* #'promotion-native-barrier))
                     (physical-promotion-publish-intent)))))
             (assert (= 3 (getf *physical-recovery-state* :phase)))
             (assert (handler-case (progn (check-recovery-available) nil)
                       (pg-error (condition) (equal "57P03" (pg-error-sqlstate condition)))))
             (format t "promotion-intent-native: published pending child ~d at ~d~%"
                     (getf *physical-recovery-state* :promotion-child) *last-replay-lsn*))
        (when *physical-receive-store* (physical-store-close *physical-receive-store*))
        (when *clog-stream* (close *clog-stream*))
        (postmaster-remove-pid-file)))
  (error (condition)
    (format *error-output* "promotion-intent-native: ~a~%" condition)
    (sb-ext:exit :code 1)))
