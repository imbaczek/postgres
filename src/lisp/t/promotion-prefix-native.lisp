;;;; Native child-prefix process-crash fixture.
(require :asdf)
(let ((base (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))))
  (pushnew base asdf:*central-registry* :test #'equal)
  (let ((*compile-verbose* nil) (*load-verbose* nil) (*standard-output* *error-output*))
    (asdf:load-system "pg")))
(in-package #:pg)

(defun promotion-prefix-native ()
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
               (physical-promotion-publish-intent)
               (let ((child (wal-segment-path (floor *last-replay-lsn* +wal-segment-size+)
                                              (getf *physical-recovery-state* :promotion-child) :timeline)))
                 (wal-atomic-write-bytes child (string->bytes "pending-stale-child")))
               (let ((*wal-timeline-test-hook*
                       (lambda (point)
                         (when (string-equal (symbol-name point) (uiop:getenv "PG_PROMOTION_PAUSE"))
                           (wal-atomic-write-bytes
                            (pathname (uiop:getenv "PG_PROMOTION_MARKER"))
                            (string->bytes (format nil "~d~%" (sb-posix:getpid))))
                           (loop (sleep 1))))))
                 (physical-promotion-install-prefix)))))
         (assert (= 3 (getf *physical-recovery-state* :phase)))
         (assert (handler-case (progn (check-recovery-available) nil)
                   (pg-error (condition) (equal "57P03" (pg-error-sqlstate condition)))))
         (format t "promotion-prefix-native: installed pending child at ~d~%" *last-replay-lsn*))
    (when *physical-receive-store* (physical-store-close *physical-receive-store*))
    (when *clog-stream* (close *clog-stream*))
    (postmaster-remove-pid-file)))

(handler-case (promotion-prefix-native)
  (error (condition)
    (format *error-output* "promotion-prefix-native: ~a~%" condition)
    (sb-ext:exit :code 1)))
