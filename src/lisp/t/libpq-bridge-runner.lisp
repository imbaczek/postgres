(require :asdf)

(asdf:load-asd (merge-pathnames "../pg.asd" *load-truename*))

(asdf:load-system "pg")

(defun run-libpq-startup-failure-fixture (mode)
  (let ((published nil) (failed nil))
    (handler-case
        (if (equal mode "cancelled")
            (pg::physical-libpq-open-channel
             (uiop:getenv "PG_LIBPQ_HELPER")
             :on-spawn (lambda (channel)
                         (setf published channel)
                         (pg::physical-libpq-stop channel)))
            (pg::physical-libpq-connect
             (uiop:getenv "PG_LIBPQ_HELPER")
             (pg::make-physical-libpq-profile "unknown_secret_option=private-marker")
             :on-spawn (lambda (channel) (setf published channel))))
      (error (condition)
        (assert (not (search "private-marker" (princ-to-string condition))))
        (setf failed t)))
    (assert failed)
    (assert published)
    (assert (pg::physical-libpq-channel-closed published))
    (assert (null (pg::physical-libpq-channel-pidfd published)))
    (assert (zerop (hash-table-count pg::*physical-libpq-channels*)))
    (format t "libpq-bridge ~a: PASS~%" mode)))

(defun run-libpq-bridge-fixture ()
  (when (member (uiop:getenv "PG_LIBPQ_TEST_MODE") '("cancelled" "connect-failure") :test #'equal)
    (return-from run-libpq-bridge-fixture
      (run-libpq-startup-failure-fixture (uiop:getenv "PG_LIBPQ_TEST_MODE"))))
  (let* ((mode (uiop:getenv "PG_LIBPQ_TEST_MODE"))
         (profile (pg::make-physical-libpq-profile
                   (format nil "host=127.0.0.1 port=~a user=postgres dbname=postgres sslmode=disable gssencmode=disable application_name=bridge-explicit"
                           (uiop:getenv "PG_LIBPQ_TEST_PORT"))
                   :fallback "cluster-fallback"))
         (channel (pg::physical-libpq-connect (uiop:getenv "PG_LIBPQ_HELPER") profile)))
    (unwind-protect
         (cond
           ((equal mode "query")
            (assert (equalp #("42")
                            (pg::physical-libpq-query channel "IDENTIFY_SYSTEM"
                                                      '(("systemid" 25)) "IDENTIFY_SYSTEM")))
            (pg::physical-libpq-exchange channel #\X (pg::make-payload) '(#\A) 0))
           ((equal mode "immediate")
            (assert (equal '(:switch 2 64) (pg::physical-libpq-start channel "START_STREAMING")))
            (pg::physical-libpq-exchange channel #\X (pg::make-payload) '(#\A) 0))
           ((equal mode "copy")
            (assert (eq :copy-both (pg::physical-libpq-start channel "START_STREAMING")))
            (multiple-value-bind (kind body) (pg::physical-libpq-receive channel)
              (assert (char= kind #\D))
              (assert (= (length body) (+ 25 65536)))
              (assert (= (aref body 0) (char-code #\w)))
              (assert (every (lambda (byte) (= byte (char-code #\z))) (subseq body 25))))
            (multiple-value-bind (kind body) (pg::physical-libpq-receive channel)
              (assert (char= kind #\F))
              (assert (zerop (length body))))
            (pg::physical-libpq-feedback channel (pg::string->bytes "truthful-feedback"))
            (assert (equal '(:switch 2 64) (pg::physical-libpq-finish channel 2 64)))
            (pg::physical-libpq-exchange channel #\X (pg::make-payload) '(#\A) 0))
           (t (error "unknown libpq bridge fixture")))
      (pg::physical-libpq-close channel))
    (assert (zerop (hash-table-count pg::*physical-libpq-channels*)))
    (format t "libpq-bridge ~a: PASS~%" mode)))

(run-libpq-bridge-fixture)
