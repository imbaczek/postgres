(in-package #:pg-physical-tests)

(defun call-with-reseed-cancellation-test-fixture (function)
  (call-with-reseed-follow-installed-fixture
   (lambda (root stage incoming quarantine plan)
     (declare (ignore stage incoming quarantine plan))
     (pg::call-with-physical-reseed-follow-configuration-owner
      root
      (lambda (owner)
        (let* ((initial (pg::physical-reseed-follow-initial-record owner))
               (record (reseed-follow-intent-test-record initial root)))
          (pg::physical-reseed-follow-publish-intent owner record)
          (funcall function owner root record)))))))

(deftest reseed-cancellation-pending-fences-ordinary-entry-before-effects
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (let* ((pending (pg::physical-reseed-follow-cancellation-begin-record record))
              (path (merge-pathnames "physical-reseed.follow" root))
              (attempt-path (merge-pathnames "physical-reseed.attempt" root))
              (original-connect (symbol-function 'pg::physical-libpq-connect-frozen))
              (connections 0)
              (callbacks 0))
         (reseed-test-replace-follow-record root pending)
         (let ((identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat (namestring path))))
               (bytes (pg::recovery-read-bytes path))
               (attempt (pg::recovery-read-bytes attempt-path)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                        (lambda (&rest arguments)
                          (declare (ignore arguments))
                          (incf connections)
                          (error "unexpected source connection after cancellation")))
                  (dolist (operation
                           (list
                            (lambda () (pg::physical-reseed-follow-read owner))
                            (lambda () (pg::physical-reseed-follow-initial-record owner))
                            (lambda () (pg::physical-reseed-follow-recover-intent owner))
                            (lambda () (pg::physical-reseed-follow-publish-intent owner record :previous record))
                            (lambda () (pg::physical-reseed-follow-prepare owner :helper "/unused/helper"))
                            (lambda () (pg::physical-reseed-follow-prepare-remote owner "/unused/helper"))
                            (lambda () (pg::physical-reseed-follow-begin-configuration owner "/unused/workspace/" 0))
                            (lambda () (pg::physical-reseed-follow-stage-configuration owner))
                            (lambda () (pg::physical-reseed-follow-recover-configuration owner))
                            (lambda () (pg::physical-reseed-follow-settle-remote owner :helper "/unused/helper"))
                            (lambda () (pg::physical-reseed-follow-recover-activation owner :helper "/unused/helper"))
                            (lambda ()
                              (pg::call-with-physical-reseed-follow-store
                               owner (lambda (&rest arguments) (declare (ignore arguments)) (incf callbacks))))
                            (lambda ()
                              (pg::call-with-physical-reseed-follow-activation-operation
                               owner (lambda (&rest arguments) (declare (ignore arguments)) (incf callbacks))))
                            (lambda ()
                              (pg::call-with-physical-reseed-follow-installation-operation
                               owner (lambda (&rest arguments) (declare (ignore arguments)) (incf callbacks))))))
                    (ok (handler-case (progn (funcall operation) nil)
                          (error (condition)
                            (string= "target cancellation fences ordinary follow operations"
                                     (princ-to-string condition)))))))
             (setf (symbol-function 'pg::physical-libpq-connect-frozen) original-connect))
           (ok (zerop connections))
           (ok (zerop callbacks))
           (ok (equal identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat (namestring path)))))
           (ok (equalp bytes (pg::recovery-read-bytes path)))
           (ok (equalp attempt (pg::recovery-read-bytes attempt-path)))
           (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
           (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
           (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner))))))))

(defun check-reseed-cancellation-after-idle (owner root record mode)
  (reseed-test-replace-follow-record root record)
  (let* ((pending (pg::physical-reseed-follow-cancellation-begin-record record))
         (original-idle (symbol-function 'pg::physical-reseed-follow-check-idle))
         (original-connect (symbol-function 'pg::physical-libpq-connect-frozen))
         (original-read (symbol-function 'pg::physical-reseed-follow-read-at))
         (injected 0) (connections 0) (callbacks 0) (stabilizations 0)
         (message nil)
         (path (merge-pathnames "physical-reseed.follow" root))
         (attempt-path (merge-pathnames "physical-reseed.attempt" root))
         (attempt (pg::recovery-read-bytes attempt-path)))
    (unwind-protect
         (progn
           (setf (symbol-function 'pg::physical-reseed-follow-check-idle)
                 (lambda (value)
                   (prog1 (funcall original-idle value)
                     (when (zerop injected)
                       (reseed-test-replace-follow-record root pending)
                       (incf injected)))))
           (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                 (lambda (&rest arguments)
                   (declare (ignore arguments))
                   (incf connections)
                   (error "unexpected source connection after cancellation")))
           (setf (symbol-function 'pg::physical-reseed-follow-read-at)
                 (lambda (root-fd name &rest arguments)
                   (when (getf arguments :stabilize) (incf stabilizations))
                   (apply original-read root-fd name arguments)))
           (setf message
                 (handler-case
                     (progn
                       (ecase mode
                         (:prepare (pg::physical-reseed-follow-prepare-remote owner "/unused/helper"))
                         (:read (pg::physical-reseed-follow-read owner))
                         (:settlement (pg::physical-reseed-follow-settle-remote owner :helper "/unused/helper"))
                         (:activation-probe
                          (pg::physical-reseed-follow-probe-activation-source owner :helper "/unused/helper"))
                         (:activation-start
                          (pg::physical-reseed-follow-start-activation owner :helper "/unused/helper"))
                         (:store
                          (pg::call-with-physical-reseed-follow-store
                           owner
                           (lambda (root-fd check)
                             (incf callbacks)
                             (funcall check)
                             (let ((snapshot (pg::physical-reseed-follow-snapshot-at root-fd)))
                               (values (getf snapshot :record) snapshot))))))
                       nil)
                   (error (condition) (princ-to-string condition)))))
      (setf (symbol-function 'pg::physical-reseed-follow-check-idle) original-idle
            (symbol-function 'pg::physical-libpq-connect-frozen) original-connect
            (symbol-function 'pg::physical-reseed-follow-read-at) original-read))
    (ok (equal "target cancellation fences ordinary follow operations" message))
    (ok (= 1 injected))
    (ok (zerop connections))
    (ok (zerop callbacks))
    (ok (zerop stabilizations))
    (ok (equalp (pg::physical-reseed-follow-image pending) (pg::recovery-read-bytes path)))
    (ok (equalp attempt (pg::recovery-read-bytes attempt-path)))
    (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
    (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))

(deftest reseed-cancellation-rechecks-after-idle-before-private-work
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (dolist (mode '(:prepare :store :read))
         (check-reseed-cancellation-after-idle owner root record mode)))))

(deftest reseed-cancellation-rechecks-pending-settlement-before-source-work
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let ((record (pg::physical-reseed-follow-begin-settlement owner)))
         (check-reseed-cancellation-after-idle owner root record :settlement)))))

(deftest reseed-cancellation-rechecks-staged-activation-before-source-work
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore workspace settled))
       (pg::physical-reseed-follow-begin-activation owner)
       (let ((record (pg::physical-reseed-follow-stage-activation owner)))
         (check-reseed-cancellation-after-idle owner root record :activation-probe)))))

(deftest reseed-cancellation-rechecks-committed-activation-before-offline-retry
    (call-with-reseed-activating-fixture
     :same-timeline
     (lambda (owner root workspace record)
       (declare (ignore workspace))
       (check-reseed-cancellation-after-idle owner root record :activation-start))))
