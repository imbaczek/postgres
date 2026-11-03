(in-package #:pg-physical-tests)

(defun call-with-adoption-startup-entry-fixture (function &key (complete t) (distance 0))
  (call-with-adoption-follow-transaction-fixture
   (lambda (root owner handoff directory)
     (declare (ignore handoff))
     (let ((state (if complete (pg::physical-adoption-follow-start owner directory)
                      (pg::physical-adoption-follow-prepare owner directory))))
       (sb-posix:chmod (namestring (merge-pathnames ".pg_lisp.lock" root)) #o600)
       (setf (pg::physical-adoption-owner-active-p owner) nil)
       (close (pg::physical-adoption-owner-lock owner))
       (call-with-adoption-postmaster-fixture
        root (lambda () (funcall function root state directory)))))
   :fork-distance distance :generations 3))

(defun adoption-startup-test-own ()
  (pg::postmaster-own-data-directory
   "127.0.0.1" 5439 :startup-check #'pg::physical-adoption-startup-preflight
   :startup-reconcile #'pg::physical-adoption-reconcile-startup))

(deftest adoption-startup-entry-retires-terminal-follow-before-returning-ownership
    (dolist (distance '(0 64))
      (call-with-adoption-startup-entry-fixture
       (lambda (root state directory)
         (declare (ignore state directory))
         (let ((before (adoption-retire-test-protected-image root)))
           (adoption-startup-test-own)
           (ok (pg::postmaster-pid-owned-p))
           (ok (open-stream-p pg::*postmaster-data-lock*))
           (ok (not (intake-fails-p #'pg::physical-adoption-check-startup)))
           (ok (equal before
                      (remove "postmaster.pid" (adoption-retire-test-protected-image root)
                              :key #'first :test #'equal)))
           (ok (not (intake-fails-p #'pg::physical-adoption-startup-preflight)))
           (ok (not (intake-fails-p #'pg::physical-adoption-reconcile-startup)))))
       :distance distance)))

(deftest adoption-startup-entry-refuses-incomplete-or-corrupt-follow-before-reconciliation
    (dolist (scenario '(:incomplete :corrupt :missing-follow))
      (call-with-adoption-startup-entry-fixture
       (lambda (root state directory)
         (declare (ignore state directory))
         (ecase scenario
           (:incomplete nil)
           (:corrupt
            (with-open-file (stream (pg::physical-adoption-follow-path root)
                                    :direction :output :element-type '(unsigned-byte 8)
                                    :if-exists :overwrite)
              (write-byte 0 stream)))
           (:missing-follow (delete-file (pg::physical-adoption-follow-path root))))
         (let ((before (pg::physical-adoption-install-snapshot root)) (entered nil))
           (ok (intake-fails-p
                (lambda ()
                  (pg::postmaster-own-data-directory
                   "127.0.0.1" 5439 :startup-check #'pg::physical-adoption-startup-preflight
                   :startup-reconcile (lambda () (setf entered t))))))
           (ok (not entered))
           (ok (null pg::*postmaster-data-lock*))
           (ok (null pg::*postmaster-pid-stream*))
           (ok (equal before (pg::physical-adoption-install-snapshot root)))))
       :complete (not (eq scenario :incomplete)))))

(deftest adoption-startup-entry-recovers-a-torn-marker-before-retirement
    (call-with-adoption-startup-entry-fixture
     (lambda (root state directory)
       (declare (ignore directory))
       (let ((marker (adoption-retire-expected-marker (getf state :body-crc))))
         (with-open-file (stream (pg::physical-adoption-follow-path root)
                                 :direction :output :element-type '(unsigned-byte 8)
                                 :if-exists :append)
           (write-sequence marker stream :end 5)))
       (adoption-startup-test-own)
       (ok (pg::postmaster-pid-owned-p))
       (ok (not (intake-fails-p #'pg::physical-adoption-check-startup))))))
