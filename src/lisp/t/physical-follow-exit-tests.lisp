;;;; Receiver exit durability is distinct from transport and decoder intake progress.
(in-package #:pg-physical-tests)

(defun call-with-exit-flush-store (function)
  (with-physical-store-directory (root)
    (let* ((store (pg::physical-store-open root 123 1 :create t))
           (pg::*physical-receive-store* store)
           (pg::*physical-recovery-state* (list :phase 2 :receive-lsn 0))
           (pg::*physical-recovery-failed-p* nil)
           (runtime (pg::make-physical-follow-runtime :host "127.0.0.1" :port 5432 :pin "test")))
      (unwind-protect (funcall function root store runtime)
        (pg::physical-store-close store)))))

(deftest exit-flush-publishes-only-complete-records
    (call-with-exit-flush-store
     (lambda (root store runtime)
       (let* ((first (sample-physical-record))
              (end (length first))
              (second (sample-physical-record end)))
         (pg::physical-store-input store 0 first)
         (pg::physical-store-input store end (subseq second 0 10))
         (ok (= 0 (pg::physical-store-flushed-lsn store)))
         (ok (= end (pg::physical-follow-flush-on-exit runtime)))
         (ok (= end (getf pg::*physical-recovery-state* :receive-lsn)))
         (ok (= 10 (pg::physical-wal-decoder-used (pg::physical-store-decoder store))))
         (pg::physical-store-close store)
         (let ((reopened (pg::physical-store-open root 123 1)))
           (unwind-protect
                (progn
                  (ok (= end (pg::physical-store-flushed-lsn reopened)))
                  (ok (= end (pg::physical-store-reset-unflushed reopened))))
             (pg::physical-store-close reopened)))))))

(deftest exit-flush-failure-is-terminal-even-after-stop-request
    (call-with-exit-flush-store
     (lambda (root store runtime)
       (pg::physical-store-input store 0 (sample-physical-record))
       (setf (pg::physical-follow-runtime-stopped-p runtime) t)
       (let ((pg::*physical-store-test-hook*
               (lambda (point ignored)
                 (declare (ignore ignored))
                 (when (eq point :after-segment-fsync) (error "exit fsync failpoint")))))
         (ok (eq :failed (pg::physical-follow-flush-on-exit runtime))))
       (ok pg::*physical-recovery-failed-p*)
       (ok (pg::physical-store-failed-p store))
       (ok (search "exit fsync failpoint" (pg::physical-follow-runtime-last-error runtime)))
       (ok (= 0 (pg::physical-store-flushed-lsn store)))
       (ok (= 0 (getf pg::*physical-recovery-state* :receive-lsn)))
       (pg::physical-store-close store)
       (let ((reopened (pg::physical-store-open root 123 1)))
         (unwind-protect
              (ok (= 0 (pg::physical-store-flushed-lsn reopened)))
           (pg::physical-store-close reopened))))))

(deftest exit-flush-does-not-retry-poisoned-input
    (call-with-exit-flush-store
     (lambda (root store runtime)
       (declare (ignore root))
       (setf (pg::physical-store-failed-p store) t)
       (ok (eq :failed (pg::physical-follow-flush-on-exit runtime)))
       (ok (null (pg::physical-follow-runtime-last-error runtime)))
       (ok (= 0 (pg::physical-store-flushed-lsn store))))))

(deftest idle-exit-flush-does-not-invent-received-progress
    (call-with-exit-flush-store
     (lambda (root store runtime)
       (declare (ignore root store))
       (remf pg::*physical-recovery-state* :receive-lsn)
       (ok (= 0 (pg::physical-follow-flush-on-exit runtime)))
       (ok (not (member :receive-lsn pg::*physical-recovery-state*))))))

(deftest exit-flush-failure-preserves-unwinding-transport-error
    (call-with-exit-flush-store
     (lambda (root store runtime)
       (declare (ignore root))
       (pg::physical-store-input store 0 (sample-physical-record))
       (let* ((pg::*physical-store-test-hook*
                (lambda (point ignored)
                  (declare (ignore ignored))
                  (when (eq point :before-control-fsync) (error "exit fsync failpoint"))))
              (condition
                (handler-case
                    (unwind-protect
                         (error 'pg::pg-client-error :sqlstate "08006" :message "original transport failure")
                      (pg::physical-follow-flush-on-exit runtime))
                  (pg::pg-client-error (condition) condition))))
         (ok (typep condition 'pg::pg-client-error))
         (ok (equal "original transport failure" (pg::pg-client-error-message condition)))
         (ok pg::*physical-recovery-failed-p*)))))
