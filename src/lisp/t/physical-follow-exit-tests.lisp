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

(deftest stop-flushes-writes-left-by-an-older-receiver-frame
    (call-with-exit-flush-store
     (lambda (root store runtime)
       (let ((pg::*pgdata* root)
             (pg::*physical-follow-runtime* runtime)
             (record (sample-physical-record)))
         ;; A worker from the previous live definition has already exited without
         ;; the new session cleanup. Stop still owns the final durability contract.
         (pg::physical-store-input store 0 record)
         (ok (eq :stopped (pg::physical-follow-stop)))
         (ok (= (length record) (pg::physical-store-flushed-lsn store)))
         (ok (= (length record) (getf pg::*physical-recovery-state* :receive-lsn)))
         (ok (null pg::*physical-follow-runtime*))))))

(deftest receiver-only-quiescence-flushes-and-keeps-replay-running
    (call-with-exit-flush-store
     (lambda (root store runtime)
       (declare (ignore root))
       (let* ((pg::*physical-follow-runtime* runtime)
              (record (sample-physical-record))
              (allow-replay (sb-thread:make-semaphore))
              (replayed (sb-thread:make-semaphore))
              (old-replay (symbol-function 'pg::physical-recovery-replay))
              (receiver nil) (replay nil))
         (pg::physical-store-input store 0 record)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-recovery-replay)
                      (lambda ()
                        (setf pg::*last-replay-lsn* (pg::physical-store-flushed-lsn store))
                        (sb-thread:signal-semaphore replayed)))
                (setf receiver
                      (sb-thread:make-thread
                       (lambda ()
                         (loop until (pg::physical-follow-receiver-stopped-p runtime)
                               do (sleep 0.005))))
                      replay
                      (sb-thread:make-thread
                       (lambda ()
                         (let ((pg::*physical-receive-store* store)
                               (pg::*last-replay-lsn* 0)
                               (pg::*physical-replay-paused-p* nil)
                               (pg::*shutdown* pg::+no-shutdown+))
                           (sb-thread:wait-on-semaphore allow-replay)
                           (pg::physical-follow-replay-loop runtime))))
                      (pg::physical-follow-runtime-receiver-thread runtime) receiver
                      (pg::physical-follow-runtime-replay-thread runtime) replay)
                (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                  (ok (eq :stopped (pg::physical-follow-quiesce-receiver))))
                (ok (not (sb-thread:thread-alive-p receiver)))
                (ok (sb-thread:thread-alive-p replay))
                (ok (eq runtime pg::*physical-follow-runtime*))
                (ok (not (pg::physical-follow-runtime-stopped-p runtime)))
                (ok (= (length record) (pg::physical-store-flushed-lsn store)))
                (sb-thread:signal-semaphore allow-replay)
                (ok (sb-thread:wait-on-semaphore replayed :timeout 2))
                (ok (sb-thread:thread-alive-p replay)))
           (setf (pg::physical-follow-runtime-stopped-p runtime) t)
           (sb-thread:signal-semaphore allow-replay)
           (dolist (thread (list receiver replay))
             (when thread (sb-thread:join-thread thread :timeout 2 :default :timeout)))
           (setf (symbol-function 'pg::physical-recovery-replay) old-replay))))))

(deftest receiver-only-quiescence-rejects-late-client-without-stopping-replay
    (let* ((runtime (pg::make-physical-follow-runtime))
           (pg::*physical-follow-runtime* runtime))
      (sb-thread:with-mutex (pg::*physical-follow-lock*)
        (setf (pg::physical-follow-runtime-receiver-stopped-p runtime) t))
      (ok (signals (pg::physical-follow-register-runtime-client runtime :late-client)
                   'pg::physical-follow-receiver-stopped))
      (ok (null (pg::physical-follow-runtime-client runtime)))
      (ok (not (pg::physical-follow-runtime-stopped-p runtime)))))

(deftest receiver-only-quiescence-cannot-join-inside-admission
    (let ((pg::*live-operation-depth* 1))
      (ok (intake-fails-p #'pg::physical-follow-quiesce-receiver))))
