;;;; Receiver exit durability is distinct from transport and decoder intake progress.
(in-package #:pg-physical-tests)

(defun call-with-exit-flush-store (function)
  (with-physical-store-directory (root)
    (let* ((store (pg::physical-store-open root 123 1 :create t))
           (pg::*physical-receive-store* store)
           (pg::*physical-recovery-state* (list :phase 2 :receive-lsn 0))
           (pg::*physical-recovery-failed-p* nil)
           (runtime (pg::make-physical-follow-runtime :host "127.0.0.1" :port 5432 :pin "test"))
           (pg::*physical-follow-runtime* runtime))
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

(deftest generation-operation-rejects-inverted-admission-before-running-body
    (let ((entered nil))
      (pg::call-with-physical-follow-generation-fence
       (lambda ()
         (ok (intake-fails-p
              (lambda ()
                (pg::with-physical-follow-generation-operation
                  (setf entered t)))))))
      (ok (not entered))))

(deftest generation-operation-preserves-nested-admission-and-multiple-values
    (let ((depths nil))
      (ok (equal
           '(:first :second)
           (multiple-value-list
            (pg::with-physical-follow-generation-operation
              (push pg::*live-operation-depth* depths)
              (pg::with-physical-follow-generation-operation
                (push pg::*live-operation-depth* depths)
                (values :first :second))))))
      (ok (equal '(1 2) (reverse depths)))
      (ok (not (sb-thread:holding-mutex-p pg::*physical-follow-generation-lock*)))
      (ok (zerop pg::*live-operation-depth*))))

(deftest receiver-frame-keeps-telemetry-write-and-flush-in-one-generation-fence
    (call-with-exit-flush-store
     (lambda (root store runtime)
       (declare (ignore root))
       (let* ((pg::*physical-follow-desired* nil)
              (pg::*physical-receive-flush-bytes* 0)
              (payload (pg::make-payload))
              (raw (sample-physical-record))
              (monitor (symbol-function 'pg::physical-follow-monitor-message))
              (receive (symbol-function 'pg::physical-recovery-receive))
              (flush (symbol-function 'pg::physical-recovery-flush))
              (events nil))
         (pg::payload-push-byte payload (char-code #\w))
         (pg::pg-client-push-u64 payload 0)
         (pg::pg-client-push-u64 payload (length raw))
         (pg::pg-client-push-u64 payload 0)
         (pg::payload-push payload raw)
         (flet ((record (stage)
                  (push (list stage (sb-thread:holding-mutex-p pg::*physical-follow-generation-lock*))
                        events)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-monitor-message)
                        (lambda (&rest arguments) (record :monitor) (apply monitor arguments))
                        (symbol-function 'pg::physical-recovery-receive)
                        (lambda (&rest arguments) (record :receive) (apply receive arguments))
                        (symbol-function 'pg::physical-recovery-flush)
                        (lambda () (record :flush) (funcall flush)))
                  (pg::physical-follow-process-frame payload runtime)
                  (ok (equal '((:monitor t) (:receive t) (:flush t)) (reverse events)))
                  (ok (= (length raw) (pg::physical-store-flushed-lsn store))))
             (setf (symbol-function 'pg::physical-follow-monitor-message) monitor
                   (symbol-function 'pg::physical-recovery-receive) receive
                   (symbol-function 'pg::physical-recovery-flush) flush)))))))

(deftest generation-replacement-waits-for-an-accepted-frame-to-flush
    (call-with-exit-flush-store
     (lambda (root store runtime)
       (declare (ignore root))
       (let* ((first (pg::physical-follow-next-desired nil "host=first" "" "rx" nil t))
              (next (pg::physical-follow-next-desired first "host=next" "" "rx" nil t))
              (saved pg::*physical-follow-desired*)
              (state pg::*physical-recovery-state*)
              (entered (sb-thread:make-semaphore))
              (resume (sb-thread:make-semaphore))
              (attempted (sb-thread:make-semaphore))
              (published (sb-thread:make-semaphore))
              (receive (symbol-function 'pg::physical-recovery-receive))
              (flush (symbol-function 'pg::physical-recovery-flush))
              (payload (pg::make-payload))
              (raw (sample-physical-record))
              (errors (vector nil nil))
              (events nil) (receiver nil) (publisher nil))
         (setf (pg::physical-follow-runtime-generation runtime) first)
         (pg::payload-push-byte payload (char-code #\w))
         (pg::pg-client-push-u64 payload 0)
         (pg::pg-client-push-u64 payload (length raw))
         (pg::pg-client-push-u64 payload 0)
         (pg::payload-push payload raw)
         (unwind-protect
              (progn
                (setf pg::*physical-follow-desired* first
                      (symbol-function 'pg::physical-recovery-receive)
                      (lambda (&rest arguments)
                        (sb-thread:signal-semaphore entered)
                        (sb-thread:wait-on-semaphore resume)
                        (prog1 (apply receive arguments) (push :received events)))
                      (symbol-function 'pg::physical-recovery-flush)
                      (lambda () (prog1 (funcall flush) (push :flushed events))))
                (setf receiver
                      (sb-thread:make-thread
                       (lambda ()
                         (let ((pg::*physical-receive-store* store)
                               (pg::*physical-recovery-state* state)
                               (pg::*physical-recovery-failed-p* nil)
                               (pg::*physical-follow-runtime* runtime)
                               (pg::*physical-follow-statistics* nil)
                               (pg::*physical-follow-last-end-lsn* 0)
                               (pg::*physical-receive-flush-bytes* 0)
                               (pg::*backend* nil))
                           (handler-case (pg::physical-follow-process-frame payload runtime)
                             (error (condition) (setf (aref errors 0) condition)))))))
                (ok (sb-thread:wait-on-semaphore entered :timeout 3))
                (setf publisher
                      (sb-thread:make-thread
                       (lambda ()
                         (handler-case
                             (progn
                               (sb-thread:signal-semaphore attempted)
                               (pg::call-with-physical-follow-generation-fence
                                (lambda ()
                                  (sb-thread:with-mutex (pg::*physical-follow-lock*)
                                    (setf pg::*physical-follow-desired* next))
                                  (push :published events)))
                               (sb-thread:signal-semaphore published))
                           (error (condition) (setf (aref errors 1) condition))))))
                (ok (sb-thread:wait-on-semaphore attempted :timeout 3))
                (ok (not (sb-thread:wait-on-semaphore published :timeout 0.05)))
                (sb-thread:signal-semaphore resume)
                (ok (sb-thread:wait-on-semaphore published :timeout 3))
                (sb-thread:join-thread receiver)
                (sb-thread:join-thread publisher)
                (ok (equalp #(nil nil) errors))
                (ok (equal '(:received :flushed :published) (reverse events)))
                (ok (= (length raw) (pg::physical-store-flushed-lsn store)))
                (ok (handler-case (progn (pg::physical-follow-process-frame payload runtime) nil)
                      (pg::physical-follow-generation-changed () t)))
                (ok (equal '(:received :flushed :published) (reverse events))))
           (sb-thread:signal-semaphore resume)
           (when receiver (sb-thread:join-thread receiver))
           (when publisher (sb-thread:join-thread publisher))
           (setf (symbol-function 'pg::physical-recovery-receive) receive
                 (symbol-function 'pg::physical-recovery-flush) flush)
           (sb-thread:with-mutex (pg::*physical-follow-lock*)
             (setf pg::*physical-follow-desired* saved)))))))

(deftest receiver-feedback-fences-only-the-local-snapshot
    (call-with-exit-flush-store
     (lambda (root store runtime)
       (declare (ignore root store))
       (let ((pg::*physical-follow-desired* nil)
             (pg::*last-replay-lsn* 0)
             (check (symbol-function 'pg::physical-store-check-live))
             (send (symbol-function 'pg::physical-client-feedback))
             (events nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-store-check-live)
                      (lambda (store)
                        (push (list :snapshot
                                    (sb-thread:holding-mutex-p pg::*physical-follow-generation-lock*)
                                    (plusp pg::*live-operation-depth*)) events)
                        (funcall check store))
                      (symbol-function 'pg::physical-client-feedback)
                      (lambda (client payload)
                        (declare (ignore client payload))
                        (push (list :send
                                    (sb-thread:holding-mutex-p pg::*physical-follow-generation-lock*)
                                    (plusp pg::*live-operation-depth*)) events)))
                (pg::physical-follow-feedback nil runtime)
                (ok (equal '((:snapshot t t) (:send nil nil)) (reverse events))))
           (setf (symbol-function 'pg::physical-store-check-live) check
                 (symbol-function 'pg::physical-client-feedback) send))))))

(deftest receiver-reset-fences-generation-check-through-input-reset
    (call-with-exit-flush-store
     (lambda (root store runtime)
       (declare (ignore root store))
       (let ((pg::*physical-follow-desired* nil)
             (reset (symbol-function 'pg::physical-store-reset-unflushed))
             (session (symbol-function 'pg::physical-follow-session))
             (fenced nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-store-reset-unflushed)
                      (lambda (store)
                        (setf fenced
                              (and (sb-thread:holding-mutex-p pg::*physical-follow-generation-lock*)
                                   (plusp pg::*live-operation-depth*)))
                        (funcall reset store))
                      (symbol-function 'pg::physical-follow-session)
                      (lambda (&rest arguments)
                        (declare (ignore arguments))
                        (error 'pg::physical-client-stopped)))
                (ok (eq :stopped (pg::physical-follow-receiver-loop runtime)))
                (ok fenced))
           (setf (symbol-function 'pg::physical-store-reset-unflushed) reset
                 (symbol-function 'pg::physical-follow-session) session))))))

(deftest retired-receiver-exit-does-not-touch-the-current-store
    (call-with-exit-flush-store
     (lambda (root store runtime)
       (declare (ignore root))
       (let ((pg::*physical-follow-runtime* (pg::make-physical-follow-runtime)))
         (pg::physical-store-input store 0 (sample-physical-record))
         (ok (eq :superseded (pg::physical-follow-flush-on-exit runtime)))
         (ok (= 0 (pg::physical-store-flushed-lsn store)))
         (ok (not pg::*physical-recovery-failed-p*))
         (setf (pg::physical-store-failed-p store) t)
         (ok (eq :superseded (pg::physical-follow-flush-on-exit runtime)))
         (ok (not pg::*physical-recovery-failed-p*))
         (ok (null (pg::physical-follow-runtime-last-error runtime)))))))

(deftest superseded-generation-exit-still-flushes-its-owned-store
    (call-with-exit-flush-store
     (lambda (root store runtime)
       (declare (ignore root))
       (let* ((first (pg::physical-follow-next-desired nil "host=first" "" "rx" nil t))
              (next (pg::physical-follow-next-desired first "host=next" "" "rx" nil t))
              (pg::*physical-follow-desired* next)
              (pg::*physical-follow-runtime* runtime)
              (record (sample-physical-record)))
         (setf (pg::physical-follow-runtime-generation runtime) first)
         (ok (intake-fails-p (lambda () (pg::physical-follow-require-runtime-generation runtime))))
         (pg::physical-store-input store 0 record)
         (ok (= (length record) (pg::physical-follow-flush-on-exit runtime)))
         (ok (= (length record) (pg::physical-store-flushed-lsn store)))
         (ok (not pg::*physical-recovery-failed-p*))))))

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
