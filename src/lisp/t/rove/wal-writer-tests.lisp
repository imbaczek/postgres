;;;; Primary WAL writer authority regressions.
(in-package #:pg)

(declaim (special *primary-wal-writer* *primary-wal-writer-generation*
                  *primary-wal-writer-lock*))

(defun call-with-primary-writer-authority-test (function &key (isolate t))
  (let* ((*wal-lock* (if isolate (sb-thread:make-mutex :name "writer authority test WAL")
                         *wal-lock*))
         (*wal-temp-cleanup-domain* (unless isolate *wal-temp-cleanup-domain*))
         (*primary-wal-writer-lock* (sb-thread:make-mutex :name "writer authority test state"))
         (*primary-wal-writer-generation* 7)
         (*wal-storage-incarnation* (if isolate (gensym "WRITER-TEST-STORE-")
                                        *wal-storage-incarnation*))
         (*wal-insert-timeline* (if isolate 3 *wal-insert-timeline*))
         (*wal-logical-policy* (unless isolate *wal-logical-policy*))
         (*logical-capture-force* (unless isolate *logical-capture-force*))
         (*postmaster-data-lock* (list :owned-directory))
         (*physical-recovery-state* nil)
         (*wal-ready* t)
         (*wal-poisoned* nil)
         (*wal-end-lsn* (if isolate 0 *wal-end-lsn*))
         (*wal-durable-lsn* (if isolate 0 *wal-durable-lsn*))
         (*wal-pending-page-publication* (unless isolate *wal-pending-page-publication*))
         (*clog-pending-commits* (if isolate (make-hash-table :test #'eql)
                                     *clog-pending-commits*))
         (*wal-control-metadata* (make-wal-control-metadata 1 +db-in-production+))
         (*wal-pending-page-publication-tail* nil)
         (*primary-wal-writer*
           (%make-primary-wal-writer
            7 (make-wal-writer-epoch *wal-storage-incarnation* *wal-insert-timeline*
                                     *postmaster-data-lock*)
            *standard-output* *error-output*))
         (owned (symbol-function 'postmaster-pid-owned-p)))
    (setf (primary-wal-writer-thread *primary-wal-writer*) sb-thread:*current-thread*
          (primary-wal-writer-state *primary-wal-writer*) :running)
    (unwind-protect
         (progn
           (setf (symbol-function 'postmaster-pid-owned-p) (lambda () t))
           (funcall function *primary-wal-writer*))
      (setf (symbol-function 'postmaster-pid-owned-p) owned))))

(defun primary-writer-idle-flush-result (debt)
  (call-with-primary-writer-authority-test
   (lambda (writer)
     (let ((flush (symbol-function 'wal-flush-core)) (calls 0))
       (setf *wal-durable-lsn* 1024 *wal-end-lsn* (+ 1024 debt)
             (primary-wal-writer-last-drained-end writer) 77)
       (unwind-protect
            (progn
              (setf (symbol-function 'wal-flush-core)
                    (lambda ()
                      (assert (sb-thread:holding-mutex-p *wal-lock*))
                      (assert (null *wal-pending-page-publication*))
                      (assert (null *wal-pending-page-publication-tail*))
                      (incf calls)
                      (setf *wal-durable-lsn* *wal-end-lsn*)))
              (let ((first (primary-wal-writer-step writer))
                    (second (primary-wal-writer-step writer)))
                (and (eq :continue first) (eq :continue second)
                     (= (if (plusp debt) 1 0) calls)
                     (= *wal-end-lsn* *wal-durable-lsn*)
                     (= 2 (primary-wal-writer-cycles writer))
                     (= 77 (primary-wal-writer-last-drained-end writer))
                     (not *wal-poisoned*))))
         (setf (symbol-function 'wal-flush-core) flush))))))

(cl-spec:defproperty primary-writer-idle-cycles-flush-complete-tail
    ((debt (range integer 0 65536)))
  (:about primary-wal-writer-drain-under-lock)
  (:trials (:normal 100))
  (primary-writer-idle-flush-result debt))

(rove:deftest primary-writer-idle-flushes-once-and-preserves-fifo-metric
    (rove:ok (primary-writer-idle-flush-result 0))
  (rove:ok (primary-writer-idle-flush-result 4096))
  (unit-check-property 'primary-writer-idle-cycles-flush-complete-tail))

(rove:deftest primary-writer-idle-flush-fences-errors-before-unlock
    (dolist (escape '(nil t))
      (call-with-primary-writer-authority-test
       (lambda (writer)
         (let ((flush (symbol-function 'wal-flush-core))
               (failure (make-condition 'simple-error :format-control "idle flush failed"))
               (observed nil) (poisoned-before-unlock nil))
           (setf *wal-end-lsn* 32 *wal-durable-lsn* 0)
           (unwind-protect
                (progn
                  (setf (symbol-function 'wal-flush-core)
                        (lambda () (if escape (throw 'idle-flush-escape :escaped) (error failure))))
                  (handler-case
                      (catch 'idle-flush-escape
                        (sb-thread:with-mutex (*wal-lock*)
                          (unwind-protect
                               (primary-wal-writer-drain-under-lock writer (primary-wal-writer-epoch writer))
                            (setf poisoned-before-unlock *wal-poisoned*))))
                    (error (condition) (setf observed condition)))
                  (rove:ok poisoned-before-unlock)
                  (rove:ok (not (sb-thread:holding-mutex-p *wal-lock*)))
                  (rove:ok (if escape (typep observed 'error) (eq failure observed)))
                  (rove:ok (zerop (primary-wal-writer-cycles writer)))
                  (rove:ok (zerop *wal-durable-lsn*)))
             (setf (symbol-function 'wal-flush-core) flush)))))))

(defun primary-writer-authority-result (mismatch)
  (call-with-primary-writer-authority-test
   (lambda (writer)
     (let ((epoch (primary-wal-writer-epoch writer))
           (settle (symbol-function 'wal-settle-page-publications-under-lock))
           (flush (symbol-function 'wal-flush-core))
           (calls 0) (flushes 0))
       (setf *wal-end-lsn* 32 *wal-durable-lsn* 0)
       (case mismatch
         (1 (setf *wal-storage-incarnation* (gensym "REPLACED-STORE-")))
         (2 (incf *wal-insert-timeline*))
         (3 (setf *postmaster-data-lock* (list :other-directory)))
         (4 (incf *primary-wal-writer-generation*))
         (5 (setf (primary-wal-writer-thread writer) nil))
         (6 (setf (primary-wal-writer-epoch writer)
                  (make-wal-writer-epoch *wal-storage-incarnation* *wal-insert-timeline*
                                         *postmaster-data-lock*)))
         (7 (setf *primary-wal-writer* nil))
         (8 (setf *wal-ready* nil))
         (9 (setf *wal-control-metadata* (list :db-state +db-shutdowned+)))
         (10 (setf (symbol-function 'postmaster-pid-owned-p) (lambda () nil)))
         (11 (setf *physical-recovery-state* (list :recovering)))
         (12 (setf (primary-wal-writer-failure writer)
                   (make-condition 'simple-error :format-control "previous failure"))))
       (unwind-protect
            (progn
              (setf (symbol-function 'wal-settle-page-publications-under-lock)
                    (lambda () (incf calls))
                    (symbol-function 'wal-flush-core)
                    (lambda () (incf flushes) (setf *wal-durable-lsn* *wal-end-lsn*)))
              (let ((outcome
                      (handler-case
                          (sb-thread:with-mutex (*wal-lock*)
                            (primary-wal-writer-drain-under-lock writer epoch))
                        (wal-input-validation-error () :rejected))))
                (and (if (zerop mismatch)
                         (and (eq :continue outcome) (= 1 calls) (= 1 flushes)
                              (= 1 (primary-wal-writer-cycles writer)))
                         (and (eq :rejected outcome) (zerop calls) (zerop flushes)
                              (zerop (primary-wal-writer-cycles writer))))
                     (not *wal-poisoned*))))
         (setf (symbol-function 'wal-settle-page-publications-under-lock) settle
               (symbol-function 'wal-flush-core) flush))))))

(cl-spec:defproperty primary-writer-rejects-stale-authority-before-settlement
    ((mismatch (range integer 0 12)))
  (:about primary-wal-writer-drain-under-lock)
  (:trials (:normal 100))
  (primary-writer-authority-result mismatch))

(rove:deftest primary-writer-authority-precedes-storage-effects
    (loop for mismatch from 0 to 12 do
      (rove:ok (primary-writer-authority-result mismatch)))
  (unit-check-property 'primary-writer-rejects-stale-authority-before-settlement))

(rove:deftest primary-writer-cooperative-stop-skips-an-unstarted-cycle
    (call-with-primary-writer-authority-test
     (lambda (writer)
       (let ((settle (symbol-function 'wal-settle-page-publications-under-lock))
             (flush (symbol-function 'wal-flush-core))
             (calls 0) (flushes 0))
         (setf *wal-end-lsn* 32 *wal-durable-lsn* 0)
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-flush-core)
                      (lambda () (incf flushes) (setf *wal-durable-lsn* *wal-end-lsn*)))
                (setf (symbol-function 'wal-settle-page-publications-under-lock)
                      (lambda () (incf calls)))
                (request-primary-wal-writer-stop writer)
                (rove:ok (primary-wal-writer-stopping-p writer))
                (sb-thread:with-mutex (*wal-lock*)
                  (rove:ok (eq :stop (primary-wal-writer-drain-under-lock
                                      writer (primary-wal-writer-epoch writer)))))
                (rove:ok (zerop calls))
                (rove:ok (zerop (primary-wal-writer-cycles writer)))
                (rove:ok (= 0 flushes))
                (rove:ok (not *wal-poisoned*)))
           (setf (symbol-function 'wal-settle-page-publications-under-lock) settle
                 (symbol-function 'wal-flush-core) flush))))))

(rove:deftest primary-writer-cooperative-stop-allows-an-accepted-cycle-to-finish
    (call-with-primary-writer-authority-test
     (lambda (writer)
       (let ((settle (symbol-function 'wal-settle-page-publications-under-lock))
             (flush (symbol-function 'wal-flush-core))
             (calls 0) (flushes 0))
         (setf *wal-end-lsn* 32 *wal-durable-lsn* 0)
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-flush-core)
                      (lambda () (incf flushes) (setf *wal-durable-lsn* *wal-end-lsn*)))
                (setf (symbol-function 'wal-settle-page-publications-under-lock)
                      (lambda ()
                        (incf calls)
                        (request-primary-wal-writer-stop writer)))
                (rove:ok (eq :continue (primary-wal-writer-step writer)))
                (rove:ok (= 1 calls))
                (rove:ok (= 1 (primary-wal-writer-cycles writer)))
                (rove:ok (eq :stop (primary-wal-writer-step writer)))
                (rove:ok (= 1 calls))
                (rove:ok (= 1 flushes))
                (rove:ok (not *wal-poisoned*)))
           (setf (symbol-function 'wal-settle-page-publications-under-lock) settle
                 (symbol-function 'wal-flush-core) flush))))))

(rove:deftest primary-writer-drains-real-page-and-clog-prefix
    (call-with-queued-clog-test
     (lambda ()
       (call-with-primary-writer-authority-test
        (lambda (writer)
          (loop for xid in '(44 45) for block from 0 do
            (let ((page (page-init (make-page))) (*wal-row-page-write-p* t))
              (setf (backend-xid *backend*) xid (aref page 100) (+ 10 block))
              (wal-buffer-write 7310 block page))
            (sb-thread:with-mutex (*wal-lock*)
              (wal-admit-transaction-under-lock xid nil)))
          (let ((end (wal-page-publication-terminal-end *wal-pending-page-publication-tail*)))
            (rove:ok (< *wal-durable-lsn* end))
            (rove:ok (eq :continue (primary-wal-writer-step writer)))
            (rove:ok (= end (primary-wal-writer-last-drained-end writer)))
            (rove:ok (= 1 (primary-wal-writer-cycles writer)))
            (rove:ok (>= *wal-durable-lsn* end))
            (rove:ok (null *wal-pending-page-publication*))
            (rove:ok (zerop (hash-table-count *clog-pending-commits*)))
            (rove:ok (= +clog-committed+ (pending-clog-raw-status 44)))
            (rove:ok (= +clog-committed+ (pending-clog-raw-status 45)))
            (rove:ok (= 10 (aref (smgr-read 7310 0) 100)))
            (rove:ok (= 11 (aref (smgr-read 7310 1) 100)))))
        :isolate nil))))

(rove:deftest primary-writer-wakeups-coalesce-and-failed-services-stay-asleep
    (call-with-primary-writer-authority-test
     (lambda (writer)
       (sb-thread:with-mutex (*wal-lock*)
         (dotimes (index 100)
           (notify-primary-wal-writer-under-lock)))
       (rove:ok (= 1 (sb-thread:semaphore-count (primary-wal-writer-wake writer))))
       (request-primary-wal-writer-stop writer)
       (rove:ok (= 1 (sb-thread:semaphore-count (primary-wal-writer-wake writer))))
       (rove:ok (sb-thread:wait-on-semaphore (primary-wal-writer-wake writer) :timeout 0.1))
       (sb-thread:with-mutex (*primary-wal-writer-lock*)
         (setf (primary-wal-writer-wake-pending-p writer) nil
               (primary-wal-writer-stop-p writer) nil
               (primary-wal-writer-state writer) :failed
               (primary-wal-writer-failure writer)
               (make-condition 'simple-error :format-control "failed writer")))
       (sb-thread:with-mutex (*wal-lock*)
         (notify-primary-wal-writer-under-lock))
       (rove:ok (zerop (sb-thread:semaphore-count (primary-wal-writer-wake writer)))))))

(rove:deftest primary-writer-blocks-storage-adoption-until-joined
    (dolist (dead-p '(nil t))
      (call-with-primary-writer-authority-test
       (lambda (writer)
         (let ((reads 0)
               (read-control (symbol-function 'wal-read-control-fields))
               (*physical-recovery-failed-p* nil)
               (incarnation *wal-storage-incarnation*))
           (when dead-p
             (setf (primary-wal-writer-thread writer)
                   (sb-thread:make-thread #'values :name "retired writer authority test"))
             (sb-thread:join-thread (primary-wal-writer-thread writer))
             (setf (primary-wal-writer-state writer) :stopped))
           (unwind-protect
                (progn
                  (setf (symbol-function 'wal-read-control-fields)
                        (lambda () (incf reads) (error "storage inspection before writer retirement")))
                  (dolist (operation
                           (list #'wal-init #'wal-mark-primary-startup
                                 #'startup-check-cold-installation-state
                                 #'physical-recovery-open-storage
                                 #'physical-promotion-adopt-live-child-runtime
                                 (lambda () (physical-promotion-restore-cold-state nil nil))
                                 #'wal-replace-storage-incarnation))
                    (rove:ok
                     (handler-case (progn (funcall operation) nil)
                       (wal-input-validation-error () t)))
                    (rove:ok (eq incarnation *wal-storage-incarnation*))
                    (rove:ok (not *physical-recovery-failed-p*)))
                  (rove:ok (zerop reads))
                  (when dead-p
                    (setf (primary-wal-writer-joined-p writer) t)
                    (wal-replace-storage-incarnation)
                    (rove:ok (not (eq incarnation *wal-storage-incarnation*)))))
             (setf (symbol-function 'wal-read-control-fields) read-control)))))))

(defun call-with-writer-clog-reset-test (function)
  (call-with-queued-clog-test
   (lambda ()
     (call-with-primary-writer-authority-test
      (lambda (writer)
        (let ((log-reset (symbol-function 'wal-log-clog-reset-under-lock)))
          (unwind-protect
               (progn
                 (setf (symbol-function 'wal-log-clog-reset-under-lock) #'values)
                 (funcall function writer))
            (setf (symbol-function 'wal-log-clog-reset-under-lock) log-reset))))
      :isolate nil))))

(rove:deftest primary-writer-clog-reset-publishes-a-new-epoch-after-durability
    (call-with-writer-clog-reset-test
     (lambda (writer)
       (let ((old (primary-wal-writer-epoch writer))
             (incarnation *wal-storage-incarnation*)
             (thread (primary-wal-writer-thread writer))
             (generation (primary-wal-writer-generation writer))
             (syncs 0))
         (clog-set 44 +clog-aborted+ :fsync t)
         (clog-reset
          :fsync-function
          (lambda (stream)
            (rove:ok (eq old (primary-wal-writer-epoch writer)))
            (rove:ok (eq incarnation *wal-storage-incarnation*))
            (incf syncs)
            (fsync-stream stream))
          :fsync-directory-function
          (lambda (directory)
            (rove:ok (eq old (primary-wal-writer-epoch writer)))
            (rove:ok (eq incarnation *wal-storage-incarnation*))
            (incf syncs)
            (fsync-directory directory)))
         (rove:ok (= 2 syncs))
         (rove:ok (not (eq old (primary-wal-writer-epoch writer))))
         (rove:ok (eq *wal-storage-incarnation*
                      (wal-writer-epoch-incarnation (primary-wal-writer-epoch writer))))
         (rove:ok (eq thread (primary-wal-writer-thread writer)))
         (rove:ok (= generation (primary-wal-writer-generation writer)))
         (rove:ok (= +clog-in-progress+ (pending-clog-raw-status 44)))
         (sb-thread:with-mutex (*wal-lock*)
           (rove:ok
            (handler-case (progn (primary-wal-writer-drain-under-lock writer old) nil)
              (wal-input-validation-error () t))))
         (rove:ok (eq :continue (primary-wal-writer-step writer)))))))

(rove:deftest primary-writer-clog-reset-preserves-a-later-stop-request
    (call-with-writer-clog-reset-test
     (lambda (writer)
       (let ((old (primary-wal-writer-epoch writer)))
         (clog-reset :fsync-function
                     (lambda (stream)
                       (request-primary-wal-writer-stop writer)
                       (fsync-stream stream)))
         (rove:ok (not (eq old (primary-wal-writer-epoch writer))))
         (rove:ok (primary-wal-writer-stop-p writer))
         (rove:ok (eq :stopping (primary-wal-writer-state writer)))
         (rove:ok (eq :stop (primary-wal-writer-step writer)))
         (rove:ok (not *wal-poisoned*))))))

(rove:deftest primary-writer-clog-reset-failure-retains-both-old-identities
    (dolist (phase '(:before-sync :after-sync :after-directory-sync))
      (call-with-writer-clog-reset-test
       (lambda (writer)
         (let ((old (primary-wal-writer-epoch writer))
               (incarnation *wal-storage-incarnation*)
               (fail-stop (symbol-function 'wal-fail-stop))
               (failure (make-condition 'simple-error :format-control "reset durability failure"))
               (fenced nil))
           (unwind-protect
                (progn
                  (setf (symbol-function 'wal-fail-stop)
                        (lambda (condition)
                          (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
                          (setf fenced condition)
                          (error condition)))
                  (rove:ok
                   (eq failure
                       (handler-case
                           (clog-reset
                            :fsync-function
                            (lambda (stream)
                              (when (eq phase :before-sync) (error failure))
                              (fsync-stream stream)
                              (when (eq phase :after-sync) (error failure)))
                            :fsync-directory-function
                            (lambda (directory)
                              (fsync-directory directory)
                              (when (eq phase :after-directory-sync) (error failure))))
                         (error (condition) condition))))
                  (rove:ok (eq failure fenced))
                  (rove:ok *wal-poisoned*)
                  (rove:ok (eq old (primary-wal-writer-epoch writer)))
                  (rove:ok (eq incarnation *wal-storage-incarnation*)))
             (setf (symbol-function 'wal-fail-stop) fail-stop)))))))

(rove:deftest primary-writer-rejects-cold-clog-reset-before-effects
    (call-with-writer-clog-reset-test
     (lambda (writer)
       (let ((epoch (primary-wal-writer-epoch writer))
             (stream *clog-stream*)
             (effects 0))
         (rove:ok
          (handler-case
              (progn (clog-reset :wal-reset-p nil
                                 :fsync-function (lambda (out) (declare (ignore out)) (incf effects)))
                     nil)
            (wal-input-validation-error () t)))
         (rove:ok (zerop effects))
         (rove:ok (eq stream *clog-stream*))
         (rove:ok (open-stream-p stream))
         (rove:ok (eq epoch (primary-wal-writer-epoch writer)))
         (rove:ok (not *wal-poisoned*))))))

(defun primary-writer-clog-plan-result (mismatch)
  (call-with-writer-clog-reset-test
   (lambda (writer)
     (sb-thread:with-mutex (*wal-lock*)
       (let* ((plan (prepare-primary-wal-writer-clog-reset-under-lock))
              (stream *clog-stream*)
              (effects 0))
         (case mismatch
           (1 (setf *wal-storage-incarnation* (gensym "STALE-RESET-STORE-")))
           (2 (incf *wal-insert-timeline*))
           (3 (setf *postmaster-data-lock* (list :other-owner)))
           (4 (setf *primary-wal-writer* nil))
           (5 (incf *primary-wal-writer-generation*))
           (6 (setf (primary-wal-writer-epoch writer)
                    (make-wal-writer-epoch *wal-storage-incarnation*
                                           *wal-insert-timeline* *postmaster-data-lock*)))
           (7 (setf (clog-reset-epoch-plan-consumed-p plan) t))
           (8 (setf (primary-wal-writer-failure writer)
                    (make-condition 'simple-error :format-control "writer failure")))
           (9 (setf (primary-wal-writer-thread writer) nil)))
         (when (>= mismatch 10)
           (let ((new (case mismatch
                        (13 *wal-storage-incarnation*)
                        (14 nil)
                        (t (clog-reset-epoch-plan-new-incarnation plan)))))
             (setf plan
                   (%make-clog-reset-epoch-plan
                    writer *primary-wal-writer-generation* (primary-wal-writer-thread writer)
                    (primary-wal-writer-epoch writer) *wal-storage-incarnation*
                    new
                    *wal-insert-timeline* *postmaster-data-lock*
                    (make-wal-writer-epoch
                     (if (= mismatch 10) (gensym "WRONG-RESET-EPOCH-") new)
                     (if (= mismatch 11) (1+ *wal-insert-timeline*) *wal-insert-timeline*)
                     (if (= mismatch 12) (list :other-owner) *postmaster-data-lock*))))))
         (let* ((incarnation *wal-storage-incarnation*)
                (epoch (primary-wal-writer-epoch writer))
                (outcome
                  (handler-case
                      (progn
                        (clog-reset-file-under-wal-lock
                         (lambda (out) (incf effects) (fsync-stream out))
                         (lambda (directory) (incf effects) (fsync-directory directory))
                         plan)
                        :published)
                    (wal-input-validation-error () :rejected))))
           (if (zerop mismatch)
               (and (eq :published outcome) (= 2 effects)
                    (clog-reset-epoch-plan-consumed-p plan)
                    (not (eq incarnation *wal-storage-incarnation*))
                    (not (eq epoch (primary-wal-writer-epoch writer)))
                    (handler-case
                        (progn (publish-primary-wal-writer-clog-reset-under-lock plan) nil)
                      (wal-input-validation-error () t)))
               (and (eq :rejected outcome) (zerop effects)
                    (eq incarnation *wal-storage-incarnation*)
                    (eq epoch (primary-wal-writer-epoch writer))
                    (eq stream *clog-stream*) (open-stream-p stream)))))))))

(cl-spec:defproperty stale-clog-reset-plans-have-no-storage-effects
    ((mismatch (range integer 0 14)))
  (:about validate-primary-wal-writer-clog-reset-plan-under-lock)
  (:trials (:normal 100))
  (primary-writer-clog-plan-result mismatch))

(rove:deftest primary-writer-clog-plan-rejects-stale-and-replayed-authority
    (loop for mismatch from 0 to 14 do
      (rove:ok (primary-writer-clog-plan-result mismatch)))
  (unit-check-property 'stale-clog-reset-plans-have-no-storage-effects))

(rove:deftest primary-writer-clog-reset-nonlocal-exit-retains-old-epochs
    (call-with-writer-clog-reset-test
     (lambda (writer)
       (let ((old (primary-wal-writer-epoch writer))
             (incarnation *wal-storage-incarnation*)
             (fail-stop (symbol-function 'wal-fail-stop))
             (shutdown (symbol-function 'request-fast-shutdown))
             (stops 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-fail-stop)
                      (lambda (condition)
                        (assert (not (sb-thread:holding-mutex-p *wal-lock*)))
                        (incf stops)
                        (funcall fail-stop condition))
                      (symbol-function 'request-fast-shutdown) #'values)
                (rove:ok
                 (typep
                  (handler-case
                      (catch 'leave-reset
                        (clog-reset :fsync-function
                                    (lambda (stream)
                                      (fsync-stream stream)
                                      (throw 'leave-reset :left))))
                    (error (condition) condition))
                  'error))
                (rove:ok (= 1 stops))
                (rove:ok *wal-poisoned*)
                (rove:ok (eq old (primary-wal-writer-epoch writer)))
                (rove:ok (eq incarnation *wal-storage-incarnation*)))
           (setf (symbol-function 'wal-fail-stop) fail-stop
                 (symbol-function 'request-fast-shutdown) shutdown))))))

(rove:deftest primary-writer-stopped-before-reset-rejects-before-publication
    (call-with-writer-clog-reset-test
     (lambda (writer)
       (let ((old (primary-wal-writer-epoch writer))
             (stream *clog-stream*)
             (incarnation *wal-storage-incarnation*)
             (effects 0))
         (request-primary-wal-writer-stop writer)
         (rove:ok
          (handler-case
              (progn (clog-reset :fsync-function
                                 (lambda (out) (declare (ignore out)) (incf effects)))
                     nil)
            (wal-input-validation-error () t)))
         (rove:ok (zerop effects))
         (rove:ok (eq stream *clog-stream*))
         (rove:ok (open-stream-p stream))
         (rove:ok (eq old (primary-wal-writer-epoch writer)))
         (rove:ok (eq incarnation *wal-storage-incarnation*))
         (rove:ok (not *wal-poisoned*))))))

(rove:deftest primary-writer-clog-reset-logs-dirty-scratch-before-requiring-cleanup
    (call-with-queued-clog-test
     (lambda ()
       (call-with-primary-writer-authority-test
        (lambda (writer)
          (let ((page (page-init (make-page)))
                (*wal-row-page-write-p* t)
                (old (primary-wal-writer-epoch writer)))
            (setf (aref page 100) 73)
            (wal-buffer-write 7310 0 page)
            (rove:ok (plusp (wal-dirty-store-disk-refs *wal-dirty-store*)))
            (sb-thread:with-mutex (*wal-lock*)
              (rove:ok (not (query-temp-cleanup-domain-empty-p *wal-temp-cleanup-domain*))))
            (clog-reset)
            (rove:ok (= 73 (aref (smgr-read 7310 0) 100)))
            (rove:ok (zerop (hash-table-count *wal-page-cache*)))
            (rove:ok (null *wal-dirty-store*))
            (sb-thread:with-mutex (*wal-lock*)
              (rove:ok (query-temp-cleanup-domain-empty-p *wal-temp-cleanup-domain*)))
            (rove:ok (not (eq old (primary-wal-writer-epoch writer))))
            (rove:ok (eq :continue (primary-wal-writer-step writer)))))
        :isolate nil))))
