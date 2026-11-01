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
         (*postmaster-data-lock* (list :owned-directory))
         (*physical-recovery-state* nil)
         (*wal-ready* t)
         (*wal-poisoned* nil)
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

(defun primary-writer-authority-result (mismatch)
  (call-with-primary-writer-authority-test
   (lambda (writer)
     (let ((epoch (primary-wal-writer-epoch writer))
           (settle (symbol-function 'wal-settle-page-publications-under-lock))
           (calls 0))
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
                    (lambda () (incf calls)))
              (let ((outcome
                      (handler-case
                          (sb-thread:with-mutex (*wal-lock*)
                            (primary-wal-writer-drain-under-lock writer epoch))
                        (wal-input-validation-error () :rejected))))
                (and (if (zerop mismatch)
                         (and (eq :continue outcome) (= 1 calls)
                              (= 1 (primary-wal-writer-cycles writer)))
                         (and (eq :rejected outcome) (zerop calls)
                              (zerop (primary-wal-writer-cycles writer))))
                     (not *wal-poisoned*))))
         (setf (symbol-function 'wal-settle-page-publications-under-lock) settle))))))

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
             (calls 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-settle-page-publications-under-lock)
                      (lambda () (incf calls)))
                (request-primary-wal-writer-stop writer)
                (rove:ok (primary-wal-writer-stopping-p writer))
                (sb-thread:with-mutex (*wal-lock*)
                  (rove:ok (eq :stop (primary-wal-writer-drain-under-lock
                                      writer (primary-wal-writer-epoch writer)))))
                (rove:ok (zerop calls))
                (rove:ok (zerop (primary-wal-writer-cycles writer)))
                (rove:ok (not *wal-poisoned*)))
           (setf (symbol-function 'wal-settle-page-publications-under-lock) settle))))))

(rove:deftest primary-writer-cooperative-stop-allows-an-accepted-cycle-to-finish
    (call-with-primary-writer-authority-test
     (lambda (writer)
       (let ((settle (symbol-function 'wal-settle-page-publications-under-lock))
             (calls 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-settle-page-publications-under-lock)
                      (lambda ()
                        (incf calls)
                        (request-primary-wal-writer-stop writer)))
                (rove:ok (eq :continue (primary-wal-writer-step writer)))
                (rove:ok (= 1 calls))
                (rove:ok (= 1 (primary-wal-writer-cycles writer)))
                (rove:ok (eq :stop (primary-wal-writer-step writer)))
                (rove:ok (= 1 calls))
                (rove:ok (not *wal-poisoned*)))
           (setf (symbol-function 'wal-settle-page-publications-under-lock) settle))))))

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
