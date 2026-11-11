(in-package #:pg)

(defun call-with-logical-policy-test (function)
  (let ((*wal-logical-policy* nil) (*logical-capture-force* nil)
                                   (*wal-level* :replica) (*wal-logical-policy-test-hook* nil)
                                   (*logical-capture-live-slots* 0)
                                   (*logical-capture-arming* (make-hash-table :test #'eq))
                                   (*logical-capture-xid-modes* (make-hash-table :test #'eql)))
    (call-with-writer-clog-reset-test
     (lambda (writer)
       (let ((*backend* nil))
         (funcall function writer))))))

(cl-spec:defproperty logical-policy-epoch-transfer-preserves-state
    ((status (member :unknown nil t)) (pending (member nil t)))
  (:about wal-logical-policy-rebind-incarnation)
  (:trials (:normal 100))
  (let* ((old (gensym "LOGICAL-OLD-")) (new (gensym "LOGICAL-NEW-"))
                                       (state (wal-logical-status-from-checkpoint
                                               (unit-logical-status-snapshot status nil nil :unknown)))
                                       (pending (and pending state (wal-logical-status-enabled state)))
                                       (policy (%make-wal-logical-policy old state pending))
                                       (copy (wal-logical-policy-rebind-incarnation policy old new)))
    (and (not (eq policy copy))
         (eq old (wal-logical-policy-incarnation policy))
         (eq new (wal-logical-policy-incarnation copy))
         (eq state (wal-logical-policy-state copy))
         (eq pending (wal-logical-policy-pending-disable copy)))))

(rove:deftest logical-policy-publication-and-pending-disable-keep-capture
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (rove:ok (= 65 (wal-report-logical-decoding-status t)))
       (rove:ok *logical-capture-force*)
       (let ((slot (make-wal-replication-slot :name "logical" :kind :logical :restart-lsn 0)))
         (sb-thread:with-mutex (*wal-lock*)
           (setf (gethash "logical" *wal-replication-slots*) slot)
           (wal-refresh-logical-capture-slot-mirror-under-wal-lock)
           (rove:ok (not (wal-logical-policy-pending-disable *wal-logical-policy*)))
           (remhash "logical" *wal-replication-slots*)
           (wal-refresh-logical-capture-slot-mirror-under-wal-lock)
           (rove:ok (wal-logical-policy-pending-disable *wal-logical-policy*))
           (rove:ok (eql 0 *logical-capture-live-slots*))
           (rove:ok *logical-capture-force*)))
       (let ((first (sb-thread:with-mutex (*xid-lock*) (xid-allocate-under-lock))))
         (rove:ok (eq :capture (gethash first *logical-capture-xid-modes*)))
         (with-wal-publication-fence (begin)
           (wal-process-pending-logical-disable-under-lock #'begin))
         (rove:ok (not *logical-capture-force*))
         (rove:ok (not (wal-logical-policy-pending-disable *wal-logical-policy*)))
         (rove:ok (eq :capture (gethash first *logical-capture-xid-modes*)))
         (let ((next (sb-thread:with-mutex (*xid-lock*) (xid-allocate-under-lock))))
           (rove:ok (eq :skip (gethash next *logical-capture-xid-modes*))))))))

(rove:deftest logical-policy-disable-defers-an-arm-without-losing-the-request
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (wal-report-logical-decoding-status t)
       (let ((token (gensym "PENDING-ARM-")) (end *wal-end-lsn*))
         (sb-thread:with-mutex (*xid-lock*) (setf (gethash token *logical-capture-arming*) t))
         (with-wal-publication-fence (begin)
           (rove:ok (eq :deferred (wal-process-pending-logical-disable-under-lock #'begin))))
         (rove:ok (= end *wal-end-lsn*))
         (rove:ok *logical-capture-force*)
         (rove:ok (wal-logical-policy-pending-disable *wal-logical-policy*))
         (sb-thread:with-mutex (*xid-lock*) (remhash token *logical-capture-arming*))
         (with-wal-publication-fence (begin)
           (wal-process-pending-logical-disable-under-lock #'begin))
         (rove:ok (> *wal-end-lsn* end))
         (rove:ok (not *logical-capture-force*))))))

(rove:deftest logical-policy-physical-slots-do-not-enable-capture
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (wal-report-logical-decoding-status nil)
       (sb-thread:with-mutex (*wal-lock*)
         (setf (gethash "physical" *wal-replication-slots*)
               (make-wal-replication-slot :name "physical" :kind :physical))
         (wal-refresh-logical-capture-slot-mirror-under-wal-lock)
         (rove:ok (equal '(:logical-decoding-enabled nil)
                         (wal-current-logical-decoding-fields)))
         (rove:ok (not *logical-capture-force*))
         (rove:ok (not (wal-logical-policy-pending-disable *wal-logical-policy*)))))))

(rove:deftest logical-policy-clog-reset-preserves-status-and-pending-disable
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (wal-report-logical-decoding-status t)
       (let ((old *wal-logical-policy*) (state (wal-logical-policy-state *wal-logical-policy*)))
         (clog-reset)
         (rove:ok (not (eq old *wal-logical-policy*)))
         (rove:ok (not (eq (wal-logical-policy-incarnation old) *wal-storage-incarnation*)))
         (rove:ok (eq *wal-storage-incarnation* (wal-logical-policy-incarnation *wal-logical-policy*)))
         (rove:ok (eq state (wal-logical-policy-state *wal-logical-policy*)))
         (rove:ok (wal-logical-policy-pending-disable *wal-logical-policy*))
         (rove:ok *logical-capture-force*)))))

(rove:deftest logical-policy-disable-tolerates-an-arm-between-rechecks
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (wal-report-logical-decoding-status t)
       (let ((count (symbol-function 'wal-logical-arm-count-under-lock))
             (calls 0) (token (gensym "RACING-ARM-")) (end *wal-end-lsn*))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-logical-arm-count-under-lock)
                      (lambda ()
                        (incf calls)
                        (when (= calls 2)
                          (sb-thread:with-mutex (*xid-lock*)
                            (setf (gethash token *logical-capture-arming*) t)))
                        (funcall count)))
                (with-wal-publication-fence (begin)
                  (rove:ok (eq :deferred
                               (wal-process-pending-logical-disable-under-lock #'begin))))
                (rove:ok (= 2 calls))
                (rove:ok (= end *wal-end-lsn*))
                (rove:ok (wal-logical-policy-pending-disable *wal-logical-policy*))
                (rove:ok *logical-capture-force*)
                (rove:ok (not *wal-poisoned*)))
           (setf (symbol-function 'wal-logical-arm-count-under-lock) count)
           (sb-thread:with-mutex (*xid-lock*)
             (remhash token *logical-capture-arming*)))))))

(rove:deftest logical-policy-checkpoint-fences-incomplete-disable
    (dolist (point '(:before-record :after-record :after-flush :after-state))
      (dolist (nonlocal '(nil t))
        (call-with-logical-policy-test
         (lambda (writer)
           (declare (ignore writer))
           (wal-report-logical-decoding-status t)
           (let ((shutdown (symbol-function 'request-fast-shutdown))
                 (stops 0)
                 (*wal-logical-policy-test-hook*
                   (lambda (stage candidate)
                     (declare (ignore candidate))
                     (when (eq stage point)
                       (if nonlocal (throw 'logical-checkpoint-exit :escaped)
                           (error "injected checkpoint logical status failure"))))))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'request-fast-shutdown) (lambda () (incf stops)))
                    (rove:ok (unit-commit-ts-error-p
                              (lambda () (catch 'logical-checkpoint-exit (wal-checkpoint-core)))))
                    (rove:ok *wal-poisoned*)
                    (rove:ok (plusp stops))
                    (rove:ok (zerop *wal-checkpoint-lsn*)))
               (setf (symbol-function 'request-fast-shutdown) shutdown))))))))

(rove:deftest logical-policy-stale-clog-plan-has-no-file-effects
    (call-with-logical-policy-test
     (lambda (writer)
       (declare (ignore writer))
       (wal-report-logical-decoding-status t)
       (sb-thread:with-mutex (*wal-lock*)
         (let ((plan (prepare-primary-wal-writer-clog-reset-under-lock))
               (incarnation *wal-storage-incarnation*) (effects 0))
           (wal-logical-set-pending-under-lock *wal-logical-policy* nil)
           (rove:ok
            (handler-case
                (progn
                  (clog-reset-file-under-wal-lock
                   (lambda (stream) (declare (ignore stream)) (incf effects))
                   (lambda (directory) (declare (ignore directory)) (incf effects))
                   plan)
                  nil)
              (wal-input-validation-error () t)))
           (rove:ok (zerop effects))
           (rove:ok (eq incarnation *wal-storage-incarnation*))
           (rove:ok (not *wal-poisoned*)))))))

(rove:deftest logical-policy-pending-disable-requires-enabled-state
    (let ((owner (gensym "PENDING-OWNER-")) (target (gensym "PENDING-TARGET-")))
      (dolist (status '(:unknown nil))
        (let* ((state (wal-logical-status-from-checkpoint
                       (unit-logical-status-snapshot status nil nil :unknown)))
               (policy (%make-wal-logical-policy owner state t)))
          (rove:ok (not (wal-logical-policy-valid-p policy)))
          (rove:ok (unit-commit-ts-error-p
                    (lambda () (wal-logical-policy-rebind-incarnation policy owner target))))))))

(rove:deftest logical-policy-generated-epoch-transfer
    (unit-check-property 'logical-policy-epoch-transfer-preserves-state))
