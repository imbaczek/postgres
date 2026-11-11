(in-package #:pg)

(defun unit-clog-fpw-state (incarnation enabled &optional (timeline *wal-insert-timeline*))
  (let ((snapshot (unit-fpw-snapshot enabled nil nil)))
    (setf (getf snapshot :timeline) timeline (getf snapshot :prev-timeline) timeline)
    (wal-fpw-state-from-checkpoint snapshot incarnation)))

(cl-spec:defproperty fpw-epoch-transfer-preserves-evidence
    ((enabled (member nil t)))
  (:about wal-fpw-rebind-incarnation)
  (:trials (:normal 100))
  (let* ((old (gensym "OLD-FPW-")) (new (gensym "NEW-FPW-"))
                                   (state (unit-clog-fpw-state old enabled))
                                   (copy (wal-fpw-rebind-incarnation state old new))
                                   (restored (wal-fpw-rebind-incarnation copy new old)))
    (and (equalp state restored) (not (eq state copy))
         (eq old (wal-fpw-state-incarnation state))
         (eq new (wal-fpw-state-incarnation copy)))))

(rove:deftest clog-reset-fpw-transfer-preserves-known-and-unavailable-evidence
    (let ((old (gensym "OLD-FPW-")) (new (gensym "NEW-FPW-")))
      (dolist (state '(nil :unrecovered))
        (rove:ok (eq state (wal-fpw-rebind-incarnation state old new))))
      (dolist (enabled '(nil t))
        (let* ((seed (unit-clog-fpw-state old t 3))
               (state (wal-fpw-state-next seed (unit-fpw-change-record enabled) 3 old))
               (copy (wal-fpw-rebind-incarnation state old new)))
          (rove:ok (eq enabled (wal-fpw-state-enabled copy)))
          (rove:ok (= (wal-fpw-state-start state) (wal-fpw-state-start copy)))
          (rove:ok (= (wal-fpw-state-end state) (wal-fpw-state-end copy)))
          (rove:ok (= (wal-fpw-state-coverage-start state) (wal-fpw-state-coverage-start copy)))
          (rove:ok (eql (wal-fpw-state-last-disable-start state)
                        (wal-fpw-state-last-disable-start copy)))
          (rove:ok (eq old (wal-fpw-state-incarnation state)))
          (rove:ok (eq new (wal-fpw-state-incarnation copy)))
          (rove:ok (unit-commit-ts-error-p
                    (lambda () (wal-fpw-rebind-incarnation state new old)))))))
  (unit-check-property 'fpw-epoch-transfer-preserves-evidence))

(rove:deftest clog-reset-publishes-fpw-policy-with-new-writer-identity
    (dolist (enabled '(nil t))
      (call-with-writer-clog-reset-test
       (lambda (writer)
         (let* ((*wal-end-lsn* 512) (*wal-durable-lsn* 512)
                                    (*wal-fpw-state* (unit-clog-fpw-state *wal-storage-incarnation* enabled))
                                    (old *wal-storage-incarnation*)
                                    (before *wal-fpw-state*))
           (clog-reset
            :fsync-function
            (lambda (stream)
              (rove:ok (eq old *wal-storage-incarnation*))
              (rove:ok (eq before *wal-fpw-state*))
              (fsync-stream stream)))
           (rove:ok (not (eq old *wal-storage-incarnation*)))
           (rove:ok (eq old (wal-fpw-state-incarnation before)))
           (rove:ok (eq *wal-storage-incarnation*
                        (wal-writer-epoch-incarnation (primary-wal-writer-epoch writer))))
           (sb-thread:with-mutex (*wal-lock*)
             (rove:ok (eq enabled
                          (wal-fpw-state-enabled (wal-checked-fpw-state-under-lock))))))))))

(rove:deftest clog-reset-plan-rejects-changed-fpw-before-file-effects
    (call-with-writer-clog-reset-test
     (lambda (writer)
       (declare (ignore writer))
       (let ((*wal-end-lsn* 512) (*wal-durable-lsn* 512)
                                 (*wal-fpw-state* (unit-clog-fpw-state *wal-storage-incarnation* t))
                                 (effects 0))
         (sb-thread:with-mutex (*wal-lock*)
           (let ((plan (prepare-primary-wal-writer-clog-reset-under-lock)))
             (setf *wal-fpw-state* (unit-clog-fpw-state *wal-storage-incarnation* nil))
             (rove:ok
              (handler-case
                  (progn
                    (clog-reset-file-under-wal-lock
                     (lambda (stream) (declare (ignore stream)) (incf effects))
                     (lambda (directory) (declare (ignore directory)) (incf effects))
                     plan)
                    nil)
                (wal-input-validation-error () t)))
             (rove:ok (zerop effects))))))))

(rove:deftest clog-reset-recovery-transfer-requires-a-completed-reset
    (let* ((old (gensym "OLD-RECOVERY-")) (new (gensym "NEW-RECOVERY-"))
                                          (record (make-wal-record :type +wal-record-clog-reset+))
                                          (other (make-wal-record :type +wal-record-maintenance+))
                                          (state (unit-clog-fpw-state old nil 3))
                                          (*wal-storage-incarnation* old))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (wal-recovery-policy-incarnation state old record t))))
      (setf *wal-storage-incarnation* new)
      (dolist (prior (list nil state))
        (multiple-value-bind (copy incarnation)
            (wal-recovery-policy-incarnation prior old record t)
          (rove:ok (eq new incarnation))
          (if prior
              (progn
                (rove:ok (eq new (wal-fpw-state-incarnation copy)))
                (rove:ok (eq old (wal-fpw-state-incarnation state)))
                (rove:ok (null (wal-fpw-state-enabled copy))))
              (rove:ok (null copy)))))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (wal-recovery-policy-incarnation state old record nil))))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (wal-recovery-policy-incarnation state old other t))))
      (setf *wal-storage-incarnation* old)
      (multiple-value-bind (copy incarnation)
          (wal-recovery-policy-incarnation state old other t)
        (rove:ok (eq state copy))
        (rove:ok (eq old incarnation)))))

(rove:deftest clog-reset-primary-recovery-installs-policy-under-final-identity
    (dolist (enabled '(:unknown nil t))
      (let* ((*wal-lock* (sb-thread:make-mutex :name "CLOG policy replay"))
             (*wal-storage-incarnation* (gensym "CLOG-REPLAY-"))
             (old *wal-storage-incarnation*)
             (*wal-checkpoint-snapshot* (unit-fpw-snapshot enabled nil nil))
             (*wal-checkpoint-lsn* (getf *wal-checkpoint-snapshot* :end))
             (*wal-insert-timeline* 3) (*wal-end-lsn* 512) (*wal-durable-lsn* 512)
             (*wal-ready* t) (*wal-poisoned* nil) (*wal-fpw-state* :unrecovered)
             (*wal-logical-policy* nil) (*logical-capture-force* nil)
             (*wal-logical-info* nil) (*wal-logical-info-disable-pending* nil)
             (*wal-logical-info-activations* (make-hash-table :test #'eq))
             (record (make-wal-record :version 8 :type +wal-record-clog-reset+
                                      :xid 3 :start 216 :end 256
                                      :payload (make-array 8 :element-type '(unsigned-byte 8)
                                                             :initial-element 0)))
             (names '(wal-scan wal-replay-record clog-fsync))
             (originals (mapcar #'symbol-function names))
             (syncs 0))
        (unwind-protect
             (progn
               (setf (symbol-function 'wal-scan)
                     (lambda (function &rest options)
                       (declare (ignore options)) (funcall function record))
                     (symbol-function 'wal-replay-record)
                     (lambda (record)
                       (declare (ignore record))
                       (setf *wal-storage-incarnation* (gensym "CLOG-REPLAY-NEW-")
                             *wal-recovery-group-active* nil))
                     (symbol-function 'clog-fsync) (lambda () (incf syncs)))
               (wal-recover)
               (rove:ok (= 1 syncs))
               (rove:ok (not (eq old *wal-storage-incarnation*)))
               (if (eq enabled :unknown)
                   (rove:ok (null *wal-fpw-state*))
                   (progn
                     (rove:ok (eq enabled (wal-fpw-state-enabled *wal-fpw-state*)))
                     (rove:ok (eq *wal-storage-incarnation*
                                  (wal-fpw-state-incarnation *wal-fpw-state*)))
                     (rove:ok (= (getf *wal-checkpoint-snapshot* :end)
                                 (wal-fpw-state-end *wal-fpw-state*))))))
          (loop for name in names for original in originals
                do (setf (symbol-function name) original))))))
