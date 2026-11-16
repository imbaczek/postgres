(in-package #:pg)

(declaim (special *backend-lifetimes*))

(defun call-with-backend-lifetime-test (function)
  (let ((*backend-registry* (make-hash-table :test #'eql :synchronized t))
        (*backend-lifetimes* (make-hash-table :test #'eq :synchronized t))
        (*backend-lifetime-seals* (make-hash-table :test #'eq :synchronized t))
        (*backend-lifetime-revocations* (make-hash-table :test #'eq :synchronized t))
        (*backend-admission-classes* (make-hash-table :test #'eq))
        (*n-backends* 0)
        (*live-operation-backend-query-tokens* (make-hash-table :test #'eq))
        (*live-operation-cleanup-backends* nil)
        (*live-operation-admission-open* t)
        (*wal-logical-info* nil) (*wal-ready* nil) (*backend* nil))
    (unwind-protect (funcall function)
      (dolist (backend (list-backends)) (unregister-backend backend)))))

(defun backend-lifetime-test-backend ()
  (make-backend :pid (sb-thread:thread-os-tid sb-thread:*current-thread*)
                :thread sb-thread:*current-thread*))

(defun backend-lifetime-test-capture (backend)
  (sb-thread:with-mutex (*wal-lock*)
    (uiop:symbol-call :pg :backend-lifetime-capture-under-wal-lock backend)))

(defun backend-lifetime-test-check (token)
  (sb-thread:with-mutex (*wal-lock*)
    (uiop:symbol-call :pg :backend-lifetime-check-under-wal-lock token)))

(defun backend-lifetime-test-refused-p (token)
  (handler-case (progn (backend-lifetime-test-check token) nil) (error () t)))

(cl-spec:defproperty backend-lifetimes-follow-registration-not-object-reuse
    ((count (range integer 1 6)))
  (:about backend-lifetime-capture-under-wal-lock)
  (:trials (:normal 80))
  (call-with-backend-lifetime-test
   (lambda ()
     (let ((backend (backend-lifetime-test-backend)) (old nil) (valid t))
       (loop repeat count
             do (setf (backend-thread backend) sb-thread:*current-thread*)
                (register-backend backend)
                (let ((token (backend-lifetime-test-capture backend)))
                  (setf valid
                        (and valid (not (eq token old))
                             (eq token (backend-lifetime-test-check token))
                             (or (null old) (backend-lifetime-test-refused-p old))))
                  (unregister-backend backend)
                  (setf old token)))
       (and valid (zerop *n-backends*)
            (backend-lifetime-test-refused-p old))))))

(cl-spec:defproperty backend-lifetimes-reject-copies-and-reused-pids
    ((ignored (range integer 0 65535)))
  (:about backend-lifetime-check-under-wal-lock)
  (:trials (:normal 80))
  (declare (ignore ignored))
  (call-with-backend-lifetime-test
   (lambda ()
     (let ((first (backend-lifetime-test-backend))
           (second (backend-lifetime-test-backend)))
       (register-backend first)
       (let* ((token (backend-lifetime-test-capture first))
              (copy (copy-structure token)))
         (and (backend-lifetime-test-refused-p copy)
              (eq token (backend-lifetime-test-check token))
              (progn
                (unregister-backend first)
                (register-backend second)
                (let ((next (backend-lifetime-test-capture second)))
                  (and (not (eq next token))
                       (backend-lifetime-test-refused-p token)
                       (eq next (backend-lifetime-test-check next)))))))))))

(cl-spec:defproperty backend-lifetimes-fence-observed-registry-substitution
    ((mode (range integer 0 5)))
  (:about backend-lifetime-check-under-wal-lock)
  (:trials (:normal 80))
  (call-with-backend-lifetime-test
   (lambda ()
     (let* ((backend (backend-lifetime-test-backend))
            (registry *backend-registry*))
       (register-backend backend)
       (let* ((token (backend-lifetime-test-capture backend))
              (refused
                (ecase mode
                  (0 (let ((*backend-registry* (make-hash-table :test #'eql :synchronized t)))
                       (setf (gethash (backend-pid backend) *backend-registry*) backend)
                       (backend-lifetime-test-refused-p token)))
                  (1 (let ((foreign (backend-lifetime-test-backend)))
                       (sb-thread:with-mutex (*backend-registry-lock*)
                         (setf (gethash (backend-pid backend) registry) foreign))
                       (unwind-protect (backend-lifetime-test-refused-p token)
                         (sb-thread:with-mutex (*backend-registry-lock*)
                           (setf (gethash (backend-pid backend) registry) backend)))))
                  (2 (remhash backend *backend-lifetimes*)
                   (unwind-protect (backend-lifetime-test-refused-p token)
                     (setf (gethash backend *backend-lifetimes*) token)))
                  (3 (let ((seal (gethash token *backend-lifetime-seals*)))
                       (remhash token *backend-lifetime-seals*)
                       (unwind-protect (backend-lifetime-test-refused-p token)
                         (setf (gethash token *backend-lifetime-seals*) seal))))
                  (4 (let ((pid (backend-pid backend)))
                       (setf (backend-pid backend) 0)
                       (unwind-protect (backend-lifetime-test-refused-p token)
                         (setf (backend-pid backend) pid))))
                  (5 (let ((thread (backend-thread backend)))
                       (setf (backend-thread backend) nil)
                       (unwind-protect (backend-lifetime-test-refused-p token)
                         (setf (backend-thread backend) thread)))))))
         (and refused (backend-lifetime-test-refused-p token)))))))

(cl-spec:defproperty backend-lifetimes-do-not-authorize-synthetic-registration
    ((mode (range integer 0 2)))
  (:about backend-lifetime-capture-under-wal-lock)
  (:trials (:normal 80))
  (call-with-backend-lifetime-test
   (lambda ()
     (let ((backend (make-backend
                     :pid (if (zerop mode) 0
                              (sb-thread:thread-os-tid sb-thread:*current-thread*))
                     :thread (when (zerop mode) sb-thread:*current-thread*))))
       (when (= mode 2)
         (setf *backend-registry* (make-hash-table :test #'eql)
               (backend-thread backend) sb-thread:*current-thread*))
       (register-backend backend)
       (and (= 1 *n-backends*)
            (handler-case (progn (backend-lifetime-test-capture backend) nil)
              (error () t)))))))

(rove:deftest backend-lifetime-rejects-fabricated-validator
    (call-with-backend-lifetime-test
     (lambda ()
       (let* ((backend (backend-lifetime-test-backend))
              (called nil)
              (forged (uiop:symbol-call
                       :pg :%make-backend-lifetime
                       :backend backend
                       :state (lambda (candidate action)
                                (declare (ignore action))
                                (setf called t)
                                candidate))))
         (register-backend backend)
         (let ((token (backend-lifetime-test-capture backend)))
           (rove:ok (backend-lifetime-test-refused-p forged))
           (rove:ok (not called))
           (rove:ok (eq token (backend-lifetime-test-check token))))))))

(rove:deftest backend-lifetime-unregister-rejects-installed-forgery
    (dolist (kind '(:callback :nil-state :non-token))
      (call-with-backend-lifetime-test
       (lambda ()
         (let ((backend (backend-lifetime-test-backend)) (called nil))
           (register-backend backend)
           (let* ((original (backend-lifetime-test-capture backend))
                  (seal (gethash original *backend-lifetime-seals*))
                  (forged
                    (if (eq kind :non-token) 42
                        (uiop:symbol-call
                         :pg :%make-backend-lifetime :backend backend
                         :state (when (eq kind :callback)
                                  (lambda (candidate action)
                                    (declare (ignore action))
                                    (setf called t)
                                    candidate))))))
             (setf (gethash backend *backend-lifetimes*) forged)
             (rove:ok (backend-lifetime-test-refused-p forged))
             (unregister-backend backend)
             (rove:ok (not called))
             (rove:ok (zerop *n-backends*))
             (rove:ok (null (gethash (backend-pid backend) *backend-registry*)))
             (setf (backend-thread backend) sb-thread:*current-thread*
                   (gethash backend *backend-lifetimes*) original
                   (gethash original *backend-lifetime-seals*) seal
                   (gethash (backend-pid backend) *backend-registry*) backend
                   *n-backends* 1)
             (rove:ok (backend-lifetime-test-refused-p original))))))))

(rove:deftest backend-lifetime-stale-rejection-masks-interrupts
    (call-with-backend-lifetime-test
     (lambda ()
       (let ((backend (backend-lifetime-test-backend)) (masked nil))
         (register-backend backend)
         (let ((token (backend-lifetime-test-capture backend)))
           (let ((*backend-registry* (make-hash-table :test #'eql :synchronized t)))
             (rove:ok
              (handler-case
                  (handler-bind ((error (lambda (condition)
                                          (declare (ignore condition))
                                          (setf masked (not sb-sys:*interrupts-enabled*)))))
                    (backend-lifetime-test-check token)
                    nil)
                (error () t))))
           (rove:ok masked)
           (rove:ok (backend-lifetime-test-refused-p token)))))))

(rove:deftest backend-lifetime-rejects-dead-owner-without-unregister
    (let* ((thread
             (sb-thread:make-thread
              (lambda ()
                (let ((*backend-registry* (make-hash-table :test #'eql :synchronized t))
                      (*backend-lifetimes* (make-hash-table :test #'eq :synchronized t))
                      (*backend-lifetime-seals* (make-hash-table :test #'eq :synchronized t))
                      (*backend-admission-classes* (make-hash-table :test #'eq))
                      (*n-backends* 0) (*live-operation-admission-open* t)
                      (*wal-ready* nil) (*wal-logical-info* nil) (*backend* nil))
                  (let ((backend (backend-lifetime-test-backend)))
                    (register-backend backend)
                    (list backend (backend-lifetime-test-capture backend)
                          *backend-registry* *backend-lifetimes*
                          *backend-lifetime-seals* *backend-admission-classes*
                          *n-backends*))))))
           (bundle (sb-thread:join-thread thread)))
      (destructuring-bind
          (backend token registry lifetimes seals classes count) bundle
        (let ((*backend-registry* registry) (*backend-lifetimes* lifetimes)
                                            (*backend-lifetime-seals* seals) (*backend-admission-classes* classes)
                                            (*n-backends* count)
                                            (*live-operation-backend-query-tokens* (make-hash-table :test #'eq)))
          (unwind-protect
               (progn
                 (rove:ok (not (sb-thread:thread-alive-p thread)))
                 (rove:ok (eq backend (gethash (backend-pid backend) registry)))
                 (rove:ok (backend-lifetime-test-refused-p token)))
            (unregister-backend backend))
          (rove:ok (zerop *n-backends*))))))

(rove:deftest backend-lifetime-generated-contracts
    (dolist (entry '((backend-lifetimes-follow-registration-not-object-reuse 11801)
                     (backend-lifetimes-reject-copies-and-reused-pids 11802)
                     (backend-lifetimes-fence-observed-registry-substitution 11803)
                     (backend-lifetimes-do-not-authorize-synthetic-registration 11804)))
      (let ((result (cl-spec:result-data
                     (cl-spec:run-property (first entry) :seed (second entry) :profile :normal))))
        (let ((*print-length* nil) (*print-level* nil))
          (format t "~&BACKEND-LIFETIME-CONTRACT ~s~%" result))
        (rove:ok (eq :passed (getf result :status)))
        (rove:ok (= 80 (getf result :trials))))))

(rove:deftest backend-lifetime-observation-does-not-borrow-owner-or-registry-lock
    (call-with-backend-lifetime-test
     (lambda ()
       (let ((backend (backend-lifetime-test-backend)))
         (register-backend backend)
         (let* ((token (backend-lifetime-test-capture backend))
                (registry *backend-registry*) (lifetimes *backend-lifetimes*)
                (seals *backend-lifetime-seals*) (revocations *backend-lifetime-revocations*)
                (wal *wal-lock*) (registry-lock *backend-registry-lock*)
                (held (sb-thread:make-semaphore))
                (release (sb-thread:make-semaphore))
                (finished (sb-thread:make-semaphore))
                (holder nil) (checker nil) (result nil) (completed nil))
           (unwind-protect
                (progn
                  (setf holder
                        (sb-thread:make-thread
                         (lambda ()
                           (sb-thread:with-mutex (registry-lock)
                             (sb-thread:signal-semaphore held)
                             (sb-thread:wait-on-semaphore release)))))
                  (assert (sb-thread:wait-on-semaphore held :timeout 5))
                  (setf checker
                        (sb-thread:make-thread
                         (lambda ()
                           (let ((*backend-registry* registry) (*backend-lifetimes* lifetimes)
                                                               (*backend-lifetime-seals* seals) (*backend-lifetime-revocations* revocations)
                                                               (*wal-lock* wal))
                             (unwind-protect
                                  (handler-case
                                      (setf result
                                            (and (eq token (backend-lifetime-test-check token))
                                                 (handler-case
                                                     (progn (backend-lifetime-test-capture backend) nil)
                                                   (error () t))))
                                    (error (condition) (setf result (princ-to-string condition))))
                               (sb-thread:signal-semaphore finished))))))
                  (setf completed (sb-thread:wait-on-semaphore finished :timeout 2)))
             (sb-thread:signal-semaphore release)
             (when holder (sb-thread:join-thread holder))
             (when checker (sb-thread:join-thread checker)))
           (rove:ok completed)
           (rove:ok (eq t result))
           (rove:ok (eq token (backend-lifetime-test-check token))))))))
