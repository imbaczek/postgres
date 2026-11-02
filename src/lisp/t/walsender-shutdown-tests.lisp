(in-package #:pg-physical-tests)

(defun call-with-walsender-shutdown-fixture (function)
  (let* ((backend (pg::make-backend :pid 777 :thread sb-thread:*current-thread*
                                    :replication-mode :physical))
         (registration (pg::make-wal-sender-status :backend backend :index 0))
         (pg::*backend* backend)
         (pg::*wal-sender* registration)
         (pg::*wal-sender-registry* (make-hash-table :test #'eq))
         (pg::*wal-sender-lock* (sb-thread:make-mutex :name "shutdown sender fixture"))
         (pg::*wal-sender-streams* (make-hash-table :test #'eq))
         (pg::*wal-sender-shutdown-plan* nil)
         (pg::*live-operation-lock* (sb-thread:make-mutex :name "shutdown admission fixture"))
         (pg::*live-operation-cv* (sb-thread:make-waitqueue))
         (pg::*live-operation-admission-open* t)
         (pg::*live-operation-drain-open* nil)
         (pg::*live-operation-terminal-shutdown* nil)
         (pg::*live-operation-shutdown-drain-open* nil)
         (pg::*live-operation-shutdown-drain-check* nil)
         (pg::*live-operation-depth* 0)
         (pg::*live-operation-parked-p* nil))
    (setf (gethash backend pg::*wal-sender-registry*) registration)
    (funcall function backend registration)))

(deftest shutdown-sender-lane-is-exact-and-terminal-fence-wins
    (call-with-walsender-shutdown-fixture
     (lambda (backend registration)
       (declare (ignore registration))
       (let ((owner (pg::make-physical-sender :backend backend :system-id 123 :timeline 1)))
         (pg::call-with-wal-sender-stream-scope
          (lambda ()
            (ok (intake-fails-p #'pg::wal-sender-note-shutdown-acknowledged))
            (pg::wal-sender-stream-physical-ready owner nil)
            (ok (intake-fails-p (lambda () (pg::wal-sender-stream-physical-ready owner 256))))
            (ok (not (pg::wal-sender-shutdown-streams-p)))
            (pg::wal-sender-request-stopping)
            (ok (pg::wal-sender-shutdown-streams-p))
            (pg::wal-sender-shutdown-observe)
            (let ((pg::*live-operation-admission-open* nil)
                  (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*))
              (pg::wal-sender-publish-shutdown-target 123 1 nil 128)
              (flet ((lane ()
                       (pg::call-with-live-operation-lock
                        (lambda () (pg::live-operation-admitted-p t)))))
                (ok (eq :shutdown-drain (lane)))
                (ok (not pg::*live-operation-drain-open*))
                (let ((pg::*live-operation-shutdown-drain-check* nil))
                  (ok (null (lane))))
                (let ((pg::*live-operation-parked-p* t)) (ok (null (lane))))
                (let ((pg::*live-operation-terminal-shutdown* t)) (ok (null (lane))))
                (let ((pg::*wal-sender-stream-token* (pg::make-wal-sender-stream-token)))
                  (ok (null (lane))))
                (let ((pg::*backend* (pg::make-backend :pid 777))) (ok (null (lane))))
                (ok (= 128 (pg::wal-sender-shutdown-target))))))
          :physical))
       (ok (zerop (hash-table-count pg::*wal-sender-streams*))))))

(deftest shutdown-sender-epochs-do-not-reuse-the-backend-registration
    (call-with-walsender-shutdown-fixture
     (lambda (backend registration)
       (declare (ignore registration))
       (let ((first nil) (second nil)
                         (owner (pg::make-physical-sender :backend backend :system-id 123 :timeline 1)))
         (pg::call-with-wal-sender-stream-scope
          (lambda () (setf first pg::*wal-sender-stream-token*)) :physical)
         (pg::call-with-wal-sender-stream-scope
          (lambda ()
            (setf second pg::*wal-sender-stream-token*)
            (pg::wal-sender-stream-physical-ready owner 96)
            (pg::wal-sender-request-stopping)
            (ok (signals (pg::wal-sender-stream-physical-ready owner nil) 'pg::pg-error))
            (pg::wal-sender-shutdown-observe)
            (let ((pg::*live-operation-admission-open* nil)
                  (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*))
              (pg::wal-sender-publish-shutdown-target 123 1 nil 128)
              (ok (= 96 (pg::wal-sender-shutdown-target)))
              (let ((pg::*wal-sender-stream-token* first))
                (ok (null (pg::wal-sender-shutdown-target))))))
          :physical)
         (ok (not (eq first second)))
         (ok (eq (pg::wal-sender-stream-token-registration first)
                 (pg::wal-sender-stream-token-registration second)))
         (ok (signals (pg::call-with-wal-sender-stream-scope #'identity :physical)
                      'pg::pg-error))))))

(deftest shutdown-sender-scope-rejects-outer-admission-and-cleans-abnormal-exit
    (call-with-walsender-shutdown-fixture
     (lambda (backend registration)
       (declare (ignore backend registration))
       (let ((pg::*live-operation-depth* 1))
         (ok (intake-fails-p (lambda () (pg::call-with-wal-sender-stream-scope #'identity :physical)))))
       (ok (eq :unwound
               (catch 'shutdown-stream-test
                 (pg::call-with-wal-sender-stream-scope
                  (lambda () (throw 'shutdown-stream-test :unwound)) :physical))))
       (ok (zerop (hash-table-count pg::*wal-sender-streams*))))))

(deftest shutdown-target-requires-stopping-and-matching-source-identity
    (call-with-walsender-shutdown-fixture
     (lambda (backend registration)
       (declare (ignore registration))
       (let ((owner (pg::make-physical-sender :backend backend :system-id 123 :timeline 1)))
         (pg::call-with-wal-sender-stream-scope
          (lambda ()
            (pg::wal-sender-stream-physical-ready owner nil)
            (pg::wal-sender-request-stopping)
            (let ((pg::*live-operation-admission-open* nil)
                  (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*))
              (ok (intake-fails-p (lambda () (pg::wal-sender-publish-shutdown-target 123 1 nil 128))))
              (pg::wal-sender-shutdown-observe)
              (ok (intake-fails-p (lambda () (pg::wal-sender-publish-shutdown-target 456 1 nil 128))))
              (ok (not pg::*live-operation-shutdown-drain-open*))
              (pg::wal-sender-publish-shutdown-target 123 1 nil 128)))
          :physical)))))

(deftest shutdown-finalization-waits-for-the-stream-safe-point
    (call-with-walsender-shutdown-fixture
     (lambda (backend registration)
       (declare (ignore registration))
       (let* ((pg::*backend-registry* (make-hash-table))
              (pg::*backend-registry-lock* (sb-thread:make-mutex :name "shutdown backend fixture"))
              (pg::*n-connections* 1) (pg::*n-backends* 1)
              (pg::*shutdown* pg::+fast-shutdown+)
              (pg::*shutdown-finished* nil) (pg::*shutdown-finalizing-p* nil)
              (pg::*accept-stop-requested* nil)
              (pg::*accept-lock* (sb-thread:make-mutex :name "shutdown accept fixture"))
              (owner (pg::make-physical-sender :backend backend :system-id 123 :timeline 1)))
         (setf (gethash (pg::backend-pid backend) pg::*backend-registry*) backend)
         (pg::call-with-wal-sender-stream-scope
          (lambda ()
            (pg::wal-sender-stream-physical-ready owner nil)
            (pg::wal-sender-request-stopping)
            (pg::finish-shutdown)
            (ok (not pg::*shutdown-finalizing-p*))
            (ok (not pg::*accept-stop-requested*))
            (pg::wal-sender-shutdown-observe)
            (let ((pg::*n-connections* 2))
              (ok (not (pg::wal-sender-shutdown-clients-drained-p))))
            (setf (pg::backend-xid backend) 9)
            (ok (not (pg::wal-sender-shutdown-clients-drained-p)))
            (setf (pg::backend-xid backend) nil)
            (pg::finish-shutdown)
            (ok pg::*shutdown-finalizing-p*)
            (ok pg::*accept-stop-requested*))
          :physical)))))

(deftest shutdown-cleanup-capability-survives-scope-exit-without-admitting-normal-work
    (call-with-walsender-shutdown-fixture
     (lambda (backend registration)
       (declare (ignore registration))
       (let ((plan nil)
             (owner (pg::make-physical-sender :backend backend :system-id 123 :timeline 1)))
         (pg::call-with-wal-sender-stream-scope
          (lambda ()
            (pg::wal-sender-stream-physical-ready owner nil)
            (setf plan (pg::wal-sender-request-stopping))
            (pg::wal-sender-shutdown-observe)
            (let ((pg::*live-operation-admission-open* nil)
                  (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*))
              (pg::wal-sender-publish-shutdown-target 123 1 nil 128)))
          :physical)
         (let* ((pg::*live-operation-admission-open* nil)
                (check (pg::wal-sender-shutdown-cleanup-check backend)))
           (ok check)
           (ok (null (pg::call-with-live-operation-lock
                      (lambda () (pg::live-operation-admitted-p t)))))
           (let ((pg::*live-operation-shutdown-drain-check* check))
             (ok (eq :shutdown-drain (pg::call-with-live-operation-lock
                                      (lambda () (pg::live-operation-admitted-p nil)))))
             (let ((pg::*backend* (pg::make-backend :pid 777)))
               (ok (not (funcall check))))
             (let ((pg::*wal-sender-shutdown-plan* (pg::make-wal-sender-shutdown-plan)))
               (ok (not (funcall check))))
             (setf (pg::wal-sender-shutdown-plan-stage plan) :finished)
             (ok (not (funcall check)))))))))

(deftest shutdown-state-publication-defers-interruption-through-gate-release
    (call-with-walsender-shutdown-fixture
     (lambda (backend registration)
       (declare (ignore backend registration))
       (let ((first nil) (second nil) (observed nil))
         (pg::call-with-wal-sender-state-lock
          (lambda ()
            (setf first t)
            (sb-thread:interrupt-thread
             sb-thread:*current-thread*
             (lambda ()
               (setf observed
                     (list first second (sb-thread:holding-mutex-p pg::*live-operation-lock*)))))
            (setf second t)))
         (loop repeat 100 until observed do (sleep 0.001))
         (ok (equal '(t t nil) observed))
         (ok (pg::call-with-live-operation-lock (lambda () t)))))))

(deftest shutdown-requires-physical-claims-released-even-without-recycling-pressure
    (let ((pg::*wal-sender-streams* (make-hash-table :test #'eq))
          (pg::*wal-sender-registry* (make-hash-table :test #'eq))
          (pg::*physical-source-senders* (make-hash-table :test #'equal))
          (pg::*wal-replication-slots* (make-hash-table :test #'equal)))
      (setf (gethash "retained" pg::*physical-source-senders*) :stale-owner)
      (ok (intake-fails-p #'pg::wal-sender-check-shutdown-released))
      (remhash "retained" pg::*physical-source-senders*)
      (ok (not (multiple-value-list (pg::wal-sender-check-shutdown-released))))))
