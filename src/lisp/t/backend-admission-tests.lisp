(in-package #:pg-physical-tests)

(declaim (special pg::*max-connections* pg::*max-wal-senders* pg::*backend-admission-classes*))

(defun call-with-backend-admission-fixture (function &key (connections 1) (senders 1))
  (let ((pg::*max-connections* connections)
        (pg::*max-wal-senders* senders)
        (pg::*backend-admission-classes* (make-hash-table :test #'eq))
        (pg::*backend-registry* (make-hash-table :test #'eql))
        (pg::*n-backends* 0)
        (pg::*backend* nil)
        (pg::*live-operation-backend-query-tokens* (make-hash-table :test #'eq))
        (pg::*live-operation-cleanup-backends* nil)
        (pg::*live-operation-admission-open* t))
    (unwind-protect (funcall function)
      (dolist (backend (pg::list-backends)) (pg::unregister-backend backend)))))

(defun admission-test-register (pid &optional mode)
  (let ((backend (pg::make-backend :pid pid :replication-mode mode)))
    (pg::register-backend backend)
    backend))

(defun admission-test-result (backend)
  (handler-case
      (progn (pg::postmaster-admit-startup-backend backend) :accepted)
    (pg::pg-error (condition)
      (list (pg::pg-error-sqlstate condition) (pg::fatal-error-p condition)))))

(deftest backend-startup-quotas-separate-sql-and-replication-sessions
    (call-with-backend-admission-fixture
     (lambda ()
       (let ((sql (admission-test-register 1))
             (physical (admission-test-register 2 :physical))
             (database (admission-test-register 3 :database))
             (other-sql (admission-test-register 4))
             (provisional (admission-test-register 5)))
         (declare (ignore provisional))
         (ok (eq :accepted (admission-test-result sql)))
         (ok (eq :accepted (admission-test-result sql)))
         (ok (eq :accepted (admission-test-result physical)))
         (ok (equal '("53300" t) (admission-test-result database)))
         (ok (equal '("53300" t) (admission-test-result other-sql)))
         (ok (= 2 (hash-table-count pg::*backend-admission-classes*)))
         (pg::unregister-backend physical)
         (pg::unregister-backend physical)
         (ok (eq :accepted (admission-test-result database)))
         (ok (equal '("53300" t) (admission-test-result other-sql)))
         (pg::unregister-backend sql)
         (ok (eq :accepted (admission-test-result other-sql)))
         (ok (= 2 (hash-table-count pg::*backend-admission-classes*)))))))

(deftest zero-senders-rejects-both-replication-modes-without-consuming-sql
    (call-with-backend-admission-fixture
     (lambda ()
       (dolist (mode '(:physical :database))
         (ok (equal '("53300" t)
                    (admission-test-result (admission-test-register (if (eq mode :physical) 1 2) mode)))))
       (ok (zerop (hash-table-count pg::*backend-admission-classes*)))
       (ok (eq :accepted (admission-test-result (admission-test-register 3)))))
     :senders 0))

(deftest backend-admission-requires-registration-and-a-stable-session-class
    (call-with-backend-admission-fixture
     (lambda ()
       (let ((backend (pg::make-backend :pid 1)))
         (ok (intake-fails-p (lambda () (pg::postmaster-admit-startup-backend backend))))
         (ok (zerop (hash-table-count pg::*backend-admission-classes*)))
         (pg::register-backend backend)
         (ok (eq :accepted (admission-test-result backend)))
         (setf (pg::backend-replication-mode backend) :physical)
         (ok (intake-fails-p (lambda () (pg::postmaster-admit-startup-backend backend))))
         (ok (eq :sql (gethash backend pg::*backend-admission-classes*)))
         (pg::unregister-backend backend)
         (ok (zerop (hash-table-count pg::*backend-admission-classes*)))))))

(deftest duplicate-backend-registration-cannot-corrupt-accounting
    (call-with-backend-admission-fixture
     (lambda ()
       (let ((backend (admission-test-register 1)))
         (ok (eq :accepted (admission-test-result backend)))
         (ok (intake-fails-p (lambda () (pg::register-backend backend))))
         (ok (intake-fails-p (lambda () (pg::register-backend (pg::make-backend :pid 1)))))
         (ok (= 1 pg::*n-backends*))
         (ok (= 1 (hash-table-count pg::*backend-admission-classes*)))
         (ok (eq backend (pg::lookup-backend 1)))
         (pg::unregister-backend backend)
         (ok (zerop pg::*n-backends*))
         (ok (zerop (hash-table-count pg::*backend-admission-classes*)))))))

(deftest duplicate-registration-during-reload-does-not-enroll-an-unregistered-backend
    (call-with-backend-admission-fixture
     (lambda ()
       (let ((old (admission-test-register 1))
             (duplicate (pg::make-backend :pid 1))
             (pg::*live-operation-admission-open* nil)
             (pg::*live-operation-cleanup-backends* nil))
         (ok (intake-fails-p (lambda () (pg::register-backend duplicate))))
         (ok (intake-fails-p (lambda () (pg::register-backend old))))
         (ok (null pg::*live-operation-cleanup-backends*))
         (ok (= 1 pg::*n-backends*))
         (ok (eq old (pg::lookup-backend 1)))
         (let ((fresh (admission-test-register 2)))
           (ok (equal (list fresh) pg::*live-operation-cleanup-backends*))
           (ok (= 2 pg::*n-backends*)))))))

(deftest stale-backend-cleanup-cannot-release-another-generation
    (call-with-backend-admission-fixture
     (lambda ()
       (let* ((old (admission-test-register 1))
              (new (pg::make-backend :pid 1)))
         (ok (eq :accepted (admission-test-result old)))
         (setf (gethash 1 pg::*backend-registry*) new)
         (ok (intake-fails-p (lambda () (pg::postmaster-admit-startup-backend old))))
         (ok (eq :accepted (admission-test-result new)))
         (pg::unregister-backend old)
         (ok (eq new (gethash 1 pg::*backend-registry*)))
         (ok (eq :sql (gethash new pg::*backend-admission-classes*)))
         (ok (null (gethash old pg::*backend-admission-classes*)))
         (ok (= 1 pg::*n-backends*))))
     :connections 2))

(deftest concurrent-startup-admission-cannot-overbook-either-class
    (call-with-backend-admission-fixture
     (lambda ()
       (let* ((registry pg::*backend-registry*)
              (classes pg::*backend-admission-classes*)
              (connections pg::*max-connections*)
              (senders pg::*max-wal-senders*)
              (gate (sb-thread:make-semaphore :count 0))
              (results (make-array 16 :initial-element nil))
              (threads
                (loop for index below 16
                      collect
                      (let ((backend (admission-test-register (1+ index) (when (oddp index) :physical)))
                            (position index))
                        (sb-thread:make-thread
                         (lambda ()
                           (let ((pg::*backend-registry* registry)
                                 (pg::*backend-admission-classes* classes)
                                 (pg::*max-connections* connections)
                                 (pg::*max-wal-senders* senders))
                             (sb-thread:wait-on-semaphore gate)
                             (setf (aref results position) (admission-test-result backend))))
                         :name "quota-contender")))))
         (unwind-protect
              (progn
                (sb-thread:signal-semaphore gate 16)
                (dolist (thread threads) (sb-thread:join-thread thread :timeout 5 :default :timeout))
                (ok (every (lambda (thread) (not (sb-thread:thread-alive-p thread))) threads))
                (ok (= 2 (count :accepted results)))
                (ok (= 14 (count '("53300" t) results :test #'equal)))
                (ok (= 2 (hash-table-count classes))))
           (sb-thread:signal-semaphore gate 16)
           (dolist (thread threads) (sb-thread:join-thread thread :timeout 5 :default :timeout)))))))

(deftest backend-teardown-releases-admission-despite-earlier-cleanup-failure
    (dolist (exit '(:error :throw :timeout))
      (call-with-backend-admission-fixture
       (lambda ()
         (let* ((backend (admission-test-register 1))
                (pg::*n-connections* 1)
                (pg::*shutdown* pg::+no-shutdown+)
                (old (symbol-function 'pg::wal-cleanup-backend-physical-slots)))
           (ok (eq :accepted (admission-test-result backend)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::wal-cleanup-backend-physical-slots)
                        (lambda (ignored)
                          (declare (ignore ignored))
                          (ecase exit
                            (:error (error "injected cleanup failure"))
                            (:throw (throw 'cleanup-cut :throw))
                            (:timeout (error 'sb-ext:timeout)))))
                  (let ((result
                          (catch 'cleanup-cut
                            (handler-case (pg::finish-backend-connection backend nil nil)
                              (sb-ext:timeout () :timeout)
                              (error () :error)))))
                    (ok (eq exit result)))
                  (ok (zerop pg::*n-connections*))
                  (ok (zerop pg::*n-backends*))
                  (ok (zerop (hash-table-count pg::*backend-admission-classes*)))
                  (ok (null (gethash 1 pg::*backend-registry*)))
                  (ok (eq :accepted (admission-test-result (admission-test-register 2)))))
             (setf (symbol-function 'pg::wal-cleanup-backend-physical-slots) old)))))))

(deftest connection-guc-validation-and-installation-are-atomic
    (call-with-backend-admission-fixture
     (lambda ()
       (let ((settings (pg::startup-default-settings "/tmp/connection-gucs/"))
             (native (make-hash-table :test #'equal))
             (overrides (make-hash-table :test #'equal))
             (pg::*startup-installed-configuration* nil)
             (pg::*startup-server-settings* :unchanged)
             (pg::*startup-native-inputs* nil)
             (pg::*startup-command-overrides* nil)
             (pg::*physical-restartpoint-timeout* pg::*physical-restartpoint-timeout*)
             (pg::*physical-restartpoint-wal-bytes* pg::*physical-restartpoint-wal-bytes*)
             (pg::*max-slot-wal-keep-bytes* pg::*max-slot-wal-keep-bytes*)
             (pg::*wal-slot-retention-policy* pg::*wal-slot-retention-policy*)
             (pg::*max-replication-slots* pg::*max-replication-slots*))
         (setf (getf (gethash "max_connections" settings) :value) 3)
         (ok (intake-fails-p (lambda () (pg::startup-install-settings settings native overrides))))
         (ok (eq :unchanged pg::*startup-server-settings*))
         (ok (= 1 pg::*max-connections*))
         (ok (= 1 pg::*max-wal-senders*))
         (setf (getf (gethash "superuser_reserved_connections" settings) :value) 0
               (getf (gethash "max_wal_senders" settings) :value) 0)
         (ok (eq settings (pg::startup-install-settings settings native overrides)))
         (ok (= 3 pg::*max-connections*))
         (ok (zerop pg::*max-wal-senders*))
         (let ((row (pg::startup-settings-row "max_connections" (gethash "max_connections" settings))))
           (ok (equal "postmaster" (aref row 3)))
           (ok (equal "integer" (aref row 4))))
         (setf (getf (gethash "max_connections" settings) :value) (- 262143 26)
               (getf (gethash "max_wal_senders" settings) :value) 0)
         (ok (progn (pg::startup-check-connection-limits settings) t))
         (setf (getf (gethash "max_wal_senders" settings) :value) 1)
         (ok (intake-fails-p (lambda () (pg::startup-check-connection-limits settings))))))))

(cl-spec:defproperty backend-admission-occupancy-follows-independent-limits
    ((connections (range integer 1 4))
     (senders (range integer 0 4))
     (mask (range integer 0 65535)))
  (:about pg::postmaster-admit-startup-backend)
  (:trials (:normal 200))
  (call-with-backend-admission-fixture
   (lambda ()
     (block checked
       (let ((admitted nil))
         (dotimes (index 16)
           (let* ((sender (logbitp index mask))
                  (backend (admission-test-register (1+ index) (when sender :database)))
                  (count (count sender admitted :key #'cdr))
                  (allowed (< count (if sender senders connections)))
                  (accepted (eq :accepted (admission-test-result backend))))
             (unless (eql allowed accepted) (return-from checked nil))
             (if accepted (push (cons backend sender) admitted)
                 (pg::unregister-backend backend))
             (when (and admitted (= 2 (mod index 3)))
               (pg::unregister-backend (caar admitted))
               (pop admitted))
             (unless (= (length admitted) (hash-table-count pg::*backend-admission-classes*))
               (return-from checked nil))))
         t)))
   :connections connections :senders senders))

(deftest generated-backend-admission-occupancy
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'backend-admission-occupancy-follows-independent-limits :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 200 (getf data :trials)))))
