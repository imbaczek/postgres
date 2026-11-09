(in-package #:pg-physical-tests)

(cl-spec:defgenerator logical-shutdown-frontier-arguments ()
  (let* ((maximum (1- (ash 1 64)))
         (target (case (random 4) (0 0) (1 maximum) (t (random (ash 1 64)))))
         (scanned target) (sent target) (confirmed target))
    (case (random 7)
      (0 (setf target nil))
      (1 (setf scanned (max 0 (1- scanned))))
      (2 (setf sent (max 0 (1- sent))))
      (3 (setf confirmed (max 0 (1- confirmed))))
      (4 (setf scanned (min maximum (+ target (random 16)))
               sent (min maximum (+ target (random 16)))
               confirmed (min maximum (+ target (random 16)))))
      (5 (setf scanned (random (ash 1 64))
               sent (random (ash 1 64))
               confirmed (random (ash 1 64)))))
    (list target scanned sent confirmed)))

(cl-spec:defspec-function pg::wal-sender-logical-shutdown-complete-p
    (:args (target (type (or null (unsigned-byte 64))))
           (scanned (type (unsigned-byte 64)))
           (sent (type (unsigned-byte 64)))
           (confirmed (type (unsigned-byte 64))))
  (:args-generator logical-shutdown-frontier-arguments)
  (:cases
   (:complete
    (:when (and target (>= scanned target) (>= sent target) (>= confirmed target)))
    (:returns (type boolean))
    (:post (eq result t)))
   (:incomplete
    (:when (not (and target (>= scanned target) (>= sent target) (>= confirmed target))))
    (:returns (type boolean))
    (:post (null result)))))

(cl-spec:defproperty logical-shutdown-requires-every-frontier
    ((target (range integer 1 18446744073709551615)))
  (:about pg::wal-sender-logical-shutdown-complete-p)
  (:trials (:normal 100))
  (and (pg::wal-sender-logical-shutdown-complete-p target target target target)
       (not (pg::wal-sender-logical-shutdown-complete-p nil target target target))
       (loop for index from 1 to 3
             always (let ((arguments (list target target target target)))
                      (setf (nth index arguments) (1- target))
                      (not (apply #'pg::wal-sender-logical-shutdown-complete-p arguments))))))

(deftest generated-logical-shutdown-frontier-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:check-function 'pg::wal-sender-logical-shutdown-complete-p
                                         :trials 100 :seed 235))))
      (ok (eq :passed (getf data :status))))
  (let ((data (cl-spec:result-data
               (cl-spec:run-property 'logical-shutdown-requires-every-frontier
                                     :seed 236 :profile :normal))))
    (ok (eq :passed (getf data :status)))))

(deftest logical-shutdown-frontier-checks-zero-and-uint64-boundaries
    (ok (pg::wal-sender-logical-shutdown-complete-p 0 0 0 0))
  (ok (not (pg::wal-sender-logical-shutdown-complete-p nil 0 0 0)))
  (let ((maximum (1- (ash 1 64))))
    (ok (pg::wal-sender-logical-shutdown-complete-p maximum maximum maximum maximum))
    (dolist (arguments (list (list maximum (1- maximum) maximum maximum)
                             (list maximum maximum (1- maximum) maximum)
                             (list maximum maximum maximum (1- maximum))))
      (ok (not (apply #'pg::wal-sender-logical-shutdown-complete-p arguments))))))

(defun call-with-logical-walsender-shutdown-fixture (function)
  (call-with-walsender-shutdown-fixture
   (lambda (backend registration)
     (setf (pg::backend-replication-mode backend) :database)
     (let* ((pg::*catalog* (pg::make-catalog :cluster-identity "logical-shutdown"))
            (pg::*wal-ready* t) (pg::*wal-poisoned* nil)
            (pg::*physical-recovery-state* nil) (pg::*physical-recovery-failed-p* nil)
            (pg::*wal-insert-timeline* 1) (pg::*wal-segment-namespace* :legacy)
            (pg::*wal-lock* (sb-thread:make-mutex :name "logical shutdown WAL fixture"))
            (pg::*live-operation-count* 0)
            (pg::*live-operation-backend-counts* (make-hash-table :test #'eq))
            (pg::*wal-storage-incarnation* (gensym "LOGICAL-STORE-"))
            (pg::*logical-slot-owner-termination-lock* (sb-thread:make-mutex))
            (pg::*logical-slot-owner-termination-requests* (make-hash-table :test #'eq))
            (pg::*wal-replication-slots* (make-hash-table :test #'equal))
            (pg::*wal-logical-slot-owner* (gensym "LOGICAL-SHUTDOWN-"))
            (slot (pg::make-wal-replication-slot
                   :name "shutdown_logical" :plugin "pgoutput" :database-oid 16384
                   :restart-lsn 0 :confirmed-flush-lsn 0
                   :active-owner pg::*wal-logical-slot-owner* :active-owner-backend backend)))
       (setf (gethash (pg::wal-replication-slot-name slot) pg::*wal-replication-slots*) slot)
       (pg::call-with-wal-sender-stream-scope
        (lambda () (funcall function backend registration slot))
        :logical)))))

(deftest logical-shutdown-readiness-freezes-the-exact-slot-claim
    (call-with-logical-walsender-shutdown-fixture
     (lambda (backend registration slot)
       (declare (ignore backend registration))
       (pg::wal-sender-stream-logical-ready slot)
       (let* ((token pg::*wal-sender-stream-token*)
              (plan (pg::wal-sender-request-stopping)))
         (ok (member token (pg::wal-sender-shutdown-plan-tokens plan) :test #'eq))
         (ok (eq :stopping (pg::wal-sender-stream-token-state token)))
         (ok (not (pg::wal-sender-stream-token-stopped-p token)))
         (pg::wal-sender-shutdown-observe)
         (ok (pg::wal-sender-stream-token-stopped-p token))
         (ok (eq plan (pg::wal-sender-request-stopping)))
         (ok (= 1 (length (pg::wal-sender-shutdown-plan-tokens plan))))
         (ok (signals (pg::wal-sender-stream-logical-ready slot) 'pg::pg-error))))))

(deftest logical-shutdown-readiness-refuses-stale-or-lost-slot-claims
    (call-with-logical-walsender-shutdown-fixture
     (lambda (backend registration slot)
       (declare (ignore backend registration))
       (let ((name (pg::wal-replication-slot-name slot)))
         (setf (gethash name pg::*wal-replication-slots*) (copy-structure slot))
         (ok (signals (pg::wal-sender-stream-logical-ready slot) 'error))
         (setf (gethash name pg::*wal-replication-slots*) slot)
         (let ((pg::*wal-logical-slot-owner* (gensym "OTHER-OWNER-")))
           (ok (signals (pg::wal-sender-stream-logical-ready slot) 'error)))
         (setf (pg::wal-replication-slot-wal-status slot) :lost)
         (ok (signals (pg::wal-sender-stream-logical-ready slot) 'error))
         (setf (pg::wal-replication-slot-wal-status slot) :reserved)
         (ok (eq :starting (pg::wal-sender-stream-token-state pg::*wal-sender-stream-token*)))
         (ok (null (pg::wal-sender-stream-token-owner pg::*wal-sender-stream-token*)))
         (pg::wal-sender-stream-logical-ready slot)
         (ok (eq :ready (pg::wal-sender-stream-token-state pg::*wal-sender-stream-token*)))))))

(deftest logical-shutdown-owner-rejects-source-epoch-and-slot-changes
    (call-with-logical-walsender-shutdown-fixture
     (lambda (backend registration slot)
       (declare (ignore backend registration))
       (pg::wal-sender-stream-logical-ready slot)
       (let ((owner (pg::wal-sender-stream-token-owner pg::*wal-sender-stream-token*)))
         (flet ((refused ()
                  (sb-thread:with-mutex (pg::*wal-lock*)
                    (ok (signals (pg::logical-shutdown-validate-owner-under-lock owner) 'error)))))
           (let ((pg::*catalog* (pg::make-catalog :cluster-identity "another-logical-source")))
             (refused))
           (let ((pg::*wal-insert-timeline* 2)) (refused))
           (let ((pg::*wal-segment-namespace* :timeline)) (refused))
           (let ((pg::*wal-storage-incarnation* (gensym "REPLACED-STORE-"))) (refused))
           (let ((name (pg::wal-replication-slot-name slot)))
             (unwind-protect
                  (progn
                    (setf (gethash name pg::*wal-replication-slots*) (copy-structure slot))
                    (refused))
               (setf (gethash name pg::*wal-replication-slots*) slot)))
           (let ((previous (pg::wal-replication-slot-active-owner slot)))
             (unwind-protect
                  (progn
                    (setf (pg::wal-replication-slot-active-owner slot) (gensym "FOREIGN-CLAIM-"))
                    (refused))
               (setf (pg::wal-replication-slot-active-owner slot) previous)))
           (let ((previous (pg::wal-replication-slot-active-owner-backend slot)))
             (unwind-protect
                  (progn
                    (setf (pg::wal-replication-slot-active-owner-backend slot) (pg::make-backend))
                    (refused))
               (setf (pg::wal-replication-slot-active-owner-backend slot) previous)))
           (let ((previous (pg::wal-replication-slot-database-oid slot)))
             (unwind-protect
                  (progn (incf (pg::wal-replication-slot-database-oid slot)) (refused))
               (setf (pg::wal-replication-slot-database-oid slot) previous)))
           (let ((previous (pg::wal-replication-slot-plugin slot)))
             (unwind-protect
                  (progn (setf (pg::wal-replication-slot-plugin slot) "pg_lisp") (refused))
               (setf (pg::wal-replication-slot-plugin slot) previous)))
           (sb-thread:with-mutex (pg::*wal-lock*)
             (ok (eq slot (pg::logical-shutdown-validate-owner-under-lock owner)))))))))

(deftest logical-shutdown-cleanup-marks-departure-before-clearing-the-slot
    (call-with-logical-walsender-shutdown-fixture
     (lambda (backend registration slot)
       (declare (ignore registration))
       (setf (gethash (pg::backend-database backend) (pg::catalog-databases pg::*catalog*))
             (pg::make-database :name (pg::backend-database backend) :oid 16384)
             (pg::wal-replication-slot-active-owner slot) nil
             (pg::wal-replication-slot-active-owner-backend slot) nil)
       (let ((original (symbol-function 'pg::wal-release-replication-slot-owner))
             (releases 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::wal-release-replication-slot-owner)
                      (lambda (&rest arguments)
                        (incf releases)
                        (ok (eq :cleanup (pg::wal-sender-stream-token-state pg::*wal-sender-stream-token*)))
                        (ok (pg::wal-sender-stream-token-stopped-p pg::*wal-sender-stream-token*))
                        (ok (eq :abandoned (pg::wal-sender-stream-token-outcome pg::*wal-sender-stream-token*)))
                        (apply original arguments)))
                (ok (eq :departed
                        (catch 'logical-shutdown-departure
                          (pg::call-with-logical-replication-slot
                           (pg::wal-replication-slot-name slot)
                           (lambda (actual)
                             (pg::wal-sender-stream-logical-ready actual)
                             (pg::wal-sender-request-stopping)
                             (throw 'logical-shutdown-departure :departed))))))
                (ok (= 1 releases))
                (ok (null (pg::wal-replication-slot-active-owner slot)))
                (ok (null (pg::wal-replication-slot-active-owner-backend slot))))
           (setf (symbol-function 'pg::wal-release-replication-slot-owner) original))))))

(deftest logical-shutdown-departure-requires-the-frozen-claim
    (call-with-logical-walsender-shutdown-fixture
     (lambda (backend registration slot)
       (declare (ignore backend registration))
       (pg::wal-sender-stream-logical-ready slot)
       (pg::wal-sender-request-stopping)
       (let ((token pg::*wal-sender-stream-token*)
             (claim pg::*wal-logical-slot-owner*))
         (pg::wal-sender-stream-logical-leaving (copy-structure slot) claim)
         (ok (eq :stopping (pg::wal-sender-stream-token-state token)))
         (pg::wal-sender-stream-logical-leaving slot (gensym "FOREIGN-CLAIM-"))
         (ok (eq :stopping (pg::wal-sender-stream-token-state token)))
         (ok (not (pg::wal-sender-stream-token-stopped-p token)))
         (setf (pg::wal-sender-stream-token-outcome token) :acknowledged)
         (pg::wal-sender-stream-logical-leaving slot claim)
         (ok (eq :cleanup (pg::wal-sender-stream-token-state token)))
         (ok (eq :acknowledged (pg::wal-sender-stream-token-outcome token)))
         (ok (pg::wal-sender-stream-token-stopped-p token))))))

(deftest logical-shutdown-departed-slot-keeps-its-identity-checks
    (call-with-logical-walsender-shutdown-fixture
     (lambda (backend registration slot)
       (declare (ignore backend registration))
       (pg::wal-sender-stream-logical-ready slot)
       (let ((owner (pg::wal-sender-stream-token-owner pg::*wal-sender-stream-token*)))
         (setf (pg::wal-replication-slot-active-owner slot) nil
               (pg::wal-replication-slot-active-owner-backend slot) nil)
         (sb-thread:with-mutex (pg::*wal-lock*)
           (ok (signals (pg::logical-shutdown-validate-owner-under-lock owner) 'error))
           (multiple-value-bind (actual active)
               (pg::logical-shutdown-validate-owner-under-lock owner :allow-departed t)
             (ok (eq slot actual))
             (ok (null active)))
           (setf (pg::wal-replication-slot-active-owner slot) (gensym "NEW-CLAIM-"))
           (ok (signals (pg::logical-shutdown-validate-owner-under-lock owner :allow-departed t) 'error))
           (setf (pg::wal-replication-slot-active-owner slot) nil)
           (let ((pg::*wal-storage-incarnation* (gensym "NEW-STORE-")))
             (ok (signals (pg::logical-shutdown-validate-owner-under-lock owner :allow-departed t) 'error)))
           (setf (gethash (pg::wal-replication-slot-name slot) pg::*wal-replication-slots*)
                 (copy-structure slot))
           (ok (signals (pg::logical-shutdown-validate-owner-under-lock owner :allow-departed t) 'error)))))))

(deftest logical-shutdown-cleanup-releases-the-slot-if-marking-fails
    (call-with-logical-walsender-shutdown-fixture
     (lambda (backend registration slot)
       (declare (ignore registration))
       (setf (gethash (pg::backend-database backend) (pg::catalog-databases pg::*catalog*))
             (pg::make-database :name (pg::backend-database backend) :oid 16384)
             (pg::wal-replication-slot-active-owner slot) nil
             (pg::wal-replication-slot-active-owner-backend slot) nil)
       (let ((original (symbol-function 'pg::wal-sender-stream-logical-leaving))
             (marks 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::wal-sender-stream-logical-leaving)
                      (lambda (actual claim)
                        (incf marks)
                        (ok (eq slot actual))
                        (ok (eq claim (pg::wal-replication-slot-active-owner slot)))
                        (error "injected logical cleanup marker failure")))
                (ok (signals
                     (pg::call-with-logical-replication-slot
                      (pg::wal-replication-slot-name slot)
                      (lambda (actual) (declare (ignore actual)) (values)))
                     'error))
                (ok (= 1 marks))
                (ok (null (pg::wal-replication-slot-active-owner slot)))
                (ok (null (pg::wal-replication-slot-active-owner-backend slot))))
           (setf (symbol-function 'pg::wal-sender-stream-logical-leaving) original))))))

(deftest logical-shutdown-target-captures-the-final-precheckpoint-flush
    (call-with-logical-walsender-shutdown-fixture
     (lambda (backend registration slot)
       (declare (ignore backend registration))
       (pg::wal-sender-stream-logical-ready slot)
       (pg::wal-sender-request-stopping)
       (pg::wal-sender-shutdown-observe)
       (let ((original (symbol-function 'pg::wal-flush))
             (flushes 0)
             (pg::*wal-end-lsn* 256)
             (pg::*wal-durable-lsn* 128))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::wal-flush)
                      (lambda ()
                        (incf flushes)
                        (ok (sb-thread:holding-mutex-p pg::*wal-lock*))
                        (setf pg::*wal-durable-lsn* pg::*wal-end-lsn*)
                        (values)))
                (ok (signals (pg::wal-sender-capture-logical-shutdown-target) 'error))
                (ok (zerop flushes))
                (let ((pg::*live-operation-admission-open* nil)
                      (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*))
                  (pg::wal-sender-capture-logical-shutdown-target)
                  (ok (= 1 flushes))
                  (ok (= 256 (pg::wal-sender-stream-token-limit pg::*wal-sender-stream-token*)))
                  (ok (null (pg::wal-sender-stream-token-target pg::*wal-sender-stream-token*)))
                  (ok (eq :stopping (pg::wal-sender-shutdown-plan-stage pg::*wal-sender-shutdown-plan*)))
                  (ok (not pg::*live-operation-shutdown-drain-open*))
                  (ok (signals (pg::wal-sender-capture-logical-shutdown-target) 'error))
                  (ok (= 1 flushes))))
           (setf (symbol-function 'pg::wal-flush) original))))))

(deftest logical-shutdown-target-requires-proof-of-slot-departure
    (dolist (marked '(nil t))
      (call-with-logical-walsender-shutdown-fixture
       (lambda (backend registration slot)
         (declare (ignore backend registration))
         (pg::wal-sender-stream-logical-ready slot)
         (pg::wal-sender-request-stopping)
         (pg::wal-sender-shutdown-observe)
         (when marked
           (pg::wal-sender-stream-logical-leaving slot pg::*wal-logical-slot-owner*))
         (setf (pg::wal-replication-slot-active-owner slot) nil
               (pg::wal-replication-slot-active-owner-backend slot) nil)
         (let ((original (symbol-function 'pg::wal-flush))
               (pg::*wal-durable-lsn* 256)
               (pg::*live-operation-admission-open* nil)
               (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::wal-flush) (lambda () (values)))
                  (if marked
                      (progn
                        (pg::wal-sender-capture-logical-shutdown-target)
                        (ok (= 256 (pg::wal-sender-stream-token-limit pg::*wal-sender-stream-token*))))
                      (progn
                        (ok (signals (pg::wal-sender-capture-logical-shutdown-target) 'error))
                        (ok (null (pg::wal-sender-stream-token-limit pg::*wal-sender-stream-token*))))))
             (setf (symbol-function 'pg::wal-flush) original)))))))

(deftest logical-shutdown-capture-follows-abandoned-transaction-retirement
    (call-with-logical-walsender-shutdown-fixture
     (lambda (backend registration slot)
       (declare (ignore backend registration))
       (pg::wal-sender-stream-logical-ready slot)
       (pg::wal-sender-request-stopping)
       (pg::wal-sender-shutdown-observe)
       (let* ((symbols '(pg::wal-set-primary-state pg::postmaster-retire-abandoned-backends
                         pg::wal-flush pg::wal-checkpoint-core))
              (originals (mapcar #'symbol-function symbols))
              (events nil)
              (pg::*wal-end-lsn* 128)
              (pg::*wal-durable-lsn* 64)
              (pg::*live-operation-admission-open* nil)
              (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
              (pg::*native-control-test-hook* (lambda (point) (push point events))))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::wal-set-primary-state)
                      (lambda (state)
                        (ok (= pg::+db-shutdowning+ state))
                        (push :dirty events))
                      (symbol-function 'pg::postmaster-retire-abandoned-backends)
                      (lambda ()
                        (push :retire events)
                        (setf pg::*wal-end-lsn* 256))
                      (symbol-function 'pg::wal-flush)
                      (lambda ()
                        (push :flush events)
                        (setf pg::*wal-durable-lsn* pg::*wal-end-lsn*)
                        (values))
                      (symbol-function 'pg::wal-checkpoint-core)
                      (lambda ()
                        (push :checkpoint events)
                        (ok (= 256 (pg::wal-sender-stream-token-limit pg::*wal-sender-stream-token*)))
                        (throw 'logical-shutdown-captured :captured)))
                (ok (eq :captured
                        (catch 'logical-shutdown-captured
                          (pg::prepare-primary-shutdown-with-senders))))
                (ok (equal '(:dirty :after-primary-shutdown-dirty :retire :flush :checkpoint)
                           (reverse events))))
           (loop for symbol in symbols for original in originals
                 do (setf (symbol-function symbol) original)))))))

(deftest logical-shutdown-publisher-keeps-the-precheckpoint-frontier
    (call-with-logical-walsender-shutdown-fixture
     (lambda (backend registration slot)
       (declare (ignore backend registration))
       (pg::wal-sender-stream-logical-ready slot)
       (pg::wal-sender-request-stopping)
       (pg::wal-sender-shutdown-observe)
       (let ((pg::*live-operation-admission-open* nil)
             (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
             (system-id (pg::catalog-system-identifier))
             (token pg::*wal-sender-stream-token*))
         (ok (signals (pg::wal-sender-publish-shutdown-target system-id 1 :legacy 256) 'error))
         (setf (pg::wal-sender-stream-token-limit token) 512)
         (ok (signals (pg::wal-sender-publish-shutdown-target system-id 1 :legacy 256) 'error))
         (setf (pg::wal-sender-stream-token-limit token) 128)
         (let ((pg::*wal-storage-incarnation* (gensym "REPLACED-STORE-")))
           (ok (signals (pg::wal-sender-publish-shutdown-target system-id 1 :legacy 256) 'error)))
         (ok (null (pg::wal-sender-stream-token-target token)))
         (ok (not pg::*live-operation-shutdown-drain-open*))
         (pg::wal-sender-publish-shutdown-target system-id 1 :legacy 256)
         (ok (= 128 (pg::wal-sender-shutdown-target)))
         (ok (= 256 (pg::wal-sender-shutdown-plan-endpoint pg::*wal-sender-shutdown-plan*)))
         (ok (eq :draining (pg::wal-sender-shutdown-plan-stage pg::*wal-sender-shutdown-plan*)))
         (ok pg::*live-operation-shutdown-drain-open*)))))

(deftest logical-shutdown-publisher-rechecks-departure-after-checkpoint
    (dolist (marked '(nil t))
      (call-with-logical-walsender-shutdown-fixture
       (lambda (backend registration slot)
         (declare (ignore backend registration))
         (pg::wal-sender-stream-logical-ready slot)
         (pg::wal-sender-request-stopping)
         (pg::wal-sender-shutdown-observe)
         (when marked
           (pg::wal-sender-stream-logical-leaving slot pg::*wal-logical-slot-owner*))
         (setf (pg::wal-sender-stream-token-limit pg::*wal-sender-stream-token*) 128
               (pg::wal-replication-slot-active-owner slot) nil
               (pg::wal-replication-slot-active-owner-backend slot) nil)
         (let ((pg::*live-operation-admission-open* nil)
               (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
               (system-id (pg::catalog-system-identifier)))
           (if marked
               (progn
                 (pg::wal-sender-publish-shutdown-target system-id 1 :legacy 256)
                 (ok (= 128 (pg::wal-sender-shutdown-target)))
                 (ok (eq :abandoned (pg::wal-sender-stream-token-outcome pg::*wal-sender-stream-token*))))
               (progn
                 (ok (signals (pg::wal-sender-publish-shutdown-target system-id 1 :legacy 256) 'error))
                 (ok (not pg::*live-operation-shutdown-drain-open*)))))))))

(deftest logical-target-capture-leaves-physical-only-plans-without-an-extra-flush
    (call-with-walsender-shutdown-fixture
     (lambda (backend registration)
       (declare (ignore registration))
       (pg::call-with-wal-sender-stream-scope
        (lambda ()
          (pg::wal-sender-stream-physical-ready
           (pg::make-physical-sender :backend backend :system-id 123 :timeline 1) 96)
          (pg::wal-sender-request-stopping)
          (pg::wal-sender-shutdown-observe)
          (let ((original (symbol-function 'pg::wal-flush))
                (pg::*live-operation-admission-open* nil)
                (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::wal-flush)
                         (lambda () (error "physical-only capture must not flush")))
                   (pg::wal-sender-capture-logical-shutdown-target)
                   (ok (= 96 (pg::wal-sender-stream-token-limit pg::*wal-sender-stream-token*)))
                   (ok (eq :stopping (pg::wal-sender-shutdown-plan-stage pg::*wal-sender-shutdown-plan*))))
              (setf (symbol-function 'pg::wal-flush) original))))
        :physical))))

(deftest logical-shutdown-rechecks-the-owner-after-releasing-wal
    (dolist (phase '(:capture :publish))
      (call-with-logical-walsender-shutdown-fixture
       (lambda (backend registration slot)
         (declare (ignore backend registration))
         (pg::wal-sender-stream-logical-ready slot)
         (pg::wal-sender-request-stopping)
         (pg::wal-sender-shutdown-observe)
         (let* ((token pg::*wal-sender-stream-token*)
                (owner (pg::wal-sender-stream-token-owner token))
                (original-state-lock (symbol-function 'pg::call-with-wal-sender-state-lock))
                (original-flush (symbol-function 'pg::wal-flush))
                (checks 0)
                (pg::*wal-durable-lsn* 256)
                (pg::*live-operation-admission-open* nil)
                (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*))
           (when (eq phase :publish)
             (setf (pg::wal-sender-stream-token-limit token) 128))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::wal-flush) (lambda () (values))
                        (symbol-function 'pg::call-with-wal-sender-state-lock)
                        (lambda (function)
                          (incf checks)
                          (funcall original-state-lock
                                   (lambda ()
                                     (when (= checks 2)
                                       (ok (not (sb-thread:holding-mutex-p pg::*wal-lock*)))
                                       (setf (pg::wal-sender-stream-token-owner token)
                                             (copy-structure owner)))
                                     (funcall function)))))
                  (ok (signals
                       (ecase phase
                         (:capture (pg::wal-sender-capture-logical-shutdown-target))
                         (:publish (pg::wal-sender-publish-shutdown-target
                                    (pg::catalog-system-identifier) 1 :legacy 256)))
                       'error))
                  (ok (= 2 checks))
                  (ok (null (pg::wal-sender-stream-token-target token)))
                  (ok (not pg::*live-operation-shutdown-drain-open*))
                  (ok (eql (if (eq phase :publish) 128 nil)
                           (pg::wal-sender-stream-token-limit token))))
             (setf (symbol-function 'pg::call-with-wal-sender-state-lock) original-state-lock
                   (symbol-function 'pg::wal-flush) original-flush
                   (pg::wal-sender-stream-token-owner token) owner)))))))

(deftest logical-shutdown-acknowledgement-requires-scanned-sent-and-durable-flush
    (call-with-logical-walsender-shutdown-fixture
     (lambda (backend registration slot)
       (declare (ignore backend))
       (pg::wal-sender-stream-logical-ready slot)
       (ok (not (pg::wal-sender-acknowledge-logical-shutdown slot 128)))
       (pg::wal-sender-request-stopping)
       (pg::wal-sender-shutdown-observe)
       (setf (pg::wal-sender-stream-token-limit pg::*wal-sender-stream-token*) 128)
       (let ((pg::*live-operation-admission-open* nil)
             (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*))
         (pg::wal-sender-publish-shutdown-target (pg::catalog-system-identifier) 1 :legacy 256)
         (setf (pg::wal-sender-status-sent-lsn registration) 128
               (pg::wal-replication-slot-confirmed-flush-lsn slot) 128)
         (ok (not (pg::wal-sender-acknowledge-logical-shutdown slot 127)))
         (setf (pg::wal-sender-status-sent-lsn registration) 127)
         (ok (not (pg::wal-sender-acknowledge-logical-shutdown slot 128)))
         (setf (pg::wal-sender-status-sent-lsn registration) 128
               (pg::wal-replication-slot-confirmed-flush-lsn slot) 127)
         (ok (not (pg::wal-sender-acknowledge-logical-shutdown slot 128)))
         (setf (pg::wal-replication-slot-confirmed-flush-lsn slot) 128)
         (ok (not (pg::wal-sender-acknowledge-logical-shutdown slot 128 :partial t)))
         (ok (eq :stopping (pg::wal-sender-stream-token-state pg::*wal-sender-stream-token*)))
         (ok (pg::wal-sender-acknowledge-logical-shutdown slot 128))
         (ok (eq :complete (pg::wal-sender-stream-token-state pg::*wal-sender-stream-token*)))
         (ok (eq :acknowledged (pg::wal-sender-stream-token-outcome pg::*wal-sender-stream-token*)))))))

(deftest logical-shutdown-completion-requires-acknowledgement-and-slot-release
    (call-with-logical-walsender-shutdown-fixture
     (lambda (backend registration slot)
       (declare (ignore backend))
       (pg::wal-sender-stream-logical-ready slot)
       (pg::wal-sender-request-stopping)
       (pg::wal-sender-shutdown-observe)
       (setf (pg::wal-sender-stream-token-limit pg::*wal-sender-stream-token*) 128)
       (let ((pg::*live-operation-admission-open* nil)
             (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*))
         (pg::wal-sender-publish-shutdown-target (pg::catalog-system-identifier) 1 :legacy 256)
         (ok (signals (pg::wal-sender-check-logical-shutdown-completion slot) 'error))
         (setf (pg::wal-sender-status-sent-lsn registration) 128
               (pg::wal-replication-slot-confirmed-flush-lsn slot) 128)
         (ok (pg::wal-sender-acknowledge-logical-shutdown slot 128))
         (ok (signals (pg::wal-sender-check-logical-shutdown-completion slot) 'error))
         (pg::wal-sender-stream-logical-leaving slot pg::*wal-logical-slot-owner*)
         (ok (signals (pg::wal-sender-check-logical-shutdown-completion slot) 'error))
         (pg::wal-release-replication-slot-owner
          (pg::wal-replication-slot-name slot) pg::*wal-logical-slot-owner* slot)
         (ok (pg::wal-sender-check-logical-shutdown-completion slot))
         (ok (signals (pg::wal-sender-check-logical-shutdown-completion (copy-structure slot)) 'error))
         (let ((pg::*wal-storage-incarnation* (gensym "ANOTHER-STORE-")))
           (ok (signals (pg::wal-sender-check-logical-shutdown-completion slot) 'error)))
         (setf (pg::wal-sender-stream-token-outcome pg::*wal-sender-stream-token*) :abandoned)
         (ok (signals (pg::wal-sender-check-logical-shutdown-completion slot) 'error))))))

(deftest logical-shutdown-zero-target-still-requires-a-published-sent-position
    (call-with-logical-walsender-shutdown-fixture
     (lambda (backend registration slot)
       (declare (ignore backend))
       (pg::wal-sender-stream-logical-ready slot)
       (pg::wal-sender-request-stopping)
       (pg::wal-sender-shutdown-observe)
       (setf (pg::wal-sender-stream-token-limit pg::*wal-sender-stream-token*) 0)
       (let ((pg::*live-operation-admission-open* nil)
             (pg::*live-operation-quiescence-owner-thread* sb-thread:*current-thread*))
         (pg::wal-sender-publish-shutdown-target (pg::catalog-system-identifier) 1 :legacy 128)
         (ok (null (pg::wal-sender-status-sent-lsn registration)))
         (ok (not (pg::wal-sender-acknowledge-logical-shutdown slot 0)))
         (pg::wal-sender-note-sent registration 0)
         (ok (pg::wal-sender-acknowledge-logical-shutdown slot 0))
         (ok (eq :acknowledged (pg::wal-sender-stream-token-outcome pg::*wal-sender-stream-token*)))))))

(deftest shutdown-owned-quiescence-checks-every-gate
    (call-with-walsender-shutdown-fixture
     (lambda (backend registration)
       (declare (ignore backend registration))
       (loop for mask below 32
             for plan = (and (logbitp 0 mask)
                             (pg::make-wal-sender-shutdown-plan
                              :stage (if (logbitp 1 mask) :stopping :draining)))
             do (let ((pg::*live-operation-admission-open* (not (logbitp 2 mask)))
                      (pg::*live-operation-terminal-shutdown* (not (logbitp 3 mask)))
                      (pg::*live-operation-quiescence-owner-thread*
                        (if (logbitp 4 mask) sb-thread:*current-thread* :another-owner)))
                  (ok (eq (= mask 31)
                          (pg::call-with-wal-sender-state-lock
                           (lambda () (pg::wal-sender-shutdown-owned-quiescence-p plan))))))))))
