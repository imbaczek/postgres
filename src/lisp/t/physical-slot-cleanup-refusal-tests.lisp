(in-package #:pg-physical-tests)

(rove:deftest issued-physical-cleanup-refusal-rechecks-authority
    (call-with-physical-slot-lifetime-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((slot (pg::wal-create-physical-replication-slot "cleanup_refusal" :temporary t))
              (backend pg::*backend*)
              (group-live t)
              (checks 0)
              (condition nil)
              (path (pg::wal-replication-slot-path "cleanup_refusal"))
              (before (physical-slot-test-file-bytes path)))
         (sb-thread:with-mutex (pg::*wal-lock*)
           (setf condition
                 (handler-case
                     (pg::physical-slot-refuse-cold-cleanup-under-lock
                      slot (lambda ()
                             (incf checks)
                             (unless group-live (error "test group is no longer current"))))
                   (pg::physical-slot-cold-cleanup-refusal (failure) failure)))
           (rove:ok (typep condition 'pg::physical-slot-cold-cleanup-refusal))
           (rove:ok (= checks 1))
           (rove:ok (pg::physical-slot-cold-cleanup-refusal-check-under-lock
                     condition slot backend))
           (rove:ok (= checks 2))
           (dolist (arguments (list (list (pg::copy-wal-replication-slot slot) backend)
                                    (list slot (pg::make-backend :pid 0))))
             (rove:ok
              (handler-case
                  (progn
                    (apply #'pg::physical-slot-cold-cleanup-refusal-check-under-lock
                           condition arguments)
                    nil)
                (error (failure)
                  (search "changed its slot or creator" (princ-to-string failure))))))
           (setf group-live nil)
           (rove:ok
            (handler-case
                (progn
                  (pg::physical-slot-cold-cleanup-refusal-check-under-lock condition slot backend)
                  nil)
              (error (failure)
                (search "test group is no longer current" (princ-to-string failure)))))
           (setf group-live t)
           (rove:ok (pg::physical-slot-cold-cleanup-refusal-check-under-lock
                     condition slot backend)))
         (rove:ok (equalp before (physical-slot-test-file-bytes path)))
         (pg::unregister-backend backend)
         (sb-thread:with-mutex (pg::*wal-lock*)
           (rove:ok
            (handler-case
                (progn
                  (pg::physical-slot-cold-cleanup-refusal-check-under-lock condition slot backend)
                  nil)
              (error () t))))
         (rove:ok (eq slot (gethash "cleanup_refusal" pg::*wal-replication-slots*)))
         (rove:ok (equalp before (physical-slot-test-file-bytes path)))))))

(rove:deftest cold-cleanup-slot-keeps-its-name-after-creator-revocation
    (call-with-physical-slot-lifetime-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((name "cold_cleanup_name")
              (slot (pg::wal-create-physical-replication-slot name :temporary t))
              (backend pg::*backend*)
              (capability (pg::wal-replication-slot-temporary-lifetime slot))
              (path (pg::wal-replication-slot-path name))
              (before (physical-slot-test-file-bytes path))
              (pg::*physical-slot-cold-cleanup-owner* nil)
              (pg::*physical-slot-cold-cleanup-exit-ready* nil)
              (pg::*primary-wal-writer* nil)
              (pg::*primary-wal-writer-shutdown-p* nil))
         (sb-thread:with-mutex (pg::*wal-lock*)
           (let ((condition
                   (handler-case
                       (pg::physical-slot-refuse-cold-cleanup-under-lock slot (constantly t))
                     (pg::physical-slot-cold-cleanup-refusal (failure) failure))))
             (rove:ok
              (eq slot
                  (pg::physical-slot-lifetime-call-under-lock
                   capability :adopt-cold-cleanup slot condition)))
             (rove:ok (eq backend pg::*physical-slot-cold-cleanup-owner*))))
         (rove:ok pg::*primary-wal-writer-shutdown-p*)
         (setf (pg::backend-termination-pending backend) t)
         (rove:ok (progn (pg::deliver-backend-termination backend)
                         (pg::check-for-interrupts)
                         (pg::backend-termination-pending backend)))
         (let ((other (pg::make-backend)))
           (setf (pg::backend-termination-pending other) t)
           (rove:ok (handler-case
                        (progn (pg::deliver-backend-termination other) nil)
                      (pg::pg-error (failure)
                        (string= "57P01" (pg::pg-error-sqlstate failure))))))
         (pg::wal-cleanup-backend-physical-slots backend)
         (pg::request-cold-slot-reconciliation-exit (pg::make-backend))
         (rove:ok (not pg::*physical-slot-cold-cleanup-exit-ready*))
         (pg::unregister-backend backend)
         (pg::request-cold-slot-reconciliation-exit backend)
         (rove:ok pg::*physical-slot-cold-cleanup-exit-ready*)
         (sb-thread:with-mutex (pg::*wal-lock*)
           (rove:ok (pg::physical-slot-check-name-under-lock name))
           (dolist (operation '(:live :candidate :owner :creating :drop-check :begin-drop :finish-drop))
             (rove:ok (handler-case
                          (progn
                            (pg::physical-slot-lifetime-call-under-lock capability operation slot backend)
                            nil)
                        (error () t))))
           (rove:ok (eq slot (gethash name pg::*wal-replication-slots*)))
           (rove:ok (not (pg::wal-replication-slot-state-uncertain-p slot))))
         (rove:ok (equalp before (physical-slot-test-file-bytes path)))))))
