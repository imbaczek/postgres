(in-package #:pg)

(declaim (special *relation-lock-object-limit* *relation-lock-owner-limit*))

(defun call-with-relation-resource-fixture (function &key (objects 8) (owners 16))
  (let ((*relation-lock-object-limit* objects)
        (*relation-lock-owner-limit* owners)
        (*relation-writers* (make-hash-table :test #'eql))
        (*relation-readers* (make-hash-table :test #'eql))
        (*relation-builders* (make-hash-table :test #'eql))
        (*relation-reader-blockers* (make-hash-table :test #'eql))
        (*relation-build-groups* (make-hash-table :test #'eq))
        (*relation-maintainers* (make-hash-table :test #'eql))
        (*recovery-reader-xids* (make-hash-table :test #'eql))
        (*active-xids* nil)
        (*wal-xid-first-lsn* (make-hash-table :test #'eql))
        (*logical-capture-xid-modes* (make-hash-table :test #'eql))
        (*logical-binary-required-xids* (make-hash-table :test #'eql))
        (*backend* nil))
    (funcall function)))

(defun relation-resource-error (function)
  (handler-case (progn (funcall function) nil)
    (pg-error (condition) condition)))

(rove:deftest relation-resource-multi-relation-admission-is-atomic
    (call-with-relation-resource-fixture
     (lambda ()
       (register-relation-readers '(1) 11)
       (let ((condition (relation-resource-error
                         (lambda () (register-relation-accesses '(2) '(3) 12)))))
         (rove:ok (typep condition 'pg-error))
         (when condition
           (rove:ok (string= "53200" (pg-error-sqlstate condition)))
           (rove:ok (string= "out of shared memory" (pg-error-message condition))))
         (rove:ok (null (gethash 2 *relation-writers*)))
         (rove:ok (null (gethash 3 *relation-readers*))))
       (release-relation-writer-xid 11)
       (register-relation-accesses '(2) '(3) 12)
       (rove:ok (equal '(12) (gethash 2 *relation-writers*)))
       (rove:ok (equal '(12) (gethash 3 *relation-readers*))))
     :objects 2 :owners 4))

(rove:deftest relation-resource-budget-is-shared-and-reusable
    (call-with-relation-resource-fixture
     (lambda ()
       (register-relation-readers '(1 2 3) 11)
       (rove:ok (equal '(3 3) (multiple-value-list (relation-resource-footprint))))
       (rove:ok (relation-resource-error (lambda () (register-relation-reader 4 12))))
       (release-relation-writer-xid 11)
       (register-relation-readers '(4 5 6) 12)
       (rove:ok (equal '(3 3) (multiple-value-list (relation-resource-footprint))))
       (release-relation-writer-xid 12)
       (rove:ok (equal '(0 0) (multiple-value-list (relation-resource-footprint)))))
     :objects 3 :owners 6))

(rove:deftest relation-resource-owner-budget-reuses-repeated-and-upgraded-access
    (call-with-relation-resource-fixture
     (lambda ()
       (dotimes (attempt 3)
         (declare (ignorable attempt))
         (register-relation-reader 1 11)
         (register-relation-writer 1 11))
       (register-relation-reader 1 12)
       (rove:ok (equal '(1 2) (multiple-value-list (relation-resource-footprint))))
       (rove:ok (relation-resource-error (lambda () (register-relation-reader 1 13))))
       (rove:ok (not (member 13 (gethash 1 *relation-readers*))))
       (release-relation-writer-xid 11)
       (register-relation-reader 1 13)
       (rove:ok (equal '(1 2) (multiple-value-list (relation-resource-footprint)))))
     :objects 8 :owners 2))

(rove:deftest relation-resource-build-failure-publishes-no-gates
    (call-with-relation-resource-fixture
     (lambda ()
       (let ((published nil))
         (register-relation-reader 1 11)
         (rove:ok
          (relation-resource-error
           (lambda ()
             (acquire-relation-build-locks
              '(2 3) 12 t
              (lambda (relations token)
                (declare (ignore relations token))
                (setf published t))))))
         (rove:ok (not published))
         (rove:ok (zerop (hash-table-count *relation-builders*)))
         (rove:ok (zerop (hash-table-count *relation-reader-blockers*)))
         (rove:ok (zerop (hash-table-count *relation-build-groups*)))))
     :objects 2 :owners 8))

(rove:deftest relation-resource-build-owner-reuses-existing-transaction-access
    (call-with-relation-resource-fixture
     (lambda ()
       (register-relation-reader 1 11)
       (let ((token (acquire-relation-build-locks '(1) 11 t)))
         (unwind-protect
              (progn
                (register-relation-writer 1 11)
                (rove:ok (equal '(1 1) (multiple-value-list (relation-resource-footprint)))))
           (release-relation-build-locks '(1) token)))
       (rove:ok (equal '(1 1) (multiple-value-list (relation-resource-footprint))))
       (release-relation-writer-xid 11)
       (rove:ok (equal '(0 0) (multiple-value-list (relation-resource-footprint)))))
     :objects 1 :owners 1))

(rove:deftest relation-resource-maintenance-shares-the-object-budget
    (call-with-relation-resource-fixture
     (lambda ()
       (register-relation-reader 1 11)
       (rove:ok (relation-resource-error
                 (lambda ()
                   (multiple-value-bind (relations token) (acquire-relation-maintenance-locks '(2))
                     (release-relation-maintenance-locks relations token)))))
       (rove:ok (zerop (hash-table-count *relation-maintainers*)))
       (release-relation-writer-xid 11)
       (multiple-value-bind (relations token) (acquire-relation-maintenance-locks '(2))
         (unwind-protect
              (rove:ok (equal '(1 1) (multiple-value-list (relation-resource-footprint))))
           (release-relation-maintenance-locks relations token)))
       (rove:ok (equal '(0 0) (multiple-value-list (relation-resource-footprint)))))
     :objects 1 :owners 2))

(cl-spec:defproperty relation-resource-footprint-deduplicates-ownership
    ((count (range integer 0 80)) (offset (range integer 0 15)))
  (:about relation-resource-footprint)
  (:trials (:normal 200))
  (call-with-relation-resource-fixture
   (lambda ()
     (let ((pairs nil))
       (dotimes (index count)
         (let ((node (mod (+ offset index) 9))
               (owner (1+ (mod (+ offset (* index 3)) 7))))
           (register-relation-reader node owner)
           (register-relation-writer node owner)
           (push (list node owner) pairs)))
       (multiple-value-bind (objects owners) (relation-resource-footprint)
         (and (= objects (length (remove-duplicates (mapcar #'first pairs))))
              (= owners (length (remove-duplicates pairs :test #'equal)))))))
   :objects nil :owners nil))

(rove:deftest generated-relation-resource-footprint
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'relation-resource-footprint-deduplicates-ownership :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials)))))

(rove:deftest relation-resource-gate-before-xid-reuses-the-later-owner
    (call-with-relation-resource-fixture
     (lambda ()
       (let* ((*backend* (make-backend))
              (token nil))
         (unwind-protect
              (progn
                (multiple-value-bind (relations acquired)
                    (acquire-relation-maintenance-locks '(1))
                  (declare (ignore relations))
                  (setf token acquired))
                (rove:ok (null (backend-xid *backend*)))
                (rove:ok (equal '(1 1) (multiple-value-list (relation-resource-footprint))))
                (setf (backend-xid *backend*) 11)
                (register-relation-reader 1 11)
                (rove:ok (equal '(1 1) (multiple-value-list (relation-resource-footprint)))))
           (when token (release-relation-maintenance-locks '(1) token))
           (release-relation-writer-xid 11))))
     :objects 1 :owners 1))

(rove:deftest relation-resource-partial-gate-release-keeps-remaining-owner
    (call-with-relation-resource-fixture
     (lambda ()
       (register-relation-readers '(1 2) 11)
       (let ((token (acquire-relation-build-locks '(1 2) 11 t)))
         (unwind-protect
              (progn
                (release-relation-build-locks '(1) token)
                (rove:ok (null (gethash token *relation-build-groups*)))
                (rove:ok (equal '(2 2) (multiple-value-list (relation-resource-footprint)))))
           (release-relation-build-locks '(1 2) token)))
       (release-relation-writer-xid 11)
       (rove:ok (equal '(0 0) (multiple-value-list (relation-resource-footprint)))))
     :objects 2 :owners 2))

(rove:deftest relation-resource-transaction-gate-and-writer-publish-one-owner
    (call-with-relation-resource-fixture
     (lambda ()
       (let ((*backend* (make-backend :xid 11))
             (*transaction-exclusive-relation-locks* (make-hash-table :test #'eq :synchronized t)))
         (unwind-protect
              (progn
                (acquire-transaction-relation-locks '(1))
                (rove:ok (equal '(11) (gethash 1 *relation-writers*)))
                (rove:ok (equal '(1 1) (multiple-value-list (relation-resource-footprint)))))
           (release-transaction-relation-locks *backend*)
           (release-relation-writer-xid 11)))
       (rove:ok (equal '(0 0) (multiple-value-list (relation-resource-footprint)))))
     :objects 1 :owners 1))

(rove:deftest relation-resource-callback-failure-releases-every-published-gate
    (dolist (kind '(:build :maintenance))
      (call-with-relation-resource-fixture
       (lambda ()
         (register-relation-reader 1 11)
         (let ((callback (lambda (relations token)
                           (declare (ignore relations token))
                           (error "fixture ownership publication error"))))
           (rove:ok
            (handler-case
                (progn
                  (ecase kind
                    (:build (acquire-relation-build-locks '(2) 12 t callback))
                    (:maintenance (acquire-relation-maintenance-locks '(2) callback)))
                  nil)
              (simple-error () t))))
         (rove:ok (equal '(1 1) (multiple-value-list (relation-resource-footprint))))
         (rove:ok (zerop (hash-table-count *relation-builders*)))
         (rove:ok (zerop (hash-table-count *relation-maintainers*))))
       :objects 2 :owners 4)))

(rove:deftest relation-resource-virtual-reader-and-maintenance-reuse-owner
    (call-with-relation-resource-fixture
     (lambda ()
       (let ((*backend* (make-backend :virtual-reader-xid -1)))
         (setf (gethash -1 *recovery-reader-xids*) t)
         (register-relation-reader 1 -1)
         (multiple-value-bind (relations token) (acquire-relation-maintenance-locks '(1))
           (unwind-protect
                (rove:ok (equal '(1 1) (multiple-value-list (relation-resource-footprint))))
             (release-relation-maintenance-locks relations token)))
         (release-relation-writer-xid -1 *backend*)
         (rove:ok (null (backend-virtual-reader-xid *backend*))))
       (rove:ok (equal '(0 0) (multiple-value-list (relation-resource-footprint)))))
     :objects 1 :owners 1))

(rove:deftest relation-resource-cancelled-published-builder-restores-capacity
    (call-with-relation-resource-fixture
     (lambda ()
       (let* ((*active-xids* '(11))
              (variables '(*relation-readers* *relation-writers* *relation-builders*
                           *relation-reader-blockers* *relation-build-groups* *relation-maintainers*
                           *relation-lock-object-limit* *relation-lock-owner-limit* *active-xids* *backend*))
              (bindings (mapcar #'symbol-value variables))
              (published (sb-thread:make-semaphore))
              (outcome nil)
              (worker nil))
         (register-relation-reader 1 11)
         (unwind-protect
              (progn
                (setf worker
                      (sb-thread:make-thread
                       (lambda ()
                         (handler-case
                             (progv variables bindings
                               (let ((token
                                       (acquire-relation-build-locks
                                        '(1) 22 t
                                        (lambda (relations token)
                                          (declare (ignore relations token))
                                          (sb-thread:signal-semaphore published)))))
                                 (unwind-protect (setf outcome :unexpected-return)
                                   (release-relation-build-locks '(1) token))))
                           (error (condition) (setf outcome (princ-to-string condition)))))
                       :name "relation-resource-cancelled-builder"))
                (unless (sb-thread:wait-on-semaphore published :timeout 5)
                  (error "builder did not publish its waiting gate"))
                (rove:ok (equal '(1 2) (multiple-value-list (relation-resource-footprint))))
                (sb-thread:interrupt-thread worker (lambda () (error "fixture builder cancelled")))
                (sb-thread:join-thread worker :timeout 5 :default :timeout)
                (rove:ok (not (sb-thread:thread-alive-p worker)))
                (rove:ok (and (stringp outcome) (search "fixture builder cancelled" outcome)))
                (rove:ok (equal '(1 1) (multiple-value-list (relation-resource-footprint))))
                (rove:ok (zerop (hash-table-count *relation-builders*))))
           (when (and worker (sb-thread:thread-alive-p worker))
             (sb-thread:interrupt-thread worker (lambda () (error "fixture cleanup")))
             (sb-thread:join-thread worker :timeout 5 :default :timeout))
           (release-relation-writer-xid 11))))
     :objects 1 :owners 2))
