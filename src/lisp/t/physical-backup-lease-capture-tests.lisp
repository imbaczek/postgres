(in-package #:pg-physical-tests)

(cl-spec:defproperty backup-lease-capture-owner-rejects-detached-context
    ((kind (range integer 0 4)))
  (:about pg::physical-backup-lease-check-capture-owner)
  (:trials (:normal 100))
  (call-with-backup-lease-authority-fixture
   (lambda (root)
     (declare (ignore root))
     (let ((request (backup-lease-test-request))
           (rejected nil))
       (pg::call-with-physical-backup-lease-capture
        request
        (lambda (owner)
          (let ((pg::*backend* (if (= kind 0) nil pg::*backend*))
                (pg::*wal-storage-incarnation* (if (= kind 1) (gensym) pg::*wal-storage-incarnation*))
                (pg::*wal-segment-namespace* (if (= kind 2) :detached pg::*wal-segment-namespace*))
                (pg::*physical-backup-lease-capture-owner*
                  (if (= kind 3) nil pg::*physical-backup-lease-capture-owner*))
                (pg::*physical-backup-lease-capture-owners*
                  (if (= kind 4) (make-hash-table :test #'equal)
                      pg::*physical-backup-lease-capture-owners*)))
            (pg::with-write-lock
              (pg::with-wal-critical
                (setf rejected (backup-lease-authority-error-p
                                (lambda () (pg::physical-backup-lease-check-capture-owner owner)))))))))
       (and rejected (not pg::*wal-poisoned*)
            (zerop (hash-table-count pg::*physical-backup-lease-capture-owners*)))))))

(deftest generated-backup-lease-capture-owner-boundaries
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-capture-owner-rejects-detached-context
                                         :seed 92 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest backup-lease-capture-retries-never-acquire-a-second-owner
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((request (backup-lease-test-request)) (calls 0))
         (multiple-value-bind (value claimed)
             (pg::call-with-physical-backup-lease-capture
              request
              (lambda (owner)
                (declare (ignore owner))
                (incf calls)
                (ok (eq :capturing (getf (pg::physical-backup-lease-status request) :phase)))
                (multiple-value-bind (record duplicate)
                    (pg::call-with-physical-backup-lease-capture
                     request (lambda (owner) (declare (ignore owner)) (incf calls)))
                  (ok (not duplicate))
                  (ok (eq :capturing (getf record :phase))))
                :first))
           (ok claimed)
           (ok (eq :first value)))
         (ok (= 1 calls))
         (ok (eq :interrupted (getf (pg::physical-backup-lease-status request) :phase)))
         (pg::call-with-physical-backup-lease-capture
          request (lambda (owner) (declare (ignore owner)) (incf calls)))
         (ok (= 1 calls))
         (ok (zerop (hash-table-count pg::*physical-backup-lease-capture-owners*)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-abort-before-capture-permanently-reserves-the-request
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((request (backup-lease-test-request)) (calls 0))
         (ok (null (pg::physical-backup-lease-status request)))
         (ok (eq :closed (getf (pg::physical-backup-lease-abort request) :phase)))
         (multiple-value-bind (record claimed)
             (pg::call-with-physical-backup-lease-capture
              request (lambda (owner) (declare (ignore owner)) (incf calls)))
           (ok (not claimed))
           (ok (eq :closed (getf record :phase))))
         (ok (zerop calls))
         (let ((wrong (copy-tree request)))
           (setf (getf wrong :token) (make-string 64 :initial-element #\b))
           (ok (intake-fails-p (lambda () (pg::physical-backup-lease-status wrong))))
           (ok (intake-fails-p (lambda () (pg::physical-backup-lease-abort wrong)))))
         (ok (eq :closed (getf (pg::physical-backup-lease-abort request) :phase)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-abort-waits-for-the-capture-owner-before-retiring-its-pin
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((request (backup-lease-test-request)) (name (getf request :name)) (pin nil))
         (pg::call-with-physical-backup-lease-capture
          request
          (lambda (owner)
            (declare (ignore owner))
            (setf pin (pg::with-live-operation-quiescence
                        (pg::physical-backup-lease-create-pin request)))
            (ok (eq :closing (getf (pg::physical-backup-lease-abort request) :phase)))
            (ok (probe-file (pg::physical-source-pin-path name)))
            (ok (= 32 (gethash pin pg::*wal-snapshot-retention-pins*)))
            (ok (eq :closing (getf (pg::physical-backup-lease-abort request) :phase)))))
         (ok (eq :closed (getf (pg::physical-backup-lease-status request) :phase)))
         (ok (not (probe-file (pg::physical-source-pin-path name))))
         (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (zerop (hash-table-count pg::*physical-backup-lease-capture-owners*)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-capture-error-keeps-the-pin-and-interrupts-the-attempt
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((request (backup-lease-test-request)) (name (getf request :name)) (pin nil))
         (ok (intake-fails-p
              (lambda ()
                (pg::call-with-physical-backup-lease-capture
                 request
                 (lambda (owner)
                   (declare (ignore owner))
                   (setf pin (pg::with-live-operation-quiescence
                               (pg::physical-backup-lease-create-pin request)))
                   (error "capture fixture failure"))))))
         (ok (eq :interrupted (getf (pg::physical-backup-lease-status request) :phase)))
         (ok (probe-file (pg::physical-source-pin-path name)))
         (ok (= 32 (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (zerop (hash-table-count pg::*physical-backup-lease-capture-owners*)))
         (ok (eq :closed (getf (pg::physical-backup-lease-abort request) :phase)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-capture-abort-after-claim-prevents-entry
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let* ((request (backup-lease-test-request))
              (calls 0)
              (pg::*physical-backup-lease-test-hook*
                (lambda (point record)
                  (declare (ignore record))
                  (when (eq point :after-capture-claim)
                    (ok (eq :closing (getf (pg::physical-backup-lease-abort request) :phase)))))))
         (ok (intake-fails-p
              (lambda ()
                (pg::call-with-physical-backup-lease-capture
                 request (lambda (owner) (declare (ignore owner)) (incf calls))))))
         (ok (zerop calls))
         (ok (eq :closed (getf (pg::physical-backup-lease-status request) :phase)))
         (ok (zerop (hash-table-count pg::*physical-backup-lease-capture-owners*)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-capture-nonlocal-exit-interrupts-and-releases-its-owner
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((request (backup-lease-test-request)))
         (ok (eq :cut
                 (catch 'capture-cut
                   (pg::call-with-physical-backup-lease-capture
                    request (lambda (owner) (declare (ignore owner)) (throw 'capture-cut :cut))))))
         (ok (eq :interrupted (getf (pg::physical-backup-lease-status request) :phase)))
         (ok (zerop (hash-table-count pg::*physical-backup-lease-capture-owners*)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-capture-cleanup-keeps-its-original-request-after-owner-mutation
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((request (backup-lease-test-request)))
         (ok (intake-fails-p
              (lambda ()
                (pg::call-with-physical-backup-lease-capture
                 request
                 (lambda (owner)
                   (setf (getf (pg::physical-backup-lease-capture-owner-request owner) :name)
                         "reseed_ffffffffffffffffffffffffffffffff")
                   (error "mutated capture owner fixture"))))))
         (ok (zerop (hash-table-count pg::*physical-backup-lease-capture-owners*)))
         (ok (eq :interrupted (getf (pg::physical-backup-lease-status request) :phase)))
         (ok (eq :closed (getf (pg::physical-backup-lease-abort request) :phase)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-poisoned-capture-releases-runtime-owner-for-cold-recovery
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((request (backup-lease-test-request))
             (seen nil))
         (let ((pg::*physical-source-test-hook*
                 (lambda (point pin)
                   (declare (ignore pin))
                   (when (eq point :after-save)
                     (setf seen t)
                     (throw 'capture-save-cut :cut)))))
           (ok (intake-fails-p
                (lambda ()
                  (catch 'capture-save-cut
                    (pg::call-with-physical-backup-lease-capture
                     request
                     (lambda (owner)
                       (declare (ignore owner))
                       (pg::with-live-operation-quiescence
                         (pg::physical-backup-lease-create-pin request)))))))))
         (ok seen)
         (ok pg::*wal-poisoned*)
         (ok (zerop (hash-table-count pg::*physical-backup-lease-capture-owners*)))
         (let ((pg::*wal-poisoned* nil))
           (pg::physical-backup-lease-load-records)
           (pg::physical-source-pin-load)
           (ok (eq :interrupted (getf (pg::physical-backup-lease-status request) :phase)))
           (ok (eq :closed (getf (pg::physical-backup-lease-abort request) :phase)))
           (ok (not pg::*wal-poisoned*)))))))

(deftest backup-lease-active-capture-denies-inventory-reload-and-direct-retirement
    (call-with-backup-lease-authority-fixture
     (lambda (root)
       (declare (ignore root))
       (let ((request (backup-lease-test-request)))
         (pg::call-with-physical-backup-lease-capture
          request
          (lambda (owner)
            (declare (ignore owner))
            (let ((pin (pg::with-live-operation-quiescence
                         (pg::physical-backup-lease-create-pin request))))
              (ok (intake-fails-p #'pg::physical-backup-lease-load-records))
              (ok (eq :closing (getf (pg::physical-backup-lease-abort request) :phase)))
              (ok (intake-fails-p (lambda () (pg::physical-backup-lease-finish-closing request))))
              (pg::with-write-lock
                (pg::with-wal-critical
                  (ok (intake-fails-p (lambda () (pg::physical-source-pin-unlink pin request))))))
              (ok (= 32 (gethash pin pg::*wal-snapshot-retention-pins*))))))
         (ok (eq :closed (getf (pg::physical-backup-lease-status request) :phase)))
         (ok (not pg::*wal-poisoned*))))))
