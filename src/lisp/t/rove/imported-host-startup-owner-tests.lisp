(in-package #:pg)

(rove:deftest imported-host-owner-observes-empty-slot-inventory
    (call-with-logical-slot-real-startup-test
     (lambda ()
       (assert (zerop (hash-table-count *wal-replication-slots*)))
       (call-with-logical-slot-owned-startup-test
        (lambda (gate ownership)
          (let ((saved nil) (registry *wal-replication-slots*))
            (call-with-logical-slot-imported-host-startup-owner
             gate ownership
             (lambda (owner)
               (setf saved owner)
               (rove:ok (physical-directory-check-owner owner))
               (rove:ok (rove:signals (physical-adoption-check-owner owner) 'error))
               (sb-thread:with-mutex (*wal-lock*)
                 (multiple-value-bind (system-id history end)
                     (logical-slot-imported-host-startup-facts-under-lock owner)
                   (rove:ok (eql system-id (physical-slot-startup-system-id)))
                   (rove:ok (eql end *wal-durable-lsn*))
                   (rove:ok (= 1 (length history)))
                   (setf (timeline-history-entry-timeline (first history)) 99)
                   (rove:ok (= 1 (timeline-history-entry-timeline
                                  (first (nth-value 1
                                                    (logical-slot-imported-host-startup-facts-under-lock owner))))))))))
            (rove:ok (eq registry *wal-replication-slots*))
            (rove:ok (zerop (hash-table-count registry)))
            (rove:ok (null (logical-slot-startup-read-inventory)))
            (rove:ok (not (physical-adoption-owner-active-p saved)))
            (rove:ok (open-stream-p *postmaster-data-lock*))
            (sb-thread:with-mutex (*wal-lock*)
              (rove:ok (rove:signals (logical-slot-imported-host-startup-under-lock saved) 'error)))))))))

(rove:deftest imported-host-owner-rejects-foreign-execution-without-poisoning
    (call-with-logical-slot-real-startup-test
     (lambda ()
       (call-with-logical-slot-owned-startup-test
        (lambda (gate ownership)
          (call-with-logical-slot-imported-host-startup-owner
           gate ownership
           (lambda (owner)
             (sb-thread:with-mutex (*wal-lock*)
               (rove:ok (rove:signals
                         (logical-slot-imported-host-startup-under-lock
                          (copy-logical-slot-imported-host-startup-owner owner)) 'error)))
             (rove:ok
              (eq :rejected
                  (sb-thread:join-thread
                   (sb-thread:make-thread
                    (lambda ()
                      (handler-case (logical-slot-imported-host-startup-check-owner owner)
                        (error () :rejected)))))))
             (let ((*wal-lock* (sb-thread:make-mutex :name "foreign host owner test")))
               (sb-thread:with-mutex (*wal-lock*)
                 (rove:ok (rove:signals (logical-slot-imported-host-startup-under-lock owner) 'error))))
             (rove:ok (logical-slot-imported-host-startup-check-owner owner)))))))))

(rove:deftest imported-host-owner-latches-observed-source-failure
    (dolist (kind '(:active :registration :registry :incarnation :durable :postmaster))
      (let ((entered nil))
        (rove:ok
         (rove:signals
          (call-with-logical-slot-real-startup-test
           (lambda ()
             (call-with-logical-slot-owned-startup-test
              (lambda (gate ownership)
                (call-with-logical-slot-imported-host-startup-owner
                 gate ownership
                 (lambda (owner)
                   (setf entered t)
                   (ecase kind
                     (:active
                      (setf (physical-adoption-owner-active-p owner) nil)
                      (rove:ok (rove:signals (logical-slot-imported-host-startup-check-owner owner) 'error))
                      (setf (physical-adoption-owner-active-p owner) t))
                     (:registration
                      (let ((*logical-slot-imported-host-startup-owners* nil))
                        (rove:ok (rove:signals (logical-slot-imported-host-startup-check-owner owner) 'error))))
                     (:registry
                      (let ((*wal-replication-slots* (make-hash-table :test #'equal)))
                        (sb-thread:with-mutex (*wal-lock*)
                          (rove:ok (rove:signals (logical-slot-imported-host-startup-under-lock owner) 'error)))))
                     (:incarnation
                      (let ((*wal-storage-incarnation* (list :foreign)))
                        (sb-thread:with-mutex (*wal-lock*)
                          (rove:ok (rove:signals (logical-slot-imported-host-startup-under-lock owner) 'error)))))
                     (:durable
                      (let ((*wal-durable-lsn* (1+ *wal-durable-lsn*)))
                        (sb-thread:with-mutex (*wal-lock*)
                          (rove:ok (rove:signals (logical-slot-imported-host-startup-under-lock owner) 'error)))))
                     (:postmaster
                      (let ((*postmaster-pid-inode* nil))
                        (rove:ok (rove:signals (logical-slot-imported-host-startup-check-owner owner) 'error)))))
                   (rove:ok (rove:signals (logical-slot-imported-host-startup-check-owner owner) 'error))))))))
          'error))
        (rove:ok entered))))

(rove:deftest imported-host-owner-rejects-control-catalog-identity-mismatch
    (call-with-logical-slot-real-startup-test
     (lambda ()
       (call-with-logical-slot-owned-startup-test
        (lambda (gate ownership)
          (let ((bytes (logical-slot-startup-control-image)) (entered nil))
            (unwind-protect
                 (let ((*wal-control-metadata* (copy-tree *wal-control-metadata*)))
                   (incf (getf *wal-control-metadata* :system-id))
                   (wal-save-control-fields *wal-checkpoint-lsn* *wal-durable-lsn*)
                   (rove:ok (equalp *wal-control-metadata*
                                    (seventh (multiple-value-list (wal-read-control-fields)))))
                   (rove:ok
                    (rove:signals
                     (call-with-logical-slot-imported-host-startup-owner
                      gate ownership (lambda (owner) (declare (ignore owner)) (setf entered t)))
                     'error))
                   (rove:ok (not entered)))
              (wal-atomic-write-bytes (wal-control-path) bytes))))))))
