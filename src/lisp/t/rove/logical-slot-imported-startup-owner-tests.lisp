(in-package #:pg)

(rove:deftest imported-startup-owner-borrows-unpublished-candidate
    (let ((saved nil))
      (call-with-imported-startup-principal-test
       (lambda (source entry)
         (let ((slot (logical-slot-startup-entry-slot entry)))
           (rove:ok (not (eq slot (gethash (wal-replication-slot-name slot) *wal-replication-slots*))))
           (call-with-logical-slot-imported-startup-owner
            source entry
            (lambda (owner)
              (setf saved owner)
              (rove:ok (physical-directory-check-owner owner))
              (rove:ok (rove:signals (physical-adoption-check-owner owner) 'error))
              (setf (wal-replication-slot-pending-xids slot) '(17 19))
              (sb-thread:with-mutex (*wal-lock*)
                (rove:ok (logical-slot-imported-startup-owner-under-lock owner))
                (rove:ok (rove:signals
                          (logical-slot-imported-startup-owner-under-lock
                           (copy-logical-slot-imported-startup-owner owner)) 'error))
                (rove:ok (logical-slot-imported-startup-owner-under-lock owner)))))
           (rove:ok (open-stream-p *postmaster-data-lock*))
           (rove:ok (open-stream-p *postmaster-pid-stream*))
           (sb-thread:with-mutex (*wal-lock*)
             (rove:ok (rove:signals (logical-slot-imported-startup-owner-under-lock saved) 'error))))))
      (rove:ok (not (physical-adoption-owner-active-p saved)))))

(rove:deftest imported-startup-owner-rejects-foreign-entry-and-execution
    (call-with-imported-startup-principal-test
     (lambda (source entry)
       (dolist (candidate (list (copy-logical-slot-startup-entry entry)
                                (find-if (lambda (other)
                                           (not (wal-replication-slot-child-owner
                                                 (logical-slot-startup-entry-slot other))))
                                         (logical-slot-startup-source-entries source))))
         (let ((entered nil))
           (rove:ok (rove:signals
                     (call-with-logical-slot-imported-startup-owner
                      source candidate (lambda (owner) (declare (ignore owner)) (setf entered t)))
                     'error))
           (rove:ok (not entered))))
       (call-with-logical-slot-imported-startup-owner
        source entry
        (lambda (owner)
          (let ((foreign-result
                  (sb-thread:join-thread
                   (sb-thread:make-thread
                    (lambda ()
                      (handler-case (logical-slot-imported-startup-check-owner owner)
                        (error () :rejected)))))))
            (rove:ok (eq :rejected foreign-result)))
          (let ((*wal-lock* (sb-thread:make-mutex :name "foreign startup owner test")))
            (sb-thread:with-mutex (*wal-lock*)
              (rove:ok (rove:signals (logical-slot-imported-startup-owner-under-lock owner) 'error))))
          (rove:ok (logical-slot-imported-startup-check-owner owner)))))))

(rove:deftest imported-startup-owner-sticky-failure-and-unwind
    (dolist (failure '(:active :registration :principal :postmaster :control))
      (let ((entered nil))
        (rove:ok
         (rove:signals
          (call-with-imported-startup-principal-test
           (lambda (source entry)
             (call-with-logical-slot-imported-startup-owner
              source entry
              (lambda (owner)
                (setf entered t)
                (ecase failure
                  (:active
                   (setf (physical-adoption-owner-active-p owner) nil)
                   (rove:ok (rove:signals (logical-slot-imported-startup-check-owner owner) 'error)))
                  (:registration
                   (let ((*logical-slot-imported-startup-owners* nil))
                     (rove:ok (rove:signals (logical-slot-imported-startup-check-owner owner) 'error))))
                  (:principal
                   (let ((*logical-slot-imported-startup-principals* nil))
                     (sb-thread:with-mutex (*wal-lock*)
                       (rove:ok (rove:signals (logical-slot-imported-startup-owner-under-lock owner) 'error)))))
                  (:postmaster
                   (let ((*postmaster-pid-inode* nil))
                     (rove:ok (rove:signals (logical-slot-imported-startup-check-owner owner) 'error))))
                  (:control
                   (let* ((bytes (logical-slot-startup-source-control source))
                          (prior (aref bytes 0)))
                     (unwind-protect
                          (progn
                            (setf (aref bytes 0) (logxor 1 prior))
                            (sb-thread:with-mutex (*wal-lock*)
                              (rove:ok (rove:signals
                                        (logical-slot-imported-startup-owner-under-lock owner) 'error))))
                       (setf (aref bytes 0) prior)))))
                (setf (physical-adoption-owner-active-p owner) t
                      (logical-slot-startup-source-active-p source) t)
                (sb-thread:with-mutex (*wal-lock*)
                  (rove:ok (rove:signals (logical-slot-imported-startup-owner-under-lock owner) 'error)))))))
          'error))
        (rove:ok entered)))
  (call-with-imported-startup-principal-test
   (lambda (source entry)
     (let ((saved nil))
       (rove:ok
        (eq :escaped
            (catch 'startup-owner-exit
              (call-with-logical-slot-imported-startup-owner
               source entry (lambda (owner) (setf saved owner) (throw 'startup-owner-exit :escaped))))))
       (rove:ok (not (physical-adoption-owner-active-p saved)))
       (rove:ok (open-stream-p *postmaster-data-lock*))
       (call-with-logical-slot-imported-startup-owner
        source entry (lambda (owner) (rove:ok (logical-slot-imported-startup-check-owner owner))))))))
