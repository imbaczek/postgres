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

(rove:deftest imported-startup-preparation-keeps-captured-registry
    (call-with-imported-startup-principal-test
     (lambda (source entry)
       (let* ((registry *wal-replication-slots*) (calls 0)
                                                 (candidates (mapcar #'logical-slot-startup-entry-slot
                                                                     (logical-slot-startup-source-entries source)))
                                                 (restored
                                                   (logical-slot-prepare-startup-inventory
                                                    candidates (wal-discover-segments) (postgres-current-timestamp)
                                                    (lambda (slot)
                                                      (incf calls)
                                                      (rove:ok (eq registry *wal-replication-slots*))
                                                      (sb-thread:with-mutex (*wal-lock*)
                                                        (rove:ok (logical-slot-imported-startup-principal-under-lock source)))
                                                      (when (wal-replication-slot-child-owner slot)
                                                        (call-with-logical-slot-imported-startup-owner
                                                         source entry
                                                         (lambda (owner)
                                                           (rove:ok (logical-slot-imported-startup-check-owner owner)))))
                                                      slot))))
         (rove:ok (= calls (length candidates)))
         (rove:ok (eq registry *wal-replication-slots*))
         (rove:ok (not (eq registry restored)))
         (dolist (slot candidates)
           (rove:ok (eq slot (gethash (wal-replication-slot-name slot) restored))))
         (sb-thread:with-mutex (*wal-lock*)
           (rove:ok (logical-slot-imported-startup-principal-under-lock source)))))))

(rove:deftest imported-startup-preparation-preflights-and-rejects-substitution
    (call-with-imported-startup-principal-test
     (lambda (source entry)
       (declare (ignore source))
       (let ((slot (logical-slot-startup-entry-slot entry))
             (registry *wal-replication-slots*) (calls 0))
         (rove:ok (rove:signals
                   (logical-slot-prepare-startup-inventory
                    (list slot slot) nil 0 (lambda (candidate) (incf calls) candidate)) 'error))
         (rove:ok (zerop calls))
         (dolist (result (list nil (copy-wal-replication-slot slot)))
           (rove:ok (rove:signals
                     (logical-slot-prepare-startup-inventory
                      (list slot) nil 0 (lambda (candidate) (declare (ignore candidate)) result))
                     'error)))
         (rove:ok (eq registry *wal-replication-slots*))))))

(rove:deftest imported-startup-preparation-rechecks-names-after-callbacks
    (call-with-imported-startup-principal-test
     (lambda (source entry)
       (let* ((slot (logical-slot-startup-entry-slot entry))
              (name (wal-replication-slot-name slot))
              (candidates (mapcar #'logical-slot-startup-entry-slot
                                  (logical-slot-startup-source-entries source)))
              (other (find-if (lambda (candidate) (not (eq slot candidate))) candidates))
              (registry *wal-replication-slots*) (changed nil))
         (unwind-protect
              (rove:ok
               (rove:signals
                (logical-slot-prepare-startup-inventory
                 candidates nil 0
                 (lambda (candidate)
                   (when (eq slot candidate)
                     (setf (wal-replication-slot-name slot) (wal-replication-slot-name other)
                           changed t))
                   candidate))
                'error))
           (setf (wal-replication-slot-name slot) name))
         (rove:ok changed)
         (rove:ok (eq registry *wal-replication-slots*))))))

(rove:deftest startup-preparation-refuses-redirected-temporary-cleanup
    (call-with-logical-slot-real-startup-test
     (lambda ()
       (create-standard-logical-slot "prepare_logical" :nothing)
       (wal-create-physical-replication-slot "prepare_temp" :temporary t)
       (wal-create-physical-replication-slot "prepare_victim")
       (let* ((entries (logical-slot-startup-read-inventory))
              (candidates (mapcar #'logical-slot-startup-entry-slot entries))
              (temporary (find "prepare_temp" candidates :key #'wal-replication-slot-name :test #'equal))
              (original-name (wal-replication-slot-name temporary))
              (paths (mapcar #'wal-replication-slot-path '("prepare_temp" "prepare_victim")))
              (images (mapcar (lambda (path) (read-bounded-regular-file path 65536 :minimum 1)) paths))
              (changed nil))
         (unwind-protect
              (rove:ok
               (rove:signals
                (logical-slot-prepare-startup-inventory
                 candidates nil 0
                 (lambda (slot)
                   (setf (wal-replication-slot-name temporary) "prepare_victim"
                         changed t)
                   slot))
                'error))
           (setf (wal-replication-slot-name temporary) original-name))
         (rove:ok changed)
         (loop for path in paths for image in images
               do (rove:ok (equalp image (read-bounded-regular-file path 65536 :minimum 1))))))))

(rove:deftest startup-preparation-bounds-callback-list-mutations
    (dolist (change '(:cycle :reorder :extra))
      (call-with-imported-startup-principal-test
       (lambda (source entry)
         (declare (ignore entry))
         (let* ((candidates (mapcar #'logical-slot-startup-entry-slot
                                    (logical-slot-startup-source-entries source)))
                (calls 0) (count (length candidates)))
           (rove:ok
            (rove:signals
             (logical-slot-prepare-startup-inventory
              candidates nil 0
              (lambda (slot)
                (when (= 1 (incf calls))
                  (ecase change
                    (:cycle (setf (cdr (last candidates)) candidates))
                    (:reorder (rotatef (first candidates) (second candidates)))
                    (:extra (setf (cdr (last candidates)) (list slot)))))
                slot))
             'error))
           (rove:ok (= count calls)))))))
