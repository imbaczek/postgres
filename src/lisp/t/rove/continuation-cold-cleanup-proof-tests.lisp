(in-package #:pg)

(declaim (ftype function logical-slot-continuation-active-child-snapshot
                logical-slot-continuation-empty-snapshot
                logical-slot-continuation-check-empty-snapshot))

(cl-spec:defproperty continuation-cleanup-child-bootstrap-matches-full-proof
    ((generation (range integer 0 340282366920938463463374607431768211455))
     (restart (range integer 32 64)))
  (:about logical-slot-continuation-active-child-snapshot)
  (:trials (:normal 80))
  (multiple-value-bind (certificate host rows)
      (unit-continuation-active-inputs generation restart)
    (let* ((binding (string->bytes "binding"))
           (ordinary (physical-slot-encode
                      (make-wal-replication-slot
                       :name "ordinary" :kind :physical :system-id 42
                       :database-oid nil :plugin nil :consistent-lsn nil
                       :restart-lsn 17 :restart-timeline 1 :inactive-since 0)))
           (full (logical-slot-continuation-active-snapshot
                  binding certificate host (append rows (list (list "ordinary" ordinary)))))
           (children (logical-slot-continuation-active-child-snapshot
                      binding certificate host rows)))
      (and (equalp full children)
           (equalp (logical-slot-continuation-encode full)
                   (logical-slot-continuation-encode children))))))

(rove:deftest continuation-cleanup-child-bootstrap-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-cleanup-child-bootstrap-matches-full-proof
                                         :seed 11912 :profile :normal))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 80 (getf result :trials)))))

(rove:deftest continuation-cleanup-child-bootstrap-excludes-unrelated-temporary-eligibility
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (let* ((binding (string->bytes "binding"))
             (temporary (physical-slot-encode
                         (make-wal-replication-slot
                          :name "temporary" :kind :physical :system-id 999
                          :database-oid nil :plugin nil :consistent-lsn nil
                          :restart-lsn 99999 :restart-timeline 77 :inactive-since 0
                          :temporary-p t)))
             (mixed (append rows (list (list "temporary" temporary)))))
        (rove:ok (handler-case
                     (progn (logical-slot-continuation-active-snapshot binding certificate host mixed) nil)
                   (error () t)))
        (rove:ok (equalp (logical-slot-continuation-active-snapshot binding certificate host rows)
                         (logical-slot-continuation-active-child-snapshot binding certificate host rows)))
        (rove:ok (handler-case
                     (progn (logical-slot-continuation-active-child-snapshot
                             binding certificate host mixed) nil)
                   (error () t)))
        (rove:ok (handler-case
                     (progn (logical-slot-continuation-active-child-snapshot
                             binding certificate host nil) nil)
                   (error () t))))))

(rove:deftest continuation-cleanup-empty-metadata-does-not-require-an-empty-filesystem
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (unit-continuation-inventory-fixture
       rows
       (lambda (directory check)
         (let* ((*pgdata* (uiop:pathname-parent-directory-pathname
                           (truename (physical-image-fd-path directory))))
                (*wal-durable-lsn* 192)
                (binding (string->bytes "binding"))
                (snapshot (logical-slot-continuation-empty-snapshot binding certificate host)))
           (rove:ok (= 2 (length (logical-slot-continuation-read-slot-inventory directory check))))
           (rove:ok (logical-slot-continuation-check-empty-snapshot
                     snapshot binding certificate host))
           (rove:ok (null (getf snapshot :slots)))
           (rove:ok (handler-case
                        (progn (logical-slot-continuation-require-empty-inventory) nil)
                      (error () t)))
           (let ((changed-certificate (copy-tree certificate)))
             (setf (getf changed-certificate :system-id) 999)
             (rove:ok (handler-case
                          (progn (logical-slot-continuation-check-empty-snapshot
                                  snapshot binding changed-certificate host) nil)
                        (error () t))))
           (dolist (mode '(:binding :system :seal))
             (let ((changed (logical-slot-continuation-copy snapshot)))
               (ecase mode
                 (:binding (setf (getf changed :binding) (string->bytes "other")))
                 (:system (setf (getf changed :system-id) 99))
                 (:seal (setf (getf changed :sealed-end) '(193 2 :prefix-end))))
               (rove:ok (handler-case
                            (progn (logical-slot-continuation-check-empty-snapshot
                                    changed binding certificate host) nil)
                          (error () t))))))))))

(rove:deftest continuation-cleanup-empty-metadata-keeps-descendant-history-support
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (declare (ignore rows))
      (let* ((binding (string->bytes "binding"))
             (snapshot (logical-slot-continuation-empty-snapshot binding certificate host))
             (later (copy-list host)))
        (setf (fourth later) (timeline-encode-history 3 2 224 "later" (fourth host))
              (fifth later) 3 (sixth later) 256)
        (rove:ok (logical-slot-continuation-check-empty-snapshot
                  snapshot binding certificate later)))))
