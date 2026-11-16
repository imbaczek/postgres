(in-package #:pg)

(cl-spec:defproperty continuation-staged-group-never-authorizes-runtime
    ((generation (range integer 0 340282366920938463463374607431768211455)))
  (:about logical-slot-continuation-build-protection-under-lock)
  (:trials (:normal 80))
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs generation)
    (let* ((binding (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)))
           (snapshot (logical-slot-continuation-active-snapshot binding certificate host rows))
           (image (logical-slot-continuation-encode snapshot))
           (registry (make-hash-table :test #'equal))
           (*logical-slot-continuation-protections* (make-hash-table :test #'equal))
           (*logical-slot-continuation-protection-registries* (make-hash-table :test #'eq))
           (*logical-slot-continuation-protection-identities* (make-hash-table :test #'eq))
           (*wal-snapshot-retention-pins* (make-hash-table :test #'eq)))
      (dolist (row (getf snapshot :slots))
        (setf (gethash (first row) registry) (logical-slot-state-decode (second row) (first row))))
      (sb-thread:with-mutex (*wal-lock*)
        (multiple-value-bind (group control cleanup)
            (logical-slot-continuation-build-protection-under-lock
             registry image binding '(1 2 3) nil :timeline :staged)
          (setf (gethash '(1 2 3) *logical-slot-continuation-protections*) group
                (gethash registry *logical-slot-continuation-protection-registries*) group
                (gethash group *logical-slot-continuation-protection-identities*)
                (logical-slot-continuation-protection-state group)
                (gethash group *wal-snapshot-retention-pins*) 96)
          (and
           (eq :staged (svref control 0))
           (every (lambda (operation)
                    (handler-case
                        (progn (logical-slot-continuation-protection-call group operation nil) nil)
                      (error () t)))
                  '(:check :begin :owns-cell :cancel))
           (equalp image (logical-slot-continuation-protection-call group :image))
           (eq cleanup (logical-slot-continuation-protection-call group :failure-cleanup))
           (funcall cleanup)
           (svref control 1)
           (loop for slot being the hash-values of registry
                 always (wal-replication-slot-state-uncertain-p slot))
           (= 96 (gethash group *wal-snapshot-retention-pins*))))))))

(rove:deftest continuation-staged-group-generated-runtime-refusal-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-staged-group-never-authorizes-runtime
                                         :seed 11541 :profile :normal))))
      (format t "~&COLD-STAGED-GROUP-CONTRACT ~s~%" result)
      (rove:ok (eq :passed (getf result :status)))
      (rove:ok (= 80 (getf result :trials)))))

(cl-spec:defproperty continuation-authority-map-copy-preserves-independent-registrations
    ((count (range integer 0 16)) (mode (range integer 0 7)))
  (:about logical-slot-continuation-cold-copy-map)
  (:trials (:normal 80))
  (let* ((table (make-hash-table :test (if (oddp mode) #'eq #'equal)
                                 :weakness (when (logbitp 1 mode) :key)
                                 :synchronized (logbitp 2 mode)))
         (keys (loop for index below count collect (list :root index)))
         (values (loop for index below count collect (list :authority index))))
    (loop for key in keys for value in values do (setf (gethash key table) value))
    (let ((copy (logical-slot-continuation-cold-copy-map table)))
      (and (not (eq table copy))
           (eq (hash-table-test table) (hash-table-test copy))
           (eq (sb-ext:hash-table-weakness table) (sb-ext:hash-table-weakness copy))
           (eq (sb-ext:hash-table-synchronized-p table) (sb-ext:hash-table-synchronized-p copy))
           (logical-slot-continuation-cold-map-matches-p
            copy (logical-slot-continuation-cold-map-image table))
           (loop for key in keys for value in values always (eq value (gethash key copy)))
           (or (null keys)
               (progn
                 (setf (gethash (first keys) copy) :replacement)
                 (eq (first values) (gethash (first keys) table))))))))

(rove:deftest continuation-authority-map-copy-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-authority-map-copy-preserves-independent-registrations
                                         :seed 11542 :profile :normal))))
      (format t "~&COLD-AUTHORITY-MAP-COPY-CONTRACT ~s~%" result)
      (rove:ok (eq :passed (getf result :status)))
      (rove:ok (= 80 (getf result :trials)))))
