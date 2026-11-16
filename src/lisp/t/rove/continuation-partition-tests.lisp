(in-package #:pg)

(defun unit-continuation-partition-physical (name &key (system-id 42) (restart 17)
                                                    (timeline 1) temporary)
  (list name
        (wal-encode-replication-slot
         (make-wal-replication-slot
          :name name :kind :physical :system-id system-id :database-oid nil
          :plugin nil :consistent-lsn nil :confirmed-flush-lsn nil
          :restart-lsn restart :restart-timeline timeline :inactive-since 0
          :temporary-p temporary :wal-status :reserved))))

(cl-spec:defproperty continuation-partition-preserves-full-inventory-and-row-identities
    ((generation (range integer 0 340282366920938463463374607431768211455))
     (count (range integer 0 8)))
  (:about logical-slot-continuation-partition-rows)
  (:trials (:normal 80))
  (multiple-value-bind (certificate host children) (unit-continuation-active-inputs generation)
    (let* ((ordinary
             (loop for index below count
                   collect (unit-continuation-partition-physical (format nil "physical~d" index))))
           (ordinary (if (oddp generation) (reverse ordinary) ordinary))
           (rows (if (oddp generation) (append ordinary children) (append children ordinary)))
           (before (logical-slot-continuation-copy rows)))
      (multiple-value-bind (selected others)
          (uiop:symbol-call :pg :logical-slot-continuation-partition-rows rows certificate host)
        (and (equalp before rows)
             (= (length rows) (+ (length selected) (length others)))
             (= (length selected) (length children))
             (= (length others) (length ordinary))
             (every #'eq selected children)
             (every #'eq others ordinary))))))

(cl-spec:defproperty continuation-partition-rejects-unproved-ordinary-and-child-grafts
    ((generation (range integer 0 340282366920938463463374607431768211455))
     (mode (range integer 0 7)))
  (:about logical-slot-continuation-partition-rows)
  (:trials (:normal 80))
  (multiple-value-bind (certificate host children) (unit-continuation-active-inputs generation)
    (let* ((alpha (assoc "alpha" children :test #'string=))
           (extra
             (case mode
               (0 (unit-continuation-partition-physical "physical" :system-id 43))
               (1 (unit-continuation-partition-physical "physical" :restart 193 :timeline 2))
               (2 (unit-continuation-partition-physical "physical" :timeline 99))
               (3 (unit-continuation-partition-physical "physical" :temporary t))
               (4 (let ((slot (logical-slot-state-decode (second alpha) "alpha")))
                    (setf (wal-replication-slot-name slot) "foreign")
                    (list "foreign" (logical-slot-state-encode slot))))
               (5 (let ((slot (unit-imported-state-with-origin
                               (logical-slot-state-decode (second alpha) "alpha") :owner nil)))
                    (list "alpha" (logical-slot-state-encode slot))))
               (6 (unit-continuation-partition-physical "alpha"))
               (7 (unit-continuation-partition-physical "physical"))))
           (rows
             (case mode
               ((5 6) (cons extra (remove alpha children :test #'eq)))
               (7 (append children (list extra extra)))
               (otherwise (append children (list extra)))))
           (before (logical-slot-continuation-copy rows)))
      (and (handler-case
               (progn
                 (uiop:symbol-call :pg :logical-slot-continuation-partition-rows rows certificate host)
                 nil)
             (error () t))
           (equalp before rows)))))

(cl-spec:defproperty continuation-partition-keeps-selected-generation-and-choice
    ((identity (range integer 0 340282366920938463463374607431768211455))
     (mode (range integer 0 2))
     (count (range integer 0 6)))
  (:about logical-slot-continuation-cold-classify)
  (:trials (:normal 80))
  (multiple-value-bind (certificate host ignored) (unit-continuation-active-inputs identity)
    (declare (ignore ignored))
    (let* ((advance (mod identity 17))
           (record (unit-continuation-prepared :generation (1+ (mod identity 100))
                                               :identity identity :advance advance))
           (choice (if (= mode 2) :new :old))
           (snapshot (getf record choice))
           (kind (if (zerop mode) :stable :prepared))
           (model (if (zerop mode) snapshot record))
           (rows (append (getf snapshot :slots)
                         (loop for index below count
                               collect (unit-continuation-partition-physical
                                        (format nil "physical~d" index)))))
           (before (logical-slot-continuation-copy rows)))
      (setf (sixth host) (+ 224 advance))
      (multiple-value-bind (selected actual)
          (logical-slot-continuation-cold-classify
           kind model rows (getf snapshot :binding) certificate host)
        (and (eq selected snapshot)
             (eq actual (if (zerop mode) :stable choice))
             (equalp before rows))))))

(cl-spec:defproperty continuation-mixed-reproof-normalizes-only-physical-inactivity
    ((tick (range integer 0 4611686018427387903))
     (index (range integer 0 5)))
  (:about logical-slot-continuation-cold-preparation-semantic-image)
  (:trials (:normal 80))
  (let* ((row (unit-continuation-partition-physical "physical"))
         (physical (wal-decode-replication-slot-image (second row) (first row)))
         (logical (unit-imported-state-baseline))
         (registry (make-hash-table :test #'equal)))
    (setf (gethash "physical" registry) physical
          (gethash (wal-replication-slot-name logical) registry) logical
          (wal-replication-slot-inactive-since physical) tick)
    (let* ((first (logical-slot-publication-registry-image registry))
           (saved (copy-tree (mapcar #'eighth first))))
      (setf (wal-replication-slot-inactive-since physical) (1+ tick))
      (let* ((second (logical-slot-publication-registry-image registry))
             (changed (copy-tree second)))
        (setf (nth index (eighth (assoc "physical" changed :test #'string=))) :foreign)
        (and
         (equalp (logical-slot-continuation-cold-preparation-semantic-image first)
                 (logical-slot-continuation-cold-preparation-semantic-image second))
         (not (equalp (logical-slot-continuation-cold-preparation-semantic-image first)
                      (logical-slot-continuation-cold-preparation-semantic-image changed)))
         (equal saved (mapcar #'eighth first))
         (handler-case
             (progn (logical-slot-publication-check-registry registry first) nil)
           (error () t))
         (progn
           (setf (wal-replication-slot-inactive-since logical) 1)
           (not (equalp
                 (logical-slot-continuation-cold-preparation-semantic-image second)
                 (logical-slot-continuation-cold-preparation-semantic-image
                  (logical-slot-publication-registry-image registry))))))))))

(cl-spec:defproperty continuation-staged-group-protects-only-certified-children
    ((generation (range integer 0 340282366920938463463374607431768211455))
     (count (range integer 1 5)))
  (:about logical-slot-continuation-build-protection-under-lock)
  (:trials (:normal 80))
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs generation)
    (let* ((binding (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)))
           (snapshot (logical-slot-continuation-active-snapshot binding certificate host rows))
           (registry (make-hash-table :test #'equal))
           (*wal-durable-lsn* (sixth host))
           (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
           (*logical-slot-continuation-protections* (make-hash-table :test #'equal))
           (*logical-slot-continuation-protection-registries* (make-hash-table :test #'eq))
           (*logical-slot-continuation-protection-identities* (make-hash-table :test #'eq)))
      (dolist (row (append rows
                           (loop for index below count
                                 collect (unit-continuation-partition-physical
                                          (format nil "physical~d" index)))))
        (setf (gethash (first row) registry)
              (wal-decode-replication-slot-image (second row) (first row))))
      (sb-thread:with-mutex (*wal-lock*)
        (multiple-value-bind (group control cleanup)
            (logical-slot-continuation-build-protection-under-lock
             registry (logical-slot-continuation-encode snapshot) binding '(1 2 3) nil :timeline :staged)
          (setf (gethash '(1 2 3) *logical-slot-continuation-protections*) group
                (gethash registry *logical-slot-continuation-protection-registries*) group
                (gethash group *logical-slot-continuation-protection-identities*)
                (logical-slot-continuation-protection-state group)
                (gethash group *wal-snapshot-retention-pins*) 96)
          (and (eq :staged (svref control 0))
               (equal '("alpha" "beta") (logical-slot-continuation-protection-names group))
               (every (lambda (operation)
                        (handler-case
                            (progn (logical-slot-continuation-protection-call group operation nil) nil)
                          (error () t)))
                      '(:check :begin :owns-cell :cancel))
               (funcall cleanup)
               (loop for slot being the hash-values of registry
                     always (eq (not (null (wal-replication-slot-child-owner slot)))
                                (not (null (wal-replication-slot-state-uncertain-p slot)))))
               (= 96 (gethash group *wal-snapshot-retention-pins*))))))))

(cl-spec:defproperty continuation-group-rejects-unproved-extra-members
    ((generation (range integer 0 340282366920938463463374607431768211455))
     (mode (range integer 0 2)))
  (:about logical-slot-continuation-build-protection-under-lock)
  (:trials (:normal 80))
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs generation)
    (let* ((binding (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3)))
           (snapshot (logical-slot-continuation-active-snapshot binding certificate host rows))
           (registry (make-hash-table :test #'equal))
           (*wal-durable-lsn* (sixth host))
           (*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
           (extra (if (zerop mode)
                      (let ((slot (logical-slot-state-decode (second (first rows)) "beta")))
                        (setf (wal-replication-slot-name slot) "foreign")
                        slot)
                      (let ((row (unit-continuation-partition-physical
                                  "physical" :system-id (if (= mode 1) 43 42)
                                  :temporary (= mode 2))))
                        (wal-decode-replication-slot-image (second row) (first row))))))
      (dolist (row rows)
        (setf (gethash (first row) registry)
              (wal-decode-replication-slot-image (second row) (first row))))
      (setf (gethash (wal-replication-slot-name extra) registry) extra)
      (sb-thread:with-mutex (*wal-lock*)
        (handler-case
            (progn
              (logical-slot-continuation-build-protection-under-lock
               registry (logical-slot-continuation-encode snapshot) binding '(1 2 3) nil :timeline :staged)
              nil)
          (error () t))))))

(rove:deftest continuation-mixed-partition-generated-contracts
    (dolist (entry '((continuation-partition-preserves-full-inventory-and-row-identities 11601)
                     (continuation-partition-rejects-unproved-ordinary-and-child-grafts 11602)
                     (continuation-partition-keeps-selected-generation-and-choice 11603)
                     (continuation-mixed-reproof-normalizes-only-physical-inactivity 11604)
                     (continuation-staged-group-protects-only-certified-children 11605)
                     (continuation-group-rejects-unproved-extra-members 11606)))
      (let ((result (cl-spec:result-data
                     (cl-spec:run-property (first entry) :seed (second entry) :profile :normal))))
        (let ((*print-length* nil) (*print-level* nil))
          (format t "~&CONTINUATION-PARTITION-CONTRACT ~s~%" result))
        (rove:ok (eq :passed (getf result :status)))
        (rove:ok (= 80 (getf result :trials))))))
