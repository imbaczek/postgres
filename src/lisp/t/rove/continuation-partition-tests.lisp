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

(defun unit-continuation-partition-logical (name generation kind)
  (let ((slot (unit-logical-provenance-slot
               :generation generation :end 180 :timeline 2
               :provenance (member kind '(:known :lost))
               :cause (if (member kind '(:lost :legacy-lost)) :wal-removed :none))))
    (setf (wal-replication-slot-name slot) name)
    (list name (wal-encode-replication-slot slot))))

(cl-spec:defproperty continuation-partition-admits-ordinary-logical-contracts
    ((generation (range integer 0 340282366920938463463374607431768211455))
     (index (range integer 0 3)))
  (:about logical-slot-continuation-partition-rows)
  (:trials (:normal 80))
  (multiple-value-bind (certificate host children) (unit-continuation-active-inputs generation)
    (let* ((extra (unit-continuation-partition-logical
                   "ordinary" (logxor generation 2)
                   (nth index '(:known :legacy :lost :legacy-lost))))
           (rows (append children (list extra)))
           (before (logical-slot-continuation-copy rows)))
      (multiple-value-bind (selected ordinary)
          (logical-slot-continuation-partition-rows rows certificate host)
        (and (every #'eq selected children)
             (= 1 (length ordinary))
             (eq extra (first ordinary))
             (equalp before rows))))))

(cl-spec:defproperty continuation-partition-rejects-logical-identity-and-position-grafts
    ((generation (range integer 0 340282366920938463463374607431768211455))
     (mode (range integer 0 6)))
  (:about logical-slot-continuation-partition-rows)
  (:trials (:normal 80))
  (multiple-value-bind (certificate host children) (unit-continuation-active-inputs generation)
    (let* ((slot (unit-logical-provenance-slot
                  :generation (if (= mode 4) generation (logxor generation 2))
                  :system-id (if (member mode '(0 6)) 43 42)
                  :end (if (= mode 1) 210 180)
                  :timeline (if (= mode 2) 99 2)
                  :cause (if (member mode '(4 6)) :wal-removed :none)
                  :child-owner (when (= mode 3) 999)))
           (name (if (= mode 5) "alpha" "ordinary")))
      (setf (wal-replication-slot-name slot) name)
      (let ((rows (append (if (= mode 5)
                              (remove "alpha" children :key #'first :test #'string=)
                              children)
                          (list (list name (wal-encode-replication-slot slot))))))
        (handler-case
            (progn (logical-slot-continuation-partition-rows rows certificate host) nil)
          (error () t))))))

(cl-spec:defproperty continuation-ordinary-registry-keeps-child-generation-disjoint
    ((generation (range integer 0 340282366920938463463374607431768211455)))
  (:about logical-slot-continuation-check-ordinary-registry)
  (:trials (:normal 80))
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs generation)
    (declare (ignore certificate))
    (let* ((registry (make-hash-table :test #'equal))
           (members (mapcar (lambda (row)
                              (cons (first row)
                                    (wal-decode-replication-slot-image (second row) (first row))))
                            rows))
           (row (unit-continuation-partition-logical "ordinary" (logxor generation 2) :known))
           (history (logical-slot-continuation-history (fifth host) (fourth host))))
      (dolist (entry members) (setf (gethash (car entry) registry) (cdr entry)))
      (setf (gethash "ordinary" registry)
            (wal-decode-replication-slot-image (second row) "ordinary"))
      (and (logical-slot-continuation-check-ordinary-registry registry members 42 history 192)
           (progn
             (setf (gethash "ordinary" registry)
                   (let ((clone (unit-imported-state-with-origin (cdr (assoc "alpha" members :test #'string=))
                                                                 :owner nil)))
                     (setf (wal-replication-slot-name clone) "ordinary")
                     clone))
             (handler-case
                 (progn (logical-slot-continuation-check-ordinary-registry
                         registry members 42 history 192) nil)
               (error () t)))))))

(cl-spec:defproperty continuation-ordinary-candidates-preserve-live-identity
    ((generation (range integer 0 340282366920938463463374607431768211455)))
  (:about logical-slot-continuation-check-ordinary-candidate)
  (:trials (:normal 80))
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs generation)
    (declare (ignore certificate))
    (let* ((registry (make-hash-table :test #'equal))
           (members (mapcar (lambda (row)
                              (cons (first row)
                                    (wal-decode-replication-slot-image (second row) (first row))))
                            rows))
           (row (unit-continuation-partition-logical "ordinary" (logxor generation 2) :known))
           (live (wal-decode-replication-slot-image (second row) "ordinary"))
           (candidate (wal-copy-replication-slot-with-profile live))
           (history (logical-slot-continuation-history (fifth host) (fourth host)))
           (*wal-replication-slots* registry))
      (dolist (entry members) (setf (gethash (car entry) registry) (cdr entry)))
      (setf (gethash "ordinary" registry) live
            (wal-replication-slot-confirmed-flush-lsn candidate) 181
            (wal-replication-slot-restart-lsn candidate) 181)
      (let ((forged (wal-decode-replication-slot-image (wal-encode-replication-slot candidate) "ordinary")))
        (sb-thread:with-mutex (*wal-lock*)
          (and
           (eq live (logical-slot-continuation-check-ordinary-candidate
                     registry members live 42 history 192))
           (eq candidate (logical-slot-continuation-check-ordinary-candidate
                          registry members candidate 42 history 192))
           (handler-case
               (progn (logical-slot-continuation-check-ordinary-candidate
                       registry members forged 42 history 192) nil)
             (error () t))
           (handler-case
               (progn (logical-slot-continuation-check-ordinary-candidate
                       registry members (cdr (first members)) 42 history 192) nil)
             (error () t))
           (progn
             (setf (gethash "ordinary" registry) forged)
             (handler-case
                 (progn (logical-slot-continuation-check-ordinary-candidate
                         registry members candidate 42 history 192) nil)
               (error () t)))))))))

(rove:deftest continuation-ordinary-logical-partition-generated-contracts
    (dolist (entry '((continuation-partition-admits-ordinary-logical-contracts 11701)
                     (continuation-partition-rejects-logical-identity-and-position-grafts 11702)
                     (continuation-ordinary-registry-keeps-child-generation-disjoint 11703)
                     (continuation-ordinary-candidates-preserve-live-identity 11704)))
      (let ((result (cl-spec:result-data
                     (cl-spec:run-property (first entry) :seed (second entry) :profile :normal))))
        (let ((*print-length* nil) (*print-level* nil))
          (format t "~&CONTINUATION-ORDINARY-LOGICAL-CONTRACT ~s~%" result))
        (rove:ok (eq :passed (getf result :status)))
        (rove:ok (= 80 (getf result :trials))))))

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
