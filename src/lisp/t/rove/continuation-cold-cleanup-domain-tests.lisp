(in-package #:pg)

(declaim (ftype function logical-slot-continuation-cold-certified-domain))

(cl-spec:defproperty continuation-cleanup-certified-domain-keeps-only-installed-images
    ((generation (range integer 0 340282366920938463463374607431768211455)))
  (:about logical-slot-continuation-cold-certified-domain)
  (:trials (:normal 80))
  (multiple-value-bind (certificate host rows)
      (unit-continuation-active-inputs generation)
    (declare (ignore rows))
    (setf (getf certificate :slots)
          (append (reverse (getf certificate :slots)) '(("future" nil nil nil))))
    (multiple-value-bind (domain history)
        (logical-slot-continuation-cold-certified-domain (string->bytes "binding") certificate host)
      (and (equal '("alpha" "beta") (mapcar #'first domain))
           (equalp history (logical-slot-continuation-history (fifth host) (fourth host)))
           (every (lambda (entry)
                    (let ((saved (find (first entry) (getf certificate :slots)
                                       :key #'first :test #'string=)))
                      (equalp (string->bytes (fourth saved))
                              (logical-slot-state-encode (second entry)))))
                  domain)))))

(rove:deftest continuation-cleanup-certified-domain-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-cleanup-certified-domain-keeps-only-installed-images
                                         :seed 11913 :profile :normal))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 80 (getf result :trials)))))

(rove:deftest continuation-cleanup-certified-domain-rejects-ambiguous-certificate
    (dolist (mode '(:duplicate :deferred-duplicate :name :bytes :system :horizon :owner))
      (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
        (declare (ignore rows))
        (let ((entry (first (getf certificate :slots))))
          (ecase mode
            (:duplicate (push (copy-list entry) (getf certificate :slots)))
            (:deferred-duplicate (push (list (first entry) nil nil nil) (getf certificate :slots)))
            (:name (setf (first entry) "alpha.tmp"))
            (:bytes (let ((bytes (copy-seq (fourth entry))))
                      (setf (char bytes 0) #\?
                            (fourth entry) bytes)))
            (:system (setf (first host) 999))
            (:horizon (setf (sixth host) 1))
            (:owner
             (let ((slot (unit-imported-state-baseline :owner nil)))
               (setf (wal-replication-slot-name slot) "alpha"
                     (fourth entry) (bytes->string (logical-slot-state-encode slot))))))
          (rove:ok (handler-case
                       (progn (logical-slot-continuation-cold-certified-domain
                               (string->bytes "binding") certificate host) nil)
                     (error () t))
                   (format nil "reject ~s" mode))))))

(rove:deftest continuation-cleanup-certified-domain-preserves-empty-descendant-protection
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (declare (ignore rows))
      (setf (fourth host) (timeline-encode-history 3 2 224 "later" (fourth host))
            (fifth host) 3 (sixth host) 256)
      (multiple-value-bind (domain history)
          (logical-slot-continuation-cold-certified-domain (string->bytes "binding") certificate host)
        (rove:ok (equal '("alpha" "beta") (mapcar #'first domain)))
        (rove:ok (= 3 (timeline-history-entry-timeline (first history))))
        (rove:ok (logical-slot-continuation-check-empty-snapshot
                  (logical-slot-continuation-empty-snapshot (string->bytes "binding") certificate host)
                  (string->bytes "binding") certificate host)))
      (rove:ok (handler-case
                   (progn (logical-slot-continuation-origin-inputs
                           (string->bytes "binding") certificate host) nil)
                 (error () t)))))

(rove:deftest continuation-cleanup-certified-domain-detaches-certified-state
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (declare (ignore rows))
      (let* ((saved (copy-seq (fourth (first (getf certificate :slots)))))
             (domain (logical-slot-continuation-cold-certified-domain
                      (string->bytes "binding") certificate host)))
        (setf (wal-replication-slot-restart-lsn (second (first domain))) 64
              (char (first (first domain)) 0) #\z)
        (rove:ok (equalp saved (fourth (first (getf certificate :slots)))))
        (rove:ok (string= "alpha" (first (first (getf certificate :slots)))))
        (rove:ok (equal '("alpha" "beta")
                        (mapcar #'first (logical-slot-continuation-cold-certified-domain
                                         (string->bytes "binding") certificate host)))))))

(declaim (ftype function logical-slot-continuation-cold-check-child-selection))

(cl-spec:defproperty continuation-cleanup-child-selection-keeps-bootstrap-and-stable
    ((generation (range integer 0 340282366920938463463374607431768211455)))
  (:about logical-slot-continuation-cold-check-child-selection)
  (:trials (:normal 80))
  (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs generation)
    (let* ((binding (string->bytes "binding"))
           (expected (logical-slot-continuation-active-child-snapshot binding certificate host rows)))
      (multiple-value-bind (bootstrap choice)
          (logical-slot-continuation-cold-check-child-selection
           :absent nil rows binding certificate host)
        (multiple-value-bind (stable selected)
            (logical-slot-continuation-cold-check-child-selection
             :stable expected rows binding certificate host)
          (and (eq :bootstrap choice) (eq :stable selected)
               (equalp expected bootstrap) (equalp expected stable)))))))

(rove:deftest continuation-cleanup-child-selection-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'continuation-cleanup-child-selection-keeps-bootstrap-and-stable
                                         :seed 11914 :profile :normal))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 80 (getf result :trials)))))

(rove:deftest continuation-cleanup-child-selection-preserves-empty-descendant
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (declare (ignore rows))
      (let* ((binding (string->bytes "binding"))
             (selected (logical-slot-continuation-empty-snapshot binding certificate host)))
        (setf (fourth host) (timeline-encode-history 3 2 224 "later" (fourth host))
              (fifth host) 3 (sixth host) 256)
        (multiple-value-bind (snapshot choice)
            (logical-slot-continuation-cold-check-child-selection
             :stable selected nil binding certificate host)
          (rove:ok (eq :stable choice))
          (rove:ok (equalp selected snapshot)))
        (multiple-value-bind (snapshot choice)
            (logical-slot-continuation-cold-check-child-selection
             :absent nil nil binding certificate host)
          (rove:ok (eq :bootstrap choice))
          (rove:ok (logical-slot-continuation-check-empty-snapshot
                    snapshot binding certificate host))))))

(rove:deftest continuation-cleanup-child-selection-refuses-uncertified-and-mixed-rows
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (let* ((binding (string->bytes "binding"))
             (selected (logical-slot-continuation-active-child-snapshot binding certificate host rows)))
        (setf (fourth (first (getf certificate :slots))) nil)
        (dolist (kind '(:absent :stable))
          (rove:ok (handler-case
                       (progn (logical-slot-continuation-cold-check-child-selection
                               kind (when (eq kind :stable) selected) rows binding certificate host) nil)
                     (error () t)))))
      (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
        (let* ((binding (string->bytes "binding"))
               (selected (logical-slot-continuation-active-child-snapshot binding certificate host rows)))
          (dolist (arguments
                   (list (list :absent selected rows binding certificate host)
                         (list :stable selected nil binding certificate host)
                         (list :stable selected rows (string->bytes "other") certificate host)
                         (list :absent nil (append rows '(("ordinary" #(0)))) binding certificate host)))
            (rove:ok (handler-case
                         (progn (apply #'logical-slot-continuation-cold-check-child-selection arguments) nil)
                       (error () t))))))))

(rove:deftest continuation-cleanup-child-selection-keeps-prepared-old-and-new
    (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
      (declare (ignore rows))
      (let* ((record (unit-continuation-prepared))
             (binding (getf (getf record :old) :binding)))
        (dolist (key '(:old :new))
          (multiple-value-bind (snapshot choice)
              (logical-slot-continuation-cold-check-child-selection
               :prepared record (getf (getf record key) :slots) binding certificate host)
            (rove:ok (eq key choice))
            (rove:ok (equalp snapshot (getf record key)))))
        (let* ((changed (logical-slot-continuation-copy (getf (getf record :old) :slots)))
               (slot (logical-slot-state-decode (second (first changed)) "alpha")))
          (setf (wal-replication-slot-restart-lsn slot) 48
                (second (first changed)) (logical-slot-state-encode slot))
          (rove:ok (handler-case
                       (progn (logical-slot-continuation-cold-check-child-selection
                               :prepared record changed binding certificate host) nil)
                     (error () t))))
        (setf (fourth (first (getf certificate :slots))) nil)
        (rove:ok (handler-case
                     (progn (logical-slot-continuation-cold-check-child-selection
                             :prepared record nil binding certificate host) nil)
                   (error () t))))))

(rove:deftest continuation-cleanup-certified-domain-rejects-unrelated-origin-history
    (dolist (mode '(:branch :empty-branch :fork))
      (multiple-value-bind (certificate host rows) (unit-continuation-active-inputs)
        (declare (ignore rows))
        (if (eq mode :fork)
            (setf (fourth host) (timeline-encode-history 2 1 80 "wrong fork" nil))
            (setf (fourth host) (timeline-encode-history 3 1 224 "wrong branch" nil)
                  (fifth host) 3 (sixth host) 256))
        (when (eq mode :empty-branch) (setf (getf certificate :slots) nil))
        (rove:ok (handler-case
                     (progn (logical-slot-continuation-cold-certified-domain
                             (string->bytes "binding") certificate host) nil)
                   (error () t))
                 (format nil "reject origin ~s" mode)))))
