(in-package #:pg-physical-tests)

(defun reseed-follow-activation-codec-fixture (selected)
  (multiple-value-bind (record binding) (reseed-follow-activation-config-fixture selected)
    (let* ((bytes (pg::physical-follow-config-image
                   (pg::physical-reseed-follow-activation-config record binding)))
           (image (list (length bytes) (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
           (device (getf (getf (getf record :attempt) :local) :device))
           (entry (list (copy-seq "follow.active") :file #o600 device 200000
                        (first image) (second image))))
      (values record
              (loop for (phase step) in '((:building 0) (:staged 0) (:activating 0)
                                          (:activating 1) (:activating 2) (:active 2))
                    collect (list :phase phase :step step :image (copy-list image)
                                  :entry (unless (eq phase :building)
                                           (pg::physical-adoption-attempt-value-copy entry))))))))

(defun reseed-follow-with-activation (record group)
  (append (pg::physical-reseed-follow-copy record)
          (when group (list :activation (pg::physical-adoption-attempt-value-copy group)))))

(cl-spec:defproperty reseed-follow-activation-copies-preserve-predecessors
    ((selected boolean) (position (range integer 0 5)))
  (:about pg::physical-reseed-follow-activation-copy)
  (:trials (:normal 100))
  (multiple-value-bind (record groups) (reseed-follow-activation-codec-fixture selected)
    (let* ((group (nth position groups))
           (before (pg::physical-adoption-attempt-value-copy (list record group)))
           (copy (funcall (symbol-function 'pg::physical-reseed-follow-activation-copy) group record))
           (journal (reseed-follow-with-activation record copy)))
      (and (equal group copy)
           (equal journal (pg::physical-reseed-follow-decode (pg::physical-reseed-follow-image journal)))
           (progn
             (incf (second (getf copy :image)))
             (when (getf copy :entry) (setf (char (first (getf copy :entry)) 0) #\x))
             (equal before (list record group)))))))

(deftest generated-reseed-follow-activation-codec-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-activation-copies-preserve-predecessors
                                       :seed 128 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-activation-codec-keeps-absent-encoding
    (dolist (selected '(nil t))
      (multiple-value-bind (record groups) (reseed-follow-activation-codec-fixture selected)
        (ok (equalp (pg::physical-reseed-follow-image record)
                    (pg::physical-backup-lease-encode-literal
                     "PGRESEEDFOLLOW1" record pg::+physical-reseed-follow-maximum-bytes+)))
        (dolist (group groups)
          (let ((journal (reseed-follow-with-activation record group)))
            (ok (equal journal (pg::physical-reseed-follow-decode
                                (pg::physical-reseed-follow-image journal))))))
        (ok (handler-case
                (progn (pg::physical-reseed-follow-copy (append record (list :activation nil))) nil)
              (error () t))))))

(deftest reseed-follow-activation-transitions-advance-one-prefix
    (dolist (selected '(nil t))
      (multiple-value-bind (record groups) (reseed-follow-activation-codec-fixture selected)
        (loop for old from -1 to 5
              do (loop for new from -1 to 5
                       for expected = (or (= old new) (= new (1+ old)))
                       for accepted = (handler-case
                                          (progn
                                            (pg::physical-reseed-follow-check-transition
                                             (reseed-follow-with-activation record (when (>= old 0) (nth old groups)))
                                             (reseed-follow-with-activation record (when (>= new 0) (nth new groups))))
                                            t)
                                        (error () nil))
                       do (ok (eq expected accepted)))))))

(deftest reseed-follow-activation-codec-rejects-impossible-phase-shapes
    (multiple-value-bind (record groups) (reseed-follow-activation-codec-fixture nil)
      (let ((entry (getf (second groups) :entry)))
        (dolist (phase '(:building :staged :activating :active))
          (dotimes (step 3)
            (dolist (present '(nil t))
              (let* ((group (pg::physical-adoption-attempt-value-copy (first groups)))
                     (expected (not (null
                                     (case phase
                                       (:building (and (zerop step) (not present)))
                                       (:staged (and (zerop step) present))
                                       (:activating present)
                                       (:active (and (= step 2) present)))))))
                (setf (getf group :phase) phase (getf group :step) step
                      (getf group :entry) (when present entry))
                (ok (eq expected
                        (handler-case
                            (progn
                              (funcall (symbol-function 'pg::physical-reseed-follow-activation-copy) group record)
                              t)
                          (error () nil)))))))))))

(deftest reseed-follow-activation-codec-rejects-visible-aliases
    (multiple-value-bind (record groups) (reseed-follow-activation-codec-fixture t)
      (let* ((local (getf (getf record :attempt) :local))
             (config (getf record :configuration))
             (directories (getf config :directories))
             (inodes (append (list (getf local :inode) (getf local :lock-inode)
                                   (getf (getf (getf record :attempt) :stage) :inode)
                                   (second (getf (getf record :install) :identity))
                                   (getf (getf config :workspace) :inode)
                                   (getf directories :lock-inode) (getf directories :fence-inode))
                             (loop for role in '(:staged :saved :receive)
                                   collect (getf (getf directories role) :inode))
                             (loop for entry in (getf config :entries)
                                   collect (fifth (first (second entry)))))))
        (dolist (inode inodes)
          (let ((group (pg::physical-adoption-attempt-value-copy (second groups))))
            (setf (fifth (getf group :entry)) inode)
            (ok (handler-case
                    (progn (funcall (symbol-function 'pg::physical-reseed-follow-activation-copy) group record) nil)
                  (error () t))))))))

(deftest reseed-follow-activation-transitions-freeze-image-and-entry
    (multiple-value-bind (record groups) (reseed-follow-activation-codec-fixture nil)
      (dolist (kind '(:image :entry))
        (let* ((old (reseed-follow-with-activation record (second groups)))
               (new (pg::physical-adoption-attempt-value-copy old))
               (group (getf new :activation)))
          (ecase kind
            (:image (incf (second (getf group :image))) (incf (seventh (getf group :entry))))
            (:entry (incf (fifth (getf group :entry)))))
          (ok (pg::physical-reseed-follow-copy new))
          (ok (handler-case
                  (progn (pg::physical-reseed-follow-check-transition old new) nil)
                (error () t)))))))

(deftest reseed-follow-activation-codec-rejects-malformed-fields
    (multiple-value-bind (record groups) (reseed-follow-activation-codec-fixture nil)
      (let ((prototype (second groups)))
        (dolist (image (list nil (list 1) (list 1 2 3) (list 0 17)
                             (list (1+ pg::+physical-follow-config-maximum-bytes+) 17)
                             (list 1 -1) (list 1 (ash 1 64)) (list "bad" 17)))
          (let ((group (pg::physical-adoption-attempt-value-copy prototype)))
            (setf (getf group :image) image)
            (ok (handler-case
                    (progn (pg::physical-reseed-follow-activation-copy group record) nil)
                  (error () t)))))
        (let ((entry (getf prototype :entry)))
          (dolist (case (list (list 0 "wrong.active") (list 1 :directory) (list 2 #o644)
                              (list 3 -1) (list 3 (1+ (fourth entry)))
                              (list 4 0) (list 4 (ash 1 64))
                              (list 5 (1+ (sixth entry))) (list 6 (logxor 1 (seventh entry)))))
            (let ((group (pg::physical-adoption-attempt-value-copy prototype)))
              (setf (nth (first case) (getf group :entry)) (second case))
              (ok (handler-case
                      (progn (pg::physical-reseed-follow-activation-copy group record) nil)
                    (error () t)))))
          (let ((group (pg::physical-adoption-attempt-value-copy prototype)))
            (setf (getf group :entry) (append (getf group :entry) (list 0)))
            (ok (handler-case
                    (progn (pg::physical-reseed-follow-activation-copy group record) nil)
                  (error () t)))))
        (dolist (kind '(:missing :duplicate :extra))
          (let ((group (pg::physical-adoption-attempt-value-copy prototype)))
            (ecase kind
              (:missing (remf group :image))
              (:duplicate (setf group (append group (list :step 0))))
              (:extra (setf group (append group (list :unknown 0)))))
            (ok (handler-case
                    (progn (pg::physical-reseed-follow-activation-copy group record) nil)
                  (error () t))))))))

(deftest reseed-follow-activation-rejects-unsettled-predecessors-and-bundled-handoff
    (multiple-value-bind (record groups) (reseed-follow-activation-codec-fixture nil)
      (dolist (phase '(:handoff-pending :source-aborted))
        (let* ((other (pg::physical-adoption-attempt-value-copy record))
               (settlement (getf other :settlement)))
          (setf (getf settlement :phase) phase)
          (if (eq phase :handoff-pending)
              (setf (getf settlement :receipt) nil)
              (setf (getf (getf settlement :receipt) :outcome) :aborted
                    (getf (getf settlement :receipt) :successor) nil))
          (ok (pg::physical-reseed-follow-copy other))
          (ok (handler-case
                  (progn (pg::physical-reseed-follow-activation-copy (first groups) other) nil)
                (error () t)))
          (when (eq phase :handoff-pending)
            (let ((candidate (reseed-follow-with-activation record (first groups))))
              (ok (pg::physical-reseed-follow-copy candidate))
              (ok (handler-case
                      (progn (pg::physical-reseed-follow-check-transition other candidate) nil)
                    (error () t)))))))))

(deftest reseed-follow-activation-step-freezes-other-predecessors
    (multiple-value-bind (record groups) (reseed-follow-activation-codec-fixture nil)
      (let* ((old (reseed-follow-with-activation record (second groups)))
             (new (reseed-follow-with-activation record (third groups))))
        (incf (getf (getf new :configuration) :time))
        (ok (pg::physical-reseed-follow-copy new))
        (ok (handler-case
                (progn (pg::physical-reseed-follow-check-transition old new) nil)
              (error () t)))
        (ok (handler-case
                (progn (pg::physical-reseed-follow-activation-check-transition old new) nil)
              (error () t))))))
