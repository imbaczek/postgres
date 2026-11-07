(in-package #:pg-physical-tests)

(defun reseed-cancellation-codec-fixture (shape selected)
  (case shape
    (0 (let ((record (reseed-follow-codec-fixture :same-timeline :preparing)))
         (setf (getf record :generations) nil)
         record))
    (1 (reseed-follow-codec-fixture :same-timeline :preparing))
    (2 (reseed-follow-codec-fixture :same-timeline :proved))
    ((3 4 5 6)
     (multiple-value-bind (group core)
         (reseed-follow-configuration-codec-fixture
          (nth (- shape 3) '(:building :staged :installing :inert)) selected)
       (append core (list :configuration group))))
    ((7 8 9)
     (multiple-value-bind (record pending receipt) (reseed-follow-settlement-fixture selected)
       (declare (ignore receipt))
       (let ((record (append record (list :settlement pending))))
         (if (= shape 7) record
             (pg::physical-reseed-follow-completion-terminal
              record (if (= shape 8) :handed-off :source-aborted))))))
    ((10 11 12 13 14 15)
     (multiple-value-bind (record groups) (reseed-follow-activation-codec-fixture selected)
       (reseed-follow-with-activation record (nth (- shape 10) groups))))
    ((16 17 18 19)
     (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture selected)
       (reseed-follow-with-retirement record (nth (- shape 16) groups))))
    (otherwise (error "unknown cancellation fixture shape"))))

(cl-spec:defproperty reseed-cancellation-pending-preserves-all-installed-history
    ((shape (range integer 0 19)) (selected boolean))
  (:about pg::physical-reseed-follow-cancellation-begin-record
          pg::physical-reseed-follow-copy pg::physical-reseed-follow-check-transition)
  (:trials (:normal 100))
  (let* ((record (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture shape selected)))
         (original (pg::physical-bootstrap-manifest-text record))
         (candidate (pg::physical-reseed-follow-cancellation-begin-record record))
         (retry (pg::physical-reseed-follow-cancellation-begin-record candidate))
         (without (copy-list candidate)))
    (remf without :cancellation)
    (and (equal record without)
         (equal original (pg::physical-bootstrap-manifest-text record))
         (equal '(:phase :pending) (getf candidate :cancellation))
         (equal candidate retry)
         (not (eq candidate retry))
         (not (eq (getf candidate :cancellation) (getf retry :cancellation)))
         (pg::physical-reseed-follow-check-transition record candidate)
         (pg::physical-reseed-follow-check-transition candidate retry)
         (equal candidate
                (pg::physical-reseed-follow-decode
                 (pg::physical-reseed-follow-image candidate))))))

(deftest generated-reseed-cancellation-pending-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'reseed-cancellation-pending-preserves-all-installed-history
                                         :seed 184 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest reseed-cancellation-pending-freezes-every-historical-shape
    (dotimes (shape 20)
      (dolist (selected '(nil t))
        (let* ((record (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture shape selected)))
               (pending (pg::physical-reseed-follow-cancellation-begin-record record))
               (removed (copy-list pending))
               (rebound (copy-tree pending)))
          (remf removed :cancellation)
          (incf (getf (getf rebound :install) :digest))
          (ok (pg::physical-reseed-follow-check-transition record pending))
          (ok (signals (pg::physical-reseed-follow-check-transition pending removed) 'error))
          (ok (signals (pg::physical-reseed-follow-check-transition pending rebound) 'error))))))

(deftest reseed-cancellation-pending-rejects-unearned-outcomes-and-unknown-fields
    (dolist (group '(nil (:phase :canceled) (:phase :complete) (:phase :pending :receipt nil)
                     (:phase :pending :phase :pending) (:phase :pending :released t)))
      (let ((record (reseed-follow-codec-fixture :same-timeline :proved)))
        (setf (getf record :cancellation) group)
        (ok (signals (pg::physical-reseed-follow-copy record) 'error)))))

(cl-spec:defproperty reseed-cancellation-pending-consumes-only-existing-forward-reserve
    ((shape (range integer 0 19)) (selected boolean))
  (:about pg::physical-reseed-follow-capacity-reserve
          pg::physical-reseed-follow-check-image-capacity)
  (:trials (:normal 100))
  (let* ((record (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture shape selected)))
         (pending (pg::physical-reseed-follow-cancellation-begin-record record))
         (old (pg::physical-backup-lease-encode-literal
               "PGRESEEDFOLLOW1" record pg::+physical-reseed-follow-maximum-bytes+))
         (new (pg::physical-backup-lease-encode-literal
               "PGRESEEDFOLLOW1" pending pg::+physical-reseed-follow-maximum-bytes+)))
    (and (= 32 (- (length new) (length old)))
         (zerop (pg::physical-reseed-follow-capacity-reserve pending))
         (pg::physical-reseed-follow-check-image-capacity pending (length new) (length new))
         (equalp new (pg::physical-reseed-follow-image pending))
         (equalp old (pg::physical-reseed-follow-image record)))))

(deftest generated-reseed-cancellation-capacity-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'reseed-cancellation-pending-consumes-only-existing-forward-reserve
                                         :seed 185 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest reseed-cancellation-capacity-keeps-exact-pending-and-legacy-boundaries
    (dotimes (shape 20)
      (dolist (selected '(nil t))
        (let* ((record (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture shape selected)))
               (pending (pg::physical-reseed-follow-cancellation-begin-record record))
               (old-length (length (pg::physical-reseed-follow-image record)))
               (new-length (length (pg::physical-reseed-follow-image pending)))
               (legacy-needed
                 (+ (pg::physical-reseed-follow-capacity-reserve record)
                    (if (eq :handoff-pending (getf (getf record :settlement) :phase))
                        (max old-length (pg::physical-reseed-follow-settlement-terminal-size record))
                        old-length))))
          (ok (pg::physical-reseed-follow-check-image-capacity record old-length legacy-needed))
          (ok (pg::physical-reseed-follow-check-image-capacity pending new-length new-length))
          (ok (signals (pg::physical-reseed-follow-check-image-capacity
                        pending new-length (1- new-length)) 'error))))))

(deftest reseed-cancellation-pending-cannot-use-generic-intent-publication
    (let ((record (reseed-follow-codec-fixture :same-timeline :preparing)))
      (setf (getf record :cancellation) (list :phase :pending))
      (ok (signals (pg::physical-reseed-follow-intent-copy record) 'error))))

(deftest reseed-cancellation-pending-refuses-terminal-main-startup
    (dolist (selected '(nil t))
      (let* ((record (pg::physical-reseed-follow-cancellation-begin-record
                      (reseed-cancellation-codec-fixture 19 selected)))
             (observation (list :branch :retired :evidence
                                (list :prefix 2 :snapshot (list :record record)))))
        (ok (signals (pg::physical-reseed-follow-check-terminal-startup observation) 'error)))))

(deftest reseed-cancellation-pending-denies-retirement-owner-admission
    (dolist (selected '(nil t))
      (let* ((record (pg::physical-reseed-follow-cancellation-begin-record
                      (reseed-cancellation-codec-fixture 19 selected)))
             (evidence (list :prefix 2 :snapshot (list :record record)))
             (observation (list :branch :retired :evidence evidence))
             (owner (pg::make-physical-reseed-follow-retirement-startup-owner
                     :active-p t :thread sb-thread:*current-thread* :evidence evidence))
             (callbacks 0))
        (dolist (operation
                 (list (lambda ()
                         (pg::call-with-physical-reseed-follow-retirement-startup-owner
                          observation (lambda (value) (declare (ignore value)) (incf callbacks))))
                       (lambda () (pg::physical-reseed-follow-retirement-check-startup-idle owner))))
          (ok (handler-case (progn (funcall operation) nil)
                (error (condition)
                  (search "target cancellation fences retirement startup" (princ-to-string condition))))))
        (ok (zerop callbacks)))))
