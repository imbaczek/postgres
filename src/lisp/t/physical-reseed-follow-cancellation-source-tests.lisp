(in-package #:pg-physical-tests)

(defparameter *reseed-cancellation-source-observation-cases*
  '((nil nil nil :unknown)
    (:requested nil nil :unbound)
    (:capturing nil nil :unbound)
    (:ready nil t :open)
    (:interrupted nil t :open)
    (:interrupted nil nil :unbound)
    (:closing :aborted t :aborting)
    (:closed :aborted t :aborting)
    (:closing :aborted nil :unbound)
    (:closed :aborted nil :unbound)
    (:closing :handed-off t :handed-off)
    (:closed :handed-off t :handed-off)))

(defun reseed-cancellation-source-parent (record phase outcome base-p)
  (when phase
    (let* ((generation (car (last (getf record :generations))))
           (anchor (when generation (pg::physical-reseed-follow-generation-anchor generation))))
      (pg::physical-backup-lease-copy
       (list :version 1 :request (getf (getf record :attempt) :request)
             :phase phase :outcome outcome
             :base (when base-p (pg::physical-reseed-follow-settlement-base record))
             :successor (when (eq outcome :handed-off)
                          (list :name (if generation (getf generation :name) "foreign_cleanup_pin")
                                :anchor-digest (if anchor (pg::physical-follow-anchor-digest anchor) 0))))))))

(defun reseed-cancellation-source-observation-case-p (record case)
  (destructuring-bind (phase outcome base-p expected) case
    (let* ((source (reseed-cancellation-source-parent record phase outcome base-p))
           (original-record (pg::physical-bootstrap-manifest-text record))
           (original-source (pg::physical-bootstrap-manifest-text source))
           (generation (car (last (getf record :generations))))
           (receipt (getf (getf record :settlement) :receipt))
           (conflict (and source (getf source :base) receipt (not (equal source receipt))))
           (missing (and (eq expected :handed-off) (null generation)))
           (expected (if (and (eq expected :handed-off) generation
                              (null (pg::physical-reseed-follow-generation-anchor generation)))
                         :unproved expected)))
      (handler-case
          (multiple-value-bind (copy classification)
              (pg::physical-reseed-follow-cancellation-source-observation-copy record source)
            (and (not conflict) (not missing) (eq expected classification) (equal source copy)
                 (or (null source) (not (eq source copy)))
                 (equal original-record (pg::physical-bootstrap-manifest-text record))
                 (equal original-source (pg::physical-bootstrap-manifest-text source))))
        (error (condition)
          (and (or conflict missing)
               (equal (if conflict
                          "source cleanup lease differs from its frozen closed receipt"
                          "source cleanup selected an unrecorded allocation")
                      (princ-to-string condition))))))))

(cl-spec:defproperty reseed-cancellation-source-observations-preserve-unresolved-debt
    ((shape (range integer 0 19)) (selected boolean) (case-index (range integer 0 11)))
  (:about pg::physical-reseed-follow-cancellation-source-observation-copy)
  (:trials (:normal 100))
  (reseed-cancellation-source-observation-case-p
   (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture shape selected))
   (nth case-index *reseed-cancellation-source-observation-cases*)))

(deftest generated-reseed-cancellation-source-observation-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'reseed-cancellation-source-observations-preserve-unresolved-debt
                                         :seed 187 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest reseed-cancellation-source-observation-checks-every-history-and-parent-phase
    (dotimes (shape 20)
      (dolist (selected '(nil t))
        (dolist (case *reseed-cancellation-source-observation-cases*)
          (ok (reseed-cancellation-source-observation-case-p
               (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture shape selected))
               case))))))

(deftest reseed-cancellation-source-observation-refuses-changed-owner-base-and-successor
    (let ((record (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture 2 nil))))
      (dolist (fault '(:request :base :selected-name :selected-digest))
        (let ((source (reseed-cancellation-source-parent
                       record :closed (if (member fault '(:selected-name :selected-digest))
                                          :handed-off :aborted) t)))
          (ecase fault
            (:request (incf (getf (getf source :request) :timeline)))
            (:base (setf (getf (getf source :base) :manifest-digest)
                         (logxor 1 (getf (getf source :base) :manifest-digest))))
            (:selected-name (setf (getf (getf source :successor) :name) "foreign_cleanup_pin"))
            (:selected-digest
             (setf (getf (getf source :successor) :anchor-digest)
                   (logxor 1 (getf (getf source :successor) :anchor-digest)))))
          (ok (equal (ecase fault
                       (:request "source cleanup lease differs from the frozen request")
                       (:base "source cleanup lease differs from the installed base")
                       (:selected-name "source cleanup selected an unrecorded allocation")
                       (:selected-digest "source cleanup successor differs from its saved proof"))
                     (handler-case
                         (pg::physical-reseed-follow-cancellation-source-observation-copy record source)
                       (error (condition) (princ-to-string condition)))))))))

(deftest reseed-cancellation-source-observation-returns-detached-parent-data
    (let* ((record (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture 2 nil)))
           (source (reseed-cancellation-source-parent record :closed :handed-off t))
           (before (pg::physical-bootstrap-manifest-text source)))
      (multiple-value-bind (copy classification)
          (pg::physical-reseed-follow-cancellation-source-observation-copy record source)
        (ok (eq :handed-off classification))
        (setf (char (getf (getf copy :request) :token) 0)
              (if (char= #\a (char (getf (getf copy :request) :token) 0)) #\b #\a)
              (getf (getf copy :base) :manifest-digest) 0
              (getf (getf copy :successor) :anchor-digest) 0)
        (ok (equal before (pg::physical-bootstrap-manifest-text source))))))

(deftest reseed-cancellation-source-observation-preserves-frozen-terminal-receipts
    (dolist (shape '(8 9 10 11 12 13 14 15 16 17 18 19))
      (dolist (selected '(nil t))
        (let* ((record (pg::physical-reseed-follow-copy
                        (reseed-cancellation-codec-fixture shape selected)))
               (receipt (getf (getf record :settlement) :receipt)))
          (ok (equal receipt
                     (pg::physical-reseed-follow-cancellation-source-observation-copy record receipt)))
          (dolist (fault '(:reopened :closing :other-outcome))
            (let* ((outcome (getf receipt :outcome))
                   (observed-outcome
                     (case fault
                       (:reopened nil)
                       (:other-outcome (if (eq outcome :aborted) :handed-off :aborted))
                       (otherwise outcome)))
                   (phase (case fault (:reopened :ready) (:closing :closing) (otherwise :closed)))
                   (source (reseed-cancellation-source-parent record phase observed-outcome t)))
              (ok (equal "source cleanup lease differs from its frozen closed receipt"
                         (handler-case
                             (pg::physical-reseed-follow-cancellation-source-observation-copy record source)
                           (error (condition) (princ-to-string condition)))))))))))

(cl-spec:defproperty reseed-cancellation-cleanup-phases-freeze-history-and-source-outcome
    ((shape (range integer 0 19)) (selected boolean))
  (:about pg::physical-reseed-follow-cancellation-source-closed-record
          pg::physical-reseed-follow-cancellation-cleaned-record)
  (:trials (:normal 100))
  (let* ((record (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture shape selected)))
         (original (pg::physical-bootstrap-manifest-text record))
         (pending (pg::physical-reseed-follow-cancellation-begin-record record))
         (receipt (or (getf (getf record :settlement) :receipt)
                      (reseed-cancellation-source-parent record :closed :aborted t)))
         (closed (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
         (ack (if (eq :aborted (getf receipt :outcome)) :parent-abort :selected-release))
         (cleaned (pg::physical-reseed-follow-cancellation-cleaned-record closed receipt ack))
         (without (copy-list cleaned)))
    (remf without :cancellation)
    (and (equal without record)
         (equal original (pg::physical-bootstrap-manifest-text record))
         (equal receipt (getf (getf closed :cancellation) :receipt))
         (equal receipt (getf (getf cleaned :cancellation) :receipt))
         (eq :source-closed (getf (getf closed :cancellation) :phase))
         (eq :cleaned (getf (getf cleaned :cancellation) :phase))
         (pg::physical-reseed-follow-check-transition pending closed)
         (pg::physical-reseed-follow-check-transition closed cleaned)
         (equal cleaned (pg::physical-reseed-follow-cancellation-cleaned-record cleaned receipt ack))
         (equal cleaned (pg::physical-reseed-follow-decode (pg::physical-reseed-follow-image cleaned))))))

(deftest generated-reseed-cancellation-cleanup-phase-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'reseed-cancellation-cleanup-phases-freeze-history-and-source-outcome
                                         :seed 188 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest reseed-cancellation-cleanup-phases-require-pending-and-bound-closed-receipts
    (let* ((record (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture 2 nil)))
           (pending (pg::physical-reseed-follow-cancellation-begin-record record))
           (aborted (reseed-cancellation-source-parent record :closed :aborted t))
           (handed-off (reseed-cancellation-source-parent record :closed :handed-off t))
           (closed (pg::physical-reseed-follow-cancellation-source-closed-record pending handed-off)))
      (ok (signals (pg::physical-reseed-follow-cancellation-source-closed-record record aborted) 'error))
      (dolist (receipt (list nil
                             (reseed-cancellation-source-parent record :closing :aborted t)
                             (reseed-cancellation-source-parent record :closed :aborted nil)))
        (ok (signals (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt) 'error)))
      (ok (signals (pg::physical-reseed-follow-cancellation-cleaned-record pending handed-off :selected-release) 'error))
      (ok (signals (pg::physical-reseed-follow-cancellation-cleaned-record closed aborted :parent-abort) 'error))
      (ok (signals (pg::physical-reseed-follow-cancellation-cleaned-record closed handed-off :parent-abort) 'error))
      (ok (signals (pg::physical-reseed-follow-cancellation-source-closed-record closed aborted) 'error))
      (let ((cleaned (pg::physical-reseed-follow-cancellation-cleaned-record pending aborted :parent-abort)))
        (ok (signals (pg::physical-reseed-follow-check-transition cleaned pending) 'error))
        (ok (signals (pg::physical-reseed-follow-check-forward-record cleaned) 'error))
        (ok (signals (pg::physical-reseed-follow-check-transition
                      cleaned (pg::physical-reseed-follow-cancellation-source-closed-record pending aborted))
                     'error)))))

(deftest reseed-cancellation-cleanup-phases-cover-all-historical-records
    (dotimes (shape 20)
      (dolist (selected '(nil t))
        (let* ((record (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture shape selected)))
               (pending (pg::physical-reseed-follow-cancellation-begin-record record))
               (receipt (or (getf (getf record :settlement) :receipt)
                            (reseed-cancellation-source-parent record :closed :aborted t)))
               (closed (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
               (ack (if (eq :aborted (getf receipt :outcome)) :parent-abort :selected-release))
               (cleaned (pg::physical-reseed-follow-cancellation-cleaned-record closed receipt ack)))
          (ok (equal closed (pg::physical-reseed-follow-cancellation-source-closed-record closed receipt)))
          (ok (equal cleaned (pg::physical-reseed-follow-decode (pg::physical-reseed-follow-image cleaned))))
          (ok (signals (pg::physical-reseed-follow-check-forward-record closed) 'error))
          (ok (signals (pg::physical-reseed-follow-check-transition closed pending) 'error))
          (let ((rebound (copy-tree cleaned)))
            (incf (getf (getf rebound :install) :digest))
            (ok (signals (pg::physical-reseed-follow-check-transition closed rebound) 'error)))))))

(deftest reseed-cancellation-cleanup-codec-refuses-malformed-groups
    (let* ((record (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture 2 nil)))
           (receipt (reseed-cancellation-source-parent record :closed :aborted t)))
      (dolist (group (list (list :phase :source-closed)
                           (list :phase :source-closed :receipt nil)
                           (list :phase :source-closed :receipt receipt :ack :parent-abort)
                           (list :phase :source-closed :receipt receipt :receipt receipt)
                           (list :phase :cleaned :receipt receipt)
                           (list :phase :cleaned :receipt receipt :ack :selected-release)
                           (list :phase :cleaned :receipt receipt :ack :unknown)
                           (list :phase :cleaned :receipt receipt :ack :parent-abort :extra nil)))
        (let ((candidate (append record (list :cancellation group))))
          (ok (signals (pg::physical-reseed-follow-copy candidate) 'error))))))

(deftest reseed-cancellation-cleanup-preserves-unproved-handoff-receipts
    (let* ((record (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture 1 nil)))
           (pending (pg::physical-reseed-follow-cancellation-begin-record record))
           (receipt (reseed-cancellation-source-parent record :closed :handed-off t))
           (closed (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
           (cleaned (pg::physical-reseed-follow-cancellation-cleaned-record closed receipt :selected-release)))
      (ok (eq :unproved (nth-value 1 (pg::physical-reseed-follow-cancellation-source-observation-copy record receipt))))
      (ok (equal closed (pg::physical-reseed-follow-decode (pg::physical-reseed-follow-image closed))))
      (ok (equal cleaned (pg::physical-reseed-follow-decode (pg::physical-reseed-follow-image cleaned))))))

(cl-spec:defproperty reseed-cancellation-cleanup-capacity-covers-receipt-outcomes
    ((shape (range integer 0 19)) (selected boolean))
  (:about pg::physical-reseed-follow-cancellation-check-cleanup-capacity)
  (:trials (:normal 100))
  (let* ((record (pg::physical-reseed-follow-cancellation-begin-record
                  (reseed-cancellation-codec-fixture shape selected)))
         (required (pg::physical-reseed-follow-cancellation-check-cleanup-capacity record))
         (receipt (or (getf (getf record :settlement) :receipt)
                      (reseed-cancellation-source-parent record :closed :aborted t)))
         (closed (pg::physical-reseed-follow-cancellation-source-closed-record record receipt))
         (ack (if (eq :aborted (getf receipt :outcome)) :parent-abort :selected-release))
         (cleaned (pg::physical-reseed-follow-cancellation-cleaned-record closed receipt ack)))
    (and (<= (length (pg::physical-reseed-follow-image closed)) required)
         (<= (length (pg::physical-reseed-follow-image cleaned)) required)
         (= required (pg::physical-reseed-follow-cancellation-check-cleanup-capacity record required))
         (handler-case
             (progn (pg::physical-reseed-follow-cancellation-check-cleanup-capacity record (1- required)) nil)
           (error () t)))))

(deftest generated-reseed-cancellation-cleanup-capacity-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'reseed-cancellation-cleanup-capacity-covers-receipt-outcomes
                                         :seed 189 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest reseed-cancellation-cleanup-capacity-bounds-unproved-digest-width
    (let* ((record (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture 1 nil)))
           (pending (pg::physical-reseed-follow-cancellation-begin-record record))
           (required (pg::physical-reseed-follow-cancellation-check-cleanup-capacity pending))
           (largest 0))
      (dolist (digest '(0 9 10 99 100 #xffffffffffffffff))
        (let ((receipt (reseed-cancellation-source-parent record :closed :handed-off t)))
          (setf (getf (getf receipt :successor) :anchor-digest) digest)
          (let* ((closed (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
                 (cleaned (pg::physical-reseed-follow-cancellation-cleaned-record closed receipt :selected-release))
                 (length (length (pg::physical-reseed-follow-image cleaned))))
            (setf largest (max largest length))
            (ok (<= length required)))))
      (ok (= largest required))
      (ok (signals (pg::physical-reseed-follow-cancellation-check-cleanup-capacity record) 'error))))

(deftest reseed-cancellation-cleanup-capacity-keeps-fixed-receipt-and-terminal-boundaries
    (dotimes (shape 20)
      (dolist (selected '(nil t))
        (let* ((pending (pg::physical-reseed-follow-cancellation-begin-record
                         (reseed-cancellation-codec-fixture shape selected)))
               (receipt (or (getf (getf pending :settlement) :receipt)
                            (reseed-cancellation-source-parent pending :closed :aborted t)))
               (closed (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
               (ack (if (eq :aborted (getf receipt :outcome)) :parent-abort :selected-release))
               (cleaned (pg::physical-reseed-follow-cancellation-cleaned-record closed receipt ack))
               (required (max (length (pg::physical-reseed-follow-image closed))
                              (length (pg::physical-reseed-follow-image cleaned)))))
          (ok (= required (pg::physical-reseed-follow-cancellation-check-cleanup-capacity closed required)))
          (ok (signals (pg::physical-reseed-follow-cancellation-check-cleanup-capacity closed (1- required)) 'error))
          (ok (= (length (pg::physical-reseed-follow-image cleaned))
                 (pg::physical-reseed-follow-cancellation-check-cleanup-capacity cleaned)))))))
