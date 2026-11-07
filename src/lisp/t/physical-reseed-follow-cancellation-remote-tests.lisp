(in-package #:pg-physical-tests)

(cl-spec:defproperty reseed-cancellation-action-keeps-unproved-generation-debt
    ((shape (range integer 0 2)) (case-index (range integer 0 11)))
  (:about pg::physical-reseed-follow-cancellation-source-action)
  (:trials (:normal 100))
  (let* ((record (pg::physical-reseed-follow-cancellation-begin-record
                  (reseed-cancellation-codec-fixture shape nil)))
         (case (nth case-index *reseed-cancellation-source-observation-cases*)))
    (destructuring-bind (phase outcome base-p classification) case
      (let* ((source (reseed-cancellation-source-parent record phase outcome base-p))
             (expected
               (case classification
                 ((:unknown :unbound) :unresolved)
                 (:open :abort)
                 (:aborting
                  (if (and (eq phase :closed) (plusp shape)) :defer :abort))
                 (:handed-off (if (zerop shape) :invalid :cleanup-selected))))
             (before (pg::physical-bootstrap-manifest-text record)))
        (if (eq expected :invalid)
            (handler-case
                (progn (pg::physical-reseed-follow-cancellation-source-action record source) nil)
              (error () t))
            (multiple-value-bind (action observation)
                (pg::physical-reseed-follow-cancellation-source-action record source)
              (and (eq expected action) (equal source observation)
                   (equal before (pg::physical-bootstrap-manifest-text record)))))))))

(deftest generated-reseed-cancellation-action-debt-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'reseed-cancellation-action-keeps-unproved-generation-debt
                                         :seed 199 :profile :normal))))
      (ok (eq :passed (getf result :status)))
      (ok (= 100 (getf result :trials)))))

(deftest reseed-cancellation-action-resumes-only-saved-closed-selection
    (dotimes (shape 3)
      (let* ((pending (pg::physical-reseed-follow-cancellation-begin-record
                       (reseed-cancellation-codec-fixture shape nil)))
             (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
             (closed (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt)))
        (multiple-value-bind (action actual)
            (pg::physical-reseed-follow-cancellation-source-action closed nil)
          (ok (eq (if (zerop shape) :abort :defer) action))
          (ok (equal receipt actual)))
        (when (plusp shape)
          (let* ((handoff (reseed-cancellation-source-parent pending :closed :handed-off t))
                 (selected (pg::physical-reseed-follow-cancellation-source-closed-record pending handoff)))
            (multiple-value-bind (action actual)
                (pg::physical-reseed-follow-cancellation-source-action selected nil)
              (ok (eq :release action))
              (ok (equal handoff actual))))))))

(deftest reseed-cancellation-network-snapshot-keeps-actual-abort-and-detached-state
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (let ((pending (pg::physical-reseed-follow-begin-cancellation owner)))
         (ok (signals (pg::physical-reseed-follow-cancellation-network-snapshot owner) 'error))
         (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
         (unwind-protect
              (let ((snapshot (pg::physical-reseed-follow-cancellation-network-snapshot owner)))
                (ok (equal pending (getf (getf snapshot :follow) :record)))
                (ok (equal snapshot (pg::physical-reseed-follow-cancellation-network-snapshot owner snapshot)))
                (reseed-follow-prepare-test-abort root record)
                (ok (signals (pg::physical-reseed-follow-cancellation-network-snapshot owner snapshot) 'error))
                (let ((after (pg::physical-reseed-follow-cancellation-network-snapshot owner)))
                  (ok (eq :aborting (getf (getf (getf after :attempt) :record) :phase)))
                  (setf (getf (getf (getf after :follow) :record) :generations) nil)
                  (ok (signals (pg::physical-reseed-follow-cancellation-network-snapshot owner after) 'error))
                  (ok (equal pending
                             (getf (getf (pg::physical-reseed-follow-cancellation-network-snapshot owner) :follow)
                                   :record)))))
           (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))))))

(deftest reseed-cancellation-network-snapshot-refuses-unrecovered-temporary
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (path (merge-pathnames "physical-reseed.follow.tmp" root)))
         (reseed-follow-owner-test-write path (subseq (pg::physical-reseed-follow-image pending) 0 17))
         (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
         (unwind-protect
              (progn
                (ok (signals (pg::physical-reseed-follow-cancellation-network-snapshot owner) 'error))
                (ok (= 17 (length (lineage-test-bytes path))))
                (ok (equalp (pg::physical-reseed-follow-image pending)
                            (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
           (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))))))

(deftest reseed-cancellation-network-snapshot-detaches-attempt-and-install
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore root record))
       (pg::physical-reseed-follow-begin-cancellation owner)
       (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
       (unwind-protect
            (let ((first (pg::physical-reseed-follow-cancellation-network-snapshot owner))
                  (second (pg::physical-reseed-follow-cancellation-network-snapshot owner)))
              (ok (equal first second))
              (ok (not (eq (getf (getf first :attempt) :record)
                           (getf (getf second :attempt) :record))))
              (ok (not (eq (getf (getf first :install) :record)
                           (getf (getf second :install) :record))))
              (ok (not (eq (first (getf (getf first :install) :row))
                           (first (getf (getf second :install) :row))))))
         (setf (pg::physical-reseed-follow-owner-busy-p owner) nil)))))

(deftest reseed-cancellation-operation-keeps-busy-and-revokes-scope
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore root record))
       (let ((pending (pg::physical-reseed-follow-begin-cancellation owner))
             (saved nil))
         (pg::call-with-physical-reseed-follow-cancellation-operation
          owner
          (lambda (operation)
            (setf saved operation)
            (ok (string= (first (pg::physical-reseed-follow-cancellation-operation-root operation))
                         (namestring (pg::physical-adoption-owner-directory owner))))
            (let ((snapshot (pg::physical-reseed-follow-cancellation-operation-call operation :snapshot)))
              (ok (equal pending (getf (getf snapshot :follow) :record)))
              (ok (pg::physical-reseed-follow-owner-busy-p owner))
              (ok (signals (pg::call-with-physical-reseed-follow-cancellation-operation owner #'values) 'error))
              (pg::physical-reseed-follow-cancellation-operation-call operation :network-begin snapshot)
              (ok (signals
                   (pg::physical-reseed-follow-cancellation-operation-call
                    operation :source-closed snapshot
                    (reseed-cancellation-source-parent pending :closed :aborted t)) 'error))
              (ok (equal snapshot
                         (pg::physical-reseed-follow-cancellation-operation-call operation :check snapshot)))
              (pg::physical-reseed-follow-cancellation-operation-call operation :network-end snapshot))))
         (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
         (ok (signals (pg::physical-reseed-follow-cancellation-operation-call saved :snapshot) 'error))))))

(deftest reseed-cancellation-held-source-closed-keeps-operation-through-publication
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore root record))
       (let ((pending (pg::physical-reseed-follow-begin-cancellation owner)))
         (pg::call-with-physical-reseed-follow-cancellation-operation
          owner
          (lambda (operation)
            (let* ((before (pg::physical-reseed-follow-cancellation-operation-call operation :snapshot))
                   (receipt (reseed-cancellation-source-parent pending :closed :handed-off t)))
              (multiple-value-bind (closed after)
                  (pg::physical-reseed-follow-record-cancellation-source-closed-held operation before receipt)
                (ok (eq :source-closed (getf (getf closed :cancellation) :phase)))
                (ok (pg::physical-reseed-follow-owner-busy-p owner))
                (ok (equal closed (getf (getf after :follow) :record)))
                (ok (signals
                     (pg::physical-reseed-follow-cancellation-operation-call operation :check before) 'error))
                (ok (equal after
                           (pg::physical-reseed-follow-cancellation-operation-call operation :check after)))))))
         (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))

(deftest reseed-cancellation-operation-cannot-complete-uncertain-publication
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore root record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
              (injected 0)
              (pg::*physical-reseed-follow-test-hook*
                (lambda (event path)
                  (declare (ignore path))
                  (when (and (eq event :after-follow-temporary-create) (zerop injected))
                    (incf injected)
                    (error "source-closed publication interrupted")))))
         (let ((rejected
                 (handler-case
                     (progn
                       (pg::call-with-physical-reseed-follow-cancellation-operation
                        owner
                        (lambda (operation)
                          (let ((snapshot
                                  (pg::physical-reseed-follow-cancellation-operation-call operation :snapshot)))
                            (ok (signals
                                 (pg::physical-reseed-follow-record-cancellation-source-closed-held
                                  operation snapshot receipt) 'error))
                            (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                            (ok (signals
                                 (pg::physical-reseed-follow-cancellation-operation-call
                                  operation :publication-complete snapshot) 'error))
                            (ok (signals
                                 (pg::physical-reseed-follow-cancellation-operation-call
                                  operation :network-begin snapshot) 'error)))))
                       nil)
                   (error () t))))
           (ok rejected)
           (ok (= 1 injected))
           (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
           (ok (not (pg::physical-reseed-follow-owner-busy-p owner))))))))

(deftest reseed-cancellation-operation-rejects-direct-publication-completion
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore root record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
              (closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)))
         (pg::call-with-physical-reseed-follow-cancellation-operation
          owner
          (lambda (operation)
            (let ((snapshot (pg::physical-reseed-follow-cancellation-operation-call operation :snapshot)))
              (ok (signals (pg::physical-reseed-follow-cancellation-operation-call
                            operation :publication-begin snapshot closed) 'error))
              (ok (signals (pg::physical-reseed-follow-cancellation-operation-call
                            operation :publication-complete snapshot) 'error))
              (ok (equal snapshot
                         (pg::physical-reseed-follow-cancellation-operation-call operation :check snapshot))))))))))

(deftest reseed-cancellation-capacity-preflight-precedes-network-but-uses-known-receipts
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore root record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
              (original (symbol-function 'pg::physical-reseed-follow-cancellation-check-cleanup-capacity))
              (phases nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-reseed-follow-cancellation-check-cleanup-capacity)
                      (lambda (value &optional (maximum pg::+physical-reseed-follow-maximum-bytes+))
                        (let ((phase (getf (getf value :cancellation) :phase)))
                          (push phase phases)
                          (when (eq phase :pending) (error "unknown outcome capacity unavailable"))
                          (funcall original value maximum))))
                (pg::call-with-physical-reseed-follow-cancellation-operation
                 owner
                 (lambda (operation)
                   (let ((snapshot (pg::physical-reseed-follow-cancellation-operation-call operation :snapshot)))
                     (ok (null phases))
                     (ok (signals
                          (pg::physical-reseed-follow-cancellation-operation-call
                           operation :network-begin snapshot) 'error)))))
                (ok (equal '(:pending) phases))
                (setf phases nil)
                (let ((closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)))
                  (ok (eq :source-closed (getf (getf closed :cancellation) :phase))))
                (ok (equal '(:source-closed) phases)))
           (setf (symbol-function 'pg::physical-reseed-follow-cancellation-check-cleanup-capacity) original))))))

(deftest reseed-cancellation-operation-requires-closed-network-phase-on-return
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore record))
       (let ((pending (pg::physical-reseed-follow-begin-cancellation owner)))
         (ok (signals
              (pg::call-with-physical-reseed-follow-cancellation-operation
               owner
               (lambda (operation)
                 (let ((snapshot (pg::physical-reseed-follow-cancellation-operation-call operation :snapshot)))
                   (pg::physical-reseed-follow-cancellation-operation-call operation :network-begin snapshot))))
              'error))
         (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
         (ok (equalp (pg::physical-reseed-follow-image pending)
                     (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))

(deftest reseed-cancellation-operation-result-requires-exact-closed-receipt
    (dolist (outcome '(:aborted :handed-off))
      (let* ((shape (if (eq outcome :aborted) 0 1))
             (pending (pg::physical-reseed-follow-cancellation-begin-record
                       (reseed-cancellation-codec-fixture shape nil)))
             (receipt (reseed-cancellation-source-parent pending :closed outcome t))
             (closed (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
             (plan (pg::physical-reseed-follow-cancellation-ack-plan closed receipt))
             (before (pg::physical-bootstrap-manifest-text closed)))
        (ok (equal plan
                   (pg::physical-reseed-follow-cancellation-confirm-operation-result closed plan receipt)))
        (ok (signals
             (pg::physical-reseed-follow-cancellation-confirm-operation-result
              closed plan (reseed-cancellation-source-parent pending :closing outcome t)) 'error))
        (ok (signals
             (pg::physical-reseed-follow-cancellation-confirm-operation-result
              closed plan (reseed-cancellation-source-parent pending :closed
                                                             (if (eq outcome :aborted) :handed-off :aborted) t)) 'error))
        (let ((changed (pg::physical-adoption-attempt-value-copy plan)))
          (setf (getf changed :ack) :caller-asserted)
          (ok (signals
               (pg::physical-reseed-follow-cancellation-confirm-operation-result closed changed receipt) 'error)))
        (ok (equal before (pg::physical-bootstrap-manifest-text closed))))))

(deftest reseed-cancellation-operation-result-preserves-history-and-plan-fields
    (dotimes (shape 3)
      (let* ((pending (pg::physical-reseed-follow-cancellation-begin-record
                       (reseed-cancellation-codec-fixture shape nil)))
             (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
             (plan (pg::physical-reseed-follow-cancellation-ack-plan pending receipt)))
        (if (zerop shape)
            (progn
              (ok (equal plan
                         (pg::physical-reseed-follow-cancellation-confirm-operation-result pending plan receipt)))
              (dolist (key '(:operation :ack :receipt :batch :successor))
                (let ((changed (pg::physical-adoption-attempt-value-copy plan)))
                  (setf (getf changed key) :changed)
                  (ok (signals
                       (pg::physical-reseed-follow-cancellation-confirm-operation-result pending changed receipt)
                       'error)))))
            (progn
              (ok (null plan))
              (ok (signals
                   (pg::physical-reseed-follow-cancellation-confirm-operation-result pending nil receipt) 'error))))
        (when (plusp shape)
          (let* ((handoff (reseed-cancellation-source-parent pending :closed :handed-off t))
                 (closed (pg::physical-reseed-follow-cancellation-source-closed-record pending handoff))
                 (release (pg::physical-reseed-follow-cancellation-ack-plan closed handoff)))
            (ok (signals
                 (pg::physical-reseed-follow-cancellation-confirm-operation-result pending release handoff) 'error))
            (dolist (key '(:name :anchor-digest))
              (let ((changed (pg::physical-adoption-attempt-value-copy release)))
                (setf (getf (getf changed :successor) key) :changed)
                (ok (signals
                     (pg::physical-reseed-follow-cancellation-confirm-operation-result closed changed handoff)
                     'error)))))))))

(defun reseed-cancellation-source-cleanup-events (state)
  (remove-if-not (lambda (event) (member event '(:cleanup :release :close)))
                 (reverse (getf state :events))))

(deftest reseed-cancellation-rpc-evidence-follows-checked-release-and-close
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore root record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :handed-off t))
              (closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt))
              (saved nil))
         (call-with-reseed-cancellation-source
          owner closed receipt
          (lambda (state)
            (setf (getf state :allowed-commands) '(:cleanup :release))
            (dolist (case '(:success :network-claim :wrong-receipt :lost-release :close-failure))
              (setf (getf state :events) nil
                    (getf state :cleanup-result)
                    (if (eq case :wrong-receipt)
                        (reseed-cancellation-source-parent pending :closed :aborted t) receipt)
                    (getf state :on-event)
                    (lambda (event)
                      (when (or (and (eq case :lost-release) (eq event :release))
                                (and (eq case :close-failure) (eq event :close)))
                        (error "source release reply or client close failed"))))
              (let ((exposed 0))
                (flet ((run ()
                         (pg::call-with-physical-reseed-follow-cancellation-operation
                          owner
                          (lambda (operation)
                            (let ((snapshot
                                    (pg::physical-reseed-follow-cancellation-operation-call
                                     operation :snapshot)))
                              (pg::call-with-physical-reseed-follow-cleanup-operation-evidence
                               operation snapshot
                               (lambda (evidence)
                                 (incf exposed)
                                 (setf saved evidence)
                                 (ok (equal '(:cleanup :release :close) (reseed-cancellation-source-cleanup-events state)))
                                 (ok (signals
                                      (pg::call-with-physical-reseed-follow-cleanup-operation-evidence
                                       operation snapshot #'identity :helper "/unused/helper") 'error))
                                 (ok (signals
                                      (pg::physical-reseed-follow-claim-cleanup-operation-evidence
                                       (pg::%make-physical-reseed-follow-cleanup-operation-evidence
                                        :claim (lambda (&rest arguments)
                                                 (declare (ignore arguments)) :forged))
                                       operation snapshot) 'error))
                                 (if (eq case :network-claim)
                                     (progn
                                       (pg::physical-reseed-follow-cancellation-operation-call
                                        operation :network-begin snapshot)
                                       (ok (signals
                                            (pg::physical-reseed-follow-claim-cleanup-operation-evidence
                                             evidence operation snapshot) 'error))
                                       (pg::physical-reseed-follow-cancellation-operation-call
                                        operation :network-end snapshot))
                                     (let ((plan
                                             (pg::physical-reseed-follow-claim-cleanup-operation-evidence
                                              evidence operation snapshot)))
                                       (ok (eq :selected-release (getf plan :ack)))
                                       (ok (equal receipt (getf plan :receipt)))))
                                 (ok (signals
                                      (pg::physical-reseed-follow-claim-cleanup-operation-evidence
                                       evidence operation snapshot) 'error)))
                               :helper "/unused/helper"))))))
                  (if (member case '(:success :network-claim))
                      (progn (run) (ok (= 1 exposed)))
                      (progn
                        (ok (signals (run) 'error))
                        (ok (zerop exposed)))))
                (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                (ok (equal (if (eq case :wrong-receipt) '(:cleanup :close)
                               '(:cleanup :release :close))
                           (reseed-cancellation-source-cleanup-events state)))))
            (ok (signals
                 (pg::physical-reseed-follow-claim-cleanup-operation-evidence saved nil nil) 'error))))))))

(deftest reseed-cancellation-rpc-evidence-aborts-empty-history-on-current-source
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-configuration-owner
        root
        (lambda (owner)
          (pg::physical-reseed-follow-publish-intent owner (pg::physical-reseed-follow-initial-record owner))
          (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                 (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
                 (closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)))
            (call-with-reseed-cancellation-source
             owner closed receipt
             (lambda (state)
               (setf (getf state :allowed-commands) '(:abort))
               (pg::call-with-physical-reseed-follow-cancellation-operation
                owner
                (lambda (operation)
                  (let ((snapshot
                          (pg::physical-reseed-follow-cancellation-operation-call operation :snapshot)))
                    (pg::call-with-physical-reseed-follow-cleanup-operation-evidence
                     operation snapshot
                     (lambda (evidence)
                       (ok (= 1 (count :abort (getf state :events))))
                       (ok (= 1 (count :close (getf state :events))))
                       (let ((ack
                               (pg::physical-reseed-follow-claim-cleanup-operation-evidence
                                evidence operation snapshot)))
                         (ok (eq :parent-abort (getf ack :ack)))
                         (ok (null (getf ack :batch)))
                         (ok (equal receipt (getf ack :receipt)))))
                     :helper "/unused/helper"))))
               
               (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))))))))))

(deftest reseed-cancellation-cleaned-publication-consumes-actual-release-evidence
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore root record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :handed-off t))
              (closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt))
              (saved nil) (exposed 0))
         (call-with-reseed-cancellation-source
          owner closed receipt
          (lambda (state)
            (setf (getf state :allowed-commands) '(:cleanup :release))
            (pg::call-with-physical-reseed-follow-cancellation-operation
             owner
             (lambda (operation)
               (let ((snapshot
                       (pg::physical-reseed-follow-cancellation-operation-call
                        operation :snapshot)))
                 (pg::call-with-physical-reseed-follow-cleanup-operation-evidence
                  operation snapshot
                  (lambda (evidence)
                    (incf exposed)
                    (setf saved evidence)
                    (ok (equal '(:cleanup :release :close) (reseed-cancellation-source-cleanup-events state)))
                    (ok (signals
                         (pg::physical-reseed-follow-record-cancellation-cleaned-held
                          operation snapshot nil) 'error))
                    (multiple-value-bind (cleaned after)
                        (pg::physical-reseed-follow-record-cancellation-cleaned-held
                         operation snapshot evidence)
                      (ok (eq :cleaned (getf (getf cleaned :cancellation) :phase)))
                      (ok (eq :selected-release (getf (getf cleaned :cancellation) :ack)))
                      (ok (equal receipt (getf (getf cleaned :cancellation) :receipt)))
                      (ok (pg::physical-reseed-follow-owner-busy-p owner))
                      (ok (equal cleaned (getf (getf after :follow) :record)))
                      (ok (equal after
                                 (pg::physical-reseed-follow-cancellation-operation-call
                                  operation :check after)))
                      (ok (signals
                           (pg::physical-reseed-follow-record-cancellation-cleaned-held
                            operation after evidence) 'error)))
                    (ok (signals
                         (pg::physical-reseed-follow-claim-cleanup-operation-evidence
                          evidence operation snapshot) 'error)))
                  :helper "/unused/helper"))))
            (ok (= 1 exposed))
            (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
            (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
            (ok (equal '(:cleanup :release :close) (reseed-cancellation-source-cleanup-events state)))
            (ok (signals
                 (pg::physical-reseed-follow-claim-cleanup-operation-evidence saved nil nil) 'error))))))))

(deftest reseed-cancellation-cleaned-publication-consumes-actual-abort-evidence
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-configuration-owner
        root
        (lambda (owner)
          (pg::physical-reseed-follow-publish-intent owner (pg::physical-reseed-follow-initial-record owner))
          (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                 (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
                 (closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)))
            (call-with-reseed-cancellation-source
             owner closed receipt
             (lambda (state)
               (setf (getf state :allowed-commands) '(:abort))
               (pg::call-with-physical-reseed-follow-cancellation-operation
                owner
                (lambda (operation)
                  (let ((snapshot
                          (pg::physical-reseed-follow-cancellation-operation-call operation :snapshot)))
                    (pg::call-with-physical-reseed-follow-cleanup-operation-evidence
                     operation snapshot
                     (lambda (evidence)
                       (ok (= 1 (count :abort (getf state :events))))
                       (ok (= 1 (count :close (getf state :events))))
                       (multiple-value-bind (cleaned after)
                           (pg::physical-reseed-follow-record-cancellation-cleaned-held
                            operation snapshot evidence)
                         (ok (eq :cleaned (getf (getf cleaned :cancellation) :phase)))
                         (ok (eq :parent-abort (getf (getf cleaned :cancellation) :ack)))
                         (ok (equal receipt (getf (getf cleaned :cancellation) :receipt)))
                         (ok (equal cleaned (getf (getf after :follow) :record)))))
                     :helper "/unused/helper"))))
               
               (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))))))))))

(deftest reseed-cancellation-cleaned-publication-failure-consumes-evidence-and-fences-owner
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore root record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :handed-off t))
              (closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt))
              (saved nil) (exposed 0))
         (call-with-reseed-cancellation-source
          owner closed receipt
          (lambda (state)
            (setf (getf state :allowed-commands) '(:cleanup :release))
            (ok (signals
                 (pg::call-with-physical-reseed-follow-cancellation-operation
                  owner
                  (lambda (operation)
                    (let ((snapshot
                            (pg::physical-reseed-follow-cancellation-operation-call
                             operation :snapshot)))
                      (pg::call-with-physical-reseed-follow-cleanup-operation-evidence
                       operation snapshot
                       (lambda (evidence)
                         (incf exposed)
                         (setf saved evidence)
                         (ok (equal '(:cleanup :release :close) (reseed-cancellation-source-cleanup-events state)))
                         (ok (signals
                              (pg::physical-reseed-follow-record-cancellation-cleaned-held
                               operation snapshot nil) 'error))
                         (let* ((injected 0)
                                (pg::*physical-reseed-follow-test-hook*
                                  (lambda (event path)
                                    (declare (ignore path))
                                    (when (eq event :after-follow-temporary-create)
                                      (incf injected)
                                      (error "CLEANED publication interrupted")))))
                           (ok (signals
                                (pg::physical-reseed-follow-record-cancellation-cleaned-held
                                 operation snapshot evidence) 'error))
                           (ok (= 1 injected))
                           (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                           (ok (signals
                                (pg::physical-reseed-follow-cancellation-operation-call
                                 operation :network-begin snapshot) 'error)))
                         (ok (signals
                              (pg::physical-reseed-follow-claim-cleanup-operation-evidence
                               evidence operation snapshot) 'error)))
                       :helper "/unused/helper"))))
                 'error))
            (ok (= 1 exposed))
            (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
            (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
            (ok (equal '(:cleanup :release :close) (reseed-cancellation-source-cleanup-events state)))
            (ok (signals
                 (pg::physical-reseed-follow-claim-cleanup-operation-evidence saved nil nil) 'error))))))))

(defun call-with-reseed-cancellation-source (owner record receipt function)
  (let* ((source (getf (getf record :attempt) :source))
         (request (getf (getf record :attempt) :request))
         (clients (make-hash-table :test #'eq))
         (state (list :observation receipt :abort-result receipt :cleanup-result receipt
                      :release-result t :events nil :on-event nil
                      :allowed-commands '(:status :abort :cleanup :release)))
         (symbols '(pg::physical-libpq-connect-frozen pg::physical-client-close
                    pg::physical-adoption-attempt-check-client pg::physical-client-identity-row
                    pg::pg-client-native-backup-lease-status pg::pg-client-native-backup-lease-abort
                    pg::pg-client-native-backup-lease-cleanup-selected pg::pg-client-native-release))
         (originals (mapcar #'symbol-function symbols)))
    (labels ((event (name)
               (when (member name '(:status :abort :cleanup :release))
                 (assert (member name (getf state :allowed-commands))))
               (assert (pg::physical-reseed-follow-owner-busy-p owner))
               (assert (null pg::*physical-reseed-follow-installation-scope*))
               (push name (getf state :events))
               (when (getf state :on-event) (funcall (getf state :on-event) name)))
             (client-check (client)
               (assert (eq :live (gethash client clients))))
             (result (client supplied key name)
               (client-check client)
               (assert (equal request supplied))
               (let ((copy (when (getf state key) (pg::physical-backup-lease-copy (getf state key)))))
                 (event name)
                 copy)))
      (unwind-protect
           (progn
             (setf (symbol-function (first symbols))
                   (lambda (helper frozen &key on-spawn)
                     (assert (string= helper "/unused/helper"))
                     (assert (equal (getf source :owner) (pg::physical-libpq-frozen-profile-datum frozen)))
                     (let ((client (gensym "CANCELLATION-CLIENT")))
                       (setf (gethash client clients) :live)
                       (when on-spawn (funcall on-spawn client))
                       (event :connect)
                       client))
                   (symbol-function (second symbols))
                   (lambda (client)
                     (client-check client)
                     (setf (gethash client clients) :closed)
                     (event :close))
                   (symbol-function (third symbols))
                   (lambda (client actual-source)
                     (client-check client)
                     (assert (equal source actual-source))
                     t)
                   (symbol-function (fourth symbols))
                   (lambda (client)
                     (client-check client)
                     (event :identity)
                     (vector (write-to-string (getf request :system-id))
                             (write-to-string (1+ (getf request :timeline))) "0/0" nil))
                   (symbol-function (fifth symbols))
                   (lambda (client supplied) (result client supplied :observation :status))
                   (symbol-function (sixth symbols))
                   (lambda (client supplied) (result client supplied :abort-result :abort))
                   (symbol-function (seventh symbols))
                   (lambda (client supplied selected)
                     (assert (equal (getf receipt :successor) selected))
                     (result client supplied :cleanup-result :cleanup))
                   (symbol-function (eighth symbols))
                   (lambda (client name digest)
                     (client-check client)
                     (assert (equal (getf (getf receipt :successor) :name) name))
                     (assert (= (getf (getf receipt :successor) :anchor-digest) digest))
                     (event :release)
                     (getf state :release-result)))
             (funcall function state)
             (ok (loop for value being the hash-values of clients always (eq value :closed))))
        (loop for symbol in symbols for original in originals
              do (setf (symbol-function symbol) original))))))

(deftest reseed-cancellation-public-actor-closes-selected-debt-and-retries-locally
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :handed-off t)))
         (call-with-reseed-cancellation-source
          owner pending receipt
          (lambda (state)
            (multiple-value-bind (cleaned outcome)
                (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper")
              (ok (eq :complete outcome))
              (ok (eq :cleaned (getf (getf cleaned :cancellation) :phase)))
              (ok (eq :selected-release (getf (getf cleaned :cancellation) :ack)))
              (ok (equal receipt (getf (getf cleaned :cancellation) :receipt)))
              (ok (equal cleaned
                         (pg::physical-reseed-follow-decode
                          (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
              (ok (= 2 (count :cleanup (getf state :events))))
              (ok (= 1 (count :release (getf state :events))))
              (ok (= 1 (count :status (getf state :events))))
              (ok (= 2 (count :close (getf state :events))))
              (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
              (let ((events (copy-list (getf state :events))))
                (multiple-value-bind (again result)
                    (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper")
                  (ok (equal cleaned again))
                  (ok (eq :complete result)))
                (ok (equal events (getf state :events)))))))))))

(deftest reseed-cancellation-public-actor-preserves-unknown-and-unrevoked-debt
    (dolist (case '(:unknown :unbound :aborted :open :target-aborted))
      (call-with-reseed-cancellation-test-fixture
       (lambda (owner root record)
         (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                (receipt (reseed-cancellation-source-parent pending :closed :aborted t)))
           (when (eq case :target-aborted) (reseed-follow-prepare-test-abort root record))
           (let ((attempt (lineage-test-bytes (merge-pathnames "physical-reseed.attempt" root))))
             (call-with-reseed-cancellation-source
              owner pending receipt
              (lambda (state)
                (setf (getf state :observation)
                      (case case
                        (:unknown nil)
                        (:unbound (reseed-cancellation-source-parent pending :requested nil nil))
                        (:open (reseed-cancellation-source-parent pending :ready nil t))
                        (otherwise receipt)))
                (multiple-value-bind (after outcome)
                    (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper")
                  (let ((unresolved (member case '(:unknown :unbound))))
                    (ok (eq (if unresolved :unresolved :deferred) outcome))
                    (ok (eq (if unresolved :pending :source-closed)
                            (getf (getf after :cancellation) :phase)))
                    (when unresolved (ok (equal pending after))))
                  (ok (equalp attempt (lineage-test-bytes (merge-pathnames "physical-reseed.attempt" root))))
                  (ok (= (if (eq case :open) 1 0) (count :abort (getf state :events))))
                  (ok (zerop (count :cleanup (getf state :events))))
                  (ok (zerop (count :release (getf state :events))))
                  (ok (= 1 (count :close (getf state :events))))
                  (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                  (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner))))))))))))

(deftest reseed-cancellation-public-actor-source-failures-preserve-pending-intent
    (dolist (cut '(:connect :identity :status))
      (call-with-reseed-cancellation-test-fixture
       (lambda (owner root record)
         (declare (ignore record))
         (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                (receipt (reseed-cancellation-source-parent pending :closed :handed-off t))
                (bytes (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
                (injected 0))
           (call-with-reseed-cancellation-source
            owner pending receipt
            (lambda (state)
              (setf (getf state :on-event)
                    (lambda (event)
                      (when (eq event cut)
                        (incf injected)
                        (error "source connection or observation failed"))))
              (ok (signals
                   (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper") 'error))
              (ok (= 1 injected))
              (ok (= 1 (count :close (getf state :events))))
              (ok (zerop (count :cleanup (getf state :events))))
              (ok (zerop (count :release (getf state :events))))
              (ok (equalp bytes (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
              (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
              (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner))))))))))

(deftest reseed-cancellation-public-actor-completes-empty-history-abort
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-configuration-owner
        root
        (lambda (owner)
          (pg::physical-reseed-follow-publish-intent owner (pg::physical-reseed-follow-initial-record owner))
          (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                 (receipt (reseed-cancellation-source-parent pending :closed :aborted t)))
            (call-with-reseed-cancellation-source
             owner pending receipt
             (lambda (state)
               (multiple-value-bind (cleaned outcome)
                   (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper")
                 (ok (eq :complete outcome))
                 (ok (eq :cleaned (getf (getf cleaned :cancellation) :phase)))
                 (ok (eq :parent-abort (getf (getf cleaned :cancellation) :ack)))
                 (ok (null (getf cleaned :generations)))
                 (ok (equal receipt (getf (getf cleaned :cancellation) :receipt)))
                 (ok (= 2 (count :abort (getf state :events))))
                 (ok (= 2 (count :close (getf state :events))))
                 (ok (zerop (count :release (getf state :events)))))))))))))

(deftest reseed-cancellation-public-actor-resumes-after-uncertain-source-effects
    (dolist (cut '(:second-connect :cleanup-reply :release-reply))
      (call-with-reseed-cancellation-test-fixture
       (lambda (owner root record)
         (declare (ignore record))
         (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                (receipt (reseed-cancellation-source-parent pending :closed :handed-off t))
                (connections 0) (injected 0))
           (call-with-reseed-cancellation-source
            owner pending receipt
            (lambda (state)
              (setf (getf state :on-event)
                    (lambda (event)
                      (when (eq event :connect) (incf connections))
                      (when (and (zerop injected)
                                 (case cut
                                   (:second-connect (and (eq event :connect) (= 2 connections)))
                                   (:cleanup-reply (eq event :cleanup))
                                   (:release-reply (eq event :release))))
                        (incf injected)
                        (error "source effect or connection completed without a usable reply"))))
              (ok (signals
                   (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper") 'error))
              (ok (= 1 injected))
              (let ((saved (pg::physical-reseed-follow-decode
                            (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
                (ok (eq (if (eq cut :cleanup-reply) :pending :source-closed)
                        (getf (getf saved :cancellation) :phase))))
              (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
              (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
              (setf (getf state :on-event) nil)
              (multiple-value-bind (cleaned outcome)
                  (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper")
                (ok (eq :complete outcome))
                (ok (eq :cleaned (getf (getf cleaned :cancellation) :phase)))
                (ok (equal receipt (getf (getf cleaned :cancellation) :receipt))))
              (ok (= (if (eq cut :cleanup-reply) 2 1) (count :status (getf state :events))))
              (ok (= (if (eq cut :release-reply) 2 1) (count :release (getf state :events)))))))))))

(deftest reseed-cancellation-public-actor-refuses-local-change-around-source-work
    (dolist (cut '(:connect :status :cleanup :close))
      (call-with-reseed-cancellation-test-fixture
       (lambda (owner root record)
         (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                (receipt (reseed-cancellation-source-parent pending :closed :handed-off t))
                (bytes (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
                (injected 0))
           (call-with-reseed-cancellation-source
            owner pending receipt
            (lambda (state)
              (setf (getf state :on-event)
                    (lambda (event)
                      (when (and (zerop injected) (eq event cut))
                        (incf injected)
                        (reseed-follow-prepare-test-abort root record))))
              (ok (signals
                   (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper") 'error))
              (ok (= 1 injected))
              (ok (equalp bytes (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
              (ok (zerop (count :release (getf state :events))))
              (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
              (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
              (setf (getf state :on-event) nil)
              (multiple-value-bind (cleaned outcome)
                  (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper")
                (ok (eq :complete outcome))
                (ok (eq :cleaned (getf (getf cleaned :cancellation) :phase)))))))))))

(deftest reseed-cancellation-source-mock-rejects-unexpected-commands
    (dolist (selected '(nil t))
      (call-with-reseed-cancellation-test-fixture
       (lambda (owner root record)
         (declare (ignore root record))
         (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                (receipt (reseed-cancellation-source-parent pending :closed :handed-off t))
                (request (getf (getf pending :attempt) :request))
                (source (getf (getf pending :attempt) :source)))
           (call-with-reseed-cancellation-source
            owner pending receipt
            (lambda (state)
              (setf (getf state :allowed-commands) (if selected '(:cleanup :release) '(:abort)))
              (let ((client nil))
                (unwind-protect
                     (progn
                       (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
                       (setf client
                             (pg::physical-libpq-connect-frozen
                              "/unused/helper" (nth-value 1 (pg::physical-follow-source-profile source))))
                       (ok (signals (pg::pg-client-native-backup-lease-status client request) 'error))
                       (if selected
                           (ok (signals (pg::pg-client-native-backup-lease-abort client request) 'error))
                           (progn
                             (ok (signals
                                  (pg::pg-client-native-backup-lease-cleanup-selected
                                   client request (getf receipt :successor)) 'error))
                             (ok (signals
                                  (pg::pg-client-native-release
                                   client (getf (getf receipt :successor) :name)
                                   (getf (getf receipt :successor) :anchor-digest)) 'error))))
                       (ok (null (reseed-cancellation-source-cleanup-events state))))
                  (unwind-protect
                       (when client (pg::physical-client-close client))
                    (setf (pg::physical-reseed-follow-owner-busy-p owner) nil)))))))))))

(deftest reseed-cancellation-issuer-refuses-mutation-around-abort-release-and-close
    (dolist (command '(:abort :release :close))
      (dolist (timing '(:before :after))
        (call-with-reseed-follow-installed-fixture
         (lambda (root stage incoming quarantine plan)
           (declare (ignore stage incoming quarantine plan))
           (let ((closed nil) (receipt nil) (injected 0))
             (pg::call-with-physical-reseed-follow-configuration-owner
              root
              (lambda (owner)
                (let* ((initial (pg::physical-reseed-follow-initial-record owner))
                       (record (if (eq command :abort) initial
                                   (reseed-follow-intent-test-record initial root))))
                  (pg::physical-reseed-follow-publish-intent owner record)
                  (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                         (path (merge-pathnames "physical-reseed.follow" root))
                         (install (lineage-test-bytes (merge-pathnames "physical-reseed.install" root))))
                    (setf receipt (reseed-cancellation-source-parent
                                   pending :closed (if (eq command :abort) :aborted :handed-off) t)
                          closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt))
                    (let ((bytes (lineage-test-bytes path))
                          (expected-attempt (pg::physical-reseed-attempt-copy (getf record :attempt))))
                      (setf (getf expected-attempt :phase) :aborting)
                      (call-with-reseed-cancellation-source
                       owner closed receipt
                       (lambda (state)
                         (let* ((symbol (ecase command
                                          (:abort 'pg::pg-client-native-backup-lease-abort)
                                          (:release 'pg::pg-client-native-release)
                                          (:close 'pg::physical-client-close)))
                                (original (symbol-function symbol)))
                           (flet ((mutate ()
                                    (when (zerop injected)
                                      (incf injected)
                                      (reseed-follow-prepare-test-abort root record))))
                             (unwind-protect
                                  (progn
                                    (setf (symbol-function symbol)
                                          (lambda (&rest arguments)
                                            (when (eq timing :before) (mutate))
                                            (multiple-value-prog1 (apply original arguments)
                                              (when (eq timing :after) (mutate)))))
                                    (ok (signals
                                         (pg::physical-reseed-follow-cleanup-cancellation
                                          owner :helper "/unused/helper") 'error)))
                               (setf (symbol-function symbol) original))))
                         (ok (= 1 injected))
                         (ok (= 1 (count :close (getf state :events))))
                         (ok (= (if (eq command :abort) 1 0) (count :abort (getf state :events))))
                         (ok (= (if (eq command :abort) 0 1) (count :cleanup (getf state :events))))
                         (ok (= (if (eq command :abort) 0 1) (count :release (getf state :events))))
                         (ok (equalp bytes (lineage-test-bytes path)))
                         (ok (equalp install (lineage-test-bytes (merge-pathnames "physical-reseed.install" root))))
                         (ok (equal expected-attempt
                                    (pg::physical-reseed-attempt-decode
                                     (lineage-test-bytes (pg::physical-reseed-attempt-path root)))))
                         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                         (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner))))))))))
             (pg::call-with-physical-reseed-follow-configuration-owner
              root
              (lambda (owner)
                (call-with-reseed-cancellation-source
                 owner closed receipt
                 (lambda (state)
                   (declare (ignore state))
                   (multiple-value-bind (record outcome)
                       (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper")
                     (ok (eq :complete outcome))
                     (ok (eq :cleaned (getf (getf record :cancellation) :phase)))
                     (ok (equal receipt (getf (getf record :cancellation) :receipt)))
                     (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                     (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner))))))))))))))
