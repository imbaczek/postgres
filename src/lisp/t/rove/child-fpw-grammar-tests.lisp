(in-package #:pg)

(defun unit-child-fpw-fact (enabled timeline)
  (let ((fact (unit-source-fpw-fact :enabled enabled :start 216)))
    (setf (getf fact :witness-timeline) timeline
          (getf fact :coverage-timeline) timeline
          (getf fact :through-timeline) timeline
          (getf fact :last-disable-timeline) (unless enabled timeline))
    fact))

(defun unit-child-fpw-checkpoint (policy)
  (let* ((record (unit-fpw-checkpoint-record policy 296))
         (snapshot (wal-checkpoint-record-snapshot record)))
    (setf (getf snapshot :timeline) 2 (getf snapshot :prev-timeline) 1
          (wal-record-payload record) (wal-encode-checkpoint-snapshot snapshot))
    record))

(cl-spec:defgenerator unit-child-fpw-policy-arguments ()
  (let ((enabled (zerop (random 2))))
    (if (zerop (random 2))
        (list (unit-fpw-change-record enabled)
              (when (zerop (random 2)) (unit-child-fpw-fact enabled 1)) 2)
        (list (unit-child-fpw-checkpoint enabled) (unit-child-fpw-fact enabled 2) 2))))

(cl-spec:defspec-function physical-replay-check-child-policy
    (:args (record (type wal-record)) (fpw-fact (type list))
           (timeline (type (integer 1 4294967295))))
  (:args-generator unit-child-fpw-policy-arguments)
  (:pre (and *wal-recovery-group-active* *wal-recovery-reservation-batch-clean-p*))
  (:returns (type boolean))
  (:post result))

(cl-spec:defproperty child-fpw-checkpoint-policy-agrees-with-marker
    ((enabled (member nil t)) (policy (member :unknown nil t)))
  (:about physical-replay-check-child-policy)
  (:trials (:normal 100))
  (let ((*wal-recovery-group-active* t)
        (*wal-recovery-reservation-batch-clean-p* t))
    (eql (not (unit-commit-ts-error-p
               (lambda ()
                 (physical-replay-check-child-policy
                  (unit-child-fpw-checkpoint policy) (unit-child-fpw-fact enabled 2) 2))))
         (eql enabled policy))))

(rove:deftest child-fpw-marker-needs-one-clean-empty-batch
    (dolist (enabled '(nil t))
      (dolist (prior (list nil (unit-child-fpw-fact enabled 1)))
        (let ((*wal-recovery-group-active* t)
              (*wal-recovery-reservation-batch-clean-p* t))
          (rove:ok (physical-replay-check-child-policy (unit-fpw-change-record enabled) prior 2))
          (rove:ok *wal-recovery-group-active*)
          (rove:ok *wal-recovery-reservation-batch-clean-p*)))
      (dolist (framing '((nil t) (t nil) (nil nil)))
        (let ((*wal-recovery-group-active* (first framing))
              (*wal-recovery-reservation-batch-clean-p* (second framing)))
          (rove:ok (unit-commit-ts-error-p
                    (lambda () (physical-replay-check-child-policy
                                (unit-fpw-change-record enabled) nil 2))))))
      (let ((*wal-recovery-group-active* t)
            (*wal-recovery-reservation-batch-clean-p* t)
            (fact (unit-child-fpw-fact enabled 2)))
        (rove:ok (physical-fpw-fact-valid-p fact))
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (physical-replay-check-child-policy
                              (unit-fpw-change-record (not enabled)) fact 2)))))))

(rove:deftest child-fpw-checkpoint-preserves-marker-policy-and-legacy-absence
    (dolist (enabled '(nil t))
      (dolist (policy '(:unknown nil t))
        (let ((*wal-recovery-group-active* t)
              (*wal-recovery-reservation-batch-clean-p* t)
              (fact (unit-child-fpw-fact enabled 2)))
          (rove:ok (eql (eql enabled policy)
                        (not (unit-commit-ts-error-p
                              (lambda () (physical-replay-check-child-policy
                                          (unit-child-fpw-checkpoint policy) fact 2)))))))))
  (let ((*wal-recovery-group-active* t)
        (*wal-recovery-reservation-batch-clean-p* t))
    (rove:ok (physical-replay-check-child-policy (unit-child-fpw-checkpoint :unknown) nil 2))
    (rove:ok (physical-replay-check-child-policy
              (unit-child-fpw-checkpoint :unknown) (unit-child-fpw-fact nil 1) 2))))

(rove:deftest child-fpw-checkpoint-validator-uses-the-private-prefix-fact
    (let* ((*physical-recovery-state* (list :timeline 1 :replay-timeline 2
                                            :source-checkpoint (list :timeline 1)))
           (before (copy-tree *physical-recovery-state*))
           (*physical-receive-store* (%make-physical-store :selection (list :target 2 :parent 1)))
           (*wal-recovery-group-active* t)
           (*wal-recovery-reservation-batch-clean-p* t))
      (dolist (enabled '(nil t))
        (dolist (policy '(:unknown nil t))
          (rove:ok (eql (eql enabled policy)
                        (not (unit-commit-ts-error-p
                              (lambda () (physical-replay-check-child-initialization
                                          (unit-child-fpw-checkpoint policy) 3
                                          (unit-child-fpw-fact enabled 2))))))))
        (let* ((record (unit-child-fpw-checkpoint enabled))
               (next (getf (wal-checkpoint-record-snapshot record) :next-xid)))
          (rove:ok (unit-commit-ts-error-p
                    (lambda () (physical-replay-check-child-initialization
                                record (1+ next) (unit-child-fpw-fact enabled 2)))))))
      (rove:ok (equal before *physical-recovery-state*))))

(rove:deftest child-fpw-marker-rejects-malformed-record-before-credit
    (let ((*wal-recovery-group-active* t)
          (*wal-recovery-reservation-batch-clean-p* t))
      (dolist (record (list (unit-fpw-change-record nil :version 7)
                            (unit-fpw-change-record t :xid 3)))
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (physical-replay-check-child-policy record nil 2)))))
      (let ((record (unit-fpw-change-record nil)))
        (setf (aref (wal-record-payload record) 9) 1)
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (physical-replay-check-child-policy record nil 2)))))))

(rove:deftest child-fpw-checkpoint-generated-policy-contract
    (let ((*wal-recovery-group-active* t)
          (*wal-recovery-reservation-batch-clean-p* t))
      (dolist (result (list (cl-spec:result-data
                             (cl-spec:check-function 'physical-replay-check-child-policy :trials 100 :seed 268))
                            (cl-spec:result-data
                             (cl-spec:run-property 'child-fpw-checkpoint-policy-agrees-with-marker :seed 267))))
        (rove:ok (eq :passed (getf result :status)) (format nil "~s" result)))))
