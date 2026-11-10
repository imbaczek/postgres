(in-package #:pg)

(cl-spec:defproperty eor-stage-requires-a-completed-current-checkpoint
    ((delta (range integer 0 10000)) (wrong (member nil t)) (future (member nil t)))
  (:about physical-replay-check-eor-stage)
  (:trials (:normal 100))
  (let* ((checkpoint (unit-scan-stage-checkpoint))
         (end (getf checkpoint :end))
         (store (%make-physical-store :timeline 1))
         (*physical-recovery-state* (list :timeline 1 :source-checkpoint checkpoint)))
    (when wrong (setf (getf checkpoint :timeline) 2))
    (eql (not (unit-commit-ts-error-p
               (lambda () (physical-replay-check-eor-stage store (if future (1- end) (+ end delta)) 1))))
         (not (or wrong future)))))

(rove:deftest eor-stage-requires-the-current-checkpoint-to-have-finished
    (let* ((checkpoint (unit-scan-stage-checkpoint))
           (end (getf checkpoint :end))
           (store (%make-physical-store :timeline 1))
           (*physical-recovery-state* (list :timeline 1 :source-checkpoint checkpoint))
           (before (copy-tree *physical-recovery-state*)))
      (rove:ok (physical-replay-check-eor-stage store end 1))
      (rove:ok (unit-commit-ts-error-p (lambda () (physical-replay-check-eor-stage store (1- end) 1))))
      (rove:ok (unit-commit-ts-error-p (lambda () (physical-replay-check-eor-stage store end 2))))
      (rove:ok (equal before *physical-recovery-state*))
      (setf (getf *physical-recovery-state* :source-checkpoint) (list :timeline 1 :end end))
      (rove:ok (unit-commit-ts-error-p (lambda () (physical-replay-check-eor-stage store end 1))))))

(rove:deftest eor-stage-allows-an-old-base-snapshot-but-not-an-uninitialized-child
    (let ((store (%make-physical-store :timeline 1)))
      (dolist (state '(nil (:timeline 1) (:timeline 1 :source-checkpoint nil)))
        (let ((*physical-recovery-state* (copy-tree state)))
          (rove:ok (physical-replay-check-eor-stage store 96 1))
          (rove:ok (unit-commit-ts-error-p (lambda () (physical-replay-check-eor-stage store 152 2))))))))

(rove:deftest eor-stage-generated-checkpoint-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'eor-stage-requires-a-completed-current-checkpoint :seed 270))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))))
