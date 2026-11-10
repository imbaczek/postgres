(in-package #:pg)

(defun unit-scan-stage-checkpoint ()
  (let ((snapshot (unit-commit-ts-snapshot)))
    (setf (getf snapshot :timeline) 1 (getf snapshot :prev-timeline) 1)
    snapshot))

(cl-spec:defproperty scan-stage-keeps-its-caller-private
    ((next (range integer 3 100000)))
  (:about physical-recovery-scan-stage)
  (:trials (:normal 100))
  (let* ((checkpoint (unit-scan-stage-checkpoint))
         (start (getf checkpoint :end))
         (store (%make-physical-store :timeline 1 :base-lsn start))
         (state (list :timeline 1 :end start :next-xid 3 :source-checkpoint checkpoint))
         (before (copy-tree state))
         (private (physical-recovery-scan-stage store start state checkpoint next)))
    (setf (getf (getf private :source-checkpoint) :next-xid) next)
    (and (equal state before) (not (eq private state))
         (not (eq (getf private :source-checkpoint) checkpoint)))))

(rove:deftest suffix-scan-stage-requires-a-checkpoint-and-counter-from-its-prefix
    (let* ((checkpoint (unit-scan-stage-checkpoint))
           (start (getf checkpoint :end))
           (store (%make-physical-store :timeline 1 :base-lsn start))
           (state (list :timeline 1 :end start :next-xid 3 :observed-next-xid 10000)))
      (rove:ok (= 1 (physical-recovery-replay-timeline
                     (physical-recovery-scan-stage store start state checkpoint 3))))
      (dolist (next '(nil 2 18446744073709551617))
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (physical-recovery-scan-stage store start state checkpoint next)))))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-recovery-scan-stage store (1- start) state checkpoint 3))))
      (rove:ok (physical-recovery-scan-stage store start state nil nil))
      (rove:ok (physical-recovery-scan-stage store (+ start 32) state nil 50))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-recovery-scan-stage store (+ start 32) state nil nil))))
      (rove:ok (physical-recovery-scan-stage store start state checkpoint 18446744073709551616))))

(rove:deftest suffix-scan-child-stage-does-not-borrow-a-later-runtime-checkpoint
    (let* ((checkpoint (unit-scan-stage-checkpoint))
           (history (timeline-parse-history 2 (format nil "1~c0/000000C0~%" #\Tab)))
           (store (%make-physical-store :timeline 1 :base-lsn (getf checkpoint :end)
                                        :selection (list :target 2 :history history)))
           (state (list :timeline 1 :source-checkpoint (list :timeline 2 :end 999)
                        :observed-next-xid 10000))
           (before (copy-tree state)))
      (let ((private (physical-recovery-scan-stage store 240 state nil 3)))
        (rove:ok (= 2 (physical-recovery-replay-timeline private)))
        (rove:ok (null (getf private :source-checkpoint))))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-recovery-scan-stage store 240 state nil nil))))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-recovery-scan-stage store 240 state checkpoint nil))))
      (let ((private (physical-recovery-scan-stage store 240 state checkpoint 3)))
        (rove:ok (= 2 (physical-recovery-replay-timeline private)))
        (rove:ok (= 1 (getf (getf private :source-checkpoint) :timeline))))
      (rove:ok (equal state before))))

(rove:deftest suffix-scan-stage-generated-private-state-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'scan-stage-keeps-its-caller-private :seed 269))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))))

(rove:deftest suffix-scan-seeds-never-use-a-future-checkpoint-or-observed-counter
    (let* ((checkpoint (unit-scan-stage-checkpoint))
           (end (getf checkpoint :end))
           (state (list :timeline 1 :end 32 :next-xid 3 :source-checkpoint checkpoint
                        :observed-next-xid 10000)))
      (rove:ok (equal (list checkpoint 3 t)
                      (multiple-value-list (physical-recovery-scan-seeds state end nil nil nil nil))))
      (rove:ok (equal '(nil nil nil)
                      (multiple-value-list (physical-recovery-scan-seeds state 32 nil nil nil nil))))
      (rove:ok (equal '(nil nil nil)
                      (multiple-value-list (physical-recovery-scan-seeds state (1+ end) nil nil nil nil))))
      (dolist (pair '((t nil) (nil t)))
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (physical-recovery-scan-seeds state end checkpoint (first pair) 3 (second pair))))))
      (rove:ok (equal '(nil 50 t)
                      (multiple-value-list (physical-recovery-scan-seeds state 32 nil t 50 t))))))

(rove:deftest suffix-scan-checkpoint-policy-cannot-be-downgraded-by-an-unknown-seed
    (dolist (enabled '(nil t))
      (let* ((checkpoint (wal-checkpoint-record-snapshot (unit-fpw-checkpoint-record enabled 64))))
        (setf (getf checkpoint :timeline) 1 (getf checkpoint :prev-timeline) 1)
        (let* ((end (getf checkpoint :end))
               (fact (physical-recovery-scan-checkpoint-fpw checkpoint nil end 1))
               (before (copy-list fact)))
          (rove:ok (physical-fpw-fact-at-p fact end 1))
          (rove:ok (eql enabled (getf fact :enabled)))
          (rove:ok (eq fact (physical-recovery-scan-checkpoint-fpw checkpoint fact end 1)))
          (let ((bad (copy-list fact)))
            (setf (getf bad :enabled) (not enabled))
            (rove:ok (unit-commit-ts-error-p
                      (lambda () (physical-recovery-scan-checkpoint-fpw checkpoint bad end 1)))))
          (rove:ok (equal before fact))
          (rove:ok (null (physical-recovery-scan-checkpoint-fpw checkpoint nil (1+ end) 1)))))))
