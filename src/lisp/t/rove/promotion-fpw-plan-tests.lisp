(in-package #:pg)

(defun unit-promotion-fpw-plan-state (fork source &optional (intent :legacy))
  (let ((state (list :phase 3 :applied fork :timeline 1 :promotion-child 2
                     :next-xid 50 :state-time 1793030000
                     :source-reservation (list :version 1 :through fork :bound 1002 :complete t))))
    (unless (eq source :unknown)
      (let ((fact (unit-source-fpw-fact :enabled source)))
        (setf (getf fact :through) fork (getf state :source-fpw-fact) fact)))
    (unless (eq intent :legacy) (setf (getf state :promotion-full-page-writes) intent))
    state))

(defun unit-promotion-fpw-plan-records (plan)
  (mapcar (lambda (frame) (wal-decode-record (second frame) (first frame))) (getf plan :frames)))

(defun unit-promotion-fpw-plan-geometry-p (plan)
  (loop with previous = (getf plan :fork)
        for (start bytes) in (getf plan :frames)
        always (and (<= previous start) (<= (+ (mod start +wal-segment-size+) (length bytes)) +wal-segment-size+)
                    (= start (wal-record-start (wal-decode-record bytes start))))
        do (setf previous (+ start (length bytes)))))

(cl-spec:defgenerator unit-promotion-fpw-plan-arguments ()
  (list (unit-promotion-fpw-plan-state (+ 256 (random 50000000))
                                       (nth (random 3) '(:unknown nil t)) (zerop (random 2)))
        20000 1003))

(cl-spec:defspec-function physical-promotion-checkpoint-plan
    (:args (state (type list)) (next-oid (type integer)) (local-floor (type integer)))
  (:args-generator unit-promotion-fpw-plan-arguments)
  (:returns (type list))
  (:post (and (unit-promotion-fpw-plan-geometry-p result)
              (eql (getf state :promotion-full-page-writes)
                   (getf (getf result :snapshot) :full-page-writes))
              (get-properties (getf result :snapshot) '(:full-page-writes)))))

(cl-spec:defproperty promotion-fpw-planning-keeps-frozen-policy
    ((fork (range integer 256 50000000)) (source (member :unknown nil t)) (desired (member nil t)))
  (:about physical-promotion-checkpoint-plan)
  (:trials (:normal 100))
  (let* ((state (unit-promotion-fpw-plan-state fork source desired))
         (before (copy-tree state))
         (plan (physical-promotion-checkpoint-plan state 20000 1003))
         (records (unit-promotion-fpw-plan-records plan))
         (marker (find +wal-record-full-page-writes-change+ records :key #'wal-record-type))
         (change (or (eq source :unknown) (not (eql source desired)))))
    (and (equal before state) (unit-promotion-fpw-plan-geometry-p plan)
         (= (if change 5 3) (length records))
         (eq change (not (null marker)))
         (or (not change) (eql desired (wal-full-page-writes-change-record-info marker)))
         (get-properties (getf plan :snapshot) '(:full-page-writes))
         (eql desired (getf (getf plan :snapshot) :full-page-writes)))))

(rove:deftest promotion-fpw-plan-selects-marker-and-checkpoint-policy
    (dolist (source '(:unknown nil t))
      (dolist (desired '(nil t))
        (let* ((state (unit-promotion-fpw-plan-state 216 source desired))
               (before (copy-tree state))
               (plan (physical-promotion-checkpoint-plan state 20000 1003))
               (records (unit-promotion-fpw-plan-records plan))
               (change (or (eq source :unknown) (not (eql source desired)))))
          (rove:ok (equal before state))
          (rove:ok (equal (mapcar #'wal-record-type records)
                          (if change (list +wal-record-end-of-recovery+ +wal-record-batch-start+
                                           +wal-record-full-page-writes-change+ +wal-record-batch-start+
                                           +wal-record-checkpoint+)
                              (list +wal-record-end-of-recovery+ +wal-record-batch-start+ +wal-record-checkpoint+))))
          (rove:ok (get-properties (getf plan :snapshot) '(:full-page-writes)))
          (rove:ok (eql desired (getf (getf plan :snapshot) :full-page-writes)))
          (rove:ok (equal (getf plan :snapshot) (wal-checkpoint-record-snapshot (car (last records)))))
          (when change (rove:ok (eql desired (wal-full-page-writes-change-record-info (third records)))))
          (rove:ok (= 1003 (getf (getf plan :snapshot) :next-xid)))
          (rove:ok (unit-promotion-fpw-plan-geometry-p plan))))))

(rove:deftest promotion-fpw-absent-intent-keeps-legacy-plan-and-ignores-current-configuration
    (dolist (source '(:unknown nil t))
      (let* ((state (unit-promotion-fpw-plan-state 216 source))
             (*startup-installed-configuration* nil)
             (before (physical-promotion-checkpoint-plan state 20000 1003))
             (*startup-installed-configuration* (unit-commit-ts-configuration))
             (retry (physical-promotion-checkpoint-plan state 20000 1003)))
        (rove:ok (equalp before retry))
        (rove:ok (= 3 (length (getf retry :frames))))
        (rove:ok (not (get-properties (getf retry :snapshot) '(:full-page-writes))))))
  (dolist (desired '(nil t))
    (let* ((state (unit-promotion-fpw-plan-state 216 :unknown desired))
           (*startup-installed-configuration* nil)
           (before (physical-promotion-checkpoint-plan state 20000 1003))
           (*startup-installed-configuration* (unit-commit-ts-configuration))
           (retry (physical-promotion-checkpoint-plan state 20000 1003)))
      (rove:ok (equalp before retry))
      (rove:ok (eql desired (getf (getf retry :snapshot) :full-page-writes))))))

(rove:deftest promotion-fpw-plan-rejects-invalid-policy-or-source-frontier
    (let ((state (unit-promotion-fpw-plan-state 216 t nil)))
      (setf (getf state :promotion-full-page-writes) 0)
      (rove:ok (unit-commit-ts-error-p (lambda () (physical-promotion-checkpoint-plan state 20000 1003)))))
  (dolist (pair '((:through 215) (:through-timeline 2) (:enabled 3)))
    (let ((state (unit-promotion-fpw-plan-state 216 t nil)))
      (setf (getf (getf state :source-fpw-fact) (first pair)) (second pair))
      (rove:ok (unit-commit-ts-error-p (lambda () (physical-promotion-checkpoint-plan state 20000 1003))))))
  (rove:ok (unit-commit-ts-error-p
            (lambda () (physical-promotion-checkpoint-plan
                        (unit-promotion-fpw-plan-state #xffffffffffffffc0 :unknown t) 20000 1003)))))

(rove:deftest promotion-fpw-plan-rotates-complete-metadata-records
    (dolist (fork (list (- +wal-segment-size+ 200) (- +wal-segment-size+ 168)
                        (- +wal-segment-size+ 136) (- +wal-segment-size+ 104)
                        (- +wal-segment-size+ 56) (- +wal-segment-size+ 55)
                        +wal-segment-size+ (1- (* 2 +wal-segment-size+))))
      (dolist (source '(:unknown nil t))
        (dolist (desired '(nil t))
          (rove:ok (unit-promotion-fpw-plan-geometry-p
                    (physical-promotion-checkpoint-plan (unit-promotion-fpw-plan-state fork source desired) 20000 1003)))))))

(rove:deftest promotion-fpw-plan-generated-contracts
    (dolist (result (list (cl-spec:result-data
                           (cl-spec:check-function 'physical-promotion-checkpoint-plan :trials 100 :seed 265))
                          (cl-spec:result-data
                           (cl-spec:run-property 'promotion-fpw-planning-keeps-frozen-policy :seed 266))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))))

(rove:deftest promotion-fpw-policy-captures-one-installed-root
    (let ((*startup-installed-configuration* nil))
      (rove:ok (null (startup-promotion-fpw-policy-arguments))))
  (let* ((*startup-installed-configuration* (unit-commit-ts-configuration))
         (arguments (startup-promotion-fpw-policy-arguments)))
    (rove:ok (equal arguments '(:promotion-full-page-writes t)))
    (setf (getf arguments :promotion-full-page-writes) nil)
    (rove:ok (equal (startup-promotion-fpw-policy-arguments) '(:promotion-full-page-writes t))))
  (let ((capture (symbol-function 'startup-capture-configuration))
        (configuration (unit-commit-ts-configuration)) (calls 0))
    (unwind-protect
         (progn
           (setf (symbol-function 'startup-capture-configuration)
                 (lambda () (incf calls) configuration))
           (rove:ok (equal (startup-promotion-fpw-policy-arguments) '(:promotion-full-page-writes t)))
           (rove:ok (= calls 1)))
      (setf (symbol-function 'startup-capture-configuration) capture)))
  (let ((*startup-installed-configuration* (make-startup-configuration :settings nil)))
    (rove:ok (unit-commit-ts-error-p #'startup-promotion-fpw-policy-arguments))))

(rove:deftest promotion-fpw-source-fact-is-bound-to-selected-generation
    (let* ((fact (unit-source-fpw-fact :enabled nil))
           (metadata (list :lsn (getf fact :through) :timeline (getf fact :through-timeline)
                           :source-fpw-fact fact))
           (control (list :fpw-fact (copy-list fact)))
           (restored (physical-promotion-frozen-fpw-fact control metadata)))
      (rove:ok (equal fact restored))
      (rove:ok (not (eq fact restored)))
      (setf (getf restored :enabled) t)
      (rove:ok (null (getf fact :enabled)))
      (rove:ok (equal fact (physical-promotion-frozen-fpw-fact nil metadata)))
      (rove:ok (null (physical-promotion-frozen-fpw-fact nil (list :lsn 256 :timeline 1))))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-promotion-frozen-fpw-fact control (list :lsn 256 :timeline 1)))))
      (dolist (key '(:lsn :timeline))
        (let ((bad (copy-list metadata)))
          (incf (getf bad key))
          (rove:ok (unit-commit-ts-error-p
                    (lambda () (physical-promotion-frozen-fpw-fact control bad))))))
      (setf (getf (getf control :fpw-fact) :enabled) t)
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-promotion-frozen-fpw-fact control metadata))))))

(rove:deftest promotion-fpw-pending-freezes-source-and-explicit-intent
    (dolist (source '(nil t :unknown))
      (dolist (desired '(nil t))
        (let* ((fact (unless (eq source :unknown) (unit-source-fpw-fact :enabled source)))
               (state (physical-recovery-decode-control
                       (unit-promotion-fpw-image nil :intent-p nil)))
               (metadata (list :lsn 80 :next-xid 3 :timeline 1
                               :source-fpw-fact fact
                               :source-reservation '(:version 1 :through 80 :bound 1002 :complete t)))
               (before (copy-tree metadata)))
          (multiple-value-bind (bytes pending)
              (physical-promotion-make-pending
               state metadata (make-array 0 :element-type '(unsigned-byte 8))
               9 :legacy 4 0 nil :promotion-full-page-writes desired)
            (let ((control (physical-recovery-decode-control bytes)))
              (rove:ok (equal before metadata))
              (rove:ok (get-properties control '(:promotion-full-page-writes)))
              (rove:ok (eql desired (getf control :promotion-full-page-writes)))
              (rove:ok (equal fact (getf control :fpw-fact)))
              (rove:ok (equal fact (getf pending :source-fpw-fact)))
              (rove:ok (equal fact (getf pending :required-fpw-fact)))
              (rove:ok (equalp
                        (getf (physical-promotion-checkpoint-plan pending 20000 1003) :frames)
                        (getf (physical-promotion-checkpoint-plan (copy-tree pending) 20000 1003) :frames)))
              (when fact
                (setf (getf fact :through) 81)
                (rove:ok (= 80 (getf (getf pending :source-fpw-fact) :through)))
                (setf (getf (getf pending :source-fpw-fact) :through) 82)
                (rove:ok (= 80 (getf (getf pending :required-fpw-fact) :through))))))))))
