(in-package #:pg)

(defun unit-source-fpw-control (fact &key resources selected pending retained)
  (physical-recovery-control-image
   (if pending 3 2) 42 1 4 32 80 3 :state-time 0
   :applied-timeline 1 :history-target (when selected 3) :history-digest (when selected 7)
   :promotion-child (when pending 4) :promotion-generation (when pending 9)
   :promotion-source-namespace (when pending :legacy)
   :retained-reference (when retained '(:root-id 1 :base-timeline 1 :base 32
                                        :manifest-digest 4 :end 80 :end-digest 7))
   :resource-fact (when resources
                    '(:resource-parameters (100 8 10 0 128) :resource-lsn 16 :resource-timeline 1))
   :fpw-fact fact))

(rove:deftest source-fpw-recovery-control-optional-fact-roundtrip
    (let ((legacy (make-array 72 :element-type '(unsigned-byte 8)
                                 :initial-contents '(80 71 82 83 84 65 84 51 2 0 0 0 1 0 0 0
                                                     42 0 0 0 0 0 0 0 4 0 0 0 0 0 0 0
                                                     32 0 0 0 0 0 0 0 80 0 0 0 0 0 0 0
                                                     3 0 0 0 0 0 0 0 5 0 0 0 0 0 0 0
                                                     0 0 0 0 59 253 186 22))))
      (rove:ok (equalp legacy (unit-source-fpw-control nil)))
      (rove:ok (null (get-properties (physical-recovery-decode-control legacy) '(:fpw-fact)))))
  (dolist (pending '(nil t))
    (dolist (selected '(nil t))
      (dolist (resources '(nil t))
        (dolist (retained (if pending '(nil t) '(nil)))
          (dolist (fact (list nil (unit-source-fpw-fact) (unit-source-fpw-fact :enabled t)))
            (let* ((bytes (unit-source-fpw-control fact :pending pending :selected selected
                                                        :resources resources :retained retained))
                   (control (physical-recovery-decode-control bytes))
                   (base (+ (if pending 88 72) (if selected 16 0)
                            (if resources 48 0) (if retained 44 0))))
              (rove:ok (= (+ base (if fact 72 0)) (length bytes)))
              (rove:ok (= (if pending 4 3) (getf control :control-version)))
              (rove:ok (equal fact (getf control :fpw-fact)))
              (rove:ok (= 80 (getf control :applied)))
              (rove:ok (eql (not (null resources)) (not (null (getf control :resource-fact)))))
              (when fact
                (rove:ok (not (eq fact (getf control :fpw-fact))))
                (rove:ok (eql (getf fact :enabled) (getf (getf control :fpw-fact) :enabled)))))))))))

(rove:deftest source-fpw-recovery-control-rejects-corruption-and-stale-facts
    (let ((fact (unit-source-fpw-fact)))
      (dolist (pair '((:through 79) (:through 81) (:through-timeline 2)))
        (let ((bad (copy-list fact)))
          (setf (getf bad (first pair)) (second pair))
          (rove:ok (unit-commit-ts-error-p (lambda () (unit-source-fpw-control bad))))))
      (let* ((claimed (unit-source-fpw-fact :enabled t))
             (history (timeline-parse-history 3 (format nil "1 0/A0 ancestor~%"))))
        (setf (getf claimed :witness-timeline) 2
              (getf claimed :through) 256 (getf claimed :through-timeline) 3)
        (let* ((bytes (physical-recovery-control-image
                       2 42 1 4 32 256 3 :state-time 0 :applied-timeline 3
                       :history-target 3 :history-digest 7 :fpw-fact claimed))
               (decoded (getf (physical-recovery-decode-control bytes) :fpw-fact)))
          (rove:ok (physical-fpw-fact-valid-p claimed))
          (rove:ok (equal claimed decoded))
          (rove:ok (unit-commit-ts-error-p
                    (lambda () (physical-fpw-check-fact decoded history 256 3))))))
      (let ((bytes (unit-source-fpw-control fact)))
        (dolist (offset '(0 8 68 76 80 84 92 100 104 108 116 124 128 132 140))
          (let ((bad (copy-seq bytes)))
            (setf (aref bad offset) (logxor 1 (aref bad offset)))
            (rove:ok (unit-commit-ts-error-p (lambda () (physical-recovery-decode-control bad))))))
        (let ((bad (copy-seq bytes)))
          (wal-set-u64 bad (+ 68 48) 81)
          (wal-set-u32 bad (- (length bad) 4) 0)
          (wal-set-u32 bad (- (length bad) 4) (wal-crc32 bad))
          (rove:ok (unit-commit-ts-error-p (lambda () (physical-recovery-decode-control bad))))))))

(defun unit-source-fpw-checkpoint-fact (start through &key (coverage start) disabled)
  (list :enabled t :witness-kind :checkpoint :witness-start start :witness-end (+ start 120)
        :witness-timeline 1 :coverage-start coverage :coverage-timeline 1
        :through through :through-timeline 1 :last-disable-start disabled
        :last-disable-timeline (when disabled 1)))

(rove:deftest source-fpw-required-frontier-preserves-and-extends-evidence
    (let* ((required (unit-source-fpw-checkpoint-fact 192 312))
           (stronger (unit-source-fpw-checkpoint-fact 192 312 :coverage 32 :disabled 32))
           (later (copy-list stronger)))
      (rove:ok (equal required (physical-fpw-required-fact nil required 80 1 312 1)))
      (rove:ok (equal stronger (physical-fpw-required-fact stronger required 312 1 312 1)))
      (rove:ok (equal required (unit-source-fpw-checkpoint-fact 192 312)))
      (setf (getf later :through) 392)
      (rove:ok (equal later (physical-fpw-required-fact later required 392 1 312 1)))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-fpw-required-fact nil required 312 1 312 1))))
      (let ((weaker (unit-source-fpw-checkpoint-fact 224 400)))
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (physical-fpw-required-fact weaker required 400 1 312 1)))))
      (let ((invented (unit-source-fpw-checkpoint-fact 256 400 :coverage 32 :disabled 192)))
        (rove:ok (physical-fpw-fact-valid-p invented))
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (physical-fpw-required-fact invented required 400 1 312 1))))))
  (let* ((required (unit-source-fpw-fact))
         (current (copy-list required)))
    (setf (getf current :through) 160)
    (rove:ok (equal current (physical-fpw-required-fact current required 160 1 80 1)))
    (setf (getf current :last-disable-start) nil (getf current :last-disable-timeline) nil
          (getf current :enabled) t)
    (rove:ok (unit-commit-ts-error-p
              (lambda () (physical-fpw-required-fact current required 160 1 80 1))))
    (let ((new (unit-source-fpw-fact :start 112)))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-fpw-required-fact new required 160 1 80 1))))
      (setf (getf new :coverage-start) 32)
      (rove:ok (equal new (physical-fpw-required-fact new required 160 1 80 1))))
    (let ((lost (unit-source-fpw-fact :enabled t :start 112)))
      (setf (getf lost :coverage-start) 32)
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-fpw-required-fact lost required 160 1 80 1))))
      (setf (getf lost :last-disable-start) 32 (getf lost :last-disable-timeline) 1)
      (rove:ok (equal lost (physical-fpw-required-fact lost required 160 1 80 1))))
    (rove:ok (unit-commit-ts-error-p
              (lambda () (physical-fpw-required-fact required required 80 3 80 1))))))

(rove:deftest source-fpw-required-frontier-rejects-disable-in-promised-interval
    (let* ((required (unit-source-fpw-checkpoint-fact 192 312))
           (candidate (unit-source-fpw-fact :enabled t :start 392)))
      (setf (getf candidate :coverage-start) 192
            (getf candidate :last-disable-start) 256 (getf candidate :last-disable-timeline) 1)
      (rove:ok (physical-fpw-fact-valid-p candidate))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-fpw-required-fact candidate required 440 1 312 1))))
      (setf (getf candidate :last-disable-start) 328)
      (rove:ok (physical-fpw-fact-valid-p candidate))
      (rove:ok (equal candidate (physical-fpw-required-fact candidate required 440 1 312 1)))))

(rove:deftest source-fpw-save-progress-copies-fact-and-installs-only-after-publication
    (let* ((old (unit-source-fpw-fact))
           (next (copy-list old))
           (*physical-recovery-state* (list :timeline 1 :source-fpw-fact old))
           (input *physical-recovery-state*)
           (before (copy-tree input))
           (writer (symbol-function 'physical-recovery-write-control))
           (seen nil)
           (fail nil))
      (setf (getf next :through) 160)
      (unwind-protect
           (progn
             (setf (symbol-function 'physical-recovery-write-control)
                   (lambda (candidate phase applied next-xid)
                     (rove:ok (= phase 2))
                     (rove:ok (= applied 160))
                     (rove:ok (= next-xid 3))
                     (setf seen candidate)
                     (when fail (error "injected control publication failure"))
                     candidate))
             (physical-recovery-save-progress 160 3 :fpw-fact next)
             (rove:ok (equal next (getf seen :source-fpw-fact)))
             (rove:ok (not (eq next (getf seen :source-fpw-fact))))
             (rove:ok (equal before input))
             (rove:ok (eq seen *physical-recovery-state*))
             (setf *physical-recovery-state* input fail t)
             (rove:ok (unit-commit-ts-error-p
                       (lambda () (physical-recovery-save-progress 160 3 :fpw-fact next))))
             (rove:ok (eq input *physical-recovery-state*))
             (rove:ok (equal before input)))
        (setf (symbol-function 'physical-recovery-write-control) writer))))

(rove:deftest source-fpw-progress-arguments-preserve-unknown-and-explicit-clear
    (dolist (old-resource '(nil t))
      (dolist (old-fpw '(nil t))
        (dolist (new-resource '(nil t))
          (dolist (new-fpw '(nil t))
            (let* ((resource '(:resource-parameters (100 8 10 0 64) :resource-lsn 16 :resource-timeline 1))
                   (fpw (unit-source-fpw-fact))
                   (*physical-recovery-state*
                     (list :source-resource-fact (when old-resource resource)
                           :source-fpw-fact (when old-fpw fpw)))
                   (before (copy-tree *physical-recovery-state*))
                   (arguments (physical-replay-fact-arguments
                               (when new-resource resource) (when new-fpw fpw))))
              (rove:ok (eql (or old-resource new-resource)
                            (not (null (get-properties arguments '(:resource-fact))))))
              (rove:ok (eql (or old-fpw new-fpw)
                            (not (null (get-properties arguments '(:fpw-fact))))))
              (rove:ok (equal (when new-resource resource) (getf arguments :resource-fact)))
              (rove:ok (equal (when new-fpw fpw) (getf arguments :fpw-fact)))
              (rove:ok (equal before *physical-recovery-state*))))))))
