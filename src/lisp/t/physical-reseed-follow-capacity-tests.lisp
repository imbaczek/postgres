(in-package #:pg-physical-tests)

(defun reseed-follow-capacity-states (selected)
  (multiple-value-bind (record activation) (reseed-follow-activation-codec-fixture selected)
    (multiple-value-bind (active retirement) (reseed-follow-retirement-codec-fixture selected)
      (append (list record)
              (mapcar (lambda (group) (reseed-follow-with-activation record group)) activation)
              (mapcar (lambda (group) (reseed-follow-with-retirement active group)) retirement)))))

(defun reseed-follow-capacity-outcome (record phase)
  (let* ((copy (pg::physical-adoption-attempt-value-copy record))
         (settlement (getf copy :settlement))
         (receipt (getf settlement :receipt)))
    (unless (eq phase :handed-off)
      (remf copy :activation)
      (remf copy :retirement)
      (setf (getf settlement :phase) phase)
      (if (eq phase :handoff-pending)
          (setf (getf settlement :receipt) nil)
          (setf (getf receipt :outcome) :aborted (getf receipt :successor) nil)))
    (pg::physical-reseed-follow-copy copy)))

(cl-spec:defproperty reseed-follow-future-capacity-covers-completion
    ((selected boolean) (state (range integer 0 10)) (branch (range integer 0 2)))
  (:about pg::physical-reseed-follow-future-completion-size-bound)
  (:trials (:normal 100))
  (let* ((states (reseed-follow-capacity-states selected))
         (record (reseed-follow-capacity-outcome
                  (nth state states) (nth branch '(:handoff-pending :handed-off :source-aborted))))
         (before (pg::physical-adoption-attempt-value-copy record))
         (bound (funcall (symbol-function 'pg::physical-reseed-follow-future-completion-size-bound) record))
         (candidates (if (= branch 2) (list record) states)))
    (and (integerp bound)
         (every (lambda (candidate) (<= (length (pg::physical-reseed-follow-image candidate)) bound))
                candidates)
         (equal before record))))

(deftest generated-reseed-follow-future-capacity-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-future-capacity-covers-completion
                                       :seed 130 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-future-capacity-exact-fit-and-legacy-image
    (dolist (selected '(nil t))
      (dolist (record (reseed-follow-capacity-states selected))
        (let* ((before (pg::physical-reseed-follow-image record))
               (bound (funcall (symbol-function 'pg::physical-reseed-follow-future-completion-size-bound)
                               record)))
          (ok (funcall (symbol-function 'pg::physical-reseed-follow-check-future-capacity) record bound))
          (ok (handler-case
                  (progn (funcall (symbol-function 'pg::physical-reseed-follow-check-future-capacity)
                                  record (1- bound)) nil)
                (error () t)))
          (ok (equalp before (pg::physical-reseed-follow-image record)))
          (ok (equal record (pg::physical-reseed-follow-decode before)))))))

(deftest reseed-follow-future-capacity-ignores-caller-printer-bindings
    (let* ((record (first (reseed-follow-capacity-states t)))
           (bound (pg::physical-reseed-follow-future-completion-size-bound record))
           (actual (let ((*print-base* 16) (*print-radix* t) (*print-case* :downcase)
                                           (*print-pretty* t) (*print-length* 1) (*print-level* 1))
                     (pg::physical-reseed-follow-future-completion-size-bound record))))
      (ok (= bound actual))))

(deftest reseed-follow-future-capacity-preserves-exact-terminal-sizing
    (let* ((record (reseed-follow-capacity-outcome
                    (first (reseed-follow-capacity-states nil)) :handoff-pending))
           (exact (pg::physical-reseed-follow-settlement-terminal-size record))
           (bound (funcall (symbol-function 'pg::physical-reseed-follow-future-completion-size-bound)
                           record)))
      (ok (> bound (+ exact 32)))
      (ok (pg::physical-reseed-follow-check-image-capacity
           record (length (pg::physical-reseed-follow-image record)) (+ exact 32)))
      (ok (= exact (pg::physical-reseed-follow-settlement-terminal-size record)))))

(defun call-with-reseed-insufficient-capacity (function)
  (let* ((symbol 'pg::physical-reseed-follow-check-future-capacity)
         (original (symbol-function symbol))
         (calls 0))
    (unwind-protect
         (progn
           (setf (symbol-function symbol)
                 (lambda (record &optional maximum)
                   (declare (ignore maximum))
                   (incf calls)
                   (funcall original record
                            (1- (pg::physical-reseed-follow-future-completion-size-bound record)))))
           (funcall function (lambda () calls)))
      (setf (symbol-function symbol) original))))

(deftest reseed-follow-future-capacity-refuses-before-pending-temporary
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore workspace))
       (let ((before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (call-with-reseed-insufficient-capacity
          (lambda (calls)
            (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-begin-settlement owner))))
            (ok (plusp (funcall calls)))
            (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
            (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))))

(deftest reseed-follow-future-capacity-gates-handoff-but-allows-historical-status
    (dolist (outcome '(:handed-off :aborted))
      (call-with-reseed-inert-settlement-fixture
       :same-timeline
       (lambda (owner root workspace)
         (declare (ignore workspace))
         (let ((pending (pg::physical-reseed-follow-begin-settlement owner)))
           (call-with-reseed-settlement-source
            owner root pending
            (lambda (state)
              (call-with-reseed-insufficient-capacity
               (lambda (calls)
                 (ok (intake-fails-p
                      (lambda () (pg::physical-reseed-follow-settle-remote owner :helper "/unused/helper"))))
                 (ok (plusp (funcall calls)))
                 (ok (= 1 (getf state :statuses)))
                 (ok (zerop (getf state :handoffs)))
                 (ok (= 1 (getf state :closes)))
                 (ok (equal pending (pg::physical-reseed-follow-read owner)))
                 (setf (getf state :phase) :closed (getf state :outcome) outcome)
                 (let* ((previous (funcall calls))
                        (terminal (pg::physical-reseed-follow-settle owner :helper "/unused/helper")))
                   (ok (= previous (funcall calls)))
                   (ok (eq (if (eq outcome :handed-off) :handed-off :source-aborted)
                           (getf (getf terminal :settlement) :phase)))
                   (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))))))))))

(deftest reseed-follow-future-capacity-covers-extreme-widths
    (dolist (selected '(nil t))
      (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture selected)
        (let* ((wide (1- (ash 1 64)))
               (image (getf (getf record :activation) :image))
               (entry (getf (getf record :activation) :entry)))
          (setf (first image) pg::+physical-follow-config-maximum-bytes+
                (second image) wide
                (fifth entry) wide
                (sixth entry) (first image)
                (seventh entry) wide)
          (dolist (group groups)
            (let ((identity (getf (getf group :attempt-file) :identity)))
              (setf (second identity) (1- wide)
                    (fourth identity) wide
                    (seventh identity) (- (ash 1 63))
                    (eighth identity) (- (ash 1 63))))
            (let* ((candidate (reseed-follow-with-retirement record group))
                   (bound (pg::physical-reseed-follow-future-completion-size-bound candidate)))
              (ok (<= (+ 32 (length (pg::physical-reseed-follow-image candidate))) bound))))))))

(deftest reseed-follow-future-capacity-allows-old-complete-pending-recovery
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace)
       (declare (ignore root workspace))
       (let ((pg::*physical-reseed-follow-test-hook*
               (lambda (event path)
                 (declare (ignore path))
                 (when (eq event :after-follow-temporary-readback)
                   (error "pending capacity recovery cut")))))
         (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-begin-settlement owner))))))
     (lambda (root workspace)
       (declare (ignore workspace))
       (pg::call-with-physical-reseed-follow-configuration-owner
        root
        (lambda (owner)
          (call-with-reseed-insufficient-capacity
           (lambda (calls)
             (let ((record (pg::physical-reseed-follow-recover-configuration owner)))
               (ok (eq :handoff-pending (getf (getf record :settlement) :phase)))
               (ok (zerop (funcall calls)))
               (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))))))))

(deftest reseed-follow-future-capacity-covers-every-completion-from-each-prefix
    (dolist (selected '(nil t))
      (let ((states (reseed-follow-capacity-states selected)))
        (dolist (record states)
          (let ((bound (pg::physical-reseed-follow-future-completion-size-bound record)))
            (ok (every (lambda (candidate)
                         (<= (+ 32 (length (pg::physical-reseed-follow-image candidate))) bound))
                       states)))))))
