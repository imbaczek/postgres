(in-package #:pg-physical-tests)

(defun reseed-follow-activation-config-fixture (selected)
  (multiple-value-bind (record binding) (reseed-follow-configuration-plan-fixture selected)
    (let* ((configuration (getf record :configuration))
           (successor (pg::physical-reseed-follow-settlement-successor record))
           (request (getf (getf record :attempt) :request)))
      (setf (getf configuration :phase) :inert
            (getf configuration :step) (if selected 5 4)
            (getf record :settlement)
            (list :phase :handed-off :successor successor
                  :receipt (list :version 1 :request request :phase :closed
                                 :base (pg::physical-reseed-follow-settlement-base record)
                                 :outcome :handed-off :successor successor)))
      (values (pg::physical-reseed-follow-copy record) binding))))

(cl-spec:defproperty reseed-follow-activation-config-preserves-frozen-fields
    ((selected boolean))
  (:about pg::physical-reseed-follow-activation-config)
  (:trials (:normal 80))
  (multiple-value-bind (record binding) (reseed-follow-activation-config-fixture selected)
    (let* ((before (pg::physical-adoption-attempt-value-copy (list record binding)))
           (expected (pg::physical-reseed-follow-pending-config record binding)))
      (setf (getf (fifth expected) :phase) :active
            (getf (sixth expected) :enabled) t)
      (let ((actual (funcall (symbol-function 'pg::physical-reseed-follow-activation-config)
                             record binding)))
        (and (equal expected actual)
             (equal before (list record binding))
             (equal actual (pg::physical-follow-decode-config
                            (pg::physical-follow-config-image actual))))))))

(cl-spec:defproperty reseed-follow-activation-config-owns-returned-values
    ((selected boolean))
  (:about pg::physical-reseed-follow-activation-config)
  (:trials (:normal 80))
  (multiple-value-bind (record binding) (reseed-follow-activation-config-fixture selected)
    (let* ((before (pg::physical-adoption-attempt-value-copy (list record binding)))
           (actual (funcall (symbol-function 'pg::physical-reseed-follow-activation-config)
                            record binding))
           (child (getf (fifth actual) :child-history))
           (anchor (getf (fifth actual) :anchor))
           (eor (getf anchor :eor-octets)))
      (labels ((damage-strings (value)
                 (typecase value
                   (string (when (plusp (length value)) (setf (char value 0) #\x)))
                   (cons (damage-strings (car value)) (damage-strings (cdr value))))))
        (damage-strings (getf (sixth actual) :source))
        (damage-strings anchor))
      (setf (char (fourth actual) 0) #\x)
      (incf (first (getf (fifth actual) :required)))
      (when (and (stringp child) (plusp (length child))) (setf (char child 0) #\x))
      (when eor (setf (elt eor 0) (logxor 1 (elt eor 0))))
      (equal before (list record binding)))))

(deftest generated-reseed-follow-activation-config-contract
    (dolist (case '((reseed-follow-activation-config-preserves-frozen-fields 126)
                    (reseed-follow-activation-config-owns-returned-values 127)))
      (let ((data (cl-spec:result-data
                   (cl-spec:run-property (first case) :seed (second case) :profile :normal))))
        (ok (eq :passed (getf data :status))))))

(deftest reseed-follow-activation-config-refuses-unsettled-outcomes
    (assert (fboundp 'pg::physical-reseed-follow-activation-config))
  (dolist (phase '(nil :handoff-pending :source-aborted))
    (multiple-value-bind (record binding) (reseed-follow-activation-config-fixture nil)
      (if phase
          (let ((settlement (getf record :settlement)))
            (setf (getf settlement :phase) phase)
            (if (eq phase :handoff-pending)
                (setf (getf settlement :receipt) nil)
                (setf (getf (getf settlement :receipt) :outcome) :aborted
                      (getf (getf settlement :receipt) :successor) nil)))
          (remf record :settlement))
      (ok (pg::physical-reseed-follow-copy record))
      (ok (handler-case
              (progn (funcall (symbol-function 'pg::physical-reseed-follow-activation-config)
                              record binding) nil)
            (error () t))))))

(deftest reseed-follow-activation-config-refuses-rebound-pending-image
    (assert (fboundp 'pg::physical-reseed-follow-activation-config))
  (multiple-value-bind (record binding) (reseed-follow-activation-config-fixture t)
    (let* ((other (pg::physical-adoption-attempt-value-copy binding))
           (after (getf other :after))
           (text (concatenate 'string (getf other :child-history)
                              (string #\Newline) "# alternative child history" (string #\Newline))))
      (setf (getf other :child-history) text
            (ninth after) (pg::physical-follow-binding-selection-digest
                           (getf other :anchor) (eighth after) text
                           (getf other :before) (fifth after)))
      (ok (pg::physical-follow-binding-check other))
      (ok (equal (getf other :anchor) (getf binding :anchor)))
      (ok (handler-case
              (progn (funcall (symbol-function 'pg::physical-reseed-follow-activation-config)
                              record other) nil)
            (error () t))))))
