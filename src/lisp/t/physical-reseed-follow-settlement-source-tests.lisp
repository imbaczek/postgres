(in-package #:pg-physical-tests)

(defun reseed-follow-settlement-source-fixture (phase outcome)
  (multiple-value-bind (record pending receipt) (reseed-follow-settlement-fixture)
    (let ((source (pg::physical-backup-lease-copy receipt)))
      (setf (getf source :phase) phase (getf source :outcome) outcome)
      (unless (member phase '(:closing :closed))
        (setf (getf source :outcome) nil (getf source :successor) nil))
      (when (eq outcome :aborted) (setf (getf source :successor) nil))
      (when (member phase '(:requested :capturing)) (setf (getf source :base) nil))
      (values (pg::physical-reseed-follow-copy (append record (list :settlement pending)))
              (pg::physical-backup-lease-copy source)))))

(deftest reseed-follow-settlement-source-decisions-separate-observation-from-handoff
    (dolist (phase '(:requested :capturing :ready :interrupted :closing :closed))
      (dolist (active '(nil t))
        (multiple-value-bind (record source)
            (reseed-follow-settlement-source-fixture phase (when (member phase '(:closing :closed)) :handed-off))
          (let ((expected (cond ((eq phase :closed) :terminal)
                                ((and active (member phase '(:ready :interrupted :closing))) :handoff)
                                (t :wait))))
            (multiple-value-bind (action copy)
                (pg::physical-reseed-follow-settlement-source-action record source active)
              (ok (eq expected action))
              (ok (equal source copy))
              (setf (getf source :phase) :requested)
              (ok (eq phase (getf copy :phase))))))))
  (multiple-value-bind (record source) (reseed-follow-settlement-source-fixture :closed :aborted)
    (ok (eq :terminal (pg::physical-reseed-follow-settlement-source-action record source nil))))
  (multiple-value-bind (record source) (reseed-follow-settlement-source-fixture :closing :aborted)
    (ok (eq :wait (pg::physical-reseed-follow-settlement-source-action record source t)))
    (ok (eq :wait (pg::physical-reseed-follow-settlement-source-action record nil t)))))

(deftest reseed-follow-settlement-source-decisions-reject-different-authority
    (dolist (mutation '(:request :base :successor))
      (multiple-value-bind (record source) (reseed-follow-settlement-source-fixture :closing :handed-off)
        (ecase mutation
          (:request (setf (getf (getf source :request) :token) (make-string 64 :initial-element #\b)))
          (:base (incf (getf (getf source :base) :manifest-digest)))
          (:successor (incf (getf (getf source :successor) :anchor-digest))))
        (ok (handler-case
                (progn (pg::physical-reseed-follow-settlement-source-action record source nil) nil)
              (error () t))))))

(cl-spec:defproperty reseed-follow-settlement-source-decisions-never-handoff-after-abort
    ((phase-index (range integer 0 5)))
  (:about pg::physical-reseed-follow-settlement-source-action)
  (:trials (:normal 80))
  (let ((phase (nth phase-index '(:requested :capturing :ready :interrupted :closing :closed))))
    (multiple-value-bind (record source)
        (reseed-follow-settlement-source-fixture phase (when (member phase '(:closing :closed)) :handed-off))
      (not (eq :handoff (pg::physical-reseed-follow-settlement-source-action record source nil))))))

(deftest generated-reseed-follow-settlement-source-observation-authority
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-settlement-source-decisions-never-handoff-after-abort
                                       :seed 125 :profile :normal))))
      (ok (eq :passed (getf data :status)))))
