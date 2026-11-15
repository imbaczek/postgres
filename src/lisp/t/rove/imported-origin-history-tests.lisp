(in-package #:pg)

(defun unit-imported-origin-history (base delta depth)
  (let* ((older (1+ base)) (fork (+ older 100 delta))
                           (parent (timeline-encode-history 3 1 older "older" nil))
                           (original (timeline-encode-history 5 3 fork "origin" parent))
                           (current original) (tip 5) (end fork))
    (dotimes (step depth)
      (incf end (+ 1 delta))
      (setf current (timeline-encode-history (+ tip 2) tip end "descendant" current))
      (incf tip 2))
    (values original (timeline-parse-history tip (bytes->string current)) fork parent)))

(cl-spec:defproperty imported-origin-history-keeps-ancestor-intervals
    ((base (range integer 0 128)) (delta (range integer 0 128)) (depth (range integer 0 8)))
  (:about logical-slot-imported-check-origin-history)
  (:trials (:normal 200))
  (multiple-value-bind (bytes history fork) (unit-imported-origin-history base delta depth)
    (let ((before-bytes (copy-seq bytes))
          (before-history (mapcar #'copy-timeline-history-entry history)))
      (and (logical-slot-imported-check-origin-history 5 3 fork bytes history)
           (equalp before-bytes bytes)
           (equalp before-history history)))))

(cl-spec:defproperty imported-origin-history-refuses-sibling-branches
    ((base (range integer 0 128)) (delta (range integer 0 128)))
  (:about logical-slot-imported-check-origin-history)
  (:trials (:normal 200))
  (multiple-value-bind (bytes history fork parent) (unit-imported-origin-history base delta 0)
    (declare (ignore history))
    (let ((sibling (timeline-parse-history
                    7 (bytes->string (timeline-encode-history 7 3 fork "sibling" parent)))))
      (handler-case
          (progn (logical-slot-imported-check-origin-history 5 3 fork bytes sibling) nil)
        (error () t)))))

(rove:deftest imported-origin-history-generated-contracts
    (dolist (property '(imported-origin-history-keeps-ancestor-intervals
                        imported-origin-history-refuses-sibling-branches))
      (let ((result (cl-spec:result-data
                     (cl-spec:run-property property :seed 49310 :profile :normal))))
        (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
        (rove:ok (= 200 (getf result :trials))))))

(rove:deftest imported-origin-history-refuses-rewritten-forks
    (multiple-value-bind (bytes history fork parent) (unit-imported-origin-history 9 4 2)
      (declare (ignore parent))
      (rove:ok (logical-slot-imported-check-origin-history 5 3 fork bytes history))
      (dolist (mode '(:fork :earlier :parent :child))
        (let ((changed (mapcar #'copy-timeline-history-entry history)))
          (ecase mode
            (:fork
             (let ((child (find 5 changed :key #'timeline-history-entry-timeline))
                   (older (find 3 changed :key #'timeline-history-entry-timeline)))
               (incf (timeline-history-entry-begin child))
               (incf (timeline-history-entry-end older))))
            (:earlier
             (let ((middle (find 3 changed :key #'timeline-history-entry-timeline))
                   (oldest (find 1 changed :key #'timeline-history-entry-timeline)))
               (incf (timeline-history-entry-begin middle))
               (incf (timeline-history-entry-end oldest))))
            (:parent (setf (timeline-history-entry-timeline
                            (find 3 changed :key #'timeline-history-entry-timeline)) 2))
            (:child (setf (timeline-history-entry-timeline
                           (find 5 changed :key #'timeline-history-entry-timeline)) 4)))
          (rove:ok (rove:signals
                    (logical-slot-imported-check-origin-history 5 3 fork bytes changed) 'error))))))

(rove:deftest imported-origin-history-bounds-malformed-observations
    (multiple-value-bind (bytes history fork) (unit-imported-origin-history 0 0 1)
      (dolist (mode '(:empty :cycle :improper :entry :float))
        (let ((changed (mapcar #'copy-timeline-history-entry history)))
          (ecase mode
            (:empty (setf changed nil))
            (:cycle (setf (cdr (last changed)) changed))
            (:improper (setf (cdr (last changed)) :bad))
            (:entry (setf (first changed) :bad))
            (:float (setf (timeline-history-entry-begin (first changed)) 1d0)))
          (rove:ok (rove:signals
                    (logical-slot-imported-check-origin-history 5 3 fork bytes changed) 'error))))
      (dolist (parameters (list (list 5 3 (1+ fork) bytes)
                                (list 5 2 fork bytes)
                                (list 4 3 fork bytes)
                                (list 5 3 fork (string->bytes "invalid history"))))
        (rove:ok (rove:signals
                  (apply #'logical-slot-imported-check-origin-history
                         (append parameters (list history))) 'error)))))
