(in-package #:pg-physical-tests)

(defun rejoin-history (tip &rest splits)
  (pg::timeline-parse-history
   tip
   (with-output-to-string (stream)
     (loop for (timeline position) in splits
           do (format stream "~d~c~a~cfixture~%" timeline #\Tab
                      (pg::wal-format-lsn position) #\Tab)))))

(deftest rejoin-common-ancestor-matches-c-forward-history-walk
    (dolist (scenario
             (list (list (rejoin-history 1) (rejoin-history 1) '(1 nil))
                   (list (rejoin-history 1) (rejoin-history 4 '(1 100) '(2 200) '(3 300)) '(1 100))
                   (list (rejoin-history 2 '(1 100))
                         (rejoin-history 4 '(1 100) '(2 200) '(3 300)) '(2 200))
                   (list (rejoin-history 3 '(1 100) '(2 220))
                         (rejoin-history 4 '(1 100) '(2 200)) '(2 200))
                   (list (rejoin-history 3 '(1 100) '(2 220))
                         (rejoin-history 4 '(1 150) '(2 250)) '(1 100))
                   (list (rejoin-history 3 '(1 100) '(2 200))
                         (rejoin-history 4 '(1 100) '(2 200)) '(2 200))
                   (list (rejoin-history 2 '(1 100)) (rejoin-history 2 '(1 150)) '(1 100))))
      (destructuring-bind (left right expected) scenario
        (ok (equal expected (multiple-value-list (pg::timeline-common-ancestor left right))))
        (ok (equal expected (multiple-value-list (pg::timeline-common-ancestor right left)))))))

(deftest rejoin-common-ancestor-does-not-change-the-input-lineages
    (let* ((left (rejoin-history 3 '(1 100) '(2 200)))
           (right (rejoin-history 4 '(1 100) '(2 250)))
           (saved-left (mapcar #'copy-structure left))
           (saved-right (mapcar #'copy-structure right)))
      (pg::timeline-common-ancestor left right)
      (ok (equalp saved-left left))
      (ok (equalp saved-right right))))

(deftest rejoin-common-ancestor-refuses-unrelated-or-invalid-history
    (ok (intake-fails-p
         (lambda () (pg::timeline-common-ancestor (rejoin-history 1) (rejoin-history 2)))))
  (let ((broken (rejoin-history 3 '(1 100) '(2 200))))
    (setf (pg::timeline-history-entry-begin (first broken)) 199)
    (ok (intake-fails-p
         (lambda () (pg::timeline-common-ancestor broken (rejoin-history 1))))))
  (ok (intake-fails-p
       (lambda () (pg::timeline-common-ancestor nil (rejoin-history 1))))))

(cl-spec:defproperty rejoin-common-ancestor-keeps-the-earlier-parent-end
    ((left-end (range integer 129 1000000)) (right-end (range integer 129 1000000)))
  (:about pg::timeline-common-ancestor)
  (:trials (:normal 100))
  (let ((left (rejoin-history 3 '(1 128) (list 2 left-end)))
        (right (rejoin-history 4 '(1 128) (list 2 right-end))))
    (equal (list 2 (min left-end right-end))
           (multiple-value-list (pg::timeline-common-ancestor left right)))))

(deftest generated-rejoin-common-ancestor-forks
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'rejoin-common-ancestor-keeps-the-earlier-parent-end
                                         :seed 87 :profile :normal))))
      (ok (eq :passed (getf result :status)))))
