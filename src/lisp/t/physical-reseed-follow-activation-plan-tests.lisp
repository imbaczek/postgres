(in-package #:pg-physical-tests)

(defun reseed-follow-activation-plan-fixture (selected old-count)
  (multiple-value-bind (record binding plan)
      (reseed-follow-configuration-inventory-fixture selected old-count)
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
      (let* ((bytes (pg::physical-follow-config-image
                     (pg::physical-reseed-follow-activation-config record binding)))
             (image (list (length bytes) (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
             (device (getf (getf (getf record :attempt) :local) :device)))
        (setf (getf record :activation)
              (list :phase :staged :step 0 :image image
                    :entry (list "follow.active" :file #o600 device 200000
                                 (first image) (second image))))
        (values (pg::physical-reseed-follow-copy record) binding plan)))))

(cl-spec:defproperty reseed-follow-activation-plan-preserves-historical-objects
    ((selected boolean) (old-count (range integer 0 3)) (prefix (range integer 0 2)))
  (:about pg::physical-reseed-follow-activation-positions)
  (:trials (:normal 100))
  (multiple-value-bind (record binding plan)
      (reseed-follow-activation-plan-fixture selected old-count)
    (let* ((before (pg::physical-adoption-attempt-value-copy (list record binding plan)))
           (terminal (getf (getf record :configuration) :step))
           (historical (pg::physical-reseed-follow-configuration-positions record binding terminal))
           (positions (funcall (symbol-function 'pg::physical-reseed-follow-activation-positions)
                               record binding prefix))
           (inventories (funcall (symbol-function 'pg::physical-reseed-follow-activation-inventories)
                                 plan record binding prefix nil))
           (original (pg::physical-reseed-follow-configuration-inventories
                      plan record binding terminal nil)))
      (and (= (1+ (length historical)) (length positions))
           (= (length positions) (length (remove-duplicates positions :key #'first :test #'equal)))
           (equal (assoc "follow.pending" positions :test #'equal)
                  (if (zerop prefix)
                      '("follow.pending" :root "physical-follow.control")
                      '("follow.pending" :saved "follow.pending")))
           (equal (assoc "follow.active" positions :test #'equal)
                  (if (= prefix 2)
                      '("follow.active" :root "physical-follow.control")
                      '("follow.active" :staged "follow.active")))
           (every (lambda (position)
                    (or (equal "follow.pending" (first position))
                        (equal position (assoc (first position) positions :test #'equal))))
                  historical)
           (loop for role in '(:incoming :quarantine :base :global :wal :workspace :receive)
                 always (equal (getf original role) (getf inventories role)))
           (= (1+ (loop for names in (cdr original) by #'cddr sum (length names)))
              (loop for names in (cdr inventories) by #'cddr sum (length names)))
           (equal before (list record binding plan))))))

(deftest generated-reseed-follow-activation-plan-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-activation-plan-preserves-historical-objects
                                       :seed 131 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-activation-plan-moves-only-the-two-config-files
    (dolist (selected '(nil t))
      (multiple-value-bind (record binding plan) (reseed-follow-activation-plan-fixture selected 2)
        (declare (ignore plan))
        (ok (equal '(("follow.pending" :root "physical-follow.control" :saved "follow.pending")
                     ("follow.active" :staged "follow.active" :root "physical-follow.control"))
                   (funcall (symbol-function 'pg::physical-reseed-follow-activation-moves)
                            record binding))))))

(deftest reseed-follow-activation-plan-rejects-uncaptured-and-rebound-images
    (multiple-value-bind (record binding plan) (reseed-follow-activation-plan-fixture t 1)
      (declare (ignore plan))
      (let ((bad (pg::physical-adoption-attempt-value-copy record)))
        (setf (getf (getf bad :activation) :phase) :building
              (getf (getf bad :activation) :entry) nil)
        (ok (intake-fails-p
             (lambda () (funcall (symbol-function 'pg::physical-reseed-follow-activation-moves)
                                 bad binding)))))
      (let ((bad (pg::physical-adoption-attempt-value-copy record)))
        (incf (second (getf (getf bad :activation) :image)))
        (incf (seventh (getf (getf bad :activation) :entry)))
        (ok (pg::physical-reseed-follow-copy bad))
        (ok (intake-fails-p
             (lambda () (funcall (symbol-function 'pg::physical-reseed-follow-activation-moves)
                                 bad binding)))))
      (let ((bad (pg::physical-adoption-attempt-value-copy binding)))
        (setf (getf bad :phase) :active)
        (ok (intake-fails-p
             (lambda () (funcall (symbol-function 'pg::physical-reseed-follow-activation-moves)
                                 record bad)))))))

(deftest reseed-follow-activation-plan-rejects-prefixes-outside-the-two-moves
    (multiple-value-bind (record binding plan) (reseed-follow-activation-plan-fixture nil 0)
      (dolist (prefix '(-1 3 nil 1.5 "1"))
        (ok (intake-fails-p
             (lambda () (funcall (symbol-function 'pg::physical-reseed-follow-activation-positions)
                                 record binding prefix))))
        (ok (intake-fails-p
             (lambda () (funcall (symbol-function 'pg::physical-reseed-follow-activation-inventories)
                                 plan record binding prefix nil)))))))

(deftest reseed-follow-activation-plan-returns-owned-names
    (multiple-value-bind (record binding plan) (reseed-follow-activation-plan-fixture t 3)
      (let* ((before (pg::physical-adoption-attempt-value-copy (list record binding plan)))
             (moves (funcall (symbol-function 'pg::physical-reseed-follow-activation-moves) record binding))
             (positions (funcall (symbol-function 'pg::physical-reseed-follow-activation-positions)
                                 record binding 2))
             (inventory (funcall (symbol-function 'pg::physical-reseed-follow-activation-inventories)
                                 plan record binding 2 nil)))
        (setf (char (first (first moves)) 0) #\X
              (char (third (assoc "follow.pending" positions :test #'equal)) 0) #\X
              (char (first (getf inventory :saved)) 0) #\X)
        (ok (equal before (list record binding plan)))
        (ok (string= "follow.pending"
                     (first (first (funcall (symbol-function 'pg::physical-reseed-follow-activation-moves)
                                            record binding))))))))

(deftest reseed-follow-activation-inventories-match-every-config-location
    (dolist (selected '(nil t))
      (multiple-value-bind (record binding plan) (reseed-follow-activation-plan-fixture selected 3)
        (let ((original (pg::physical-reseed-follow-configuration-inventories
                         plan record binding (getf (getf record :configuration) :step) nil)))
          (dolist (prefix '(0 1 2))
            (let ((actual (pg::physical-reseed-follow-activation-inventories
                           plan record binding prefix nil)))
              (ok (equal (getf actual :root)
                         (if (= prefix 1)
                             (remove "physical-follow.control" (getf original :root) :test #'equal)
                             (getf original :root))))
              (ok (equal (getf actual :staged)
                         (sort (append (getf original :staged)
                                       (unless (= prefix 2) (list "follow.active")))
                               #'string<)))
              (ok (equal (getf actual :saved)
                         (sort (append (getf original :saved)
                                       (unless (zerop prefix) (list "follow.pending")))
                               #'string<)))))))))

(deftest reseed-follow-activation-plan-keeps-captured-phase-plans-static
    (multiple-value-bind (record binding plan) (reseed-follow-activation-plan-fixture t 1)
      (declare (ignore plan))
      (dolist (state '((:staged 0) (:activating 0) (:activating 1) (:activating 2) (:active 2)))
        (let ((candidate (pg::physical-adoption-attempt-value-copy record)))
          (setf (getf (getf candidate :activation) :phase) (first state)
                (getf (getf candidate :activation) :step) (second state))
          (dolist (prefix '(0 1 2))
            (ok (equal (pg::physical-reseed-follow-activation-positions record binding prefix)
                       (pg::physical-reseed-follow-activation-positions candidate binding prefix))))))))

(deftest reseed-follow-activation-inventories-reject-another-valid-install-plan
    (multiple-value-bind (record binding plan) (reseed-follow-activation-plan-fixture nil 2)
      (setf (getf (getf plan :journal) :inode) 300000)
      (ok (pg::physical-reseed-install-copy plan))
      (ok (intake-fails-p
           (lambda () (pg::physical-reseed-follow-activation-inventories plan record binding 0 nil))))))

(cl-spec:defproperty reseed-follow-activation-prefix-matches-recorded-progress
    ((selected boolean) (state (range integer 0 6)) (prefix (range integer 0 2)))
  (:about pg::physical-reseed-follow-activation-check-prefix)
  (:trials (:normal 100))
  (multiple-value-bind (record groups) (reseed-follow-activation-codec-fixture selected)
    (when (plusp state)
      (setf (getf record :activation) (nth (1- state) groups)))
    (let* ((before (pg::physical-adoption-attempt-value-copy record))
           (check (symbol-function 'pg::physical-reseed-follow-activation-check-prefix))
           (expected (not (null (member prefix (nth state '((0) (0) (0) (0 1) (1 2) (2) (2)))))))
           (accepted (handler-case (progn (funcall check record prefix) t)
                       (error () nil))))
      (and (eq expected accepted) (equal before record)))))

(deftest generated-reseed-follow-activation-prefix-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-activation-prefix-matches-recorded-progress
                                       :seed 132 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-activation-prefix-checks-every-recorded-state
    (dolist (selected '(nil t))
      (multiple-value-bind (base groups) (reseed-follow-activation-codec-fixture selected)
        (loop for group in (cons nil groups)
              for allowed in '((0) (0) (0) (0 1) (1 2) (2) (2))
              do (let ((record (pg::physical-adoption-attempt-value-copy base)))
                   (when group (setf (getf record :activation) group))
                   (dolist (prefix '(0 1 2 -1 3 nil 1.5 "1"))
                     (let* ((check (symbol-function 'pg::physical-reseed-follow-activation-check-prefix))
                            (accepted (handler-case (progn (funcall check record prefix) t)
                                        (error () nil))))
                       (ok (eq (not (null (member prefix allowed))) accepted)))))))))

(defun reseed-follow-retirement-plan-fixture (selected old-count)
  (multiple-value-bind (record binding plan)
      (reseed-follow-activation-plan-fixture selected old-count)
    (setf (getf (getf record :activation) :phase) :active
          (getf (getf record :activation) :step) 2)
    (values (pg::physical-reseed-follow-copy record) binding plan)))

(cl-spec:defproperty reseed-follow-retirement-plan-preserves-historical-layout
    ((selected boolean) (old-count (range integer 0 3)) (prefix (range integer 0 2)))
  (:about pg::physical-reseed-follow-retirement-inventories)
  (:trials (:normal 100))
  (multiple-value-bind (record binding plan)
      (reseed-follow-retirement-plan-fixture selected old-count)
    (let* ((before (pg::physical-adoption-attempt-value-copy (list record binding plan)))
           (extra '(("physical-reseed.follow" nil)))
           (original (pg::physical-reseed-follow-activation-inventories plan record binding 2 extra))
           (actual (funcall (symbol-function 'pg::physical-reseed-follow-retirement-inventories)
                            plan record binding prefix extra))
           (moved (subseq '("physical-reseed.install" "physical-reseed.attempt") 0 prefix)))
      (and (equal (getf actual :root)
                  (remove-if (lambda (name) (member name moved :test #'equal)) (getf original :root)))
           (equal (getf actual :saved)
                  (sort (append (copy-list (getf original :saved)) (copy-list moved)) #'string<))
           (loop for role in '(:incoming :quarantine :base :global :wal :workspace :staged :receive)
                 always (equal (getf original role) (getf actual role)))
           (= (loop for names in (cdr original) by #'cddr sum (length names))
              (loop for names in (cdr actual) by #'cddr sum (length names)))
           (equal before (list record binding plan))))))

(deftest generated-reseed-follow-retirement-layout-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-retirement-plan-preserves-historical-layout
                                       :seed 140 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-retirement-plan-moves-journals-in-fixed-order
    (multiple-value-bind (record binding plan) (reseed-follow-retirement-plan-fixture t 2)
      (declare (ignore plan))
      (ok (equal '((:install-journal :root "physical-reseed.install" :saved "physical-reseed.install")
                   (:attempt-journal :root "physical-reseed.attempt" :saved "physical-reseed.attempt"))
                 (funcall (symbol-function 'pg::physical-reseed-follow-retirement-moves) record binding)))
      (dolist (prefix '(0 1 2))
        (let ((positions (funcall (symbol-function 'pg::physical-reseed-follow-retirement-positions)
                                  record binding prefix)))
          (ok (equal (assoc :install-journal positions)
                     (list :install-journal (if (zerop prefix) :root :saved) "physical-reseed.install")))
          (ok (equal (assoc :attempt-journal positions)
                     (list :attempt-journal (if (= prefix 2) :saved :root) "physical-reseed.attempt")))))))

(deftest reseed-follow-retirement-prefix-checks-every-recorded-state
    (dolist (selected '(nil t))
      (multiple-value-bind (record groups) (reseed-follow-retirement-codec-fixture selected)
        (loop for group in (cons nil groups)
              for allowed in '((0) (0 1) (1 2) (2) (2))
              do (let ((candidate (reseed-follow-with-retirement record group)))
                   (dolist (prefix '(0 1 2 -1 3 nil 1.5 "1"))
                     (let* ((check (symbol-function 'pg::physical-reseed-follow-retirement-check-prefix))
                            (accepted (handler-case (progn (funcall check candidate prefix) t)
                                        (error () nil))))
                       (ok (eq (not (null (member prefix allowed))) accepted)))))))))

(deftest reseed-follow-retirement-plan-rejects-incomplete-and-invalid-prefixes
    (multiple-value-bind (record binding plan) (reseed-follow-activation-plan-fixture nil 0)
      (dolist (state '((:staged 0) (:activating 2)))
        (setf (getf (getf record :activation) :phase) (first state)
              (getf (getf record :activation) :step) (second state))
        (ok (intake-fails-p
             (lambda () (pg::physical-reseed-follow-retirement-moves record binding))))
        (ok (intake-fails-p
             (lambda () (pg::physical-reseed-follow-retirement-positions record binding 0))))
        (ok (intake-fails-p
             (lambda () (pg::physical-reseed-follow-retirement-inventories plan record binding 0 nil)))))
      (setf (getf (getf record :activation) :phase) :active
            (getf (getf record :activation) :step) 2)
      (dolist (prefix '(-1 3 nil 1.5 "1"))
        (ok (intake-fails-p
             (lambda () (pg::physical-reseed-follow-retirement-positions record binding prefix))))
        (ok (intake-fails-p
             (lambda () (pg::physical-reseed-follow-retirement-inventories plan record binding prefix nil)))))))

(deftest reseed-follow-retirement-plan-owns-names-and-preserves-all-predecessors
    (multiple-value-bind (record binding plan) (reseed-follow-retirement-plan-fixture t 3)
      (let* ((extra (list (list (copy-seq "physical-reseed.follow") nil)))
             (before (pg::physical-adoption-attempt-value-copy (list record binding plan extra)))
             (moves (pg::physical-reseed-follow-retirement-moves record binding))
             (positions (pg::physical-reseed-follow-retirement-positions record binding 2))
             (inventory (pg::physical-reseed-follow-retirement-inventories plan record binding 2 extra)))
        (setf (char (third (first moves)) 0) #\X
              (char (third (assoc :attempt-journal positions)) 0) #\X
              (char (first (getf inventory :root)) 0) #\X
              (char (first (getf inventory :saved)) 0) #\X)
        (ok (equal before (list record binding plan extra)))
        (ok (string= "physical-reseed.install"
                     (third (first (pg::physical-reseed-follow-retirement-moves record binding))))))))

(deftest reseed-follow-retirement-plan-keeps-recorded-progress-separate-from-static-prefix
    (multiple-value-bind (record binding plan) (reseed-follow-retirement-plan-fixture nil 1)
      (let* ((bytes (pg::physical-reseed-attempt-image (getf record :attempt)))
             (device (getf (getf (getf record :attempt) :local) :device))
             (reference (list :identity (list device 300000 #o100600 1000 1 (length bytes) 0 0)
                              :digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
        (dolist (state '((:retiring 0) (:retiring 1) (:retiring 2) (:retired 2)))
          (let ((candidate (append (pg::physical-reseed-follow-copy record)
                                   (list :retirement
                                         (list :phase (first state) :step (second state)
                                               :attempt-file reference)))))
            (dolist (prefix '(0 1 2))
              (ok (equal (pg::physical-reseed-follow-retirement-positions record binding prefix)
                         (pg::physical-reseed-follow-retirement-positions candidate binding prefix))))
            (dolist (prefix '(0 1 2))
              (ok (equal (pg::physical-reseed-follow-retirement-inventories plan record binding prefix nil)
                         (pg::physical-reseed-follow-retirement-inventories plan candidate binding prefix nil)))))))))
