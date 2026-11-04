(in-package #:pg-physical-tests)

(defun reseed-follow-configuration-plan-fixture (selected)
  (multiple-value-bind (configuration core) (reseed-follow-configuration-codec-fixture :staged selected)
    (let* ((record (pg::physical-reseed-follow-copy (append core (list :configuration configuration))))
           (anchor (pg::physical-reseed-follow-generation-anchor (first (getf core :generations))))
           (control (pg::physical-recovery-decode-control
                     (pg::physical-libpq-datum-octets (getf (getf core :base) :recovery-control) 196)))
           (end (getf control :applied))
           (base (list :system-id (getf control :system-id) :timeline (getf control :timeline)
                       :end end :checkpoint (getf control :checkpoint)
                       :manifest-digest (getf (getf core :base) :manifest-digest)
                       :cluster-identity (getf (getf (getf core :attempt) :request) :cluster-identity)
                       :next-xid (getf control :next-xid) :observed-next-xid (getf control :next-xid)
                       :required-applied end :required-timeline (getf control :timeline))))
      (let* ((binding (pg::physical-follow-empty-base-binding base anchor))
             (pending (pg::physical-follow-config-image (pg::physical-reseed-follow-pending-config record binding)))
             (size (length pending)) (digest (pg::physical-store-update-digest #xCBF29CE484222325 pending))
             (state (getf record :configuration))
             (specification (assoc "follow.pending" (getf state :images) :test #'equal))
             (row (first (second (assoc "follow.pending" (getf state :entries) :test #'equal)))))
        (setf (second specification) size (third specification) digest
              (sixth row) size (seventh row) digest)
        (values (pg::physical-reseed-follow-copy record) binding)))))

(cl-spec:defproperty reseed-follow-configuration-positions-preserve-every-logical-object
    ((selected boolean) (choice (range integer 0 5)))
  (:about pg::physical-reseed-follow-configuration-positions)
  (:trials (:normal 80))
  (multiple-value-bind (record binding) (reseed-follow-configuration-plan-fixture selected)
    (let* ((before (pg::physical-reseed-follow-image record))
           (moves (pg::physical-reseed-follow-configuration-moves record binding))
           (maximum (length moves)) (prefix (mod choice (1+ maximum)))
           (positions (pg::physical-reseed-follow-configuration-positions record binding prefix))
           (initial (pg::physical-reseed-follow-configuration-positions record binding 0))
           (original (assoc :original-recovery positions))
           (receive (assoc :receive-directory positions))
           (recovery (assoc (if selected "recovery.selected" "recovery.initial") positions :test #'equal)))
      (and (= maximum (if selected 5 4))
           (= (+ 2 (length (getf (getf record :configuration) :images))) (length positions))
           (= (length positions) (length (remove-duplicates (mapcar #'first positions) :test #'equal)))
           (= (length positions) (length (remove-duplicates (mapcar #'rest positions) :test #'equal)))
           (equal (mapcar #'first initial) (mapcar #'first positions))
           (eq (second original) (if (< prefix (1- maximum)) :root :saved))
           (eq (second receive) (if (zerop prefix) :workspace :root))
           (eq (second recovery) (if (= prefix maximum) :root :staged))
           (eq (= prefix (1- maximum))
               (not (find '(:root "physical-recovery.control") positions :key #'rest :test #'equal)))
           (equalp before (pg::physical-reseed-follow-image record))))))

(deftest generated-reseed-follow-configuration-position-preservation
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'reseed-follow-configuration-positions-preserve-every-logical-object
                                       :seed 121 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-configuration-move-plan-preserves-the-original-control-and-unused-images
    (dolist (selected '(nil t))
      (multiple-value-bind (record binding) (reseed-follow-configuration-plan-fixture selected)
        (let* ((moves (pg::physical-reseed-follow-configuration-moves record binding))
               (expected
                 (append (list '(:receive-directory :workspace "receive" :root "pg_physical_receive"))
                         (when selected
                           (list (list "history.wal" :staged "history.wal" :wal
                                       (pg::timeline-history-filename (eighth (getf binding :after))))))
                         (list '("follow.pending" :staged "follow.pending" :root "physical-follow.control")
                               '(:original-recovery :root "physical-recovery.control" :saved "recovery.original")
                               (list (if selected "recovery.selected" "recovery.initial")
                                     :staged (if selected "recovery.selected" "recovery.initial")
                                     :root "physical-recovery.control")))))
          (ok (equal expected moves))
          (loop for prefix from 0 to (length moves)
                for positions = (pg::physical-reseed-follow-configuration-positions record binding prefix)
                do (ok (= (if selected 10 6) (length positions)))
                   (when selected
                     (ok (equal '("receive.initial" :staged "receive.initial")
                                (assoc "receive.initial" positions :test #'equal)))
                     (ok (equal '("recovery.initial" :staged "recovery.initial")
                                (assoc "recovery.initial" positions :test #'equal)))))
          (dolist (bad (list -1 (1+ (length moves))))
            (ok (intake-fails-p
                 (lambda () (pg::physical-reseed-follow-configuration-positions record binding bad)))))))))

(deftest reseed-follow-configuration-move-plan-uses-the-actual-immediate-child
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-follow-building-fixture
       kind
       (lambda (owner root workspace building)
         (declare (ignore workspace building))
         (let ((record (pg::physical-reseed-follow-stage-configuration owner))
               (before (sort (pg::directory-entry-names root :follow-symlinks t) #'string<)))
           (pg::call-with-physical-reseed-follow-image
            owner
            (lambda (fd check)
              (declare (ignore check))
              (multiple-value-bind (images binding)
                  (pg::physical-reseed-follow-configuration-images-at
                   owner fd (pg::physical-reseed-follow-core-projection record) 12345)
                (declare (ignore images))
                (let* ((selected (member kind '(:at-fork :later-writer)))
                       (moves (pg::physical-reseed-follow-configuration-moves record binding))
                       (wal (assoc "history.wal" moves :test #'equal)))
                  (ok (= (if selected 5 4) (length moves)))
                  (if selected
                      (ok (equal (pg::timeline-history-filename 2) (fifth wal)))
                      (ok (null wal)))))))
           (ok (equal record (pg::physical-reseed-follow-read owner)))
           (ok (equal before (sort (pg::directory-entry-names root :follow-symlinks t) #'string<))))))))

(deftest reseed-follow-configuration-move-plan-rejects-unready-and-rebound-inputs
    (dolist (selected '(nil t))
      (multiple-value-bind (record binding) (reseed-follow-configuration-plan-fixture selected)
        (let* ((core (pg::physical-reseed-follow-core-projection record))
               (building (append core (list :configuration
                                            (reseed-follow-configuration-codec-fixture :building selected)))))
          (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-configuration-moves building binding))))
          (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-configuration-moves core binding)))))
        (multiple-value-bind (other other-binding) (reseed-follow-configuration-plan-fixture (not selected))
          (declare (ignore other))
          (ok (pg::physical-follow-binding-check other-binding))
          (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-configuration-moves record other-binding)))))
        (dolist (prefix '(nil "0" 1/2 -1 6))
          (ok (intake-fails-p
               (lambda () (pg::physical-reseed-follow-configuration-positions record binding prefix))))))))

(deftest reseed-follow-configuration-move-plan-returns-owned-names
    (dolist (selected '(nil t))
      (multiple-value-bind (record binding) (reseed-follow-configuration-plan-fixture selected)
        (let* ((before (pg::physical-reseed-follow-image record))
               (moves (pg::physical-reseed-follow-configuration-moves record binding))
               (positions (pg::physical-reseed-follow-configuration-positions record binding 0))
               (expected-moves (pg::physical-reseed-follow-configuration-moves record binding))
               (expected-positions (pg::physical-reseed-follow-configuration-positions record binding 0)))
          (setf (char (third (first moves)) 0) #\x
                (char (third (first positions)) 0) #\x)
          (ok (equal expected-moves (pg::physical-reseed-follow-configuration-moves record binding)))
          (ok (equal expected-positions (pg::physical-reseed-follow-configuration-positions record binding 0)))
          (let ((same (equalp before (pg::physical-reseed-follow-image record)))) (ok same))))))

(deftest reseed-follow-configuration-move-plan-rejects-a-same-selection-rebind
    (multiple-value-bind (record binding) (reseed-follow-configuration-plan-fixture t)
      (let* ((other (copy-tree binding))
             (after (getf other :after))
             (text (concatenate 'string (getf other :child-history)
                                (string #\Newline) "# alternative child history encoding" (string #\Newline))))
        (setf (getf other :child-history) text
              (ninth after) (pg::physical-follow-binding-selection-digest
                             (getf other :anchor) (eighth after) text (getf other :before) (fifth after)))
        (ok (pg::physical-follow-binding-check other))
        (ok (equal (getf other :anchor) (getf binding :anchor)))
        (let ((new-name (pg::physical-store-history-filename (eighth after) (ninth after)))
              (old-name (pg::physical-store-history-filename
                         (eighth (getf binding :after)) (ninth (getf binding :after)))))
          (ok (not (equal new-name old-name))))
        (let ((outcome (handler-case
                           (progn (pg::physical-reseed-follow-configuration-positions record other 1) :accepted)
                         (simple-error (condition)
                           (declare (ignore condition))
                           :rejected))))
          (ok (eq :rejected outcome))))))
