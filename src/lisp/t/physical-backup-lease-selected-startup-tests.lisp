(in-package #:pg-physical-tests)

(cl-spec:defproperty backup-lease-startup-recognizes-legacy-selection
    ((closed boolean))
  (:about pg::physical-backup-lease-startup-pin-class
          pg::physical-backup-lease-startup-managed-pin-p)
  (:trials (:normal 100))
  (let* ((record (backup-lease-test-record :ready))
         (name "legacy_selected_startup")
         (pg::*wal-lock* (sb-thread:make-mutex :name "legacy selected startup contract"))
         (pg::*physical-backup-lease-records* (make-hash-table :test #'equal))
         (pg::*physical-backup-lease-allocations* (make-hash-table :test #'equal))
         (pg::*physical-backup-lease-successor-cache* nil))
    (setf (getf record :phase) (if closed :closed :closing)
          (getf record :outcome) :handed-off
          (getf record :successor) (list :name name :anchor-digest 42)
          record (pg::physical-backup-lease-copy record)
          (gethash (getf (getf record :request) :name) pg::*physical-backup-lease-records*) record)
    (let ((before (pg::physical-bootstrap-manifest-text record)))
      (sb-thread:with-mutex (pg::*wal-lock*)
        (multiple-value-bind (kind row parent) (pg::physical-backup-lease-startup-pin-class name)
          (and (eq :selected kind) (null row) (equal record parent)
               (pg::physical-backup-lease-startup-managed-pin-p name)
               (eq :ordinary (pg::physical-backup-lease-startup-pin-class "ordinary_unassigned"))
               (not (pg::physical-backup-lease-startup-managed-pin-p "ordinary_unassigned"))
               (zerop (hash-table-count pg::*physical-backup-lease-allocations*))
               (equal before (pg::physical-bootstrap-manifest-text record))))))))

(deftest generated-backup-lease-legacy-selected-startup-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-startup-recognizes-legacy-selection
                                         :seed 191 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest backup-lease-startup-classifies-actual-legacy-handoffs
    (dolist (phase '(:closing :closed))
      (call-with-backup-lease-authority-fixture
       (lambda (root)
         (declare (ignore root))
         (multiple-value-bind (record bootstrap) (backup-lease-authority-prepare t)
           (declare (ignore bootstrap))
           (backup-lease-startup-save-origin record)
           (let* ((pin (backup-lease-startup-prepare-successor record))
                  (name (pg::physical-source-pin-name pin))
                  (successor (list :name name :anchor-digest
                                   (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin))))
                  (parent
                    (if (eq phase :closed)
                        (pg::physical-backup-lease-handoff (getf record :request) successor)
                        (let ((closing (copy-tree record)))
                          (setf (getf closing :phase) :closing (getf closing :outcome) :handed-off
                                (getf closing :successor) successor)
                          (backup-lease-startup-inject-closing record closing))))
                  (before (lineage-test-bytes (pg::physical-source-pin-path name))))
             (sb-thread:with-mutex (pg::*wal-lock*)
               (multiple-value-bind (kind row owner) (pg::physical-backup-lease-startup-pin-class name)
                 (ok (eq :selected kind))
                 (ok (null row))
                 (ok (equal parent owner)))
               (ok (pg::physical-backup-lease-startup-managed-pin-p name)))
             (ok (zerop (hash-table-count pg::*physical-backup-lease-allocations*)))
             (ok (equalp before (lineage-test-bytes (pg::physical-source-pin-path name))))
             (ok (not pg::*wal-poisoned*))))))))

(deftest backup-lease-legacy-selected-standby-removes-only-managed-retention
    (call-with-backup-lease-closed-successor-fixture
     (lambda (root closed pin anchor)
       (declare (ignore root))
       (let* ((name (pg::physical-source-pin-name pin))
              (ordinary (pg::physical-source-follow-ready "unmanaged_ordinary_startup" anchor))
              (ordinary-name (pg::physical-source-pin-name ordinary))
              (ordinary-floor (gethash ordinary pg::*wal-snapshot-retention-pins*))
              (ordinary-before (lineage-test-bytes (pg::physical-source-pin-path ordinary-name)))
              (before (lineage-test-bytes (pg::physical-source-pin-path name)))
              (parent-path (pg::physical-backup-lease-path (getf closed :request)))
              (parent-before (lineage-test-bytes parent-path)))
         (let ((pg::*physical-recovery-state* (list :phase 2)))
           (dotimes (attempt 2)
             (declare (ignorable attempt))
             (pg::physical-backup-lease-load-standby-pins)
             (ok (null (gethash name pg::*physical-source-pins*)))
             (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))
             (ok (eq ordinary (gethash ordinary-name pg::*physical-source-pins*)))
             (ok (eql ordinary-floor (gethash ordinary pg::*wal-snapshot-retention-pins*)))))
         (ok (equalp before (lineage-test-bytes (pg::physical-source-pin-path name))))
         (ok (equalp parent-before (lineage-test-bytes parent-path)))
         (ok (equalp ordinary-before (lineage-test-bytes (pg::physical-source-pin-path ordinary-name))))
         (ok (zerop (hash-table-count pg::*physical-backup-lease-allocations*)))
         (backup-lease-startup-reopen)
         (let ((restored (gethash name pg::*physical-source-pins*)))
           (ok restored)
           (ok (not (pg::physical-source-pin-lost-p restored)))
           (ok (= (pg::physical-source-pin-restart-lsn restored)
                  (gethash restored pg::*wal-snapshot-retention-pins*))))
         (ok (not pg::*wal-poisoned*))))))

(defun backup-lease-selected-fields-example (closed lost advanced)
  (let* ((parent (backup-lease-test-record :ready))
         (anchor (same-timeline-follow-anchor 152 123 456 2))
         (position (if advanced 160 152))
         (pin (pg::make-physical-follow-pin
               :name "selected_fields" :system-id (getf (getf parent :request) :system-id)
               :timeline 2 :manifest-digest 991 :consistent-lsn 152
               :restart-lsn position :confirmed-lsn position :prefix-digest 123
               :lost-p lost :stage :ready :anchor anchor)))
    (setf (getf anchor :system-id) (pg::physical-source-pin-system-id pin)
          (getf anchor :base-timeline) 2
          (getf anchor :base) 152
          (getf anchor :manifest-digest) 991
          (getf parent :phase) (if closed :closed :closing)
          (getf parent :outcome) :handed-off
          (getf parent :successor)
          (list :name (pg::physical-source-pin-name pin)
                :anchor-digest (pg::physical-follow-anchor-digest anchor)))
    (values (pg::physical-backup-lease-copy parent) pin)))

(cl-spec:defproperty backup-lease-selected-fields-ignore-retention-usability
    ((closed boolean) (lost boolean) (advanced boolean))
  (:about pg::physical-backup-lease-check-selected-pin-fields)
  (:trials (:normal 100))
  (multiple-value-bind (parent pin) (backup-lease-selected-fields-example closed lost advanced)
    (let ((before (pg::physical-source-pin-encode pin))
          (parent-before (copy-tree parent))
          (pg::*wal-insert-timeline* 99)
          (pg::*wal-durable-lsn* 0)
          (pg::*physical-recovery-state* (list :phase 2)))
      (and
       (eql (or closed (not advanced))
            (handler-case
                (eq pin (pg::physical-backup-lease-check-selected-pin-fields parent pin))
              (error () nil)))
       (equalp before (pg::physical-source-pin-encode pin))
       (equal parent-before parent)))))

(deftest generated-backup-lease-selected-fields-usability-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-selected-fields-ignore-retention-usability
                                         :seed 192 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest backup-lease-selected-fields-reject-changed-identity-and-invalid-progress
    (dolist (closed '(nil t))
      (multiple-value-bind (parent pin) (backup-lease-selected-fields-example closed t nil)
        (dolist (change
                 (list
                  (lambda (copy) (setf (pg::physical-source-pin-name copy) "foreign_selection"))
                  (lambda (copy) (incf (pg::physical-source-pin-system-id copy)))
                  (lambda (copy) (incf (pg::physical-source-pin-manifest-digest copy)))
                  (lambda (copy) (incf (pg::physical-source-pin-restart-lsn copy)))
                  (lambda (copy) (setf (pg::physical-follow-pin-stage copy) :retaining))
                  (lambda (copy) (incf (getf (pg::physical-follow-pin-anchor copy) :overlap-digest)))))
          (let ((copy (copy-structure pin)))
            (setf (pg::physical-follow-pin-anchor copy) (copy-tree (pg::physical-follow-pin-anchor pin)))
            (funcall change copy)
            (ok (signals (pg::physical-backup-lease-check-selected-pin-fields parent copy)))))
        (ok (signals (pg::physical-backup-lease-check-selected-pin-fields parent nil)))
        (dolist (change
                 (list
                  (lambda (copy) (setf (getf copy :phase) :ready))
                  (lambda (copy) (setf (getf copy :outcome) :aborted))
                  (lambda (copy) (setf (getf copy :base) nil))
                  (lambda (copy) (incf (getf (getf copy :request) :system-id)))
                  (lambda (copy) (incf (getf (getf copy :successor) :anchor-digest)))))
          (let ((copy (copy-tree parent)))
            (funcall change copy)
            (ok (signals (pg::physical-backup-lease-check-selected-pin-fields copy pin))))))))

(deftest backup-lease-strict-successor-check-keeps-forward-use-requirements
    (let ((pg::*physical-source-senders* (make-hash-table :test #'equal)))
      (dolist (closed '(nil t))
        (dolist (lost '(nil t))
          (dolist (advanced '(nil t))
            (multiple-value-bind (parent pin) (backup-lease-selected-fields-example closed lost advanced)
              (ok (eql (and (not lost) (not advanced))
                       (handler-case
                           (eq pin (pg::physical-backup-lease-check-successor-pin parent pin))
                         (error () nil))))))))
      (multiple-value-bind (parent pin) (backup-lease-selected-fields-example nil nil nil)
        (setf (gethash (pg::physical-source-pin-name pin) pg::*physical-source-senders*) :busy)
        (ok (signals (pg::physical-backup-lease-check-successor-pin parent pin))))))
