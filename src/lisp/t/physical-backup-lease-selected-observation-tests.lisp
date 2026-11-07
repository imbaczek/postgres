(in-package #:pg-physical-tests)

(defun call-with-backup-lease-selected-observation-fixture (function allocated &optional closing)
  (call-with-backup-lease-allocation-authority-fixture
   (lambda (root parent row)
     (when allocated (pg::physical-backup-lease-allocation-publish nil row))
     (let* ((pin (if allocated
                     (backup-lease-allocation-authority-save-pin row)
                     (pg::physical-source-follow-prepare (getf row :name) (getf row :coordinates))))
            (successor (list :name (pg::physical-source-pin-name pin)
                             :anchor-digest
                             (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin))))
            (terminal
              (if closing
                  (let ((candidate (copy-tree parent)))
                    (setf (getf candidate :phase) :closing
                          (getf candidate :outcome) :handed-off
                          (getf candidate :successor) successor)
                    (backup-lease-startup-inject-closing parent candidate))
                  (pg::physical-backup-lease-handoff (getf parent :request) successor))))
       (funcall function root terminal pin (when allocated row))))))

(defun backup-lease-selected-observe (parent)
  (pg::with-write-lock
    (sb-thread:with-mutex (pg::*wal-lock*)
      (multiple-value-list (pg::physical-backup-lease-selected-observation-under-lock parent)))))

(cl-spec:defproperty backup-lease-selected-absence-preserves-only-proven-coordinates
    ((allocated boolean) (absent boolean))
  (:about pg::physical-backup-lease-selected-observation-under-lock)
  (:trials (:normal 100))
  (call-with-backup-lease-selected-observation-fixture
   (lambda (root parent pin row)
     (declare (ignore root))
     (let* ((name (pg::physical-source-pin-name pin))
            (expected (pg::physical-follow-anchor-request (pg::physical-follow-pin-anchor pin)))
            (parent-before (lineage-test-bytes (pg::physical-backup-lease-path (getf parent :request))))
            (row-before (when row (lineage-test-bytes (pg::physical-backup-lease-allocation-path row)))))
       (when absent
         (pg::physical-source-follow-release name (getf (getf parent :successor) :anchor-digest)))
       (delete-file (pg::wal-segment-path 0))
       (let* ((pg::*wal-insert-timeline* 99)
              (pg::*wal-durable-lsn* 0)
              (observation (handler-case (backup-lease-selected-observe parent) (error () nil)))
              (coordinates (third observation))
              (saved-row (fourth observation)))
         (and (eq (if absent :absent :present) (first observation))
              (eq (unless absent pin) (second observation))
              (equal (when (or allocated (not absent)) expected) coordinates)
              (equal row saved-row)
              (or (null coordinates) (not (eq coordinates (getf row :coordinates))))
              (or (null saved-row) (not (eq saved-row row)))
              (equalp parent-before
                      (lineage-test-bytes (pg::physical-backup-lease-path (getf parent :request))))
              (or (null row)
                  (equalp row-before
                          (lineage-test-bytes (pg::physical-backup-lease-allocation-path row))))
              (= (if allocated 1 0) (hash-table-count pg::*physical-backup-lease-allocations*))))))
   allocated))

(deftest generated-backup-lease-selected-observation-absence-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'backup-lease-selected-absence-preserves-only-proven-coordinates
                                         :seed 193 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest backup-lease-selected-observation-keeps-lost-pins-owned-without-wal
    (dolist (allocated '(nil t))
      (dolist (closing '(nil t))
        (call-with-backup-lease-selected-observation-fixture
         (lambda (root parent pin row)
           (declare (ignore root))
           (pg::with-write-lock
             (sb-thread:with-mutex (pg::*wal-lock*)
               (setf (pg::physical-source-pin-lost-p pin) t)
               (pg::physical-source-pin-save pin)))
           (delete-file (pg::wal-segment-path 0))
           (let* ((pg::*wal-insert-timeline* 99)
                  (pg::*wal-durable-lsn* 0)
                  (observation (backup-lease-selected-observe parent)))
             (ok (eq :present (first observation)))
             (ok (eq pin (second observation)))
             (ok (equal (pg::physical-follow-anchor-request (pg::physical-follow-pin-anchor pin))
                        (third observation)))
             (ok (equal row (fourth observation)))
             (when closing
               (pg::with-write-lock
                 (sb-thread:with-mutex (pg::*wal-lock*)
                   (ok (signals (pg::physical-backup-lease-closing-pin parent))))))
             (ok (not pg::*wal-poisoned*))))
         allocated closing))))

(deftest backup-lease-selected-observation-rejects-one-sided-or-changed-pins
    (dolist (allocated '(nil t))
      (dolist (state '(:disk-only :registry-only :different-payload :unsafe-file))
        (call-with-backup-lease-selected-observation-fixture
         (lambda (root parent pin row)
           (declare (ignore root row))
           (let* ((name (pg::physical-source-pin-name pin))
                  (path (pg::physical-source-pin-path name))
                  (parent-before (lineage-test-bytes (pg::physical-backup-lease-path (getf parent :request)))))
             (ecase state
               (:disk-only (remhash name pg::*physical-source-pins*))
               (:registry-only (delete-file path))
               (:different-payload (setf (pg::physical-source-pin-lost-p pin) t))
               (:unsafe-file
                (delete-file path)
                (sb-posix:symlink "/dev/null" (namestring path))))
             (ok (signals (backup-lease-selected-observe parent)))
             (when (eq state :disk-only)
               (let ((pg::*physical-recovery-state* (list :phase 2)))
                 (let ((observed (backup-lease-selected-observe parent)))
                   (ok (eq :present (first observed)))
                   (ok (equalp (pg::physical-source-pin-encode pin)
                               (pg::physical-source-pin-encode (second observed)))))))
             (ok (equalp parent-before
                         (lineage-test-bytes (pg::physical-backup-lease-path (getf parent :request)))))
             (ok (not pg::*wal-poisoned*))))
         allocated))))

(deftest backup-lease-selected-observation-rejects-stale-parent-and-allocation-evidence
    (dolist (state '(:caller-token :parent-disk :duplicate-selection :missing-row
                     :row-disk :row-registry :canceled-row :coordinates :missing-parent :foreign-row :history-gap))
      (call-with-backup-lease-selected-observation-fixture
       (lambda (root parent pin row)
         (declare (ignore root pin))
         (pg::with-write-lock
           (sb-thread:with-mutex (pg::*wal-lock*)
             (ecase state
               (:missing-parent
                (remhash (getf (getf parent :request) :name) pg::*physical-backup-lease-records*))
               (:foreign-row
                (let ((copy (pg::physical-backup-lease-allocation-copy row)))
                  (setf (char (getf (getf copy :request) :token) 0) #\b)
                  (pg::physical-backup-lease-allocation-write copy)
                  (setf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) copy
                        pg::*physical-backup-lease-allocation-history-cache* nil)))
               (:history-gap
                (let ((copy (pg::physical-backup-lease-allocation-copy row)))
                  (setf (getf copy :generation) 2)
                  (pg::physical-backup-lease-allocation-write copy)
                  (setf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) copy
                        pg::*physical-backup-lease-allocation-history-cache* nil)))
               (:caller-token (setf (char (getf (getf parent :request) :token) 0) #\b))
               (:parent-disk
                (let ((copy (copy-tree parent)))
                  (incf (getf (getf copy :successor) :anchor-digest))
                  (pg::physical-backup-lease-write copy)))
               (:duplicate-selection
                (let* ((copy (copy-tree parent))
                       (request (getf copy :request)))
                  (setf (char (getf request :name) 7) #\f
                        (gethash (getf request :name) pg::*physical-backup-lease-records*) copy
                        pg::*physical-backup-lease-successor-cache* nil)))
               (:missing-row
                (pg::physical-backup-lease-allocation-histories)
                (remhash (getf row :name) pg::*physical-backup-lease-allocations*))
               (:row-disk
                (let ((copy (pg::physical-backup-lease-allocation-copy row)))
                  (setf (getf copy :state) :canceled)
                  (pg::physical-backup-lease-allocation-write copy)))
               (:row-registry
                (let ((copy (pg::physical-backup-lease-allocation-copy row)))
                  (incf (getf (getf copy :coordinates) :receive-digest))
                  (setf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) copy)))
               (:canceled-row
                (let ((copy (pg::physical-backup-lease-allocation-copy row)))
                  (setf (getf copy :state) :canceled)
                  (pg::physical-backup-lease-allocation-write copy)
                  (setf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) copy
                        pg::*physical-backup-lease-allocation-history-cache* nil)))
               (:coordinates
                (let ((copy (pg::physical-backup-lease-allocation-copy row)))
                  (incf (getf (getf copy :coordinates) :receive-digest))
                  (pg::physical-backup-lease-allocation-write copy)
                  (setf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) copy
                        pg::*physical-backup-lease-allocation-history-cache* nil))))))
         (ok (signals (backup-lease-selected-observe parent)))
         (ok (not pg::*wal-poisoned*)))
       t)))

(deftest backup-lease-selected-observation-keeps-phase-and-absence-boundaries
    (dolist (allocated '(nil t))
      (dolist (closing '(nil t))
        (dolist (absent '(nil t))
          (call-with-backup-lease-selected-observation-fixture
           (lambda (root parent pin row)
             (declare (ignore root))
             (let* ((name (pg::physical-source-pin-name pin))
                    (path (pg::physical-source-pin-path name))
                    (parent-path (pg::physical-backup-lease-path (getf parent :request)))
                    (before (lineage-test-bytes parent-path)))
               (pg::with-write-lock
                 (sb-thread:with-mutex (pg::*wal-lock*)
                   (if absent
                       (progn
                         (delete-file path)
                         (remhash name pg::*physical-source-pins*)
                         (remhash pin pg::*wal-snapshot-retention-pins*))
                       (progn
                         (setf (pg::physical-source-pin-confirmed-lsn pin) 160
                               (pg::physical-source-pin-restart-lsn pin) 160)
                         (pg::physical-source-pin-save pin)))))
               (if (and closing (not absent))
                   (ok (signals (backup-lease-selected-observe parent)))
                   (let ((observation (backup-lease-selected-observe parent)))
                     (ok (eq (if absent :absent :present) (first observation)))
                     (ok (equal (when (or allocated (not absent))
                                  (pg::physical-follow-anchor-request (pg::physical-follow-pin-anchor pin)))
                                (third observation)))
                     (ok (equal row (fourth observation)))))
               (when closing
                 (pg::with-write-lock
                   (sb-thread:with-mutex (pg::*wal-lock*)
                     (ok (signals (pg::physical-backup-lease-closing-pin parent))))))
               (ok (equalp before (lineage-test-bytes parent-path)))
               (ok (not pg::*wal-poisoned*))))
           allocated closing)))))
