(in-package #:pg-physical-tests)

(defun call-with-backup-lease-allocation-pin-graph-fixture (function)
  (call-with-backup-lease-allocation-authority-fixture
   (lambda (root parent row)
     (let ((parents (make-hash-table :test #'equal))
           (allocations (make-hash-table :test #'equal))
           (pins (make-hash-table :test #'equal)))
       (setf (gethash (getf (getf parent :request) :name) parents) (copy-tree parent)
             (gethash (getf row :name) allocations) (pg::physical-backup-lease-allocation-copy row))
       (maphash (lambda (name pin) (setf (gethash name pins) pin)) pg::*physical-source-pins*)
       (funcall function root parents allocations pins row)))))

(defun backup-lease-allocation-pin-graph-check (parents allocations pins)
  (funcall 'pg::physical-backup-lease-allocation-check-pin-inventory parents allocations pins))

(defun backup-lease-allocation-pin-graph-save-follow (row pins &optional advanced lost)
  (let ((pin (backup-lease-allocation-authority-save-pin row)))
    (when advanced
      (incf (pg::physical-source-pin-confirmed-lsn pin) 8)
      (incf (pg::physical-source-pin-restart-lsn pin) 8))
    (setf (pg::physical-source-pin-lost-p pin) lost
          (gethash (getf row :name) pins) pin)
    pin))

(deftest backup-lease-allocation-pin-graph-accepts-unprepared-and-matching-pins
    (dolist (stage '(:absent :retaining :ready :lost))
      (call-with-backup-lease-allocation-pin-graph-fixture
       (lambda (root parents allocations pins row)
         (declare (ignore root))
         (unless (eq stage :absent)
           (let ((pin (backup-lease-allocation-pin-graph-save-follow row pins nil (eq stage :lost))))
             (when (eq stage :retaining)
               (setf (pg::physical-follow-pin-stage pin) :retaining
                     (pg::physical-source-pin-confirmed-lsn pin) nil
                     (pg::physical-source-pin-restart-lsn pin) (getf (getf row :coordinates) :overlap-start)))))
         (ok (backup-lease-allocation-pin-graph-check parents allocations pins))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-pin-graph-rejects-wrong-pin-ownership
    (dolist (fault '(:bootstrap-type :coordinates :table-key))
      (call-with-backup-lease-allocation-pin-graph-fixture
       (lambda (root parents allocations pins row)
         (declare (ignore root))
         (let ((pin (backup-lease-allocation-pin-graph-save-follow row pins)))
           (ecase fault
             (:bootstrap-type
              (setf (gethash (getf row :name) pins)
                    (pg::make-physical-source-pin :name (getf row :name)
                                                  :system-id (pg::catalog-system-identifier)
                                                  :timeline 2 :restart-lsn 0 :consistent-lsn 0)))
             (:coordinates
              (incf (getf (getf (gethash (getf row :name) allocations) :coordinates) :manifest-digest)))
             (:table-key
              (remhash (getf row :name) pins)
              (setf (gethash "wrong_graph_key" pins) pin))))
         (ok (intake-fails-p
              (lambda () (backup-lease-allocation-pin-graph-check parents allocations pins))))
         (ok (not pg::*wal-poisoned*))))))

(defun backup-lease-allocation-pin-graph-snapshot (parents allocations pins)
  (flet ((snapshot (table copier)
           (sort (loop for name being the hash-keys of table using (hash-value value)
                       collect (list (copy-seq name) (funcall copier value)))
                 #'string< :key #'first)))
    (list (snapshot parents #'pg::physical-backup-lease-copy)
          (snapshot allocations #'pg::physical-backup-lease-allocation-copy)
          (snapshot pins (lambda (pin) (coerce (pg::physical-source-pin-encode pin) 'list))))))

(defun backup-lease-allocation-pin-graph-select (parents pins row phase pin)
  (let* ((name (getf (getf row :request) :name))
         (parent (gethash name parents)))
    (setf (getf parent :phase) phase
          (getf parent :outcome) :handed-off
          (getf parent :successor)
          (list :name (copy-seq (getf row :name))
                :anchor-digest (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin))))
    (when (eq phase :closed) (remhash name pins))
    parent))

(deftest backup-lease-allocation-pin-graph-closing-requires-unused-ready-selection
    (dolist (fault '(:none :absent :lost :advanced :wrong-digest))
      (call-with-backup-lease-allocation-pin-graph-fixture
       (lambda (root parents allocations pins row)
         (declare (ignore root))
         (let* ((pin (backup-lease-allocation-pin-graph-save-follow row pins))
                (parent (backup-lease-allocation-pin-graph-select parents pins row :closing pin)))
           (ecase fault
             (:none)
             (:absent (remhash (getf row :name) pins))
             (:lost (setf (pg::physical-source-pin-lost-p pin) t))
             (:advanced
              (incf (pg::physical-source-pin-confirmed-lsn pin) 8)
              (incf (pg::physical-source-pin-restart-lsn pin) 8))
             (:wrong-digest (incf (getf (getf parent :successor) :anchor-digest))))
           (let ((before (backup-lease-allocation-pin-graph-snapshot parents allocations pins)))
             (ok (eq (not (null (handler-case
                                    (backup-lease-allocation-pin-graph-check parents allocations pins)
                                  (error () nil))))
                     (eq fault :none)))
             (ok (equal before (backup-lease-allocation-pin-graph-snapshot parents allocations pins)))))))))

(deftest backup-lease-allocation-pin-graph-closed-allows-selected-progress-or-retirement
    (dolist (state '(:ready :absent :advanced :lost-advanced :wrong-digest))
      (call-with-backup-lease-allocation-pin-graph-fixture
       (lambda (root parents allocations pins row)
         (declare (ignore root))
         (let* ((pin (backup-lease-allocation-pin-graph-save-follow
                      row pins (member state '(:advanced :lost-advanced))
                      (eq state :lost-advanced)))
                (parent (backup-lease-allocation-pin-graph-select parents pins row :closed pin)))
           (when (eq state :absent) (remhash (getf row :name) pins))
           (when (eq state :wrong-digest)
             (incf (getf (getf parent :successor) :anchor-digest)))
           (let ((before (backup-lease-allocation-pin-graph-snapshot parents allocations pins)))
             (ok (eq (not (null (handler-case
                                    (backup-lease-allocation-pin-graph-check parents allocations pins)
                                  (error () nil))))
                     (not (eq state :wrong-digest))))
             (ok (equal before (backup-lease-allocation-pin-graph-snapshot parents allocations pins)))))))))

(deftest backup-lease-allocation-pin-graph-closed-aborted-forbids-bootstrap-debt
    (call-with-backup-lease-allocation-pin-graph-fixture
     (lambda (root parents allocations pins row)
       (declare (ignore root))
       (let* ((name (getf (getf row :request) :name))
              (parent (gethash name parents)))
         (backup-lease-allocation-pin-graph-save-follow row pins t)
         (setf (getf parent :phase) :closed (getf parent :outcome) :aborted)
         (ok (intake-fails-p
              (lambda () (backup-lease-allocation-pin-graph-check parents allocations pins))))
         (remhash name pins)
         (delete-file (pg::wal-segment-path 0))
         (ok (backup-lease-allocation-pin-graph-check parents allocations pins))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-loader-preflights-pins-before-interruption-and-temp-cleanup
    (dolist (fault '(:wrong-owner :malformed))
      (call-with-backup-lease-allocation-pin-graph-fixture
       (lambda (root parents allocations pins row)
         (declare (ignore root allocations pins))
         (let* ((parent (gethash (getf (getf row :request) :name) parents))
                (pending (copy-tree parent))
                (pin nil))
           (setf (getf (getf pending :request) :name) "reseed_1123456789abcdef0123456789abcdef"
                 (getf pending :phase) :requested (getf pending :base) nil)
           (pg::physical-backup-lease-allocation-publish nil row)
           (setf pin (backup-lease-allocation-authority-save-pin row))
           (pg::physical-backup-lease-publish nil pending)
           (let* ((path (pg::physical-backup-lease-path (getf pending :request)))
                  (temporary (pg::physical-backup-lease-path (getf pending :request) t))
                  (pin-path (pg::physical-source-pin-path (getf row :name)))
                  (registry pg::*physical-backup-lease-records*)
                  (before (pg::read-bounded-regular-file path 8192)))
             (with-open-file (out temporary :direction :output :if-exists :error)
               (write-string "partial private temporary" out))
             (sb-posix:chmod (namestring temporary) #o600)
             (ecase fault
               (:wrong-owner
                (incf (getf (pg::physical-follow-pin-anchor pin) :manifest-digest))
                (incf (pg::physical-source-pin-manifest-digest pin))
                (pg::physical-source-pin-save pin))
               (:malformed
                (with-open-file (out pin-path :direction :output :if-exists :overwrite
                                              :element-type '(unsigned-byte 8))
                  (write-byte 0 out)
                  (pg::fsync-stream out))))
             (let ((pin-before (pg::read-bounded-regular-file pin-path
                                                              (pg::physical-follow-pin-image-limit))))
               (ok (intake-fails-p #'pg::physical-backup-lease-load-records))
               (ok (equalp before (pg::read-bounded-regular-file path 8192)))
               (ok (probe-file temporary))
               (ok (eq registry pg::*physical-backup-lease-records*))
               (ok (equalp pin-before
                           (pg::read-bounded-regular-file pin-path
                                                          (pg::physical-follow-pin-image-limit)))))))))))

(deftest backup-lease-allocation-parent-abort-retires-owned-debt-without-wal
    (dolist (state '(:live :canceled :closed-debt))
      (call-with-backup-lease-allocation-authority-fixture
       (lambda (root parent row)
         (declare (ignore root))
         (pg::physical-backup-lease-allocation-publish nil row)
         (let* ((pin (backup-lease-allocation-authority-save-pin row))
                (request (getf parent :request))
                (name (getf row :name)))
           (when (eq state :canceled)
             (let ((next (pg::physical-backup-lease-allocation-copy row)))
               (setf (getf next :state) :canceled)
               (pg::physical-backup-lease-allocation-publish row next)))
           (when (eq state :closed-debt) (backup-lease-allocation-parent-abort-stage-closed-debt parent))
           (delete-file (pg::wal-segment-path 0))
           (let ((result (pg::physical-backup-lease-abort request)))
             (ok (eq :closed (getf result :phase)))
             (ok (eq :aborted (getf result :outcome)))
             (ok (eq :canceled (getf (gethash name pg::*physical-backup-lease-allocations*) :state)))
             (ok (null (gethash name pg::*physical-source-pins*)))
             (ok (not (probe-file (pg::physical-source-pin-path name))))
             (ok (null (gethash pin pg::*wal-snapshot-retention-pins*))))
           (ok (not pg::*wal-poisoned*)))))))

(deftest backup-lease-allocation-parent-abort-preflights-later-generation-before-cleanup
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent first)
       (declare (ignore root))
       (pg::physical-backup-lease-allocation-publish nil first)
       (let* ((old (backup-lease-allocation-authority-save-pin first))
              (second (pg::physical-backup-lease-allocation-copy first)))
         (backup-lease-allocation-call #'pg::physical-backup-lease-follow-cancel first)
         (setf (getf second :generation) 2 (getf second :name) "next_cleanup_generation")
         (pg::physical-backup-lease-allocation-publish nil second)
         (let* ((later (backup-lease-allocation-authority-save-pin second))
                (old-name (getf first :name))
                (old-path (pg::physical-source-pin-path old-name))
                (later-path (pg::physical-source-pin-path (getf second :name))))
           (pg::physical-source-pin-save old)
           (setf (gethash old-name pg::*physical-source-pins*) old
                 (gethash old pg::*wal-snapshot-retention-pins*) (pg::physical-source-pin-restart-lsn old))
           (incf (getf (pg::physical-follow-pin-anchor later) :manifest-digest))
           (incf (pg::physical-source-pin-manifest-digest later))
           (pg::physical-source-pin-save later)
           (let ((old-before (pg::read-bounded-regular-file old-path (pg::physical-follow-pin-image-limit)))
                 (later-before (pg::read-bounded-regular-file later-path (pg::physical-follow-pin-image-limit))))
             (ok (intake-fails-p
                  (lambda () (pg::physical-backup-lease-abort (getf parent :request)))))
             (ok (equalp old-before (pg::read-bounded-regular-file old-path
                                                                   (pg::physical-follow-pin-image-limit))))
             (ok (equalp later-before (pg::read-bounded-regular-file later-path
                                                                     (pg::physical-follow-pin-image-limit))))
             (ok (eq old (gethash old-name pg::*physical-source-pins*))))
           (ok (eq :live (getf (gethash (getf second :name) pg::*physical-backup-lease-allocations*) :state)))
           (ok (probe-file (pg::physical-source-pin-path (getf (getf parent :request) :name)))))))))

(deftest backup-lease-allocation-startup-cleans-aborted-retaining-debt-without-wal
    (dolist (standby '(nil t))
      (call-with-backup-lease-allocation-authority-fixture
       (lambda (root parent row)
         (declare (ignore root))
         (pg::physical-backup-lease-allocation-publish nil row)
         (let ((pin (backup-lease-allocation-authority-save-pin row)))
           (setf (pg::physical-follow-pin-stage pin) :retaining
                 (pg::physical-source-pin-confirmed-lsn pin) nil
                 (pg::physical-source-pin-restart-lsn pin) (getf (getf row :coordinates) :overlap-start))
           (pg::physical-source-pin-save pin)
           (setf (gethash pin pg::*wal-snapshot-retention-pins*) (pg::physical-source-pin-restart-lsn pin))
           (backup-lease-allocation-parent-abort-stage-closed-debt parent)
           (delete-file (pg::wal-segment-path 0))
           (ok (not (intake-fails-p (lambda () (backup-lease-startup-reopen standby)))))
           (ok (eq :canceled
                   (getf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) :state)))
           (ok (not (probe-file (pg::physical-source-pin-path (getf row :name)))))
           (ok (null (gethash (getf row :name) pg::*physical-source-pins*)))
           (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*)))
           (ok (not pg::*wal-poisoned*)))))))

(deftest backup-lease-allocation-startup-invalidates-only-explicit-preparation-role-loss
    (dolist (role '(:current :standby :obsolete))
      (call-with-backup-lease-allocation-authority-fixture
       (lambda (root parent row)
         (declare (ignore root))
         (pg::physical-backup-lease-allocation-publish nil row)
         (let* ((pin (backup-lease-allocation-authority-save-pin row))
                (bootstrap (gethash (getf (getf parent :request) :name) pg::*physical-source-pins*))
                (path (pg::physical-source-pin-path (getf row :name))))
           (setf (pg::physical-follow-pin-stage pin) :retaining
                 (pg::physical-source-pin-confirmed-lsn pin) nil
                 (pg::physical-source-pin-restart-lsn pin) (getf (getf row :coordinates) :overlap-start)
                 (pg::physical-source-pin-lost-p bootstrap) t)
           (pg::physical-source-pin-save pin)
           (pg::physical-source-pin-save bootstrap)
           (delete-file (pg::wal-segment-path 0))
           (let ((before (pg::read-bounded-regular-file path (pg::physical-follow-pin-image-limit)))
                 (pg::*wal-insert-timeline* (if (eq role :obsolete) 3 2)))
             (if (eq role :current)
                 (progn
                   (ok (intake-fails-p (lambda () (backup-lease-startup-reopen nil))))
                   (ok (equalp before (pg::read-bounded-regular-file path
                                                                     (pg::physical-follow-pin-image-limit)))))
                 (progn
                   (ok (not (intake-fails-p
                             (lambda () (backup-lease-startup-reopen (eq role :standby))))))
                   (ok (pg::physical-source-pin-lost-p
                        (pg::physical-source-pin-read-raw path :stabilize t)))
                   (ok (eq :live
                           (getf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) :state)))
                   (ok (not pg::*wal-poisoned*))))))))))

(deftest backup-lease-allocation-standby-preserves-selected-history-and-primary-retention
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (pg::physical-backup-lease-allocation-publish nil row)
       (let* ((pin (backup-lease-allocation-authority-save-pin row))
              (successor (list :name (getf row :name)
                               :anchor-digest (pg::physical-follow-anchor-digest
                                               (pg::physical-follow-pin-anchor pin))))
              (path (pg::physical-source-pin-path (getf row :name))))
         (pg::physical-backup-lease-handoff (getf parent :request) successor)
         (let ((before (pg::read-bounded-regular-file path (pg::physical-follow-pin-image-limit))))
           (dotimes (attempt 2)
             (declare (ignorable attempt))
             (ok (not (intake-fails-p (lambda () (backup-lease-startup-reopen t)))))
             (ok (equalp before (pg::read-bounded-regular-file path
                                                               (pg::physical-follow-pin-image-limit))))
             (ok (eq :live
                     (getf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) :state))))
           (ok (not (intake-fails-p (lambda () (backup-lease-startup-reopen nil)))))
           (let ((registered (gethash (getf row :name) pg::*physical-source-pins*)))
             (ok registered)
             (ok (not (pg::physical-source-pin-lost-p registered)))
             (ok (= (getf (getf row :coordinates) :receive-lsn)
                    (gethash registered pg::*wal-snapshot-retention-pins*))))
           (ok (equalp before (pg::read-bounded-regular-file path
                                                             (pg::physical-follow-pin-image-limit)))))))))

(deftest backup-lease-allocation-startup-cleans-canceled-debt-under-interrupted-parent
    (dolist (standby '(nil t))
      (call-with-backup-lease-allocation-authority-fixture
       (lambda (root parent row)
         (declare (ignore root))
         (pg::physical-backup-lease-allocation-publish nil row)
         (let* ((pin (backup-lease-allocation-authority-save-pin row))
                (bootstrap (gethash (getf (getf parent :request) :name) pg::*physical-source-pins*))
                (canceled (pg::physical-backup-lease-allocation-copy row))
                (interrupted (pg::physical-backup-lease-copy parent)))
           (setf (pg::physical-follow-pin-stage pin) :retaining
                 (pg::physical-source-pin-confirmed-lsn pin) nil
                 (pg::physical-source-pin-restart-lsn pin) (getf (getf row :coordinates) :overlap-start)
                 (pg::physical-source-pin-lost-p bootstrap) t
                 (getf canceled :state) :canceled
                 (getf interrupted :phase) :interrupted)
           (pg::physical-source-pin-save pin)
           (pg::physical-source-pin-save bootstrap)
           (pg::physical-backup-lease-allocation-publish row canceled)
           (pg::physical-backup-lease-publish parent interrupted)
           (delete-file (pg::wal-segment-path 0))
           (ok (not (intake-fails-p (lambda () (backup-lease-startup-reopen standby)))))
           (ok (eq :interrupted
                   (getf (gethash (getf (getf parent :request) :name)
                                  pg::*physical-backup-lease-records*) :phase)))
           (ok (eq :canceled
                   (getf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) :state)))
           (ok (not (probe-file (pg::physical-source-pin-path (getf row :name)))))
           (ok (null (gethash (getf row :name) pg::*physical-source-pins*)))
           (ok (not pg::*wal-poisoned*)))))))

(deftest backup-lease-allocation-startup-keeps-ordinary-pin-availability-policy
    (dolist (standby '(nil t))
      (call-with-backup-lease-allocation-authority-fixture
       (lambda (root parent row)
         (declare (ignore root))
         (let* ((pin (pg::physical-source-follow-prepare "ordinary_startup_pin" (getf row :coordinates)))
                (bootstrap (gethash (getf (getf parent :request) :name) pg::*physical-source-pins*))
                (path (pg::physical-source-pin-path "ordinary_startup_pin")))
           (setf (pg::physical-follow-pin-stage pin) :retaining
                 (pg::physical-source-pin-confirmed-lsn pin) nil
                 (pg::physical-source-pin-restart-lsn pin) (getf (getf row :coordinates) :overlap-start)
                 (pg::physical-source-pin-lost-p bootstrap) t)
           (pg::physical-source-pin-save pin)
           (pg::physical-source-pin-save bootstrap)
           (sb-thread:with-mutex (pg::*wal-lock*)
             (ok (pg::physical-source-pin-read path :stabilize t)))
           (let ((before (pg::read-bounded-regular-file path (pg::physical-follow-pin-image-limit))))
             (delete-file (pg::wal-segment-path 0))
             (let ((pg::*physical-source-pins* (make-hash-table :test #'equal))
                   (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq)))
               (ok (eq (intake-fails-p (lambda () (backup-lease-startup-reopen standby)))
                       (not standby)))
               (ok (equalp before (pg::read-bounded-regular-file path
                                                                 (pg::physical-follow-pin-image-limit))))
               (when standby
                 (ok (null (gethash "ordinary_startup_pin" pg::*physical-source-pins*)))
                 (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*)))))))))))

(deftest backup-lease-allocation-unavailable-closing-source-prevents-all-role-loss
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (pg::physical-backup-lease-allocation-publish nil row)
       (let* ((selected (backup-lease-allocation-authority-save-pin row))
              (closing (pg::physical-backup-lease-copy parent))
              (other (pg::physical-backup-lease-copy parent))
              (other-row (pg::physical-backup-lease-allocation-copy row)))
         (setf (getf closing :phase) :closing (getf closing :outcome) :handed-off
               (getf closing :successor)
               (list :name (getf row :name)
                     :anchor-digest (pg::physical-follow-anchor-digest
                                     (pg::physical-follow-pin-anchor selected))))
         (setf closing (backup-lease-startup-inject-closing parent closing)
               (getf (getf other :request) :name) "reseed_2123456789abcdef0123456789abcdef"
               (getf other-row :request) (getf other :request)
               (getf other-row :name) "other_preparation")
         (pg::with-write-lock
           (pg::with-wal-publication-fence (begin-publication)
             (begin-publication)
             (pg::physical-backup-lease-write other)
             (setf pg::*physical-backup-lease-successor-cache* nil
                   (gethash (getf (getf other :request) :name) pg::*physical-backup-lease-records*) other)))
         (backup-lease-allocation-store-test-save-bootstrap other)
         (pg::physical-backup-lease-allocation-publish nil other-row)
         (backup-lease-allocation-authority-save-pin other-row)
         (pg::with-write-lock
           (pg::with-wal-publication-fence (begin-publication)
             (ok (pg::physical-backup-lease-closing-pin closing))))
         (let ((before
                 (loop for name in (list (getf (getf parent :request) :name) (getf row :name)
                                         (getf (getf other :request) :name) (getf other-row :name))
                       for path = (pg::physical-source-pin-path name)
                       collect (list path (pg::read-bounded-regular-file
                                           path (pg::physical-follow-pin-image-limit)))))
               (pg::*wal-insert-timeline* 3))
           (ok (intake-fails-p (lambda () (backup-lease-startup-reopen nil))))
           (dolist (entry before)
             (ok (equalp (second entry)
                         (pg::read-bounded-regular-file (first entry) (pg::physical-follow-pin-image-limit))))
             (ok (not (pg::physical-source-pin-lost-p
                       (pg::physical-source-pin-read-raw (first entry) :stabilize t)))))
           (ok (eq :live
                   (getf (gethash (getf other-row :name) pg::*physical-backup-lease-allocations*) :state))))))))

(defun backup-lease-allocation-recovery-child-body (setup operation)
  (format nil
          "(let ((pg::*wal-ready* t)
                 (pg::*wal-durable-lsn* 152)
                 (pg::*wal-end-lsn* 152)
                 (pg::*wal-checkpoint-lsn* 152)
                 (pg::*wal-checkpoint-snapshot* '~a)
                 (pg::*wal-segment-namespace* :timeline)
                 (pg::*physical-recovery-state* nil))
             ~a
             (pg::physical-backup-lease-load-records)
             (pg::physical-source-pin-load)
             (pg::physical-export-origin-load)
             (pg::physical-backup-lease-reconcile-startup)
             ~a)"
          (pg::physical-bootstrap-manifest-text pg::*wal-checkpoint-snapshot*)
          setup operation))

(defun backup-lease-allocation-recovery-child-label (expected actual cut log)
  (if (equal expected actual)
      (format nil "~s" cut)
      (let ((text (uiop:read-file-string log)))
        (format nil "~s; ~a~%~a" cut log (subseq text (max 0 (- (length text) 4000)))))))

(deftest backup-lease-allocation-recovery-survives-real-cleanup-process-kills
    (with-physical-store-directory (cache-root)
      (let ((cache (uiop:ensure-directory-pathname
                    (sb-posix:mkdtemp (namestring (merge-pathnames "allocation-recovery.XXXXXX" cache-root))))))
        (dolist (cut '((:allocation :after-directory-fsync)
                       (:allocation :before-memory)
                       (:pin :before-release-unlink)
                       (:pin :after-release-unlink)
                       (:pin :after-release-directory-fsync)
                       (:pin :after-release-memory)))
          (call-with-backup-lease-allocation-authority-fixture
           (lambda (root parent row)
             (pg::physical-backup-lease-allocation-publish nil row)
             (let* ((pin (backup-lease-allocation-authority-save-pin row))
                    (closing (pg::physical-backup-lease-copy parent))
                    (name (getf row :name))
                    (parent-name (getf (getf parent :request) :name))
                    (log (merge-pathnames "allocation-recovery-child.log" root)))
               (setf (pg::physical-follow-pin-stage pin) :retaining
                     (pg::physical-source-pin-confirmed-lsn pin) nil
                     (pg::physical-source-pin-restart-lsn pin) (getf (getf row :coordinates) :overlap-start)
                     (getf closing :phase) :closing (getf closing :outcome) :aborted)
               (pg::physical-source-pin-save pin)
               (backup-lease-startup-inject-closing parent closing)
               (delete-file (pg::wal-segment-path 0))
               (let* ((setup
                        (if (eq (first cut) :allocation)
                            (format nil
                                    "(setf pg::*physical-backup-lease-allocation-test-hook*
                                    (lambda (point value)
                                      (when (and (eq point ~s) (eq :canceled (getf value :state))
                                                 (equal ~s (getf value :name)))
                                        (sb-posix:kill (sb-posix:getpid) 9))))"
                                    (second cut) name)
                            (format nil
                                    "(setf pg::*physical-source-test-hook*
                                    (lambda (point value)
                                      (when (and (eq point ~s)
                                                 (equal ~s (pg::physical-source-pin-name value)))
                                        (if (eq point :after-release-memory)
                                            (progn
                                              (assert (null (gethash value pg::*wal-snapshot-retention-pins*)))
                                              (assert (null (gethash ~s pg::*physical-source-pins*))))
                                            (progn
                                              (assert (eq value (gethash ~s pg::*physical-source-pins*)))
                                              (assert (gethash value pg::*wal-snapshot-retention-pins*))))
                                        (sb-posix:kill (sb-posix:getpid) 9))))"
                                    (second cut) name name name)))
                      (result
                        (backup-lease-store-run-child
                         root log (backup-lease-allocation-recovery-child-body setup "(values)") cache)))
                 (ok (equal '(:signaled 9) result)
                     (backup-lease-allocation-recovery-child-label '(:signaled 9) result cut log)))
               (ok (eq :canceled
                       (getf (pg::physical-backup-lease-allocation-read
                              (pg::physical-backup-lease-allocation-path row)) :state)))
               (let* ((verify
                        (format nil
                                "(assert (eq :closed (getf (gethash ~s pg::*physical-backup-lease-records*) :phase)))
                               (assert (eq :aborted (getf (gethash ~s pg::*physical-backup-lease-records*) :outcome)))
                               (assert (eq :canceled (getf (gethash ~s pg::*physical-backup-lease-allocations*) :state)))
                               (assert (not (probe-file (pg::physical-source-pin-path ~s))))
                               (assert (not (probe-file (pg::physical-source-pin-path ~s))))
                               (assert (zerop (hash-table-count pg::*physical-source-pins*)))
                               (assert (zerop (hash-table-count pg::*wal-snapshot-retention-pins*)))"
                                parent-name parent-name name name parent-name))
                      (result
                        (backup-lease-store-run-child
                         root log (backup-lease-allocation-recovery-child-body "(values)" verify) cache)))
                 (ok (equal '(:exited 0) result)
                     (backup-lease-allocation-recovery-child-label '(:exited 0) result cut log))))))))))

(cl-spec:defproperty backup-lease-allocation-pin-graph-preserves-debt-and-inputs
    ((advanced boolean) (canceled boolean) (aborted boolean) (lost boolean))
  (:about pg::physical-backup-lease-allocation-check-pin-inventory)
  (:trials (:normal 80))
  (call-with-backup-lease-allocation-pin-graph-fixture
   (lambda (root parents allocations pins row)
     (declare (ignore root))
     (let* ((name (getf (getf row :request) :name))
            (parent (gethash name parents))
            (allocation (gethash (getf row :name) allocations))
            (pin (backup-lease-allocation-pin-graph-save-follow row pins advanced lost)))
       (declare (ignore pin))
       (when canceled (setf (getf allocation :state) :canceled))
       (when aborted (setf (getf parent :phase) :closing (getf parent :outcome) :aborted))
       (let ((before (backup-lease-allocation-pin-graph-snapshot parents allocations pins)))
         (delete-file (pg::wal-segment-path 0))
         (let ((accepted
                 (handler-case
                     (progn (backup-lease-allocation-pin-graph-check parents allocations pins) t)
                   (error () nil))))
           (and (eq accepted (not (null (or (not advanced) canceled aborted))))
                (equal before (backup-lease-allocation-pin-graph-snapshot parents allocations pins))
                (not pg::*wal-poisoned*))))))))

(deftest generated-backup-lease-allocation-pin-graph-debt-classification
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property
                    'backup-lease-allocation-pin-graph-preserves-debt-and-inputs
                    :seed 109 :profile :normal))))
      (ok (eq :passed (getf result :status)))))
