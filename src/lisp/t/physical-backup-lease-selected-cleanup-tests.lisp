(in-package #:pg-physical-tests)

(cl-spec:defproperty selected-cleanup-closes-only-bootstrap-debt
    ((allocated boolean) (absent boolean) (bootstrap-absent boolean))
  (:about pg::physical-backup-lease-cleanup-selected)
  (:trials (:normal 100))
  (call-with-backup-lease-selected-observation-fixture
   (lambda (root parent pin row)
     (declare (ignore root))
     (let* ((request (getf parent :request))
            (successor (getf parent :successor))
            (name (getf successor :name))
            (bootstrap-name (getf request :name))
            (selected-path (pg::physical-source-pin-path name))
            (row-image (when row
                         (lineage-test-bytes (pg::physical-backup-lease-allocation-path row)))))
       (pg::with-write-lock
         (pg::with-wal-publication-fence (begin-publication)
           (when bootstrap-absent
             (begin-publication)
             (pg::physical-source-pin-unlink
              (gethash bootstrap-name pg::*physical-source-pins*) request))
           (begin-publication)
           (if absent
               (progn
                 (delete-file selected-path)
                 (pg::fsync-directory (pg::physical-source-pin-directory))
                 (remhash name pg::*physical-source-pins*)
                 (remhash pin pg::*wal-snapshot-retention-pins*))
               (progn
                 (setf (pg::physical-source-pin-lost-p pin) t)
                 (pg::physical-source-pin-save pin)))))
       (delete-file (pg::wal-segment-path 0))
       (let* ((selected-before (unless absent (lineage-test-bytes selected-path)))
              (floor-before (gethash pin pg::*wal-snapshot-retention-pins*))
              (expected (pg::physical-backup-lease-copy parent))
              (result (funcall 'pg::physical-backup-lease-cleanup-selected request successor)))
         (setf (getf expected :phase) :closed)
         (and (equal expected result)
              (equal result (funcall 'pg::physical-backup-lease-cleanup-selected request successor))
              (equal result (pg::physical-backup-lease-read
                             (pg::physical-backup-lease-path request)))
              (not (probe-file (pg::physical-source-pin-path bootstrap-name)))
              (null (gethash bootstrap-name pg::*physical-source-pins*))
              (if absent
                  (and (not (probe-file selected-path))
                       (null (gethash name pg::*physical-source-pins*)))
                  (and (eq pin (gethash name pg::*physical-source-pins*))
                       (equalp selected-before (lineage-test-bytes selected-path))
                       (eql floor-before (gethash pin pg::*wal-snapshot-retention-pins*))))
              (or (null row)
                  (equalp row-image
                          (lineage-test-bytes (pg::physical-backup-lease-allocation-path row))))
              (not pg::*wal-poisoned*)))))
   allocated t))

(deftest generated-selected-cleanup-bootstrap-debt-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'selected-cleanup-closes-only-bootstrap-debt
                                         :seed 196 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest selected-cleanup-authority-rejects-invalid-scope-and-phase-before-retirement
    (dolist (fault '(:wrong-type :publish-first :retired-first :done-first :other-record
                     :other-candidate :backend :incarnation :root :nested :phase))
      (call-with-backup-lease-selected-observation-fixture
       (lambda (root parent pin row)
         (declare (ignore pin row))
         (let* ((request (getf parent :request))
                (bootstrap-path (pg::physical-source-pin-path (getf request :name)))
                (before (lineage-test-bytes bootstrap-path))
                (escaped nil)
                (rejected
                  (handler-case
                      (pg::with-write-lock
                        (pg::with-wal-publication-fence (begin-publication)
                          (pg::call-with-physical-backup-lease-selected-cleanup
                           parent
                           (lambda (authority)
                             (setf escaped authority)
                             (ecase fault
                               (:wrong-type
                                (pg::physical-backup-lease-selected-cleanup-claim :fake parent :retire))
                               (:publish-first
                                (let ((next (pg::physical-backup-lease-copy parent)))
                                  (setf (getf next :phase) :closed)
                                  (pg::physical-backup-lease-selected-cleanup-claim
                                   authority parent :publish next)))
                               (:retired-first
                                (pg::physical-backup-lease-selected-cleanup-claim authority parent :retired))
                               (:done-first
                                (pg::physical-backup-lease-selected-cleanup-claim authority parent :done))
                               (:other-record
                                (let ((changed (pg::physical-backup-lease-copy parent)))
                                  (incf (getf (getf changed :base) :manifest-digest))
                                  (pg::physical-backup-lease-selected-cleanup-claim authority changed :retire)))
                               (:other-candidate
                                (pg::physical-backup-lease-selected-cleanup-claim
                                 authority parent :retire parent))
                               (:backend
                                (let ((pg::*backend* (list :wrong-backend)))
                                  (pg::physical-backup-lease-selected-cleanup-claim authority parent :retire)))
                               (:incarnation
                                (let ((pg::*wal-storage-incarnation* (list :wrong-incarnation)))
                                  (pg::physical-backup-lease-selected-cleanup-claim authority parent :retire)))
                               (:root
                                (let ((pg::*pgdata* (merge-pathnames "different-root/" root)))
                                  (pg::physical-backup-lease-selected-cleanup-claim authority parent :retire)))
                               (:nested
                                (pg::call-with-physical-backup-lease-selected-cleanup parent #'identity))
                               (:phase
                                (setf (pg::physical-backup-lease-selected-cleanup-authority-phase authority)
                                      :publishing)
                                (pg::physical-backup-lease-selected-cleanup-claim authority parent :retire)))
                             (error "invalid cleanup authority operation returned")))))
                    (pg::wal-input-validation-error () t))))
           (ok rejected (format nil "~s" fault))
           (ok (not (pg::physical-backup-lease-selected-cleanup-authority-active-p escaped)))
           (ok (handler-case
                   (progn
                     (pg::physical-backup-lease-selected-cleanup-claim escaped parent :retire) nil)
                 (pg::wal-input-validation-error () t)))
           (ok (equalp before (lineage-test-bytes bootstrap-path)))
           (ok (eq :closing (getf (gethash (getf request :name)
                                           pg::*physical-backup-lease-records*) :phase)))
           (ok (not pg::*wal-poisoned*))))
       t t)))

(deftest selected-cleanup-keeps-normal-closing-strict
    (dolist (allocated '(nil t))
      (call-with-backup-lease-selected-observation-fixture
       (lambda (root parent pin row)
         (declare (ignore root row))
         (let ((request (getf parent :request)))
           (pg::with-write-lock
             (sb-thread:with-mutex (pg::*wal-lock*)
               (setf (pg::physical-source-pin-lost-p pin) t)
               (pg::physical-source-pin-save pin)))
           (delete-file (pg::wal-segment-path 0))
           (ok (handler-case
                   (progn (pg::physical-backup-lease-finish-closing request) nil)
                 (pg::wal-input-validation-error () t)))
           (ok (probe-file (pg::physical-source-pin-path (getf request :name))))
           (ok (eq :closed
                   (getf (pg::physical-backup-lease-cleanup-selected request (getf parent :successor))
                         :phase)))
           (ok (not pg::*wal-poisoned*))))
       allocated t)))

(deftest selected-cleanup-rejects-recursive-completion-claims
    (dolist (operation '(:retired :done))
      (call-with-backup-lease-selected-observation-fixture
       (lambda (root parent pin row)
         (declare (ignore root pin row))
         (let ((original (symbol-function 'pg::physical-backup-lease-selected-cleanup-observation))
               (seen 0)
               (rejected nil))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-backup-lease-selected-cleanup-observation)
                        (lambda (record retired)
                          (let ((authority pg::*physical-backup-lease-selected-cleanup-authority*))
                            (when (and authority retired (zerop seen)
                                       (eq (pg::physical-backup-lease-selected-cleanup-authority-phase authority)
                                           (if (eq operation :retired) :retiring :publishing))
                                       (eq (getf record :phase)
                                           (if (eq operation :retired) :closing :closed)))
                              (incf seen)
                              (setf rejected
                                    (handler-case
                                        (progn
                                          (pg::physical-backup-lease-selected-cleanup-claim
                                           authority record operation) nil)
                                      (pg::wal-input-validation-error () t)))))
                          (funcall original record retired)))
                  (ok (eq :closed
                          (getf (pg::physical-backup-lease-cleanup-selected
                                 (getf parent :request) (getf parent :successor)) :phase)))
                  (ok (= 1 seen))
                  (ok rejected)
                  (ok (not pg::*wal-poisoned*)))
             (setf (symbol-function 'pg::physical-backup-lease-selected-cleanup-observation) original))))
       t t)))

(deftest selected-cleanup-requires-explicit-authority-at-private-entry-points
    (dolist (operation '(:finish :publish))
      (call-with-backup-lease-selected-observation-fixture
       (lambda (root parent pin row)
         (declare (ignore root pin row))
         (let* ((request (getf parent :request))
                (bootstrap-path (pg::physical-source-pin-path (getf request :name)))
                (before (lineage-test-bytes bootstrap-path))
                (next (pg::physical-backup-lease-copy parent)))
           (setf (getf next :phase) :closed)
           (pg::with-write-lock
             (pg::with-wal-publication-fence (begin-publication)
               (pg::call-with-physical-backup-lease-selected-cleanup
                parent
                (lambda (authority)
                  (ok (handler-case
                          (progn
                            (ecase operation
                              (:finish
                               (pg::physical-backup-lease-finish-closing-under-lock
                                request #'begin-publication))
                              (:publish
                               (pg::physical-backup-lease-publish-checked-under-lock
                                parent next #'begin-publication)))
                            nil)
                        (pg::wal-input-validation-error (condition)
                          (not (null (search "explicit active authority" (princ-to-string condition)))))))
                  (ok (equalp before (lineage-test-bytes bootstrap-path)))
                  (ok (equal parent (pg::physical-backup-lease-current-record request)))
                  (pg::physical-backup-lease-finish-closing-under-lock
                   request #'begin-publication authority)))))
           (ok (not pg::*wal-poisoned*))))
       t t)))

(deftest selected-cleanup-authority-is-unusable-during-acquisition
    (call-with-backup-lease-selected-observation-fixture
     (lambda (root parent pin row)
       (declare (ignore root pin row))
       (let ((original (symbol-function 'pg::physical-backup-lease-selected-cleanup-observation))
             (seen 0)
             (claim-rejected nil)
             (entry-rejected nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-backup-lease-selected-cleanup-observation)
                      (lambda (record retired)
                        (let ((authority pg::*physical-backup-lease-selected-cleanup-authority*))
                          (when (and authority (zerop seen)
                                     (eq :acquiring
                                         (pg::physical-backup-lease-selected-cleanup-authority-phase authority)))
                            (incf seen)
                            (setf claim-rejected
                                  (handler-case
                                      (progn
                                        (pg::physical-backup-lease-selected-cleanup-claim
                                         authority record :retire) nil)
                                    (pg::wal-input-validation-error () t))
                                  entry-rejected
                                  (handler-case
                                      (progn
                                        (pg::physical-backup-lease-selected-cleanup-check-entry authority) nil)
                                    (pg::wal-input-validation-error () t)))))
                        (funcall original record retired)))
                (ok (eq :closed
                        (getf (pg::physical-backup-lease-cleanup-selected
                               (getf parent :request) (getf parent :successor)) :phase)))
                (ok (= 1 seen))
                (ok claim-rejected)
                (ok entry-rejected)
                (ok (not pg::*wal-poisoned*)))
           (setf (symbol-function 'pg::physical-backup-lease-selected-cleanup-observation) original))))
     t t))

(deftest selected-cleanup-verifier-is-immutable-and-expired-scopes-stay-dead
    (call-with-backup-lease-selected-observation-fixture
     (lambda (root parent pin row)
       (declare (ignore root pin row))
       (let ((escaped nil)
             (request (getf parent :request)))
         (pg::with-write-lock
           (pg::with-wal-publication-fence (begin-publication)
             (pg::call-with-physical-backup-lease-selected-cleanup
              parent
              (lambda (authority)
                (setf escaped authority)
                (ok (not (fboundp '(setf pg::physical-backup-lease-selected-cleanup-authority-check))))
                (ok (handler-case
                        (progn
                          (funcall
                           (fdefinition '(setf pg::physical-backup-lease-selected-cleanup-authority-check))
                           (lambda (&rest arguments) (declare (ignore arguments))) authority)
                          nil)
                      (undefined-function () t)))
                (pg::physical-backup-lease-finish-closing-under-lock
                 request #'begin-publication authority)))))
         (let ((pg::*physical-backup-lease-selected-cleanup-authority* escaped)
               (closed (pg::physical-backup-lease-copy parent)))
           (setf (getf closed :phase) :closed
                 (pg::physical-backup-lease-selected-cleanup-authority-active-p escaped) t)
           (unwind-protect
                (ok (handler-case
                        (progn
                          (pg::physical-backup-lease-selected-cleanup-claim escaped closed :done) nil)
                      (pg::wal-input-validation-error () t)))
             (setf (pg::physical-backup-lease-selected-cleanup-authority-active-p escaped) nil)))
         (ok (not pg::*wal-poisoned*))))
     t t))

(deftest selected-cleanup-expired-unused-authority-cannot-be-revived
    (call-with-backup-lease-selected-observation-fixture
     (lambda (root parent pin row)
       (declare (ignore root pin row))
       (let ((escaped nil))
         (pg::with-write-lock
           (pg::with-wal-publication-fence (begin-publication)
             (ok (handler-case
                     (pg::call-with-physical-backup-lease-selected-cleanup
                      parent
                      (lambda (authority)
                        (setf escaped authority)
                        (pg::wal-invalid-input "leave unused cleanup scope")))
                   (pg::wal-input-validation-error () t)))
             (let ((pg::*physical-backup-lease-selected-cleanup-authority* escaped))
               (setf (pg::physical-backup-lease-selected-cleanup-authority-active-p escaped) t)
               (unwind-protect
                    (ok (handler-case
                            (progn
                              (pg::physical-backup-lease-selected-cleanup-claim
                               escaped parent :retire) nil)
                          (pg::wal-input-validation-error () t)))
                 (setf (pg::physical-backup-lease-selected-cleanup-authority-active-p escaped) nil)))))
         (ok (probe-file (pg::physical-source-pin-path (getf (getf parent :request) :name))))
         (ok (not pg::*wal-poisoned*))))
     t t))

(deftest selected-cleanup-preserves-selection-across-source-roles
    (dolist (allocated '(nil t))
      (dolist (standby '(nil t))
        (dolist (state '(:usable :lost :missing :writer :absent))
          (call-with-backup-lease-selected-observation-fixture
           (lambda (root parent pin row)
             (declare (ignore root))
             (let* ((request (getf parent :request))
                    (successor (getf parent :successor))
                    (name (getf successor :name))
                    (path (pg::physical-source-pin-path name))
                    (expected (pg::physical-backup-lease-copy parent))
                    (row-before (when row
                                  (lineage-test-bytes (pg::physical-backup-lease-allocation-path row)))))
               (case state
                 (:lost
                  (pg::with-write-lock
                    (sb-thread:with-mutex (pg::*wal-lock*)
                      (setf (pg::physical-source-pin-lost-p pin) t)
                      (pg::physical-source-pin-save pin))))
                 (:missing
                  (backup-lease-selected-startup-add-suffix pin)
                  (pg::with-write-lock
                    (sb-thread:with-mutex (pg::*wal-lock*)
                      (delete-file (pg::wal-segment-path 0)))))
                 (:absent
                  (pg::with-write-lock
                    (sb-thread:with-mutex (pg::*wal-lock*)
                      (delete-file path)
                      (pg::fsync-directory (pg::physical-source-pin-directory))
                      (remhash name pg::*physical-source-pins*)
                      (remhash pin pg::*wal-snapshot-retention-pins*)))))
               (let ((selected-before (unless (eq state :absent) (lineage-test-bytes path)))
                     (pg::*wal-insert-timeline* (if (eq state :writer) 3 2))
                     (pg::*physical-recovery-state* (when standby (list :phase 2)))
                     (pg::*physical-source-pins*
                       (if standby (make-hash-table :test #'equal) pg::*physical-source-pins*))
                     (pg::*wal-snapshot-retention-pins*
                       (if standby (make-hash-table :test #'eq) pg::*wal-snapshot-retention-pins*)))
                 (setf (getf expected :phase) :closed)
                 (ok (equal expected (pg::physical-backup-lease-cleanup-selected request successor)))
                 (ok (equal expected (pg::physical-backup-lease-cleanup-selected request successor)))
                 (ok (not (probe-file (pg::physical-source-pin-path (getf request :name)))))
                 (ok (if selected-before
                         (equalp selected-before (lineage-test-bytes path))
                         (not (probe-file path))))
                 (ok (= (if allocated 1 0) (hash-table-count pg::*physical-backup-lease-allocations*)))
                 (when row
                   (ok (equalp row-before
                               (lineage-test-bytes (pg::physical-backup-lease-allocation-path row)))))
                 (when standby
                   (ok (zerop (hash-table-count pg::*physical-source-pins*)))
                   (ok (zerop (hash-table-count pg::*wal-snapshot-retention-pins*))))
                 (ok (not pg::*wal-poisoned*)))))
           allocated t)))))

(deftest selected-cleanup-authenticates-closed-retries-with-progress-or-retirement
    (dolist (allocated '(nil t))
      (call-with-backup-lease-selected-observation-fixture
       (lambda (root parent pin row)
         (declare (ignore root row))
         (let ((request (getf parent :request))
               (successor (getf parent :successor))
               (name (pg::physical-source-pin-name pin)))
           (pg::with-write-lock
             (sb-thread:with-mutex (pg::*wal-lock*)
               (setf (pg::physical-source-pin-lost-p pin) t
                     (pg::physical-source-pin-confirmed-lsn pin) 160
                     (pg::physical-source-pin-restart-lsn pin) 160)
               (pg::physical-source-pin-save pin)))
           (pg::with-write-lock
             (sb-thread:with-mutex (pg::*wal-lock*)
               (delete-file (pg::wal-segment-path 0))))
           (let ((before (lineage-test-bytes (pg::physical-source-pin-path name))))
             (ok (equal parent (pg::physical-backup-lease-cleanup-selected request successor)))
             (ok (equalp before (lineage-test-bytes (pg::physical-source-pin-path name))))
             (ok (eq :released (pg::physical-source-follow-release name (getf successor :anchor-digest))))
             (ok (equal parent (pg::physical-backup-lease-cleanup-selected request successor)))
             (ok (eq :absent (pg::physical-source-follow-release name (getf successor :anchor-digest))))
             (ok (handler-case
                     (progn
                       (pg::physical-source-follow-prepare
                        name (pg::physical-follow-anchor-request (pg::physical-follow-pin-anchor pin)))
                       nil)
                   (pg::wal-input-validation-error () t)))
             (ok (not pg::*wal-poisoned*)))))
       allocated nil)))

(deftest selected-cleanup-rejects-metadata-and-active-owners-before-unlink
    (dolist (allocated '(nil t))
      (dolist (fault '(:request :successor :pin-digest :retaining :advanced
                       :disk-only :registry-only :capture :bootstrap-sender :selected-sender))
        (call-with-backup-lease-selected-observation-fixture
         (lambda (root parent pin row)
           (declare (ignore root row))
           (let* ((request (pg::physical-backup-lease-request-copy (getf parent :request)))
                  (successor (pg::physical-backup-lease-successor-copy (getf parent :successor) request))
                  (name (getf successor :name))
                  (bootstrap-path (pg::physical-source-pin-path (getf request :name)))
                  (before (lineage-test-bytes bootstrap-path))
                  (parent-before (lineage-test-bytes (pg::physical-backup-lease-path request))))
             (pg::with-write-lock
               (sb-thread:with-mutex (pg::*wal-lock*)
                 (ecase fault
                   (:request (setf (getf request :token) (make-string 64 :initial-element #\b)))
                   (:successor (incf (getf successor :anchor-digest)))
                   (:pin-digest
                    (incf (getf (pg::physical-follow-pin-anchor pin) :manifest-digest))
                    (incf (pg::physical-source-pin-manifest-digest pin))
                    (pg::physical-source-pin-save pin))
                   (:retaining
                    (setf (pg::physical-follow-pin-stage pin) :retaining
                          (pg::physical-source-pin-confirmed-lsn pin) nil
                          (pg::physical-source-pin-restart-lsn pin) 0)
                    (pg::physical-source-pin-save pin))
                   (:advanced
                    (incf (pg::physical-source-pin-confirmed-lsn pin) 8)
                    (incf (pg::physical-source-pin-restart-lsn pin) 8)
                    (pg::physical-source-pin-save pin))
                   (:disk-only (remhash name pg::*physical-source-pins*))
                   (:registry-only (delete-file (pg::physical-source-pin-path name)))
                   (:capture
                    (setf (gethash (getf request :name) pg::*physical-backup-lease-capture-owners*) :test-owner))
                   (:bootstrap-sender
                    (setf (gethash (getf request :name) pg::*physical-source-senders*) :test-owner))
                   (:selected-sender
                    (setf (gethash name pg::*physical-source-senders*) :test-owner)))))
             (let ((selected-before (when (probe-file (pg::physical-source-pin-path name))
                                      (lineage-test-bytes (pg::physical-source-pin-path name))))
                   (registered-before (gethash name pg::*physical-source-pins*))
                   (floor-before (gethash pin pg::*wal-snapshot-retention-pins*)))
               (ok (handler-case
                       (progn (pg::physical-backup-lease-cleanup-selected request successor) nil)
                     (pg::wal-input-validation-error () t)))
               (ok (if selected-before
                       (equalp selected-before
                               (lineage-test-bytes (pg::physical-source-pin-path name)))
                       (not (probe-file (pg::physical-source-pin-path name)))))
               (ok (eq registered-before (gethash name pg::*physical-source-pins*)))
               (ok (eql floor-before (gethash pin pg::*wal-snapshot-retention-pins*)))
               (ok (not pg::*wal-poisoned*)))
             (ok (equalp before (lineage-test-bytes bootstrap-path)))
             (ok (equalp parent-before
                         (lineage-test-bytes (pg::physical-backup-lease-path (getf parent :request)))))))
         allocated t))))

(deftest selected-cleanup-rechecks-frozen-ownership-at-both-mutation-boundaries
    (dolist (operation '(:retire :publish))
      (call-with-backup-lease-selected-observation-fixture
       (lambda (root parent pin row)
         (declare (ignore root row))
         (let* ((request (getf parent :request))
                (parent-before (lineage-test-bytes (pg::physical-backup-lease-path request)))
                (original (symbol-function 'pg::physical-backup-lease-selected-cleanup-claim))
                (seen 0)
                (unlinks 0)
                (writes 0)
                (pg::*physical-source-test-hook*
                  (lambda (point candidate)
                    (when (and (eq point :before-release-unlink)
                               (equal (getf request :name) (pg::physical-source-pin-name candidate)))
                      (incf unlinks))))
                (pg::*physical-backup-lease-test-hook*
                  (lambda (point record)
                    (when (and (eq point :before-write) (eq :closed (getf record :phase)))
                      (incf writes)))))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-backup-lease-selected-cleanup-claim)
                        (lambda (authority record actual &optional candidate)
                          (when (and (eq actual operation) (zerop seen))
                            (incf seen)
                            (setf (pg::physical-source-pin-lost-p pin) t)
                            (pg::physical-source-pin-save pin))
                          (funcall original authority record actual candidate)))
                  (ok (handler-case
                          (progn
                            (pg::physical-backup-lease-cleanup-selected request (getf parent :successor))
                            nil)
                        (error () t)))
                  (ok (= 1 seen))
                  (ok (= (if (eq operation :retire) 0 1) unlinks))
                  (ok (zerop writes))
                  (ok (equalp parent-before
                              (lineage-test-bytes (pg::physical-backup-lease-path request))))
                  (ok (eq :closing
                          (getf (gethash (getf request :name) pg::*physical-backup-lease-records*) :phase)))
                  (ok (eq (not (null pg::*wal-poisoned*)) (eq operation :publish))))
             (setf (symbol-function 'pg::physical-backup-lease-selected-cleanup-claim) original))))
       t t)))

(deftest selected-cleanup-rejects-authority-on-another-thread
    (call-with-backup-lease-selected-observation-fixture
     (lambda (root parent pin row)
       (declare (ignore root pin row))
       (pg::with-write-lock
         (pg::with-wal-publication-fence (begin-publication)
           (pg::call-with-physical-backup-lease-selected-cleanup
            parent
            (lambda (authority)
              (let ((thread nil)
                    (rejected nil)
                    (backend pg::*backend*)
                    (incarnation pg::*wal-storage-incarnation*)
                    (root pg::*pgdata*))
                (unwind-protect
                     (progn
                       (sb-sys:without-interrupts
                         (setf thread
                               (sb-thread:make-thread
                                (lambda ()
                                  (let ((pg::*physical-backup-lease-selected-cleanup-authority* authority)
                                        (pg::*backend* backend)
                                        (pg::*wal-storage-incarnation* incarnation)
                                        (pg::*pgdata* root))
                                    (setf rejected
                                          (handler-case
                                              (progn
                                                (pg::physical-backup-lease-selected-cleanup-claim
                                                 authority parent :retire) nil)
                                            (error (condition)
                                              (typep condition 'pg::wal-input-validation-error))))))
                                :name "selected-cleanup-wrong-thread")))
                       (sb-thread:join-thread thread)
                       (ok rejected))
                  (when (and thread (sb-thread:thread-alive-p thread))
                    (sb-thread:join-thread thread))))
              (pg::physical-backup-lease-finish-closing-under-lock
               (getf parent :request) #'begin-publication authority)))))
       (ok (not pg::*wal-poisoned*)))
     t t))

(defun selected-cleanup-recovery-operation (pin absent)
  (format nil
          "(let* ((request (getf parent :request))
                  (successor (getf parent :successor))
                  (name (getf successor :name))
                  (expected (pg::physical-backup-lease-copy parent))
                  (receipt (pg::physical-backup-lease-cleanup-selected request successor)))
             (setf (getf expected :phase) :closed)
             (assert (equal expected receipt))
             (assert (equal receipt (pg::physical-backup-lease-cleanup-selected request successor)))
             (assert (equal receipt (pg::physical-backup-lease-read
                                    (pg::physical-backup-lease-path request) :stabilize t)))
             (assert (not (probe-file (pg::physical-source-pin-path (getf request :name)))))
             (assert (= (if allocated 1 0) (hash-table-count pg::*physical-backup-lease-allocations*)))
             (maphash (lambda (key row)
                        (declare (ignore key))
                        (assert (eq :live (getf row :state))))
                      pg::*physical-backup-lease-allocations*)
             (if ~s
                 (assert (not (probe-file (pg::physical-source-pin-path name))))
                 (assert (equal '~s
                                (coerce (pg::physical-source-pin-encode
                                         (gethash name pg::*physical-source-pins*)) 'list))))
             (assert (eq ~s (pg::physical-source-follow-release name (getf successor :anchor-digest))))
             (assert (eq :absent (pg::physical-source-follow-release name (getf successor :anchor-digest))))
             (assert (handler-case
                         (progn (pg::physical-source-follow-prepare name coordinates) nil)
                       (pg::wal-input-validation-error () t)))
             (assert (not pg::*wal-poisoned*)))"
          absent
          (unless absent (coerce (pg::physical-source-pin-encode pin) 'list))
          (if absent :absent :released)))

(deftest selected-cleanup-recovers-real-kills-before-and-after-terminal-publication
    (with-physical-store-directory (cache-root)
      (let ((cache (uiop:ensure-directory-pathname
                    (sb-posix:mkdtemp
                     (namestring (merge-pathnames "selected-cleanup.XXXXXX" cache-root))))))
        (dolist (allocated '(nil t))
          (dolist (absent '(nil t))
            (dolist (cut '(:before-release-unlink :after-release-unlink
                           :after-release-directory-fsync :after-release-memory
                           :before-write :after-write :after-file-fsync
                           :after-rename :after-directory-fsync :before-memory))
              (call-with-backup-lease-selected-observation-fixture
               (lambda (root parent pin row)
                 (declare (ignore row))
                 (let* ((request (getf parent :request))
                        (name (pg::physical-source-pin-name pin))
                        (path (pg::physical-source-pin-path name))
                        (before nil))
                   (pg::with-write-lock
                     (pg::with-wal-publication-fence (begin-publication)
                       (begin-publication)
                       (if absent
                           (progn
                             (delete-file path)
                             (pg::fsync-directory (pg::physical-source-pin-directory))
                             (remhash name pg::*physical-source-pins*)
                             (remhash pin pg::*wal-snapshot-retention-pins*))
                           (progn
                             (setf (pg::physical-source-pin-lost-p pin) t)
                             (pg::physical-source-pin-save pin)
                             (setf before (lineage-test-bytes path))))))
                   (backup-lease-selected-startup-add-suffix pin)
                   (pg::with-write-lock
                     (sb-thread:with-mutex (pg::*wal-lock*)
                       (delete-file (pg::wal-segment-path 0))))
                   (dolist (purpose '(:cut :recover))
                     (let* ((log (merge-pathnames
                                  (format nil "cleanup-~(~a~)-~(~a~).log" cut purpose) root))
                            (pin-cut (member cut '(:before-release-unlink :after-release-unlink
                                                   :after-release-directory-fsync :after-release-memory)))
                            (setup
                              (if (eq purpose :recover)
                                  "(values)"
                                  (format nil
                                          "(setf ~a
                                               (lambda (point value)
                                                 (when (and (eq point ~s) ~a)
                                                   (stop-at-cut))))"
                                          (if pin-cut "pg::*physical-source-test-hook*"
                                              "pg::*physical-backup-lease-test-hook*")
                                          cut
                                          (if pin-cut
                                              (format nil "(equal ~s (pg::physical-source-pin-name value))"
                                                      (getf request :name))
                                              "(eq :closed (getf value :phase))"))))
                            (operation
                              (if (eq purpose :recover)
                                  (selected-cleanup-recovery-operation pin absent)
                                  "(pg::physical-backup-lease-cleanup-selected
                                  (getf parent :request) (getf parent :successor))
                                 (error \"selected cleanup missed its crash cut\")")))
                       (multiple-value-bind (result pid)
                           (reseed-follow-store-run-child
                            root log
                            (selected-startup-recovery-child-body
                             parent pin allocated t cut setup operation)
                            cache)
                         (format t "~a" (uiop:read-file-string log))
                         (format t "SELECTED-CLEANUP-CHILD-OWNED ~s~%"
                                 (list pid (namestring root) allocated absent cut purpose result))
                         (ok (equal (if (eq purpose :cut) '(:signaled 9) '(:exited 0)) result)
                             (format nil "~s ~s ~a" cut purpose log))))
                     (if (eq purpose :cut)
                         (ok (if absent (not (probe-file path))
                                 (equalp before (lineage-test-bytes path))))
                         (ok (not (probe-file path)))))))
               allocated t)))))))

(deftest selected-cleanup-fences-sync-and-readback-failures
    (dolist (fault '(:bootstrap-sync :closed-file-sync :closed-directory-sync :closed-readback))
      (call-with-backup-lease-selected-observation-fixture
       (lambda (root parent pin row)
         (declare (ignore root row))
         (let* ((request (getf parent :request))
                (name (pg::physical-source-pin-name pin))
                (path (pg::physical-source-pin-path name))
                (parent-path (pg::physical-backup-lease-path request))
                (bootstrap (gethash (getf request :name) pg::*physical-source-pins*))
                (original-directory (symbol-function 'pg::fsync-directory))
                (original-file (symbol-function 'pg::fsync-stream))
                (original-read (symbol-function 'pg::physical-backup-lease-read))
                (unlinked nil)
                (closed-written nil)
                (seen 0)
                (selected-before nil)
                (pg::*physical-backup-lease-test-hook*
                  (lambda (point record)
                    (when (and (eq point :after-write) (eq :closed (getf record :phase)))
                      (setf closed-written t))))
                (pg::*physical-source-test-hook*
                  (lambda (point candidate)
                    (when (and (eq point :after-release-unlink)
                               (equal (getf request :name) (pg::physical-source-pin-name candidate)))
                      (setf unlinked t)))))
           (pg::with-write-lock
             (sb-thread:with-mutex (pg::*wal-lock*)
               (setf (pg::physical-source-pin-lost-p pin) t)
               (pg::physical-source-pin-save pin)))
           (setf selected-before (lineage-test-bytes path))
           (delete-file (pg::wal-segment-path 0))
           (labels ((fail ()
                      (incf seen)
                      (error "selected cleanup injected sync/readback failure")))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::fsync-directory)
                          (lambda (directory)
                            (let ((authority pg::*physical-backup-lease-selected-cleanup-authority*))
                              (when (and authority (zerop seen)
                                         (if (eq fault :bootstrap-sync)
                                             (and unlinked
                                                  (equal directory (pg::physical-source-pin-directory)))
                                             (and (eq fault :closed-directory-sync)
                                                  (eq :publishing
                                                      (pg::physical-backup-lease-selected-cleanup-authority-phase
                                                       authority))
                                                  (equal directory (pg::physical-backup-lease-directory)))))
                                (fail)))
                            (funcall original-directory directory))
                          (symbol-function 'pg::fsync-stream)
                          (lambda (stream)
                            (when (and (eq fault :closed-file-sync) (zerop seen)
                                       pg::*physical-backup-lease-selected-cleanup-authority*
                                       closed-written (output-stream-p stream))
                              (fail))
                            (funcall original-file stream))
                          (symbol-function 'pg::physical-backup-lease-read)
                          (lambda (target &key stabilize)
                            (when (and (eq fault :closed-readback) (zerop seen) stabilize
                                       pg::*physical-backup-lease-selected-cleanup-authority*
                                       (equal target parent-path))
                              (fail))
                            (funcall original-read target :stabilize stabilize)))
                    (ok (handler-case
                            (progn
                              (pg::physical-backup-lease-cleanup-selected request (getf parent :successor))
                              nil)
                          (error () t)))
                    (ok (= 1 seen))
                    (ok unlinked)
                    (ok pg::*wal-poisoned*)
                    (ok (equalp selected-before (lineage-test-bytes path)))
                    (ok (eq :closing
                            (getf (gethash (getf request :name) pg::*physical-backup-lease-records*) :phase)))
                    (ok (eq (not (null (gethash bootstrap pg::*wal-snapshot-retention-pins*)))
                            (eq fault :bootstrap-sync))))
               (setf (symbol-function 'pg::fsync-directory) original-directory
                     (symbol-function 'pg::fsync-stream) original-file
                     (symbol-function 'pg::physical-backup-lease-read) original-read)))
           (let ((pg::*wal-poisoned* nil)
                 (pg::*physical-source-test-hook* nil))
             (backup-lease-startup-reopen)
             (ok (eq :closed
                     (getf (pg::physical-backup-lease-cleanup-selected request (getf parent :successor))
                           :phase)))
             (ok (equalp selected-before (lineage-test-bytes path)))
             (ok (not pg::*wal-poisoned*)))))
       t t)))

(deftest selected-cleanup-fences-nonlocal-retirement-and-publication-exits
    (dolist (point '(:before-release-unlink :after-release-unlink
                     :after-release-directory-fsync :after-release-memory
                     :before-write :after-write :after-rename :before-memory :completion))
      (call-with-backup-lease-selected-observation-fixture
       (lambda (root parent pin row)
         (declare (ignore root row))
         (let* ((request (getf parent :request))
                (name (getf request :name))
                (selected-path (pg::physical-source-pin-path (pg::physical-source-pin-name pin)))
                (original (symbol-function 'pg::physical-backup-lease-selected-cleanup-claim))
                (escaped nil)
                (seen 0)
                (selected-before nil))
           (pg::with-write-lock
             (sb-thread:with-mutex (pg::*wal-lock*)
               (setf (pg::physical-source-pin-lost-p pin) t)
               (pg::physical-source-pin-save pin)
               (delete-file (pg::wal-segment-path 0))))
           (setf selected-before (lineage-test-bytes selected-path))
           (flet ((cut ()
                    (incf seen)
                    (setf escaped pg::*physical-backup-lease-selected-cleanup-authority*)
                    (throw 'selected-cleanup-cut :cut)))
             (let ((pg::*physical-source-test-hook*
                     (lambda (actual candidate)
                       (when (and (eq actual point) (equal name (pg::physical-source-pin-name candidate)))
                         (cut))))
                   (pg::*physical-backup-lease-test-hook*
                     (lambda (actual record)
                       (when (and (eq actual point) (eq :closed (getf record :phase))) (cut)))))
               (unwind-protect
                    (progn
                      (setf (symbol-function 'pg::physical-backup-lease-selected-cleanup-claim)
                            (lambda (authority record operation &optional candidate)
                              (when (and (eq point :completion) (eq operation :done)) (cut))
                              (funcall original authority record operation candidate)))
                      (ok (intake-fails-p
                           (lambda ()
                             (catch 'selected-cleanup-cut
                               (pg::physical-backup-lease-cleanup-selected
                                request (getf parent :successor))))))
                      (ok (= 1 seen))
                      (ok pg::*wal-poisoned*)
                      (ok (not (pg::physical-backup-lease-selected-cleanup-authority-active-p escaped)))
                      (ok (equalp selected-before (lineage-test-bytes selected-path)))
                      (ok (eq pin (gethash (pg::physical-source-pin-name pin) pg::*physical-source-pins*)))
                      (ok (eq (if (eq point :completion) :closed :closing)
                              (getf (gethash name pg::*physical-backup-lease-records*) :phase)))
                      (ok (eq (if (member point '(:after-rename :before-memory :completion)) :closed :closing)
                              (getf (pg::physical-backup-lease-read
                                     (pg::physical-backup-lease-path request)) :phase))))
                 (setf (symbol-function 'pg::physical-backup-lease-selected-cleanup-claim) original))))
           (let ((pg::*wal-poisoned* nil)
                 (pg::*physical-source-test-hook* nil)
                 (pg::*physical-backup-lease-test-hook* nil))
             (backup-lease-startup-reopen)
             (ok (eq :closed
                     (getf (pg::physical-backup-lease-cleanup-selected request (getf parent :successor))
                           :phase)))
             (ok (equalp selected-before (lineage-test-bytes selected-path)))
             (ok (not pg::*wal-poisoned*)))))
       t t)))

(deftest selected-cleanup-scope-is-checked-before-unrelated-terminal-branches
    (call-with-backup-lease-selected-observation-fixture
     (lambda (root parent pin row)
       (declare (ignore root pin))
       (let* ((pg::*physical-backup-lease-limit* 4)
              (other (pg::physical-backup-lease-copy parent))
              (other-row (pg::physical-backup-lease-allocation-copy row))
              (closed (pg::physical-backup-lease-copy parent)))
         (setf (getf (getf other :request) :name) "reseed_2123456789abcdef0123456789abcdef"
               (getf other :phase) :ready
               (getf other :outcome) nil
               (getf other :successor) nil
               (getf other-row :request) (getf other :request)
               (getf other-row :name) "unrelated_cleanup_debt"
               (getf (getf closed :request) :name) "reseed_3123456789abcdef0123456789abcdef"
               (getf closed :phase) :closed
               (getf closed :outcome) :aborted
               (getf closed :successor) nil)
         (setf other (pg::physical-backup-lease-copy other)
               closed (pg::physical-backup-lease-copy closed))
         (pg::with-write-lock
           (pg::with-wal-publication-fence (begin-publication)
             (begin-publication)
             (dolist (record (list other closed))
               (pg::physical-backup-lease-write record)
               (setf (gethash (getf (getf record :request) :name)
                              pg::*physical-backup-lease-records*) record))
             (setf pg::*physical-backup-lease-successor-cache* nil)))
         (let ((bootstrap (backup-lease-allocation-store-test-save-bootstrap other)))
           (pg::physical-backup-lease-allocation-publish nil other-row)
           (let* ((other-pin (backup-lease-allocation-authority-save-pin other-row))
                  (closing (pg::physical-backup-lease-copy other)))
             (setf (getf closing :phase) :closing
                   (getf closing :outcome) :aborted
                   other (backup-lease-startup-inject-closing other closing))
             (let* ((request (getf other :request))
                    (parent-before (lineage-test-bytes (pg::physical-backup-lease-path request)))
                    (row-before (lineage-test-bytes (pg::physical-backup-lease-allocation-path other-row)))
                    (pin-before (lineage-test-bytes
                                 (pg::physical-source-pin-path (getf other-row :name))))
                    (bootstrap-before (lineage-test-bytes
                                       (pg::physical-source-pin-path (getf request :name)))))
               (pg::with-write-lock
                 (pg::with-wal-publication-fence (begin-publication)
                   (pg::call-with-physical-backup-lease-selected-cleanup
                    parent
                    (lambda (authority)
                      (dolist (unrelated (list other closed))
                        (ok (handler-case
                                (progn
                                  (pg::physical-backup-lease-finish-closing-under-lock
                                   (getf unrelated :request) #'begin-publication authority)
                                  nil)
                              (pg::wal-input-validation-error () t))))
                      (ok (equalp parent-before
                                  (lineage-test-bytes (pg::physical-backup-lease-path request))))
                      (ok (equalp row-before
                                  (lineage-test-bytes
                                   (pg::physical-backup-lease-allocation-path other-row))))
                      (ok (and (probe-file (pg::physical-source-pin-path (getf other-row :name)))
                               (equalp pin-before
                                       (lineage-test-bytes
                                        (pg::physical-source-pin-path (getf other-row :name))))))
                      (ok (equalp bootstrap-before
                                  (lineage-test-bytes
                                   (pg::physical-source-pin-path (getf request :name)))))
                      (ok (eq other-pin (gethash (getf other-row :name) pg::*physical-source-pins*)))
                      (ok (eql 152 (gethash other-pin pg::*wal-snapshot-retention-pins*)))
                      (ok (= 32 (gethash bootstrap pg::*wal-snapshot-retention-pins*)))
                      (pg::physical-backup-lease-finish-closing-under-lock
                       (getf parent :request) #'begin-publication authority)))))
               (ok (not pg::*wal-poisoned*)))))))
     t t))

(deftest selected-cleanup-claim-rejects-nil-outside-an-active-scope
    (ok (null pg::*physical-backup-lease-selected-cleanup-authority*))
  (ok (handler-case
          (progn (pg::physical-backup-lease-selected-cleanup-claim nil nil :retire) nil)
        (pg::wal-input-validation-error () t))))
