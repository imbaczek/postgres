(in-package #:pg-physical-tests)

(defun call-with-backup-lease-allocation-authority-fixture (function)
  (call-with-backup-lease-authority-fixture
   (lambda (root)
     (let* ((pg::*physical-follow-aborted-attempts* (make-hash-table :test #'equal))
            (parent (backup-lease-authority-prepare t))
            (row (backup-lease-allocation-test-record)))
       (backup-lease-startup-save-origin parent)
       (pg::timeline-write-history 2 1 32 "promotion")
       (with-open-file (stream (pg::wal-segment-path 0) :direction :io
                                                        :if-exists :overwrite
                                                        :element-type '(unsigned-byte 8))
         (write-sequence (legacy-checkpoint-record 0) stream)
         (pg::fsync-stream stream))
       (let* ((bytes (pg::read-bounded-regular-file (pg::wal-segment-path 0) 1024))
              (digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))
         (setf (getf (getf row :coordinates) :receive-digest) digest
               (getf (getf row :coordinates) :overlap-digest) digest))
       (funcall function root parent row)))))

(deftest backup-lease-allocation-publication-authenticates-and-detaches
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (let* ((expected (pg::physical-backup-lease-allocation-copy row))
              (observed nil)
              (pg::*physical-backup-lease-allocation-test-hook*
                (lambda (point value)
                  (when (eq point :before-memory)
                    (setf observed
                          (and (null (gethash (getf value :name) pg::*physical-backup-lease-allocations*))
                               (equal value (pg::physical-backup-lease-allocation-read
                                             (pg::physical-backup-lease-allocation-path value))))))))
              (result (pg::physical-backup-lease-allocation-publish nil row)))
         (ok observed)
         (ok (equal expected result))
         (ok (null (gethash (getf row :name) pg::*physical-source-pins*)))
         (setf (char (getf result :name) 0) #\b
               (char (getf (getf row :request) :token) 0) #\b)
         (ok (equal expected (gethash (getf expected :name) pg::*physical-backup-lease-allocations*)))
         (ok (equal expected (pg::physical-backup-lease-allocation-publish expected expected)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-publication-rejects-clean-caller-errors
    (dolist (fault '(:token :base :digest :generation :capacity :foreign-pin))
      (call-with-backup-lease-allocation-authority-fixture
       (lambda (root parent row)
         (declare (ignore root parent))
         (let ((pg::*physical-backup-lease-allocation-limit* 65536))
           (ecase fault
             (:token (setf (char (getf (getf row :request) :token) 0) #\b))
             (:base (decf (getf (getf row :coordinates) :base)))
             (:digest (incf (getf (getf row :coordinates) :manifest-digest)))
             (:generation (setf (getf row :generation) 2))
             (:capacity (setf pg::*physical-backup-lease-allocation-limit* 0))
             (:foreign-pin
              (let ((anchor (pg::physical-source-follow-prepare (getf row :name)
                                                                (getf row :coordinates))))
                (ok anchor))))
           (ok (pg::physical-backup-lease-allocation-copy row))
           (ok (backup-lease-authority-error-p
                (lambda () (pg::physical-backup-lease-allocation-publish nil row)))
               (format nil "~s" fault))
           (ok (not pg::*wal-poisoned*))
           (ok (zerop (hash-table-count pg::*physical-backup-lease-allocations*)))
           (ok (not (probe-file (pg::physical-backup-lease-allocation-path row)))))))))

(deftest backup-lease-allocation-publication-canceled-first-needs-no-old-wal
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (delete-file (pg::wal-segment-path 0))
       (setf (getf row :state) :canceled)
       (let ((pg::*wal-insert-timeline* 3))
         (ok (equal row (pg::physical-backup-lease-allocation-publish nil row)))
         (ok (intake-fails-p
              (lambda ()
                (let ((revived (pg::physical-backup-lease-allocation-copy row)))
                  (setf (getf revived :state) :live)
                  (pg::physical-backup-lease-allocation-publish row revived)))))
         (ok (not pg::*wal-poisoned*))
         (ok (equal parent (gethash (getf (getf parent :request) :name)
                                    pg::*physical-backup-lease-records*)))))))

(deftest backup-lease-allocation-publication-checks-disk-before-memory
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (pg::physical-backup-lease-allocation-publish nil row)
       (let* ((registered (gethash (getf row :name) pg::*physical-backup-lease-allocations*))
              (canceled (pg::physical-backup-lease-allocation-copy row))
              (path (pg::physical-backup-lease-allocation-path row)))
         (setf (getf canceled :state) :canceled)
         (delete-file path)
         (ok (intake-fails-p
              (lambda () (pg::physical-backup-lease-allocation-publish row canceled))))
         (ok pg::*wal-poisoned*)
         (ok (eq registered (gethash (getf row :name) pg::*physical-backup-lease-allocations*)))))))

(deftest backup-lease-allocation-publication-keeps-history-across-restart
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (pg::physical-backup-lease-allocation-publish nil row)
       (let ((canceled (pg::physical-backup-lease-allocation-copy row))
             (next (pg::physical-backup-lease-allocation-copy row)))
         (setf (getf canceled :state) :canceled
               (getf next :generation) 2 (getf next :name) "next_allocated_follow")
         (pg::physical-backup-lease-allocation-publish row canceled)
         (pg::physical-backup-lease-load-records)
         (ok (equal canceled
                    (gethash (getf row :name) pg::*physical-backup-lease-allocations*)))
         (ok (equal next (pg::physical-backup-lease-allocation-publish nil next)))
         (ok (= 2 (hash-table-count pg::*physical-backup-lease-allocations*)))
         (ok (intake-fails-p
              (lambda () (pg::physical-backup-lease-allocation-publish nil row))))
         (ok (not pg::*wal-poisoned*))))))

(defun backup-lease-allocation-authority-save-pin (row)
  (let* ((coordinates (getf row :coordinates))
         (anchor (pg::physical-source-follow-anchor coordinates))
         (cursor (getf coordinates :receive-lsn))
         (pin (pg::make-physical-follow-pin
               :name (copy-seq (getf row :name)) :system-id (getf coordinates :system-id)
               :timeline (getf coordinates :source-timeline)
               :manifest-digest (getf coordinates :manifest-digest)
               :consistent-lsn cursor :confirmed-lsn cursor :restart-lsn cursor
               :prefix-digest (getf coordinates :receive-digest) :anchor anchor :stage :ready)))
    (pg::with-write-lock
      (pg::with-wal-publication-fence (begin-publication)
        (pg::physical-follow-pin-check-fields pin)
        (begin-publication)
        (pg::physical-source-pin-save pin)
        (setf (gethash (getf row :name) pg::*physical-source-pins*) pin
              (gethash pin pg::*wal-snapshot-retention-pins*) cursor)))
    pin))

(deftest backup-lease-allocation-publication-waits-for-predecessor-pin-cleanup
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (pg::physical-backup-lease-allocation-publish nil row)
       (let* ((pin (backup-lease-allocation-authority-save-pin row))
              (canceled (pg::physical-backup-lease-allocation-copy row))
              (next (pg::physical-backup-lease-allocation-copy row)))
         (ok (pg::physical-follow-pin-ready-p pin))
         (setf (getf canceled :state) :canceled
               (getf next :generation) 2 (getf next :name) "next_allocated_follow")
         (pg::physical-backup-lease-allocation-publish row canceled)
         (ok (backup-lease-authority-error-p
              (lambda () (pg::physical-backup-lease-allocation-publish nil next))))
         (ok (eq pin (gethash (getf row :name) pg::*physical-source-pins*)))
         (ok (gethash pin pg::*wal-snapshot-retention-pins*))
         (ok (not (probe-file (pg::physical-backup-lease-allocation-path next))))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-publication-syncs-predecessor-absence-before-next-row
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (setf (getf row :state) :canceled)
       (pg::physical-backup-lease-allocation-publish nil row)
       (let* ((next (backup-lease-allocation-test-record :live 2))
              (original (fdefinition 'pg::fsync-directory))
              (synced nil)
              (observed nil)
              (pg::*physical-backup-lease-allocation-test-hook*
                (lambda (point value)
                  (declare (ignore value))
                  (when (eq point :before-write) (setf observed synced)))))
         (setf (getf next :coordinates) (copy-list (getf row :coordinates))
               (getf next :name) "next_allocated_follow")
         (unwind-protect
              (progn
                (setf (fdefinition 'pg::fsync-directory)
                      (lambda (directory)
                        (prog1 (funcall original directory)
                          (when (equal directory (pg::physical-source-pin-directory))
                            (setf synced t)))))
                (pg::physical-backup-lease-allocation-publish nil next))
           (setf (fdefinition 'pg::fsync-directory) original))
         (ok synced)
         (ok observed)
         (ok (equal next (pg::physical-backup-lease-allocation-read
                          (pg::physical-backup-lease-allocation-path next))))))))

(deftest backup-lease-allocation-publication-faults-never-expose-uncommitted-memory
    (dolist (point '(:before-write :after-write :after-file-fsync :after-rename
                     :after-directory-fsync :before-memory))
      (dolist (mode '(:error :nonlocal))
        (call-with-backup-lease-allocation-authority-fixture
         (lambda (root parent row)
           (declare (ignore root parent))
           (let* ((seen nil)
                  (pg::*physical-backup-lease-allocation-test-hook*
                    (lambda (actual value)
                      (declare (ignore value))
                      (when (eq point actual)
                        (setf seen t)
                        (if (eq mode :error) (error "allocation publication cut")
                            (throw 'allocation-publication-cut :cut))))))
             (ok (intake-fails-p
                  (lambda ()
                    (catch 'allocation-publication-cut
                      (pg::physical-backup-lease-allocation-publish nil row)))))
             (ok seen)
             (ok pg::*wal-poisoned*)
             (ok (zerop (hash-table-count pg::*physical-backup-lease-allocations*)))
             (let ((path (pg::physical-backup-lease-allocation-path row)))
               (when (probe-file path)
                 (ok (equal row (pg::physical-backup-lease-allocation-read path)))))))))))

(deftest backup-lease-allocation-publication-revokes-aborted-parent-debt-without-wal
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (pg::physical-backup-lease-allocation-publish nil row)
       (let* ((closed (pg::physical-backup-lease-abort (getf parent :request)))
              (canceled (pg::physical-backup-lease-allocation-copy row))
              (next (pg::physical-backup-lease-allocation-copy row)))
         (ok (eq :aborted (getf closed :outcome)))
         (ok (eq :closed (getf closed :phase)))
         (delete-file (pg::wal-segment-path 0))
         (setf (getf canceled :state) :canceled
               (getf next :state) :canceled (getf next :generation) 2
               (getf next :name) "never_allocated_follow")
         (ok (equal canceled (pg::physical-backup-lease-allocation-publish row canceled)))
         (ok (backup-lease-authority-error-p
              (lambda () (pg::physical-backup-lease-allocation-publish nil next))))
         (ok (not (probe-file (pg::physical-backup-lease-allocation-path next))))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-publication-cannot-cancel-a-selected-handoff
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (pg::physical-backup-lease-allocation-publish nil row)
       (let* ((pin (backup-lease-allocation-authority-save-pin row))
              (successor (list :name (getf row :name)
                               :anchor-digest (pg::physical-follow-anchor-digest
                                               (pg::physical-follow-pin-anchor pin))))
              (closed (pg::physical-backup-lease-handoff (getf parent :request) successor))
              (canceled (pg::physical-backup-lease-allocation-copy row)))
         (ok (eq :closed (getf closed :phase)))
         (ok (eq :handed-off (getf closed :outcome)))
         (setf (getf canceled :state) :canceled)
         (ok (backup-lease-authority-error-p
              (lambda () (pg::physical-backup-lease-allocation-publish row canceled))))
         (ok (equal row (pg::physical-backup-lease-allocation-publish row row)))
         (ok (eq pin (gethash (getf row :name) pg::*physical-source-pins*)))
         (ok (gethash pin pg::*wal-snapshot-retention-pins*))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-publication-fail-stops-on-corrupt-parent-metadata
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (let* ((path (pg::physical-backup-lease-path (getf parent :request)))
              (bad (append parent '(:unexpected t)))
              (bytes (pg::physical-backup-lease-encode-literal "PGBACKUPLEASE1" bad 8192))
              (registered pg::*physical-backup-lease-records*))
         (with-open-file (stream path :direction :output :if-exists :supersede
                                      :element-type '(unsigned-byte 8))
           (write-sequence bytes stream))
         (sb-posix:chmod (namestring path) #o600)
         (ok (intake-fails-p
              (lambda () (pg::physical-backup-lease-allocation-publish nil row))))
         (ok pg::*wal-poisoned*)
         (ok (eq registered pg::*physical-backup-lease-records*))
         (ok (zerop (hash-table-count pg::*physical-backup-lease-allocations*)))
         (ok (equalp bytes (lineage-test-bytes path)))))))

(deftest backup-lease-allocation-publication-syncs-an-absent-pin-directory-parent
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore parent))
       (setf (getf row :state) :canceled)
       (pg::physical-backup-lease-allocation-publish nil row)
       (sb-posix:rename (namestring (pg::physical-source-pin-directory))
                        (namestring (merge-pathnames "saved-pins/" root)))
       (let* ((next (pg::physical-backup-lease-allocation-copy row))
              (original (fdefinition 'pg::fsync-directory))
              (synced nil)
              (observed nil)
              (pg::*physical-backup-lease-allocation-test-hook*
                (lambda (point value)
                  (declare (ignore value))
                  (when (eq point :before-write) (setf observed synced)))))
         (setf (getf next :generation) 2 (getf next :name) "next_canceled_follow")
         (unwind-protect
              (progn
                (setf (fdefinition 'pg::fsync-directory)
                      (lambda (directory)
                        (prog1 (funcall original directory)
                          (when (equal directory root) (setf synced t)))))
                (pg::physical-backup-lease-allocation-publish nil next))
           (setf (fdefinition 'pg::fsync-directory) original))
         (ok observed)
         (ok (equal next (pg::physical-backup-lease-allocation-read
                          (pg::physical-backup-lease-allocation-path next))))))))

(defun backup-lease-allocation-authority-check-rejection (row function)
  (let* ((name (getf row :name))
         (path (pg::physical-source-pin-path name))
         (before (when (probe-file path) (lineage-test-bytes path)))
         (pin-count (hash-table-count pg::*physical-source-pins*))
         (lease-count (hash-table-count pg::*physical-backup-lease-records*))
         (aborted-count (hash-table-count pg::*physical-follow-aborted-attempts*)))
    (ok (backup-lease-authority-error-p function))
    (ok (not pg::*wal-poisoned*))
    (ok (= pin-count (hash-table-count pg::*physical-source-pins*)))
    (ok (= lease-count (hash-table-count pg::*physical-backup-lease-records*)))
    (ok (= aborted-count (hash-table-count pg::*physical-follow-aborted-attempts*)))
    (ok (equalp before (when (probe-file path) (lineage-test-bytes path))))))

(deftest backup-lease-allocation-guards-ordinary-prepare-with-and-without-pins
    (dolist (state '(:live :canceled))
      (dolist (present '(nil t))
        (dolist (operation '(:existing :prepare :reserve :ready))
          (call-with-backup-lease-allocation-authority-fixture
           (lambda (root parent row)
             (declare (ignore root parent))
             (setf (getf row :state) state)
             (pg::physical-backup-lease-allocation-publish nil row)
             (when present (backup-lease-allocation-authority-save-pin row))
             (let ((name (getf row :name))
                   (coordinates (getf row :coordinates))
                   (anchor (pg::physical-source-follow-anchor (getf row :coordinates))))
               (backup-lease-allocation-authority-check-rejection
                row
                (lambda ()
                  (ecase operation
                    (:existing (pg::physical-source-follow-existing-anchor name coordinates))
                    (:prepare (pg::physical-source-follow-prepare name coordinates))
                    (:reserve (pg::physical-source-follow-reserve name anchor))
                    (:ready (pg::physical-source-follow-ready name anchor))))))))))))

(deftest backup-lease-allocation-guards-bootstrap-and-new-lease-namespace
    (dolist (state '(:live :canceled))
      (dolist (operation '(:bootstrap :lease))
        (call-with-backup-lease-allocation-authority-fixture
         (lambda (root parent row)
           (declare (ignore root parent))
           (setf (getf row :state) state
                 (getf row :name) "reseed_fedcba9876543210fedcba9876543210")
           (pg::physical-backup-lease-allocation-publish nil row)
           (let ((candidate (backup-lease-test-record)))
             (setf (getf (getf candidate :request) :name) (copy-seq (getf row :name)))
             (backup-lease-allocation-authority-check-rejection
              row
              (lambda ()
                (ecase operation
                  (:bootstrap (pg::physical-source-pin-create (getf row :name)))
                  (:lease (pg::physical-backup-lease-publish nil candidate)))))))))))

(deftest backup-lease-allocation-guards-ordinary-retirement-before-ram-fences
    (dolist (state '(:live :canceled))
      (dolist (present '(nil t))
        (dolist (operation (if present '(:abort :release :local) '(:abort :release)))
          (call-with-backup-lease-allocation-authority-fixture
           (lambda (root parent row)
             (declare (ignore root parent))
             (setf (getf row :state) state)
             (pg::physical-backup-lease-allocation-publish nil row)
             (when present (backup-lease-allocation-authority-save-pin row))
             (let ((name (getf row :name))
                   (coordinates (getf row :coordinates))
                   (digest (pg::physical-follow-anchor-digest
                            (pg::physical-source-follow-anchor (getf row :coordinates)))))
               (backup-lease-allocation-authority-check-rejection
                row
                (lambda ()
                  (ecase operation
                    (:abort (pg::physical-source-follow-abort name coordinates))
                    (:release (pg::physical-source-follow-release name digest))
                    (:local (pg::physical-source-pin-release name))))))))))))

(deftest backup-lease-allocation-context-prepares-only-within-its-active-extent
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (pg::physical-backup-lease-allocation-publish nil row)
       (let* ((escaped nil)
              (anchor
                (pg::call-with-physical-backup-lease-allocation-context
                 :prepare row
                 (lambda ()
                   (setf escaped pg::*physical-backup-lease-allocation-context*)
                   (pg::physical-source-follow-prepare (getf row :name) (getf row :coordinates)))))
              (pin (gethash (getf row :name) pg::*physical-source-pins*)))
         (ok (pg::physical-follow-pin-ready-p pin))
         (ok (pg::physical-follow-request-matches-anchor-p
              (getf row :coordinates) (pg::physical-follow-pin-anchor anchor)))
         (ok (not (pg::physical-backup-lease-allocation-context-active escaped)))
         (let ((text (write-to-string escaped)))
           (ok (not (search (getf (getf row :request) :token) text)))
           (ok (not (search (getf row :name) text))))
         (let ((pg::*physical-backup-lease-allocation-context* escaped))
           (backup-lease-allocation-authority-check-rejection
            row (lambda () (pg::physical-source-follow-existing-anchor
                            (getf row :name) (getf row :coordinates)))))
         (backup-lease-allocation-authority-check-rejection
          row (lambda () (pg::physical-source-follow-prepare
                          (getf row :name) (getf row :coordinates))))))))

(deftest backup-lease-allocation-context-checks-backend-and-storage-incarnation
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (pg::physical-backup-lease-allocation-publish nil row)
       (pg::call-with-physical-backup-lease-allocation-context
        :prepare row
        (lambda ()
          (let ((pg::*backend* (pg::make-backend :pid 997 :thread sb-thread:*current-thread*)))
            (backup-lease-allocation-authority-check-rejection
             row (lambda () (pg::physical-source-follow-existing-anchor
                             (getf row :name) (getf row :coordinates)))))
          (let ((pg::*wal-storage-incarnation* (gensym "OTHER-STORE-")))
            (backup-lease-allocation-authority-check-rejection
             row (lambda () (pg::physical-source-follow-existing-anchor
                             (getf row :name) (getf row :coordinates))))))))))

(deftest backup-lease-allocation-context-cancellation-during-proof-blocks-ready-publication
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (pg::physical-backup-lease-allocation-publish nil row)
       (let* ((canceled (pg::physical-backup-lease-allocation-copy row))
              (seen nil)
              (pg::*physical-source-test-hook*
                (lambda (point pin)
                  (declare (ignore pin))
                  (when (eq point :after-follow-proof)
                    (setf seen t)
                    (pg::physical-backup-lease-allocation-publish row canceled)))))
         (setf (getf canceled :state) :canceled)
         (ok (backup-lease-authority-error-p
              (lambda ()
                (pg::call-with-physical-backup-lease-allocation-context
                 :prepare row
                 (lambda () (pg::physical-source-follow-prepare
                             (getf row :name) (getf row :coordinates)))))))
         (ok seen)
         (let ((pin (gethash (getf row :name) pg::*physical-source-pins*)))
           (ok pin)
           (ok (not (pg::physical-follow-pin-ready-p pin)))
           (ok (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (equal canceled (gethash (getf row :name) pg::*physical-backup-lease-allocations*)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-context-retires-canceled-pins-without-old-wal
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (pg::physical-backup-lease-allocation-publish nil row)
       (pg::call-with-physical-backup-lease-allocation-context
        :prepare row (lambda () (pg::physical-source-follow-prepare
                                 (getf row :name) (getf row :coordinates))))
       (let ((pin (gethash (getf row :name) pg::*physical-source-pins*))
             (canceled (pg::physical-backup-lease-allocation-copy row)))
         (setf (getf canceled :state) :canceled)
         (pg::physical-backup-lease-allocation-publish row canceled)
         (delete-file (pg::wal-segment-path 0))
         (pg::call-with-physical-backup-lease-allocation-context
          :retire canceled
          (lambda ()
            (pg::with-write-lock
              (pg::with-wal-publication-fence (begin-publication)
                (begin-publication)
                (pg::physical-source-pin-unlink pin)))))
         (ok (null (gethash (getf row :name) pg::*physical-source-pins*)))
         (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (not (probe-file (pg::physical-source-pin-path (getf row :name)))))
         (ok (equal canceled (pg::physical-backup-lease-allocation-read
                              (pg::physical-backup-lease-allocation-path canceled))))
         (ok (zerop (hash-table-count pg::*physical-follow-aborted-attempts*)))
         (ok (not pg::*wal-poisoned*))))))

(defun backup-lease-allocation-authority-start-command (row)
  (let ((coordinates (getf row :coordinates)))
    (pg::parse-replication-command
     (format nil
             "START_REPLICATION PHYSICAL 0/00000098 (native_format '~a', mode 'follow', proof_pin '~a', system_id '~d', timeline '2', base_lsn '0/00000098', manifest_digest '~d', prefix_digest '~d')"
             pg::+physical-native-format+ (getf row :name) (getf coordinates :system-id)
             (getf coordinates :manifest-digest) (getf coordinates :receive-digest)))))

(deftest backup-lease-allocation-guards-sender-and-feedback-before-handoff
    (dolist (state '(:live :canceled))
      (call-with-backup-lease-allocation-authority-fixture
       (lambda (root parent row)
         (declare (ignore root parent))
         (setf (getf row :state) state)
         (pg::physical-backup-lease-allocation-publish nil row)
         (let* ((pin (backup-lease-allocation-authority-save-pin row))
                (name (getf row :name))
                (command (backup-lease-allocation-authority-start-command row))
                (owner (gensym "UNEXPECTED-SENDER-"))
                (before (lineage-test-bytes (pg::physical-source-pin-path name))))
           (ok (backup-lease-authority-error-p
                (lambda () (pg::physical-source-acquire-sender command))))
           (ok (zerop (hash-table-count pg::*physical-source-senders*)))
           (unwind-protect
                (progn
                  (setf (gethash name pg::*physical-source-senders*) owner)
                  (ok (backup-lease-authority-error-p
                       (lambda () (pg::physical-source-confirm-receipt
                                   name owner 152 (getf (getf row :coordinates) :receive-digest))))))
             (remhash name pg::*physical-source-senders*))
           (ok (= 152 (gethash pin pg::*wal-snapshot-retention-pins*)))
           (ok (equalp before (lineage-test-bytes (pg::physical-source-pin-path name))))
           (ok (not pg::*wal-poisoned*)))))))

(deftest backup-lease-allocation-closed-handoff-allows-progress-and-ordinary-retirement
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (pg::physical-backup-lease-allocation-publish nil row)
       (let* ((pin (backup-lease-allocation-authority-save-pin row))
              (name (getf row :name))
              (anchor-digest (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin)))
              (closed (pg::physical-backup-lease-handoff
                       (getf parent :request) (list :name name :anchor-digest anchor-digest)))
              (command (backup-lease-allocation-authority-start-command row))
              (owner (pg::physical-source-acquire-sender command))
              (raw (pg::wal-encode-record pg::+wal-record-commit+ 3 152
                                          (pg::wal-encode-commit-timestamp 0)))
              (endpoint (+ 152 (length raw))))
         (ok (eq :closed (getf closed :phase)))
         (unwind-protect
              (progn
                (with-open-file (stream (pg::wal-segment-path 0) :direction :output
                                                                 :if-exists :append
                                                                 :element-type '(unsigned-byte 8))
                  (write-sequence raw stream)
                  (pg::fsync-stream stream))
                (setf pg::*wal-end-lsn* endpoint pg::*wal-durable-lsn* endpoint)
                (multiple-value-bind (digest proof)
                    (pg::physical-source-prove-range
                     name owner 152 endpoint (getf (getf row :coordinates) :receive-digest))
                  (pg::physical-source-confirm-receipt name owner endpoint digest proof))
                (ok (= endpoint (pg::physical-source-pin-confirmed-lsn pin)))
                (ok (= endpoint (gethash pin pg::*wal-snapshot-retention-pins*)))
                (sb-thread:with-mutex (pg::*wal-lock*)
                  (ok (eq pin (pg::physical-source-check-sender-under-lock name owner)))))
           (pg::physical-source-release-sender name owner))
         (ok (eq :released (pg::physical-source-follow-release name anchor-digest)))
         (ok (eq :absent (pg::physical-source-follow-release name anchor-digest)))
         (backup-lease-allocation-authority-check-rejection
          row (lambda () (pg::physical-source-follow-prepare name (getf row :coordinates))))
         (ok (equal row (gethash name pg::*physical-backup-lease-allocations*)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-context-retries-ready-prepare-during-and-after-handoff
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (pg::physical-backup-lease-allocation-publish nil row)
       (let* ((pin (backup-lease-allocation-authority-save-pin row))
              (name (getf row :name))
              (path (pg::physical-source-pin-path name))
              (before (lineage-test-bytes path))
              (closing (copy-tree parent))
              (proved nil)
              (pg::*physical-source-test-hook*
                (lambda (point value)
                  (declare (ignore value))
                  (when (eq point :after-follow-proof) (setf proved t)))))
         (setf (getf closing :phase) :closing (getf closing :outcome) :handed-off
               (getf closing :successor)
               (list :name name :anchor-digest
                     (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin))))
         (pg::physical-backup-lease-publish parent closing)
         (dolist (phase '(:closing :closed))
           (when (eq phase :closed)
             (pg::physical-backup-lease-finish-closing (getf parent :request)))
           (ok (eq pin
                   (pg::call-with-physical-backup-lease-allocation-context
                    :prepare row (lambda () (pg::physical-source-follow-prepare
                                             name (getf row :coordinates))))))
           (backup-lease-allocation-authority-check-rejection
            row (lambda () (pg::physical-source-follow-prepare name (getf row :coordinates))))
           (ok (equalp before (lineage-test-bytes path)))
           (ok (not proved)))))))

(deftest backup-lease-allocation-handoff-rejects-canceled-or-rebound-preparation
    (dolist (fault '(:canceled :coordinates :unallocated-successor))
      (call-with-backup-lease-allocation-authority-fixture
       (lambda (root parent row)
         (declare (ignore root))
         (pg::physical-backup-lease-allocation-publish nil row)
         (let ((candidate (pg::physical-backup-lease-allocation-copy row)))
           (when (eq fault :coordinates)
             (incf (getf (getf candidate :coordinates) :receive-digest)))
           (when (eq fault :unallocated-successor)
             (setf (getf candidate :name) "unallocated_successor"))
           (let* ((pin (backup-lease-allocation-authority-save-pin candidate))
                  (closing (copy-tree parent))
                  (name (getf candidate :name))
                  (parent-path (pg::physical-backup-lease-path (getf parent :request)))
                  (parent-bytes (lineage-test-bytes parent-path))
                  (pin-bytes (lineage-test-bytes (pg::physical-source-pin-path name))))
             (when (eq fault :canceled)
               (let ((canceled (pg::physical-backup-lease-allocation-copy row)))
                 (setf (getf canceled :state) :canceled)
                 (pg::physical-backup-lease-allocation-publish row canceled)))
             (setf (getf closing :phase) :closing (getf closing :outcome) :handed-off
                   (getf closing :successor)
                   (list :name name :anchor-digest
                         (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin))))
             (ok (backup-lease-authority-error-p
                  (lambda () (pg::physical-backup-lease-publish parent closing))))
             (ok (not pg::*wal-poisoned*))
             (ok (equalp parent-bytes (lineage-test-bytes parent-path)))
             (ok (equalp pin-bytes (lineage-test-bytes (pg::physical-source-pin-path name))))
             (ok (eq pin (gethash name pg::*physical-source-pins*)))))))))

(defun backup-lease-allocation-call (function row)
  (funcall function (getf row :request) (getf row :generation)
           (getf row :name) (getf row :coordinates)))

(deftest backup-lease-allocation-entry-prepare-persists-before-pin-publication
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (let* ((seen 0)
              (pg::*physical-source-test-hook*
                (lambda (point pin)
                  (when (and (eq point :before-save)
                             (equal (getf row :name) (pg::physical-source-pin-name pin)))
                    (ok (equal row (pg::physical-backup-lease-allocation-read
                                    (pg::physical-backup-lease-allocation-path row))))
                    (ok (equal row (gethash (getf row :name) pg::*physical-backup-lease-allocations*)))
                    (incf seen))))
              (pin (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare row)))
         (ok (pg::physical-follow-pin-ready-p pin))
         (ok (>= seen 2))
         (ok (eq pin (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare row)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-entry-cancel-before-prepare-permanently-fences-intent
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (let ((result (backup-lease-allocation-call #'pg::physical-backup-lease-follow-cancel row)))
         (ok (equal parent result))
         (ok (eq :canceled (getf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) :state)))
         (ok (backup-lease-authority-error-p
              (lambda () (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare row))))
         (ok (null (gethash (getf row :name) pg::*physical-source-pins*)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-entry-cancel-syncs-revocation-before-retiring-pin
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (let* ((pin (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare row))
              (canceled (pg::physical-backup-lease-allocation-copy row))
              (unlink-seen nil) (sync-seen nil)
              (pg::*physical-source-test-hook*
                (lambda (point value)
                  (when (eq pin value)
                    (case point
                      (:before-release-unlink
                       (setf unlink-seen
                             (and (equal canceled (pg::physical-backup-lease-allocation-read
                                                   (pg::physical-backup-lease-allocation-path row)))
                                  (gethash pin pg::*wal-snapshot-retention-pins*))))
                      (:after-release-directory-fsync
                       (setf sync-seen (gethash pin pg::*wal-snapshot-retention-pins*))))))))
         (setf (getf canceled :state) :canceled)
         (ok (equal parent (backup-lease-allocation-call #'pg::physical-backup-lease-follow-cancel row)))
         (ok unlink-seen) (ok sync-seen)
         (ok (null (gethash (getf row :name) pg::*physical-source-pins*)))
         (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (not (probe-file (pg::physical-source-pin-path (getf row :name)))))
         (ok (equal canceled (gethash (getf row :name) pg::*physical-backup-lease-allocations*)))
         (ok (equal parent (backup-lease-allocation-call #'pg::physical-backup-lease-follow-cancel row)))
         (ok (zerop (hash-table-count pg::*physical-follow-aborted-attempts*)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-entry-cancel-settles-handoff-without-retiring-successor
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (let* ((pin (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare row))
              (name (getf row :name))
              (digest (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin)))
              (closed (pg::physical-backup-lease-handoff
                       (getf parent :request) (list :name name :anchor-digest digest)))
              (before (lineage-test-bytes (pg::physical-source-pin-path name))))
         (ok (equal closed (backup-lease-allocation-call #'pg::physical-backup-lease-follow-cancel row)))
         (ok (eq pin (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare row)))
         (ok (equalp before (lineage-test-bytes (pg::physical-source-pin-path name))))
         (ok (equal row (gethash name pg::*physical-backup-lease-allocations*)))
         (pg::physical-source-follow-release name digest)
         (ok (backup-lease-authority-error-p
              (lambda () (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare row))))
         (ok (equal closed (backup-lease-allocation-call #'pg::physical-backup-lease-follow-cancel row)))
         (ok (not (probe-file (pg::physical-source-pin-path name))))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-entry-aborted-parent-does-not-claim-a-foreign-pin
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (let* ((pin (pg::physical-source-follow-prepare (getf row :name) (getf row :coordinates)))
              (closed (pg::physical-backup-lease-abort (getf parent :request)))
              (before (lineage-test-bytes (pg::physical-source-pin-path (getf row :name)))))
         (ok (equal closed (backup-lease-allocation-call #'pg::physical-backup-lease-follow-cancel row)))
         (ok (zerop (hash-table-count pg::*physical-backup-lease-allocations*)))
         (ok (eq pin (gethash (getf row :name) pg::*physical-source-pins*)))
         (ok (equalp before (lineage-test-bytes (pg::physical-source-pin-path (getf row :name)))))
         (ok (backup-lease-authority-error-p
              (lambda () (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare row))))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-entry-busy-cancel-keeps-live-intent-and-wal-healthy
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (let* ((pin (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare row))
              (name (getf row :name))
              (owner (gensym "BUSY-ALLOCATION-")))
         (unwind-protect
              (progn
                (setf (gethash name pg::*physical-source-senders*) owner)
                (ok (handler-case
                        (progn (backup-lease-allocation-call #'pg::physical-backup-lease-follow-cancel row)
                               nil)
                      (pg::pg-error (condition) (equal "55006" (pg::pg-error-sqlstate condition)))))
                (ok (equal row (gethash name pg::*physical-backup-lease-allocations*)))
                (ok (eq pin (gethash name pg::*physical-source-pins*)))
                (ok (not pg::*wal-poisoned*)))
           (remhash name pg::*physical-source-senders*))
         (backup-lease-allocation-call #'pg::physical-backup-lease-follow-cancel row)
         (ok (null (gethash name pg::*physical-source-pins*)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-entry-cancels-aborted-parent-debt-without-old-wal
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (let* ((pin (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare row))
              (closed (pg::physical-backup-lease-abort (getf parent :request))))
         (delete-file (pg::wal-segment-path 0))
         (ok (equal closed (backup-lease-allocation-call #'pg::physical-backup-lease-follow-cancel row)))
         (ok (null (gethash (getf row :name) pg::*physical-source-pins*)))
         (ok (null (gethash pin pg::*wal-snapshot-retention-pins*)))
         (ok (eq :canceled (getf (gethash (getf row :name) pg::*physical-backup-lease-allocations*) :state)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-entry-old-cancel-keeps-the-new-handed-off-sender
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare row)
       (backup-lease-allocation-call #'pg::physical-backup-lease-follow-cancel row)
       (let ((next (pg::physical-backup-lease-allocation-copy row)))
         (setf (getf next :generation) 2 (getf next :name) "new_selected_follow")
         (let* ((pin (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare next))
                (digest (pg::physical-follow-anchor-digest (pg::physical-follow-pin-anchor pin)))
                (closed (pg::physical-backup-lease-handoff
                         (getf parent :request) (list :name (getf next :name) :anchor-digest digest)))
                (owner (pg::physical-source-acquire-sender
                        (backup-lease-allocation-authority-start-command next)))
                (before (lineage-test-bytes (pg::physical-source-pin-path (getf next :name)))))
           (unwind-protect
                (progn
                  (ok (equal closed (backup-lease-allocation-call
                                     #'pg::physical-backup-lease-follow-cancel row)))
                  (ok (eq pin (gethash (getf next :name) pg::*physical-source-pins*)))
                  (ok (eq owner (gethash (getf next :name) pg::*physical-source-senders*)))
                  (ok (gethash pin pg::*wal-snapshot-retention-pins*))
                  (ok (equalp before (lineage-test-bytes
                                      (pg::physical-source-pin-path (getf next :name)))))
                  (ok (equal next (gethash (getf next :name) pg::*physical-backup-lease-allocations*))))
             (pg::physical-source-release-sender (getf next :name) owner))
           (ok (not pg::*wal-poisoned*)))))))

(cl-spec:defproperty backup-lease-allocation-entry-traces-never-revive-canceled-names
    ((count (range integer 1 6)) (cancel-first boolean))
  (:about pg::physical-backup-lease-follow-prepare pg::physical-backup-lease-follow-cancel)
  (:trials (:normal 80))
  (call-with-backup-lease-allocation-authority-fixture
   (lambda (root parent template)
     (declare (ignore root parent))
     (let ((first nil))
       (and
        (loop for generation from 1 to (1+ count)
              always
              (let ((row (pg::physical-backup-lease-allocation-copy template)))
                (setf (getf row :generation) generation
                      (getf row :name) (format nil "generated_follow_~d" generation))
                (unless first (setf first row))
                (if (and cancel-first (> generation 1))
                    (progn
                      (backup-lease-allocation-call #'pg::physical-backup-lease-follow-cancel row)
                      (unless (intake-fails-p
                               (lambda () (backup-lease-allocation-call
                                           #'pg::physical-backup-lease-follow-prepare row)))
                        (return nil)))
                    (let ((pin (backup-lease-allocation-call
                                #'pg::physical-backup-lease-follow-prepare row)))
                      (unless (and (pg::physical-follow-pin-ready-p pin)
                                   (eq pin (backup-lease-allocation-call
                                            #'pg::physical-backup-lease-follow-prepare row)))
                        (return nil))
                      (backup-lease-allocation-call #'pg::physical-backup-lease-follow-cancel row)))
                (and (eq :canceled (getf (gethash (getf row :name)
                                                  pg::*physical-backup-lease-allocations*) :state))
                     (null (gethash (getf row :name) pg::*physical-source-pins*))
                     (not (probe-file (pg::physical-source-pin-path (getf row :name)))))))
        (= (1+ count) (hash-table-count pg::*physical-backup-lease-allocations*))
        (intake-fails-p (lambda () (backup-lease-allocation-call
                                    #'pg::physical-backup-lease-follow-prepare first)))
        (not pg::*wal-poisoned*))))))

(deftest generated-backup-lease-allocation-entry-cancellation-traces
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property
                    'backup-lease-allocation-entry-traces-never-revive-canceled-names
                    :seed 107 :profile :normal))))
      (ok (eq :passed (getf result :status)))))

(deftest backup-lease-allocation-revoke-primitive-never-settles-its-parent
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root))
       (backup-lease-allocation-call #'pg::physical-backup-lease-follow-prepare row)
       (let ((closing (copy-tree parent))
             (canceled (pg::physical-backup-lease-allocation-copy row)))
         (setf (getf closing :phase) :closing (getf closing :outcome) :aborted
               (getf canceled :state) :canceled)
         (pg::physical-backup-lease-publish parent closing)
         (pg::with-write-lock
           (pg::with-wal-publication-fence (begin-publication)
             (pg::physical-backup-lease-allocation-revoke-under-lock row canceled #'begin-publication)))
         (ok (equal closing (gethash (getf (getf parent :request) :name)
                                     pg::*physical-backup-lease-records*)))
         (ok (probe-file (pg::physical-source-pin-path (getf (getf parent :request) :name))))
         (ok (not (probe-file (pg::physical-source-pin-path (getf row :name)))))
         (ok (equal canceled (gethash (getf row :name) pg::*physical-backup-lease-allocations*)))
         (ok (eq :closed (getf (pg::physical-backup-lease-finish-closing
                                (getf parent :request)) :phase)))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-reserve-checks-the-owned-anchor-snapshot
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (pg::physical-backup-lease-allocation-publish nil row)
       (let* ((coordinates (copy-list (getf row :coordinates)))
              (original (fdefinition 'pg::physical-follow-pin-encode))
              (seen nil))
         (incf (getf coordinates :receive-digest))
         (let ((input (pg::physical-source-follow-anchor coordinates)))
           (unwind-protect
                (progn
                  (setf (fdefinition 'pg::physical-follow-pin-encode)
                        (lambda (pin)
                          (prog1 (funcall original pin)
                            (unless seen
                              (setf seen t
                                    (getf input :receive-digest)
                                    (getf (getf row :coordinates) :receive-digest))))))
                  (ok (backup-lease-authority-error-p
                       (lambda ()
                         (pg::call-with-physical-backup-lease-allocation-context
                          :prepare row
                          (lambda () (pg::physical-source-follow-reserve (getf row :name) input)))))))
             (setf (fdefinition 'pg::physical-follow-pin-encode) original)))
         (ok seen)
         (ok (null (gethash (getf row :name) pg::*physical-source-pins*)))
         (ok (not (probe-file (pg::physical-source-pin-path (getf row :name)))))
         (ok (not pg::*wal-poisoned*))))))

(deftest backup-lease-allocation-ready-checks-the-reserved-pin-name-after-proof
    (call-with-backup-lease-allocation-authority-fixture
     (lambda (root parent row)
       (declare (ignore root parent))
       (pg::physical-backup-lease-allocation-publish nil row)
       (let* ((input-name (copy-seq (getf row :name)))
              (anchor (pg::physical-source-follow-anchor (getf row :coordinates)))
              (canceled (pg::physical-backup-lease-allocation-copy row))
              (seen nil)
              (pg::*physical-source-test-hook*
                (lambda (point pin)
                  (declare (ignore pin))
                  (when (eq point :after-follow-proof)
                    (pg::physical-backup-lease-allocation-publish row canceled)
                    (setf (char input-name 0) #\x seen t)))))
         (setf (getf canceled :state) :canceled)
         (ok (backup-lease-authority-error-p
              (lambda ()
                (pg::call-with-physical-backup-lease-allocation-context
                 :prepare row (lambda () (pg::physical-source-follow-ready input-name anchor))))))
         (ok seen)
         (let ((pin (gethash (getf row :name) pg::*physical-source-pins*)))
           (ok pin)
           (ok (not (pg::physical-follow-pin-ready-p pin))))
         (ok (not pg::*wal-poisoned*))))))
