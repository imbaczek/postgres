;;;; Pending intent is bound to durable data and never opens writer admission.
(in-package #:pg-physical-tests)

(defun call-with-promotion-exclusion (function)
  (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
    (sb-thread:with-mutex (pg::*reload-run-lock*)
      (pg::with-live-operation-quiescence (funcall function)))))

(defun call-with-promotion-fixture (function &key (namespace :legacy) (system-id 123) (applied 216))
  (call-with-restartpoint-fixture
   (lambda (root old-store)
     (pg::physical-store-close old-store)
     (let* ((pg::*physical-retained-root-references* (make-hash-table :test #'eq))
            (pg::*physical-retained-root-holds* (make-hash-table :test #'eq))
            (pg::*physical-retained-root-updates* (make-hash-table :test #'eq))
            (snapshot (sample-checkpoint-snapshot :version 2 :next-xid 50 :oldest-active-xid 0))
            (base (concatenate '(vector (unsigned-byte 8)) (sample-physical-record 0)
                               (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 32
                                                      (pg::wal-encode-checkpoint-snapshot snapshot))))
            (control (pg::wal-encode-control-image 152 152 snapshot :namespace namespace))
            (name (concatenate 'string "pg_wal/" (pg::wal-segment-filename 0 1 namespace)))
            (store (progn
                     (pg::wal-ensure-directory-durable (merge-pathnames "promotion-spool/" root))
                     (pg::physical-store-open (merge-pathnames "promotion-spool/" root) system-id 1
                                              :create t :start-lsn 152)))
            (pg::*physical-receive-store* store) (pg::*physical-follow-runtime* nil)
            (pg::*physical-restartpoint-worker* nil) (pg::*physical-shutdown-state* nil)
            (pg::*shutdown-mode* nil) (pg::*shutdown-requested* nil)
            (pg::*swank-listener-enabled-p* nil)
            (pg::*wal-replication-slots* (make-hash-table :test #'equal))
            (pg::*physical-source-pins* (make-hash-table :test #'equal))
            (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
            (pg::*wal-ready* t) (pg::*wal-insert-timeline* 1) (pg::*wal-checkpoint-timeline* 1)
            (pg::*wal-checkpoint-lsn* 152) (pg::*wal-end-lsn* 152) (pg::*wal-durable-lsn* 152)
            (pg::*wal-segment-namespace* namespace) (pg::*last-replay-lsn* applied))
       (unwind-protect
            (progn
              (when (eq namespace :timeline) (delete-file (pg::wal-segment-path 0 1 :legacy)))
              (pg::wal-atomic-write-bytes (merge-pathnames name root) base)
              (pg::wal-atomic-write-bytes (pg::wal-control-path) control)
              (setf (getf pg::*physical-recovery-state* :version) 3
                    (getf pg::*physical-recovery-state* :phase) 2
                    (getf pg::*physical-recovery-state* :checkpoint) 152
                    (getf pg::*physical-recovery-state* :checkpoint-start) 32
                    (getf pg::*physical-recovery-state* :end) 152
                    (getf pg::*physical-recovery-state* :required-applied) applied
                    (getf pg::*physical-recovery-state* :wal-namespace) namespace
                    (getf pg::*physical-recovery-state* :source-checkpoint) snapshot
                    (getf pg::*physical-recovery-state* :source-reservation)
                    (list :version 1 :through applied :bound 1002 :complete t)
                    (getf pg::*physical-recovery-state* :files)
                    (list (list name (length base) (pg::physical-store-update-digest #xCBF29CE484222325 base))
                          (list "global/pg_wal_control" (length control)
                                (pg::physical-store-update-digest #xCBF29CE484222325 control))))
              (pg::wal-atomic-write-bytes
               (pg::physical-recovery-control-path)
               (let* ((manifest (list :version 3 :system-id system-id :cluster-identity "history-source"
                                      :timeline 1 :checkpoint 152 :checkpoint-start 32
                                      :end 152 :next-xid 50 :source-pin "copy"
                                      :files (getf pg::*physical-recovery-state* :files)))
                      (text (pg::physical-bootstrap-manifest-text manifest)))
                 (write-recovery-manifest-text root text)
                 (let ((digest (nth-value 1 (pg::recovery-read-manifest root))))
                   (setf (getf pg::*physical-recovery-state* :manifest-digest) digest
                         (pg::physical-store-retained-identity store)
                         (list :system-id system-id :base-timeline 1 :base 152 :manifest-digest digest)
                         (pg::physical-store-retained-directory-value store)
                         (pg::physical-recovery-retained-directory))
                   (pg::physical-recovery-control-image 2 system-id 1 digest 152 applied 50))))
              (pg::wal-atomic-write-bytes
               (merge-pathnames "recovery.required" root) (pg::physical-recovery-marker-bytes))
              (when (> applied 152)
                (pg::physical-store-input store 152 (sample-physical-record 152))
                (pg::physical-store-input store 184
                                          (pg::wal-encode-record pg::+wal-record-maintenance+ 0 184 (empty-octets))))
              (pg::physical-store-flush store)
              (pg::physical-store-compact store applied #'pg::physical-restartpoint-publish)
              (funcall function root store))
         (pg::physical-store-close store))))
   :system-id system-id))

(defmacro with-promotion-fixture ((root store &key (system-id 123)) &body body)
  `(call-with-promotion-fixture (lambda (,root ,store) ,@body) :system-id ,system-id))

(deftest promotion-child-prefix-replaces-stale-files-without-changing-parent
    (dolist (namespace '(:legacy :timeline))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root store))
         (let* ((parent (pg::wal-segment-path 0 1 namespace))
                (before (lineage-test-bytes parent))
                (control (lineage-test-bytes (pg::wal-control-path)))
                (child (pg::wal-segment-path 0 2 :timeline)))
           (pg::wal-atomic-write-bytes child #(9 8 7))
           (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent)
           (let ((intent (lineage-test-bytes (pg::physical-recovery-control-path))))
             (loop repeat 2 do
               (ok (equal child (call-with-promotion-exclusion #'pg::physical-promotion-install-prefix)))
               (ok (equalp (getf pg::*physical-recovery-state* :wal-prefix-bytes)
                           (lineage-test-bytes child))))
             (ok (equalp intent (lineage-test-bytes (pg::physical-recovery-control-path))))
             (ok (equalp control (lineage-test-bytes (pg::wal-control-path))))
             (ok (equalp before (lineage-test-bytes parent)))
             (ok (= 1 pg::*wal-insert-timeline* pg::*wal-checkpoint-timeline*))
             (ok (equal "57P03" (recovery-state-of #'pg::check-recovery-available)))
             (ok (null pg::*physical-recovery-failed-p*)))))
       :namespace namespace)))

(deftest promotion-child-prefix-rejects-changed-proof-or-special-destination
    (dolist (change '(:memory-child :memory-prefix :generation :checkpoint :reservation :special))
      (with-promotion-fixture (root store)
        (declare (ignore root store))
        (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent)
        (let ((child (pg::wal-segment-path 0 2 :timeline)))
          (pg::wal-atomic-write-bytes child #(1 2 3))
          (case change
            (:memory-child (setf (getf pg::*physical-recovery-state* :promotion-child) 3))
            (:memory-prefix (setf (getf pg::*physical-recovery-state* :wal-prefix-bytes) #(7)))
            (:generation (setf (getf pg::*physical-recovery-state* :promotion-generation) 0))
            (:checkpoint (setf (getf pg::*physical-recovery-state* :source-checkpoint) nil))
            (:reservation (setf (getf pg::*physical-recovery-state* :source-reservation) nil))
            (:special (delete-file child) (sb-posix:symlink "/missing-promotion-child" (namestring child))))
          (ok (intake-fails-p (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-install-prefix))))
          (ok (null pg::*physical-recovery-failed-p*))
          (unless (eq change :special) (ok (equalp #(1 2 3) (lineage-test-bytes child))))))))

(deftest promotion-child-prefix-preserves-later-history-and-eor
    (with-promotion-fixture (root store)
      (declare (ignore root store))
      (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent)
      (let* ((child (pg::wal-segment-path 0 2 :timeline))
             (bytes (concatenate '(vector (unsigned-byte 8))
                                 (getf pg::*physical-recovery-state* :wal-prefix-bytes)
                                 (pg::wal-encode-end-of-recovery-record 216 1 2 1 1))))
        (pg::wal-atomic-write-bytes child bytes)
        (pg::wal-atomic-write-bytes (pg::timeline-history-path 2 pg::*pgdata*) #(49 9 48 47 68 56 10))
        (ok (intake-fails-p (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-install-prefix))))
        (ok (equalp bytes (lineage-test-bytes child)))
        (ok (null pg::*physical-recovery-failed-p*)))))

(deftest promotion-child-prefix-failures-fence-and-clean-temporaries
    (dolist (point '(:before-prefix-fsync :after-prefix-fsync :after-prefix-directory-fsync :after-child-unlink
                     :after-child-unlink-directory-fsync :after-child-rename :after-child-directory-fsync))
      (dolist (nonlocal '(nil t))
        (with-promotion-fixture (root store)
          (declare (ignore root store))
          (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent)
          (let ((child (pg::wal-segment-path 0 2 :timeline)))
            (pg::wal-atomic-write-bytes child #(9))
            (let ((pg::*wal-timeline-test-hook*
                    (lambda (seen) (when (eq point seen)
                                     (if nonlocal (throw 'abort-install :aborted)
                                         (error "injected install failure"))))))
              (if nonlocal
                  (ok (eq :aborted (catch 'abort-install
                                     (call-with-promotion-exclusion #'pg::physical-promotion-install-prefix))))
                  (ok (intake-fails-p (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-install-prefix))))))
            (ok pg::*physical-recovery-failed-p*)
            (ok (= 3 (getf (pg::physical-recovery-read-control pg::*pgdata*) :phase)))
            (ok (null (timeline-test-staging-files)))
            ;; Simulate a new recovery attempt after the owning process has ended.
            (let ((pg::*physical-recovery-failed-p* nil))
              (call-with-promotion-exclusion #'pg::physical-promotion-install-prefix)
              (ok (equalp (getf pg::*physical-recovery-state* :wal-prefix-bytes)
                          (lineage-test-bytes child)))))))))

(cl-spec:defproperty child-installation-replaces-only-the-uncommitted-child
    ((stale (range integer 0 255)) (qualified (member nil t)))
  (:about pg::physical-promotion-install-prefix)
  (:trials (:normal 50))
  (call-with-promotion-fixture
   (lambda (root store)
     (declare (ignore root store))
     (let* ((parent (pg::wal-segment-path 0 1 pg::*wal-segment-namespace*))
            (before (lineage-test-bytes parent))
            (child (pg::wal-segment-path 0 2 :timeline)))
       (pg::wal-atomic-write-bytes child (make-array 300 :element-type '(unsigned-byte 8) :initial-element stale))
       (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent)
       (call-with-promotion-exclusion #'pg::physical-promotion-install-prefix)
       (and (equalp before (lineage-test-bytes parent))
            (equalp (getf pg::*physical-recovery-state* :wal-prefix-bytes) (lineage-test-bytes child))
            (= 1 pg::*wal-insert-timeline*) (not pg::*physical-recovery-failed-p*))))
   :namespace (if qualified :timeline :legacy)))

(deftest generated-promotion-child-prefix
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'child-installation-replaces-only-the-uncommitted-child :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest promotion-intent-binds-the-selected-generation-without-changing-parent-wal
    (dolist (namespace '(:legacy :timeline))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root store))
         (let ((stale (copy-list pg::*physical-recovery-state*))
               (base (lineage-test-bytes (pg::wal-control-path)))
               (pointer (lineage-test-bytes (pg::physical-restartpoint-pointer-path))))
           (multiple-value-bind (metadata catalog prefix generation)
               (pg::physical-restartpoint-read pg::*physical-recovery-state*)
             (declare (ignore metadata catalog))
             (let ((pending (call-with-promotion-exclusion
                             (lambda () (pg::physical-promotion-publish-intent 2)))))
               (ok (= 3 (getf pending :phase)))
               (ok (= 216 (getf pending :applied)))
               (ok (= generation (getf pending :promotion-generation)))
               (ok (eq namespace (getf pending :promotion-source-namespace)))
               (ok (equalp prefix (getf pending :wal-prefix-bytes)))
               (ok (= 50 (getf pending :next-xid)))
               (ok (= 1 pg::*wal-insert-timeline* pg::*wal-checkpoint-timeline*))
               (ok (= 152 pg::*wal-durable-lsn*))
               (ok (pg::recovery-in-progress-p))
               (ok (equal "57P03" (recovery-state-of #'pg::check-recovery-available)))
               (ok (null pg::*physical-recovery-failed-p*))))
           (ok (intake-fails-p (lambda () (pg::physical-recovery-write-control stale 2 216 50))))
           (let ((disk (pg::physical-recovery-read-control pg::*pgdata*)))
             (ok (= 3 (getf disk :phase))) (ok (= 2 (getf disk :promotion-child))))
           (ok (equalp base (lineage-test-bytes (pg::wal-control-path))))
           (ok (equalp pointer (lineage-test-bytes (pg::physical-restartpoint-pointer-path))))))
       :namespace namespace)))

(deftest promotion-intent-preflight-rejects-missing-proof-without-fencing-recovery
    (dolist (key '(:source-checkpoint :source-reservation :wal-prefix-digest))
      (with-promotion-fixture (root store)
        (declare (ignore root store))
        (multiple-value-bind (metadata catalog prefix) (pg::physical-restartpoint-read pg::*physical-recovery-state*)
          (setf (getf metadata key) nil)
          (when (eq key :wal-prefix-digest) (setf prefix nil))
          (let* ((bytes (pg::physical-restartpoint-encode metadata catalog prefix))
                 (digest (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
                 (pointer (lineage-test-bytes (pg::physical-restartpoint-pointer-path)))
                 (before (lineage-test-bytes (pg::physical-recovery-control-path))))
            (pg::wal-atomic-write-bytes (pg::physical-restartpoint-generation-path 216 digest) bytes)
            (pg::wal-set-u64 pointer 16 digest)
            (pg::wal-set-u32 pointer 24 0)
            (pg::wal-set-u32 pointer 24 (pg::wal-crc32 pointer))
            (pg::wal-atomic-write-bytes (pg::physical-restartpoint-pointer-path) pointer)
            (ok (intake-fails-p (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))))
            (ok (null pg::*physical-recovery-failed-p*))
            (ok (= 2 (getf pg::*physical-recovery-state* :phase)))
            (ok (equalp before (lineage-test-bytes (pg::physical-recovery-control-path)))))))))

(deftest promotion-intent-rejects-unflushed-input-or-foreign-receive-identity
    (dolist (foreign '(nil t))
      (with-promotion-fixture (root store)
        (declare (ignore root))
        (let ((before (lineage-test-bytes (pg::physical-recovery-control-path))))
          (if foreign (setf (pg::physical-store-system-id store) 456)
              (pg::physical-store-input store 216 (sample-physical-record 216)))
          (ok (intake-fails-p (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))))
          (ok (null pg::*physical-recovery-failed-p*))
          (ok (equalp before (lineage-test-bytes (pg::physical-recovery-control-path))))))))

(deftest promotion-intent-validates-base-namespace-against-immutable-provenance
    (with-promotion-fixture (root store)
      (declare (ignore root store))
      (pg::wal-atomic-write-bytes
       (pg::wal-control-path)
       (pg::wal-encode-control-image 152 152 (sample-checkpoint-snapshot :version 2 :next-xid 50
                                                                         :oldest-active-xid 0)
                                     :namespace :timeline))
      (let ((before (lineage-test-bytes (pg::physical-recovery-control-path))))
        (ok (intake-fails-p (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))))
        (ok (null pg::*physical-recovery-failed-p*))
        (ok (equalp before (lineage-test-bytes (pg::physical-recovery-control-path)))))))

(deftest promotion-intent-refuses-unowned-admission-and-live-maintenance-workers
    (with-promotion-fixture (root store)
      (declare (ignore root store))
      (let ((before (lineage-test-bytes (pg::physical-recovery-control-path))))
        (ok (intake-fails-p #'pg::physical-promotion-publish-intent))
        (let* ((release (sb-thread:make-semaphore :count 0))
               (worker (sb-thread:make-thread (lambda () (sb-thread:wait-on-semaphore release))))
               (pg::*physical-restartpoint-worker* worker))
          (unwind-protect
               (ok (intake-fails-p (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))))
            (sb-thread:signal-semaphore release)
            (sb-thread:join-thread worker)))
        (ok (null pg::*physical-recovery-failed-p*))
        (ok (equalp before (lineage-test-bytes (pg::physical-recovery-control-path)))))))

(deftest promotion-intent-requires-replayed-complete-backlog-but-permits-an-incomplete-batch
    (dolist (complete '(nil t))
      (with-promotion-fixture (root store)
        (declare (ignore root))
        (pg::physical-store-input store 216 (sample-physical-record 216))
        (when complete
          (pg::physical-store-input store 248
                                    (pg::wal-encode-record pg::+wal-record-maintenance+ 0 248 (empty-octets))))
        (pg::physical-store-flush store)
        (if complete
            (progn
              (ok (intake-fails-p (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))))
              (ok (null pg::*physical-recovery-failed-p*))
              (ok (= 2 (getf pg::*physical-recovery-state* :phase))))
            (ok (= 216 (getf (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent) :applied)))))))

(deftest promotion-intent-errors-and-nonlocal-exits-fence-before-leaving-quiescence
    (dolist (point '(:before-write :after-fsync :after-rename :after-directory-fsync :after-write))
      (dolist (nonlocal '(nil t))
        (with-promotion-fixture (root store)
          (declare (ignore root store))
          (let ((pg::*physical-recovery-control-test-hook*
                  (lambda (observed ignored)
                    (declare (ignore ignored))
                    (when (eq observed point)
                      (if nonlocal (throw 'publication :interrupted) (error "publication failure"))))))
            (if nonlocal
                (ok (eq :interrupted (catch 'publication
                                       (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))))
                (ok (intake-fails-p
                     (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))))))
          (ok pg::*physical-recovery-failed-p*)
          (let ((before (lineage-test-bytes (pg::physical-recovery-control-path))))
            (ok (intake-fails-p
                 (lambda () (pg::physical-recovery-write-control pg::*physical-recovery-state* 2 216 50))))
            (ok (equalp before (lineage-test-bytes (pg::physical-recovery-control-path)))))
          (let ((disk (pg::physical-recovery-read-control pg::*pgdata*)))
            (ok (= (if (member point '(:after-rename :after-directory-fsync :after-write)) 3 2)
                   (getf disk :phase))))
          (ok (= 1 pg::*wal-insert-timeline*))))))

(deftest promotion-child-selection-matches-c-consecutive-history-and-rejects-dangling-aliases
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root))
        (ok (= 2 (pg::physical-promotion-select-child 1)))
        (pg::wal-atomic-write-bytes (pg::timeline-history-path 2 root) #(1))
        (pg::wal-atomic-write-bytes (pg::timeline-history-path 4 root) #(1))
        (ok (= 3 (pg::physical-promotion-select-child 1)))
        (sb-posix:symlink "/missing-history-fixture" (namestring (pg::timeline-history-path 3 root)))
        (ok (intake-fails-p (lambda () (pg::physical-promotion-select-child 1)))))))

(cl-spec:defproperty pending-intent-references-the-selected-local-generation
    ((histories (range integer 0 6)) (kind (range integer 0 1)))
  (:about pg::physical-promotion-publish-intent)
  (:trials (:normal 50))
  (call-with-promotion-fixture
   (lambda (root store)
     (declare (ignore store))
     (loop for timeline from 2 to (1+ histories)
           do (pg::wal-atomic-write-bytes (pg::timeline-history-path timeline root) #(1)))
     (multiple-value-bind (metadata catalog prefix generation)
         (pg::physical-restartpoint-read pg::*physical-recovery-state*)
       (declare (ignore metadata catalog))
       (let ((pending (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent)))
         (and (= (+ 2 histories) (getf pending :promotion-child))
              (= generation (getf pending :promotion-generation))
              (= 216 (getf pending :applied))
              (= 1 (getf pending :timeline))
              (equalp prefix (getf pending :wal-prefix-bytes))
              (eq (nth kind '(:legacy :timeline)) (getf pending :promotion-source-namespace))))))
   :namespace (nth kind '(:legacy :timeline))))

(deftest generated-pending-intent-generation-binding
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'pending-intent-references-the-selected-local-generation :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
