(in-package #:pg-physical-tests)

(defun handoff-test-reference ()
  (multiple-value-bind (metadata ignored prefix generation) (pg::physical-restartpoint-read pg::*physical-recovery-state*)
    (declare (ignore ignored generation))
    (let* ((state pg::*physical-recovery-state*) (base (getf state :end)) (fork (getf metadata :lsn))
                                                 (identity (list :system-id (getf state :system-id) :base-timeline (getf state :timeline)
                                                                 :base base :manifest-digest (getf state :manifest-digest)))
                                                 (capture-state (copy-list state)) (selected nil))
      (setf (getf capture-state :version) 3)
      (multiple-value-bind (overlap bytes) (pg::physical-retained-overlap-capture capture-state pg::*pgdata*)
        (let* ((image (pg::physical-retained-overlap-encode overlap bytes))
               (extent (pg::physical-retained-extent-encode
                        (append (list :version 1) identity
                                (list :history-target 1 :history-digest nil :history-text ""
                                      :start base :end fork :start-digest #xCBF29CE484222325
                                      :end-digest (getf metadata :prefix-digest) :segment 0 :file-timeline 1
                                      :prefix-digest (pg::physical-store-update-digest #xCBF29CE484222325 prefix)))
                        prefix)))
          (pg::wal-ensure-directory-durable (pg::physical-recovery-retained-directory))
          (pg::call-with-physical-retained-root-update
           (lambda (initial hold) (declare (ignore initial))
             (let ((auxiliary (pg::physical-retained-root-stage-overlap hold 1 "" image))
                   (entry (pg::physical-retained-root-stage-image hold 1 "" extent)))
               (setf selected (pg::physical-retained-root-publish-staged-entries
                               hold 1 "" (list entry) :base-overlap auxiliary))))
           (gensym) (pg::physical-recovery-retained-directory) identity 1 "" nil
           :read-selected (lambda () selected))))
      (list :root-id selected :base-timeline (getf state :timeline) :base base
            :manifest-digest (getf state :manifest-digest) :end fork
            :end-digest (getf metadata :prefix-digest)))))

(defun handoff-test-select-pending-reference (reference)
  (let* ((control (pg::physical-recovery-read-control pg::*pgdata*))
         (bytes (pg::physical-recovery-control-image
                 3 (getf control :system-id) (getf control :timeline) (getf control :manifest-digest)
                 (getf control :checkpoint) (getf control :applied) (getf control :next-xid)
                 :state-time (getf control :state-time)
                 :promotion-child (getf control :promotion-child)
                 :promotion-generation (getf control :promotion-generation)
                 :promotion-source-namespace (getf control :promotion-source-namespace)
                 :applied-timeline (getf control :applied-timeline (getf control :timeline))
                 :history-target (getf control :history-target) :history-digest (getf control :history-digest)
                 :retained-reference reference)))
    (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path) bytes)
    (setf (getf pg::*physical-recovery-state* :retained-reference) reference)
    (pg::physical-recovery-read-control pg::*pgdata*)))

(deftest promotion-retained-reference-survives-child-commit
    (with-promotion-fixture (root store)
      (declare (ignore root store))
      (with-retained-root-registry
          (prepare-promotion-child)
        (let ((reference (handoff-test-reference)))
          (handoff-test-select-pending-reference reference)
          (ok (call-with-promotion-exclusion #'pg::physical-promotion-publish-child-control))
          (ok (equal reference (getf (nth-value 6 (pg::wal-read-control-fields)) :retained-reference)))
          (ok (multiple-value-bind (checkpoint durable snapshot child checkpoint-child) (pg::wal-read-control-fields)
                (declare (ignore checkpoint durable snapshot))
                (pg::physical-promotion-check-child-control child checkpoint-child)
                t))))))

(cl-spec:defproperty retained-parent-history-derivation-preserves-ancestry
    ((parent (range integer 2 4294967294)) (fork (range integer 64 18446744073709551615)))
  (:about pg::physical-promotion-retained-parent-history)
  (:trials (:normal 200))
  (let* ((child (1+ parent))
         (history (pg::timeline-parse-history
                   child (concatenate 'string (store-history-text 1 64) (store-history-text parent fork)))))
    (multiple-value-bind (selected end projected) (pg::physical-promotion-retained-parent-history history)
      (and (= parent selected) (= fork end)
           (= parent (pg::timeline-history-entry-timeline (first projected)))
           (= 64 (pg::timeline-history-entry-begin (first projected)))
           (null (pg::timeline-history-entry-end (first projected)))
           (= fork (pg::timeline-history-entry-end (second history)))
           (= 64 (pg::timeline-history-entry-end (second projected)))))))

(deftest generated-retained-parent-history-derivation
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'retained-parent-history-derivation-preserves-ancestry :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(defun handoff-test-authenticate-manifest ()
  (multiple-value-bind (metadata catalog prefix) (pg::physical-restartpoint-read pg::*physical-recovery-state*)
    (let* ((state pg::*physical-recovery-state*)
           (manifest (list :version 3 :system-id (getf state :system-id)
                           :cluster-identity "history-source" :timeline (getf state :timeline)
                           :checkpoint (getf state :checkpoint)
                           :checkpoint-start (getf state :checkpoint-start)
                           :end (getf state :end) :next-xid 50 :source-pin "copy"
                           :files (getf state :files))))
      (write-recovery-manifest-text pg::*pgdata* (pg::physical-bootstrap-manifest-text manifest))
      (let ((digest (nth-value 1 (pg::recovery-read-manifest pg::*pgdata*))))
        (setf (getf state :manifest-digest) digest
              (getf state :version) 3
              (getf metadata :manifest-digest) digest)
        (let* ((bytes (pg::physical-restartpoint-encode metadata catalog prefix))
               (generation (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
               (pointer (make-array 28 :element-type '(unsigned-byte 8) :initial-element 0)))
          (pg::wal-atomic-write-bytes
           (pg::physical-restartpoint-generation-path (getf metadata :lsn) generation) bytes)
          (replace pointer #(80 71 82 80 80 84 82 49))
          (pg::wal-set-u64 pointer 8 (getf metadata :lsn))
          (pg::wal-set-u64 pointer 16 generation)
          (pg::wal-set-u32 pointer 24 (pg::wal-crc32 pointer))
          (pg::wal-atomic-write-bytes (pg::physical-restartpoint-pointer-path) pointer))
        (pg::wal-atomic-write-bytes
         (pg::physical-recovery-control-path)
         (pg::physical-recovery-control-image
          2 (getf state :system-id) (getf state :timeline) digest
          (getf state :checkpoint) (getf metadata :lsn) (getf metadata :next-xid)))
        (pg::wal-atomic-write-bytes
         (merge-pathnames "recovery.required" pg::*pgdata*) (pg::physical-recovery-marker-bytes))))))

(deftest pending-archive-inspection-requires-exact-generation-and-valid-objects
    (with-promotion-fixture (root store)
      (declare (ignore root store))
      (with-retained-root-registry
          (handoff-test-authenticate-manifest)
        (prepare-promotion-child)
        (let* ((reference (handoff-test-reference))
               (directory (pg::physical-recovery-retained-directory))
               (pending (handoff-test-select-pending-reference reference))
               (control (lineage-test-bytes (pg::physical-recovery-control-path)))
               (wal (lineage-test-bytes (pg::wal-control-path)))
               (pointer-path (pg::physical-restartpoint-pointer-path))
               (pointer (lineage-test-bytes pointer-path))
               (counts (list (hash-table-count pg::*physical-retained-root-references*)
                             (hash-table-count pg::*physical-retained-root-holds*)
                             (hash-table-count pg::*physical-retained-root-updates*)))
               (facts (pg::native-control-facts (pg::native-control-capture)))
               (index (pg::physical-retained-inventory-load
                       directory (getf reference :root-id)
                       (pg::physical-promotion-retained-identity (getf pending :system-id) reference)
                       (pg::timeline-parse-history 1 ""))))
          (ok (= pg::+db-in-archive-recovery+ (getf facts :db-state)))
          (ok (= (getf pending :applied) (getf facts :recovery-lsn)))
          (dolist (offset '(48 76))
            (let ((bad (copy-seq control)))
              (pg::wal-set-u64 bad offset (1+ (pg::wal-u64 bad offset)))
              (pg::wal-set-u32 bad (- (length bad) 4) 0)
              (pg::wal-set-u32 bad (- (length bad) 4) (pg::wal-crc32 bad))
              (unwind-protect
                   (progn
                     (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path) bad)
                     (ok (intake-fails-p
                          (lambda () (pg::native-control-facts (pg::native-control-capture))))))
                (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path) control))))
          (dolist (object (list (cons :inventory (getf reference :root-id))
                                (cons :extent (caar (getf index :entries)))
                                (cons :overlap (first (getf index :base-overlap)))))
            (let* ((path (pg::physical-retained-object-path directory (car object) (cdr object)))
                   (bytes (lineage-test-bytes path)))
              (unwind-protect
                   (progn
                     (delete-file path)
                     (ok (intake-fails-p
                          (lambda () (pg::native-control-facts (pg::native-control-capture)))))
                     (let ((bad (copy-seq bytes)))
                       (setf (aref bad (1- (length bad)))
                             (logxor 1 (aref bad (1- (length bad)))))
                       (pg::wal-atomic-write-bytes path bad))
                     (ok (intake-fails-p
                          (lambda () (pg::native-control-facts (pg::native-control-capture))))))
                (pg::wal-atomic-write-bytes path bytes))))
          (unwind-protect
               (progn
                 (delete-file pointer-path)
                 (ok (intake-fails-p
                      (lambda () (pg::native-control-facts (pg::native-control-capture))))))
            (pg::wal-atomic-write-bytes pointer-path pointer))
          (ok (equal counts
                     (list (hash-table-count pg::*physical-retained-root-references*)
                           (hash-table-count pg::*physical-retained-root-holds*)
                           (hash-table-count pg::*physical-retained-root-updates*))))
          (ok (equalp control (lineage-test-bytes (pg::physical-recovery-control-path))))
          (ok (equalp wal (lineage-test-bytes (pg::wal-control-path))))))))

(deftest native-control-classifies-pending-promotion-from-child-timeline
    (with-promotion-fixture (root store)
      (declare (ignore store))
      (prepare-promotion-child)
      (let* ((control (pg::physical-recovery-read-control root))
             (fields (multiple-value-list (pg::wal-read-control-fields)))
             (snapshot (copy-list (third fields)))
             (reader (symbol-function 'pg::native-control-recovery-checkpoint))
             (calls 0))
        ;; This test isolates role selection; actual generations are checked above.
        (setf (getf snapshot :timeline) 2 (getf snapshot :prev-timeline) 1)
        (pg::wal-atomic-write-bytes
         (pg::wal-control-path)
         (pg::wal-encode-control-image (first fields) (second fields) snapshot
                                       :timeline 2 :checkpoint-timeline 2 :namespace :timeline))
        (pg::wal-atomic-write-bytes
         (pg::physical-recovery-control-path)
         (pg::physical-recovery-control-image
          3 (getf control :system-id) 2 (getf control :manifest-digest)
          (getf control :checkpoint) (getf control :applied) (getf control :next-xid)
          :promotion-child 3 :promotion-generation (getf control :promotion-generation)
          :promotion-source-namespace :timeline))
        (pg::wal-atomic-write-bytes
         (merge-pathnames "recovery.required" root) (pg::physical-recovery-marker-bytes))
        (let ((text (pg::physical-bootstrap-manifest-text (sample-recovery-manifest))))
          (write-recovery-manifest-text root text))
        (let ((before (mapcar #'lineage-test-bytes
                              (list (pg::wal-control-path) (pg::physical-recovery-control-path))))
              (capture (pg::native-control-capture)))
          (ok (every #'identity (subseq capture 2)))
          (unwind-protect
               (progn
                 (setf (symbol-function 'pg::native-control-recovery-checkpoint)
                       (lambda (pending old)
                         (declare (ignore old))
                         (incf calls)
                         (ok (= 2 (getf pending :timeline)))
                         snapshot))
                 (let ((facts (pg::native-control-facts capture)))
                   (ok (= pg::+db-in-archive-recovery+ (getf facts :db-state)))
                   (ok (= 2 (getf facts :recovery-timeline)))
                   (ok (= (getf control :applied) (getf facts :recovery-lsn)))))
            (setf (symbol-function 'pg::native-control-recovery-checkpoint) reader))
          (ok (= 1 calls))
          (ok (equalp before
                      (mapcar #'lineage-test-bytes
                              (list (pg::wal-control-path) (pg::physical-recovery-control-path)))))))))

(defun handoff-test-committed (function)
  (with-promotion-fixture (root store :system-id
                                (pg::catalog-system-identifier
                                 (pg::make-catalog :cluster-identity "history-source")))
    (with-retained-root-registry
        (prepare-promotion-child)
      (let ((reference (handoff-test-reference)))
        (handoff-test-select-pending-reference reference)
        (call-with-promotion-exclusion #'pg::physical-promotion-publish-child-control)
        (funcall function root store reference)))))

(deftest committed-archive-validation-is-independent-of-retired-recovery-files
    (handoff-test-committed
     (lambda (root store reference)
       (let* ((state pg::*physical-recovery-state*)
              (parent-path (merge-pathnames
                            (concatenate 'string "pg_wal/"
                                         (pg::wal-segment-filename 0 1 (getf state :wal-namespace))) root))
              (fields (multiple-value-list (pg::wal-read-control-fields))))
         (pg::physical-store-close store)
         (uiop:delete-directory-tree (pg::physical-store-directory store) :validate t)
         (uiop:delete-directory-tree (pg::physical-restartpoint-directory) :validate t)
         (dolist (name '("physical-backup.manifest" "physical-restartpoint.control" "physical-recovery.control"
                         "recovery.required" "physical-follow.control"))
           (let ((path (merge-pathnames name root)))
             (when (probe-file path) (delete-file path))))
         (delete-file parent-path)
         (ok (pg::physical-promotion-check-child-control (fourth fields) (fifth fields)))
         (let ((before (list (hash-table-count pg::*physical-retained-root-references*)
                             (hash-table-count pg::*physical-retained-root-holds*)
                             (hash-table-count pg::*physical-retained-root-updates*)))
               (control (lineage-test-bytes (pg::wal-control-path))))
           (ok (= (getf state :system-id)
                  (getf (pg::native-control-facts (pg::native-control-capture)) :system-id)))
           (ok (equal before
                      (list (hash-table-count pg::*physical-retained-root-references*)
                            (hash-table-count pg::*physical-retained-root-holds*)
                            (hash-table-count pg::*physical-retained-root-updates*))))
           (ok (equalp control (lineage-test-bytes (pg::wal-control-path)))))
         (ok (equal reference (getf (seventh fields) :retained-reference)))))))

(deftest missing-or-corrupt-committed-archive-stops-before-trigger-retirement
    (handoff-test-committed
     (lambda (root store reference)
       (declare (ignore store))
       (let* ((directory (pg::physical-recovery-retained-directory))
              (identity (pg::physical-promotion-retained-identity
                         (getf pg::*physical-recovery-state* :system-id) reference))
              (index (pg::physical-retained-inventory-load
                      directory (getf reference :root-id) identity (pg::timeline-parse-history 1 "")))
              (required (merge-pathnames "recovery.required" root))
              (marker (pg::physical-recovery-marker-bytes))
              (pending (lineage-test-bytes (pg::physical-recovery-control-path)))
              (primary (lineage-test-bytes (pg::wal-control-path))))
         (pg::wal-atomic-write-bytes required marker)
         (dolist (object (list (cons :inventory (getf reference :root-id))
                               (cons :extent (caar (getf index :entries)))
                               (cons :overlap (first (getf index :base-overlap)))))
           (let* ((path (pg::physical-retained-object-path directory (car object) (cdr object)))
                  (bytes (lineage-test-bytes path)))
             (dolist (corrupt '(nil t))
               (if corrupt
                   (let ((bad (copy-seq bytes)))
                     (setf (aref bad (1- (length bad)))
                           (logxor 1 (aref bad (1- (length bad)))))
                     (pg::wal-atomic-write-bytes path bad))
                   (delete-file path))
               (ok (handler-case (progn (pg::physical-promotion-startup-primary-p) nil)
                     (error (condition)
                       (search "retained promotion archive" (princ-to-string condition)))))
               (ok (equalp marker (lineage-test-bytes required)))
               (ok (equalp pending (lineage-test-bytes (pg::physical-recovery-control-path))))
               (ok (equalp primary (lineage-test-bytes (pg::wal-control-path))))
               (pg::wal-atomic-write-bytes path bytes))))))))

(deftest committed-archive-reference-must-match-identity-and-exact-fork
    (handoff-test-committed
     (lambda (root store reference)
       (declare (ignore root store))
       (let* ((fields (multiple-value-list (pg::wal-read-control-fields)))
              (original (lineage-test-bytes (pg::wal-control-path)))
              (reader (symbol-function 'pg::physical-retained-object-read))
              (reads 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-retained-object-read)
                      (lambda (directory kind id maximum)
                        (when (eq kind :extent) (incf reads))
                        (funcall reader directory kind id maximum)))
                (dolist (key '(:root-id :base-timeline :base :manifest-digest :end :end-digest :system-id))
                  (let ((metadata (copy-list (seventh fields)))
                        (bad (copy-list reference)))
                    (if (eq key :system-id)
                        (incf (getf metadata :system-id))
                        (incf (getf bad key)))
                    (setf (getf metadata :retained-reference) bad reads 0)
                    (pg::wal-atomic-write-bytes
                     (pg::wal-control-path)
                     (pg::wal-encode-control-image
                      (first fields) (second fields) (third fields)
                      :timeline (fourth fields) :checkpoint-timeline (fifth fields)
                      :namespace (sixth fields) :metadata metadata))
                    (ok (intake-fails-p
                         (lambda ()
                           (pg::physical-promotion-check-child-control
                            (fourth fields) (fifth fields)))))
                    (ok (zerop reads))
                    (pg::wal-atomic-write-bytes (pg::wal-control-path) original))))
           (setf (symbol-function 'pg::physical-retained-object-read) reader)
           (pg::wal-atomic-write-bytes (pg::wal-control-path) original))))))

(deftest committed-archive-ownership-precedes-cleanup-and-survives-interruptions
    (dolist (cut '(:after-primary-retained-registration :after-retire-required
                   :after-retire-manifest :after-retire-follow :after-retire-pointer
                   :after-retire-pending :after-pending-retained-retirement))
      (handoff-test-committed
       (lambda (root store reference)
         (declare (ignore store))
         (let* ((id (getf reference :root-id))
                (pending (pg::physical-promotion-retained-owner :pending))
                (primary (pg::physical-promotion-retained-owner :primary))
                (events nil))
           (ok (eql id (pg::physical-retained-root-current pending)))
           (ok (null (pg::physical-retained-root-current primary)))
           (call-with-current-promotion-owner
            (lambda ()
              (let ((pg::*physical-promotion-test-hook*
                      (lambda (point)
                        (push point events)
                        (ok (eql id (pg::physical-retained-root-current primary)))
                        (when (eq point :after-primary-retained-registration)
                          (ok (probe-file (pg::physical-recovery-control-path)))
                          (ok (eql id (pg::physical-retained-root-current pending))))
                        (when (eq point :after-pending-retained-retirement)
                          (ok (null (probe-file (pg::physical-recovery-control-path))))
                          (ok (null (pg::physical-retained-root-current pending))))
                        (when (eq point cut) (throw 'handoff-cut t)))))
                (ok (catch 'handoff-cut
                      (call-with-promotion-exclusion #'pg::physical-promotion-retire-recovery)
                      nil)))
              (ok (member cut events))
              (ok (eql id (pg::physical-retained-root-current primary)))
              ;; Drop only process-local state, then retry the cold cleanup path.
              (clrhash pg::*physical-retained-root-references*)
              (clrhash pg::*physical-retained-root-holds*)
              (clrhash pg::*physical-retained-root-updates*)
              (let ((pg::*wal-ready* nil) (pg::*n-backends* 0))
                (ok (pg::physical-promotion-startup-primary-p)))
              (ok (eql id (pg::physical-retained-root-current primary)))
              (ok (null (pg::physical-retained-root-current pending)))
              (ok (pg::physical-retained-root-protected-p
                   (pg::physical-recovery-retained-directory) :inventory id))
              (dolist (name '("recovery.required" "physical-backup.manifest"
                              "physical-follow.control" "physical-restartpoint.control"
                              "physical-recovery.control"))
                (ok (null (probe-file (merge-pathnames name root))))))))))))

(deftest committed-archive-cannot-retire-rootless-pending-control
    (handoff-test-committed
     (lambda (root store reference)
       (declare (ignore root store reference))
       (let* ((control (pg::physical-recovery-read-control pg::*pgdata*))
              (primary (lineage-test-bytes (pg::wal-control-path)))
              (bytes (pg::physical-recovery-control-image
                      3 (getf control :system-id) (getf control :timeline)
                      (getf control :manifest-digest) (getf control :checkpoint)
                      (getf control :applied) (getf control :next-xid)
                      :state-time (getf control :state-time)
                      :promotion-child (getf control :promotion-child)
                      :promotion-generation (getf control :promotion-generation)
                      :promotion-source-namespace (getf control :promotion-source-namespace)
                      :applied-timeline (getf control :applied-timeline (getf control :timeline))
                      :history-target (getf control :history-target)
                      :history-digest (getf control :history-digest))))
         (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path) bytes)
         (ok (handler-case (progn (pg::physical-promotion-startup-primary-p) nil)
               (error (condition)
                 (search "committed child differs from pending retained reference"
                         (princ-to-string condition)))))
         (ok (equalp bytes (lineage-test-bytes (pg::physical-recovery-control-path))))
         (ok (equalp primary (lineage-test-bytes (pg::wal-control-path))))))))

(deftest committed-archive-keeps-original-fork-after-later-checkpoint
    (handoff-test-committed
     (lambda (root store reference)
       (declare (ignore root store))
       (let* ((fields (multiple-value-list (pg::wal-read-control-fields)))
              (old (third fields))
              (start (second fields))
              (pg::*wal-insert-timeline* (fourth fields))
              (pg::*wal-prev-timeline* (getf old :prev-timeline))
              (pg::*next-xid* (getf old :next-xid))
              (pg::*xid-batch-end* (1+ (getf old :xid-reserved-through)))
              (pg::*active-xids* nil)
              (snapshot (pg::wal-capture-checkpoint-snapshot start))
              (end (getf snapshot :end))
              (path (pg::wal-segment-path 0 (fourth fields) :timeline))
              (prefix (lineage-test-bytes path))
              (record (pg::wal-encode-record
                       pg::+wal-record-checkpoint+ 0 start
                       (pg::wal-encode-checkpoint-snapshot snapshot))))
         (ok (> start (getf reference :end)))
         (pg::wal-atomic-write-bytes
          path (concatenate '(vector (unsigned-byte 8)) prefix record))
         (pg::wal-atomic-write-bytes
          (pg::wal-control-path)
          (pg::wal-encode-control-image
           end end snapshot :timeline (fourth fields)
           :checkpoint-timeline (fifth fields) :namespace (sixth fields)
           :metadata (pg::wal-refresh-control-metadata
                      (seventh fields) pg::+db-in-production+)))
         (ok (pg::physical-promotion-check-child-control (fourth fields) (fifth fields)))
         (ok (equal reference (getf (nth-value 6 (pg::wal-read-control-fields))
                                    :retained-reference)))
         (ok (= (getf reference :end)
                (pg::timeline-history-entry-begin
                 (first (nth-value 1
                                   (pg::physical-promotion-check-child-control
                                    (fourth fields) (fifth fields)))))))))))

(deftest invalid-pending-archive-stops-all-direct-publishers-before-mutation
    (dolist (publisher '(pg::physical-promotion-install-prefix
                         pg::physical-promotion-publish-wal
                         pg::physical-promotion-publish-child-control))
      (with-promotion-fixture (root store)
        (declare (ignore root store))
        (with-retained-root-registry
            (prepare-promotion-child)
          (let* ((reference (handoff-test-reference))
                 (directory (pg::physical-recovery-retained-directory))
                 (path (pg::physical-retained-object-path directory :inventory
                                                          (getf reference :root-id)))
                 (child (getf pg::*physical-recovery-state* :promotion-child))
                 (wal-path (pg::wal-segment-path 0 child :timeline))
                 (parent (lineage-test-bytes (pg::wal-control-path)))
                 (wal (lineage-test-bytes wal-path)))
            (handoff-test-select-pending-reference reference)
            (let ((pending (lineage-test-bytes (pg::physical-recovery-control-path))))
              (delete-file path)
              (ok (handler-case
                      (progn (call-with-promotion-exclusion publisher) nil)
                    (error (condition)
                      (search "retained promotion archive" (princ-to-string condition)))))
              (ok (equalp parent (lineage-test-bytes (pg::wal-control-path))))
              (ok (equalp pending (lineage-test-bytes (pg::physical-recovery-control-path))))
              (ok (equalp wal (lineage-test-bytes wal-path)))
              (ok (null (probe-file (pg::timeline-history-path child pg::*pgdata*))))))))))
