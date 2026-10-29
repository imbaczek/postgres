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

(deftest pending-archive-inspection-requires-exact-generation-and-valid-objects
    (with-promotion-fixture (root store)
      (declare (ignore root store))
      (with-retained-root-registry
          (ok (= (getf pg::*physical-recovery-state* :manifest-digest)
                 (nth-value 1 (pg::recovery-read-manifest pg::*pgdata*))))
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
          (ok (every #'identity (subseq capture 2 5)))
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

(defun call-with-historical-follow-source (function)
  (handoff-test-committed
   (lambda (root store reference)
     (pg::physical-promotion-restore-primary-retained)
     (let* ((fields (multiple-value-list (pg::wal-read-control-fields)))
            (pg::*catalog* (pg::make-catalog :cluster-identity "history-source"))
            (pg::*physical-recovery-state* nil)
            (pg::*physical-recovery-failed-p* nil)
            (pg::*wal-insert-timeline* (fourth fields))
            (pg::*wal-checkpoint-timeline* (fifth fields))
            (pg::*wal-segment-namespace* :timeline)
            (pg::*wal-durable-lsn* (second fields))
            (pg::*physical-source-senders* (make-hash-table :test #'equal))
            (prefix (lineage-test-bytes (pg::wal-segment-path 0)))
            (fork (getf reference :end))
            (eor (subseq prefix fork (+ fork pg::+wal-end-of-recovery-record-size+))))
       (pg::physical-store-close store)
       (uiop:delete-directory-tree (pg::physical-store-directory store) :validate t)
       (funcall function root reference prefix eor)))))

(defun historical-follow-request (reference prefix eor cursor)
  (list :version 1 :system-id (pg::catalog-system-identifier)
        :base-timeline 1 :base 152 :manifest-digest (getf reference :manifest-digest)
        :source-timeline 2 :receive-timeline 1
        :receive-lsn cursor :receive-digest 91 :overlap-start 0
        :overlap-digest (pg::physical-store-update-digest
                         #xCBF29CE484222325 (subseq prefix 0 cursor))
        :history-text (pg::bytes->string (pg::timeline-history-bytes 2))
        :eor-octets (coerce eor 'list)))

(deftest physical-slot-primary-validation-uses-committed-archive
    (call-with-historical-follow-source
     (lambda (root reference prefix eor)
       (declare (ignore root prefix eor))
       (let* ((system-id (pg::catalog-system-identifier))
              (slot (physical-slot-test-object
                     :restart (getf reference :base) :timeline 1 :system-id system-id)))
         (ok (eq slot (pg::physical-slot-check-primary-wal slot system-id)))
         (setf (pg::wal-replication-slot-restart-lsn slot) (getf reference :end))
         (ok (eq slot (pg::physical-slot-check-primary-wal slot system-id)))
         (setf (pg::wal-replication-slot-restart-lsn slot) (1+ pg::*wal-durable-lsn*))
         (ok (signals (pg::physical-slot-check-primary-wal slot system-id) 'error))
         (setf (pg::wal-replication-slot-restart-lsn slot) (getf reference :base))
         (uiop:delete-directory-tree (pg::physical-recovery-retained-directory) :validate t)
         (ok (signals (pg::physical-slot-check-primary-wal slot system-id) 'error))))))

(deftest historical-source-proof-does-not-confuse-receiver-and-archive-bases
    (call-with-historical-follow-source
     (lambda (root reference prefix eor)
       (declare (ignore root))
       (loop for base in '(32 152 184)
             do (let ((request (historical-follow-request reference prefix eor 216)))
                  (setf (getf request :base) base)
                  (incf (getf request :manifest-digest))
                  (let ((pin (pg::physical-source-follow-ready (format nil "base_~d" base) request)))
                    (ok (pg::physical-follow-pin-ready-p pin))
                    (ok (= (getf request :manifest-digest) (pg::physical-source-pin-manifest-digest pin)))))))))

(deftest historical-source-rejects-damaged-selected-archive-and-conflicting-child
    (call-with-historical-follow-source
     (lambda (root reference prefix eor)
       (declare (ignore root))
       (let* ((directory (pg::physical-recovery-retained-directory))
              (identity (pg::physical-promotion-retained-identity
                         (pg::catalog-system-identifier) reference))
              (index (pg::physical-retained-inventory-load
                      directory (getf reference :root-id) identity (pg::timeline-parse-history 1 "")))
              (request (historical-follow-request reference prefix eor 184)))
         (dolist (object (list (cons :inventory (getf reference :root-id))
                               (cons :extent (caar (getf index :entries)))
                               (cons :overlap (first (getf index :base-overlap)))
                               (cons :child nil)))
           (let* ((path (if (eq (car object) :child) (pg::wal-segment-path 0)
                            (pg::physical-retained-object-path directory (car object) (cdr object))))
                  (original (lineage-test-bytes path))
                  (bad (copy-seq original)))
             (setf (aref bad 0) (logxor 1 (aref bad 0)))
             (unwind-protect
                  (progn
                    (pg::wal-atomic-write-bytes path bad)
                    (ok (signals (pg::physical-source-follow-ready "damaged" request) 'error))
                    (ok (null (gethash "damaged" pg::*physical-source-pins*)))
                    (ng pg::*wal-poisoned*))
               (pg::wal-atomic-write-bytes path original))))))))

(deftest historical-follow-preparation-uses-selected-source-archive
    (call-with-historical-follow-source
     (lambda (root reference prefix eor)
       (declare (ignore root))
       (let* ((request (historical-follow-request reference prefix eor 184))
              (pin (pg::physical-source-follow-prepare
                    "source_owned" (follow-anchor-receiver-request request))))
         (ok (pg::physical-follow-pin-ready-p pin))
         (ok (equal request (pg::physical-follow-pin-anchor pin)))))))

(deftest historical-follow-anchor-proves-selected-root-and-live-child
    (call-with-historical-follow-source
     (lambda (root reference prefix eor)
       (declare (ignore root))
       (dolist (cursor '(152 184 216))
         (let* ((request (list :version 1 :system-id (pg::catalog-system-identifier)
                               :base-timeline 1 :base 152
                               :manifest-digest (getf reference :manifest-digest)
                               :source-timeline 2 :receive-timeline 1
                               :receive-lsn cursor :receive-digest 91 :overlap-start 0
                               :overlap-digest (pg::physical-store-update-digest
                                                #xCBF29CE484222325 (subseq prefix 0 cursor))
                               :history-text (pg::bytes->string (pg::timeline-history-bytes 2))
                               :eor-octets (coerce eor 'list)))
                (pin (pg::physical-source-follow-ready (format nil "sibling_~d" cursor) request)))
           (ok (pg::physical-follow-pin-ready-p pin))
           (ok (= 91 (pg::physical-source-pin-prefix-digest pin)))
           (ok (= cursor (pg::physical-source-pin-confirmed-lsn pin)))
           (sb-thread:with-mutex (pg::*wal-lock*)
             (multiple-value-bind (bytes timeline) (pg::physical-follow-source-segment pin 0)
               (ok (= 2 timeline))
               (ok (equalp prefix bytes)))))))))

(deftest historical-follow-source-proves-receipt-through-child-fork
    (call-with-historical-follow-source
     (lambda (root reference prefix eor)
       (declare (ignore root))
       (let* ((cursor 184)
              (request (list :version 1 :system-id (pg::catalog-system-identifier)
                             :base-timeline 1 :base 152
                             :manifest-digest (getf reference :manifest-digest)
                             :source-timeline 2 :receive-timeline 1
                             :receive-lsn cursor :receive-digest 91 :overlap-start 0
                             :overlap-digest (pg::physical-store-update-digest
                                              #xCBF29CE484222325 (subseq prefix 0 cursor))
                             :history-text (pg::bytes->string (pg::timeline-history-bytes 2))
                             :eor-octets (coerce eor 'list)))
              (pin (pg::physical-source-follow-ready "reader" request))
              (owner (gensym "SENDER"))
              (expected (pg::physical-store-update-digest
                         91 (subseq prefix cursor pg::*wal-durable-lsn*))))
         (setf (gethash "reader" pg::*physical-source-senders*) owner)
         (ok (= expected (pg::physical-source-prove-range
                          "reader" owner cursor pg::*wal-durable-lsn* 91)))
         (pg::physical-source-confirm-receipt "reader" owner pg::*wal-durable-lsn* expected)
         (ok (= expected (pg::physical-source-pin-prefix-digest pin)))
         (multiple-value-bind (record raw durable)
             (pg::physical-source-read-next "reader" owner pg::*wal-durable-lsn*)
           (ok (null record)) (ok (null raw)) (ok (= durable pg::*wal-durable-lsn*)))
         (ok (signals (pg::physical-source-read-next "reader" owner cursor) 'error))))))

(deftest historical-source-prebase-cursor-requires-an-eligible-live-prefix
    (call-with-historical-follow-source
     (lambda (root reference prefix eor)
       (declare (ignore root))
       (let* ((request (historical-follow-request reference prefix eor 32))
              (path (pg::wal-segment-path 0)))
         (setf (getf request :base) 32)
         (ok (pg::physical-follow-pin-ready-p
              (pg::physical-source-follow-ready "prebase_present" request)))
         (unwind-protect
              (progn
                (delete-file path)
                (ok (signals (pg::physical-source-follow-ready "prebase_missing" request) 'error))
                (ok (null (gethash "prebase_missing" pg::*physical-source-pins*)))
                (let ((bad (copy-seq prefix)))
                  (replace bad (sample-physical-record 184) :start1 184)
                  (pg::wal-atomic-write-bytes path bad))
                (ok (signals (pg::physical-source-follow-ready "prebase_conflicting" request) 'error))
                (ok (null (gethash "prebase_conflicting" pg::*physical-source-pins*))))
           (pg::wal-atomic-write-bytes path prefix))))))

(deftest historical-source-cache-refreshes-at-base-and-durable-end
    (call-with-historical-follow-source
     (lambda (root reference prefix eor)
       (declare (ignore root))
       (let* ((request (historical-follow-request reference prefix eor 32))
              (reader (symbol-function 'pg::physical-follow-source-segment))
              (calls 0)
              (owner (gensym))
              (pg::*physical-follow-source-read-cache* (list nil)))
         (setf (getf request :base) 32)
         (pg::physical-source-follow-ready "before_base" request)
         (setf (gethash "before_base" pg::*physical-source-senders*) owner)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-source-segment)
                      (lambda (&rest arguments) (incf calls) (apply reader arguments)))
                (pg::physical-source-read-next "before_base" owner 32)
                (ok (= 1 calls))
                (pg::physical-source-read-next "before_base" owner 152)
                (ok (= 2 calls))
                (pg::physical-source-read-next "before_base" owner 184)
                (ok (= 2 calls))
                (let* ((fields (multiple-value-list (pg::wal-read-control-fields)))
                       (old-control (lineage-test-bytes (pg::wal-control-path)))
                       (metadata (copy-tree (seventh fields))))
                  (unwind-protect
                       (progn
                         (incf (getf (getf metadata :retained-reference) :end-digest))
                         (pg::wal-atomic-write-bytes
                          (pg::wal-control-path)
                          (pg::wal-encode-control-image
                           (first fields) (second fields) (third fields)
                           :timeline 2 :checkpoint-timeline 2 :namespace :timeline :metadata metadata))
                         (ok (signals (pg::physical-source-read-next "before_base" owner 184) 'error)))
                    (pg::wal-atomic-write-bytes (pg::wal-control-path) old-control))
                  (ok (= 3 calls))
                  (let* ((start pg::*wal-durable-lsn*)
                         (extra (concatenate '(vector (unsigned-byte 8))
                                             (sample-physical-record start)
                                             (pg::wal-encode-record pg::+wal-record-maintenance+
                                                                    0 (+ start 32) (empty-octets))))
                         (pg::*wal-durable-lsn* (+ start (length extra))))
                    (pg::wal-atomic-write-bytes (pg::wal-segment-path 0)
                                                (concatenate '(vector (unsigned-byte 8)) prefix extra))
                    (pg::wal-atomic-write-bytes
                     (pg::wal-control-path)
                     (pg::wal-encode-control-image
                      (first fields) pg::*wal-durable-lsn* (third fields)
                      :timeline 2 :checkpoint-timeline 2 :namespace :timeline :metadata (seventh fields)))
                    (ok (= (+ start 32)
                           (pg::wal-record-end (pg::physical-source-read-next "before_base" owner start))))
                    (ok (= 4 calls))
                    (pg::physical-source-read-next "before_base" owner (+ start 32))
                    (ok (= 4 calls)))))
           (setf (symbol-function 'pg::physical-follow-source-segment) reader))))))

(deftest historical-source-reads-an-archived-segment-without-a-child-filename
    ;; Structural WAL fixture; this does not establish replay or network onboarding.
    (with-physical-store-directory (root)
      (with-retained-root-registry
          (let* ((pg::*pgdata* root)
                 (pg::*catalog* (pg::make-catalog :cluster-identity "historical-segments"))
                 (pg::*physical-recovery-state* nil) (pg::*physical-recovery-failed-p* nil)
                 (pg::*wal-insert-timeline* 2) (pg::*wal-segment-namespace* :timeline)
                 (pg::*physical-source-pins* (make-hash-table :test #'equal))
                 (pg::*physical-source-senders* (make-hash-table :test #'equal))
                 (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
                 (pg::*wal-poisoned* nil)
                 (size pg::+wal-segment-size+)
                 (base (concatenate '(vector (unsigned-byte 8))
                                    (legacy-checkpoint-record 0) (legacy-checkpoint-record 32)
                                    (legacy-checkpoint-record 64)))
                 (filler (pg::wal-encode-record
                          pg::+wal-record-batch-start+ 0 96
                          (make-array (- size 128) :element-type '(unsigned-byte 8) :initial-element 0)))
                 (first (concatenate '(vector (unsigned-byte 8)) base filler))
                 (middle-bytes (pg::wal-encode-record
                                pg::+wal-record-batch-start+ 0 size
                                (make-array (- size 32) :element-type '(unsigned-byte 8) :initial-element 0)))
                 (last (legacy-checkpoint-record (* 2 size)))
                 (fork (+ (* 2 size) 32))
                 (eor (pg::wal-encode-end-of-recovery-record fork 42 2 1 1))
                 (snapshot (sample-checkpoint-snapshot
                            :version 2 :start (+ fork 56) :end (+ fork 176) :redo (+ fork 56)
                            :timeline 2 :prev-timeline 1))
                 (checkpoint (pg::wal-encode-record
                              pg::+wal-record-checkpoint+ 0 (+ fork 56)
                              (pg::wal-encode-checkpoint-snapshot snapshot)))
                 (child (concatenate '(vector (unsigned-byte 8)) last eor checkpoint))
                 (pg::*wal-durable-lsn* (+ fork 176))
                 (identity (list :system-id (pg::catalog-system-identifier)
                                 :base-timeline 1 :base 96 :manifest-digest 77))
                 (middle (pg::physical-store-update-digest #xCBF29CE484222325 filler))
                 (second-digest (pg::physical-store-update-digest middle middle-bytes))
                 (digest (pg::physical-store-update-digest second-digest last))
                 (images (loop for bytes in (list first middle-bytes last)
                               for start in (list 96 size (* 2 size))
                               for end in (list size (* 2 size) fork)
                               for initial in (list #xCBF29CE484222325 middle second-digest)
                               for final in (list middle second-digest digest)
                               for segment from 0
                               collect (pg::physical-retained-extent-encode
                                        (append (list :version 1) identity
                                                (list :history-target 1 :history-digest nil :history-text ""
                                                      :start start :end end :start-digest initial :end-digest final
                                                      :segment segment :file-timeline 1
                                                      :prefix-digest (pg::physical-store-update-digest
                                                                      #xCBF29CE484222325 bytes)))
                                        bytes)))
                 (overlap-metadata (overlap-test-metadata 96 base))
                 (directory (pg::physical-recovery-retained-directory)))
            (setf (getf overlap-metadata :system-id) (pg::catalog-system-identifier))
            (pg::wal-ensure-directory-durable directory)
            (let* ((id (project-test-publish-source
                        directory images (pg::physical-retained-overlap-encode overlap-metadata base)
                        identity 1 ""))
                   (reference (list :root-id id :base-timeline 1 :base 96 :manifest-digest 77
                                    :end fork :end-digest digest))
                   (metadata (list :system-id (pg::catalog-system-identifier)
                                   :db-state pg::+db-in-production+ :state-time 42 :retained-reference reference))
                   (request (list :version 1 :system-id (pg::catalog-system-identifier)
                                  :base-timeline 1 :base 32 :manifest-digest 88
                                  :source-timeline 2 :receive-timeline 1
                                  :receive-lsn 96 :receive-digest 91 :overlap-start 0
                                  :overlap-digest (pg::physical-store-update-digest #xCBF29CE484222325 base)
                                  :history-text (store-history-text 1 fork) :eor-octets (coerce eor 'list))))
              (pg::wal-ensure-directory-durable (pg::wal-directory))
              (pg::wal-atomic-write-bytes (pg::wal-segment-path 2) child)
              (pg::timeline-write-history 2 1 fork "test")
              (setf (getf request :history-text) (pg::bytes->string (pg::timeline-history-bytes 2)))
              (pg::wal-atomic-write-bytes
               (pg::wal-control-path)
               (pg::wal-encode-control-image pg::*wal-durable-lsn* pg::*wal-durable-lsn* snapshot
                                             :timeline 2 :checkpoint-timeline 2 :namespace :timeline
                                             :metadata metadata))
              (pg::physical-retained-root-register
               (pg::physical-promotion-retained-owner :primary) directory id identity
               (pg::timeline-parse-history 1 ""))
              (ok (null (probe-file (pg::wal-segment-path 0))))
              (let ((slot (physical-slot-test-object
                           :restart 33 :timeline 1
                           :system-id (pg::catalog-system-identifier)))
                    (parent-path (pg::wal-segment-path 0 1 :timeline)))
                (pg::wal-atomic-write-bytes parent-path first)
                (unwind-protect
                     (progn
                       (ok (eq slot (pg::physical-slot-check-primary-wal
                                     slot (pg::catalog-system-identifier))))
                       (pg::wal-atomic-write-bytes parent-path (subseq first 0 95))
                       (ok (signals (pg::physical-slot-check-primary-wal
                                     slot (pg::catalog-system-identifier)) 'error))
                       (let ((changed (copy-seq first)))
                         (setf (aref changed 0) (logxor 1 (aref changed 0)))
                         (pg::wal-atomic-write-bytes parent-path changed))
                       (ok (signals (pg::physical-slot-check-primary-wal
                                     slot (pg::catalog-system-identifier)) 'error))
                       (pg::wal-atomic-write-bytes parent-path first)
                       (let ((ancestor-tail (pg::wal-segment-path 2 1 :timeline))
                             (child-path (pg::wal-segment-path 2)))
                         (pg::wal-atomic-write-bytes ancestor-tail child)
                         (unwind-protect
                              (progn
                                (delete-file child-path)
                                (ok (signals (pg::physical-slot-check-primary-wal
                                              slot (pg::catalog-system-identifier)) 'error)))
                           (pg::wal-atomic-write-bytes child-path child)
                           (delete-file ancestor-tail))))
                  (delete-file parent-path)))
              (let* ((pin (pg::physical-source-follow-ready "old_sibling" request))
                     (owner (gensym)))
                (let ((command
                        (pg::parse-replication-command
                         (format nil
                                 "START_REPLICATION PHYSICAL 0/00000060 (proof_pin 'old_sibling', native_format '~a', mode 'follow', system_id '~d', timeline '2', base_lsn '0/00000020', prefix_digest '91', manifest_digest '88')"
                                 pg::+physical-native-format+ (pg::catalog-system-identifier)))))
                  (ok (equal "55000" (recovery-state-of (lambda () (pg::physical-source-acquire-sender command)))))
                  (ok (null (gethash "old_sibling" pg::*physical-source-senders*)))
                  (setf (pg::logical-replication-command-requested-lsn command) fork)
                  (multiple-value-bind (child-owner limit next) (pg::physical-source-acquire-sender command)
                    (unwind-protect (progn (ok (null limit)) (ok (null next)))
                      (pg::physical-source-release-sender "old_sibling" child-owner))))
                (setf (gethash "old_sibling" pg::*physical-source-senders*) owner)
                (ok (pg::physical-follow-pin-ready-p pin))
                (multiple-value-bind (record raw durable)
                    (pg::physical-source-read-next "old_sibling" owner 96)
                  (ok (= size (pg::wal-record-end record))))
                (ok (= (pg::physical-store-update-digest
                        91 (concatenate '(vector (unsigned-byte 8)) filler middle-bytes last eor checkpoint))
                       (pg::physical-source-prove-range
                        "old_sibling" owner 96 pg::*wal-durable-lsn* 91)))
                (let ((older (copy-tree request)))
                  (setf (getf older :receive-lsn) 32
                        (getf older :overlap-digest)
                        (pg::physical-store-update-digest #xCBF29CE484222325 (subseq base 0 32)))
                  (ok (signals (pg::physical-source-follow-ready "missing_prebase" older) 'error))
                  (ok (null (gethash "missing_prebase" pg::*physical-source-pins*)))
                  (pg::wal-atomic-write-bytes (pg::wal-segment-path 0) first)
                  (ok (null (probe-file (pg::wal-segment-path 1))))
                  ;; A tip-named file in a wholly ancestral segment is not a C-eligible carrier.
                  (ok (signals (pg::physical-source-follow-ready "prebase_live" older) 'error))
                  (ok (null (gethash "prebase_live" pg::*physical-source-pins*)))
                  (let ((payload (subseq filler pg::+wal-header-size+)))
                    (setf (aref payload 0) 1)
                    (pg::wal-atomic-write-bytes
                     (pg::wal-segment-path 0)
                     (concatenate '(vector (unsigned-byte 8)) base
                                  (pg::wal-encode-record pg::+wal-record-batch-start+ 0 96 payload))))
                  (ok (signals (pg::physical-source-follow-ready "conflicting_prebase" older) 'error))
                  (ok (null (gethash "conflicting_prebase" pg::*physical-source-pins*))))))))))

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

(deftest promotion-intent-produces-and-selects-fork-bounded-retained-root
    (with-promotion-fixture (root store)
      (declare (ignore root))
      (let ((parent (lineage-test-bytes (pg::wal-control-path)))
            (source (pg::physical-store-control-tuple store)))
        (let* ((pending (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))
               (reference (getf pending :retained-reference)))
          (ok (pg::wal-retained-reference-valid-p reference))
          (ok (equal source (pg::physical-store-control-tuple store)))
          (ok (equalp parent (lineage-test-bytes (pg::wal-control-path))))
          (when reference
            (ok (eql (getf reference :root-id)
                     (pg::physical-retained-root-current
                      (pg::physical-promotion-retained-owner :pending))))
            (ok (equal reference (getf (pg::physical-recovery-read-control pg::*pgdata*)
                                       :retained-reference)))
            (multiple-value-bind (metadata) (pg::physical-restartpoint-read pending)
              (ok (= (getf reference :end) (getf metadata :lsn)))
              (ok (= (getf reference :end-digest) (getf metadata :prefix-digest)))
              (ok (pg::physical-promotion-check-pending-retained pending metadata))))))))

(deftest promotion-intent-at-base-publishes-overlap-without-logical-extents
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (ok (null (pg::physical-store-retained-root store)))
       (ok (null (probe-file (pg::physical-recovery-retained-directory))))
       (let* ((pending (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))
              (reference (getf pending :retained-reference)))
         (ok (= 152 (getf reference :base) (getf reference :end)))
         (ok (= #xCBF29CE484222325 (getf reference :end-digest)))
         (multiple-value-bind (metadata) (pg::physical-restartpoint-read pending)
           (let ((index (pg::physical-promotion-check-pending-retained pending metadata)))
             (ok (null (getf index :entries)))
             (ok (getf index :base-overlap))))
         (ok (eql (getf reference :root-id)
                  (pg::physical-retained-root-current (pg::physical-promotion-retained-owner :pending))))))
     :applied 152))

(deftest promotion-intent-projects-applied-parent-with-later-selected-history
    (with-promotion-fixture (root store)
      (declare (ignore root))
      (let* ((fork pg::*last-replay-lsn*)
             (selection (pg::physical-store-select-history
                         store 5 (store-history-text 1 fork) (store-history-eor fork 5 1)))
             (source (pg::physical-store-control-tuple store))
             (pending (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))
             (reference (getf pending :retained-reference)))
        (declare (ignore selection))
        (ok (= 2 (getf pending :promotion-child)))
        (ok (= 1 (getf pending :applied-timeline (getf pending :timeline))))
        (ok (equal source (pg::physical-store-control-tuple store)))
        (multiple-value-bind (metadata) (pg::physical-restartpoint-read pending)
          (let ((index (pg::physical-promotion-check-pending-retained
                        pending metadata (pg::physical-store-retained-history store))))
            (ok (= 1 (getf index :history-target)))
            (ok (= fork (getf reference :end)))
            (ok (every (lambda (entry) (= 1 (getf (third entry) :history-target)))
                       (getf index :entries)))))
        (call-with-promotion-exclusion #'pg::physical-promotion-install-prefix)
        (call-with-promotion-exclusion #'pg::physical-promotion-publish-child-control)
        (ok (pg::physical-promotion-check-child-control 2 2)))))

(deftest promotion-intent-uses-applied-child-selection-digest-before-future-target
    (with-promotion-fixture (root store)
      (declare (ignore root))
      (let* ((text (store-history-text 1 216))
             (snapshot (sample-checkpoint-snapshot
                        :version 2 :start 304 :end 424 :redo 304
                        :timeline 2 :prev-timeline 1 :next-xid 50 :oldest-active-xid 50)))
        (pg::physical-store-select-history store 2 text (store-history-eor 216 2 1))
        (let ((parent-digest (getf (pg::physical-store-selection store) :digest)))
          (pg::physical-store-input store 216 (store-history-eor 216 2 1))
          (pg::physical-store-input store 272 (sample-physical-record 272))
          (pg::physical-store-input
           store 304 (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 304
                                            (pg::wal-encode-checkpoint-snapshot snapshot)))
          (pg::physical-store-flush store)
          (setf pg::*last-replay-lsn* 424
                (getf pg::*physical-recovery-state* :replay-timeline) 2
                (getf pg::*physical-recovery-state* :history-target) 2
                (getf pg::*physical-recovery-state* :history-digest) parent-digest
                (getf pg::*physical-recovery-state* :required-applied) 424
                (getf pg::*physical-recovery-state* :required-timeline) 2
                (getf pg::*physical-recovery-state* :source-checkpoint) snapshot
                (getf pg::*physical-recovery-state* :source-reservation)
                (list :version 1 :through 424 :bound 1002 :complete t))
          (setf pg::*physical-recovery-state*
                (pg::physical-recovery-write-control pg::*physical-recovery-state* 2 424 50))
          (pg::physical-store-compact store 424 #'pg::physical-restartpoint-publish)
          (pg::physical-store-select-history
           store 5 (concatenate 'string text (store-history-text 2 424))
           (store-history-eor 424 5 2))
          (let* ((future-digest (getf (pg::physical-store-selection store) :digest))
                 (count-before (hash-table-count pg::*physical-retained-root-references*))
                 (recovery-facts (pg::native-control-read pg::*pgdata*))
                 (count-after (hash-table-count pg::*physical-retained-root-references*))
                 (pending (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))
                 (reference (getf pending :retained-reference))
                 (index (pg::physical-retained-inventory-load
                         (pg::physical-recovery-retained-directory) (getf reference :root-id)
                         (pg::physical-promotion-retained-identity (getf pending :system-id) reference)
                         (pg::timeline-parse-history 2 text))))
            (ok (= count-before count-after))
            (ok (= 2 (getf recovery-facts :checkpoint-timeline) (getf recovery-facts :recovery-timeline)))
            (ok (= 304 (getf recovery-facts :checkpoint)))
            (ok (= 424 (getf recovery-facts :recovery-lsn)))
            (ok (= 3 (getf pending :promotion-child)))
            (ok (= 2 (getf pending :applied-timeline)))
            (ok (/= parent-digest future-digest))
            (ok (= 2 (getf index :history-target)))
            (ok (string= text (getf index :history-text)))
            (ok (every (lambda (entry)
                         (let ((metadata (third entry)))
                           (and (= 2 (getf metadata :history-target))
                                (= parent-digest (getf metadata :history-digest))
                                (string= text (getf metadata :history-text)))))
                       (getf index :entries))))))))

(deftest retained-intent-interruptions-protect-and-reconcile-the-durable-selection
    (dolist (cut '(:after-retained-intent-publication :after-retained-intent-control
                   :after-retained-intent-handoff))
      (dolist (nonlocal '(nil t))
        (with-promotion-fixture (root store)
          (declare (ignore root))
          (let* ((owner (pg::physical-promotion-retained-owner :pending))
                 (source-owner (pg::physical-store-retained-owner store))
                 (source-id (pg::physical-store-retained-root store))
                 (directory (pg::physical-recovery-retained-directory))
                 (source (lineage-test-bytes (pg::physical-store-path store "receive.control")))
                 (candidate nil))
            (let ((pg::*physical-promotion-test-hook*
                    (lambda (point)
                      (when (eq point cut)
                        (setf candidate
                              (or (pg::physical-retained-root-current owner)
                                  (sb-thread:with-mutex (pg::*physical-retained-inventory-publish-lock*)
                                    (let ((hold (gethash owner pg::*physical-retained-root-updates*)))
                                      (pg::physical-retained-root-reference-root-id
                                       (pg::physical-retained-root-hold-reference hold))))))
                        (if nonlocal (throw 'intent-cut :interrupted)
                            (error "retained intent interruption"))))))
              (if nonlocal
                  (ok (eq :interrupted
                          (catch 'intent-cut
                            (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))))
                  (ok (intake-fails-p
                       (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))))))
            (ok pg::*physical-recovery-failed-p*)
            (ok (= 2 (getf pg::*physical-recovery-state* :phase)))
            (ok (pg::physical-retained-root-protected-p directory :inventory candidate))
            (ok (eql source-id (pg::physical-retained-root-current source-owner)))
            (ok (equalp source (lineage-test-bytes (pg::physical-store-path store "receive.control"))))
            (let* ((control (pg::physical-recovery-read-control pg::*pgdata*))
                   (phase (getf control :phase))
                   (history (pg::physical-store-retained-history store)))
              (ok (= (if (eq cut :after-retained-intent-publication) 2 3) phase))
              (if (= phase 3)
                  (multiple-value-bind (metadata) (pg::physical-restartpoint-read pg::*physical-recovery-state*)
                    (ok (eql candidate (getf (getf control :retained-reference) :root-id)))
                    (pg::physical-promotion-restore-pending-retained control metadata history)
                    (ok (eql candidate (pg::physical-retained-root-current owner))))
                  (progn
                    (pg::physical-retained-root-reconcile
                     owner directory (pg::physical-store-retained-identity store) history
                     (lambda () (pg::physical-promotion-pending-root-selection control)))
                    (ok (null (pg::physical-retained-root-current owner)))))
              (ok (zerop (hash-table-count pg::*physical-retained-root-holds*)))
              (ok (zerop (hash-table-count pg::*physical-retained-root-updates*))))
            (ok (eql source-id (pg::physical-retained-root-current source-owner))))))))

(deftest retained-intent-rejects-changed-nonroot-control-fields-before-handoff
    (dolist (kind '(:pending :receive))
      (with-promotion-fixture (root store)
        (declare (ignore root))
        (let* ((owner (pg::physical-promotion-retained-owner :pending))
               (path (if (eq kind :pending) (pg::physical-recovery-control-path)
                         (pg::physical-store-path store "receive.control")))
               (original nil)
               (pg::*physical-promotion-test-hook*
                 (lambda (point)
                   (when (eq point :after-retained-intent-control)
                     (setf original (lineage-test-bytes path))
                     (let ((bad (copy-seq original)))
                       (if (eq kind :pending)
                           (pg::wal-set-i64 bad 60 (1+ (pg::wal-i64 bad 60)))
                           (pg::wal-set-u64 bad 32 (+ 32 (pg::wal-u64 bad 32))))
                       (pg::wal-set-u32 bad (- (length bad) 4) 0)
                       (pg::wal-set-u32 bad (- (length bad) 4) (pg::wal-crc32 bad))
                       (pg::wal-atomic-write-bytes path bad))))))
          (unwind-protect
               (progn
                 (ok (intake-fails-p
                      (lambda () (call-with-promotion-exclusion #'pg::physical-promotion-publish-intent))))
                 (ok pg::*physical-recovery-failed-p*)
                 (ok (= 2 (getf pg::*physical-recovery-state* :phase)))
                 (ok (null (pg::physical-retained-root-current owner)))
                 (let* ((control (pg::physical-recovery-read-control pg::*pgdata*))
                        (id (getf (getf control :retained-reference) :root-id)))
                   (ok (= 3 (getf control :phase)))
                   (ok (pg::physical-retained-root-protected-p
                        (pg::physical-recovery-retained-directory) :inventory id))
                   (ok (= 1 (hash-table-count pg::*physical-retained-root-holds*)))))
            (when original (pg::wal-atomic-write-bytes path original)))))))

(deftest child-base-backup-control-starts-new-archive-identity-without-changing-source
    (handoff-test-committed
     (lambda (root store reference)
       (declare (ignore root store))
       (with-physical-store-directory (target)
         (let* ((before (lineage-test-bytes (pg::wal-control-path)))
                (fields (multiple-value-list (pg::wal-read-control-fields)))
                (entry (pg::physical-bootstrap-copy-file "global/pg_wal_control" target))
                (bytes (lineage-test-bytes (merge-pathnames "global/pg_wal_control" target)))
                (copied (multiple-value-list (pg::wal-decode-control-line (pg::bytes->string bytes))))
                (metadata (copy-list (seventh fields))))
           (remf metadata :retained-reference)
           (ok (equal (subseq fields 0 6) (subseq copied 0 6)))
           (ok (equal metadata (seventh copied)))
           (ok (null (getf (seventh copied) :retained-reference)))
           (ok (equal reference (getf (seventh fields) :retained-reference)))
           (ok (equalp before (lineage-test-bytes (pg::wal-control-path))))
           (ok (equal entry (list "global/pg_wal_control" (length bytes)
                                  (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))))))))

(deftest base-backup-without-historical-reference-keeps-exact-control-bytes
    (with-promotion-fixture (root store)
      (declare (ignore root store))
      (with-physical-store-directory (target)
        (let* ((before (lineage-test-bytes (pg::wal-control-path)))
               (entry (pg::physical-bootstrap-copy-file "global/pg_wal_control" target))
               (bytes (lineage-test-bytes (merge-pathnames "global/pg_wal_control" target))))
          (ok (equalp before bytes))
          (ok (equal entry (list "global/pg_wal_control" (length bytes)
                                 (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))))))

(defun call-with-child-inspection-fixture (function)
  (handoff-test-committed
   (lambda (source store reference)
     (declare (ignore source store reference))
     (with-physical-store-directory (target)
       (let* ((fields (multiple-value-list (pg::wal-read-control-fields)))
              (snapshot (third fields))
              (timeline (fourth fields))
              (files (mapcar
                      (lambda (name) (pg::physical-bootstrap-copy-file name target))
                      (list "global/pg_wal_control"
                            (concatenate 'string "pg_wal/" (pg::wal-segment-filename 0 timeline :timeline))
                            (concatenate 'string "pg_wal/" (pg::timeline-history-filename timeline)))))
              (manifest (list :version 3 :system-id (getf (seventh fields) :system-id)
                              :cluster-identity "history-source" :timeline timeline
                              :checkpoint (first fields) :checkpoint-start (getf snapshot :start)
                              :end (second fields) :next-xid (getf snapshot :next-xid)
                              :source-pin "inspect" :files files)))
         (write-recovery-manifest-text target (pg::physical-bootstrap-manifest-text manifest))
         (pg::wal-atomic-write-bytes
          (merge-pathnames "recovery.required" target) (pg::physical-recovery-marker-bytes))
         (let ((pg::*pgdata* target))
           (funcall function target manifest (nth-value 1 (pg::recovery-read-manifest target)))))))))

(deftest native-control-inspects-ordinary-child-recovery-without-mutation
    (call-with-child-inspection-fixture
     (lambda (target manifest digest)
       (let ((counts (list (hash-table-count pg::*physical-retained-root-references*)
                           (hash-table-count pg::*physical-retained-root-holds*)
                           (hash-table-count pg::*physical-retained-root-updates*))))
         (dolist (phase '(0 1 2))
           (pg::wal-atomic-write-bytes
            (pg::physical-recovery-control-path)
            (pg::physical-recovery-control-image
             phase (getf manifest :system-id) 2 (if (zerop phase) 0 digest)
             (getf manifest :checkpoint) (getf manifest :end) (getf manifest :next-xid)
             :state-time 42))
           (let* ((before (pg::native-control-capture))
                  (facts (pg::native-control-read target)))
             (ok (= (if (zerop phase) pg::+db-startup+ pg::+db-in-archive-recovery+)
                    (getf facts :db-state)))
             (ok (= 2 (getf facts :checkpoint-timeline) (getf facts :recovery-timeline)))
             (ok (= (getf manifest :checkpoint-start) (getf facts :checkpoint)))
             (ok (= (getf manifest :end) (getf facts :recovery-lsn)))
             (ok (equalp before (pg::native-control-capture)))))
         (ok (equal counts
                    (list (hash-table-count pg::*physical-retained-root-references*)
                          (hash-table-count pg::*physical-retained-root-holds*)
                          (hash-table-count pg::*physical-retained-root-updates*)))))
       (ok (null (probe-file (merge-pathnames "postmaster.pid" target)))))))

(deftest missing-child-recovery-control-cannot-select-primary
    (call-with-child-inspection-fixture
     (lambda (target manifest digest)
       (declare (ignore manifest digest))
       (let* ((names '("recovery.required" "physical-backup.manifest"
                       "physical-restartpoint.control" "physical-follow.control"))
              (paths (mapcar (lambda (name) (merge-pathnames name target)) names))
              (saved (mapcar (lambda (path)
                               (cons path (when (probe-file path) (lineage-test-bytes path))))
                             paths))
              (stray-bytes (make-array 3 :element-type '(unsigned-byte 8)
                                         :initial-contents '(1 2 3)))
              (artifacts (list (list (first paths) (lineage-test-bytes (first paths)) 2)
                               (list (second paths) (lineage-test-bytes (second paths)) 3)
                               (list (third paths) stray-bytes 4)
                               (list (fourth paths) stray-bytes 6)))
              (primary (symbol-function 'pg::physical-promotion-startup-primary-p))
              (calls 0))
         (unwind-protect
              (progn
                (dolist (path paths)
                  (when (probe-file path) (delete-file path)))
                (setf (symbol-function 'pg::physical-promotion-startup-primary-p)
                      (lambda () (incf calls) t))
                (dolist (artifact artifacts)
                  (destructuring-bind (path bytes expected-slot) artifact
                    (pg::wal-atomic-write-bytes path bytes)
                    (let ((before (pg::native-control-capture)))
                      (ok (null (second before)))
                      (dolist (slot '(2 3 4 6))
                        (ok (equalp (if (= slot expected-slot) bytes nil)
                                    (nth slot before))))
                      (ok (signals (pg::native-control-read target) 'error))
                      (ok (signals (pg::physical-bootstrap-check-startup) 'error))
                      (ok (zerop calls))
                      (ok (equalp before (pg::native-control-capture))))
                    (delete-file path))))
           (setf (symbol-function 'pg::physical-promotion-startup-primary-p) primary)
           (dolist (entry saved)
             (if (cdr entry)
                 (pg::wal-atomic-write-bytes (car entry) (cdr entry))
                 (when (probe-file (car entry)) (delete-file (car entry))))))))))

(deftest native-control-rechecks-child-history-after-validation
    (call-with-child-inspection-fixture
     (lambda (target manifest digest)
       (pg::wal-atomic-write-bytes
        (pg::physical-recovery-control-path)
        (pg::physical-recovery-control-image
         2 (getf manifest :system-id) 2 digest (getf manifest :checkpoint)
         (getf manifest :end) (getf manifest :next-xid)))
       (let* ((path (pg::timeline-history-path 2 target))
              (bytes (lineage-test-bytes path))
              (checker (symbol-function 'pg::physical-promotion-check-child-control))
              (calls 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-promotion-check-child-control)
                      (lambda (&rest arguments)
                        (multiple-value-prog1 (apply checker arguments)
                          (when (= 1 (incf calls))
                            (pg::wal-atomic-write-bytes
                             path (pg::string->bytes (store-history-text 1 215)))))))
                (ok (signals (pg::native-control-read target) 'error))
                (ok (= 1 calls)))
           (setf (symbol-function 'pg::physical-promotion-check-child-control) checker)
           (pg::wal-atomic-write-bytes path bytes))))))

(cl-spec:defproperty native-control-recovery-role-matches-startup-selection
    ((timeline (range integer 1 4294967294)) (phase (range integer 0 3))
                                             (committed (range integer 0 1)) (present (range integer 0 1)))
  (:about pg::native-control-recovery-role-p)
  (:trials (:normal 200))
  (let ((control (when (= present 1) (list :phase phase :timeline timeline))))
    (eql (and (= present 1) (or (< phase 3) (zerop committed)))
         (not (null (pg::native-control-recovery-role-p control (+ timeline committed)))))))

(deftest generated-native-control-recovery-role-selection
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'native-control-recovery-role-matches-startup-selection :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest native-control-rejects-mutated-child-base-control-and-history
    (call-with-child-inspection-fixture
     (lambda (target manifest digest)
       (declare (ignore target))
       (pg::wal-atomic-write-bytes
        (pg::physical-recovery-control-path)
        (pg::physical-recovery-control-image
         2 (getf manifest :system-id) 2 digest (getf manifest :checkpoint)
         (getf manifest :end) (getf manifest :next-xid)))
       (dolist (kind '(:control :history :marker))
         (let* ((path (ecase kind
                        (:control (pg::wal-control-path))
                        (:history (pg::timeline-history-path 2 pg::*pgdata*))
                        (:marker (merge-pathnames "recovery.required" pg::*pgdata*))))
                (original (lineage-test-bytes path)))
           (unwind-protect
                (progn
                  (ecase kind
                    (:control
                     (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-id namespace metadata)
                         (pg::wal-read-control-fields)
                       (setf (getf metadata :state-time) (1+ (getf metadata :state-time)))
                       (pg::wal-atomic-write-bytes
                        path (pg::wal-encode-control-image checkpoint durable snapshot
                                                           :timeline insertion :checkpoint-timeline checkpoint-id
                                                           :namespace namespace :metadata metadata))))
                    (:history
                     (pg::wal-atomic-write-bytes path (pg::string->bytes (store-history-text 1 215))))
                    (:marker (delete-file path)))
                  (let ((before (pg::native-control-capture)))
                    (ok (intake-fails-p (lambda () (pg::native-control-facts before))))
                    (ok (equalp before (pg::native-control-capture)))))
             (pg::wal-atomic-write-bytes path original)))))))

(deftest native-control-cannot-reclassify-rooted-primary-as-ordinary-recovery
    (handoff-test-committed
     (lambda (root store reference)
       (declare (ignore store reference))
       (let* ((fields (multiple-value-list (pg::wal-read-control-fields)))
              (snapshot (third fields))
              (files (mapcar
                      (lambda (name)
                        (let ((bytes (lineage-test-bytes (merge-pathnames name root))))
                          (list name (length bytes) (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
                      (list "global/pg_wal_control" "pg_wal/00000002.history"
                            "pg_wal/000000020000000000000000.wal")))
              (manifest (list :version 3 :system-id (getf (seventh fields) :system-id)
                              :cluster-identity "history-source" :timeline 2
                              :checkpoint (first fields) :checkpoint-start (getf snapshot :start)
                              :end (second fields) :next-xid (getf snapshot :next-xid)
                              :source-pin "inspect" :files files)))
         (write-recovery-manifest-text root (pg::physical-bootstrap-manifest-text manifest))
         (delete-file (pg::physical-restartpoint-pointer-path))
         (pg::wal-atomic-write-bytes
          (pg::physical-recovery-control-path)
          (pg::physical-recovery-control-image
           2 (getf manifest :system-id) 2 (nth-value 1 (pg::recovery-read-manifest root))
           (first fields) (second fields) (getf snapshot :next-xid)))
         (let ((before (pg::native-control-capture)))
           (ok (handler-case (progn (pg::native-control-facts before) nil)
                 (error (condition)
                   (search "immutable base checkpoint" (princ-to-string condition)))))
           (ok (equalp before (pg::native-control-capture))))))))
