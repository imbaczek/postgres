(in-package #:pg-physical-tests)

(defun reseed-follow-proof-test-record (initial root kind)
  (let* ((record (reseed-follow-intent-test-record initial root))
         (generation (first (getf record :generations)))
         (coordinates (getf generation :coordinates))
         (parent (getf coordinates :base-timeline))
         (child (1+ parent))
         (fork (if (member kind '(:at-fork :later-writer)) (getf coordinates :base)
                   (+ 128 (getf coordinates :base))))
         (history (if (= parent 1) nil (pg::timeline-history-bytes parent root))))
    (unless (eq kind :same-timeline)
      (setf history (pg::timeline-encode-history child parent fork "proof test promotion" history)
            (getf coordinates :source-timeline) child)
      (when (eq kind :later-writer)
        (setf history (pg::timeline-encode-history (1+ child) child (+ fork 128) "second promotion" history)
              (getf coordinates :source-timeline) (1+ child))))
    (when (eq kind :retained-timeline)
      (setf (getf coordinates :kind) :retained-timeline))
    (setf (getf generation :coordinates) coordinates)
    (setf (getf generation :source-history) (if history (pg::bytes->string history) "")
          (getf generation :phase) :proved)
    (let ((anchor (pg::physical-follow-anchor-copy
                   (append coordinates
                           (list :history-text (getf generation :source-history))
                           (if (member kind '(:same-timeline :retained-timeline))
                               (unless (eq kind :retained-timeline) (list :kind :same-timeline))
                               (list :eor-octets
                                     (coerce (pg::wal-encode-end-of-recovery-record fork 42 child parent 1) 'list)))))))
      (setf (getf generation :anchor)
            (loop for (key value) on anchor by #'cddr
                  unless (eq key :history-text) append (list key value))))
    (pg::physical-reseed-follow-copy record)))

(cl-spec:defproperty empty-follow-base-plan-preserves-visibility-and-geometry
    ((kind-choice (range integer 0 2)) (at-fork boolean) (xid-choice (range integer 0 2)))
  (:about pg::physical-follow-empty-base-binding)
  (:trials (:normal 100))
  (let* ((kind (nth kind-choice '(:same-timeline :descendant :retained-timeline)))
         (record (reseed-follow-codec-fixture kind :proved))
         (generation (first (getf record :generations)))
         (anchor (pg::physical-reseed-follow-generation-anchor generation))
         (selected (and (eq kind :descendant) at-fork))
         (end (if (and at-fork (not (eq kind :same-timeline))) 192 152))
         (observed (nth xid-choice '(3 42 18446744073709551616)))
         (base (list :system-id (getf anchor :system-id) :timeline 2 :end end
                     :checkpoint end :manifest-digest (getf anchor :manifest-digest)
                     :cluster-identity "empty-base-property" :next-xid observed :observed-next-xid observed
                     :required-applied end :required-timeline 2)))
    (setf (getf anchor :base) end (getf anchor :receive-lsn) end)
    (setf anchor (pg::physical-follow-anchor-copy anchor))
    (let ((before (pg::physical-bootstrap-manifest-text base)))
      (multiple-value-bind (binding model) (pg::physical-follow-empty-base-binding base anchor)
        (and (= observed (getf (getf binding :recovery) :observed-next-xid))
             (equal (list end 2) (getf binding :applied))
             (equal (getf binding :applied) (getf binding :required))
             (eq (not (null selected)) (not (equal (getf binding :before) (getf binding :after))))
             (= (if selected 3 2) (pg::physical-store-selected-timeline model))
             (null (pg::physical-store-directory model))
             (not (member :resource-fact (getf binding :recovery)))
             (equal before (pg::physical-bootstrap-manifest-text base)))))))

(deftest generated-empty-follow-base-plan-visibility-and-geometry
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'empty-follow-base-plan-preserves-visibility-and-geometry
                                       :seed 118 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-proof-validates-the-real-empty-base-without-a-receive-spool
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-follow-installed-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine plan))
         (let* ((names (sort (pg::directory-entry-names root :follow-symlinks t) #'string<))
                (paths (list (merge-pathnames "physical-recovery.control" root)
                             (merge-pathnames "global/pg_wal_control" root)
                             (merge-pathnames "global/pg_control" root)))
                (bytes (mapcar #'lineage-test-bytes paths)))
           (pg::call-with-physical-reseed-follow-owner
            root
            (lambda (owner)
              (let* ((record (reseed-follow-proof-test-record
                              (pg::physical-reseed-follow-initial-record owner) root kind))
                     (symbols '(pg::physical-store-history-segment-entries
                                pg::physical-store-read-retained-control pg::physical-restartpoint-read))
                     (originals (mapcar #'symbol-function symbols))
                     (calls 0))
                (unwind-protect
                     (progn
                       (dolist (symbol symbols)
                         (setf (symbol-function symbol)
                               (lambda (&rest arguments)
                                 (declare (ignore arguments))
                                 (incf calls)
                                 (error "proof attempted receive-spool I/O"))))
                       (pg::call-with-physical-reseed-follow-image
                        owner
                        (lambda (fd check)
                          (declare (ignore check))
                          (multiple-value-bind (binding model result) (pg::physical-reseed-follow-proof-at owner fd record)
                            (ok (= 42 (getf result :next-xid) (getf result :required-next-xid)))
                            (ok (= 152 (getf result :start) (getf result :terminal)))
                            (ok (null (pg::physical-store-directory model)))
                            (ok (eq (not (null (member kind '(:at-fork :later-writer))))
                                    (not (equal (getf binding :before) (getf binding :after)))))
                            (when (member kind '(:at-fork :later-writer))
                              (ok (= 2 (pg::physical-store-selected-timeline model)))))))
                       (ok (zerop calls)))
                  (loop for symbol in symbols for original in originals
                        do (setf (symbol-function symbol) original))))))
           (let ((unchanged (every #'equalp bytes (mapcar #'lineage-test-bytes paths))))
             (ok unchanged))
           (ok (equal names (sort (pg::directory-entry-names root :follow-symlinks t) #'string<)))
           (ok (zerop (getf (pg::physical-recovery-read-control root) :phase))))))))

(deftest follow-replay-durable-frontier-retains-its-seed-context
    (let* ((base (list :system-id 7 :timeline 1 :end 152 :manifest-digest 99
                       :cluster-identity "seed-context" :next-xid 42 :observed-next-xid 42))
           (model (pg::physical-follow-empty-base-model base))
           (required '(152 9))
           (seed (list 152 1 42 nil nil nil nil))
           (pg::*physical-recovery-state* (list :sentinel t))
           (pg::*physical-receive-store* :sentinel)
           (original (symbol-function 'pg::physical-recovery-check-store-point))
           (calls 0) (context nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-recovery-check-store-point)
                   (lambda (candidate point timeline)
                     (incf calls)
                     (setf context
                           (and (eq model candidate) (eq model pg::*physical-receive-store*)
                                (= point (first required)) (= timeline (second required))
                                (= 1 (getf pg::*physical-recovery-state* :replay-timeline))
                                (= 152 (getf pg::*physical-recovery-state* :required-applied))
                                (= 9 (getf pg::*physical-recovery-state* :required-timeline))))
                     (error "durable point rejected")))
             (ok (intake-fails-p
                  (lambda () (pg::physical-follow-scan-replay-from-seed
                              model base '(152 1) required 42 seed
                              (lambda (&rest ignored) (declare (ignore ignored)) nil) nil))))
             (ok (= 1 calls))
             (ok context)
             (ok (eq :sentinel pg::*physical-receive-store*))
             (ok (equal '(:sentinel t) pg::*physical-recovery-state*)))
        (setf (symbol-function 'pg::physical-recovery-check-store-point) original))))

(deftest reseed-follow-empty-base-replay-rejects-changed-models-and-seeds
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let ((record (reseed-follow-proof-test-record
                         (pg::physical-reseed-follow-initial-record owner) root :same-timeline)))
            (pg::call-with-physical-reseed-follow-image
             owner
             (lambda (fd check)
               (declare (ignore check))
               (pg::call-with-physical-backup-image-directory
                fd
                (lambda (directory)
                  (let ((pg::*pgdata* directory))
                    (multiple-value-bind (checked base control)
                        (pg::physical-reseed-follow-check-local-intent owner fd record)
                      (declare (ignore checked control))
                      (let ((anchor (pg::physical-reseed-follow-generation-anchor
                                     (first (getf record :generations)))))
                        (dolist (change '(:floor :written :flushed :floor-digest :written-digest
                                          :flushed-digest :directory :closed :failed :observed
                                          :next-xid :checkpoint :reservation :binding-xid :binding-resource))
                          (multiple-value-bind (binding model)
                              (pg::physical-follow-empty-base-binding base anchor)
                            (let ((changed-base (copy-tree base)))
                              (ok (= 42 (getf (pg::physical-follow-validate-empty-base-replay
                                               model changed-base binding) :next-xid)))
                              (ecase change
                                (:floor (incf (pg::physical-store-floor-lsn model)))
                                (:written (incf (pg::physical-store-written-lsn model)))
                                (:flushed (incf (pg::physical-store-flushed-lsn model)))
                                (:floor-digest (incf (pg::physical-store-floor-digest model)))
                                (:written-digest (incf (pg::physical-store-written-digest model)))
                                (:flushed-digest (incf (pg::physical-store-flushed-digest model)))
                                (:directory (setf (pg::physical-store-directory model) root))
                                (:closed (setf (pg::physical-store-closed-p model) t))
                                (:failed (setf (pg::physical-store-failed-p model) t))
                                (:observed (incf (getf changed-base :observed-next-xid)))
                                (:next-xid (incf (getf changed-base :next-xid)))
                                (:checkpoint (setf (getf changed-base :source-checkpoint) '(:invalid t)))
                                (:reservation (setf (getf changed-base :source-reservation) '(:invalid t)))
                                (:binding-xid (incf (getf (getf binding :recovery) :observed-next-xid)))
                                (:binding-resource
                                 (setf (getf (getf binding :recovery) :resource-fact)
                                       (list :resource-parameters '(100 8 10 0 64)
                                             :resource-lsn 96 :resource-timeline 1))))
                              (ok (intake-fails-p
                                   (lambda ()
                                     (pg::physical-follow-validate-empty-base-replay
                                      model changed-base binding)))
                                  (format nil "Reject changed empty-base ~A" change))))))))))))))))))

(deftest reseed-follow-proof-rejects-an-eor-crossing-the-next-fork
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((record (reseed-follow-proof-test-record
                          (pg::physical-reseed-follow-initial-record owner) root :later-writer))
                 (generation (first (getf record :generations)))
                 (parent-history (pg::timeline-encode-history 2 1 152 "proof test promotion" nil))
                 (short-history (pg::timeline-encode-history 3 2 192 "short child interval" parent-history))
                 (octets (getf (getf generation :anchor) :eor-octets)))
            (ok (> (+ 152 (length octets)) 192))
            (setf (getf generation :source-history) (pg::bytes->string short-history))
            (ok (intake-fails-p (lambda () (pg::physical-reseed-follow-copy record))))))))))

(deftest reseed-follow-proof-preserves-an-exhausted-xid-in-a-real-installed-base
    (let ((sample (symbol-function 'sample-checkpoint-snapshot))
          (writer (symbol-function 'pg::wal-atomic-write-bytes))
          (samples 0) (controls 0) (proofs 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'sample-checkpoint-snapshot)
                   (lambda (&rest changes)
                     (incf samples)
                     (let ((snapshot (apply sample changes)))
                       (setf (getf snapshot :version) 2
                             (getf snapshot :next-xid) 18446744073709551616
                             (getf snapshot :oldest-active-xid) 18446744073709551616
                             (getf snapshot :xid-reserved-through) 18446744073709551615)
                       (assert (pg::wal-checkpoint-snapshot-valid-p snapshot))
                       snapshot))
                   (symbol-function 'pg::wal-atomic-write-bytes)
                   (lambda (path bytes &rest options)
                     (let ((replacement
                             (when (and (equal (namestring path) (namestring (pg::control-path)))
                                        (equalp bytes (pg::string->bytes (format nil "1003~%"))))
                               (incf controls)
                               (pg::string->bytes (format nil "18446744073709551616~%")))))
                       (apply writer path (or replacement bytes) options))))
             (call-with-reseed-follow-installed-fixture
              (lambda (root stage incoming quarantine plan)
                (declare (ignore stage incoming quarantine plan))
                (pg::call-with-physical-reseed-follow-owner
                 root
                 (lambda (owner)
                   (let ((record (reseed-follow-proof-test-record
                                  (pg::physical-reseed-follow-initial-record owner) root :same-timeline)))
                     (pg::call-with-physical-reseed-follow-image
                      owner
                      (lambda (fd check)
                        (declare (ignore check))
                        (multiple-value-bind (binding model result) (pg::physical-reseed-follow-proof-at owner fd record)
                          (declare (ignore model))
                          (incf proofs)
                          (ok (= 18446744073709551616 (getf (getf binding :recovery) :observed-next-xid)
                                 (getf result :next-xid) (getf result :required-next-xid)))
                          (ok (= 18446744073709551615 (getf (getf result :reservation) :bound)))
                          (ok (getf (getf result :reservation) :complete))))))))))
             (ok (= 1 samples controls proofs)))
        (setf (symbol-function 'sample-checkpoint-snapshot) sample
              (symbol-function 'pg::wal-atomic-write-bytes) writer))))
