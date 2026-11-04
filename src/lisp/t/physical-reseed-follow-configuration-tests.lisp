(in-package #:pg-physical-tests)

(cl-spec:defproperty empty-follow-recovery-image-preserves-required-visibility
    ((kind-choice (range integer 0 2)) (at-fork boolean)
                                       (xid-choice (range integer 0 2)) (time (range integer -100000 100000)))
  (:about pg::physical-follow-binding-recovery-image)
  (:trials (:normal 100))
  (let* ((kind (nth kind-choice '(:same-timeline :descendant :retained-timeline)))
         (record (reseed-follow-codec-fixture kind :proved))
         (anchor (pg::physical-reseed-follow-generation-anchor (first (getf record :generations))))
         (end (if (and at-fork (not (eq kind :same-timeline))) 192 152))
         (observed (nth xid-choice '(3 42 18446744073709551616)))
         (base (list :system-id (getf anchor :system-id) :timeline 2 :end end :checkpoint end
                     :manifest-digest (getf anchor :manifest-digest) :cluster-identity "image-property"
                     :next-xid observed :observed-next-xid observed :required-applied end :required-timeline 2)))
    (setf (getf anchor :base) end (getf anchor :receive-lsn) end)
    (setf anchor (pg::physical-follow-anchor-copy anchor))
    (let* ((binding (pg::physical-follow-empty-base-binding base anchor))
           (control (pg::physical-recovery-decode-control
                     (pg::physical-follow-binding-recovery-image binding time :selected t)))
           (selected (and (eq kind :descendant) at-fork)))
      (and (= 2 (getf control :phase) (getf control :timeline) (getf control :applied-timeline (getf control :timeline)))
           (= end (getf control :applied))
           (= observed (getf control :next-xid))
           (= time (getf control :state-time))
           (= (getf anchor :manifest-digest) (getf control :manifest-digest))
           (eql (if selected 3 nil) (getf control :history-target))
           (null (getf control :resource-fact))))))

(deftest generated-empty-follow-recovery-image-visibility
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'empty-follow-recovery-image-preserves-required-visibility
                                       :seed 119 :profile :normal))))
      (ok (eq :passed (getf data :status)))))

(deftest reseed-follow-configuration-images-are-inert-and-match-existing-store-images
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-follow-installed-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine plan))
         (let* ((paths (list (merge-pathnames "physical-recovery.control" root)
                             (merge-pathnames "global/pg_wal_control" root)
                             (merge-pathnames "global/pg_control" root)))
                (before (mapcar #'lineage-test-bytes paths))
                (names (sort (pg::directory-entry-names root :follow-symlinks t) #'string<)))
           (pg::call-with-physical-reseed-follow-owner
            root
            (lambda (owner)
              (let ((record (reseed-follow-proof-test-record
                             (pg::physical-reseed-follow-initial-record owner) root kind)))
                (pg::call-with-physical-reseed-follow-image
                 owner
                 (lambda (fd check)
                   (declare (ignore check))
                   (multiple-value-bind (images binding)
                       (pg::physical-reseed-follow-configuration-images-at owner fd record 12345)
                     (let* ((config (pg::physical-follow-decode-config (cdr (assoc "follow.pending" images :test #'equal))))
                            (state (sixth config))
                            (control (pg::physical-recovery-decode-control
                                      (cdr (assoc "recovery.initial" images :test #'equal))))
                            (after (getf binding :after))
                            (selected (not (equal (getf binding :before) after)))
                            (coordinates (getf (first (getf record :generations)) :coordinates)))
                       (let ((deterministic (equalp images (pg::physical-reseed-follow-configuration-images-at
                                                            owner fd record 12345))))
                         (ok deterministic))
                       (ok (not (getf state :enabled)))
                       (ok (null (getf state :preparing)))
                       (ok (null (getf state :cleanup)))
                       (ok (eq :pending (getf (fifth config) :phase)))
                       (ok (equal (getf (getf record :attempt) :source) (getf state :source)))
                       (ok (equal (getf (first (getf record :generations)) :name) (fourth config)))
                       (ok (= (getf coordinates :source-timeline) (getf state :known-timeline)))
                       (ok (not (assoc "follow.active" images :test #'equal)))
                       (ok (= 2 (getf control :phase)))
                       (ok (= 42 (getf control :next-xid)))
                       (ok (zerop (length (cdr (assoc "receive.lock" images :test #'equal)))))
                       (with-physical-store-directory (spool)
                         (let ((store (pg::physical-store-open
                                       spool (getf coordinates :system-id) (getf coordinates :base-timeline)
                                       :create t :start-lsn (getf coordinates :base)
                                       :retained-identity
                                       (list :system-id (getf coordinates :system-id)
                                             :base-timeline (getf coordinates :base-timeline)
                                             :base (getf coordinates :base)
                                             :manifest-digest (getf coordinates :manifest-digest)))))
                           (unwind-protect
                                (progn
                                  (ok (equalp (lineage-test-bytes (merge-pathnames "receive.control" spool))
                                              (cdr (assoc "receive.initial" images :test #'equal))))
                                  (when selected
                                    (pg::physical-store-select-history
                                     store (eighth after) (getf binding :child-history)
                                     (coerce (getf (getf binding :anchor) :eor-octets) '(vector (unsigned-byte 8))))
                                    (ok (equalp (lineage-test-bytes (merge-pathnames "receive.control" spool))
                                                (cdr (assoc "receive.selected" images :test #'equal))))
                                    (ok (equalp (lineage-test-bytes
                                                 (pg::physical-store-history-path store (eighth after) (ninth after)))
                                                (cdr (assoc "history.receive" images :test #'equal))))
                                    (let ((selected-control (pg::physical-recovery-decode-control
                                                             (cdr (assoc "recovery.selected" images :test #'equal)))))
                                      (ok (= 2 (getf selected-control :history-target))))
                                    (ok (equalp (pg::string->bytes (getf binding :child-history))
                                                (cdr (assoc "history.wal" images :test #'equal))))))
                             (pg::physical-store-close store)))))))))))
           (ok (every #'equalp before (mapcar #'lineage-test-bytes paths)))
           (ok (equal names (sort (pg::directory-entry-names root :follow-symlinks t) #'string<))))))))

(deftest reseed-follow-configuration-images-require-proof-and-check-time-before-owner-io
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-owner
        root
        (lambda (owner)
          (let* ((record (reseed-follow-proof-test-record
                          (pg::physical-reseed-follow-initial-record owner) root :same-timeline))
                 (preparing (pg::physical-reseed-follow-copy record)))
            (setf (getf (first (getf preparing :generations)) :phase) :preparing
                  (getf (first (getf preparing :generations)) :anchor) nil)
            (setf preparing (pg::physical-reseed-follow-copy preparing))
            (pg::call-with-physical-reseed-follow-image
             owner
             (lambda (fd check)
               (declare (ignore check))
               (let ((owner-check (symbol-function 'pg::physical-directory-check-owner-common))
                     (connect (symbol-function 'pg::physical-libpq-connect-frozen))
                     (checks 0) (connections 0))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::physical-directory-check-owner-common)
                              (lambda (&rest arguments) (incf checks) (apply owner-check arguments))
                              (symbol-function 'pg::physical-libpq-connect-frozen)
                              (lambda (&rest arguments)
                                (declare (ignore arguments))
                                (incf connections)
                                (error "configuration image derivation attempted source I/O")))
                        (dolist (time '(18446744073709551616 "now"))
                          (ok (handler-case
                                  (progn (pg::physical-reseed-follow-configuration-images-at owner fd record time) nil)
                                (error (condition)
                                  (not (null (search "invalid inert follow image time" (princ-to-string condition))))))))
                        (ok (handler-case
                                (progn (pg::physical-reseed-follow-configuration-images-at owner fd preparing 0) nil)
                              (error (condition)
                                (not (null (search "requires one proved generation" (princ-to-string condition)))))))
                        (ok (zerop checks))
                        (let ((images (pg::physical-reseed-follow-configuration-images-at owner fd record 0)))
                          (ok (not (null images))))
                        (ok (plusp checks))
                        (ok (zerop connections)))
                   (setf (symbol-function 'pg::physical-directory-check-owner-common) owner-check
                         (symbol-function 'pg::physical-libpq-connect-frozen) connect)))))))))))
