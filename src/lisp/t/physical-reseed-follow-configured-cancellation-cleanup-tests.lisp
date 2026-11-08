(in-package #:pg-physical-tests)

(cl-spec:defproperty reseed-configured-source-closed-candidate-freezes-history
    ((selected boolean) (handed-off boolean))
  (:about pg::physical-reseed-follow-cancellation-source-closed-candidate)
  (:trials (:normal 100))
  (let* ((pending (pg::physical-reseed-follow-cancellation-begin-record
                   (reseed-cancellation-codec-fixture 6 selected)))
         (receipt (reseed-cancellation-source-parent
                   pending :closed (if handed-off :handed-off :aborted) t))
         (before (pg::physical-bootstrap-manifest-text pending))
         (closed (pg::physical-reseed-follow-cancellation-source-closed-candidate pending receipt)))
    (and (equal closed (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
         (equal closed (pg::physical-reseed-follow-cancellation-source-closed-candidate closed receipt))
         (equal before (pg::physical-bootstrap-manifest-text pending))
         (equal (pg::physical-reseed-follow-cancellation-configured-history pending)
                (pg::physical-reseed-follow-cancellation-configured-history closed)))))

(deftest generated-reseed-configured-source-closed-candidate-contract
    (let ((result
            (cl-spec:result-data
             (cl-spec:run-property 'reseed-configured-source-closed-candidate-freezes-history
                                   :seed 209 :profile :normal))))
      (ok (eq :passed (getf result :status)))
      (ok (= 100 (getf result :trials)))))

(defun check-configured-cancellation-source-closed (kind outcome aborted)
  (call-with-reseed-inert-settlement-fixture
   kind
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (pg::call-with-physical-reseed-follow-cancellation-owner
      root
      (lambda (owner)
        (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
               (receipt (reseed-cancellation-source-parent pending :closed outcome t))
               (expected (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
               (path (merge-pathnames "physical-reseed.follow" root))
               (connect (symbol-function 'pg::physical-libpq-connect-frozen))
               (connections 0) (published 0) (escaped nil) (before nil))
          (when aborted (reseed-follow-prepare-test-abort root pending))
          (setf before (configured-cancellation-test-preserved-file-facts root))
          (unwind-protect
               (progn
                 (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                       (lambda (&rest arguments)
                         (declare (ignore arguments))
                         (incf connections)
                         (error "local source-closed publication contacted its source")))
                 (pg::call-with-physical-reseed-follow-cancellation-operation
                  owner
                  (lambda (operation)
                    (setf escaped operation)
                    (let* ((snapshot (pg::physical-reseed-follow-cancellation-operation-call
                                      operation :snapshot))
                           (pg::*physical-reseed-follow-test-hook*
                             (lambda (event actual-path)
                               (declare (ignore actual-path))
                               (when (eq event :after-follow-rename)
                                 (incf published)
                                 (let ((context (pg::physical-reseed-follow-installation-context owner)))
                                   (ok (equal expected
                                              (pg::physical-reseed-follow-installation-context-record context)))
                                   (ok (equal expected
                                              (getf (pg::physical-reseed-follow-installation-context-snapshot
                                                     context) :record))))
                                 (ok (signals
                                      (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)
                                      'error))))))
                      (multiple-value-bind (record after)
                          (pg::physical-reseed-follow-record-cancellation-source-closed-held
                           operation snapshot receipt)
                        (ok (equal expected record))
                        (ok (equal expected (getf (getf after :follow) :record)))
                        (ok (pg::physical-reseed-follow-owner-busy-p owner))
                        (ok (equal after
                                   (pg::physical-reseed-follow-cancellation-operation-call
                                    operation :check after)))
                        (ok (signals
                             (pg::physical-reseed-follow-cancellation-operation-call
                              operation :check snapshot) 'error))))))
                 (ok (= 1 published))
                 (ok (signals
                      (pg::physical-reseed-follow-cancellation-operation-call escaped :snapshot) 'error))
                 (let ((identity (pg::physical-reseed-follow-file-identity
                                  (sb-posix:lstat (namestring path)))))
                   (ok (equal expected
                              (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)))
                   (ok (equal expected (pg::physical-reseed-follow-recover-cancellation owner)))
                   (ok (equal identity (pg::physical-reseed-follow-file-identity
                                        (sb-posix:lstat (namestring path))))))
                 (ok (equal expected (pg::physical-reseed-follow-read-configured-cancellation owner)))
                 (ok (equalp (pg::physical-reseed-follow-image expected) (lineage-test-bytes path)))
                 (ng (pg::physical-adoption-path-present-p
                      (merge-pathnames "physical-reseed.follow.tmp" root)))
                 (ng (pg::physical-reseed-follow-owner-busy-p owner))
                 (ng (pg::physical-reseed-follow-owner-uncertain-p owner))
                 (ok (zerop connections))
                 (ok (equalp before (configured-cancellation-test-preserved-file-facts root))))
            (setf (symbol-function 'pg::physical-libpq-connect-frozen) connect))))))))

(deftest reseed-configured-source-closed-active-handoff
    (check-configured-cancellation-source-closed :same-timeline :handed-off nil))

(deftest reseed-configured-source-closed-aborted-attempt
    (check-configured-cancellation-source-closed :same-timeline :aborted t))

(deftest reseed-configured-source-closed-retained-timeline
    (check-configured-cancellation-source-closed :retained-timeline :handed-off nil))

(deftest reseed-configured-source-closed-before-fork
    (check-configured-cancellation-source-closed :before-fork :handed-off nil))

(deftest reseed-configured-source-closed-at-fork
    (check-configured-cancellation-source-closed :at-fork :handed-off nil))

(deftest reseed-configured-source-closed-later-writer
    (check-configured-cancellation-source-closed :later-writer :handed-off nil))

(defun check-configured-source-closed-observer (outcome)
  (call-with-reseed-inert-settlement-fixture
   :same-timeline
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (pg::call-with-physical-reseed-follow-cancellation-owner
      root
      (lambda (owner)
        (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
               (receipt (reseed-cancellation-source-parent pending :closed outcome t))
               (expected (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
               (path (merge-pathnames "physical-reseed.follow" root))
               (before (configured-cancellation-test-preserved-file-facts root)))
          (call-with-reseed-cancellation-source
           owner pending receipt
           (lambda (state)
             (when (eq outcome :aborted)
               (setf (getf state :observation)
                     (reseed-cancellation-source-parent pending :ready nil t)))
             (setf (getf state :allowed-commands)
                   (if (eq outcome :aborted) '(:status :abort) '(:status :cleanup)))
             (ok (signals
                  (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper") 'error))
             (ng (getf state :events))
             (pg::call-with-physical-reseed-follow-cancellation-operation
              owner
              (lambda (operation)
                (let* ((snapshot (pg::physical-reseed-follow-cancellation-operation-call operation :snapshot))
                       (actual (pg::physical-reseed-follow-observe-cancellation-source-held
                                operation snapshot :helper "/unused/helper")))
                  (ok (equal receipt actual))
                  (ok (= 1 (count :status (getf state :events))))
                  (ok (= 1 (count (if (eq outcome :aborted) :abort :cleanup)
                                  (getf state :events))))
                  (ok (= 1 (count :close (getf state :events))))
                  (ng (member :release (getf state :events)))
                  (ok (equalp (pg::physical-reseed-follow-image pending) (lineage-test-bytes path)))
                  (ok (equal expected
                             (pg::physical-reseed-follow-record-cancellation-source-closed-held
                              operation snapshot actual))))))
             (let ((events (copy-list (getf state :events))))
               (ok (signals
                    (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper") 'error))
               (ok (equal events (getf state :events))))
             (ok (equal expected (pg::physical-reseed-follow-read-configured-cancellation owner)))
             (ok (equalp (pg::physical-reseed-follow-image expected) (lineage-test-bytes path)))
             (ng (pg::physical-reseed-follow-owner-busy-p owner))
             (ng (pg::physical-reseed-follow-owner-uncertain-p owner))
             (ok (equalp before (configured-cancellation-test-preserved-file-facts root)))))))))))

(deftest reseed-configured-source-closed-observer-records-handoff-without-release
    (check-configured-source-closed-observer :handed-off))

(deftest reseed-configured-source-closed-observer-records-abort-with-generation-debt
    (check-configured-source-closed-observer :aborted))

(deftest reseed-configured-source-closed-authority-binds-context-and-revokes
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace) (declare (ignore owner root workspace)))
     (lambda (root workspace)
       (declare (ignore workspace))
       (pg::call-with-physical-reseed-follow-cancellation-owner
        root
        (lambda (owner)
          (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                 (receipt (reseed-cancellation-source-parent pending :closed :handed-off t))
                 (candidate (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
                 (bytes (pg::physical-reseed-follow-image candidate))
                 (before (configured-cancellation-test-preserved-file-facts root))
                 (saved nil) (saved-root nil) (previous nil))
            (pg::call-with-physical-reseed-follow-cancellation-operation
             owner
             (lambda (operation)
               (declare (ignore operation))
               (pg::call-with-physical-reseed-follow-image
                owner
                (lambda (fd check)
                  (setf previous (pg::physical-reseed-follow-snapshot-at fd) saved-root fd)
                  (let* ((foreign (pg::physical-reseed-follow-cancellation-begin-record
                                   (reseed-cancellation-codec-fixture 6 nil)))
                         (closed (pg::physical-reseed-follow-cancellation-source-closed-record
                                  foreign (reseed-cancellation-source-parent foreign :closed :aborted t)))
                         (stale (pg::physical-adoption-attempt-value-copy previous)))
                    (ok (equal "configured cancellation owner changed its frozen history"
                               (handler-case
                                   (pg::physical-reseed-follow-cancellation-cleanup-candidate owner closed previous)
                                 (error (condition) (princ-to-string condition)))))
                    (setf (getf stale :record) candidate)
                    (ok (equal "cleanup publication requires the exact source-closed successor"
                               (handler-case
                                   (pg::physical-reseed-follow-cancellation-cleanup-candidate owner candidate stale)
                                 (error (condition) (princ-to-string condition))))))
                  (pg::call-with-physical-reseed-follow-cancellation-cleanup-authority
                   owner fd candidate previous bytes :write check
                   (lambda (verify)
                     (setf saved pg::*physical-reseed-follow-cancellation-authority*)
                     (ok (pg::physical-reseed-follow-cancellation-cleanup-authority-p saved))
                     (let ((getter (symbol-function 'pg::physical-reseed-follow-cancellation-publication-context)))
                       (unwind-protect
                            (progn
                              (setf (symbol-function 'pg::physical-reseed-follow-cancellation-publication-context)
                                    (lambda (actual) (declare (ignore actual)) :foreign-context))
                              (ok (signals (funcall verify) 'error)))
                         (setf (symbol-function 'pg::physical-reseed-follow-cancellation-publication-context)
                               getter)))
                     (funcall verify)
                     (ok (signals
                          (pg::physical-reseed-follow-cancellation-claim
                           (pg::make-physical-reseed-follow-owner :busy-p t)
                           fd candidate previous :write :bytes bytes) 'error))
                     (ok (signals
                          (pg::physical-reseed-follow-cancellation-claim
                           owner -1 candidate previous :write :bytes bytes) 'error))
                     (ok (eq saved (pg::physical-reseed-follow-cancellation-claim
                                    owner fd candidate previous :write :bytes bytes)))
                     (ok (signals
                          (pg::physical-reseed-follow-cancellation-claim
                           owner fd candidate previous :write :bytes bytes) 'error))))))))
            (ng (pg::physical-reseed-follow-cancellation-authority-active-p saved))
            (let ((pg::*physical-reseed-follow-cancellation-authority* saved))
              (ok (signals
                   (pg::physical-reseed-follow-cancellation-claim
                    owner saved-root candidate previous :write :bytes bytes) 'error)))
            (ok (equalp (pg::physical-reseed-follow-image pending)
                        (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
            (ng (pg::physical-adoption-path-present-p
                 (merge-pathnames "physical-reseed.follow.tmp" root)))
            (ng (pg::physical-reseed-follow-owner-busy-p owner))
            (ng (pg::physical-reseed-follow-owner-uncertain-p owner))
            (ok (equalp before (configured-cancellation-test-preserved-file-facts root)))))))))

(defun check-configured-source-closed-cut (cut escape)
  (call-with-reseed-inert-settlement-fixture
   :same-timeline
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (let ((pending nil) (expected nil) (receipt nil) (escaped nil) (before nil)
                         (tag (gensym "SOURCE-CLOSED-CUT")) (fired 0)
                         (final (merge-pathnames "physical-reseed.follow" root))
                         (temporary (merge-pathnames "physical-reseed.follow.tmp" root)))
       (ok (eq :interrupted
               (catch tag
                 (handler-case
                     (pg::call-with-physical-reseed-follow-cancellation-owner
                      root
                      (lambda (owner)
                        (setf escaped owner
                              pending (pg::physical-reseed-follow-begin-cancellation owner)
                              receipt (reseed-cancellation-source-parent pending :closed :aborted t)
                              expected (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
                        (reseed-follow-prepare-test-abort root pending)
                        (setf before (configured-cancellation-test-preserved-file-facts root))
                        (let ((pg::*physical-reseed-follow-write-limit* 257)
                              (pg::*physical-reseed-follow-test-hook*
                                (lambda (event path)
                                  (declare (ignore path))
                                  (when (and (eq event cut) (zerop fired))
                                    (incf fired)
                                    (ecase escape
                                      (:error (error "configured source-closed cut"))
                                      (:throw (throw tag :interrupted))
                                      (:interrupt
                                       (sb-thread:interrupt-thread
                                        sb-thread:*current-thread*
                                        (lambda () (throw tag :interrupted)))))))))
                          (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt))))
                   (error (condition)
                     (if (equal "configured source-closed cut" (princ-to-string condition))
                         :interrupted
                         (error condition)))))))
       (ok (= 1 fired))
       (ok (pg::physical-reseed-follow-owner-uncertain-p escaped))
       (ng (pg::physical-reseed-follow-owner-busy-p escaped))
       (ng (pg::physical-adoption-owner-active-p escaped))
       (let* ((partial (eq cut :after-follow-write-chunk))
              (renamed (member cut '(:after-follow-rename :after-follow-readback)))
              (temporary-bytes (when (pg::physical-adoption-path-present-p temporary)
                                 (lineage-test-bytes temporary)))
              (identity (when temporary-bytes
                          (pg::physical-reseed-follow-file-identity (sb-posix:lstat temporary) t)))
              (final-identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat final))))
         (ok (equalp (pg::physical-reseed-follow-image (if renamed expected pending))
                     (lineage-test-bytes final)))
         (when temporary-bytes
           (let ((entered nil))
             (ok (signals
                  (pg::call-with-physical-reseed-follow-cancellation-owner
                   root (lambda (owner) (declare (ignore owner)) (setf entered t))) 'error))
             (ng entered))
           (ok (equalp temporary-bytes (lineage-test-bytes temporary)))
           (ok (equal identity
                      (pg::physical-reseed-follow-file-identity (sb-posix:lstat temporary) t))))
         (cond
           (partial
            (ok (< 0 (length temporary-bytes) (length (pg::physical-reseed-follow-image expected))))
            (ok (<= (length temporary-bytes) 257))
            (ok (equalp temporary-bytes
                        (subseq (pg::physical-reseed-follow-image expected) 0 (length temporary-bytes))))
            (let ((entered nil))
              (ok (signals
                   (pg::call-with-physical-reseed-follow-cancellation-owner
                    root (lambda (owner) (declare (ignore owner)) (setf entered t)) :recover t) 'error))
              (ng entered))
            (ok (equalp temporary-bytes (lineage-test-bytes temporary)))
            (ok (equal identity
                       (pg::physical-reseed-follow-file-identity (sb-posix:lstat temporary) t))))
           (t
            (if renamed
                (ng (pg::physical-adoption-path-present-p temporary))
                (ok (equalp (pg::physical-reseed-follow-image expected) temporary-bytes)))
            (pg::call-with-physical-reseed-follow-cancellation-owner
             root
             (lambda (owner)
               (ok (equal expected (pg::physical-reseed-follow-read-configured-cancellation owner)))
               (ok (equal expected (pg::physical-reseed-follow-recover-cancellation owner)))
               (ng (pg::physical-reseed-follow-owner-uncertain-p owner))
               (ng (pg::physical-reseed-follow-owner-busy-p owner)))
             :recover t)
            (ok (equalp (pg::physical-reseed-follow-image expected) (lineage-test-bytes final)))
            (ng (pg::physical-adoption-path-present-p temporary))
            (when renamed
              (ok (equal (subseq final-identity 0 7)
                         (subseq (pg::physical-reseed-follow-file-identity
                                  (sb-posix:lstat final)) 0 7))))
            (when identity
              (ok (equal (subseq identity 0 7)
                         (subseq (pg::physical-reseed-follow-file-identity
                                  (sb-posix:lstat final)) 0 7)))))))
       (ok (equalp before (configured-cancellation-test-preserved-file-facts root)))))))

(deftest reseed-configured-source-closed-partial-write-cut
    (check-configured-source-closed-cut :after-follow-write-chunk :error))

(deftest reseed-configured-source-closed-temporary-error-cut
    (check-configured-source-closed-cut :before-follow-rename :error))

(deftest reseed-configured-source-closed-temporary-throw-cut
    (check-configured-source-closed-cut :before-follow-rename :throw))

(deftest reseed-configured-source-closed-renamed-error-cut
    (check-configured-source-closed-cut :after-follow-rename :error))

(deftest reseed-configured-source-closed-readback-throw-cut
    (check-configured-source-closed-cut :after-follow-readback :throw))

(deftest reseed-configured-source-closed-temporary-interrupt-cut
    (check-configured-source-closed-cut :before-follow-rename :interrupt))
