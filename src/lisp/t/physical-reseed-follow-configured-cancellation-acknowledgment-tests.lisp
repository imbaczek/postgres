(in-package #:pg-physical-tests)

(defun reseed-configured-cleanup-policy-fixture (selected multiple)
  (let ((record (pg::physical-reseed-follow-copy (reseed-cancellation-codec-fixture 6 selected))))
    (when multiple
      (let* ((old (first (getf record :generations)))
             (next (pg::physical-adoption-attempt-value-copy old)))
        (setf (getf old :phase) :canceled
              (getf next :generation) 2
              (getf next :name) "target_follow_2"
              (getf record :generations) (list old next)
              (getf (getf record :configuration) :generation) 2)))
    (pg::physical-reseed-follow-copy record)))

(cl-spec:defproperty reseed-configured-cleanup-plan-preserves-generation-debt
    ((selected boolean) (aborted boolean) (multiple boolean))
  (:about pg::physical-reseed-follow-cancellation-ack-plan)
  (:trials (:normal 100))
  (let* ((pending (pg::physical-reseed-follow-cancellation-begin-record
                   (reseed-configured-cleanup-policy-fixture selected multiple)))
         (receipt (reseed-cancellation-source-parent
                   pending :closed (if aborted :aborted :handed-off) t))
         (closed (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
         (before (pg::physical-bootstrap-manifest-text closed))
         (plan (pg::physical-reseed-follow-cancellation-ack-plan closed receipt)))
    (and (equal before (pg::physical-bootstrap-manifest-text closed))
         (if (or aborted multiple)
             (and (null plan)
                  (eq :defer (pg::physical-reseed-follow-cancellation-source-action closed nil))
                  (handler-case
                      (progn (pg::physical-reseed-follow-cancellation-cleaned-candidate closed) nil)
                    (error () t)))
             (let ((cleaned (pg::physical-reseed-follow-cancellation-cleaned-candidate closed plan)))
               (and (eq :release (getf plan :operation))
                    (eq :selected-release (getf (getf cleaned :cancellation) :ack))
                    (equal receipt (getf (getf cleaned :cancellation) :receipt))
                    (equal (pg::physical-reseed-follow-cancellation-configured-history closed)
                           (pg::physical-reseed-follow-cancellation-configured-history cleaned))))))))

(deftest generated-reseed-configured-cleanup-plan-contract
    (let ((result
            (cl-spec:result-data
             (cl-spec:run-property 'reseed-configured-cleanup-plan-preserves-generation-debt
                                   :seed 211 :profile :normal))))
      (ok (eq :passed (getf result :status)))
      (ok (= 100 (getf result :trials)))))

(defun check-configured-cancellation-cleaned (kind &optional aborted)
  (call-with-reseed-inert-settlement-fixture
   kind
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (pg::call-with-physical-reseed-follow-cancellation-owner
      root
      (lambda (owner)
        (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
               (receipt (reseed-cancellation-source-parent pending :closed :handed-off t))
               (closed (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
               (expected (pg::physical-reseed-follow-cancellation-cleaned-record closed receipt :selected-release))
               (path (merge-pathnames "physical-reseed.follow" root))
               (before nil) (publications nil))
          (when aborted (reseed-follow-prepare-test-abort root pending))
          (setf before (configured-cancellation-test-preserved-file-facts root))
          (call-with-reseed-cancellation-source
           owner pending receipt
           (lambda (state)
             (setf (getf state :on-event)
                   (lambda (event)
                     (when (eq event :release)
                       (ok (equalp (pg::physical-reseed-follow-image closed) (lineage-test-bytes path))))))
             (let ((pg::*physical-reseed-follow-test-hook*
                     (lambda (event actual-path)
                       (declare (ignore actual-path))
                       (when (eq event :after-follow-rename)
                         (let* ((authority pg::*physical-reseed-follow-cancellation-authority*)
                                (candidate (pg::physical-reseed-follow-cancellation-authority-candidate authority))
                                (context (pg::physical-reseed-follow-installation-context owner)))
                           (push (getf (getf candidate :cancellation) :phase) publications)
                           (ok (equal candidate (pg::physical-reseed-follow-installation-context-record context)))
                           (ok (equal candidate
                                      (getf (pg::physical-reseed-follow-installation-context-snapshot context)
                                            :record))))))))
               (multiple-value-bind (actual result)
                   (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper")
                 (ok (eq :complete result))
                 (ok (equal expected actual))))
             (ok (equal '(:cleaned :source-closed) publications))
             (ok (= 1 (count :status (getf state :events))))
             (ok (= 2 (count :cleanup (getf state :events))))
             (ok (= 1 (count :release (getf state :events))))
             (ok (= 2 (count :close (getf state :events))))
             (let ((events (copy-list (getf state :events)))
                   (identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat path))))
               (multiple-value-bind (actual result)
                   (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper")
                 (ok (eq :complete result))
                 (ok (equal expected actual)))
               (multiple-value-bind (actual result)
                   (pg::physical-reseed-follow-recover-cleaned-cancellation owner :helper "/unused/helper")
                 (ok (eq :complete result))
                 (ok (equal expected actual)))
               (ok (equal events (getf state :events)))
               (ok (equal identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat path)))))
             (ok (equal expected (pg::physical-reseed-follow-read-configured-cancellation owner)))
             (ok (equalp (pg::physical-reseed-follow-image expected) (lineage-test-bytes path)))
             (ng (pg::physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow.tmp" root)))
             (ng (pg::physical-reseed-follow-owner-busy-p owner))
             (ng (pg::physical-reseed-follow-owner-uncertain-p owner))
             (ok (equalp before (configured-cancellation-test-preserved-file-facts root)))))))))))

(deftest reseed-configured-cleaned-completes-selected-release
    (check-configured-cancellation-cleaned :same-timeline))

(deftest reseed-configured-cleaned-completes-after-local-abort
    (check-configured-cancellation-cleaned :same-timeline t))

(deftest reseed-configured-cleaned-completes-retained-timeline
    (check-configured-cancellation-cleaned :retained-timeline))

(deftest reseed-configured-cleaned-completes-before-fork
    (check-configured-cancellation-cleaned :before-fork))

(deftest reseed-configured-cleaned-completes-at-fork
    (check-configured-cancellation-cleaned :at-fork))

(deftest reseed-configured-cleaned-completes-later-writer
    (check-configured-cancellation-cleaned :later-writer))

(defun call-with-configured-cleaned-recovery-source (record receipt observe function)
  (let ((recover (symbol-function 'pg::physical-reseed-follow-recover-cleaned-cancellation)))
    (unwind-protect
         (progn
           (setf (symbol-function 'pg::physical-reseed-follow-recover-cleaned-cancellation)
                 (lambda (owner &key helper)
                   (call-with-reseed-cancellation-source
                    owner record receipt
                    (lambda (state)
                      (funcall observe owner state)
                      (funcall recover owner :helper helper)))))
           (funcall function))
      (setf (symbol-function 'pg::physical-reseed-follow-recover-cleaned-cancellation) recover))))

(defun check-configured-cleaned-recovery-cut (cut escape)
  (call-with-reseed-inert-settlement-fixture
   :same-timeline
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (let ((closed nil) (expected nil) (receipt nil) (before nil) (escaped nil)
                        (injected 0) (recovery-state nil) (admitted nil)
                        (final (merge-pathnames "physical-reseed.follow" root))
                        (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                        (tag (gensym "CONFIGURED-ACK-CUT")))
       (ok (eq :interrupted
               (catch tag
                 (handler-case
                     (pg::call-with-physical-reseed-follow-cancellation-owner
                      root
                      (lambda (owner)
                        (setf escaped owner)
                        (let ((pending (pg::physical-reseed-follow-begin-cancellation owner)))
                          (setf receipt (reseed-cancellation-source-parent pending :closed :handed-off t)
                                closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)
                                expected (pg::physical-reseed-follow-cancellation-cleaned-record
                                          closed receipt :selected-release)))
                        (reseed-follow-prepare-test-abort root closed)
                        (setf before (configured-cancellation-test-preserved-file-facts root))
                        (call-with-reseed-cancellation-source
                         owner closed receipt
                         (lambda (state)
                           (setf (getf state :allowed-commands) '(:cleanup :release))
                           (let ((pg::*physical-reseed-follow-write-limit* 257)
                                 (pg::*physical-reseed-follow-test-hook*
                                   (lambda (event path)
                                     (declare (ignore path))
                                     (when (and (eq event cut) (zerop injected)
                                                (pg::physical-reseed-follow-cancellation-ack-authority-p
                                                 pg::*physical-reseed-follow-cancellation-authority*))
                                       (incf injected)
                                       (ecase escape
                                         (:error (error "configured CLEANED publication cut"))
                                         (:throw (throw tag :interrupted))
                                         (:interrupt
                                          (sb-thread:interrupt-thread
                                           sb-thread:*current-thread*
                                           (lambda () (throw tag :interrupted)))))))))
                             (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper"))))))
                   (error (condition)
                     (if (equal "configured CLEANED publication cut" (princ-to-string condition))
                         :interrupted
                         (error condition)))))))
       (ok (= 1 injected))
       (ok (pg::physical-reseed-follow-owner-uncertain-p escaped))
       (ng (pg::physical-reseed-follow-owner-busy-p escaped))
       (ng (pg::physical-adoption-owner-active-p escaped))
       (let* ((renamed (member cut '(:after-follow-rename :after-follow-readback)))
              (partial (eq cut :after-follow-write-chunk))
              (image (when (pg::physical-adoption-path-present-p temporary) (lineage-test-bytes temporary)))
              (identity (pg::physical-reseed-follow-file-identity
                         (sb-posix:lstat (if renamed final temporary)) t)))
         (ok (equalp (pg::physical-reseed-follow-image (if renamed expected closed))
                     (lineage-test-bytes final)))
         (if renamed
             (ng (pg::physical-adoption-path-present-p temporary))
             (progn
               (if partial
                   (progn
                     (ok (< 0 (length image) (length (pg::physical-reseed-follow-image expected))))
                     (ok (<= (length image) 257))
                     (ok (equalp image (subseq (pg::physical-reseed-follow-image expected) 0 (length image)))))
                   (ok (equalp image (pg::physical-reseed-follow-image expected))))
               (dolist (mode '(nil t))
                 (let ((entered nil))
                   (ok (signals
                        (pg::call-with-physical-reseed-follow-cancellation-owner
                         root (lambda (owner) (declare (ignore owner)) (setf entered t)) :recover mode) 'error))
                   (ng entered)))
               (ok (equalp image (lineage-test-bytes temporary)))))
         (call-with-configured-cleaned-recovery-source
          closed receipt
          (lambda (owner state)
            (declare (ignore owner))
            (setf recovery-state state
                  (getf state :allowed-commands) (if partial nil '(:cleanup :release))))
          (lambda ()
            (flet ((open-owner ()
                     (pg::call-with-physical-reseed-follow-cancellation-owner
                      root
                      (lambda (owner)
                        (setf admitted t)
                        (ok (equal expected (pg::physical-reseed-follow-read-configured-cancellation owner)))
                        (ng (pg::physical-reseed-follow-owner-busy-p owner))
                        (ng (pg::physical-reseed-follow-owner-uncertain-p owner)))
                      :recover :cleaned :helper "/unused/helper")))
              (if partial (ok (signals (open-owner) 'error)) (open-owner)))))
         (if partial
             (progn
               (ng admitted)
               (ng (getf recovery-state :events))
               (ok (equalp image (lineage-test-bytes temporary)))
               (ok (equal identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat temporary) t))))
             (progn
               (ok admitted)
               (ok (= (if renamed 0 1) (count :release (getf recovery-state :events))))
               (ok (= (if renamed 0 1) (count :cleanup (getf recovery-state :events))))
               (ok (= (if renamed 0 1) (count :close (getf recovery-state :events))))
               (ng (member :status (getf recovery-state :events)))
               (ok (equalp (pg::physical-reseed-follow-image expected) (lineage-test-bytes final)))
               (ng (pg::physical-adoption-path-present-p temporary))
               (ok (equal (subseq identity 0 7)
                          (subseq (pg::physical-reseed-follow-file-identity (sb-posix:lstat final)) 0 7))))))
       (ok (equalp before (configured-cancellation-test-preserved-file-facts root)))))))

(deftest reseed-configured-cleaned-recovery-preserves-partial-temporary
    (check-configured-cleaned-recovery-cut :after-follow-write-chunk :error))

(deftest reseed-configured-cleaned-recovery-promotes-exact-temporary
    (check-configured-cleaned-recovery-cut :before-follow-rename :error))

(deftest reseed-configured-cleaned-recovery-survives-temporary-throw
    (check-configured-cleaned-recovery-cut :before-follow-rename :throw))

(deftest reseed-configured-cleaned-recovery-reopens-renamed-final
    (check-configured-cleaned-recovery-cut :after-follow-rename :error))

(deftest reseed-configured-cleaned-recovery-reopens-after-readback-interrupt
    (check-configured-cleaned-recovery-cut :after-follow-readback :interrupt))

(deftest reseed-configured-cleaned-requires-issued-evidence-and-mapped-context
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
                 (closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt))
                 (expected (pg::physical-reseed-follow-cancellation-cleaned-record closed receipt :selected-release))
                 (before (configured-cancellation-test-preserved-file-facts root))
                 (authority nil) (evidence nil) (checks 0))
            (call-with-reseed-cancellation-source
             owner closed receipt
             (lambda (state)
               (setf (getf state :allowed-commands) '(:cleanup :release))
               (pg::call-with-physical-reseed-follow-cancellation-operation
                owner
                (lambda (operation)
                  (let ((snapshot (pg::physical-reseed-follow-cancellation-operation-call operation :snapshot)))
                    (ok (signals
                         (pg::physical-reseed-follow-record-cancellation-cleaned-held
                          operation snapshot
                          (pg::%make-physical-reseed-follow-cleanup-operation-evidence :claim #'values)) 'error))
                    (ng (getf state :events))
                    (pg::call-with-physical-reseed-follow-cleanup-operation-evidence
                     operation snapshot
                     (lambda (issued)
                       (setf evidence issued)
                       (let ((pg::*physical-reseed-follow-test-hook*
                               (lambda (event path)
                                 (declare (ignore path))
                                 (when (eq event :before-follow-temporary-fsync)
                                   (incf checks)
                                   (setf authority pg::*physical-reseed-follow-cancellation-authority*)
                                   (ok (pg::physical-reseed-follow-cancellation-ack-authority-p authority))
                                   (let ((getter (symbol-function 'pg::physical-reseed-follow-cancellation-publication-context)))
                                     (unwind-protect
                                          (progn
                                            (setf (symbol-function 'pg::physical-reseed-follow-cancellation-publication-context)
                                                  (lambda (actual) (declare (ignore actual)) :foreign-context))
                                            (ok (equal "cleanup acknowledgment authority changed its publication scope"
                                                       (handler-case
                                                           (funcall (pg::physical-reseed-follow-cancellation-authority-check
                                                                     authority))
                                                         (error (condition) (princ-to-string condition))))))
                                       (setf (symbol-function 'pg::physical-reseed-follow-cancellation-publication-context)
                                             getter)))
                                   (funcall (pg::physical-reseed-follow-cancellation-authority-check authority))))))
                         (ok (equal expected
                                    (pg::physical-reseed-follow-record-cancellation-cleaned-held
                                     operation snapshot issued))))
                       (ok (signals
                            (pg::physical-reseed-follow-claim-cleanup-operation-evidence
                             issued operation snapshot) 'error)))
                     :helper "/unused/helper")
                    (ok (signals
                         (pg::physical-reseed-follow-claim-cleanup-operation-evidence
                          evidence operation snapshot) 'error)))))))
            (ok (= 1 checks))
            (ng (pg::physical-reseed-follow-cancellation-authority-active-p authority))
            (ok (equal expected (pg::physical-reseed-follow-read-configured-cancellation owner)))
            (ng (pg::physical-reseed-follow-owner-busy-p owner))
            (ng (pg::physical-reseed-follow-owner-uncertain-p owner))
            (ok (equalp before (configured-cancellation-test-preserved-file-facts root)))))))))

(defun check-configured-cleaned-deferred-source (kind)
  (call-with-reseed-inert-settlement-fixture
   :same-timeline
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (pg::call-with-physical-reseed-follow-cancellation-owner
      root
      (lambda (owner)
        (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
               (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
               (expected (if (eq kind :aborted)
                             (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt)
                             pending))
               (before (configured-cancellation-test-preserved-file-facts root)))
          (call-with-reseed-cancellation-source
           owner pending receipt
           (lambda (state)
             (ecase kind
               (:unknown (setf (getf state :observation) nil))
               (:unbound
                (setf (getf state :observation) (reseed-cancellation-source-parent pending :requested nil nil)))
               (:aborted nil)
               (:closing
                (setf (getf state :observation) (reseed-cancellation-source-parent pending :ready nil t)
                      (getf state :abort-result) (reseed-cancellation-source-parent pending :closing :aborted t))))
             (setf (getf state :allowed-commands) (if (eq kind :closing) '(:status :abort) '(:status)))
             (multiple-value-bind (actual outcome)
                 (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper")
               (ok (eq (if (eq kind :aborted) :deferred :unresolved) outcome))
               (ok (equal expected actual)))
             (ok (= 1 (count :status (getf state :events))))
             (ok (= (if (eq kind :closing) 1 0) (count :abort (getf state :events))))
             (ng (member :cleanup (getf state :events)))
             (ng (member :release (getf state :events)))
             (ok (= 1 (count :close (getf state :events))))
             (ok (equal expected (pg::physical-reseed-follow-read-configured-cancellation owner)))
             (ok (equalp (pg::physical-reseed-follow-image expected)
                         (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
             (ng (pg::physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow.tmp" root)))
             (ng (pg::physical-reseed-follow-owner-busy-p owner))
             (ng (pg::physical-reseed-follow-owner-uncertain-p owner))
             (ok (equalp before (configured-cancellation-test-preserved-file-facts root)))))))))))

(deftest reseed-configured-cleaned-defers-aborted-generation-debt
    (check-configured-cleaned-deferred-source :aborted))

(deftest reseed-configured-cleaned-preserves-unknown-source
    (check-configured-cleaned-deferred-source :unknown))

(deftest reseed-configured-cleaned-preserves-unbound-source
    (check-configured-cleaned-deferred-source :unbound))

(deftest reseed-configured-cleaned-preserves-closing-source
    (check-configured-cleaned-deferred-source :closing))

(defun check-configured-cleaned-source-failure (failure)
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
               (closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt))
               (expected (pg::physical-reseed-follow-cancellation-cleaned-record closed receipt :selected-release))
               (before (configured-cancellation-test-preserved-file-facts root)))
          (call-with-reseed-cancellation-source
           owner closed receipt
           (lambda (state)
             (setf (getf state :allowed-commands) '(:cleanup :release))
             (ecase failure
               (:cleanup
                (setf (getf state :cleanup-result)
                      (reseed-cancellation-source-parent closed :closing :handed-off t)))
               (:release (setf (getf state :release-result) nil))
               (:close
                (setf (getf state :on-event)
                      (lambda (event)
                        (when (eq event :close) (error "configured cleanup client close failed"))))))
             (ok (equal
                  (ecase failure
                    (:cleanup "cleanup operation evidence requires a bound closed receipt")
                    (:release "selected cleanup did not acknowledge digest release")
                    (:close "configured cleanup client close failed"))
                  (handler-case
                      (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper")
                    (error (condition) (princ-to-string condition)))))
             (ok (= 1 (count :cleanup (getf state :events))))
             (ok (= (if (eq failure :cleanup) 0 1) (count :release (getf state :events))))
             (ok (= 1 (count :close (getf state :events))))
             (ok (equal closed (pg::physical-reseed-follow-read-configured-cancellation owner)))
             (ok (equalp (pg::physical-reseed-follow-image closed)
                         (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
             (ng (pg::physical-adoption-path-present-p (merge-pathnames "physical-reseed.follow.tmp" root)))
             (ng (pg::physical-reseed-follow-owner-busy-p owner))
             (ng (pg::physical-reseed-follow-owner-uncertain-p owner))
             (setf (getf state :cleanup-result) receipt
                   (getf state :release-result) t (getf state :on-event) nil)
             (multiple-value-bind (record result)
                 (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper")
               (ok (eq :complete result))
               (ok (equal expected record)))
             (ok (equalp before (configured-cancellation-test-preserved-file-facts root)))))))))))

(deftest reseed-configured-cleaned-refuses-unclosed-cleanup-result
    (check-configured-cleaned-source-failure :cleanup))

(deftest reseed-configured-cleaned-refuses-unacknowledged-release
    (check-configured-cleaned-source-failure :release))

(deftest reseed-configured-cleaned-refuses-failed-source-close
    (check-configured-cleaned-source-failure :close))

(deftest reseed-configured-cleaned-recovery-refuses-invalid-temporaries-before-source
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace) (declare (ignore owner root workspace)))
     (lambda (root workspace)
       (declare (ignore workspace))
       (let ((pending nil) (receipt nil) (closed nil) (cleaned nil))
         (pg::call-with-physical-reseed-follow-cancellation-owner
          root
          (lambda (owner)
            (setf pending (pg::physical-reseed-follow-begin-cancellation owner)
                  receipt (reseed-cancellation-source-parent pending :closed :handed-off t)
                  closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)
                  cleaned (pg::physical-reseed-follow-cancellation-cleaned-record closed receipt :selected-release))))
         (let* ((final (merge-pathnames "physical-reseed.follow" root))
                (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                (before (configured-cancellation-test-preserved-file-facts root))
                (foreign (pg::physical-reseed-follow-cancellation-begin-record
                          (reseed-cancellation-codec-fixture 6 nil)))
                (foreign-receipt (reseed-cancellation-source-parent foreign :closed :handed-off t))
                (foreign-closed (pg::physical-reseed-follow-cancellation-source-closed-record foreign foreign-receipt))
                (foreign-cleaned (pg::physical-reseed-follow-cancellation-cleaned-record
                                  foreign-closed foreign-receipt :selected-release)))
           (dolist (kind '(:absent :empty :partial :wrong-phase :noncanonical :foreign :pending-handoff))
             (let* ((record (if (eq kind :pending-handoff) pending closed))
                    (bytes
                      (ecase kind
                        (:absent nil)
                        (:empty (make-array 0 :element-type '(unsigned-byte 8)))
                        (:partial (subseq (pg::physical-reseed-follow-image cleaned) 0 17))
                        (:wrong-phase (pg::physical-reseed-follow-image closed))
                        (:foreign (pg::physical-reseed-follow-image foreign-cleaned))
                        (:pending-handoff (pg::physical-reseed-follow-image cleaned))
                        (:noncanonical
                         (let* ((text (concatenate 'string (pg::physical-bootstrap-manifest-text cleaned) " "))
                                (crc (pg::wal-crc32 (pg::string->bytes text))))
                           (pg::string->bytes (format nil "PGRESEEDFOLLOW1 ~8,'0X~%~a~%" crc text))))))
                    (state nil) (entered nil) (identity nil))
               (when (eq kind :pending-handoff)
                 (with-open-file (stream final :direction :output :if-exists :supersede
                                               :if-does-not-exist :error :element-type '(unsigned-byte 8))
                   (write-sequence (pg::physical-reseed-follow-image record) stream)))
               (when bytes
                 (reseed-follow-owner-test-write temporary bytes)
                 (setf identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat temporary) t)))
               (call-with-configured-cleaned-recovery-source
                record receipt
                (lambda (owner actual)
                  (declare (ignore owner))
                  (setf state actual (getf actual :allowed-commands) nil))
                (lambda ()
                  (ok (signals
                       (pg::call-with-physical-reseed-follow-cancellation-owner
                        root (lambda (owner) (declare (ignore owner)) (setf entered t))
                        :recover :cleaned :helper "/unused/helper") 'error))))
               (ng entered)
               (ng (getf state :events))
               (ok (equalp (pg::physical-reseed-follow-image record) (lineage-test-bytes final)))
               (if bytes
                   (progn
                     (ok (equalp bytes (lineage-test-bytes temporary)))
                     (ok (equal identity
                                (pg::physical-reseed-follow-file-identity (sb-posix:lstat temporary) t))))
                   (ng (pg::physical-adoption-path-present-p temporary)))
               (ok (equalp before (configured-cancellation-test-preserved-file-facts root)))
               (when bytes (delete-file temporary)))))))))

(deftest reseed-configured-cleaned-recovery-mode-rejects-unknown-selection
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace) (declare (ignore owner root workspace)))
     (lambda (root workspace)
       (declare (ignore workspace))
       (let ((entered nil)
             (before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (signals
              (pg::call-with-physical-reseed-follow-cancellation-owner
               root (lambda (owner) (declare (ignore owner)) (setf entered t)) :recover :unknown) 'error))
         (ng entered)
         (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))

(deftest reseed-configured-cleaned-recovery-fences-replaced-temporary-during-source
    (call-with-reseed-inert-settlement-fixture
     :same-timeline
     (lambda (owner root workspace) (declare (ignore owner root workspace)))
     (lambda (root workspace)
       (declare (ignore workspace))
       (let ((closed nil) (receipt nil) (cleaned nil))
         (pg::call-with-physical-reseed-follow-cancellation-owner
          root
          (lambda (owner)
            (let ((pending (pg::physical-reseed-follow-begin-cancellation owner)))
              (setf receipt (reseed-cancellation-source-parent pending :closed :handed-off t)
                    closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)
                    cleaned (pg::physical-reseed-follow-cancellation-cleaned-record
                             closed receipt :selected-release)))))
         (let* ((final (merge-pathnames "physical-reseed.follow" root))
                (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                (replacement (merge-pathnames "configured-cleaned-replacement" root))
                (bytes (pg::physical-reseed-follow-image cleaned))
                (before (configured-cancellation-test-preserved-file-facts root))
                (state nil) (escaped nil) (entered nil) (replaced 0))
           (reseed-follow-owner-test-write temporary bytes)
           (let ((identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat temporary) t)))
             (call-with-configured-cleaned-recovery-source
              closed receipt
              (lambda (owner actual)
                (setf escaped owner state actual
                      (getf actual :allowed-commands) '(:cleanup)
                      (getf actual :on-event)
                      (lambda (event)
                        (when (eq event :cleanup)
                          (reseed-follow-owner-test-write replacement bytes)
                          (sb-posix:rename (namestring replacement) (namestring temporary))
                          (incf replaced)))))
              (lambda ()
                (ok (equal "cleanup recovery changed its captured predecessor or temporary"
                           (handler-case
                               (pg::call-with-physical-reseed-follow-cancellation-owner
                                root (lambda (owner) (declare (ignore owner)) (setf entered t))
                                :recover :cleaned :helper "/unused/helper")
                             (error (condition) (princ-to-string condition)))))))
             (ok (= 1 replaced))
             (ng entered)
             (ng (pg::physical-adoption-owner-active-p escaped))
             (ng (pg::physical-reseed-follow-owner-busy-p escaped))
             (ng (pg::physical-reseed-follow-owner-uncertain-p escaped))
             (ok (= 1 (count :cleanup (getf state :events))))
             (ok (= 1 (count :close (getf state :events))))
             (ng (member :release (getf state :events)))
             (ok (equalp bytes (lineage-test-bytes temporary)))
             (ng (equal identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat temporary) t)))
             (ok (equalp (pg::physical-reseed-follow-image closed) (lineage-test-bytes final)))
             (ok (equalp before (configured-cancellation-test-preserved-file-facts root)))))))))
