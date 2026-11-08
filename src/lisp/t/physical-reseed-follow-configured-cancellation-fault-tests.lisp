(in-package #:pg-physical-tests)

(defun configured-cancellation-test-preserved-file-facts (root)
  (loop for name in '("physical-reseed.attempt" "physical-reseed.install"
                      "physical-recovery.control" "global/pg_catalog.dat")
        for path = (merge-pathnames name root)
        collect (list (lineage-test-bytes path)
                      (pg::physical-reseed-file-facts (sb-posix:lstat path)))))

(defun configured-cancellation-test-phase-state (root phase)
  (assert (member phase '(:pending :source-closed :cleaned)))
  ;; Prepare the durable predecessor and local abort before injecting a fault.
  (unless (eq phase :pending)
    (pg::call-with-physical-reseed-follow-cancellation-owner
     root
     (lambda (owner)
       (let ((pending (pg::physical-reseed-follow-begin-cancellation owner)))
         (when (eq phase :cleaned)
           (pg::physical-reseed-follow-record-cancellation-source-closed
            owner (reseed-cancellation-source-parent pending :closed :handed-off t)))
         (reseed-follow-prepare-test-abort root pending)))))
  (let* ((record (pg::physical-reseed-follow-decode
                  (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (receipt (unless (eq phase :pending)
                    (reseed-cancellation-source-parent record :closed :handed-off t)))
         (candidate
           (ecase phase
             (:pending (pg::physical-reseed-follow-cancellation-begin-record record))
             (:source-closed (pg::physical-reseed-follow-cancellation-source-closed-record record receipt))
             (:cleaned (pg::physical-reseed-follow-cancellation-cleaned-record record receipt :selected-release)))))
    (values record candidate receipt)))

(defun configured-cancellation-test-phase-action (owner phase record observe)
  (ecase phase
    (:pending (pg::physical-reseed-follow-begin-cancellation owner))
    (:source-closed
     (pg::physical-reseed-follow-record-cancellation-source-closed
      owner (reseed-cancellation-source-parent record :closed :handed-off t)))
    (:cleaned
     ;; Run the cleanup actor with only cleanup and release allowed at the source.
     (call-with-reseed-cancellation-source
      owner record (getf (getf record :cancellation) :receipt)
      (lambda (state)
        (setf (getf state :allowed-commands) '(:cleanup :release))
        (funcall observe state)
        (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper"))))))

(defun check-configured-cancellation-test-source-events (state active)
  (ok (equal (when active
               '(:connect :identity :identity :cleanup :identity :identity
                 :release :identity :identity :close))
             (reverse (getf state :events)))))

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

(defun check-configured-cancellation-publication-cut (cut escape)
  (call-with-reseed-inert-settlement-fixture
   :same-timeline
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (let* ((final (merge-pathnames "physical-reseed.follow" root))
            (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
            (record (pg::physical-reseed-follow-decode (lineage-test-bytes final)))
            (pending (pg::physical-reseed-follow-cancellation-begin-record record))
            (before (configured-cancellation-test-preserved-file-facts root))
            (tag (gensym "CONFIGURED-CANCELLATION-CUT"))
            (escaped nil) (fired 0) (connections 0)
            (connect (symbol-function 'pg::physical-libpq-connect-frozen)))
       (unwind-protect
            (progn
              (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                    (lambda (&rest arguments)
                      (declare (ignore arguments))
                      (incf connections)
                      (error "configured cancellation cut contacted its source")))
              (ok (eq :interrupted
                      (catch tag
                        (handler-case
                            (pg::call-with-physical-reseed-follow-cancellation-owner
                             root
                             (lambda (owner)
                               (setf escaped owner)
                               (let ((pg::*physical-reseed-follow-write-limit* 257)
                                     (pg::*physical-reseed-follow-test-hook*
                                       (lambda (event path)
                                         (declare (ignore path))
                                         (when (and (eq event cut) (zerop fired))
                                           (incf fired)
                                           (ecase escape
                                             (:error (error "configured cancellation publication cut"))
                                             (:throw (throw tag :interrupted))
                                             (:interrupt
                                              (sb-thread:interrupt-thread
                                               sb-thread:*current-thread*
                                               (lambda () (throw tag :interrupted)))))))))
                                 (pg::physical-reseed-follow-begin-cancellation owner))))
                          (error (condition)
                            (if (equal "configured cancellation publication cut"
                                       (princ-to-string condition))
                                :interrupted
                                (error condition)))))))
              (ok (= 1 fired))
              (ng (pg::physical-reseed-follow-owner-busy-p escaped))
              (ng (pg::physical-adoption-owner-active-p escaped))
              (ok (eq (not (eq cut :before-follow-temporary-create))
                      (not (null (pg::physical-reseed-follow-owner-uncertain-p escaped)))))
              (let* ((partial (member cut '(:after-follow-temporary-create :after-follow-write-chunk)))
                     (renamed (member cut '(:after-follow-rename :after-follow-directory-fsync
                                            :before-follow-readback :after-follow-readback)))
                     (temporary-bytes (when (pg::physical-adoption-path-present-p temporary)
                                        (lineage-test-bytes temporary)))
                     (temporary-identity (when temporary-bytes
                                           (pg::physical-reseed-follow-file-identity
                                            (sb-posix:lstat (namestring temporary)) t))))
                (ok (equal (if renamed pending record)
                           (pg::physical-reseed-follow-decode (lineage-test-bytes final))))
                (ok (equalp (pg::physical-reseed-follow-image (if renamed pending record))
                            (lineage-test-bytes final)))
                (cond
                  (partial
                   (if (eq cut :after-follow-temporary-create)
                       (ok (zerop (length temporary-bytes)))
                       (progn
                         (ok (< 0 (length temporary-bytes)
                                (length (pg::physical-reseed-follow-image pending))))
                         (ok (<= (length temporary-bytes) 257))
                         (ok (equalp temporary-bytes
                                     (subseq (pg::physical-reseed-follow-image pending)
                                             0 (length temporary-bytes))))))
                   (let ((admitted nil))
                     (ok (signals
                          (pg::call-with-physical-reseed-follow-cancellation-owner
                           root (lambda (owner) (declare (ignore owner)) (setf admitted t))
                           :recover t)
                          'error))
                     (ng admitted))
                   (ok (equalp temporary-bytes (lineage-test-bytes temporary)))
                   (ok (equal temporary-identity
                              (pg::physical-reseed-follow-file-identity
                               (sb-posix:lstat (namestring temporary)) t)))
                   (ok (equalp (pg::physical-reseed-follow-image record) (lineage-test-bytes final))))
                  (t
                   (pg::call-with-physical-reseed-follow-cancellation-owner
                    root
                    (lambda (owner)
                      (when (eq cut :before-follow-temporary-create)
                        (ok (signals (pg::physical-reseed-follow-recover-cancellation owner) 'error))
                        (ok (equal pending (pg::physical-reseed-follow-begin-cancellation owner))))
                      (ok (equal pending (pg::physical-reseed-follow-read-configured-cancellation owner)))
                      (ng (pg::physical-reseed-follow-owner-uncertain-p owner))
                      (ng (pg::physical-reseed-follow-owner-busy-p owner)))
                    :recover (not (eq cut :before-follow-temporary-create)))
                   (ok (equalp (pg::physical-reseed-follow-image pending)
                               (lineage-test-bytes final)))
                   (ng (pg::physical-adoption-path-present-p temporary))
                   (when temporary-identity
                     (ok (equal (subseq temporary-identity 0 7)
                                (subseq (pg::physical-reseed-follow-file-identity
                                         (sb-posix:lstat (namestring final))) 0 7)))))))
              (ok (zerop connections))
              (ok (equalp before
                          (configured-cancellation-test-preserved-file-facts root))))
         (setf (symbol-function 'pg::physical-libpq-connect-frozen) connect))))))

(deftest reseed-configured-cancellation-cuts-before-follow-temporary-create
    (dolist (escape '(:error :throw :interrupt))
      (testing (symbol-name escape)
               (check-configured-cancellation-publication-cut :before-follow-temporary-create escape))))

(deftest reseed-configured-cancellation-cuts-after-follow-temporary-create
    (dolist (escape '(:error :throw :interrupt))
      (testing (symbol-name escape)
               (check-configured-cancellation-publication-cut :after-follow-temporary-create escape))))

(deftest reseed-configured-cancellation-cuts-after-follow-write-chunk
    (dolist (escape '(:error :throw :interrupt))
      (testing (symbol-name escape)
               (check-configured-cancellation-publication-cut :after-follow-write-chunk escape))))

(deftest reseed-configured-cancellation-cuts-before-follow-temporary-fsync
    (dolist (escape '(:error :throw :interrupt))
      (testing (symbol-name escape)
               (check-configured-cancellation-publication-cut :before-follow-temporary-fsync escape))))

(deftest reseed-configured-cancellation-cuts-after-follow-temporary-fsync
    (dolist (escape '(:error :throw :interrupt))
      (testing (symbol-name escape)
               (check-configured-cancellation-publication-cut :after-follow-temporary-fsync escape))))

(deftest reseed-configured-cancellation-cuts-before-follow-temporary-readback
    (dolist (escape '(:error :throw :interrupt))
      (testing (symbol-name escape)
               (check-configured-cancellation-publication-cut :before-follow-temporary-readback escape))))

(deftest reseed-configured-cancellation-cuts-after-follow-temporary-readback
    (dolist (escape '(:error :throw :interrupt))
      (testing (symbol-name escape)
               (check-configured-cancellation-publication-cut :after-follow-temporary-readback escape))))

(deftest reseed-configured-cancellation-cuts-before-follow-rename
    (dolist (escape '(:error :throw :interrupt))
      (testing (symbol-name escape)
               (check-configured-cancellation-publication-cut :before-follow-rename escape))))

(deftest reseed-configured-cancellation-cuts-after-follow-rename
    (dolist (escape '(:error :throw :interrupt))
      (testing (symbol-name escape)
               (check-configured-cancellation-publication-cut :after-follow-rename escape))))

(deftest reseed-configured-cancellation-cuts-after-follow-directory-fsync
    (dolist (escape '(:error :throw :interrupt))
      (testing (symbol-name escape)
               (check-configured-cancellation-publication-cut :after-follow-directory-fsync escape))))

(deftest reseed-configured-cancellation-cuts-before-follow-readback
    (dolist (escape '(:error :throw :interrupt))
      (testing (symbol-name escape)
               (check-configured-cancellation-publication-cut :before-follow-readback escape))))

(deftest reseed-configured-cancellation-cuts-after-follow-readback
    (dolist (escape '(:error :throw :interrupt))
      (testing (symbol-name escape)
               (check-configured-cancellation-publication-cut :after-follow-readback escape))))

(defun check-configured-cancellation-store-close-exit (escape)
  (call-with-reseed-inert-settlement-fixture
   :same-timeline
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (let ((stamp (symbol-function 'pg::physical-reseed-follow-cancellation-cleanup-stamp))
           (close-store (symbol-function 'pg::physical-store-close))
           (connect (symbol-function 'pg::physical-libpq-connect-frozen))
           (opened nil) (descriptors nil) (escaped nil) (expected nil)
           (fired 0) (connections 0) (tag (gensym "CONFIGURED-CANCELLATION-CLOSE")))
       (unwind-protect
            (progn
              (setf (symbol-function 'pg::physical-reseed-follow-cancellation-cleanup-stamp)
                    (lambda (owner fd check)
                      (let ((context (pg::physical-reseed-follow-installation-context owner)))
                        (setf opened (pg::physical-reseed-follow-installation-context-store context)
                              descriptors (copy-tree
                                           (pg::physical-reseed-follow-installation-context-descriptors context))))
                      (funcall stamp owner fd check))
                    (symbol-function 'pg::physical-store-close)
                    (lambda (store)
                      (multiple-value-prog1 (funcall close-store store)
                        (when (and (eq store opened) (zerop fired))
                          (incf fired)
                          (ecase escape
                            (:error (error "configured cancellation store close cut"))
                            (:throw (throw tag :interrupted))
                            (:interrupt
                             (sb-thread:interrupt-thread
                              sb-thread:*current-thread*
                              (lambda () (throw tag :interrupted))))))))
                    (symbol-function 'pg::physical-libpq-connect-frozen)
                    (lambda (&rest arguments)
                      (declare (ignore arguments))
                      (incf connections)
                      (error "configured cancellation close contacted its source")))
              (ok (eq :interrupted
                      (catch tag
                        (handler-case
                            (pg::call-with-physical-reseed-follow-cancellation-owner
                             root
                             (lambda (owner)
                               (setf escaped owner
                                     expected (pg::physical-reseed-follow-cancellation-begin-record
                                               (pg::physical-reseed-follow-read-configured-cancellation owner)))
                               (pg::physical-reseed-follow-begin-cancellation owner)))
                          (error (condition)
                            (if (equal "configured cancellation store close cut" (princ-to-string condition))
                                :interrupted
                                (error condition)))))))
              (setf (symbol-function 'pg::physical-reseed-follow-cancellation-cleanup-stamp) stamp
                    (symbol-function 'pg::physical-store-close) close-store)
              (ok (= 1 fired))
              (ok (= 10 (length descriptors)))
              (dolist (entry descriptors)
                (ok (handler-case
                        (progn (sb-posix:fstat (cdr entry)) nil)
                      (sb-posix:syscall-error (condition)
                        (= sb-posix:ebadf (sb-posix:syscall-errno condition))))))
              (ok (and opened (pg::physical-store-closed-p opened)))
              (ng (open-stream-p (pg::physical-store-lock-stream opened)))
              (ok (pg::physical-reseed-follow-owner-uncertain-p escaped))
              (ng (pg::physical-reseed-follow-owner-busy-p escaped))
              (ng (pg::physical-adoption-owner-active-p escaped))
              (ok (equalp (pg::physical-reseed-follow-image expected)
                          (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
              (ng (pg::physical-adoption-path-present-p
                   (merge-pathnames "physical-reseed.follow.tmp" root)))
              (pg::call-with-physical-reseed-follow-cancellation-owner
               root
               (lambda (owner)
                 (ok (equal expected (pg::physical-reseed-follow-read-configured-cancellation owner)))
                 (ng (pg::physical-reseed-follow-owner-uncertain-p owner)))
               :recover t)
              (ok (zerop connections)))
         (setf (symbol-function 'pg::physical-reseed-follow-cancellation-cleanup-stamp) stamp
               (symbol-function 'pg::physical-store-close) close-store
               (symbol-function 'pg::physical-libpq-connect-frozen) connect))))))

(deftest reseed-configured-cancellation-store-close-error-revokes-and-recovers
    (check-configured-cancellation-store-close-exit :error))

(deftest reseed-configured-cancellation-store-close-throw-revokes-and-recovers
    (check-configured-cancellation-store-close-exit :throw))

(deftest reseed-configured-cancellation-store-close-interrupt-revokes-and-recovers
    (check-configured-cancellation-store-close-exit :interrupt))

(defun check-configured-cancellation-postclose-mutation (role audit-index)
  (call-with-reseed-inert-settlement-fixture
   :same-timeline
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (let ((make-stamp (symbol-function 'pg::physical-reseed-follow-cancellation-cleanup-stamp))
           (audit (symbol-function 'pg::physical-reseed-follow-cancellation-audit-cleanup-stamp))
           (connect (symbol-function 'pg::physical-libpq-connect-frozen))
           (facts (configured-cancellation-test-preserved-file-facts root))
           (escaped nil) (expected nil) (descriptors nil) (store nil)
           (foreign nil) (created-p nil) (foreign-facts nil) (message nil) (returned nil)
           (audits 0) (fired 0) (connections 0)
           (bytes (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(17 23 41))))
       (unwind-protect
            (progn
              (setf (symbol-function 'pg::physical-reseed-follow-cancellation-cleanup-stamp)
                    (lambda (owner fd check)
                      (let ((context (pg::physical-reseed-follow-installation-context owner)))
                        (setf descriptors
                              (copy-tree (pg::physical-reseed-follow-installation-context-descriptors context))
                              store (pg::physical-reseed-follow-installation-context-store context)))
                      (funcall make-stamp owner fd check))
                    (symbol-function 'pg::physical-reseed-follow-cancellation-audit-cleanup-stamp)
                    (lambda (owner stamp)
                      (when (eq owner escaped)
                        (incf audits)
                        (when (= audits audit-index)
                          (ok (= 10 (length descriptors)))
                          (dolist (entry descriptors)
                            (ok (handler-case
                                    (progn (sb-posix:fstat (cdr entry)) nil)
                                  (sb-posix:syscall-error (condition)
                                    (= sb-posix:ebadf (sb-posix:syscall-errno condition))))))
                          (ok (pg::physical-store-closed-p store))
                          (ng (open-stream-p (pg::physical-store-lock-stream store)))
                          (let ((entry (assoc role (getf stamp :roles))))
                            (unless entry (error "publication stamp lacks its requested role"))
                            (setf foreign (merge-pathnames ".unexpected-published-child"
                                                           (pathname (second entry)))))
                          (with-open-file (output foreign :direction :output :if-exists :error
                                                          :element-type '(unsigned-byte 8))
                            (setf created-p t)
                            (write-sequence bytes output))
                          (sb-posix:chmod (namestring foreign) #o600)
                          (setf foreign-facts
                                (pg::physical-reseed-file-facts (sb-posix:lstat (namestring foreign))))
                          (incf fired)))
                      (funcall audit owner stamp))
                    (symbol-function 'pg::physical-libpq-connect-frozen)
                    (lambda (&rest arguments)
                      (declare (ignore arguments))
                      (incf connections)
                      (error "configured cancellation mutation check contacted its source")))
              (setf message
                    (handler-case
                        (progn
                          (pg::call-with-physical-reseed-follow-cancellation-owner
                           root
                           (lambda (owner)
                             (setf escaped owner
                                   expected (pg::physical-reseed-follow-cancellation-begin-record
                                             (pg::physical-reseed-follow-read-configured-cancellation owner)))
                             (pg::physical-reseed-follow-begin-cancellation owner)))
                          (setf returned t)
                          nil)
                      (error (condition) (princ-to-string condition))))
              (ok (equal "retirement observation changed during resource cleanup" message))
              (ok (= 1 fired))
              (ok (= audit-index audits))
              (ng returned)
              (ok (pg::physical-reseed-follow-owner-uncertain-p escaped))
              (ng (pg::physical-reseed-follow-owner-busy-p escaped))
              (ng (pg::physical-adoption-owner-active-p escaped))
              (ok (equalp (pg::physical-reseed-follow-image expected)
                          (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
              (ng (pg::physical-adoption-path-present-p
                   (merge-pathnames "physical-reseed.follow.tmp" root)))
              (ok (equalp bytes (lineage-test-bytes foreign)))
              (ok (equal foreign-facts
                         (pg::physical-reseed-file-facts (sb-posix:lstat (namestring foreign)))))
              (ok (equalp facts (configured-cancellation-test-preserved-file-facts root)))
              (ok (zerop connections)))
         (setf (symbol-function 'pg::physical-reseed-follow-cancellation-cleanup-stamp) make-stamp
               (symbol-function 'pg::physical-reseed-follow-cancellation-audit-cleanup-stamp) audit
               (symbol-function 'pg::physical-libpq-connect-frozen) connect)
         (when (and created-p (pg::physical-adoption-path-present-p foreign))
           (delete-file foreign)))))))

(deftest reseed-configured-cancellation-postclose-mutation-root
    (dolist (audit-index '(1 2))
      (testing (format nil "audit ~d" audit-index)
               (check-configured-cancellation-postclose-mutation :root audit-index))))

(deftest reseed-configured-cancellation-postclose-mutation-incoming
    (dolist (audit-index '(1 2))
      (testing (format nil "audit ~d" audit-index)
               (check-configured-cancellation-postclose-mutation :incoming audit-index))))

(deftest reseed-configured-cancellation-postclose-mutation-quarantine
    (dolist (audit-index '(1 2))
      (testing (format nil "audit ~d" audit-index)
               (check-configured-cancellation-postclose-mutation :quarantine audit-index))))

(deftest reseed-configured-cancellation-postclose-mutation-base
    (dolist (audit-index '(1 2))
      (testing (format nil "audit ~d" audit-index)
               (check-configured-cancellation-postclose-mutation :base audit-index))))

(deftest reseed-configured-cancellation-postclose-mutation-global
    (dolist (audit-index '(1 2))
      (testing (format nil "audit ~d" audit-index)
               (check-configured-cancellation-postclose-mutation :global audit-index))))

(deftest reseed-configured-cancellation-postclose-mutation-wal
    (dolist (audit-index '(1 2))
      (testing (format nil "audit ~d" audit-index)
               (check-configured-cancellation-postclose-mutation :wal audit-index))))

(deftest reseed-configured-cancellation-postclose-mutation-workspace
    (dolist (audit-index '(1 2))
      (testing (format nil "audit ~d" audit-index)
               (check-configured-cancellation-postclose-mutation :workspace audit-index))))

(deftest reseed-configured-cancellation-postclose-mutation-staged
    (dolist (audit-index '(1 2))
      (testing (format nil "audit ~d" audit-index)
               (check-configured-cancellation-postclose-mutation :staged audit-index))))

(deftest reseed-configured-cancellation-postclose-mutation-saved
    (dolist (audit-index '(1 2))
      (testing (format nil "audit ~d" audit-index)
               (check-configured-cancellation-postclose-mutation :saved audit-index))))

(deftest reseed-configured-cancellation-postclose-mutation-receive
    (dolist (audit-index '(1 2))
      (testing (format nil "audit ~d" audit-index)
               (check-configured-cancellation-postclose-mutation :receive audit-index))))

(defun check-configured-cancellation-syscall-failure (failure &optional (phase :pending))
  (call-with-reseed-inert-settlement-fixture
   :same-timeline
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (multiple-value-bind (record candidate receipt)
         (configured-cancellation-test-phase-state root phase)
       (let* ((final (merge-pathnames "physical-reseed.follow" root))
              (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
              (before (lineage-test-bytes final))
              (facts (configured-cancellation-test-preserved-file-facts root))
              (sync (symbol-function 'sb-posix:fsync))
              (close-fd (symbol-function 'sb-posix:close))
              (rename (symbol-function 'pg::port-renameat))
              (connect (symbol-function 'pg::physical-libpq-connect-frozen))
              (escaped nil) (store nil) (descriptors nil)
              (journal-fd nil) (root-fd nil) (condition nil)
              (finished nil) (entered nil) (fired 0) (connections 0)
              (source-state nil) (recovery-state nil))
         (when (eq failure :recovery-close)
           (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image candidate)))
         (unwind-protect
              (progn
                (sb-ext:without-package-locks
                  (setf (symbol-function 'sb-posix:fsync)
                        (lambda (fd)
                          (when (and (zerop fired)
                                     (case failure
                                       (:temporary-fsync (eql fd journal-fd))
                                       (:directory-fsync (eql fd root-fd))))
                            (incf fired)
                            (error 'sb-posix:syscall-error :errno sb-posix:eio :name "fsync"))
                          (funcall sync fd))
                        (symbol-function 'sb-posix:close)
                        (lambda (fd)
                          (let ((selected (and finished (zerop fired) (eql fd journal-fd)
                                               (member failure '(:writer-close :recovery-close)))))
                            (multiple-value-prog1 (funcall close-fd fd)
                              (when selected
                                (incf fired)
                                (error 'sb-posix:syscall-error :errno sb-posix:eio :name "close")))))
                        (symbol-function 'pg::port-renameat)
                        (lambda (old-fd old-name new-fd new-name)
                          (if (and (eq failure :rename) (zerop fired)
                                   (eql old-fd root-fd) (eql new-fd root-fd)
                                   (equal old-name "physical-reseed.follow.tmp")
                                   (equal new-name "physical-reseed.follow"))
                              (progn (incf fired) -1)
                              (funcall rename old-fd old-name new-fd new-name)))
                        (symbol-function 'pg::physical-libpq-connect-frozen)
                        (lambda (&rest arguments)
                          (declare (ignore arguments))
                          (incf connections)
                          (error "configured cancellation syscall check contacted its source"))))
                (flet ((open-owner ()
                         (let ((pg::*physical-reseed-follow-test-hook*
                                 (lambda (event path)
                                   (declare (ignore path))
                                   (when (eq event :before-follow-temporary-fsync)
                                     (let ((context pg::*physical-reseed-follow-installation-scope*)
                                           (authority pg::*physical-reseed-follow-cancellation-authority*))
                                       (setf escaped (pg::physical-reseed-follow-installation-context-owner context)
                                             store (pg::physical-reseed-follow-installation-context-store context)
                                             descriptors
                                             (copy-tree (pg::physical-reseed-follow-installation-context-descriptors context))
                                             root-fd (pg::physical-reseed-follow-configuration-role-fd descriptors :root)
                                             journal-fd (pg::physical-reseed-follow-cancellation-authority-fd authority))))
                                   (when (eq event :after-follow-readback) (setf finished t)))))
                           (setf condition
                                 (handler-case
                                     (pg::call-with-physical-reseed-follow-cancellation-owner
                                      root
                                      (lambda (owner)
                                        (setf entered t escaped owner)
                                        (configured-cancellation-test-phase-action
                                         owner phase record (lambda (state) (setf source-state state))))
                                      :recover (when (eq failure :recovery-close)
                                                 (if (eq phase :cleaned) :cleaned t))
                                      :helper "/unused/helper")
                                   (error (caught) caught))))))
                  (if (and (eq phase :cleaned) (eq failure :recovery-close))
                      (call-with-configured-cleaned-recovery-source
                       record receipt
                       (lambda (owner state)
                         (setf escaped owner source-state state
                               (getf state :allowed-commands) '(:cleanup :release)))
                       #'open-owner)
                      (open-owner)))
                (sb-ext:without-package-locks
                  (setf (symbol-function 'sb-posix:fsync) sync
                        (symbol-function 'sb-posix:close) close-fd
                        (symbol-function 'pg::port-renameat) rename))
                (check-configured-cancellation-test-source-events source-state (eq phase :cleaned))
                (ok (= 1 fired))
                (ok (eq (not (eq failure :recovery-close)) (not (null entered))))
                (if (eq failure :rename)
                    (ok (and condition (search "cannot promote target follow intent: errno "
                                               (princ-to-string condition))))
                    (progn
                      (ok (typep condition 'sb-posix:syscall-error))
                      (ok (= sb-posix:eio (sb-posix:syscall-errno condition)))
                      (ok (equal (if (member failure '(:writer-close :recovery-close)) "close" "fsync")
                                 (sb-posix:syscall-name condition)))))
                (ok (= 10 (length descriptors)))
                (dolist (fd (cons journal-fd (mapcar #'cdr descriptors)))
                  (ok (handler-case
                          (progn (sb-posix:fstat fd) nil)
                        (sb-posix:syscall-error (caught)
                          (= sb-posix:ebadf (sb-posix:syscall-errno caught))))))
                (ok (pg::physical-store-closed-p store))
                (ng (open-stream-p (pg::physical-store-lock-stream store)))
                (ok (pg::physical-reseed-follow-owner-uncertain-p escaped))
                (ng (pg::physical-reseed-follow-owner-busy-p escaped))
                (ng (pg::physical-adoption-owner-active-p escaped))
                (if (member failure '(:temporary-fsync :rename))
                    (progn
                      (ok (equalp before (lineage-test-bytes final)))
                      (ok (equalp (pg::physical-reseed-follow-image candidate) (lineage-test-bytes temporary))))
                    (progn
                      (ok (equalp (pg::physical-reseed-follow-image candidate) (lineage-test-bytes final)))
                      (ng (pg::physical-adoption-path-present-p temporary))))
                (when (and (eq phase :cleaned) (pg::physical-adoption-path-present-p temporary))
                  (dolist (mode '(nil t))
                    (let ((admitted nil))
                      (ok (signals
                           (pg::call-with-physical-reseed-follow-cancellation-owner
                            root (lambda (owner) (declare (ignore owner)) (setf admitted t)) :recover mode)
                           'error))
                      (ng admitted)))
                  (ok (equalp before (lineage-test-bytes final)))
                  (ok (equalp (pg::physical-reseed-follow-image candidate) (lineage-test-bytes temporary))))
                (let ((identity (pg::physical-reseed-follow-file-identity
                                 (sb-posix:lstat (if (pg::physical-adoption-path-present-p temporary)
                                                     temporary final)))))
                  (flet ((recover-owner ()
                           (pg::call-with-physical-reseed-follow-cancellation-owner
                            root
                            (lambda (owner)
                              (ok (equal candidate (pg::physical-reseed-follow-read-configured-cancellation owner)))
                              (ng (pg::physical-reseed-follow-owner-uncertain-p owner))
                              (ng (pg::physical-reseed-follow-owner-busy-p owner)))
                            :recover (if (eq phase :cleaned) :cleaned t) :helper "/unused/helper")))
                    (if (eq phase :cleaned)
                        (call-with-configured-cleaned-recovery-source
                         record receipt
                         (lambda (owner state)
                           (declare (ignore owner))
                           (setf recovery-state state (getf state :allowed-commands) '(:cleanup :release)))
                         #'recover-owner)
                        (recover-owner)))
                  (check-configured-cancellation-test-source-events
                   recovery-state (and (eq phase :cleaned) (member failure '(:temporary-fsync :rename))))
                  (ok (equal (subseq identity 0 7)
                             (subseq (pg::physical-reseed-follow-file-identity (sb-posix:lstat final)) 0 7))))
                (ok (equalp (pg::physical-reseed-follow-image candidate) (lineage-test-bytes final)))
                (ng (pg::physical-adoption-path-present-p temporary))
                (ok (equalp facts (configured-cancellation-test-preserved-file-facts root)))
                (ok (zerop connections)))
           (sb-ext:without-package-locks
             (setf (symbol-function 'sb-posix:fsync) sync
                   (symbol-function 'sb-posix:close) close-fd
                   (symbol-function 'pg::port-renameat) rename
                   (symbol-function 'pg::physical-libpq-connect-frozen) connect))))))))

(deftest reseed-configured-cancellation-syscall-temporary-fsync
    (check-configured-cancellation-syscall-failure :temporary-fsync))

(deftest reseed-configured-cancellation-syscall-directory-fsync
    (check-configured-cancellation-syscall-failure :directory-fsync))

(deftest reseed-configured-cancellation-syscall-rename
    (check-configured-cancellation-syscall-failure :rename))

(deftest reseed-configured-cancellation-syscall-writer-close
    (check-configured-cancellation-syscall-failure :writer-close))

(deftest reseed-configured-cancellation-syscall-recovery-close
    (check-configured-cancellation-syscall-failure :recovery-close))

(deftest reseed-configured-source-closed-syscall-temporary-fsync
    (check-configured-cancellation-syscall-failure :temporary-fsync :source-closed))

(deftest reseed-configured-source-closed-syscall-directory-fsync
    (check-configured-cancellation-syscall-failure :directory-fsync :source-closed))

(deftest reseed-configured-source-closed-syscall-rename
    (check-configured-cancellation-syscall-failure :rename :source-closed))

(deftest reseed-configured-source-closed-syscall-writer-close
    (check-configured-cancellation-syscall-failure :writer-close :source-closed))

(deftest reseed-configured-source-closed-syscall-recovery-close
    (check-configured-cancellation-syscall-failure :recovery-close :source-closed))

(deftest reseed-configured-cleaned-syscall-temporary-fsync
    (check-configured-cancellation-syscall-failure :temporary-fsync :cleaned))

(deftest reseed-configured-cleaned-syscall-directory-fsync
    (check-configured-cancellation-syscall-failure :directory-fsync :cleaned))

(deftest reseed-configured-cleaned-syscall-rename
    (check-configured-cancellation-syscall-failure :rename :cleaned))

(deftest reseed-configured-cleaned-syscall-writer-close
    (check-configured-cancellation-syscall-failure :writer-close :cleaned))

(deftest reseed-configured-cleaned-syscall-recovery-close
    (check-configured-cancellation-syscall-failure :recovery-close :cleaned))
