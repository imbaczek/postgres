(in-package #:pg-physical-tests)

(defun call-with-reseed-cleaned-recovery-fixture (function &optional (kind :cleaned))
  (call-with-reseed-cancellation-test-fixture
   (lambda (owner root record)
     (declare (ignore record))
     (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
            (receipt (reseed-cancellation-source-parent
                      pending :closed (if (eq kind :generation-abort) :aborted :handed-off) t))
            (closed (if (member kind '(:pending-handoff :generation-abort)) pending
                        (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)))
            (cleaned
              (if (eq kind :pending-handoff)
                  (let ((value (pg::physical-reseed-follow-copy closed)))
                    (setf (getf value :cancellation)
                          (list :phase :cleaned :receipt receipt :ack :selected-release))
                    (pg::physical-reseed-follow-copy value))
                  (pg::physical-reseed-follow-cancellation-cleaned-record
                   closed receipt (if (eq kind :generation-abort) :parent-abort :selected-release))))
            (path (merge-pathnames "physical-reseed.follow.tmp" root)))
       (reseed-follow-owner-test-write
        path (ecase kind
               ((:cleaned :pending-handoff :generation-abort) (pg::physical-reseed-follow-image cleaned))
               (:partial (subseq (pg::physical-reseed-follow-image cleaned) 0 17))
               (:wrong-phase (pg::physical-reseed-follow-image closed))
               (:noncanonical
                (let* ((text (concatenate 'string (pg::physical-bootstrap-manifest-text cleaned) " "))
                       (crc (pg::wal-crc32 (pg::string->bytes text))))
                  (pg::string->bytes (format nil "PGRESEEDFOLLOW1 ~8,'0X~%~a~%" crc text))))))
       (funcall function owner root closed cleaned path)))))

(deftest reseed-cleaned-recovery-capture-binds-complete-temporary
    (call-with-reseed-cleaned-recovery-fixture
     (lambda (owner root closed cleaned path)
       (declare (ignore root))
       (let ((bytes (lineage-test-bytes path)))
         (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
         (unwind-protect
              (let* ((context (pg::physical-reseed-follow-capture-cleaned-recovery owner))
                     (snapshot (pg::physical-reseed-follow-cleaned-recovery-network-snapshot owner context)))
                (ok (equal closed (getf (getf snapshot :follow) :record)))
                (ok (equal cleaned (getf (getf snapshot :temporary) :record)))
                (ok (signals (pg::physical-reseed-follow-cancellation-network-snapshot owner) 'error))
                (setf (getf (getf (getf snapshot :temporary) :record) :cancellation) nil)
                (ok (signals
                     (pg::physical-reseed-follow-cleaned-recovery-network-snapshot owner context snapshot) 'error))
                (ok (equal cleaned
                           (getf (getf (pg::physical-reseed-follow-cleaned-recovery-network-snapshot owner context)
                                       :temporary) :record)))
                (let* ((image (pg::physical-reseed-follow-cleaned-recovery-bytes context))
                       (first (aref image 0)))
                  (setf (aref image 0) (logxor first 1))
                  (ok (signals
                       (pg::physical-reseed-follow-cleaned-recovery-network-snapshot owner context) 'error))
                  (setf (aref image 0) first))
                (with-open-file (stream path :direction :output :if-exists :overwrite
                                             :element-type '(unsigned-byte 8))
                  (write-sequence bytes stream)
                  (pg::fsync-stream stream))
                (ok (signals
                     (pg::physical-reseed-follow-cleaned-recovery-network-snapshot owner context) 'error))
                (ok (equalp bytes (lineage-test-bytes path)))
                (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner))))
           (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))))))

(deftest reseed-cleaned-recovery-capture-preserves-invalid-temporaries
    (let ((original (symbol-function 'pg::physical-libpq-connect-frozen))
          (connections 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-libpq-connect-frozen)
                   (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (incf connections)
                     (error "invalid recovery temporary must not contact a source")))
             (dolist (kind '(:partial :wrong-phase :noncanonical :pending-handoff :generation-abort))
               (call-with-reseed-cleaned-recovery-fixture
                (lambda (owner root closed cleaned path)
                  (declare (ignore root closed cleaned))
                  (let ((bytes (lineage-test-bytes path)))
                    (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
                    (unwind-protect
                         (progn
                           (ok (signals (pg::physical-reseed-follow-capture-cleaned-recovery owner) 'error))
                           (ok (equalp bytes (lineage-test-bytes path)))
                           (ok (zerop connections))
                           (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner))))
                      (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))))
                kind)))
        (setf (symbol-function 'pg::physical-libpq-connect-frozen) original))))

(deftest reseed-cleaned-recovery-capture-admits-pending-empty-history-abort
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-configuration-owner
        root
        (lambda (owner)
          (pg::physical-reseed-follow-publish-intent owner (pg::physical-reseed-follow-initial-record owner))
          (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                 (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
                 (candidate (pg::physical-reseed-follow-cancellation-cleaned-record pending receipt :parent-abort))
                 (path (merge-pathnames "physical-reseed.follow.tmp" root)))
            (reseed-follow-owner-test-write path (pg::physical-reseed-follow-image candidate))
            (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
            (unwind-protect
                 (let* ((context (pg::physical-reseed-follow-capture-cleaned-recovery owner))
                        (snapshot (pg::physical-reseed-follow-cleaned-recovery-network-snapshot owner context)))
                   (ok (equal pending (getf (getf snapshot :follow) :record)))
                   (ok (equal candidate (getf (getf snapshot :temporary) :record)))
                   (ok (null (getf (getf pending :cancellation) :receipt)))
                   (ok (eq :parent-abort (getf (getf candidate :cancellation) :ack))))
              (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))))))))

(deftest reseed-cleaned-remote-recovery-releases-and-promotes-the-captured-inode
    (call-with-reseed-cleaned-recovery-fixture
     (lambda (owner root closed cleaned path)
       (let ((inode (sb-posix:stat-ino (sb-posix:stat path))))
         (ok (signals (pg::physical-reseed-follow-recover-cancellation owner) 'error))
         (call-with-reseed-cancellation-source
          owner closed (getf (getf cleaned :cancellation) :receipt)
          (lambda (state)
            (multiple-value-bind (actual result)
                (pg::physical-reseed-follow-recover-cleaned-cancellation owner :helper "/unused/helper")
              (ok (eq :complete result))
              (ok (equal cleaned actual))
              (ok (equal actual (pg::physical-reseed-follow-decode
                                 (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
              (ok (= inode (sb-posix:stat-ino (sb-posix:stat (merge-pathnames "physical-reseed.follow" root)))))
              (ok (not (probe-file path)))
              (ok (= 1 (count :cleanup (getf state :events))))
              (ok (= 1 (count :release (getf state :events))))
              (ok (= 1 (count :close (getf state :events))))
              (ok (zerop (count :status (getf state :events))))
              (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
              (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
              (let ((events (copy-list (getf state :events))))
                (multiple-value-bind (again outcome)
                    (pg::physical-reseed-follow-recover-cleaned-cancellation owner :helper "/unused/helper")
                  (ok (eq :complete outcome))
                  (ok (equal actual again)))
                (ok (equal events (getf state :events)))))))))))

(deftest reseed-cleaned-remote-recovery-retries-pending-abort-with-fresh-proof
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (pg::call-with-physical-reseed-follow-configuration-owner
        root
        (lambda (owner)
          (pg::physical-reseed-follow-publish-intent owner (pg::physical-reseed-follow-initial-record owner))
          (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                 (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
                 (candidate (pg::physical-reseed-follow-cancellation-cleaned-record pending receipt :parent-abort))
                 (path (merge-pathnames "physical-reseed.follow.tmp" root))
                 (bytes (pg::physical-reseed-follow-image candidate))
                 (injected 0))
            (reseed-follow-owner-test-write path bytes)
            (call-with-reseed-cancellation-source
             owner pending receipt
             (lambda (state)
               (setf (getf state :on-event)
                     (lambda (event)
                       (when (and (zerop injected) (eq event :abort))
                         (incf injected)
                         (error "ABORT committed without a usable reply"))))
               (ok (signals
                    (pg::physical-reseed-follow-recover-cleaned-cancellation owner :helper "/unused/helper") 'error))
               (ok (= 1 injected))
               (ok (equalp bytes (lineage-test-bytes path)))
               (ok (equal pending (pg::physical-reseed-follow-decode
                                   (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
               (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
               (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
               (setf (getf state :on-event) nil)
               (multiple-value-bind (actual result)
                   (pg::physical-reseed-follow-recover-cleaned-cancellation owner :helper "/unused/helper")
                 (ok (eq :complete result))
                 (ok (equal candidate actual))
                 (ok (eq :parent-abort (getf (getf actual :cancellation) :ack))))
               (ok (= 2 (count :abort (getf state :events))))
               (ok (= 2 (count :close (getf state :events))))
               (ok (zerop (count :status (getf state :events))))))))))))

(deftest reseed-cleaned-remote-recovery-rejects-invalid-temporaries-before-connect
    (dolist (kind '(:partial :wrong-phase :noncanonical :pending-handoff :generation-abort))
      (call-with-reseed-cleaned-recovery-fixture
       (lambda (owner root closed cleaned path)
         (declare (ignore root))
         (let ((bytes (lineage-test-bytes path)))
           (call-with-reseed-cancellation-source
            owner closed (getf (getf cleaned :cancellation) :receipt)
            (lambda (state)
              (ok (signals
                   (pg::physical-reseed-follow-recover-cleaned-cancellation owner :helper "/unused/helper") 'error))
              (ok (null (getf state :events)))
              (ok (equalp bytes (lineage-test-bytes path)))
              (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
              (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))
       kind)))

(deftest reseed-cleaned-remote-recovery-fences-temporary-change-during-source-work
    (dolist (cut '(:connect :cleanup :release :close))
      (call-with-reseed-cleaned-recovery-fixture
       (lambda (owner root closed cleaned path)
         (let ((bytes (lineage-test-bytes path)) (injected 0))
           (call-with-reseed-cancellation-source
            owner closed (getf (getf cleaned :cancellation) :receipt)
            (lambda (state)
              (setf (getf state :on-event)
                    (lambda (event)
                      (when (and (zerop injected) (eq event cut))
                        (incf injected)
                        (with-open-file (stream path :direction :output :if-exists :overwrite
                                                     :element-type '(unsigned-byte 8))
                          (write-sequence bytes stream)
                          (pg::fsync-stream stream)))))
              (ok (signals
                   (pg::physical-reseed-follow-recover-cleaned-cancellation owner :helper "/unused/helper") 'error))
              (ok (= 1 injected))
              (ok (equalp bytes (lineage-test-bytes path)))
              (ok (equal closed (pg::physical-reseed-follow-decode
                                 (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
              (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
              (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
              (setf (getf state :on-event) nil)
              (multiple-value-bind (actual result)
                  (pg::physical-reseed-follow-recover-cleaned-cancellation owner :helper "/unused/helper")
                (ok (eq :complete result))
                (ok (equal cleaned actual))))))))))

(deftest reseed-cleaned-remote-recovery-reopens-after-actual-publication-cuts
    (dolist (cut '(:before-follow-temporary-fsync :after-follow-temporary-fsync
                   :before-follow-rename :after-follow-rename :before-follow-readback))
      (call-with-reseed-follow-installed-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine plan))
         (let ((receipt nil) (candidate nil) (inode nil) (injected 0) (had-temporary nil))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (pg::physical-reseed-follow-publish-intent
               owner (reseed-follow-intent-test-record (pg::physical-reseed-follow-initial-record owner) root))
              (let ((pending (pg::physical-reseed-follow-begin-cancellation owner)))
                (setf receipt (reseed-cancellation-source-parent pending :closed :handed-off t)
                      candidate (pg::physical-reseed-follow-cancellation-cleaned-record
                                 (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt)
                                 receipt :selected-release))
                (call-with-reseed-cancellation-source
                 owner pending receipt
                 (lambda (state)
                   (declare (ignore state))
                   (let ((pg::*physical-reseed-follow-test-hook*
                           (lambda (event path)
                             (declare (ignore path))
                             (when (and (zerop injected) (eq event cut)
                                        (pg::physical-reseed-follow-cancellation-ack-authority-p
                                         pg::*physical-reseed-follow-cancellation-authority*))
                               (incf injected)
                               (error "CLEANED publication interrupted at owned cut")))))
                     (ok (signals
                          (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper") 'error)))
                   (ok (= 1 injected))
                   (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                   (ok (not (pg::physical-reseed-follow-owner-busy-p owner))))))
              (setf had-temporary (not (null (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                    inode (sb-posix:stat-ino
                           (sb-posix:stat
                            (merge-pathnames
                             (if had-temporary "physical-reseed.follow.tmp" "physical-reseed.follow") root))))))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (let ((saved (pg::physical-reseed-follow-decode
                            (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
                (call-with-reseed-cancellation-source
                 owner saved receipt
                 (lambda (state)
                   (multiple-value-bind (actual result)
                       (pg::physical-reseed-follow-recover-cleaned-cancellation owner :helper "/unused/helper")
                     (ok (eq :complete result))
                     (ok (equal candidate actual))
                     (ok (= inode (sb-posix:stat-ino (sb-posix:stat (merge-pathnames "physical-reseed.follow" root)))))
                     (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
                     (ok (= (if had-temporary 1 0) (count :release (getf state :events))))
                     (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                     (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner))))))))))))))

(deftest reseed-cleaned-recovers-after-syscall-boundary-failures
    (dolist (failure '(:temporary-fsync :directory-fsync :rename :writer-close :recovery-close))
      (call-with-reseed-follow-installed-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine plan))
         (let ((candidate nil) (receipt nil) (injected 0))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (pg::physical-reseed-follow-publish-intent
               owner (reseed-follow-intent-test-record (pg::physical-reseed-follow-initial-record owner) root))
              (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                     (sync (symbol-function 'sb-posix:fsync))
                     (rename (symbol-function 'pg::port-renameat))
                     (close (symbol-function 'sb-posix:close))
                     (armed nil) (finished nil) (syncs 0))
                (setf receipt (reseed-cancellation-source-parent pending :closed :handed-off t))
                (let ((closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)))
                  (setf candidate (pg::physical-reseed-follow-cancellation-cleaned-record
                                   closed receipt :selected-release))
                  (when (eq failure :recovery-close)
                    (reseed-follow-owner-test-write
                     (merge-pathnames "physical-reseed.follow.tmp" root)
                     (pg::physical-reseed-follow-image candidate)))
                  (call-with-reseed-cancellation-source
                   owner closed receipt
                   (lambda (state)
                     (declare (ignore state))
                     (let ((pg::*physical-reseed-follow-test-hook*
                             (lambda (event path)
                               (declare (ignore path))
                               (when (pg::physical-reseed-follow-cancellation-ack-authority-p
                                      pg::*physical-reseed-follow-cancellation-authority*)
                                 (when (eq event :before-follow-temporary-fsync) (setf armed t))
                                 (when (eq event :after-follow-readback) (setf finished t))))))
                       (unwind-protect
                            (progn
                              (setf (symbol-function 'sb-posix:fsync)
                                    (lambda (fd)
                                      (when armed
                                        (incf syncs)
                                        (when (and (zerop injected)
                                                   (= syncs (case failure
                                                              (:temporary-fsync 1)
                                                              (:directory-fsync 2)
                                                              (otherwise -1))))
                                          (incf injected)
                                          (error "CLEANED syscall boundary failure")))
                                      (funcall sync fd))
                                    (symbol-function 'pg::port-renameat)
                                    (lambda (&rest arguments)
                                      (when (and armed (eq failure :rename) (zerop injected))
                                        (incf injected)
                                        (error "CLEANED syscall boundary failure"))
                                      (apply rename arguments))
                                    (symbol-function 'sb-posix:close)
                                    (lambda (fd)
                                      (let ((selected
                                              (and finished
                                                   (member failure '(:writer-close :recovery-close))
                                                   (zerop injected)
                                                   (= (sb-posix:stat-ino (sb-posix:fstat fd))
                                                      (sb-posix:stat-ino
                                                       (sb-posix:lstat
                                                        (merge-pathnames "physical-reseed.follow" root)))))))
                                        (prog1 (funcall close fd)
                                          (when selected
                                            (incf injected)
                                            (error "CLEANED syscall boundary failure"))))))
                              (ok (equal "CLEANED syscall boundary failure"
                                         (handler-case
                                             (if (eq failure :recovery-close)
                                                 (pg::physical-reseed-follow-recover-cleaned-cancellation
                                                  owner :helper "/unused/helper")
                                                 (pg::physical-reseed-follow-cleanup-cancellation
                                                  owner :helper "/unused/helper"))
                                           (error (condition) (princ-to-string condition))))))
                         (setf (symbol-function 'sb-posix:fsync) sync
                               (symbol-function 'pg::port-renameat) rename
                               (symbol-function 'sb-posix:close) close)))))
                  (ok (= 1 injected))
                  (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                  (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))))))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (let ((saved (pg::physical-reseed-follow-decode
                            (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
                (call-with-reseed-cancellation-source
                 owner saved receipt
                 (lambda (state)
                   (declare (ignore state))
                   (multiple-value-bind (record outcome)
                       (pg::physical-reseed-follow-recover-cleaned-cancellation owner :helper "/unused/helper")
                     (ok (eq :complete outcome))
                     (ok (equal candidate record))
                     (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                     (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                     (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))))))))))))
