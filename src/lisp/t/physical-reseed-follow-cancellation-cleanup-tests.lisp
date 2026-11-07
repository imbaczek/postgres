(in-package #:pg-physical-tests)

(cl-spec:defproperty reseed-cancellation-source-closed-publication-freezes-predecessors
    ((aborted boolean) (handed-off boolean))
  (:about pg::physical-reseed-follow-record-cancellation-source-closed)
  (:trials (:normal 100))
  (call-with-reseed-cancellation-test-fixture
   (lambda (owner root record)
     (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
            (receipt (reseed-cancellation-source-parent
                      pending :closed (if handed-off :handed-off :aborted) t))
            (expected (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt)))
       (when aborted (reseed-follow-prepare-test-abort root record))
       (let ((actual (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)))
         (and (equal expected actual)
              (equalp (pg::physical-reseed-follow-image expected)
                      (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root)))
              (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))
              (not (pg::physical-reseed-follow-owner-busy-p owner))
              (not (pg::physical-reseed-follow-owner-uncertain-p owner))))))))

(deftest generated-reseed-cancellation-source-closed-publication-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'reseed-cancellation-source-closed-publication-freezes-predecessors
                                         :seed 198 :profile :normal))))
      (ok (eq :passed (getf result :status)))
      (ok (= 100 (getf result :trials)))
      (ok (equal "fnv1a64-v1:3bf1e422824821eb" (getf result :definition-digest)))
      (format t "~%SOURCE-CLOSED-PROPERTY ~d ~d ~a~%"
              (getf result :seed) (getf result :trials) (getf result :definition-digest))))

(deftest reseed-cancellation-source-closed-exact-retry-keeps-journal-identity
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :handed-off t))
              (closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt))
              (path (merge-pathnames "physical-reseed.follow" root))
              (identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat (namestring path)))))
         (ok (equal closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)))
         (ok (equal closed (pg::physical-reseed-follow-recover-cancellation owner)))
         (ok (equal identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat (namestring path)))))
         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))

(deftest reseed-cancellation-source-closed-temporary-recovers-after-actual-abort
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
              (expected (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
              (path (merge-pathnames "physical-reseed.follow.tmp" root)))
         (reseed-follow-owner-test-write path (pg::physical-reseed-follow-image expected))
         (let ((identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat (namestring path)))))
           (reseed-follow-prepare-test-abort root record)
           (ok (equal expected (pg::physical-reseed-follow-recover-cancellation owner)))
           (ok (not (probe-file path)))
           (ok (equal (subseq identity 0 7)
                      (subseq (pg::physical-reseed-follow-file-identity
                               (sb-posix:lstat (namestring (merge-pathnames "physical-reseed.follow" root))))
                              0 7))))
         (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))

(deftest reseed-cancellation-cleaned-temporary-needs-operation-evidence
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
              (cleaned (pg::physical-reseed-follow-cancellation-cleaned-record pending receipt :parent-abort))
              (bytes (pg::physical-reseed-follow-image cleaned))
              (path (merge-pathnames "physical-reseed.follow.tmp" root)))
         (reseed-follow-owner-test-write path bytes)
         (ok (signals (pg::physical-reseed-follow-recover-cancellation owner) 'error))
         (ok (equalp bytes (pg::recovery-read-bytes path)))
         (ok (equalp (pg::physical-reseed-follow-image pending)
                     (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))))))

(deftest reseed-cancellation-source-closed-refuses-unsettled-observations
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (bytes (pg::physical-reseed-follow-image pending)))
         (dolist (receipt
                  (list nil
                        (reseed-cancellation-source-parent pending :requested nil nil)
                        (reseed-cancellation-source-parent pending :ready nil t)
                        (reseed-cancellation-source-parent pending :closing :aborted t)))
           (ok (signals (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt) 'error))
           (ok (equalp bytes (pg::recovery-read-bytes (merge-pathnames "physical-reseed.follow" root))))
           (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
           (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
           (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner))))))))

(defun check-reseed-source-closed-publication-cut (cut escape)
  (call-with-reseed-follow-installed-fixture
   (lambda (root stage incoming quarantine plan)
     (declare (ignore stage incoming quarantine plan))
     (let ((record nil) (closed nil) (receipt nil) (fired 0)
                        (paths (mapcar (lambda (name) (merge-pathnames name root))
                                       '("physical-reseed.attempt" "physical-reseed.install"
                                         "physical-recovery.control" "global/pg_catalog.dat")))
                        (before nil))
       (pg::call-with-physical-reseed-follow-configuration-owner
        root
        (lambda (owner)
          (setf record (reseed-follow-intent-test-record
                        (pg::physical-reseed-follow-initial-record owner) root))
          (pg::physical-reseed-follow-publish-intent owner record)
          (setf record (pg::physical-reseed-follow-begin-cancellation owner)
                receipt (reseed-cancellation-source-parent record :closed :aborted t)
                closed (pg::physical-reseed-follow-cancellation-source-closed-record record receipt)
                before (mapcar (lambda (path)
                                 (list (lineage-test-bytes path)
                                       (pg::physical-reseed-file-facts (sb-posix:lstat path))))
                               paths))
          (let ((pg::*physical-reseed-follow-write-limit* 257)
                (pg::*physical-reseed-follow-test-hook*
                  (lambda (event path)
                    (declare (ignore path))
                    (when (and (eq event cut) (zerop fired))
                      (incf fired)
                      (ecase escape
                        (:error (error "source-closed publication cut"))
                        (:throw (throw 'source-closed-cut :interrupted))
                        (:interrupt
                         (sb-thread:interrupt-thread
                          sb-thread:*current-thread*
                          (lambda () (throw 'source-closed-cut :interrupted)))))))))
            (ok (eq :interrupted
                    (catch 'source-closed-cut
                      (handler-case
                          (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)
                        (error (condition)
                          (if (equal "source-closed publication cut" (princ-to-string condition))
                              :interrupted
                              (error condition))))))))
          (ok (= 1 fired))
          (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
          (ok (eq (not (eq cut :before-follow-temporary-create))
                  (not (null (pg::physical-reseed-follow-owner-uncertain-p owner)))))))
       (let* ((final (merge-pathnames "physical-reseed.follow" root))
              (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
              (partial (member cut '(:after-follow-temporary-create :after-follow-write-chunk)))
              (renamed (member cut '(:after-follow-rename :after-follow-directory-fsync
                                     :before-follow-readback :after-follow-readback)))
              (temporary-bytes (when (probe-file temporary) (lineage-test-bytes temporary))))
         (ok (equal (if renamed closed record)
                    (pg::physical-reseed-follow-decode (lineage-test-bytes final))))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root
          (lambda (owner)
            (cond
              (partial
               (ok (intake-fails-p
                    (lambda () (pg::physical-reseed-follow-recover-cancellation owner))))
               (ok (equalp temporary-bytes (lineage-test-bytes temporary)))
               (ok (equalp (pg::physical-reseed-follow-image record) (lineage-test-bytes final))))
              ((eq cut :before-follow-temporary-create)
               (ok (equal record (pg::physical-reseed-follow-recover-cancellation owner)))
               (ok (equal closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt))))
              (t
               (ok (equal closed (pg::physical-reseed-follow-recover-cancellation owner)))
               (ok (not (probe-file temporary)))
               (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))
            (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))))
         (ok (equalp before
                     (mapcar (lambda (path)
                               (list (lineage-test-bytes path)
                                     (pg::physical-reseed-file-facts (sb-posix:lstat path))))
                             paths))))))))

(deftest reseed-source-closed-error-cuts-preserve-pending-or-closed
    (dolist (cut '(:before-follow-temporary-create :after-follow-temporary-create
                   :after-follow-write-chunk :before-follow-temporary-fsync
                   :after-follow-temporary-fsync :before-follow-temporary-readback
                   :after-follow-temporary-readback :before-follow-rename
                   :after-follow-rename :after-follow-directory-fsync
                   :before-follow-readback :after-follow-readback))
      (check-reseed-source-closed-publication-cut cut :error)))

(deftest reseed-source-closed-nonlocal-cuts-preserve-pending-or-closed
    (dolist (cut '(:before-follow-temporary-create :after-follow-temporary-create
                   :after-follow-write-chunk :before-follow-temporary-fsync
                   :after-follow-temporary-fsync :before-follow-temporary-readback
                   :after-follow-temporary-readback :before-follow-rename
                   :after-follow-rename :after-follow-directory-fsync
                   :before-follow-readback :after-follow-readback))
      (check-reseed-source-closed-publication-cut cut :throw)))

(deftest reseed-source-closed-authority-rejects-wrong-scope-and-replay
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (setf record (pg::physical-reseed-follow-begin-cancellation owner))
       (let ((saved nil) (candidate (pg::physical-reseed-follow-cancellation-source-closed-record
                                     record (reseed-cancellation-source-parent record :closed :aborted t)))
                         (previous nil) (bytes nil)
                         (journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (setf bytes (pg::physical-reseed-follow-image candidate)
               (pg::physical-reseed-follow-owner-busy-p owner) t)
         (unwind-protect
              (pg::call-with-physical-directory-root
               owner
               (lambda (root-fd directory)
                 (declare (ignore directory))
                 (setf previous (pg::physical-reseed-follow-snapshot-at root-fd))
                 (ok (signals (pg::physical-reseed-follow-cancellation-claim
                               owner root-fd candidate previous :write :bytes bytes) 'error))
                 (pg::call-with-physical-reseed-follow-cancellation-cleanup-authority
                  owner root-fd candidate previous bytes :write #'values
                  (lambda (verify)
                    (setf saved pg::*physical-reseed-follow-cancellation-authority*)
                    (ok (signals (pg::physical-reseed-follow-cancellation-claim
                                  (pg::make-physical-reseed-follow-owner :busy-p t)
                                  root-fd candidate previous :write :bytes bytes) 'error))
                    (ok (signals (pg::physical-reseed-follow-cancellation-claim
                                  owner -1 candidate previous :write :bytes bytes) 'error))
                    (ok (signals (pg::physical-reseed-follow-cancellation-claim
                                  owner root-fd record previous :write :bytes bytes) 'error))
                    (ok (signals (pg::physical-reseed-follow-cancellation-claim
                                  owner root-fd candidate nil :write :bytes bytes) 'error))
                    (let ((changed (copy-seq bytes)))
                      (setf (aref changed 0) (logxor 1 (aref changed 0)))
                      (ok (signals (pg::physical-reseed-follow-cancellation-claim
                                    owner root-fd candidate previous :write :bytes changed) 'error)))
                    (ok (signals (pg::physical-reseed-follow-cancellation-claim
                                  owner root-fd candidate previous :promote :fd -1) 'error))
                    (let ((thread nil))
                      (unwind-protect
                           (progn
                             (sb-sys:without-interrupts
                               (setf thread
                                     (sb-thread:make-thread
                                      (lambda ()
                                        (let ((pg::*physical-reseed-follow-cancellation-authority* saved))
                                          (handler-case
                                              (pg::physical-reseed-follow-cancellation-claim
                                               owner root-fd candidate previous :write :bytes bytes)
                                            (error (condition) (princ-to-string condition))))))))
                             (ok (equal "cancellation publication requires its live typed authority"
                                        (sb-thread:join-thread thread))))
                        (when thread (sb-thread:join-thread thread))))
                    (funcall verify)
                    (ok (eq saved (pg::physical-reseed-follow-cancellation-claim
                                   owner root-fd candidate previous :write :bytes bytes)))
                    (ok (signals (pg::physical-reseed-follow-cancellation-claim
                                  owner root-fd candidate previous :write :bytes bytes) 'error))))
                 (ok (not (pg::physical-reseed-follow-cancellation-authority-active-p saved)))
                 (setf (pg::physical-reseed-follow-cancellation-authority-active-p saved) t
                       (pg::physical-reseed-follow-cancellation-authority-phase saved) :ready)
                 (let ((pg::*physical-reseed-follow-cancellation-authority* saved))
                   (ok (signals (pg::physical-reseed-follow-cancellation-claim
                                 owner root-fd candidate previous :write :bytes bytes) 'error)))))
           (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))
         (ok (equalp journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))

(deftest reseed-source-closed-rechecks-attempt-and-temporary-before-rename
    (dolist (fault '(:attempt :temporary-replacement :temporary-truncation))
      (call-with-reseed-cancellation-test-fixture
       (lambda (owner root record)
         (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
                (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                (fired 0)
                (pg::*physical-reseed-follow-test-hook*
                  (lambda (event path)
                    (declare (ignore path))
                    (when (and (eq event :before-follow-rename) (zerop fired))
                      (incf fired)
                      (ecase fault
                        (:attempt (reseed-follow-prepare-test-abort root record))
                        (:temporary-replacement
                         (let ((replacement (merge-pathnames "replacement-cleanup" root)))
                           (reseed-follow-owner-test-write replacement (lineage-test-bytes temporary))
                           (sb-posix:rename (namestring replacement) (namestring temporary))))
                        (:temporary-truncation
                         (with-open-file (stream temporary :direction :output :if-exists :supersede
                                                           :element-type '(unsigned-byte 8))
                           (write-byte 0 stream))))))))
           (ok (signals (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt) 'error))
           (ok (= 1 fired))
           (ok (equalp (pg::physical-reseed-follow-image pending)
                       (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
           (ok (probe-file temporary))
           (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
           (ok (not (pg::physical-reseed-follow-owner-busy-p owner))))))))

(deftest reseed-source-closed-acquisition-rejects-claims-and-nested-authority
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore root record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
              (candidate (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
              (bytes (pg::physical-reseed-follow-image candidate))
              (checks 0) (bodies 0))
         (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
         (unwind-protect
              (pg::call-with-physical-directory-root
               owner
               (lambda (root-fd directory)
                 (declare (ignore directory))
                 (let ((previous (pg::physical-reseed-follow-snapshot-at root-fd)))
                   (pg::call-with-physical-reseed-follow-cancellation-cleanup-authority
                    owner root-fd candidate previous bytes :write
                    (lambda ()
                      (incf checks)
                      (when (= checks 1)
                        (ok (signals
                             (pg::physical-reseed-follow-cancellation-claim
                              owner root-fd candidate previous :write :bytes bytes) 'error))
                        (ok (signals
                             (pg::call-with-physical-reseed-follow-cancellation-cleanup-authority
                              owner root-fd candidate previous bytes :write #'values #'values) 'error))))
                    (lambda (verify)
                      (incf bodies)
                      (funcall verify))))))
           (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))
         (ok (= 2 checks))
         (ok (= 1 bodies))))))

(defun source-closed-test-crash-child-arguments (root point cache marker)
  (let* ((arguments (retirement-intent-test-child-arguments root point cache marker))
         (form
           `(let ((pg::*pgdata* ,root)
                  (pg::*physical-reseed-follow-write-limit* 257)
                  (pg::*physical-reseed-follow-test-hook*
                    (lambda (event path)
                      (declare (ignore path))
                      (when (eq event ,point)
                        (with-open-file (out ,marker :direction :output :if-exists :error)
                          (write (list (sb-posix:getpid) ,point) :stream out :readably t))
                        (format t "~%SOURCE-CLOSED-CRASH-REACHED ~d ~s~%" (sb-posix:getpid) ,point)
                        (finish-output)
                        (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)
                        (error "cancellation child SIGKILL returned")))))
              (pg::call-with-physical-reseed-follow-owner
               ,root
               (lambda (owner)
                 (let* ((record
                          (pg::call-with-physical-directory-root
                           owner (lambda (fd directory)
                                   (declare (ignore directory))
                                   (pg::physical-reseed-follow-read-at fd "physical-reseed.follow"))))
                        (receipt (reseed-cancellation-source-parent record :closed :aborted t)))
                   (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt))))
              (error "cancellation child did not reach its crash point"))))
    (let ((*package* (find-package :cl-user))
          (*print-length* nil) (*print-level* nil) (*print-readably* t))
      (append (butlast arguments 2) (list "--eval" (write-to-string form))))))

(deftest reseed-source-closed-recovers-sigkill-cuts-in-fresh-images
    (with-physical-store-directory (cache-root)
      (let ((cache (uiop:ensure-directory-pathname
                    (sb-posix:mkdtemp (namestring (merge-pathnames "source-closed-cache.XXXXXX" cache-root))))))
        (dolist (cut '(:before-follow-temporary-create :after-follow-temporary-create
                       :after-follow-write-chunk :after-follow-temporary-fsync
                       :before-follow-rename :after-follow-rename
                       :after-follow-directory-fsync :after-follow-readback))
          (call-with-reseed-follow-installed-fixture
           (lambda (root stage incoming quarantine plan)
             (declare (ignore stage incoming quarantine plan))
             (let ((pending nil)
                   (marker (merge-pathnames
                            (format nil "source-closed-~(~a~).marker" cut) cache-root))
                   (log (merge-pathnames
                         (format nil "source-closed-recovery-~(~a~).log" cut) cache-root)))
               (pg::call-with-physical-reseed-follow-configuration-owner
                root
                (lambda (owner)
                  (let ((record (reseed-follow-intent-test-record
                                 (pg::physical-reseed-follow-initial-record owner) root)))
                    (pg::physical-reseed-follow-publish-intent owner record)
                    (setf record (pg::physical-reseed-follow-begin-cancellation owner)
                          pending (pg::physical-reseed-follow-cancellation-source-closed-record
                                   record (reseed-cancellation-source-parent record :closed :aborted t))))))
               (let ((pid (run-retirement-intent-test-crash-child
                           root cut cache marker :timeout 1200
                           :argument-builder #'source-closed-test-crash-child-arguments)))
                 (ok (plusp pid))
                 (with-open-file (input marker)
                   (let ((*read-eval* nil) (*package* (find-package :cl-user)))
                     (ok (equal (list pid cut) (read input)))
                     (ok (eq :eof (read input nil :eof))))))
               (let* ((temporary (merge-pathnames "physical-reseed.follow.tmp" root))
                      (partial (member cut '(:after-follow-temporary-create :after-follow-write-chunk)))
                      (before (when (probe-file temporary) (lineage-test-bytes temporary)))
                      (final (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                 (multiple-value-bind (status pid)
                     (reseed-follow-store-run-child
                      root log
                      (format nil
                              "(format t \"~%SOURCE-CLOSED-RECOVERY-CHILD ~~d ~~s~%\" (sb-posix:getpid) '~s)
                             (finish-output)
                             (assert (handler-case
                                         (progn (pg::physical-reseed-check-before-pid) nil)
                                       (error () t)))
                             (pg::call-with-physical-reseed-follow-owner #P~s
                               (lambda (owner) ~a))
                             (assert (handler-case
                                         (progn (pg::physical-reseed-check-before-pid) nil)
                                       (error () t)))
                             (format t \"~%SOURCE-CLOSED-RECOVERY-VERIFIED ~~d ~~s~%\" (sb-posix:getpid) '~s)
                             (finish-output)"
                              cut (namestring root)
                              (cond
                                (partial
                                 "(assert (handler-case
                                            (progn (pg::physical-reseed-follow-recover-cancellation owner) nil)
                                          (error () t)))")
                                ((eq cut :before-follow-temporary-create)
                                 (format nil "(assert (equal '~a (pg::physical-reseed-follow-record-cancellation-source-closed owner '~a)))"
                                         (pg::physical-bootstrap-manifest-text pending)
                                         (pg::physical-bootstrap-manifest-text
                                          (getf (getf pending :cancellation) :receipt))))
                                (t
                                 (format nil "(assert (equal '~a (pg::physical-reseed-follow-recover-cancellation owner)))"
                                         (pg::physical-bootstrap-manifest-text pending))))
                              cut)
                      cache)
                   (write-string (uiop:read-file-string log))
                   (format t "~%SOURCE-CLOSED-RECOVERY-VERIFIED-CHILD ~d ~s ~s~%" pid status cut)
                   (finish-output)
                   (ok (plusp pid))
                   (ok (equal '(:exited 0) status))
                   (ok (search (format nil "SOURCE-CLOSED-RECOVERY-CHILD ~d ~s" pid cut)
                               (uiop:read-file-string log)))
                   (ok (search (format nil "SOURCE-CLOSED-RECOVERY-VERIFIED ~d ~s" pid cut)
                               (uiop:read-file-string log))))
                 (if partial
                     (progn
                       (ok (equalp before (lineage-test-bytes temporary)))
                       (ok (equalp final (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
                     (progn
                       (ok (equal pending
                                  (pg::physical-reseed-follow-decode
                                   (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
                       (ok (not (probe-file temporary)))))))))))))

(deftest reseed-source-closed-recovers-after-syscall-boundary-failures
    (dolist (failure '(:temporary-fsync :directory-fsync :rename :writer-close :recovery-close))
      (call-with-reseed-follow-installed-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine plan))
         (let ((pending nil) (injected 0))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (let* ((record (reseed-follow-intent-test-record
                              (pg::physical-reseed-follow-initial-record owner) root))
                     (sync (symbol-function 'sb-posix:fsync))
                     (rename (symbol-function 'pg::port-renameat))
                     (close (symbol-function 'sb-posix:close))
                     (armed nil) (finished nil) (syncs 0)
                     (pg::*physical-reseed-follow-test-hook*
                       (lambda (event path)
                         (declare (ignore path))
                         (when (eq event :before-follow-temporary-fsync) (setf armed t))
                         (when (eq event :after-follow-readback) (setf finished t)))))
                (pg::physical-reseed-follow-publish-intent owner record)
                (setf record (pg::physical-reseed-follow-begin-cancellation owner)
                      armed nil finished nil
                      pending (pg::physical-reseed-follow-cancellation-source-closed-record
                               record (reseed-cancellation-source-parent record :closed :aborted t)))
                (when (eq failure :recovery-close)
                  (reseed-follow-owner-test-write
                   (merge-pathnames "physical-reseed.follow.tmp" root)
                   (pg::physical-reseed-follow-image pending)))
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
                                   (error "cancellation syscall boundary failure")))
                               (funcall sync fd))
                             (symbol-function 'pg::port-renameat)
                             (lambda (&rest arguments)
                               (when (and armed (eq failure :rename) (zerop injected))
                                 (incf injected)
                                 (error "cancellation syscall boundary failure"))
                               (apply rename arguments))
                             (symbol-function 'sb-posix:close)
                             (lambda (fd)
                               (let ((selected
                                       (and finished (member failure '(:writer-close :recovery-close))
                                            (zerop injected)
                                            (= (sb-posix:stat-ino (sb-posix:fstat fd))
                                               (sb-posix:stat-ino
                                                (sb-posix:lstat
                                                 (merge-pathnames "physical-reseed.follow" root)))))))
                                 (prog1 (funcall close fd)
                                   (when selected
                                     (incf injected)
                                     (error "cancellation syscall boundary failure"))))))
                       (ok (equal "cancellation syscall boundary failure"
                                  (handler-case
                                      (if (eq failure :recovery-close)
                                          (pg::physical-reseed-follow-recover-cancellation owner)
                                          (pg::physical-reseed-follow-record-cancellation-source-closed
                                           owner (getf (getf pending :cancellation) :receipt)))
                                    (error (condition) (princ-to-string condition))))))
                  (setf (symbol-function 'sb-posix:fsync) sync
                        (symbol-function 'pg::port-renameat) rename
                        (symbol-function 'sb-posix:close) close))
                (ok (= 1 injected))
                (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                (ok (not (pg::physical-reseed-follow-owner-busy-p owner))))))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (ok (equal pending (pg::physical-reseed-follow-recover-cancellation owner)))
              (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
              (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root)))))))))))

(deftest reseed-source-closed-fences-retained-journal-close-failures
    (dolist (fault '(:error :throw :interrupt :catalog-write))
      (let ((completed nil))
        (handler-case
            (call-with-reseed-cancellation-test-fixture
             (lambda (owner root record)
               (setf record (pg::physical-reseed-follow-begin-cancellation owner))
               (let ((directory-scope (symbol-function 'pg::call-with-physical-directory-root))
                     (close-stream (symbol-function 'cl:close))
                     (depth 0) (closed-root nil) (injected 0)
                     (pending (pg::physical-reseed-follow-cancellation-source-closed-record
                               record (reseed-cancellation-source-parent record :closed :aborted t))))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::call-with-physical-directory-root)
                              (lambda (actual-owner function)
                                (incf depth)
                                (unwind-protect
                                     (multiple-value-prog1
                                         (funcall directory-scope actual-owner function)
                                       (when (and (= depth 1) (eq owner actual-owner)
                                                  (equalp (pg::physical-reseed-follow-image pending)
                                                          (lineage-test-bytes
                                                           (merge-pathnames "physical-reseed.follow" root))))
                                         (setf closed-root t)))
                                  (decf depth))))
                        (sb-ext:without-package-locks
                          (setf (symbol-function 'cl:close)
                                (lambda (stream &rest arguments)
                                  (let ((selected
                                          (and closed-root (zerop injected)
                                               (typep stream 'sb-sys:fd-stream)
                                               (= (sb-posix:stat-ino
                                                   (sb-posix:fstat (sb-sys:fd-stream-fd stream)))
                                                  (sb-posix:stat-ino
                                                   (sb-posix:lstat
                                                    (merge-pathnames "physical-reseed.follow" root)))))))
                                    (prog1 (apply close-stream stream arguments)
                                      (when selected
                                        (incf injected)
                                        (ecase fault
                                          (:error (error "retained cancellation close failure"))
                                          (:throw (throw 'retained-close-cut :interrupted))
                                          (:interrupt
                                           (sb-thread:interrupt-thread
                                            sb-thread:*current-thread*
                                            (lambda () (throw 'retained-close-cut :interrupted))))
                                          (:catalog-write
                                           (with-open-file
                                               (out (merge-pathnames "global/pg_catalog.dat" root)
                                                    :direction :io :if-exists :overwrite
                                                    :element-type '(unsigned-byte 8))
                                             (let ((byte (read-byte out)))
                                               (file-position out 0)
                                               (write-byte byte out)
                                               (finish-output out)))))))))))
                        (ok (eq :interrupted
                                (catch 'retained-close-cut
                                  (handler-case
                                      (pg::physical-reseed-follow-record-cancellation-source-closed
                                       owner (getf (getf pending :cancellation) :receipt))
                                    (error (condition)
                                      (if (or (equal "retained cancellation close failure" (princ-to-string condition))
                                              (equal "cancellation image changed during resource cleanup"
                                                     (princ-to-string condition)))
                                          :interrupted
                                          (error condition)))))))
                        (ok (= 1 injected))
                        (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                        (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
                        (setf completed t))
                   (setf (symbol-function 'pg::call-with-physical-directory-root) directory-scope)
                   (sb-ext:without-package-locks
                     (setf (symbol-function 'cl:close) close-stream))))))
          (error (condition)
            (unless (and completed (eq fault :catalog-write)) (error condition))
            (ok (equal "reseed new image metadata differs from its move prefix"
                       (princ-to-string condition)))))
        (ok completed))))

(deftest reseed-source-closed-authority-detects-mutation-of-frozen-token-data
    (dolist (field '(:candidate :previous :bytes))
      (call-with-reseed-cancellation-test-fixture
       (lambda (owner root record)
         (declare (ignore root))
         (setf record (pg::physical-reseed-follow-begin-cancellation owner))
         (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
         (unwind-protect
              (pg::call-with-physical-directory-root
               owner
               (lambda (root-fd directory)
                 (declare (ignore directory))
                 (let* ((candidate (pg::physical-reseed-follow-cancellation-source-closed-record
                                    record (reseed-cancellation-source-parent record :closed :aborted t)))
                        (previous (pg::physical-reseed-follow-snapshot-at root-fd)))
                   (pg::call-with-physical-reseed-follow-cancellation-cleanup-authority
                    owner root-fd candidate previous (pg::physical-reseed-follow-image candidate)
                    :write #'values
                    (lambda (verify)
                      (let ((authority pg::*physical-reseed-follow-cancellation-authority*))
                        (ecase field
                          (:candidate
                           (setf (second (member :generations
                                                 (pg::physical-reseed-follow-cancellation-authority-candidate authority))) nil))
                          (:previous
                           (setf (second (member :record
                                                 (pg::physical-reseed-follow-cancellation-authority-previous authority))) nil))
                          (:bytes
                           (let ((bytes (pg::physical-reseed-follow-cancellation-authority-bytes authority)))
                             (setf (aref bytes 0) (logxor 1 (aref bytes 0))))))
                        (ok (equal "cancellation authority changed candidate, predecessor or scope"
                                   (handler-case (funcall verify)
                                     (error (condition) (princ-to-string condition)))))))))))
           (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))))))

(deftest reseed-source-closed-rejects-changed-owner-base-selection-and-retry
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :handed-off t))
              (path (merge-pathnames "physical-reseed.follow" root))
              (before (lineage-test-bytes path)))
         (dolist (fault '(:owner :base :selection))
           (let ((changed (pg::physical-backup-lease-copy receipt)))
             (ecase fault
               (:owner (setf (char (getf (getf changed :request) :token) 0) #\b))
               (:base (incf (getf (getf changed :base) :manifest-digest)))
               (:selection (setf (getf (getf changed :successor) :name) "foreign_cleanup_selection")))
             (ok (signals (pg::physical-reseed-follow-record-cancellation-source-closed owner changed) 'error))
             (ok (equalp before (lineage-test-bytes path)))
             (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))
         (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt)
         (let ((changed (pg::physical-backup-lease-copy receipt))
               (identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat (namestring path)))))
           (incf (getf (getf changed :successor) :anchor-digest))
           (ok (signals (pg::physical-reseed-follow-record-cancellation-source-closed owner changed) 'error))
           (ok (equal identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat (namestring path))))))
         (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))
         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))

(deftest reseed-source-closed-refuses-cleaned-successor-without-release-evidence
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :handed-off t))
              (closed (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt))
              (cleaned (pg::physical-reseed-follow-cancellation-cleaned-record closed receipt :selected-release))
              (bytes (pg::physical-reseed-follow-image cleaned))
              (path (merge-pathnames "physical-reseed.follow.tmp" root)))
         (reseed-follow-owner-test-write path bytes)
         (ok (signals (pg::physical-reseed-follow-recover-cancellation owner) 'error))
         (ok (equalp bytes (lineage-test-bytes path)))
         (ok (equalp (pg::physical-reseed-follow-image closed)
                     (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))

(deftest reseed-source-closed-preserves-malformed-or-special-temporaries
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (declare (ignore record))
       (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
              (receipt (reseed-cancellation-source-parent pending :closed :aborted t))
              (closed (pg::physical-reseed-follow-cancellation-source-closed-record pending receipt))
              (bytes (pg::physical-reseed-follow-image closed))
              (path (merge-pathnames "physical-reseed.follow.tmp" root)))
         (with-physical-store-directory (foreign)
           (let ((target (merge-pathnames "target" foreign)))
             (reseed-follow-owner-test-write target bytes)
             (dolist (fault '(:partial :checksum :symlink :fifo))
               (unwind-protect
                    (progn
                      (ecase fault
                        (:partial (reseed-follow-owner-test-write path (subseq bytes 0 17)))
                        (:checksum
                         (let ((changed (copy-seq bytes)))
                           (setf (aref changed (1- (length changed)))
                                 (logxor 1 (aref changed (1- (length changed)))))
                           (reseed-follow-owner-test-write path changed)))
                        (:symlink (sb-posix:symlink (namestring target) (namestring path)))
                        (:fifo (sb-posix:mkfifo (namestring path) #o600)))
                      (let ((identity (pg::physical-reseed-file-facts
                                       (sb-posix:lstat (namestring path))))
                            (original (when (member fault '(:partial :checksum)) (lineage-test-bytes path))))
                        (ok (signals (pg::physical-reseed-follow-recover-cancellation owner) 'error))
                        (ok (equal identity (pg::physical-reseed-file-facts
                                             (sb-posix:lstat (namestring path)))))
                        (when original (ok (equalp original (lineage-test-bytes path))))
                        (ok (equalp (pg::physical-reseed-follow-image pending)
                                    (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                        (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))))
                 (delete-file path)))))))))
