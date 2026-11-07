(in-package #:pg-physical-tests)

(defun check-reseed-cancellation-publication-cut (cut escape)
  (call-with-reseed-follow-installed-fixture
   (lambda (root stage incoming quarantine plan)
     (declare (ignore stage incoming quarantine plan))
     (let ((record nil) (pending nil) (fired 0)
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
          (setf pending (pg::physical-reseed-follow-cancellation-begin-record record)
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
                        (:error (error "cancellation publication cut"))
                        (:throw (throw 'cancellation-cut :interrupted))
                        (:interrupt
                         (sb-thread:interrupt-thread
                          sb-thread:*current-thread*
                          (lambda () (throw 'cancellation-cut :interrupted)))))))))
            (ok (eq :interrupted
                    (catch 'cancellation-cut
                      (handler-case
                          (pg::physical-reseed-follow-begin-cancellation owner)
                        (error (condition)
                          (if (equal "cancellation publication cut" (princ-to-string condition))
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
         (ok (equal (if renamed pending record)
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
               (ok (intake-fails-p
                    (lambda () (pg::physical-reseed-follow-recover-cancellation owner))))
               (ok (equal pending (pg::physical-reseed-follow-begin-cancellation owner))))
              (t
               (ok (equal pending (pg::physical-reseed-follow-recover-cancellation owner)))
               (ok (not (probe-file temporary)))
               (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))
            (ok (not (pg::physical-reseed-follow-owner-busy-p owner)))))
         (ok (equalp before
                     (mapcar (lambda (path)
                               (list (lineage-test-bytes path)
                                     (pg::physical-reseed-file-facts (sb-posix:lstat path))))
                             paths))))))))

(deftest reseed-cancellation-error-cuts-reopen-without-losing-predecessors
    (dolist (cut '(:before-follow-temporary-create :after-follow-temporary-create
                   :after-follow-write-chunk :before-follow-temporary-fsync
                   :after-follow-temporary-fsync :before-follow-temporary-readback
                   :after-follow-temporary-readback :before-follow-rename
                   :after-follow-rename :after-follow-directory-fsync
                   :before-follow-readback :after-follow-readback))
      (check-reseed-cancellation-publication-cut cut :error)))

(deftest reseed-cancellation-throw-cuts-reopen-without-losing-predecessors
    (dolist (cut '(:before-follow-temporary-create :after-follow-temporary-create
                   :after-follow-write-chunk :before-follow-temporary-fsync
                   :after-follow-temporary-fsync :before-follow-temporary-readback
                   :after-follow-temporary-readback :before-follow-rename
                   :after-follow-rename :after-follow-directory-fsync
                   :before-follow-readback :after-follow-readback))
      (check-reseed-cancellation-publication-cut cut :throw)))

(deftest reseed-cancellation-authority-rejects-wrong-scope-and-replay
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (let ((saved nil) (candidate (pg::physical-reseed-follow-cancellation-begin-record record))
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
                 (pg::call-with-physical-reseed-follow-cancellation-authority
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
                    (let ((thread
                            (sb-thread:make-thread
                             (lambda ()
                               (let ((pg::*physical-reseed-follow-cancellation-authority* saved))
                                 (handler-case
                                     (pg::physical-reseed-follow-cancellation-claim
                                      owner root-fd candidate previous :write :bytes bytes)
                                   (error (condition) (princ-to-string condition))))))))
                      (ok (equal "cancellation publication requires its live typed authority"
                                 (sb-thread:join-thread thread))))
                    (funcall verify)
                    (ok (eq saved (pg::physical-reseed-follow-cancellation-claim
                                   owner root-fd candidate previous :write :bytes bytes)))
                    (ok (signals (pg::physical-reseed-follow-cancellation-claim
                                  owner root-fd candidate previous :write :bytes bytes) 'error))))
                 (ok (not (pg::physical-reseed-follow-cancellation-authority-active-p saved)))
                 (let ((pg::*physical-reseed-follow-cancellation-authority* saved))
                   (ok (signals (pg::physical-reseed-follow-cancellation-claim
                                 owner root-fd candidate previous :write :bytes bytes) 'error)))))
           (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))
         (ok (equalp journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))

(deftest reseed-cancellation-authority-detects-mutation-of-frozen-token-data
    (dolist (field '(:candidate :previous :bytes))
      (call-with-reseed-cancellation-test-fixture
       (lambda (owner root record)
         (declare (ignore root))
         (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
         (unwind-protect
              (pg::call-with-physical-directory-root
               owner
               (lambda (root-fd directory)
                 (declare (ignore directory))
                 (let* ((candidate (pg::physical-reseed-follow-cancellation-begin-record record))
                        (previous (pg::physical-reseed-follow-snapshot-at root-fd)))
                   (pg::call-with-physical-reseed-follow-cancellation-authority
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

(deftest reseed-cancellation-recovers-after-syscall-boundary-failures
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
                (setf armed nil finished nil
                      pending (pg::physical-reseed-follow-cancellation-begin-record record))
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
                                          (pg::physical-reseed-follow-begin-cancellation owner))
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

(deftest reseed-cancellation-fences-retained-journal-close-failures
    (dolist (fault '(:error :throw :interrupt :catalog-write))
      (let ((completed nil))
        (handler-case
            (call-with-reseed-cancellation-test-fixture
             (lambda (owner root record)
               (let ((directory-scope (symbol-function 'pg::call-with-physical-directory-root))
                     (close-stream (symbol-function 'cl:close))
                     (depth 0) (closed-root nil) (injected 0)
                     (pending (pg::physical-reseed-follow-cancellation-begin-record record)))
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
                                      (pg::physical-reseed-follow-begin-cancellation owner)
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

(defun cancellation-test-crash-child-arguments (root point cache marker)
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
                        (format t "~%CANCELLATION-CRASH-REACHED ~d ~s~%" (sb-posix:getpid) ,point)
                        (finish-output)
                        (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)
                        (error "cancellation child SIGKILL returned")))))
              (pg::call-with-physical-reseed-follow-owner
               ,root #'pg::physical-reseed-follow-begin-cancellation)
              (error "cancellation child did not reach its crash point"))))
    (let ((*package* (find-package :cl-user))
          (*print-length* nil) (*print-level* nil) (*print-readably* t))
      (append (butlast arguments 2) (list "--eval" (write-to-string form))))))

(deftest reseed-cancellation-recovers-sigkill-cuts-in-fresh-images
    (with-physical-store-directory (cache-root)
      (let ((cache (uiop:ensure-directory-pathname
                    (sb-posix:mkdtemp (namestring (merge-pathnames "cancel-cache.XXXXXX" cache-root))))))
        (dolist (cut '(:before-follow-temporary-create :after-follow-temporary-create
                       :after-follow-write-chunk :after-follow-temporary-fsync
                       :before-follow-rename :after-follow-rename
                       :after-follow-directory-fsync :after-follow-readback))
          (call-with-reseed-follow-installed-fixture
           (lambda (root stage incoming quarantine plan)
             (declare (ignore stage incoming quarantine plan))
             (let ((pending nil)
                   (marker (merge-pathnames
                            (format nil "cancel-~(~a~).marker" cut) cache-root))
                   (log (merge-pathnames
                         (format nil "cancellation-recovery-~(~a~).log" cut) cache-root)))
               (pg::call-with-physical-reseed-follow-configuration-owner
                root
                (lambda (owner)
                  (let ((record (reseed-follow-intent-test-record
                                 (pg::physical-reseed-follow-initial-record owner) root)))
                    (pg::physical-reseed-follow-publish-intent owner record)
                    (setf pending (pg::physical-reseed-follow-cancellation-begin-record record)))))
               (let ((pid (run-retirement-intent-test-crash-child
                           root cut cache marker :timeout 1200
                           :argument-builder #'cancellation-test-crash-child-arguments)))
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
                              "(format t \"~%CANCELLATION-RECOVERY-CHILD ~~d ~~s~%\" (sb-posix:getpid) '~s)
                             (finish-output)
                             (assert (handler-case
                                         (progn (pg::physical-reseed-check-before-pid) nil)
                                       (error () t)))
                             (pg::call-with-physical-reseed-follow-owner #P~s
                               (lambda (owner) ~a))
                             (assert (handler-case
                                         (progn (pg::physical-reseed-check-before-pid) nil)
                                       (error () t)))
                             (format t \"~%CANCELLATION-RECOVERY-VERIFIED ~~d ~~s~%\" (sb-posix:getpid) '~s)
                             (finish-output)"
                              cut (namestring root)
                              (cond
                                (partial
                                 "(assert (handler-case
                                            (progn (pg::physical-reseed-follow-recover-cancellation owner) nil)
                                          (error () t)))")
                                ((eq cut :before-follow-temporary-create)
                                 (format nil "(assert (equal '~a (pg::physical-reseed-follow-begin-cancellation owner)))"
                                         (pg::physical-bootstrap-manifest-text pending)))
                                (t
                                 (format nil "(assert (equal '~a (pg::physical-reseed-follow-recover-cancellation owner)))"
                                         (pg::physical-bootstrap-manifest-text pending))))
                              cut)
                      cache)
                   (write-string (uiop:read-file-string log))
                   (format t "~%CANCELLATION-RECOVERY-VERIFIED-CHILD ~d ~s ~s~%" pid status cut)
                   (finish-output)
                   (ok (plusp pid))
                   (ok (equal '(:exited 0) status))
                   (ok (search (format nil "CANCELLATION-RECOVERY-CHILD ~d ~s" pid cut)
                               (uiop:read-file-string log)))
                   (ok (search (format nil "CANCELLATION-RECOVERY-VERIFIED ~d ~s" pid cut)
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

(deftest reseed-cancellation-authority-rejects-noncanonical-constructor-bytes
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (let ((checks 0) (calls 0)
                        (journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
         (unwind-protect
              (pg::call-with-physical-directory-root
               owner
               (lambda (root-fd directory)
                 (declare (ignore directory))
                 (let* ((candidate (pg::physical-reseed-follow-cancellation-begin-record record))
                        (previous (pg::physical-reseed-follow-snapshot-at root-fd))
                        (bytes (pg::physical-reseed-follow-image candidate)))
                   (setf (aref bytes 0) (logxor 1 (aref bytes 0)))
                   (ok (equal "cancellation authority requires its canonical pending image"
                              (handler-case
                                  (pg::call-with-physical-reseed-follow-cancellation-authority
                                   owner root-fd candidate previous bytes :write
                                   (lambda () (incf checks))
                                   (lambda (check) (declare (ignore check)) (incf calls)))
                                (error (condition) (princ-to-string condition))))))))
           (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))
         (ok (zerop checks))
         (ok (zerop calls))
         (ok (equalp journal (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))
         (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))))))

(deftest reseed-cancellation-initial-proof-cannot-claim-or-remint-authority
    (call-with-reseed-cancellation-test-fixture
     (lambda (owner root record)
       (let* ((checks 0) (nested-effects 0) (nested nil)
                         (claim-refusal nil) (mint-refusal nil)
                         (candidate (pg::physical-reseed-follow-cancellation-begin-record record))
                         (pg::*physical-reseed-follow-test-hook*
                           (lambda (event path)
                             (declare (ignore path))
                             (when (and nested (eq event :before-follow-temporary-create))
                               (incf nested-effects)))))
         (setf (pg::physical-reseed-follow-owner-busy-p owner) t)
         (unwind-protect
              (pg::call-with-physical-directory-root
               owner
               (lambda (root-fd directory)
                 (declare (ignore directory))
                 (let* ((previous (pg::physical-reseed-follow-snapshot-at root-fd))
                        (bytes (pg::physical-reseed-follow-image candidate)))
                   (ok (equal candidate
                              (handler-case
                                  (pg::call-with-physical-reseed-follow-cancellation-authority
                                   owner root-fd candidate previous bytes :write
                                   (lambda ()
                                     (when (zerop checks)
                                       (incf checks)
                                       (setf nested t)
                                       (unwind-protect
                                            (progn
                                              (setf claim-refusal
                                                    (handler-case
                                                        (pg::physical-reseed-follow-write-intent
                                                         owner root-fd candidate bytes previous #'values)
                                                      (error (condition) (princ-to-string condition))))
                                              (setf mint-refusal
                                                    (handler-case
                                                        (pg::call-with-physical-reseed-follow-cancellation-authority
                                                         owner root-fd candidate previous bytes :write #'values #'values)
                                                      (error (condition) (princ-to-string condition)))))
                                         (setf nested nil))))
                                   (lambda (verify)
                                     (pg::physical-reseed-follow-write-intent
                                      owner root-fd candidate bytes previous verify)))
                                (error (condition) (princ-to-string condition))))))))
           (setf (pg::physical-reseed-follow-owner-busy-p owner) nil))
         (ok (= 1 checks))
         (ok (equal "cancellation write authority was reused or changed" claim-refusal))
         (ok (equal "cancellation authority cannot reenter acquisition or publication" mint-refusal))
         (ok (zerop nested-effects))
         (ok (equal candidate
                    (pg::physical-reseed-follow-decode
                     (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))))
         (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))))

(deftest reseed-cancellation-self-interrupt-cuts-reopen-without-losing-predecessors
    (dolist (cut '(:before-follow-temporary-create :after-follow-temporary-create
                   :after-follow-write-chunk :before-follow-temporary-fsync
                   :after-follow-temporary-fsync :before-follow-temporary-readback
                   :after-follow-temporary-readback :before-follow-rename
                   :after-follow-rename :after-follow-directory-fsync
                   :before-follow-readback :after-follow-readback))
      (check-reseed-cancellation-publication-cut cut :interrupt)))

(deftest reseed-cancellation-publishes-and-reopens-all-five-proof-geometries
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (call-with-reseed-follow-installed-fixture
       (lambda (root stage incoming quarantine plan)
         (declare (ignore stage incoming quarantine plan))
         (let ((pending nil))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (let* ((initial (pg::physical-reseed-follow-initial-record owner))
                     (proved (reseed-follow-proof-test-record initial root kind))
                     (preparing (pg::physical-reseed-follow-copy proved))
                     (generation (first (getf preparing :generations))))
                (setf (getf generation :phase) :preparing
                      (getf generation :anchor) nil)
                (pg::physical-reseed-follow-publish-intent owner preparing)
                (call-with-reseed-follow-prepare-source
                 owner root preparing
                 (lambda (state)
                   (ok (equal proved (pg::physical-reseed-follow-prepare owner :helper "/unused/helper")))
                   (ok (= 1 (getf state :connections) (getf state :prepares) (getf state :closes)))
                   (setf pending (pg::physical-reseed-follow-cancellation-begin-record proved))
                   (ok (equal pending (pg::physical-reseed-follow-begin-cancellation owner)))
                   (ok (equal pending (pg::physical-reseed-follow-begin-cancellation owner)))
                   (ok (= 1 (getf state :connections) (getf state :prepares) (getf state :closes)))
                   (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner))))
                 kind))))
           (pg::call-with-physical-reseed-follow-configuration-owner
            root
            (lambda (owner)
              (let* ((path (merge-pathnames "physical-reseed.follow" root))
                     (identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat path))))
                (ok (equal pending (pg::physical-reseed-follow-recover-cancellation owner)))
                (ok (equal identity (pg::physical-reseed-follow-file-identity (sb-posix:lstat path))))
                (ok (equalp (pg::physical-reseed-follow-image pending) (lineage-test-bytes path)))
                (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
                (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))))))))

(deftest reseed-cancellation-publishes-an-empty-follow-genesis
    (call-with-reseed-follow-installed-fixture
     (lambda (root stage incoming quarantine plan)
       (declare (ignore stage incoming quarantine plan))
       (let ((pending nil) (identity nil))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root
          (lambda (owner)
            (let ((record (pg::physical-reseed-follow-initial-record owner)))
              (pg::physical-reseed-follow-publish-intent owner record)
              (setf pending (pg::physical-reseed-follow-cancellation-begin-record record))
              (ok (null (getf pending :generations)))
              (ok (equal pending (pg::physical-reseed-follow-begin-cancellation owner)))
              (setf identity (pg::physical-reseed-follow-file-identity
                              (sb-posix:lstat (merge-pathnames "physical-reseed.follow" root))))
              (ok (equal pending (pg::physical-reseed-follow-recover-cancellation owner)))
              (ok (not (pg::physical-reseed-follow-owner-uncertain-p owner)))
              (ok (not (pg::physical-reseed-follow-owner-busy-p owner))))))
         (pg::call-with-physical-reseed-follow-configuration-owner
          root
          (lambda (owner)
            (ok (equal pending (pg::physical-reseed-follow-recover-cancellation owner)))
            (ok (equal identity
                       (pg::physical-reseed-follow-file-identity
                        (sb-posix:lstat (merge-pathnames "physical-reseed.follow" root)))))
            (ok (not (probe-file (merge-pathnames "physical-reseed.follow.tmp" root))))))))))
