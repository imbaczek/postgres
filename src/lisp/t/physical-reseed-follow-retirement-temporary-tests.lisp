(in-package #:pg-physical-tests)

(defun reseed-retirement-test-initial-candidate (root-fd snapshot)
  (let* ((record (getf snapshot :record))
         (attempt (pg::physical-reseed-follow-retirement-attempt-at root-fd root-fd record 0)))
    (pg::physical-reseed-follow-retirement-next-record
     record (list :identity (getf attempt :identity) :digest (getf attempt :digest)))))

(deftest reseed-retirement-temporary-observation-holds-next-record-and-revokes-check
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore owner))
       (let* ((candidate (reseed-retirement-test-initial-candidate root-fd snapshot))
              (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
              (directories (list root workspace))
              (escaped nil))
         (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image candidate))
         (let ((before (mapcar #'reseed-mapped-test-tree-facts directories)))
           (ok (equal '(:first :second)
                      (multiple-value-list
                       (funcall (symbol-function 'pg::call-with-physical-reseed-follow-retirement-temporary-observation)
                                root-fd snapshot check
                                (lambda (descriptors state verify)
                                  (declare (ignore descriptors))
                                  (setf escaped verify)
                                  (ok (equal candidate (getf state :temporary)))
                                  (ok (= 0 (getf state :prefix)))
                                  (ok (funcall verify))
                                  (ok (sb-thread:join-thread
                                       (sb-thread:make-thread
                                        (lambda ()
                                          (handler-case (progn (funcall verify) nil)
                                            (error (condition)
                                              (search "changed thread" (princ-to-string condition))))))))
                                  (values :first :second))))))
           (ok (handler-case (progn (funcall escaped) nil)
                 (error (condition) (search "scope ended" (princ-to-string condition)))))
           (ok (reseed-activation-layout-facts-equal-p
                before (mapcar #'reseed-mapped-test-tree-facts directories))))))))

(defun check-reseed-retirement-temporary-prefixes (kind)
  (call-with-reseed-retirement-observation-fixture
   kind
   (lambda (owner root workspace root-fd snapshot check)
     (let* ((plan (pg::physical-reseed-install-owner-plan owner))
            (directories (list root workspace
                               (pathname (getf (getf plan :incoming) :directory))
                               (pathname (getf (getf plan :quarantine) :directory))))
            (record (pg::physical-reseed-follow-copy (getf snapshot :record)))
            (initial (reseed-retirement-test-initial-candidate root-fd snapshot))
            (reference (getf (getf initial :retirement) :attempt-file))
            (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
            (saved (pg::physical-backup-open-directory (merge-pathnames "saved/" workspace))))
       (unwind-protect
            (progn
              (loop for position from 0 to 3
                    for prefix = (min position 2)
                    for candidate = (pg::physical-reseed-follow-retirement-next-record record reference)
                    do (when (member position '(1 2))
                         (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image candidate))
                         (let ((before (mapcar #'reseed-mapped-test-tree-facts directories)))
                           (ok (handler-case
                                   (progn
                                     (pg::call-with-physical-reseed-follow-retirement-temporary-observation
                                      root-fd (pg::physical-reseed-follow-snapshot-at root-fd) check
                                      (lambda (descriptors state verify)
                                        (declare (ignore descriptors state verify))
                                        (error "out-of-order callback must stay unreachable")))
                                     nil)
                                 (error (condition)
                                   (search "exact next observed archive prefix" (princ-to-string condition)))))
                           (ok (reseed-activation-layout-facts-equal-p
                                before (mapcar #'reseed-mapped-test-tree-facts directories))))
                         (delete-file temporary)
                         (let ((name (if (= position 1) "physical-reseed.install" "physical-reseed.attempt")))
                           (unless (zerop (pg::physical-adoption-renameat2
                                           root-fd name saved name pg::+physical-adoption-rename-noreplace+))
                             (error "temporary observation fixture archive failed"))))
                       (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image candidate))
                       (let ((current (pg::physical-reseed-follow-snapshot-at root-fd))
                             (before (mapcar #'reseed-mapped-test-tree-facts directories))
                             (escaped nil))
                         (ok (eq :observed
                                 (pg::call-with-physical-reseed-follow-retirement-temporary-observation
                                  root-fd current check
                                  (lambda (descriptors state verify)
                                    (declare (ignore descriptors))
                                    (setf escaped verify)
                                    (ok (= prefix (getf state :prefix)))
                                    (ok (equal candidate (getf state :temporary)))
                                    (ok (funcall verify))
                                    (let ((observed (getf (getf state :temporary) :retirement)))
                                      (incf (seventh (getf (getf observed :attempt-file) :identity))))
                                    (ok (funcall verify))
                                    :observed))))
                         (ok (handler-case (progn (funcall escaped) nil)
                               (error (condition) (search "scope ended" (princ-to-string condition)))))
                         (ok (handler-case
                                 (progn
                                   (pg::call-with-physical-reseed-follow-retirement-observation
                                    root-fd current check
                                    (lambda (descriptors state verify)
                                      (declare (ignore descriptors state verify))
                                      (error "strict callback must stay unreachable")))
                                   nil)
                               (error (condition) (search "temporary follow" (princ-to-string condition)))))
                         (ok (reseed-activation-layout-facts-equal-p
                              before (mapcar #'reseed-mapped-test-tree-facts directories))))
                       (delete-file temporary)
                       (setf record candidate)
                       (reseed-retirement-layout-write root record))
              (let ((current (pg::physical-reseed-follow-snapshot-at root-fd)))
                (ok (eq :retired
                        (pg::call-with-physical-reseed-follow-retirement-temporary-observation
                         root-fd current check
                         (lambda (descriptors state verify)
                           (declare (ignore descriptors))
                           (ok (= 2 (getf state :prefix)))
                           (ok (null (getf state :temporary)))
                           (ok (funcall verify))
                           :retired))))
                (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image record))
                (ok (handler-case
                        (progn
                          (pg::call-with-physical-reseed-follow-retirement-temporary-observation
                           root-fd current check
                           (lambda (descriptors state verify)
                             (declare (ignore descriptors state verify))
                             (error "terminal callback must stay unreachable")))
                          nil)
                      (error (condition) (search "before Retired/2" (princ-to-string condition)))))))
         (sb-posix:close saved))))))

(deftest reseed-retirement-temporary-observation-checks-every-next-prefix-and-geometry
    (dolist (kind '(:same-timeline :retained-timeline :before-fork :at-fork :later-writer))
      (check-reseed-retirement-temporary-prefixes kind)))

(deftest reseed-retirement-temporary-observation-preserves-invalid-temporaries
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore owner))
       (let* ((record (getf snapshot :record))
              (candidate (reseed-retirement-test-initial-candidate root-fd snapshot))
              (reference (getf (getf candidate :retirement) :attempt-file))
              (bytes (pg::physical-reseed-follow-image candidate))
              (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
              (pid (merge-pathnames "postmaster.pid" root)))
         (dolist (kind '(:empty :partial :equal :skipped :reference :noncanonical :directory :symlink :pid))
           (let ((called nil))
             (unwind-protect
                  (progn
                    (case kind
                      (:directory (sb-posix:mkdir (namestring temporary) #o700))
                      (:symlink (sb-posix:symlink "physical-reseed.follow" (namestring temporary)))
                      (otherwise
                       (reseed-follow-owner-test-write
                        temporary
                        (case kind
                          (:empty (make-array 0 :element-type '(unsigned-byte 8)))
                          (:partial (subseq bytes 0 (1- (length bytes))))
                          (:equal (pg::physical-reseed-follow-image record))
                          (:skipped (pg::physical-reseed-follow-image
                                     (pg::physical-reseed-follow-retirement-next-record candidate reference)))
                          (:reference
                           (let ((other (pg::physical-reseed-follow-copy candidate)))
                             (incf (seventh (getf (getf (getf other :retirement) :attempt-file) :identity)))
                             (pg::physical-reseed-follow-image other)))
                          (:noncanonical
                           (pg::physical-backup-lease-encode-literal
                            "PGRESEEDFOLLOW1" (append (cddr candidate) (subseq candidate 0 2))
                            pg::+physical-reseed-follow-maximum-bytes+))
                          (otherwise bytes)))))
                    (when (eq kind :pid)
                      (reseed-follow-owner-test-write pid (make-array 1 :element-type '(unsigned-byte 8) :initial-element 10)))
                    (let ((before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))
                      (ok (handler-case
                              (progn
                                (pg::call-with-physical-reseed-follow-retirement-temporary-observation
                                 root-fd snapshot check
                                 (lambda (descriptors state verify)
                                   (declare (ignore descriptors state verify))
                                   (setf called t)))
                                nil)
                            (error () t)))
                      (ok (not called))
                      (ok (reseed-activation-layout-facts-equal-p
                           before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))))
               (when (probe-file pid) (delete-file pid))
               (if (eq kind :directory)
                   (sb-posix:rmdir (namestring temporary))
                   (delete-file temporary)))))))))

(defun reseed-retirement-test-rewrite-file (path bytes)
  (with-open-file (out path :direction :output :if-exists :overwrite
                            :if-does-not-exist :error :element-type '(unsigned-byte 8))
    (write-sequence bytes out)
    (pg::fsync-stream out)))

(deftest reseed-retirement-temporary-observation-fences-late-file-changes
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore owner))
       (let* ((candidate (reseed-retirement-test-initial-candidate root-fd snapshot))
              (bytes (pg::physical-reseed-follow-image candidate))
              (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
              (final (merge-pathnames "physical-reseed.follow" root))
              (replacement (merge-pathnames "replacement-journal" root))
              (original-proof (symbol-function 'pg::physical-reseed-follow-configuration-base-proof))
              (original-open (symbol-function 'pg::physical-image-openat)))
         (dolist (boundary '(:proof :callback))
           (dolist (kind '(:replace :rewrite :remove :appear :final-replace))
             (unless (eq kind :appear) (reseed-follow-owner-test-write temporary bytes))
             (let ((current (pg::physical-reseed-follow-snapshot-at root-fd))
                   (armed t) (injected nil) (called nil) (after nil) (message nil) (fds nil))
               (flet ((inject ()
                        (ecase kind
                          ((:replace :final-replace)
                           (let ((path (if (eq kind :final-replace) final temporary)))
                             (reseed-follow-owner-test-write replacement (lineage-test-bytes path))
                             (sb-posix:rename (namestring replacement) (namestring path))
                             (pg::fsync-directory root)))
                          (:rewrite (reseed-retirement-test-rewrite-file temporary bytes))
                          (:remove (delete-file temporary))
                          (:appear (reseed-follow-owner-test-write temporary bytes)))
                        (setf injected t
                              after (mapcar #'reseed-mapped-test-tree-facts (list root workspace)))))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::physical-image-openat)
                              (lambda (&rest arguments)
                                (let ((fd (apply original-open arguments)))
                                  (when (and (equal (second arguments) "physical-reseed.follow.tmp")
                                             (not (minusp fd)))
                                    (push fd fds))
                                  fd)))
                        (when (eq boundary :proof)
                          (setf (symbol-function 'pg::physical-reseed-follow-configuration-base-proof)
                                (lambda (&rest arguments)
                                  (multiple-value-prog1 (apply original-proof arguments)
                                    (when armed (setf armed nil) (inject))))))
                        (handler-case
                            (pg::call-with-physical-reseed-follow-retirement-temporary-observation
                             root-fd current check
                             (lambda (descriptors state verify)
                               (declare (ignore descriptors state verify))
                               (setf called t)
                               (when (eq boundary :callback) (inject))))
                          (error (condition) (setf message (princ-to-string condition)))))
                   (setf (symbol-function 'pg::physical-reseed-follow-configuration-base-proof) original-proof
                         (symbol-function 'pg::physical-image-openat) original-open))
                 (ok injected)
                 (ok (eq called (eq boundary :callback)))
                 (ok (and message
                          (search (cond ((and (eq boundary :callback) (eq kind :remove))
                                         "cannot inspect reseed old inventory entry")
                                        ((eq kind :replace) "single-link")
                                        ((eq kind :final-replace) "final generation")
                                        (t "temporary"))
                                  message))
                     (format nil "~a/~a rejects the injected journal change" boundary kind))
                 (ok (reseed-activation-layout-facts-equal-p
                      after (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))
                 (dolist (fd fds)
                   (ok (handler-case (progn (sb-posix:fstat fd) nil) (sb-posix:syscall-error () t))))
                 (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat root-fd)))))
               (when (probe-file temporary) (delete-file temporary))
               (reseed-follow-owner-test-write temporary bytes)
               (ok (eq :reopened
                       (pg::call-with-physical-reseed-follow-retirement-temporary-observation
                        root-fd (pg::physical-reseed-follow-snapshot-at root-fd) check
                        (lambda (descriptors state verify)
                          (declare (ignore descriptors state))
                          (funcall verify)
                          :reopened))))
               (delete-file temporary))))))))

(deftest reseed-retirement-temporary-observation-closes-held-files-on-every-exit
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore owner))
       (let* ((candidate (reseed-retirement-test-initial-candidate root-fd snapshot))
              (temporary (merge-pathnames "physical-reseed.follow.tmp" root))
              (original-open (symbol-function 'pg::physical-image-openat)))
         (reseed-follow-owner-test-write temporary (pg::physical-reseed-follow-image candidate))
         (dolist (boundary '(:open :callback))
           (dolist (mode '(:error :throw :interrupt))
             (let ((tag (gensym "RETIREMENT-TEMPORARY-EXIT"))
                   (injected nil) (called nil) (fd nil)
                   (before (mapcar #'reseed-mapped-test-tree-facts (list root workspace)))
                   (result nil))
               (flet ((exit-now ()
                        (setf injected t)
                        (ecase mode
                          (:error (error "injected temporary callback error"))
                          (:throw (throw tag :thrown))
                          (:interrupt
                           (sb-thread:interrupt-thread
                            sb-thread:*current-thread*
                            (lambda () (error "injected temporary interrupt")))))))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::physical-image-openat)
                              (lambda (&rest arguments)
                                (let ((opened (apply original-open arguments)))
                                  (when (and (equal (second arguments) "physical-reseed.follow.tmp")
                                             (not (minusp opened)))
                                    (setf fd opened))
                                  opened)))
                        (let ((pg::*physical-reseed-follow-test-hook*
                                (lambda (point path)
                                  (declare (ignore path))
                                  (when (and (eq boundary :open)
                                             (eq point :after-retirement-temporary-open))
                                    (exit-now)))))
                          (setf result
                                (handler-case
                                    (catch tag
                                      (pg::call-with-physical-reseed-follow-retirement-temporary-observation
                                       root-fd snapshot check
                                       (lambda (descriptors state verify)
                                         (declare (ignore descriptors state verify))
                                         (setf called t)
                                         (when (eq boundary :callback) (exit-now))))
                                      :returned)
                                  (error (condition) (princ-to-string condition))))))
                   (setf (symbol-function 'pg::physical-image-openat) original-open)))
               (ok injected)
               (ok (if (eq mode :throw) (eq result :thrown)
                       (and (stringp result) (search "injected temporary" result))))
               (ok (eq called (eq boundary :callback)))
               (ok (and fd (handler-case (progn (sb-posix:fstat fd) nil)
                             (sb-posix:syscall-error () t))))
               (ok (sb-posix:s-isdir (sb-posix:stat-mode (sb-posix:fstat root-fd))))
               (ok (reseed-activation-layout-facts-equal-p
                    before (mapcar #'reseed-mapped-test-tree-facts (list root workspace))))
               (ok (eq :reopened
                       (pg::call-with-physical-reseed-follow-retirement-temporary-observation
                        root-fd snapshot check
                        (lambda (descriptors state verify)
                          (declare (ignore descriptors state))
                          (funcall verify)
                          :reopened)))))))))))
