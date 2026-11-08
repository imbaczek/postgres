(in-package #:pg-physical-tests)

(defun check-reseed-directory-flock-loss (fd directory check)
  (funcall check)
  (ok (pg::physical-file-holds-exclusive-flock-p fd))
  (let ((contender nil) (failure nil))
    (unwind-protect
         (progn
           (ok (zerop (pg::physical-store-flock fd 8)))
           (sb-sys:without-interrupts
             (setf contender (pg::physical-backup-open-directory directory)))
           (ok (= (sb-posix:stat-ino (sb-posix:fstat fd))
                  (sb-posix:stat-ino (sb-posix:fstat contender))))
           (ok (zerop (pg::physical-store-flock contender 6)))
           (ok (not (pg::physical-file-holds-exclusive-flock-p fd)))
           (handler-case (funcall check)
             (error (condition) (setf failure condition))))
      (when contender (sb-posix:close contender))
      (unless (zerop (pg::physical-store-flock fd 6))
        (error "cannot restore the test-owned directory lock")))
    (ok (and failure (search "exclusive lock" (princ-to-string failure))))
    (ok (pg::physical-file-holds-exclusive-flock-p fd))
    (ok (signals (funcall check) 'error))
    failure))

(deftest reseed-directory-scopes-permanently-refuse-lost-flocks
    (testing "installation auxiliary"
             (call-with-reseed-auxiliary-fixture
              (lambda (root owner auxiliary)
                (declare (ignore root))
                (let ((saved nil))
                  (ok (signals
                       (pg::call-with-physical-reseed-install-auxiliary
                        owner auxiliary
                        (lambda (directory facts fd check)
                          (setf saved facts)
                          (let ((failure (check-reseed-directory-flock-loss fd directory check)))
                            (when failure (error failure))))
                        :prepare t :empty t)
                       'error))
                  (pg::call-with-physical-reseed-install-auxiliary
                   owner auxiliary
                   (lambda (directory facts fd check)
                     (declare (ignore directory fd))
                     (ok (equal saved facts))
                     (ok (funcall check)))
                   :expected saved)))))
  (testing "retirement workspace"
           (call-with-reseed-retirement-observation-fixture
            :same-timeline
            (lambda (owner root workspace root-fd snapshot check)
              (declare (ignore owner root workspace))
              (let* ((configuration (getf (getf snapshot :record) :configuration))
                     (directories (getf configuration :directories))
                     (expected (append (getf configuration :workspace)
                                       (list :lock-inode (getf directories :lock-inode)
                                             :fence-inode (getf directories :fence-inode)))))
                (ok (signals
                     (pg::call-with-physical-reseed-follow-retirement-directory
                      root-fd expected
                      (lambda (fd verify)
                        (let ((failure
                                (check-reseed-directory-flock-loss
                                 fd (pathname (getf expected :directory)) verify)))
                          (when failure (error failure))))
                      check)
                     'error))
                (pg::call-with-physical-reseed-follow-retirement-directory
                 root-fd expected
                 (lambda (fd verify)
                   (declare (ignore fd))
                   (funcall verify)
                   (ok t))
                 check))))))

(deftest reseed-workspace-verifier-rechecks-nested-revocation
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore root snapshot))
       (let ((original (symbol-function 'pg::physical-file-holds-exclusive-flock-p))
             (calls 0) (nested-failure nil))
         (ok (signals
              (pg::call-with-physical-reseed-follow-workspace
               owner root-fd workspace
               (lambda (facts fd verify)
                 (declare (ignore facts))
                 (unwind-protect
                      (progn
                        (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p)
                              (lambda (borrowed-fd)
                                (if (/= borrowed-fd fd)
                                    (funcall original borrowed-fd)
                                    (progn
                                      (incf calls)
                                      (when (= calls 1)
                                        (handler-case (funcall verify)
                                          (error (condition) (setf nested-failure condition)))
                                        t)))))
                        (ok (signals (funcall verify) 'error)))
                   (setf (symbol-function 'pg::physical-file-holds-exclusive-flock-p) original))
                 (ok (= calls 2))
                 (ok (and nested-failure
                          (search "exclusive lock" (princ-to-string nested-failure))))
                 (ok (funcall original fd))
                 (ok (signals (funcall verify) 'error))
                 (when nested-failure (error nested-failure)))
               check)
              'error))
         (ok (eq :healthy
                 (pg::call-with-physical-reseed-follow-workspace
                  owner root-fd workspace
                  (lambda (facts fd verify)
                    (declare (ignore facts fd))
                    (funcall verify)
                    :healthy)
                  check)))))))

(defun check-reseed-receive-flock-loss (store check)
  (funcall check)
  (let* ((stream (pg::physical-store-lock-stream store))
         (fd (sb-sys:fd-stream-fd stream))
         (path (pg::physical-store-path store "receive.lock"))
         (contender nil) (failure nil))
    (ok (pg::physical-file-holds-exclusive-flock-p fd))
    (unwind-protect
         (progn
           (ok (zerop (pg::physical-store-flock fd 8)))
           (sb-sys:without-interrupts
             (setf contender (open path :direction :input)))
           (ok (= (sb-posix:stat-ino (sb-posix:fstat fd))
                  (sb-posix:stat-ino (sb-posix:fstat (sb-sys:fd-stream-fd contender)))))
           (ok (zerop (pg::physical-store-flock (sb-sys:fd-stream-fd contender) 6)))
           (ok (not (pg::physical-file-holds-exclusive-flock-p fd)))
           (handler-case (funcall check)
             (error (condition) (setf failure condition))))
      (when contender (close contender))
      (unless (zerop (pg::physical-store-flock fd 6))
        (error "cannot restore the test-owned receive lock")))
    (ok (and failure (search "exclusive lock" (princ-to-string failure))))
    (ok (pg::physical-file-holds-exclusive-flock-p fd))
    (ok (signals (funcall check) 'error))
    (ok (pg::physical-store-failed-p store))
    failure))

(deftest reseed-activation-verifier-refuses-lost-receive-lock
    (call-with-reseed-activation-fixture
     :same-timeline
     (lambda (owner root workspace settled)
       (declare (ignore workspace settled))
       (let ((before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (signals
              (pg::call-with-physical-reseed-follow-activation-operation
               owner
               (lambda (fd verify)
                 (declare (ignore fd))
                 (let* ((context (pg::physical-reseed-follow-activation-context owner))
                        (store (pg::physical-reseed-follow-activation-context-store context))
                        (failure (check-reseed-receive-flock-loss store verify)))
                   (when failure (error failure))
                   (values (pg::physical-reseed-follow-activation-context-record context)
                           (pg::physical-reseed-follow-activation-context-snapshot context)))))
              'error))
         (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
         (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))
     (lambda (root workspace)
       (declare (ignore workspace))
       (pg::call-with-physical-reseed-follow-configuration-owner
        root
        (lambda (owner)
          (pg::call-with-physical-reseed-follow-activation-operation
           owner
           (lambda (fd verify)
             (declare (ignore fd))
             (ok (funcall verify))
             (let ((context (pg::physical-reseed-follow-activation-context owner)))
               (values (pg::physical-reseed-follow-activation-context-record context)
                       (pg::physical-reseed-follow-activation-context-snapshot context))))))))))

(deftest reseed-retirement-verifiers-refuse-lost-receive-lock
    (call-with-reseed-retirement-observation-fixture
     :same-timeline
     (lambda (owner root workspace root-fd snapshot check)
       (declare (ignore owner workspace))
       (let ((record (getf snapshot :record))
             (before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (dolist (kind '(:scope :phase))
           (testing (symbol-name kind)
                    (ok (signals
                         (pg::call-with-physical-reseed-follow-retirement-resources
                          root-fd record check
                          (lambda (descriptors store base binding verify)
                            (flet ((phase-check ()
                                     (pg::physical-reseed-follow-retirement-check-receive
                                      descriptors store record base binding check)))
                              (let ((failure
                                      (check-reseed-receive-flock-loss
                                       store (if (eq kind :scope) verify #'phase-check))))
                                (when failure (error failure))))))
                         'error))
                    (pg::call-with-physical-reseed-follow-retirement-resources
                     root-fd record check
                     (lambda (descriptors store base binding verify)
                       (ok (funcall verify))
                       (ok (pg::physical-reseed-follow-retirement-check-receive
                            descriptors store record base binding check))))
                    (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))))

(deftest reseed-activation-closes-store-on-interrupted-open-handoff
    (dolist (kind '(:error :throw :interrupt))
      (testing (symbol-name kind)
               (call-with-reseed-activation-fixture
                :same-timeline
                (lambda (owner root workspace settled)
                  (declare (ignore workspace settled))
                  (let ((original (symbol-function 'pg::physical-store-open))
                        (opened nil) (directory nil) (calls 0) (entered nil)
                        (tag (gensym "ACTIVATION-STORE-HANDOFF"))
                        (before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                    (unwind-protect
                         (progn
                           (setf (symbol-function 'pg::physical-store-open)
                                 (lambda (&rest arguments)
                                   (setf directory (truename (first arguments))
                                         opened (apply original arguments))
                                   (incf calls)
                                   (case kind
                                     (:error (error "injected activation store handoff"))
                                     (:throw (throw tag :cut))
                                     (:interrupt
                                      (sb-thread:interrupt-thread
                                       sb-thread:*current-thread*
                                       (lambda () (error "injected activation store handoff")))))
                                   (error "activation handoff injection did not exit")))
                           (ok (eq :cut
                                   (catch tag
                                     (handler-case
                                         (pg::call-with-physical-reseed-follow-activation-operation
                                          owner
                                          (lambda (fd verify)
                                            (declare (ignore fd verify))
                                            (setf entered t)))
                                       (error (condition)
                                         (when (search "injected activation store handoff"
                                                       (princ-to-string condition))
                                           :cut))))))
                           (setf (symbol-function 'pg::physical-store-open) original)
                           (ok (= calls 1))
                           (ok (not entered))
                           (ok (and opened (pg::physical-store-closed-p opened)))
                           (ok (and opened (not (open-stream-p (pg::physical-store-lock-stream opened)))))
                           (ok (pg::physical-reseed-follow-owner-uncertain-p owner))
                           (ok (equalp before (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
                           (ok (handler-case
                                   (let ((fresh
                                           (funcall original directory
                                                    (pg::physical-store-system-id opened)
                                                    (pg::physical-store-timeline opened)
                                                    :start-lsn (pg::physical-store-base-lsn opened)
                                                    :discard-tail nil :create-lock nil
                                                    :retained-identity (pg::physical-store-retained-identity opened))))
                                     (unwind-protect (pg::physical-store-check-held-lock fresh)
                                       (pg::physical-store-close fresh)))
                                 (error () nil))))
                      (setf (symbol-function 'pg::physical-store-open) original)
                      (when opened (pg::physical-store-close opened)))))))))
