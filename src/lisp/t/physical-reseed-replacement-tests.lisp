(in-package #:pg-physical-tests)

(cl-spec:defgenerator replacement-request-arguments ()
  (let* ((old (backup-lease-test-request))
         (new (pg::physical-backup-lease-request-copy old))
         (mode (random 5)))
    (setf (getf new :name) (string-downcase (format nil "reseed_~32,'0x" (1+ (random 65536))))
          (getf new :token) (string-downcase (format nil "~64,'0x" (random 65536)))
          (getf new :timeline) (1+ (random 4294967295)))
    (case mode
      (1 (setf (getf new :name) (copy-seq (getf old :name))))
      (2 (setf (getf new :token) (copy-seq (getf old :token))))
      (3 (setf (getf new :cluster-identity) "another-replacement-cluster"
               (getf new :system-id)
               (pg::physical-store-update-digest
                #xCBF29CE484222325 (pg::string->bytes (getf new :cluster-identity))))))
    (list old new)))

(cl-spec:defspec-function pg::physical-reseed-replacement-request-copy
    (:args (old (type list)) (new (type list)))
  (:args-generator replacement-request-arguments)
  (:capture
   (allowed (and (= (getf old :system-id) (getf new :system-id))
                 (string= (getf old :cluster-identity) (getf new :cluster-identity))
                 (not (string= (getf old :name) (getf new :name)))
                 (not (string= (getf old :token) (getf new :token)))))
   (old-before (pg::physical-bootstrap-manifest-text old))
   (new-before (pg::physical-bootstrap-manifest-text new)))
  (:cases
   (:fresh-same-cluster
    (:when allowed)
    (:returns (type list))
    (:post (equal result new)
           (not (eq result new))
           (equal old-before (pg::physical-bootstrap-manifest-text old))
           (equal new-before (pg::physical-bootstrap-manifest-text new))))
   (:reused-or-foreign
    (:when (not allowed))
    (:signals (type error)))))

(cl-spec:defproperty replacement-request-copy-detaches-credentials
    ((value (range integer 0 65535)) (timeline (range integer 1 4294967295)))
  (:about pg::physical-reseed-replacement-request-copy)
  (:trials (:normal 80))
  (let* ((old (backup-lease-test-request))
         (new (pg::physical-backup-lease-request-copy old)))
    (setf (getf new :name) (string-downcase (format nil "reseed_~32,'0x" (1+ value)))
          (getf new :token) (string-downcase (format nil "~64,'0x" value))
          (getf new :timeline) timeline)
    (let* ((old-before (pg::physical-bootstrap-manifest-text old))
           (new-before (pg::physical-bootstrap-manifest-text new))
           (copy (pg::physical-reseed-replacement-request-copy old new)))
      (setf (char (getf copy :name) 0) #\x
            (char (getf copy :token) 0) #\f
            (char (getf copy :cluster-identity) 0) #\X)
      (and (equal old-before (pg::physical-bootstrap-manifest-text old))
           (equal new-before (pg::physical-bootstrap-manifest-text new))))))

(deftest generated-replacement-request-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:check-function 'pg::physical-reseed-replacement-request-copy
                                         :trials 80 :seed 233))))
      (ok (eq :passed (getf data :status))))
  (let ((data (cl-spec:result-data
               (cl-spec:run-property 'replacement-request-copy-detaches-credentials
                                     :seed 234 :profile :normal))))
    (ok (eq :passed (getf data :status)))))

(defun replacement-test-request (record)
  (let ((request (pg::physical-backup-lease-request-copy (getf (getf record :attempt) :request))))
    (setf (char (getf request :name) 7) (if (char= #\a (char (getf request :name) 7)) #\b #\a)
          (char (getf request :token) 0) (if (char= #\a (char (getf request :token) 0)) #\b #\a))
    request))

(defun call-with-replacement-capture-stub (record action function)
  (let* ((symbols '(pg::physical-reseed-discover-source pg::call-with-physical-reseed-client
                    pg::physical-backup-lease-capture-remote))
         (originals (mapcar #'symbol-function symbols))
         (state (list :discoveries 0 :clients 0 :captures 0))
         (lock (sb-thread:make-mutex :name "replacement test counters")))
    (flet ((bump (key) (sb-thread:with-mutex (lock) (incf (getf state key)))))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-reseed-discover-source)
                   (lambda (request profile helper)
                     (declare (ignore request profile helper))
                     (bump :discoveries)
                     (values (pg::physical-adoption-attempt-value-copy (getf (getf record :attempt) :source))
                             "/unused/helper"))
                   (symbol-function 'pg::call-with-physical-reseed-client)
                   (lambda (owner current callback &key helper capture)
                     (declare (ignore helper))
                     (assert capture)
                     (bump :clients)
                     (pg::physical-reseed-require-journal owner current)
                     (funcall callback nil))
                   (symbol-function 'pg::physical-backup-lease-capture-remote)
                   (lambda (directory client request &key directory-fd)
                     (declare (ignore client request))
                     (bump :captures)
                     (assert (integerp directory-fd))
                     (when action (funcall action directory state))
                     (values :received :test-receipt)))
             (funcall function state))
        (loop for symbol in symbols for original in originals
              do (setf (symbol-function symbol) original))))))

(defun call-with-replacement-configured-fixture (function)
  (call-with-reseed-inert-settlement-fixture
   :same-timeline
   (lambda (owner root workspace) (declare (ignore owner root workspace)))
   (lambda (root workspace)
     (declare (ignore workspace))
     (funcall function root))))

(defun replacement-test-current (root)
  (pg::call-with-physical-reseed-follow-cancellation-owner
   root #'pg::physical-reseed-follow-read-configured-cancellation))

(deftest replacement-directory-policy-rejects-overlap-and-mount-aliases
    (let ((old '("/protected/root/" 11 22 33)))
      (dolist (new '(("/protected/root/" 11 44 33)
                     ("/protected/root/child/" 11 44 33)
                     ("/protected/" 11 44 33)
                     ("/independent/" 11 22 33)
                     ("/independent/" 11 44 34)))
        (ok (not (pg::physical-reseed-replacement-directories-disjoint-p old new)))
        (ok (not (pg::physical-reseed-replacement-directories-disjoint-p new old))))
      (dolist (new '(("/protected/root-suffix/" 11 44 33)
                     ("/another-filesystem/" 12 22 34)))
        (ok (pg::physical-reseed-replacement-directories-disjoint-p old new))
        (ok (pg::physical-reseed-replacement-directories-disjoint-p new old)))))

(deftest replacement-stage-guard-runs-before-discovery-on-the-held-directory
    (call-with-reseed-remote-fixture
     (lambda (root stage owner request profile state)
       (declare (ignore root))
       (let ((checked nil))
         (ok (signals
              (pg::%physical-reseed-start-remote
               owner stage request profile :helper "/unused/helper"
               :stage-check
               (lambda (directory fd)
                 (setf checked t)
                 (ok (equal (truename stage) (truename directory)))
                 (ok (= (sb-posix:stat-ino (sb-posix:stat (namestring stage)))
                        (sb-posix:stat-ino (sb-posix:fstat fd))))
                 (error "reject before any source connection")))
              'error))
         (ok checked)
         (ok (zerop (getf state :discovery)))
         (ok (zerop (getf state :captures)))
         (ok (not (probe-file (pg::physical-reseed-attempt-path
                               (pg::physical-adoption-owner-directory owner)))))
         (ok (null (pg::directory-entry-names stage)))))))

(deftest replacement-requires-cancellation-and-fresh-same-cluster-identity
    (call-with-replacement-configured-fixture
     (lambda (root)
       (let* ((record (replacement-test-current root))
              (request (replacement-test-request record))
              (profile (pg::make-physical-libpq-profile "host=localhost" :mode :physical)))
         (call-with-reseed-owner-directory
          (lambda (target)
            (with-physical-store-directory (stage)
              (sb-posix:chmod (namestring stage) #o700)
              (call-with-replacement-capture-stub
               record nil
               (lambda (state)
                 (ok (signals (pg::physical-reseed-start-configured-replacement-remote
                               root target stage request profile :helper "/unused/helper") 'error))
                 (ok (zerop (getf state :discoveries))))))
            (pg::call-with-physical-reseed-follow-cancellation-owner
             root (lambda (owner) (pg::physical-reseed-follow-begin-cancellation owner)))
            (setf record (replacement-test-current root))
            (with-physical-store-directory (stage)
              (sb-posix:chmod (namestring stage) #o700)
              (call-with-replacement-capture-stub
               record nil
               (lambda (state)
                 (dolist (key '(:name :token))
                   (let ((changed (pg::physical-backup-lease-request-copy request)))
                     (setf (getf changed key) (copy-seq (getf (getf (getf record :attempt) :request) key)))
                     (ok (signals (pg::physical-reseed-start-configured-replacement-remote
                                   root target stage changed profile :helper "/unused/helper") 'error))))
                 (ok (zerop (getf state :discoveries)))
                 (ok (not (probe-file (pg::physical-reseed-attempt-path target)))))))))))))

(deftest replacement-capture-preserves-pending-history-and-refuses-a-second-start
    (call-with-replacement-configured-fixture
     (lambda (root)
       (pg::call-with-physical-reseed-follow-cancellation-owner
        root (lambda (owner) (pg::physical-reseed-follow-begin-cancellation owner)))
       (let* ((record (replacement-test-current root))
              (request (replacement-test-request record))
              (profile (pg::make-physical-libpq-profile "host=localhost" :mode :physical))
              (history (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
              (preserved (configured-cancellation-test-preserved-file-facts root)))
         (call-with-reseed-owner-directory
          (lambda (target)
            (with-physical-store-directory (stage)
              (sb-posix:chmod (namestring stage) #o700)
              (call-with-replacement-capture-stub
               record nil
               (lambda (state)
                 (multiple-value-bind (value receipt)
                     (pg::physical-reseed-start-configured-replacement-remote
                      root target stage request profile :helper "/unused/helper")
                   (ok (eq :received value))
                   (ok (eq :test-receipt receipt)))
                 (ok (= 1 (getf state :captures)))
                 (ok (= 1 (getf state :discoveries)))
                 (ok (signals (pg::physical-reseed-start-configured-replacement-remote
                               root target stage request profile :helper "/unused/helper") 'error))
                 (ok (= 1 (getf state :captures)))
                 (ok (= 1 (getf state :discoveries)))
                 (pg::call-with-physical-reseed-owner
                  target
                  (lambda (owner)
                    (ok (equal request (getf (pg::physical-reseed-attempt-read owner) :request))))))))))
         (ok (equal record (replacement-test-current root)))
         (ok (equalp history (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (ok (equalp preserved (configured-cancellation-test-preserved-file-facts root)))))))

(deftest replacement-capture-failure-keeps-both-independent-fences
    (call-with-replacement-configured-fixture
     (lambda (root)
       (pg::call-with-physical-reseed-follow-cancellation-owner
        root (lambda (owner) (pg::physical-reseed-follow-begin-cancellation owner)))
       (let* ((record (replacement-test-current root))
              (request (replacement-test-request record))
              (profile (pg::make-physical-libpq-profile "host=localhost" :mode :physical))
              (history (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
         (call-with-reseed-owner-directory
          (lambda (target)
            (with-physical-store-directory (stage)
              (sb-posix:chmod (namestring stage) #o700)
              (call-with-replacement-capture-stub
               record (lambda (directory state)
                        (declare (ignore directory state))
                        (throw 'replacement-stop :stopped))
               (lambda (state)
                 (ok (eq :stopped
                         (catch 'replacement-stop
                           (pg::physical-reseed-start-configured-replacement-remote
                            root target stage request profile :helper "/unused/helper"))))
                 (ok (= 1 (getf state :captures)))
                 (ok (probe-file (pg::physical-reseed-attempt-path target)))
                 (ok (signals (pg::physical-reseed-start-configured-replacement-remote
                               root target stage request profile :helper "/unused/helper") 'error))
                 (ok (= 1 (getf state :captures))))))))
         (ok (equal record (replacement-test-current root)))
         (ok (equalp history (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))))))

(deftest replacement-rechecks-old-history-at-capture-boundaries
    (call-with-replacement-configured-fixture
     (lambda (root)
       (pg::call-with-physical-reseed-follow-cancellation-owner
        root (lambda (owner) (pg::physical-reseed-follow-begin-cancellation owner)))
       (let* ((record (replacement-test-current root))
              (request (replacement-test-request record))
              (profile (pg::make-physical-libpq-profile "host=localhost" :mode :physical))
              (path (merge-pathnames "physical-reseed.follow" root))
              (history (lineage-test-bytes path)))
         (dolist (cut '(:after-discovery :after-journal :before-capture :after-capture))
           (call-with-reseed-owner-directory
            (lambda (target)
              (with-physical-store-directory (stage)
                (sb-posix:chmod (namestring stage) #o700)
                (call-with-replacement-capture-stub
                 record nil
                 (lambda (state)
                   (let* ((injected nil)
                          (pg::*physical-reseed-remote-test-hook*
                            (lambda (event)
                              (when (eq event cut)
                                (sb-posix:chmod (namestring path) #o640)
                                (setf injected t)))))
                     (unwind-protect
                          (progn
                            (ok (signals
                                 (pg::physical-reseed-start-configured-replacement-remote
                                  root target stage request profile :helper "/unused/helper")
                                 'error))
                            (ok injected)
                            (ok (= #o640 (logand #o7777 (sb-posix:stat-mode (sb-posix:stat path))))))
                       (sb-posix:chmod (namestring path) #o600))
                     (ok (= 1 (getf state :discoveries)))
                     (ok (= (if (eq cut :after-discovery) 0 1)
                            (getf state :clients)))
                     (ok (= (if (eq cut :after-capture) 1 0) (getf state :captures)))
                     (ok (eq (not (null (probe-file (pg::physical-reseed-attempt-path target))))
                             (not (eq cut :after-discovery))))))))))
           (ok (equal record (replacement-test-current root)))
           (ok (equalp history (lineage-test-bytes path))))))))

(deftest replacement-accepts-source-closed-and-cleaned-history
    (call-with-replacement-configured-fixture
     (lambda (root)
       (pg::call-with-physical-reseed-follow-cancellation-owner
        root
        (lambda (owner)
          (let* ((pending (pg::physical-reseed-follow-begin-cancellation owner))
                 (receipt (reseed-cancellation-source-parent pending :closed :handed-off t)))
            (pg::physical-reseed-follow-record-cancellation-source-closed owner receipt))))
       (dolist (phase '(:source-closed :cleaned))
         (when (eq phase :cleaned)
           (pg::call-with-physical-reseed-follow-cancellation-owner
            root
            (lambda (owner)
              (let* ((record (pg::physical-reseed-follow-read-configured-cancellation owner))
                     (receipt (reseed-cancellation-source-parent record :closed :handed-off t)))
                (call-with-reseed-cancellation-source
                 owner record receipt
                 (lambda (state)
                   (declare (ignore state))
                   (multiple-value-bind (actual result)
                       (pg::physical-reseed-follow-cleanup-cancellation owner :helper "/unused/helper")
                     (declare (ignore actual))
                     (ok (eq :complete result)))))))))
         (let* ((record (replacement-test-current root))
                (request (replacement-test-request record))
                (profile (pg::make-physical-libpq-profile "host=localhost" :mode :physical))
                (history (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root)))
                (preserved (configured-cancellation-test-preserved-file-facts root)))
           (ok (eq phase (getf (getf record :cancellation) :phase)))
           (call-with-reseed-owner-directory
            (lambda (target)
              (with-physical-store-directory (stage)
                (sb-posix:chmod (namestring stage) #o700)
                (call-with-replacement-capture-stub
                 record nil
                 (lambda (state)
                   (ok (eq :received
                           (pg::physical-reseed-start-configured-replacement-remote
                            root target stage request profile :helper "/unused/helper")))
                   (ok (= 1 (getf state :discoveries)))
                   (ok (= 1 (getf state :captures))))))))
           (ok (equal record (replacement-test-current root)))
           (ok (equalp history (lineage-test-bytes (merge-pathnames "physical-reseed.follow" root))))
           (ok (equalp preserved (configured-cancellation-test-preserved-file-facts root))))))))

(deftest replacement-refuses-invalid-targets-and-unrecovered-old-temporaries-before-discovery
    (call-with-replacement-configured-fixture
     (lambda (root)
       (pg::call-with-physical-reseed-follow-cancellation-owner
        root (lambda (owner) (pg::physical-reseed-follow-begin-cancellation owner)))
       (let* ((record (replacement-test-current root))
              (request (replacement-test-request record))
              (profile (pg::make-physical-libpq-profile "host=localhost" :mode :physical)))
         (call-with-reseed-owner-directory
          (lambda (target)
            (with-physical-store-directory (stage)
              (sb-posix:chmod (namestring stage) #o700)
              (call-with-replacement-capture-stub
               record nil
               (lambda (state)
                 (flet ((reject (receive &optional (supplied request))
                          (ok (signals
                               (pg::physical-reseed-start-configured-replacement-remote
                                root target receive supplied profile :helper "/unused/helper")
                               'error))
                          (ok (zerop (getf state :discoveries)))
                          (ok (zerop (getf state :captures)))
                          (ok (not (probe-file (pg::physical-reseed-attempt-path target))))))
                   (reject root)
                   (reject target)
                   (let ((foreign (pg::physical-backup-lease-request-copy request)))
                     (setf (getf foreign :cluster-identity) "foreign-replacement-cluster"
                           (getf foreign :system-id)
                           (pg::physical-store-update-digest
                            #xCBF29CE484222325
                            (pg::string->bytes (getf foreign :cluster-identity))))
                     (reject stage foreign))
                   (let ((lock (merge-pathnames ".pg_lisp.lock" target)))
                     (unwind-protect
                          (progn (sb-posix:chmod (namestring lock) #o640) (reject stage))
                       (sb-posix:chmod (namestring lock) #o600))
                     (unwind-protect
                          (progn
                            (with-open-file (out lock :direction :output :if-exists :overwrite)
                              (write-char #\x out))
                            (reject stage))
                       (with-open-file (out lock :direction :output :if-exists :supersede)
                         (declare (ignorable out)))
                       (sb-posix:chmod (namestring lock) #o600)))
                   (dolist (path (list (merge-pathnames "unexpected" target)
                                       (merge-pathnames "physical-reseed.follow.tmp" root)))
                     (unwind-protect
                          (progn
                            (with-open-file (out path :direction :output :if-exists :error)
                              (write-line "incomplete" out))
                            (sb-posix:chmod (namestring path) #o600)
                            (reject stage))
                       (when (probe-file path) (delete-file path))))))))))))))

(deftest replacement-concurrent-starts-capture-once
    (call-with-replacement-configured-fixture
     (lambda (root)
       (pg::call-with-physical-reseed-follow-cancellation-owner
        root (lambda (owner) (pg::physical-reseed-follow-begin-cancellation owner)))
       (let* ((record (replacement-test-current root))
              (request (replacement-test-request record))
              (profile (pg::make-physical-libpq-profile "host=localhost" :mode :physical))
              (entered (sb-thread:make-semaphore :count 0))
              (release (sb-thread:make-semaphore :count 0))
              (output *standard-output*)
              (status :not-started))
         (call-with-reseed-owner-directory
          (lambda (target)
            (with-physical-store-directory (stage)
              (sb-posix:chmod (namestring stage) #o700)
              (call-with-replacement-capture-stub
               record
               (lambda (directory state)
                 (declare (ignore directory state))
                 (sb-thread:signal-semaphore entered)
                 (unless (sb-thread:wait-on-semaphore release :timeout 60)
                   (error "replacement test capture release timed out")))
               (lambda (state)
                 (let ((thread
                         (sb-thread:make-thread
                          (lambda ()
                            (let ((*standard-output* output) (*error-output* output) (*trace-output* output))
                              (handler-case
                                  (setf status
                                        (pg::physical-reseed-start-configured-replacement-remote
                                         root target stage request profile :helper "/unused/helper"))
                                (error (condition) (setf status condition)))))
                          :name "first replacement capture")))
                   (unwind-protect
                        (progn
                          (ok (sb-thread:wait-on-semaphore entered :timeout 60))
                          (ok (signals
                               (pg::physical-reseed-start-configured-replacement-remote
                                root target stage request profile :helper "/unused/helper")
                               'error))
                          (ok (= 1 (getf state :discoveries)))
                          (ok (= 1 (getf state :captures))))
                     (sb-thread:signal-semaphore release)
                     (sb-thread:join-thread thread :timeout 90))
                   (ok (eq :received status))
                   (ok (not (sb-thread:thread-alive-p thread)))
                   (ok (= 1 (getf state :captures)))))))))
         (ok (equal record (replacement-test-current root)))))))

(deftest replacement-rechecks-target-lock-and-entries-at-capture-boundaries
    (call-with-replacement-configured-fixture
     (lambda (root)
       (pg::call-with-physical-reseed-follow-cancellation-owner
        root (lambda (owner) (pg::physical-reseed-follow-begin-cancellation owner)))
       (let* ((record (replacement-test-current root))
              (request (replacement-test-request record))
              (profile (pg::make-physical-libpq-profile "host=localhost" :mode :physical))
              (preserved (configured-cancellation-test-preserved-file-facts root)))
         (dolist (cut '(:after-discovery :before-capture :after-capture))
           (dolist (fault '(:mode :contents :extra))
             (call-with-reseed-owner-directory
              (lambda (target)
                (with-physical-store-directory (stage)
                  (sb-posix:chmod (namestring stage) #o700)
                  (call-with-replacement-capture-stub
                   record nil
                   (lambda (state)
                     (let* ((lock (merge-pathnames ".pg_lisp.lock" target))
                            (extra (merge-pathnames "unexpected" target))
                            (injected nil)
                            (pg::*physical-reseed-remote-test-hook*
                              (lambda (event)
                                (when (eq event cut)
                                  (ecase fault
                                    (:mode (sb-posix:chmod (namestring lock) #o640))
                                    (:contents
                                     (with-open-file (out lock :direction :output :if-exists :overwrite)
                                       (write-char #\x out)))
                                    (:extra
                                     (with-open-file (out extra :direction :output :if-exists :error)
                                       (write-char #\x out))))
                                  (setf injected t)))))
                       (unwind-protect
                            (progn
                              (ok (signals
                                   (pg::physical-reseed-start-configured-replacement-remote
                                    root target stage request profile :helper "/unused/helper")
                                   'error))
                              (ok injected)
                              (ok (ecase fault
                                    (:mode (= #o640 (logand #o7777 (sb-posix:stat-mode (sb-posix:stat lock)))))
                                    (:contents (= 1 (sb-posix:stat-size (sb-posix:stat lock))))
                                    (:extra (probe-file extra))))
                              (ok (= 1 (getf state :discoveries)))
                              (ok (= (if (eq cut :after-discovery) 0 1) (getf state :clients)))
                              (ok (= (if (eq cut :after-capture) 1 0) (getf state :captures))))
                         (when (probe-file extra) (delete-file extra))
                         (when (eq fault :contents)
                           (with-open-file (out lock :direction :output :if-exists :supersede)
                             (declare (ignorable out))))
                         (sb-posix:chmod (namestring lock) #o600))))))))))
         (ok (equal record (replacement-test-current root)))
         (ok (equalp preserved (configured-cancellation-test-preserved-file-facts root)))))))
