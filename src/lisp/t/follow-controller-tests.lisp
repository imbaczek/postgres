(in-package #:pg-physical-tests)

(defun follow-controller-test-view (fork)
  (let ((text (store-history-text 1 fork)))
    (list :parent 1 :source 2 :child 2 :fork fork :history-text text
          :child-history text
          :history (pg::timeline-check-lineage (pg::timeline-parse-history 2 text)))))

(defun follow-controller-test-advance (store)
  (let ((cursor (pg::physical-store-flushed-lsn store)))
    (dolist (raw (list (pg::wal-encode-record pg::+wal-record-batch-start+ 0 cursor (empty-octets))
                       (pg::wal-encode-record pg::+wal-record-commit+ 70 (+ cursor 32)
                                              (pg::wal-encode-commit-timestamp 42))))
      (pg::physical-store-input store cursor raw)
      (incf cursor (length raw)))
    (pg::physical-store-flush store)
    (setf pg::*last-replay-lsn* cursor pg::*next-xid* 71 pg::*xid-batch-end* 71
          (getf pg::*physical-recovery-state* :required-applied) cursor
          (getf pg::*physical-recovery-state* :required-timeline) 1
          (getf pg::*physical-recovery-state* :observed-next-xid) 71
          (getf pg::*physical-recovery-state* :source-reservation)
          (list :version 1 :through cursor :bound 1002 :complete t)
          pg::*physical-recovery-state*
          (pg::physical-recovery-write-control pg::*physical-recovery-state* 2 cursor 71))
    cursor))

(defun follow-controller-test-publish-after-restartpoint (root store)
  (declare (ignore root))
  (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
  (let* ((fork (follow-controller-test-advance store))
         (view (follow-controller-test-view fork))
         (operation (pg::make-physical-follow-operation :host "127.0.0.1" :port 5433 :prefix "test"))
         (pg::*physical-follow-operation* operation)
         (pg::*physical-follow-controller-blocked-p* nil)
         (pg::*physical-follow-controller-stopping-p* nil))
    (pg::physical-follow-save-config '(1 "127.0.0.1" 5432 "old_bootstrap"))
    (pg::physical-follow-operation-initial-config operation)
    (multiple-value-bind (entry snapshot) (pg::physical-follow-operation-intent operation view)
      (let ((anchor (copy-list (getf entry :request))))
        (ok (= 152 (sixth (getf snapshot :before))))
        (call-with-promotion-exclusion
         (lambda () (pg::physical-store-compact store fork #'pg::physical-restartpoint-publish)))
        (setf (getf anchor :history-text) (getf view :history-text)
              (getf anchor :eor-octets) (coerce (store-history-eor fork 2 1) 'list))
        (let* ((active (pg::physical-follow-operation-publish operation view entry anchor))
               (binding (fifth active))
               (control (pg::physical-recovery-read-control pg::*pgdata*)))
          (ok (eq :active (getf binding :phase)))
          (ok (= fork (sixth (getf binding :before))))
          (ok (= fork (first (getf (getf binding :recovery) :restartpoint))))
          (ok (= fork (getf control :applied)))
          (ok (= 71 (getf control :next-xid)))
          (ok (= 1 (pg::physical-recovery-replay-timeline)))
          (ok (= 2 (pg::physical-store-selected-timeline store)))
          (ok (equal "old_bootstrap" (getf (first (getf (sixth active) :cleanup)) :pin)))
          (ok (equal active (pg::physical-follow-read-config))))))))

(deftest follow-controller-refreshes-restartpoint-witness-after-source-preparation
    (call-with-promotion-fixture #'follow-controller-test-publish-after-restartpoint
                                 :applied 152 :system-id 42))

(deftest same-timeline-controller-publishes-without-selecting-or-rewinding-history
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (ok (= 42 (pg::physical-store-system-id store)))
       (ok (= 152 pg::*last-replay-lsn*))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((view (list :source 1 :parent 1 :history-text ""
                          :history (pg::timeline-parse-history 1 "")))
              (operation (pg::make-physical-follow-operation
                          :host "127.0.0.1" :port 5433 :prefix "same"
                          :context (list :requested-slot "next_public")))
              (pg::*physical-follow-operation* operation)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (before (pg::physical-store-control-tuple store)))
         (pg::physical-follow-save-config
          (pg::physical-follow-with-controller
           '(1 "127.0.0.1" 5432 "old_bootstrap")
           '(:version 1 :enabled t :preparing nil :cleanup nil :public-slot "old_public")))
         (pg::physical-follow-operation-initial-config operation)
         (multiple-value-bind (entry snapshot) (pg::physical-follow-operation-intent operation view)
           (declare (ignore snapshot))
           (ok (equal "old_public" (pg::physical-follow-config-public-slot (pg::physical-follow-read-config))))
           (let* ((anchor (append (copy-list (getf entry :request))
                                  (list :kind :same-timeline :history-text "")))
                  (active (pg::physical-follow-operation-publish operation view entry anchor))
                  (binding (fifth active)))
             (ok (equal "next_public" (pg::physical-follow-config-public-slot active)))
             (ok (equal before (getf binding :before)))
             (ok (equal before (getf binding :after)))
             (ok (equal before (pg::physical-store-control-tuple store)))
             (ok (null (pg::physical-store-selection store)))
             (ok (eq :active (getf binding :phase)))
             (ok (pg::physical-follow-check-active-state
                  store (pg::physical-recovery-verify-base-state) active))
             (follow-controller-test-advance store)
             (ok (pg::physical-follow-check-active-state
                  store (pg::physical-recovery-verify-base-state) active))))))
     :applied 152 :system-id 42))

(deftest follow-unattached-first-publication-preserves-unowned-backup
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (ok (= 42 (pg::physical-store-system-id store)))
       (ok (= 152 pg::*last-replay-lsn*))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((view (list :source 1 :parent 1 :history-text ""
                          :history (pg::timeline-parse-history 1 "")))
              (operation (pg::make-physical-follow-operation
                          :host "127.0.0.1" :port 5433 :prefix "same"))
              (pg::*physical-follow-operation* operation)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (before (pg::physical-store-control-tuple store)))
         (pg::physical-follow-save-config
          (pg::physical-follow-unattached-config-for-base
           (pg::physical-recovery-verify-base-state)))
         (pg::physical-follow-operation-initial-config operation)
         (multiple-value-bind (entry snapshot) (pg::physical-follow-operation-intent operation view)
           (declare (ignore snapshot))
           (let* ((anchor (append (copy-list (getf entry :request))
                                  (list :kind :same-timeline :history-text "")))
                  (active (pg::physical-follow-operation-publish operation view entry anchor))
                  (binding (fifth active))
                  (state (sixth active)))
             (ok (null (getf state :mode)))
             (ok (null (getf state :cleanup)))
             (ok (equal (getf state :unowned-bootstrap)
                        (pg::physical-follow-unowned-bootstrap-from-base
                         (pg::physical-recovery-verify-base-state))))
             (ok (equal before (getf binding :before)))
             (ok (equal before (getf binding :after)))
             (ok (equal before (pg::physical-store-control-tuple store)))
             (ok (null (pg::physical-store-selection store)))
             (ok (eq :active (getf binding :phase)))
             (ok (pg::physical-follow-check-active-state
                  store (pg::physical-recovery-verify-base-state) active))
             (follow-controller-test-advance store)
             (ok (pg::physical-follow-check-active-state
                  store (pg::physical-recovery-verify-base-state) active))))))
     :applied 152 :system-id 42))

(deftest follow-controller-promotion-preserves-bootstrap-retirement-before-manifest-deletion
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root store))
       (pg::physical-follow-save-config '(1 "127.0.0.1" 5432 "original_bootstrap"))
       (let* ((config (pg::physical-follow-promotion-cleanup-config))
              (entry (first (getf (sixth config) :cleanup))))
         (ok (pg::physical-follow-cleanup-only-p config))
         (ok (eq :bootstrap (getf entry :kind)))
         (ok (= 42 (getf (getf entry :request) :system-id)))
         (ok (= 152 (getf (getf entry :request) :base)))
         (pg::physical-follow-save-config config)
         (delete-file (merge-pathnames "physical-backup.manifest" pg::*pgdata*))
         (ok (equal config (pg::physical-follow-promotion-cleanup-config)))))
     :system-id 42))

(deftest follow-unattached-promotion-preserves-unowned-backup-without-routing-cleanup
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root store))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((base (pg::physical-recovery-verify-base-state))
              (unattached (pg::physical-follow-unattached-config-for-base base)))
         (pg::physical-follow-save-config unattached)
         (let ((config (pg::physical-follow-promotion-cleanup-config)))
           (ok (pg::physical-follow-cleanup-only-p config))
           (ok (null (getf (sixth config) :cleanup)))
           (ok (equal (getf (sixth config) :unowned-bootstrap)
                      (getf (sixth unattached) :unowned-bootstrap)))
           (pg::physical-follow-save-config config)
           (delete-file (merge-pathnames "physical-backup.manifest" pg::*pgdata*))
           (ok (equal config (pg::physical-follow-promotion-cleanup-config))))))
     :system-id 42))

(deftest follow-never-attached-promotion-preserves-unowned-backup
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root store))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((base (pg::physical-recovery-verify-base-state))
              (expected (pg::physical-follow-unowned-bootstrap-from-base base))
              (config (pg::physical-follow-promotion-cleanup-config)))
         (ok config)
         (when config
           (ok (pg::physical-follow-cleanup-only-p config))
           (ok (null (getf (sixth config) :cleanup)))
           (ok (equal expected (getf (sixth config) :unowned-bootstrap)))
           (pg::physical-follow-save-config config)
           (delete-file (merge-pathnames "physical-backup.manifest" pg::*pgdata*))
           (ok (equal config (pg::physical-follow-promotion-cleanup-config))))))
     :system-id 42))

(deftest follow-controller-stop-disables-active-binding-without-losing-cleanup
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((base (pg::physical-recovery-verify-base-state))
              (pending (pg::physical-follow-with-controller
                        (follow-pending-proof-config-for-store store base)
                        (follow-controller-test-state :queue (list (follow-controller-test-entry)))))
              (active (progn
                        (pg::physical-follow-save-config pending)
                        (call-with-promotion-exclusion
                         (lambda () (pg::physical-follow-reconcile-pending-store store base pending))))))
         (ok (eq :stopped (pg::physical-follow-stop)))
         (let ((disabled (pg::physical-follow-read-config)))
           (ok (equal (fifth active) (fifth disabled)))
           (ok (equal (getf (sixth active) :cleanup) (getf (sixth disabled) :cleanup)))
           (ok (not (pg::physical-follow-controller-enabled-p disabled)))
           (pg::physical-follow-autostart)
           (ok (null pg::*physical-follow-runtime*)))))
     :system-id 42))

(defun call-with-controller-intent (function &key before-intent)
  (call-with-promotion-fixture
   (lambda (root store)
     (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
     (let* ((view (follow-controller-test-view (pg::physical-store-flushed-lsn store)))
            (operation (pg::make-physical-follow-operation :host "127.0.0.1" :port 5433 :prefix "cut"))
            (pg::*physical-follow-operation* operation)
            (pg::*physical-follow-controller-blocked-p* nil)
            (pg::*physical-follow-controller-stopping-p* nil))
       (pg::physical-follow-save-config '(1 "127.0.0.1" 5432 "old_bootstrap"))
       (when before-intent (funcall before-intent operation))
       (pg::physical-follow-operation-initial-config operation)
       (multiple-value-bind (entry snapshot) (pg::physical-follow-operation-intent operation view)
         (funcall function root store operation view entry snapshot))))
   :system-id 42))

(deftest follow-controller-cancelled-preparation-preserves-old-source-and-queues-exact-abort
    (call-with-controller-intent
     (lambda (root store operation view entry snapshot)
       (declare (ignore root store view snapshot))
       (setf (pg::physical-follow-operation-cancelled-p operation) t)
       (pg::physical-follow-operation-recover operation)
       (let ((saved (pg::physical-follow-read-config)))
         (ok (equal '(1 "127.0.0.1" 5432 "old_bootstrap")
                    (pg::physical-follow-effective-config saved)))
         (ok (null (getf (sixth saved) :preparing)))
         (ok (equal (list entry) (getf (sixth saved) :cleanup)))
         (ok (null pg::*physical-follow-runtime*))
         (ok (not pg::*physical-recovery-failed-p*))))))

(deftest follow-failed-slot-replacement-restores-old-slot-and-private-cleanup-only
    (let ((original (symbol-function 'pg::physical-follow-launch))
          (resumed nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-follow-launch)
                   (lambda (config) (setf resumed config) :started))
             (call-with-controller-intent
              (lambda (root store operation view entry snapshot)
                (declare (ignore root store view snapshot))
                (ok (equal "public_a" (pg::physical-follow-config-public-slot (pg::physical-follow-read-config))))
                (pg::physical-follow-operation-recover operation)
                (let* ((saved (pg::physical-follow-read-config))
                       (runtime (pg::make-physical-follow-runtime :pin (fourth saved) :config resumed))
                       (command (pg::parse-replication-command (pg::physical-follow-start-query runtime 152))))
                  (ok (equal "public_a" (pg::physical-follow-config-public-slot saved)))
                  (ok (equal saved resumed))
                  (ok (equal "public_a" (pg::logical-replication-command-slot-name command)))
                  (ok (equal "old_bootstrap" (pg::physical-stream-proof-pin command)))
                  (ok (equal (list entry) (getf (sixth saved) :cleanup)))
                  (ok (null (getf entry :public-slot)))
                  (ok (null (getf (sixth saved) :preparing)))
                  (ok (not pg::*physical-recovery-failed-p*))))
              :before-intent
              (lambda (operation)
                (setf (pg::physical-follow-operation-context operation) (list :requested-slot "public_b"))
                (pg::physical-follow-save-config
                 (pg::physical-follow-with-controller
                  (pg::physical-follow-read-config)
                  '(:version 1 :enabled t :preparing nil :cleanup nil :public-slot "public_a"))))))
        (setf (symbol-function 'pg::physical-follow-launch) original))))

(deftest follow-controller-uncertain-pending-publication-fences-without-resuming-old-source
    (call-with-controller-intent
     (lambda (root store operation view entry snapshot)
       (declare (ignore root store snapshot))
       (let ((anchor (copy-list (getf entry :request)))
             (pg::*physical-follow-test-hook*
               (lambda (point) (when (eq point :after-controller-pending) (error "pending cut")))))
         (setf (getf anchor :history-text) (getf view :history-text)
               (getf anchor :eor-octets) (coerce (store-history-eor (getf view :fork) 2 1) 'list))
         (ok (signals (pg::physical-follow-operation-publish operation view entry anchor) 'error))
         (pg::physical-follow-operation-recover operation)
         (ok pg::*physical-recovery-failed-p*)
         (ok (eq :pending (getf (fifth (pg::physical-follow-read-config)) :phase)))
         (ok (null pg::*physical-follow-runtime*))))))

(deftest follow-controller-early-rejection-is-contained-before-config-capture
    (let* ((operation (pg::make-physical-follow-operation))
           (pg::*physical-follow-operation* operation)
           (pg::*physical-follow-controller-blocked-p* t)
           (pg::*physical-recovery-failed-p* nil))
      (pg::physical-follow-operation-run operation)
      (ok (typep (pg::physical-follow-operation-error operation) 'error))
      (ok (null pg::*physical-follow-operation*))
      (ok (not pg::*physical-recovery-failed-p*))))

(deftest follow-controller-full-cleanup-queue-does-not-stop-the-current-receiver
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root store))
       (let* ((runtime (pg::make-physical-follow-runtime))
              (pg::*physical-follow-runtime* runtime)
              (operation (pg::make-physical-follow-operation))
              (pg::*physical-follow-operation* operation)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (queue (loop for index below pg::+physical-follow-cleanup-capacity+
                           collect (follow-controller-test-entry (format nil "queued_~d" index))))
              (config (pg::physical-follow-with-controller
                       '(1 "127.0.0.1" 5432 "current_source")
                       (follow-controller-test-state :queue queue))))
         (pg::physical-follow-save-config config)
         (pg::physical-follow-operation-run operation)
         (ok (typep (pg::physical-follow-operation-error operation) 'error))
         (ok (eq runtime pg::*physical-follow-runtime*))
         (ok (not (pg::physical-follow-runtime-stopped-p runtime)))
         (ok (equal config (pg::physical-follow-read-config)))
         (ok (not pg::*physical-recovery-failed-p*))))
     :system-id 42))

(deftest follow-controller-rejects-rewritten-older-forks-before-parent-catchup
    (let* ((segment pg::+wal-segment-size+)
           (old-text (store-history-text 1 1024))
           (old-history (pg::timeline-check-lineage (pg::timeline-parse-history 2 old-text)))
           (source-text (store-history-text 2 (* 4 segment) old-text))
           (bad-text (store-history-text 2 (* 4 segment) (store-history-text 1 2048)))
           (source (pg::timeline-check-lineage (pg::timeline-parse-history 3 source-text)))
           (bad (pg::timeline-check-lineage (pg::timeline-parse-history 3 bad-text)))
           (store (pg::%make-physical-store
                   :system-id 42 :timeline 1 :base-lsn 64 :floor-lsn 64
                   :flushed-lsn (* 3 segment)
                   :selection (list :target 2 :history-text old-text :history old-history))))
      (ok (pg::timeline-prefix-boundary-p 2 (* 3 segment) bad))
      (ok (pg::timeline-prefix-boundary-p 2 (* 3 segment) old-history))
      (ok (pg::physical-follow-check-source-history store source))
      (ok (signals (pg::physical-follow-check-source-history store bad) 'error))
      (ok (not (pg::physical-store-failed-p store)))))

(defun follow-controller-test-entry (&optional (name "pending_controller"))
  (list :kind :follow :host "127.0.0.1" :port 5433 :pin name
        :request (follow-anchor-receiver-request (follow-anchor-request 123 456))))

(cl-spec:defproperty follow-controller-envelope-preserves-cleanup-identities-and-enable-state
    ((system-id (range integer 0 18446744073709551615))
     (manifest (range integer 0 18446744073709551615))
     (enabled (range integer 0 1))
     (count (range integer 0 64)))
  (:about pg::physical-follow-decode-config)
  (:trials (:normal 200))
  (let* ((queue (loop for index below count
                      collect (list :kind :bootstrap :host "127.0.0.1" :port 5432
                                    :pin (format nil "retire_~d" index)
                                    :request (list :version 1 :system-id system-id
                                                   :source-timeline 1 :base 152
                                                   :manifest-digest manifest))))
         (config (pg::physical-follow-with-controller
                  '(1 "127.0.0.1" 5433 "current")
                  (list :version 1 :enabled (plusp enabled) :preparing nil :cleanup queue))))
    (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config)))))

(deftest generated-follow-controller-cleanup-envelope
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'follow-controller-envelope-preserves-cleanup-identities-and-enable-state
                                       :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(defun follow-controller-test-state (&key preparing queue (enabled t))
  (list :version 1 :enabled enabled :preparing preparing :cleanup queue))

(defun follow-unattached-test-config ()
  (list 1 nil nil nil nil
        (list :version 1 :enabled nil :preparing nil :cleanup nil
              :mode :standby-unattached
              :unowned-bootstrap
              (list :pin "unowned_seed"
                    :request (list :version 1 :system-id 42 :source-timeline 1
                                   :base 64 :manifest-digest 77)))))

(deftest follow-first-attachment-apis-propagate-deferred-before-starting-workers
    (let ((ensure (symbol-function 'pg::physical-follow-ensure-attachment-state))
          (start (symbol-function 'pg::physical-follow-controller-start-worker))
          (calls 0))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-follow-ensure-attachment-state)
                   (lambda () (incf calls) :deferred)
                   (symbol-function 'pg::physical-follow-controller-start-worker)
                   (lambda (&key force) (declare (ignore force))
                     (error "deferred attachment must not start workers")))
             (ok (eq :deferred (pg::physical-follow-retarget "127.0.0.1" 5432)))
             (ok (eq :deferred (pg::physical-follow-retarget-conninfo "host=localhost")))
             (ok (= 2 calls))
             (ok (intake-fails-p
                  (lambda () (pg::physical-follow-retarget "127.0.0.1" 5432 "INVALID"))))
             (ok (intake-fails-p
                  (lambda () (pg::physical-follow-retarget-conninfo
                              "host=localhost" :prefix "INVALID"))))
             (ok (= 2 calls)))
        (setf (symbol-function 'pg::physical-follow-ensure-attachment-state) ensure
              (symbol-function 'pg::physical-follow-controller-start-worker) start))))

(deftest follow-unattached-deferred-promotion-restores-only-replay
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root store))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((config (pg::physical-follow-unattached-config-for-base
                       (pg::physical-recovery-verify-base-state)))
              (receiver (symbol-function 'pg::physical-follow-launch))
              (replay (symbol-function 'pg::physical-follow-launch-replay-only))
              (pg::*physical-promotion-stopping-p* t)
              (pg::*physical-replay-paused-p* nil)
              (seen nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-launch)
                      (lambda (config) (declare (ignore config)) (error "unexpected receiver"))
                      (symbol-function 'pg::physical-follow-launch-replay-only)
                      (lambda (saved)
                        (setf seen saved)
                        (ok pg::*physical-replay-paused-p*)
                        (ok (not pg::*physical-promotion-stopping-p*))
                        :started))
                (call-with-current-promotion-owner
                 (lambda ()
                   (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                     (ok (eq :awaiting-child-initialization
                             (progn
                               (pg::physical-promotion-resume-recovery-workers config t nil)
                               (setf config (pg::physical-follow-with-controller
                                             '(1 "127.0.0.1" 5432 "accepted_bootstrap")
                                             (follow-controller-test-state :enabled nil)))
                               (pg::physical-promotion-resume-recovery-workers config t nil)))))))
                (ok (equal config seen)))
           (setf (symbol-function 'pg::physical-follow-launch) receiver
                 (symbol-function 'pg::physical-follow-launch-replay-only) replay))))
     :system-id 42))

(deftest follow-unattached-stop-rejects-before-quiescing-or-rewriting-state
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root store))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((config (pg::physical-follow-unattached-config-for-base
                       (pg::physical-recovery-verify-base-state)))
              (runtime (pg::make-physical-follow-runtime :config config))
              (pg::*physical-follow-runtime* runtime)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (quiesce (symbol-function 'pg::physical-follow-quiesce-runtime))
              (calls 0))
         (pg::physical-follow-save-config config)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-quiesce-runtime)
                      (lambda () (incf calls)))
                (ok (intake-fails-p #'pg::physical-follow-stop))
                (ok (zerop calls))
                (ok (eq runtime pg::*physical-follow-runtime*))
                (ok (equal config (pg::physical-follow-read-config))))
           (setf (symbol-function 'pg::physical-follow-quiesce-runtime) quiesce))))
     :system-id 42))

(deftest follow-first-attachment-initializes-durable-unattached-state-before-network
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root store))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let ((launch (symbol-function 'pg::physical-follow-launch-replay-only))
             (launched nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-launch-replay-only)
                      (lambda (config)
                        (ok (equal config (pg::physical-follow-read-config)))
                        (push config launched)
                        :started))
                (let ((config (pg::physical-follow-ensure-attachment-state)))
                  (ok (pg::physical-follow-unattached-p config))
                  (ok (null (getf (sixth config) :source)))
                  (ok (null (getf (sixth config) :cleanup)))
                  (ok (equal (pg::physical-follow-unowned-bootstrap-from-base
                              (pg::physical-recovery-verify-base-state))
                             (getf (sixth config) :unowned-bootstrap)))
                  (ok (equal (list config) launched))
                  (ok (equal config (pg::physical-follow-ensure-attachment-state)))
                  (ok (equal config (pg::physical-follow-read-config)))))
           (setf (symbol-function 'pg::physical-follow-launch-replay-only) launch))))
     :system-id 42))

(deftest follow-unattached-state-is-distinct-from-primary-cleanup
    (let ((config (follow-unattached-test-config)))
      (ok (eq config (pg::physical-follow-check-config config)))
      (ok (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))
      (ok (pg::physical-follow-no-source-p config))
      (ok (pg::physical-follow-unattached-p config))
      (ok (not (pg::physical-follow-cleanup-only-p config)))
      (ok (not (pg::physical-follow-controller-enabled-p config)))
      (ok (equal (list :standby-unattached (getf (sixth config) :unowned-bootstrap))
                 (pg::physical-follow-effective-config config)))
      (remf (sixth config) :mode)
      (ok (pg::physical-follow-cleanup-only-p config))
      (ok (not (pg::physical-follow-unattached-p config)))
      (ok (null (pg::physical-follow-effective-config config)))
      (ok (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))))

(deftest follow-unattached-preparation-preserves-unowned-pin-outside-cleanup
    (let* ((config (follow-unattached-test-config))
           (reference (copy-tree (getf (sixth config) :unowned-bootstrap)))
           (entry (follow-controller-test-entry "first_attempt")))
      (setf (getf (sixth config) :preparing) entry)
      (pg::physical-follow-check-config config)
      (let ((aborted (pg::physical-follow-abandon-preparation-config config)))
        (ok (pg::physical-follow-unattached-p aborted))
        (ok (equal reference (getf (sixth aborted) :unowned-bootstrap)))
        (ok (equal (list entry) (getf (sixth aborted) :cleanup)))
        (ok (null (getf (sixth aborted) :preparing)))
        (ok (equal aborted (pg::physical-follow-decode-config (pg::physical-follow-config-image aborted))))
        (ok (not (find "unowned_seed" (getf (sixth aborted) :cleanup)
                       :key (lambda (item) (getf item :pin)) :test #'equal))))))

(deftest follow-unattached-reference-must-match-the-verified-base
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root store))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((base (pg::physical-recovery-verify-base-state))
              (config (pg::physical-follow-unattached-config-for-base base))
              (reference (getf (sixth config) :unowned-bootstrap)))
         (ok (equal (getf base :source-pin) (getf reference :pin)))
         (ok (eq config (pg::physical-follow-check-unowned-bootstrap-base config base)))
         (ok (null (pg::physical-follow-current-retirement config base)))
         (dolist (key '(:system-id :source-timeline :base :manifest-digest))
           (let ((bad (copy-tree config)))
             (incf (getf (getf (getf (sixth bad) :unowned-bootstrap) :request) key))
             (ok (signals (pg::physical-follow-check-unowned-bootstrap-base bad base) 'error)
                 (symbol-name key))))
         (let ((bad (copy-tree config)))
           (setf (getf (getf (sixth bad) :unowned-bootstrap) :pin) "another_seed")
           (ok (signals (pg::physical-follow-check-unowned-bootstrap-base bad base) 'error)))))
     :system-id 42))

(defun check-replay-only-test-launch (config paused)
  (let ((pg::*physical-replay-paused-p* paused)
        (original (symbol-function 'pg::physical-follow-replay-loop)))
    (unwind-protect
         (progn
           (setf (symbol-function 'pg::physical-follow-replay-loop)
                 (lambda (runtime)
                   (loop until (pg::physical-follow-runtime-stopped-p runtime)
                         do (sleep 0.005))))
           (pg::physical-follow-controller-save-config config)
           (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
             (ok (eq :started (pg::physical-follow-launch-replay-only config)))
             (let ((runtime pg::*physical-follow-runtime*))
               (ok runtime)
               (ok (not (pg::physical-follow-controller-enabled-p config)))
               (ok (equal config (pg::physical-follow-read-config)))
               (ok (equal config (pg::physical-follow-runtime-config runtime)))
               (ok (null (pg::physical-follow-runtime-receiver-thread runtime)))
               (ok (null (pg::physical-follow-runtime-client runtime)))
               (ok (pg::physical-follow-runtime-receiver-stopped-p runtime))
               (ok (not (pg::physical-follow-runtime-stopped-p runtime)))
               (ok (sb-thread:thread-alive-p (pg::physical-follow-runtime-replay-thread runtime)))
               (ok (eql paused pg::*physical-replay-paused-p*)))
             (ok (intake-fails-p
                  (lambda () (pg::physical-follow-launch-replay-only config))))))
      (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
        (pg::physical-follow-quiesce-runtime))
      (setf (symbol-function 'pg::physical-follow-replay-loop) original))))

(deftest follow-unattached-launches-only-replay-and-preserves-pause
    (dolist (paused '(nil t))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root store))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (check-replay-only-test-launch
          (pg::physical-follow-unattached-config-for-base
           (pg::physical-recovery-verify-base-state)) paused))
       :system-id 42)))

(deftest follow-disabled-source-launches-only-replay-and-preserves-proof
    (dolist (paused '(nil t))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (let* ((base (pg::physical-recovery-verify-base-state))
                (pending (pg::physical-follow-with-controller
                          (follow-pending-proof-config-for-store store base)
                          (follow-controller-test-state :enabled nil))))
           (pg::physical-follow-save-config pending)
           (check-replay-only-test-launch
            (call-with-promotion-exclusion
             (lambda () (pg::physical-follow-reconcile-pending-store store base pending)))
            paused)))
       :system-id 42)))

(deftest follow-disabled-bootstrap-launches-only-replay-without-losing-source
    (dolist (paused '(nil t))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root store))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (check-replay-only-test-launch
          (pg::physical-follow-with-controller
           '(1 "127.0.0.1" 5432 "accepted_bootstrap")
           (follow-controller-test-state :enabled nil)) paused))
       :system-id 42)))

(deftest follow-replay-only-rejects-enabled-unresolved-and-primary-cleanup-states
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((base (pg::physical-recovery-verify-base-state))
              (pending (pg::physical-follow-with-controller
                        (follow-pending-proof-config-for-store store base)
                        (follow-controller-test-state :enabled nil)))
              (enabled (pg::physical-follow-with-controller
                        '(1 "127.0.0.1" 5432 "accepted_bootstrap")
                        (follow-controller-test-state)))
              (preparing (pg::physical-follow-with-controller
                          '(1 "127.0.0.1" 5432 "accepted_bootstrap")
                          (follow-controller-test-state
                           :enabled nil :preparing (follow-controller-test-entry))))
              (primary (pg::physical-follow-check-config
                        (list 1 nil nil nil nil (follow-controller-test-state :enabled nil))))
              (launch (symbol-function 'pg::physical-follow-launch-workers))
              (calls 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-launch-workers)
                      (lambda (config receive-p)
                        (declare (ignore config receive-p)) (incf calls) :started))
                (dolist (config (list enabled pending preparing primary))
                  (pg::physical-follow-controller-save-config config)
                  (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                    (ok (intake-fails-p
                         (lambda () (pg::physical-follow-launch-replay-only config)))))
                  (ok (equal config (pg::physical-follow-read-config)))
                  (ok (null pg::*physical-follow-runtime*)))
                (ok (zerop calls)))
           (setf (symbol-function 'pg::physical-follow-launch-workers) launch))))
     :system-id 42))

(deftest follow-unattached-autostart-resumes-replay-without-a-receiver
    (dolist (paused '(nil t))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root store))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (let* ((config (pg::physical-follow-unattached-config-for-base
                         (pg::physical-recovery-verify-base-state)))
                (pg::*physical-replay-paused-p* paused)
                (original (symbol-function 'pg::physical-follow-replay-loop)))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-replay-loop)
                        (lambda (runtime)
                          (loop until (pg::physical-follow-runtime-stopped-p runtime)
                                do (sleep 0.005))))
                  (pg::physical-follow-controller-save-config config)
                  (progn
                    (ok (eq :started (pg::physical-follow-autostart)))
                    (let ((runtime pg::*physical-follow-runtime*))
                      (ok runtime)
                      (ok (null (pg::physical-follow-runtime-receiver-thread runtime)))
                      (ok (null (pg::physical-follow-runtime-client runtime)))
                      (ok (pg::physical-follow-runtime-receiver-stopped-p runtime))
                      (ok (not (pg::physical-follow-runtime-stopped-p runtime)))
                      (ok (sb-thread:thread-alive-p (pg::physical-follow-runtime-replay-thread runtime)))
                      (ok (eql paused pg::*physical-replay-paused-p*)))
                    (let ((before pg::*physical-follow-runtime*))
                      (pg::physical-follow-autostart)
                      (ok (eq before pg::*physical-follow-runtime*)))))
             (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
               (pg::physical-follow-quiesce-runtime))
             (setf (symbol-function 'pg::physical-follow-replay-loop) original))))
       :system-id 42)))

(deftest follow-unattached-failed-preparation-restores-replay-and-queues-only-attempt
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root store))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((config (pg::physical-follow-unattached-config-for-base
                       (pg::physical-recovery-verify-base-state)))
              (operation (pg::make-physical-follow-operation
                          :host "127.0.0.1" :port 5433 :prefix "first"))
              (pg::*physical-follow-operation* operation)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (original (symbol-function 'pg::physical-follow-replay-loop)))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-replay-loop)
                      (lambda (runtime)
                        (loop until (pg::physical-follow-runtime-stopped-p runtime)
                              do (sleep 0.005))))
                (pg::physical-follow-controller-save-config config)
                (pg::physical-follow-operation-initial-config operation)
                (multiple-value-bind (entry snapshot)
                    (pg::physical-follow-operation-intent
                     operation (list :source 1 :parent 1 :history-text ""
                                     :history (pg::timeline-parse-history 1 "")))
                  (declare (ignore snapshot))
                  (pg::physical-follow-operation-recover operation)
                  (let ((saved (pg::physical-follow-read-config))
                        (runtime pg::*physical-follow-runtime*))
                    (ok (pg::physical-follow-unattached-p saved))
                    (ok (null (getf (sixth saved) :preparing)))
                    (ok (equal (list entry) (getf (sixth saved) :cleanup)))
                    (ok (equal (getf (sixth config) :unowned-bootstrap)
                               (getf (sixth saved) :unowned-bootstrap)))
                    (ok runtime)
                    (when runtime
                      (ok (null (pg::physical-follow-runtime-receiver-thread runtime)))
                      (ok (sb-thread:thread-alive-p
                           (pg::physical-follow-runtime-replay-thread runtime)))))
                  (ok (not pg::*physical-recovery-failed-p*))))
           (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
             (pg::physical-follow-quiesce-runtime))
           (setf (symbol-function 'pg::physical-follow-replay-loop) original))))
     :system-id 42))

(deftest follow-unattached-state-rejects-receipt-authority-and-pin-aliases
    (dolist (change '((:mode :invalid) (:enabled t) (:unowned-bootstrap nil) (:source :invalid)))
      (let ((config (follow-unattached-test-config)))
        (setf (getf (sixth config) (first change)) (second change))
        (ok (signals (pg::physical-follow-check-config config) 'error))))
  (dolist (kind '(:current :preparing :queued :route))
    (let* ((config (follow-unattached-test-config))
           (state (sixth config)))
      (ecase kind
        (:current
         (setf (second config) "127.0.0.1" (third config) 5432 (fourth config) "unowned_seed")
         (remf state :mode))
        (:preparing (setf (getf state :preparing) (follow-controller-test-entry "unowned_seed")))
        (:queued (setf (getf state :cleanup) (list (follow-controller-test-entry "unowned_seed"))))
        (:route (setf (getf (getf state :unowned-bootstrap) :host) "127.0.0.1")))
      (ok (signals (pg::physical-follow-check-config config) 'error) (symbol-name kind)))))

(cl-spec:defproperty follow-unattached-envelope-preserves-full-width-bootstrap-reference
    ((system-id (range integer 0 18446744073709551615))
     (base (range integer 0 18446744073709551615))
     (digest (range integer 0 18446744073709551615))
     (primary boolean))
  (:about pg::physical-follow-decode-config)
  (:trials (:normal 200))
  (let* ((config (follow-unattached-test-config))
         (state (sixth config))
         (request (getf (getf state :unowned-bootstrap) :request)))
    (setf (getf request :system-id) system-id
          (getf request :base) base
          (getf request :manifest-digest) digest)
    (when primary (remf state :mode))
    (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config)))))

(deftest generated-follow-unattached-bootstrap-reference-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'follow-unattached-envelope-preserves-full-width-bootstrap-reference
                                       :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 200 (getf data :trials)))))

(deftest follow-controller-envelope-preserves-effective-binding-and-preparation
    (dolist (bound '(nil t))
      (let* ((old (let ((config (follow-binding-test-config)))
                    (setf (getf (fifth config) :phase) :active)
                    config))
             (old (if bound old (subseq old 0 4)))
             (entry (follow-controller-test-entry))
             (config (pg::physical-follow-with-controller
                      old (follow-controller-test-state :preparing entry))))
        (ok (equal old (pg::physical-follow-effective-config config)))
        (ok (equal config
                   (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))
        (ok (signals (pg::physical-follow-require-bootstrap-config config) 'error))
        (let ((aborted (pg::physical-follow-abandon-preparation-config config)))
          (ok (equal old (pg::physical-follow-effective-config aborted)))
          (ok (null (getf (sixth aborted) :preparing)))
          (ok (equal (list entry) (getf (sixth aborted) :cleanup)))
          (ok (getf (sixth aborted) :enabled))
          (ok (equal entry (getf (sixth config) :preparing)))))))

(deftest follow-public-slot-is-durable-effective-state-without-cleanup-authority
    (let* ((state (follow-controller-test-state :queue (list (follow-controller-test-entry))))
           (plain (pg::physical-follow-with-controller '(1 "127.0.0.1" 5432 "private_proof") state)))
      (setf (getf state :public-slot) "public_slot")
      (let* ((config (pg::physical-follow-with-controller plain state))
             (decoded (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))
        (ok (equal config decoded))
        (ok (equal "public_slot" (getf (sixth decoded) :public-slot)))
        (ok (equal (getf (sixth plain) :cleanup) (getf (sixth decoded) :cleanup)))
        (ok (not (equal (pg::physical-follow-effective-config plain)
                        (pg::physical-follow-effective-config decoded))))
        (ok (not (equal (fourth decoded) (getf (sixth decoded) :public-slot)))))
      (dolist (bad '("" "INVALID" "bad-name" "slot'; SELECT 1" 42))
        (setf (getf state :public-slot) bad)
        (ok (intake-fails-p (lambda () (pg::physical-follow-with-controller plain state)))))
      (let ((unattached (follow-unattached-test-config)))
        (setf (getf (sixth unattached) :public-slot) "public_slot")
        (ok (intake-fails-p (lambda () (pg::physical-follow-check-config unattached)))))))

(deftest follow-operation-publication-installs-or-clears-only-requested-slot
    (dolist (slot '(nil "next_public"))
      (let* ((state (follow-controller-test-state))
             (operation (pg::make-physical-follow-operation :context (list :requested-slot slot))))
        (setf (getf state :public-slot) "old_public")
        (let ((accepted (pg::physical-follow-operation-source-state operation state)))
          (ok (equal slot (getf accepted :public-slot)))
          (ok (equal "old_public" (getf state :public-slot)))
          (ok (equal (getf state :cleanup) (getf accepted :cleanup)))))))

(cl-spec:defproperty follow-public-slot-roundtrip-does-not-alias-private-pin
    ((identifier (range integer 0 18446744073709551615))
     (enabled (range integer 0 1)))
  (:about pg::physical-follow-config-public-slot)
  (:trials (:normal 200))
  (let* ((slot (string-downcase (format nil "public_~16,'0x" identifier)))
         (state (list :version 1 :enabled (plusp enabled) :preparing nil :cleanup nil
                      :public-slot slot))
         (config (pg::physical-follow-with-controller '(1 "127.0.0.1" 5432 "private_proof") state))
         (decoded (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))
    (and (equal slot (pg::physical-follow-config-public-slot decoded))
         (equal "private_proof" (fourth decoded))
         (null (getf (sixth decoded) :cleanup))
         (equal config decoded))))

(deftest generated-follow-public-slot-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'follow-public-slot-roundtrip-does-not-alias-private-pin :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest follow-operation-captures-slot-inheritance-once-and-can-clear-it
    (let ((original (symbol-function 'pg::physical-follow-operation-run))
          (pg::*physical-follow-operation* nil)
          (pg::*physical-follow-controller-blocked-p* nil)
          (pg::*physical-follow-controller-stopping-p* nil))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-follow-operation-run)
                   (lambda (operation) (declare (ignore operation)) :done))
             (dolist (requested '(:inherit nil "next_public"))
               (let* ((config (pg::physical-follow-with-controller
                               '(1 "127.0.0.1" 5432 "private_proof")
                               '(:version 1 :enabled t :preparing nil :cleanup nil :public-slot "old_public")))
                      (runtime (pg::make-physical-follow-runtime :config config))
                      (operation (pg::physical-follow-start-operation
                                  "127.0.0.1" 5432 "follow" runtime nil requested))
                      (expected (if (eq requested :inherit) "old_public" requested)))
                 (sb-thread:join-thread (pg::physical-follow-operation-thread operation))
                 (setf (aref (getf (sixth config) :public-slot) 0) #\x)
                 (ok (equal expected (getf (pg::physical-follow-operation-context operation) :requested-slot)))
                 (setf pg::*physical-follow-operation* nil))))
        (setf (symbol-function 'pg::physical-follow-operation-run) original))))

(deftest follow-controller-capacity-is-reserved-before-remote-preparation
    (let* ((queue (loop for index below (1- pg::+physical-follow-cleanup-capacity+)
                        collect (follow-controller-test-entry (format nil "old_~d" index))))
           (state (follow-controller-test-state :queue queue :preparing (follow-controller-test-entry)))
           (config (pg::physical-follow-with-controller (follow-binding-test-config) state))
           (aborted (pg::physical-follow-abandon-preparation-config config)))
      (ok (= pg::+physical-follow-cleanup-capacity+ (length (getf (sixth aborted) :cleanup))))
      (ok (signals
           (pg::physical-follow-enqueue-cleanup (sixth aborted)
                                                (follow-controller-test-entry "overflow"))
           'error))
      (ok (signals
           (pg::physical-follow-controller-copy
            (follow-controller-test-state :queue (getf (sixth aborted) :cleanup)
                                          :preparing (follow-controller-test-entry "overflow")))
           'error))
      (ok (= (1- pg::+physical-follow-cleanup-capacity+) (length (getf state :cleanup))))))

(deftest follow-controller-cleanup-only-state-cannot-enable-receipt
    (let* ((state (follow-controller-test-state :enabled nil :queue (list (follow-controller-test-entry))))
           (config (list 1 nil nil nil nil state)))
      (ok (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))
      (ok (pg::physical-follow-cleanup-only-p config))
      (ok (null (pg::physical-follow-effective-config config)))
      (ok (not (pg::physical-follow-controller-enabled-p config)))
      (dolist (key '(:enabled :preparing))
        (let ((bad (copy-tree config)))
          (setf (getf (sixth bad) key) (if (eq key :enabled) t (follow-controller-test-entry)))
          (ok (signals (pg::physical-follow-check-config bad) 'error))))))

(deftest follow-controller-queue-rejects-duplicates-and-unknown-fields
    (let ((entry (follow-controller-test-entry)))
      (ok (signals
           (pg::physical-follow-controller-copy
            (follow-controller-test-state :queue (list entry entry))) 'error))
      (let ((bad (copy-tree entry)))
        (setf (getf bad :unexpected) 1)
        (ok (signals (pg::physical-follow-cleanup-copy bad) 'error)))
      (let ((state (follow-controller-test-state :queue (list entry))))
        (ok (= 1 (length (getf (pg::physical-follow-enqueue-cleanup state entry) :cleanup)))))))

(deftest follow-controller-rejects-impossible-preparing-or-current-pin-cleanup
    (dolist (kind '(:pending :current :queued :cleanup))
      (let* ((old (follow-binding-test-config))
             (entry (follow-controller-test-entry
                     (if (member kind '(:current :cleanup)) (fourth old) "fresh_attempt")))
             (state (follow-controller-test-state
                     :preparing (unless (eq kind :cleanup) entry)
                     :queue (when (member kind '(:queued :cleanup)) (list entry)))))
        (unless (eq kind :pending) (setf (getf (fifth old) :phase) :active))
        (let ((bad (pg::physical-follow-with-controller old state)))
          (ok (signals (pg::physical-follow-check-config bad) 'error))))))

(deftest follow-controller-pending-reconciliation-preserves-queued-cleanup
    (call-with-follow-pending-proof
     (lambda (root store candidate base old)
       (declare (ignore root candidate))
       (let* ((entry (follow-controller-test-entry))
              (config (pg::physical-follow-with-controller
                       old (follow-controller-test-state :queue (list entry)))))
         (pg::physical-follow-save-config config)
         (let ((active (pg::physical-follow-reconcile-pending-store store base config)))
           (ok (equal (list entry) (getf (sixth active) :cleanup)))
           (ok (eq :active (getf (fifth active) :phase)))
           (ok (= 208 (getf (pg::physical-follow-check-active-state store base active) :terminal)))
           (ok (equal active (pg::physical-follow-read-config))))))))

(deftest follow-controller-snapshot-keeps-independent-overlap-through-a-retained-root
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((fork (pg::physical-store-flushed-lsn store))
              (text (store-history-text 1 fork))
              (view (list :parent 1 :source 2 :child 2 :fork fork :history-text text
                          :child-history text
                          :history (pg::timeline-check-lineage (pg::timeline-parse-history 2 text))))
              (snapshot (call-with-promotion-exclusion
                         (lambda () (pg::physical-follow-controller-snapshot view))))
              (request (getf snapshot :coordinates))
              (index (pg::physical-retained-inventory-load
                      (pg::physical-store-retained-directory store)
                      (pg::physical-store-retained-root store)
                      (pg::physical-store-retained-identity store)
                      (pg::physical-store-retained-history store))))
         (multiple-value-bind (metadata bytes)
             (pg::physical-retained-extent-decode
              (pg::physical-retained-object-read (pg::physical-store-retained-directory store)
                                                 :extent (first (car (last (getf index :entries))))
                                                 pg::+physical-retained-image-limit+))
           (declare (ignore metadata))
           (ok (= (getf request :overlap-digest)
                  (pg::physical-store-update-digest #xCBF29CE484222325 bytes))))
         (ok (= fork (getf request :receive-lsn)))
         (ok (/= (getf request :receive-digest) (getf request :overlap-digest)))
         (ok (equal (list fork 1) (getf snapshot :applied)))))
     :system-id 42))

(deftest follow-controller-bound-start-selects-the-receive-child
    (call-with-follow-pending-proof
     (lambda (root store candidate base config)
       (declare (ignore root candidate))
       (let* ((active (pg::physical-follow-reconcile-pending-store store base config))
              (pg::*physical-recovery-state* base) (pg::*physical-receive-store* store)
              (runtime (pg::make-physical-follow-runtime
                        :host (second active) :port (third active) :pin (fourth active) :config active))
              (command (pg::parse-replication-command
                        (pg::physical-follow-start-query runtime (pg::physical-store-flushed-lsn store)))))
         (ok (equal "follow" (pg::physical-stream-mode command)))
         (ok (= 2 (pg::physical-stream-u64 (pg::physical-stream-option command "timeline"))))
         (ok (= 64 (pg::parse-replication-lsn (pg::physical-stream-option command "base_lsn"))))))))

(defun follow-controller-test-check-startup (phase root)
  (let ((pg::*wal-ready* nil) (pg::*physical-receive-store* nil)
                              (pg::*physical-recovery-state* nil) (pg::*physical-follow-runtime* nil)
                              (pg::*physical-restartpoint-worker* nil))
    (pg::physical-recovery-prepare)
    (let ((saved (pg::physical-follow-read-config))
          (control (pg::physical-recovery-read-control root)))
      (ok (eq (getf (fifth saved) :phase) :active))
      (ok (null (getf (sixth saved) :preparing)))
      (ok (= (length (getf (sixth saved) :cleanup)) (if (eq phase :preparing) 2 1)))
      (ok (eql (getf control :applied) 216))
      (ok (eql (getf control :next-xid) 50))
      (ok (eql (getf control :history-target) 2))
      (ok (equal (getf pg::*physical-recovery-state* :follow-startup-config) saved))
      (ok (pg::physical-follow-check-startup-admission pg::*physical-recovery-state*))
      (ok (null pg::*wal-ready*))
      (ok (null pg::*physical-receive-store*))
      (values saved control))))

(defun follow-controller-test-cold-phase (phase root store)
  (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
  (let* ((base (pg::physical-recovery-verify-base-state))
         (entry (follow-controller-test-entry "already_retiring"))
         (config (pg::physical-follow-with-controller
                  (follow-pending-proof-config-for-store store base)
                  (follow-controller-test-state :queue (list entry)))))
    (pg::physical-follow-save-config config)
    (unless (eq phase :pending)
      (setf config (call-with-promotion-exclusion
                    (lambda () (pg::physical-follow-reconcile-pending-store store base config)))))
    (when (eq phase :preparing)
      (let ((state (pg::physical-follow-controller-state config)))
        (setf (getf state :preparing) (follow-controller-test-entry "abandoned_preparation")
              config (pg::physical-follow-with-controller config state))
        (pg::physical-follow-save-config config)))
    (pg::physical-store-close store)
    (sb-posix:rename (namestring (pg::physical-store-directory store))
                     (namestring (pg::physical-recovery-spool-directory)))
    (call-with-current-promotion-owner
     (lambda () (follow-controller-test-check-startup phase root)))
    (values)))

(deftest follow-controller-cold-start-reconciles-and-validates-before-dirty-publication
    (dolist (phase '(:pending :active :preparing))
      (call-with-promotion-fixture
       (lambda (root store) (follow-controller-test-cold-phase phase root store))
       :system-id 42)))

(deftest follow-unattached-cold-start-preserves-base-and-abandons-preparation
    (dolist (preparing '(nil t))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (let* ((base (pg::physical-recovery-verify-base-state))
                (config (pg::physical-follow-unattached-config-for-base base))
                (entry nil))
           (pg::physical-follow-save-config config)
           (when preparing
             (let* ((operation (pg::make-physical-follow-operation
                                :host "127.0.0.1" :port 5433 :prefix "cold"))
                    (pg::*physical-follow-operation* operation)
                    (pg::*physical-follow-controller-blocked-p* nil)
                    (pg::*physical-follow-controller-stopping-p* nil))
               (pg::physical-follow-operation-initial-config operation)
               (setf entry
                     (pg::physical-follow-operation-intent
                      operation (list :source 1 :parent 1 :history-text ""
                                      :history (pg::timeline-parse-history 1 ""))))))
           (pg::physical-store-close store)
           (sb-posix:rename (namestring (pg::physical-store-directory store))
                            (namestring (pg::physical-recovery-spool-directory)))
           (call-with-current-promotion-owner
            (lambda ()
              (let ((pg::*wal-ready* nil)
                    (pg::*physical-receive-store* nil)
                    (pg::*physical-recovery-state* nil)
                    (pg::*physical-follow-runtime* nil)
                    (pg::*physical-restartpoint-worker* nil))
                (pg::physical-recovery-prepare)
                (let* ((saved (pg::physical-follow-read-config))
                       (state (sixth saved)))
                  (ok (pg::physical-follow-unattached-p saved))
                  (ok (null (getf state :preparing)))
                  (ok (equal (if entry (list entry) nil) (getf state :cleanup)))
                  (ok (equal (getf (sixth config) :unowned-bootstrap)
                             (getf state :unowned-bootstrap)))
                  (ok (equal saved (getf pg::*physical-recovery-state* :follow-startup-config)))
                  (ok (pg::physical-follow-check-startup-admission pg::*physical-recovery-state*))
                  (ok (null pg::*physical-follow-runtime*))
                  (ok (null pg::*physical-receive-store*))))))))
       :system-id 42)))

(deftest standard-recovery-signal-policy-is-latched-only-after-base-verification
    (dolist (mode '(nil :standby :both :archive))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root))
         (ok (= 42 (pg::physical-store-system-id store)))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (when (member mode '(:standby :both))
           (pg::wal-atomic-write-bytes (merge-pathnames "standby.signal" pg::*pgdata*)
                                       (empty-octets)))
         (when (member mode '(:archive :both))
           (pg::wal-atomic-write-bytes (merge-pathnames "recovery.signal" pg::*pgdata*)
                                       (empty-octets)))
         (pg::physical-store-close store)
         (sb-posix:rename (namestring (pg::physical-store-directory store))
                          (namestring (pg::physical-recovery-spool-directory)))
         (call-with-current-promotion-owner
          (lambda ()
            (let ((pg::*wal-ready* nil)
                  (pg::*physical-receive-store* nil)
                  (pg::*physical-recovery-state* nil)
                  (pg::*physical-follow-runtime* nil)
                  (pg::*physical-restartpoint-worker* nil))
              (if (eq mode :archive)
                  (progn
                    (ok (intake-fails-p #'pg::physical-bootstrap-check-startup))
                    (ok (null pg::*physical-recovery-state*)))
                  (progn
                    (pg::physical-bootstrap-check-startup)
                    (ok (eql 2 (getf pg::*physical-recovery-state* :phase)))
                    (ok (eql (not (null mode))
                             (getf pg::*physical-recovery-state* :standard-follow-managed-p)))
                    (ok (null pg::*physical-follow-runtime*))))))))
       :system-id 42)))

(deftest managed-follow-startup-disables-saved-receipt-before-autostart
    (dolist (kind '(:unattached :bootstrap :bound))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (let* ((base (pg::physical-recovery-verify-base-state))
                (config
                  (case kind
                    (:bootstrap '(1 "127.0.0.1" 5432 "accepted_bootstrap"))
                    (:bound
                     (let ((pending (pg::physical-follow-with-controller
                                     (follow-pending-proof-config-for-store store base)
                                     (follow-controller-test-state))))
                       (pg::physical-follow-save-config pending)
                       (call-with-promotion-exclusion
                        (lambda () (pg::physical-follow-reconcile-pending-store store base pending)))))))
                (full (symbol-function 'pg::physical-follow-launch))
                (replay (symbol-function 'pg::physical-follow-launch-replay-only))
                (full-starts 0) (replay-starts 0))
           (when config (pg::physical-follow-save-config config))
           (setf (getf pg::*physical-recovery-state* :standard-follow-managed-p) t
                 (getf pg::*physical-recovery-state* :follow-startup-config) config)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-launch)
                        (lambda (saved) (declare (ignore saved)) (incf full-starts))
                        (symbol-function 'pg::physical-follow-launch-replay-only)
                        (lambda (saved)
                          (ok (not (pg::physical-follow-controller-enabled-p saved)))
                          (incf replay-starts)))
                  (call-with-current-promotion-owner #'pg::physical-follow-prepare-managed-startup)
                  (let ((saved (pg::physical-follow-read-config)))
                    (ok (not (pg::physical-follow-controller-enabled-p saved)))
                    (if config
                        (ok (equal (subseq config 0 (min 5 (length config)))
                                   (subseq saved 0 (min 5 (length config)))))
                        (ok (pg::physical-follow-unattached-p saved)))
                    (ok (equal saved (getf pg::*physical-recovery-state* :follow-startup-config))))
                  (ok (zerop (+ full-starts replay-starts)))
                  (pg::physical-follow-autostart)
                  (ok (zerop full-starts))
                  (ok (= 1 replay-starts)))
             (setf (symbol-function 'pg::physical-follow-launch) full
                   (symbol-function 'pg::physical-follow-launch-replay-only) replay))))
       :system-id 42)))

(deftest follow-unattached-cold-start-rejects-required-progress-beyond-durable-spool
    (call-with-promotion-fixture
     (lambda (root store)
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((base (pg::physical-recovery-verify-base-state))
              (config (pg::physical-follow-unattached-config-for-base base)))
         (pg::physical-follow-save-config config)
         (pg::physical-recovery-write-control
          base 2 (+ 8 (pg::physical-store-flushed-lsn store)) pg::*next-xid*)
         (let ((before (lineage-test-bytes (pg::physical-recovery-control-path))))
           (pg::physical-store-close store)
           (sb-posix:rename (namestring (pg::physical-store-directory store))
                            (namestring (pg::physical-recovery-spool-directory)))
           (call-with-current-promotion-owner
            (lambda ()
              (let ((pg::*wal-ready* nil)
                    (pg::*physical-receive-store* nil)
                    (pg::*physical-recovery-state* nil)
                    (pg::*physical-follow-runtime* nil)
                    (pg::*physical-restartpoint-worker* nil))
                (ok (intake-fails-p #'pg::physical-recovery-prepare))
                (ok (null pg::*physical-recovery-state*))
                (ok (null pg::*physical-receive-store*))
                (ok (equalp before (lineage-test-bytes
                                    (pg::physical-recovery-control-path))))))))
         (ok (equal config (pg::physical-follow-read-config)))
         (ok (probe-file root))))
     :system-id 42))
