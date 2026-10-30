(in-package #:pg-physical-tests)

(deftest fixed-follow-source-discovery-fetches-only-the-permitted-child
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (let* ((two (store-history-text 1 216))
              (three (concatenate 'string two (store-history-text 2 288)))
              (names '(pg::physical-follow-client-identity pg::pg-client-native-history))
              (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
              (fetched nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-client-identity)
                      (lambda (client) (declare (ignore client)) (values 42 3))
                      (symbol-function 'pg::pg-client-native-history)
                      (lambda (client timeline)
                        (declare (ignore client))
                        (push timeline fetched)
                        (pg::string->bytes (ecase timeline (2 two) (3 three)))))
                (dolist (goal '(:latest :current 2))
                  (setf fetched nil)
                  (let* ((policy (if (eq goal :latest) nil
                                     (if (eq goal :current)
                                         (pg::physical-recovery-resolve-timeline-policy
                                          pg::*physical-recovery-state* store nil goal)
                                         (list :target 2 :history (pg::timeline-parse-history 2 two)))))
                         (view (pg::physical-follow-controller-source-view nil policy)))
                    (ok (eq :attach (getf view :action)))
                    (ok (= 3 (getf view :source)))
                    (ok (= 1 (getf view :parent)))
                    (ok (= 216 (getf view :fork)))
                    (if (eq goal :current)
                        (progn
                          (ok (eq :retained-timeline (getf view :proof-kind)))
                          (ok (null (getf view :child-history)))
                          (ok (equal '(3) (reverse fetched))))
                        (progn
                          (ok (eq :descendant (getf view :proof-kind)))
                          (ok (= 2 (getf view :child)))
                          (ok (equal '(3 2) (reverse fetched))))))))
           (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))
     :applied 152 :system-id 42))

(deftest follow-operation-captures-and-rechecks-the-startup-policy
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((policy (pg::physical-recovery-resolve-timeline-policy
                       pg::*physical-recovery-state* store nil :current))
              (operation (pg::make-physical-follow-operation))
              (pg::*physical-follow-operation* operation)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil))
         (setf (getf pg::*physical-recovery-state* :timeline-policy) policy)
         (pg::physical-follow-save-config '(1 "127.0.0.1" 5432 "old_bootstrap"))
         (pg::physical-follow-operation-initial-config operation)
         (ok (eq policy (getf (pg::physical-follow-operation-context operation) :timeline-policy)))
         (ok (eq :discover (getf (getf (pg::physical-follow-operation-context operation) :local-decision) :action)))
         (setf (getf pg::*physical-recovery-state* :timeline-policy) (copy-tree policy))
         (ok (signals (pg::physical-follow-operation-check operation) 'pg::physical-follow-controller-cancelled))))
     :applied 152 :system-id 42))

(deftest retained-follow-route-declines-a-local-parent-tail-without-clamping
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((policy (pg::physical-recovery-resolve-timeline-policy
                       pg::*physical-recovery-state* store nil :current))
              (view (append (follow-controller-test-view 216)
                            (list :proof-kind :retained-timeline :fixed-policy policy))))
         (follow-controller-test-advance store)
         (let ((before (pg::physical-store-control-tuple store)))
           (call-with-promotion-exclusion
            (lambda ()
              (ok (handler-case
                      (progn (pg::physical-follow-controller-snapshot view) nil)
                    (pg::physical-follow-route-unavailable (condition)
                      (eq :source-history-exhausted (pg::physical-follow-route-reason condition)))))))
           (ok (equal before (pg::physical-store-control-tuple store)))
           (ok (= 224 pg::*last-replay-lsn*))
           (ok (not pg::*physical-recovery-failed-p*)))))
     :applied 152 :system-id 42))

(defun call-with-retained-follow-config (function &key fork)
  (call-with-promotion-fixture
   (lambda (root store)
     (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
     (pg::wal-atomic-write-bytes (merge-pathnames "PG_VERSION" root)
                                 (pg::string->bytes (format nil "20~%")))
     (let* ((base (pg::physical-recovery-verify-base-state))
            (pending (pg::physical-follow-with-controller
                      (follow-pending-proof-config-for-store store base)
                      (follow-controller-test-state)))
            (binding (fifth pending))
            (anchor (getf binding :anchor)))
       (when fork (setf (getf anchor :history-text) (store-history-text 1 fork)))
       (remf anchor :eor-octets)
       (setf (getf anchor :kind) :retained-timeline
             (getf binding :anchor) anchor
             (getf binding :child-history) nil
             (getf binding :after) (copy-list (getf binding :before)))
       (pg::physical-follow-save-config pending)
       (let ((active (call-with-promotion-exclusion
                      (lambda () (pg::physical-follow-reconcile-pending-store store base pending)))))
         (setf (getf pg::*physical-recovery-state* :timeline-policy)
               (pg::physical-recovery-resolve-timeline-policy base store nil :current))
         (funcall function root store active))))
   :applied 152 :system-id 42))

(deftest retained-source-history-prevents-reusing-its-timeline-on-parent-promotion
    (call-with-retained-follow-config
     (lambda (root store config)
       (declare (ignore store))
       (ok (= 2 (getf (getf (fifth config) :anchor) :source-timeline)))
       (ok (not (probe-file (pg::timeline-history-path 2 root))))
       (let ((pending (call-with-promotion-exclusion
                       (lambda () (pg::physical-promotion-publish-intent)))))
         (ok (= 3 (getf pending :promotion-child)))
         (ok (= 1 (pg::physical-recovery-replay-timeline pending)))
         (ok (not (probe-file (pg::timeline-history-path 2 root))))
         (ok (= 3 (getf (pg::physical-recovery-read-control root) :promotion-child)))))))

(deftest follow-known-timeline-survives-replacement-disable-and-promotion-cleanup
    (call-with-retained-follow-config
     (lambda (root store config)
       (declare (ignore root store))
       (let* ((state (pg::physical-follow-remember-timeline
                      (pg::physical-follow-controller-state config) 19))
              (replacement (pg::physical-follow-with-controller
                            '(1 "127.0.0.1" 5434 "replacement") state)))
         (setf (getf state :enabled) nil)
         (let ((disabled (pg::physical-follow-with-controller replacement state)))
           (pg::physical-follow-save-config disabled)
           (ok (= 19 (getf (pg::physical-follow-controller-state
                            (pg::physical-follow-read-config)) :known-timeline)))
           (let ((cleanup (pg::physical-follow-promotion-cleanup-config)))
             (ok (= 19 (getf (pg::physical-follow-controller-state cleanup) :known-timeline)))
             (ok (pg::physical-follow-no-source-p cleanup))))))))

(deftest legacy-source-timeline-is-remembered-before-replacing-or-retiring-its-anchor
    (call-with-retained-follow-config
     (lambda (root store config)
       (declare (ignore root store))
       (setf (getf (getf (fifth config) :anchor) :source-timeline) 19)
       (pg::physical-follow-check-config config)
       (let* ((before (copy-tree config))
              (state (pg::physical-follow-remember-config-timeline config))
              (lower (pg::physical-follow-remember-timeline state 2)))
         (ok (= 19 (getf lower :known-timeline)))
         (ok (equal before config))
         (pg::physical-follow-save-config config)
         (ok (= 19 (getf (pg::physical-follow-controller-state
                          (pg::physical-follow-promotion-cleanup-config)) :known-timeline)))))))

(deftest promotion-freezes-known-namespace-before-intent-and-ignores-later-policy
    (call-with-promotion-fixture
     (lambda (root store)
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (pg::timeline-write-history 12 1 216 "known target" root)
       (setf (getf pg::*physical-recovery-state* :timeline-policy)
             (pg::physical-recovery-resolve-timeline-policy
              pg::*physical-recovery-state* store nil 12))
       (let ((pending (call-with-promotion-exclusion
                       (lambda () (pg::physical-promotion-publish-intent)))))
         (ok (= 13 (getf pending :promotion-child)))
         (ok (= 12 (getf (pg::physical-follow-controller-state
                          (pg::physical-follow-read-config)) :known-timeline)))
         (ok (not (probe-file (pg::timeline-history-path 2 root))))
         (setf (getf pending :timeline-policy) (list :target 99))
         (ok (= 12 (pg::physical-promotion-timeline-floor pending)))
         (ok (progn (pg::physical-promotion-check-history pending nil) t))
         (let ((config (pg::physical-follow-read-config)))
           (dolist (bad '(1 13))
             (let ((state (pg::physical-follow-controller-state config)))
               (setf (getf state :known-timeline) bad)
               (pg::physical-follow-save-config (pg::physical-follow-with-controller config state))
               (if (= bad 13)
                   (ok (signals (pg::physical-promotion-timeline-floor pending) 'error))
                   (ok (signals (pg::physical-promotion-check-history pending nil) 'error)))))
           (pg::physical-follow-save-config config))))))

(deftest known-timeline-exhaustion-rejects-before-promotion-intent
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root store))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let ((config (pg::physical-follow-with-controller
                      '(1 "127.0.0.1" 5432 "old_source")
                      (pg::physical-follow-remember-timeline
                       (follow-controller-test-state) 4294967295))))
         (pg::physical-follow-save-config config)
         (ok (signals (call-with-promotion-exclusion
                       (lambda () (pg::physical-promotion-publish-intent))) 'error))
         (ok (= 2 (getf pg::*physical-recovery-state* :phase)))
         (ok (= 2 (getf (pg::physical-recovery-read-control pg::*pgdata*) :phase)))
         (ok (equal config (pg::physical-follow-read-config)))
         (ok (not pg::*physical-recovery-failed-p*))))))

(deftest cleanup-only-recovery-preserves-retired-source-namespace-through-promotion
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root store))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let ((entry (follow-controller-test-entry "retired_high_source")))
         (setf (getf (getf entry :request) :source-timeline) 19)
         (pg::physical-follow-save-config
          (list 1 nil nil nil nil
                (follow-controller-test-state :enabled nil :queue (list entry))))
         (let ((connect (symbol-function 'pg::physical-follow-cleanup-connect))
               (abort (symbol-function 'pg::pg-client-native-abort))
               (pg::*physical-follow-controller-errors* (make-hash-table :test #'equal))
               (pg::*physical-follow-cleanup-retry-after* (make-hash-table :test #'equal))
               (acks 0))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-cleanup-connect)
                        (lambda (record) (declare (ignore record)) nil)
                        (symbol-function 'pg::pg-client-native-abort)
                        (lambda (&rest args) (declare (ignore args)) (incf acks)))
                  (pg::physical-follow-controller-cleanup entry)
                  (ok (= 1 acks))
                  (let ((state (pg::physical-follow-controller-state (pg::physical-follow-read-config))))
                    (ok (null (getf state :cleanup)))
                    (ok (= 19 (getf state :known-timeline)))))
             (setf (symbol-function 'pg::physical-follow-cleanup-connect) connect
                   (symbol-function 'pg::pg-client-native-abort) abort)))
         (let ((pending (call-with-promotion-exclusion
                         (lambda () (pg::physical-promotion-publish-intent)))))
           (ok (= 20 (getf pending :promotion-child)))
           (ok (= 19 (pg::physical-promotion-timeline-floor pending)))
           (ok (progn (pg::physical-promotion-check-history pending nil) t)))))))

(deftest promotion-known-timeline-save-cuts-leave-phase-two-retryable
    (dolist (point '(:before-follow-config-fsync :after-follow-config-fsync
                     :before-follow-config-rename :after-follow-config-rename
                     :after-follow-config-directory-fsync))
      (call-with-retained-follow-config
       (lambda (root store config)
         (declare (ignore config))
         (let ((before (pg::physical-store-control-tuple store))
               (hits 0))
           (let ((pg::*physical-follow-test-hook*
                   (lambda (at) (when (eq at point) (incf hits) (error "floor save cut")))))
             (ok (signals (call-with-promotion-exclusion
                           (lambda () (pg::physical-promotion-publish-intent))) 'error)))
           (ok (= 1 hits))
           (ok (= 2 (getf pg::*physical-recovery-state* :phase)))
           (ok (= 2 (getf (pg::physical-recovery-read-control root) :phase)))
           (ok (equal before (pg::physical-store-control-tuple store)))
           (ok (not pg::*physical-recovery-failed-p*))
           (let ((known (getf (pg::physical-follow-controller-state
                               (pg::physical-follow-read-config)) :known-timeline)))
             (if (member point '(:after-follow-config-rename :after-follow-config-directory-fsync))
                 (ok (eql 2 known))
                 (ok (null known))))
           (let ((pending (call-with-promotion-exclusion
                           (lambda () (pg::physical-promotion-publish-intent)))))
             (ok (= 3 (getf pending :promotion-child)))
             (ok (progn (pg::physical-promotion-check-history pending nil) t))))))))

(deftest cold-pending-reconstruction-uses-frozen-known-timeline
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (pg::physical-follow-save-config
        (pg::physical-follow-with-controller
         '(1 "127.0.0.1" 5432 "old_source")
         (pg::physical-follow-remember-timeline (follow-controller-test-state) 19)))
       (call-with-promotion-exclusion (lambda () (pg::physical-promotion-publish-intent)))
       (let ((pg::*physical-receive-store* nil))
         (multiple-value-bind (pending metadata prefix generation)
             (pg::physical-promotion-read-pending-state)
           (declare (ignore metadata prefix generation))
           (ok (= 20 (getf pending :promotion-child)))
           (ok (= 19 (pg::physical-promotion-timeline-floor pending)))
           (ok (progn (pg::physical-promotion-check-history pending nil) t))))
       (ok (= 1 (pg::physical-store-selected-timeline store))))
     :system-id (pg::physical-store-update-digest
                 #xCBF29CE484222325 (pg::string->bytes "history-source"))))

(deftest legacy-pending-intent-cannot-reuse-an-observed-source-timeline
    (call-with-retained-follow-config
     (lambda (root store config)
       (declare (ignore root store))
       (let ((pending (call-with-promotion-exclusion
                       (lambda () (pg::physical-promotion-publish-intent)))))
         (let ((state (pg::physical-follow-controller-state (pg::physical-follow-read-config))))
           (remf state :known-timeline)
           (pg::physical-follow-save-config (pg::physical-follow-with-controller config state)))
         (let ((colliding (copy-list pending)))
           (setf (getf colliding :promotion-child) 2)
           (ok (signals (pg::physical-promotion-timeline-floor colliding) 'error)))
         (pg::timeline-write-history 2 1 152 "legacy observed source")
         (ok (= 1 (pg::physical-promotion-timeline-floor pending)))
         (ok (progn (pg::physical-promotion-check-history pending nil) t))))))

(deftest retained-receiver-completion-is-a-timeline-end-event
    (dolist (copy '(nil t))
      (call-with-retained-follow-config
       (lambda (root store config)
         (declare (ignore root))
         (let* ((fork (pg::physical-store-flushed-lsn store))
                (runtime (pg::make-physical-follow-runtime :host "127.0.0.1" :port 1
                                                           :pin (fourth config) :config config))
                (pg::*physical-follow-statistics* nil)
                (names '(pg::physical-follow-client-identity pg::physical-client-start
                         pg::physical-client-receive pg::physical-client-finish
                         pg::physical-follow-feedback))
                (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
                (feedback 0) (finished 0))
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-client-identity)
                        (lambda (client) (declare (ignore client)) (values 42 2))
                        (symbol-function 'pg::physical-client-start)
                        (lambda (client query)
                          (declare (ignore client))
                          (ok (search "timeline '1'" query))
                          (if copy :copy-both (list :switch 2 fork)))
                        (symbol-function 'pg::physical-client-receive)
                        (lambda (client) (declare (ignore client)) (values :done nil))
                        (symbol-function 'pg::physical-client-finish)
                        (lambda (client next limit)
                          (declare (ignore client))
                          (incf finished)
                          (ok (= 2 next)) (ok (= fork limit))
                          (list :switch next limit))
                        (symbol-function 'pg::physical-follow-feedback)
                        (lambda (client) (declare (ignore client)) (incf feedback)))
                  (ok (equal (list :timeline-end :timeline 1 :fork fork :next 2)
                             (pg::physical-follow-bound-stream nil runtime fork)))
                  (ok (= (if copy 1 0) feedback finished))
                  (ok (= fork (pg::physical-store-flushed-lsn store)))
                  (ok (not pg::*physical-recovery-failed-p*)))
             (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry)))))))))

(deftest fixed-timeline-completion-stops-only-receipt-and-parent-replay-continues
    (dolist (paused '(nil t))
      (call-with-retained-follow-config
       (lambda (root store config)
         (declare (ignore root))
         (let* ((pg::*physical-replay-paused-p* paused)
                (pg::*physical-follow-operation* nil)
                (pg::*physical-follow-controller-blocked-p* nil)
                (pg::*physical-follow-controller-stopping-p* nil)
                (pg::*physical-follow-auto-error* nil)
                (pg::*physical-follow-auto-retry-after* 0)
                (receive (symbol-function 'pg::physical-follow-receiver-loop))
                (handoff (symbol-function 'pg::physical-follow-start-operation))
                (handoffs 0))
           (pg::clog-init)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-receiver-loop)
                        (lambda (runtime)
                          (loop until (pg::physical-follow-receiver-stopped-p runtime)
                                do (sleep 0.005)))
                        (symbol-function 'pg::physical-follow-start-operation)
                        (lambda (&rest args) (declare (ignore args))
                          (incf handoffs) (error "unexpected child handoff")))
                  (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                    (pg::physical-follow-launch config))
                  (let* ((runtime pg::*physical-follow-runtime*)
                         (receiver (pg::physical-follow-runtime-receiver-thread runtime))
                         (replay (pg::physical-follow-runtime-replay-thread runtime)))
                    (ok receiver)
                    (pg::with-live-operation
                      (pg::physical-recovery-receive
                       152 (pg::wal-encode-record pg::+wal-record-batch-start+ 0 152 (empty-octets)))
                      (pg::physical-recovery-receive
                       184 (pg::wal-encode-record pg::+wal-record-commit+ 70 184
                                                  (pg::wal-encode-commit-timestamp 42)))
                      (pg::physical-recovery-flush))
                    (setf (pg::physical-follow-runtime-transition runtime)
                          '(:timeline-end :timeline 1 :fork 224 :next 2))
                    (ok (eq :replay-only (pg::physical-follow-controller-step)))
                    (ok (zerop handoffs))
                    (ok (not (sb-thread:thread-alive-p receiver)))
                    (ok (eq replay (pg::physical-follow-runtime-replay-thread runtime)))
                    (ok (sb-thread:thread-alive-p replay))
                    (ok (not (pg::physical-follow-runtime-stopped-p runtime)))
                    (ok (eql paused pg::*physical-replay-paused-p*))
                    (ok (equal config (pg::physical-follow-read-config)))
                    (setf pg::*physical-replay-paused-p* nil)
                    (pg::physical-recovery-replay)
                    (ok (= 224 pg::*last-replay-lsn*))
                    (ok (= 71 pg::*next-xid*))
                    (ok (= 224 (pg::physical-store-flushed-lsn store)))
                    (ok (not pg::*physical-recovery-failed-p*))))
             (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
               (pg::physical-follow-quiesce-runtime))
             (setf (symbol-function 'pg::physical-follow-receiver-loop) receive
                   (symbol-function 'pg::physical-follow-start-operation) handoff)
             (when pg::*clog-stream* (close pg::*clog-stream*) (setf pg::*clog-stream* nil)))))
       :fork 224)))

(deftest cold-completed-retained-binding-starts-replay-without-contacting-source
    (call-with-retained-follow-config
     (lambda (root store config)
       (pg::physical-store-close store)
       (sb-posix:rename (namestring (pg::physical-store-directory store))
                        (namestring (pg::physical-recovery-spool-directory)))
       (call-with-current-promotion-owner
        (lambda ()
          (let ((pg::*wal-ready* nil) (pg::*physical-receive-store* nil)
                                      (pg::*physical-recovery-state* nil) (pg::*physical-follow-runtime* nil)
                                      (pg::*physical-restartpoint-worker* nil)
                                      (pg::*wal-checkpoint-snapshot* pg::*wal-checkpoint-snapshot*)
                                      (pg::*wal-poisoned* pg::*wal-poisoned*)
                                      (pg::*active-xids* pg::*active-xids*)
                                      (pg::*physical-follow-operation* nil)
                                      (pg::*physical-follow-controller-blocked-p* nil)
                                      (pg::*physical-follow-controller-stopping-p* nil)
                                      (receive (symbol-function 'pg::physical-follow-receiver-loop))
                                      (handoff (symbol-function 'pg::physical-follow-start-operation))
                                      (contacts 0))
            (unwind-protect
                 (progn
                   (setf (symbol-function 'pg::physical-follow-receiver-loop)
                         (lambda (runtime) (declare (ignore runtime)) (incf contacts))
                         (symbol-function 'pg::physical-follow-start-operation)
                         (lambda (&rest args) (declare (ignore args))
                           (incf contacts) (error "unexpected source discovery")))
                   (pg::physical-recovery-prepare :timeline-goal :current)
                   (pg::physical-recovery-open-storage)
                   (pg::physical-follow-autostart)
                   (let ((runtime pg::*physical-follow-runtime*))
                     (ok runtime)
                     (ok (null (pg::physical-follow-runtime-receiver-thread runtime)))
                     (ok (pg::physical-follow-runtime-receiver-stopped-p runtime))
                     (ok (sb-thread:thread-alive-p (pg::physical-follow-runtime-replay-thread runtime)))
                     (ok (equal '(:waiting :timeline-target-reached)
                                (pg::physical-follow-runtime-transition runtime))))
                   (pg::physical-follow-controller-step)
                   (pg::physical-follow-controller-step)
                   (ok (zerop contacts))
                   (ok (equal config (pg::physical-follow-read-config)))
                   (ok (not pg::*physical-recovery-failed-p*)))
              (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                (pg::physical-follow-quiesce-runtime))
              (setf (symbol-function 'pg::physical-follow-receiver-loop) receive
                    (symbol-function 'pg::physical-follow-start-operation) handoff)
              (when pg::*clog-stream* (close pg::*clog-stream*) (setf pg::*clog-stream* nil))
              (when pg::*physical-receive-store*
                (pg::physical-store-close pg::*physical-receive-store*)))))))))

(deftest manual-follow-operation-publishes-retained-proof-or-reports-unavailable
    (dolist (goal '(:current 2))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (let* ((two (store-history-text 1 216))
                (source (if (eq goal :current)
                            (concatenate 'string two (store-history-text 2 288))
                            (store-history-text 1 240)))
                (policy (if (eq goal :current)
                            (pg::physical-recovery-resolve-timeline-policy
                             pg::*physical-recovery-state* store nil goal)
                            (list :target 2 :history (pg::timeline-parse-history 2 two))))
                (operation (pg::make-physical-follow-operation :host "127.0.0.1" :port 5433 :prefix "route"))
                (pg::*physical-follow-operation* operation)
                (pg::*physical-follow-controller-blocked-p* nil)
                (pg::*physical-follow-controller-stopping-p* nil)
                (pg::*physical-follow-auto-error* nil)
                (old '(1 "127.0.0.1" 5432 "old_bootstrap"))
                (before (pg::physical-store-control-tuple store))
                (names '(pg::physical-follow-operation-connect pg::physical-follow-client-identity
                         pg::pg-client-native-history pg::pg-client-native-prepare pg::physical-follow-launch))
                (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
                (prepared nil) (launched nil))
           (setf (getf pg::*physical-recovery-state* :timeline-policy) policy)
           (pg::physical-follow-save-config old)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-operation-connect)
                        (lambda (op) (declare (ignore op)) nil)
                        (symbol-function 'pg::physical-follow-client-identity)
                        (lambda (client) (declare (ignore client)) (values 42 3))
                        (symbol-function 'pg::pg-client-native-history)
                        (lambda (client timeline)
                          (declare (ignore client))
                          (unless (= timeline 3) (error "unexpected child history request"))
                          (pg::string->bytes source))
                        (symbol-function 'pg::pg-client-native-prepare)
                        (lambda (client pin request)
                          (declare (ignore client pin))
                          (setf prepared request)
                          (append (copy-list request) (list :history-text source)))
                        (symbol-function 'pg::physical-follow-launch)
                        (lambda (config) (setf launched config) :started))
                  (pg::physical-follow-operation-run operation)
                  (ok (null (pg::physical-follow-operation-error operation)))
                  (ok (equal before (pg::physical-store-control-tuple store)))
                  (if (eq goal :current)
                      (progn
                        (ok (eq :started (pg::physical-follow-operation-result operation)))
                        (ok (eq :retained-timeline (getf prepared :kind)))
                        (ok (eq :retained-timeline (getf (getf (fifth launched) :anchor) :kind)))
                        (ok (and (equal before (getf (fifth launched) :before))
                                 (equal before (getf (fifth launched) :after)))))
                      (progn
                        (ok (equal '(:not-attached :reason :timeline-history-unavailable)
                                   (pg::physical-follow-operation-result operation)))
                        (ok (null prepared)) (ok (null launched))
                        (ok (equal (append old '(nil (:version 1 :enabled t :preparing nil :cleanup nil
                                                      :known-timeline 3)))
                                   (pg::physical-follow-read-config)))))
                  (ok (not pg::*physical-recovery-failed-p*)))
             (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))
       :applied 152 :system-id 42)))

(deftest unavailable-source-discovery-reserves-its-timeline-without-accepting-the-route
    (call-with-promotion-fixture
     (lambda (root store)
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (pg::timeline-write-history 2 1 216 "fixed target" root)
       (let* ((policy (pg::physical-recovery-resolve-timeline-policy
                       pg::*physical-recovery-state* store nil 2))
              (old '(1 "127.0.0.1" 5432 "accepted_source"))
              (operation (pg::make-physical-follow-operation
                          :host "127.0.0.1" :port 5433 :prefix "unavailable"))
              (pg::*physical-follow-operation* operation)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (pg::*physical-follow-auto-error* nil)
              (pg::*physical-follow-auto-retry-after* 0)
              (names '(pg::physical-follow-operation-connect pg::physical-follow-client-identity
                       pg::pg-client-native-history pg::pg-client-native-prepare))
              (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
              (before (pg::physical-store-control-tuple store))
              (prepares 0))
         (setf (getf pg::*physical-recovery-state* :timeline-policy) policy)
         (pg::physical-follow-save-config old)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-operation-connect)
                      (lambda (op) (declare (ignore op)) nil)
                      (symbol-function 'pg::physical-follow-client-identity)
                      (lambda (client) (declare (ignore client)) (values 42 19))
                      (symbol-function 'pg::pg-client-native-history)
                      (lambda (client timeline) (declare (ignore client))
                        (assert (= timeline 19))
                        (pg::string->bytes (store-history-text 1 240)))
                      (symbol-function 'pg::pg-client-native-prepare)
                      (lambda (&rest args) (declare (ignore args)) (incf prepares)
                              (error "unavailable route must not prepare")))
                (pg::physical-follow-operation-run operation)
                (ok (null (pg::physical-follow-operation-error operation)))
                (ok (equal '(:not-attached :reason :timeline-history-unavailable)
                           (pg::physical-follow-operation-result operation)))
                (ok (zerop prepares))
                (ok (equal before (pg::physical-store-control-tuple store)))
                (let* ((config (pg::physical-follow-read-config))
                       (state (pg::physical-follow-controller-state config)))
                  (ok (equal (pg::physical-follow-effective-config old)
                             (pg::physical-follow-effective-config config)))
                  (ok (eql 19 (getf state :known-timeline)))
                  (ok (null (getf state :preparing)))
                  (ok (null (getf state :cleanup))))
                (let ((pending (call-with-promotion-exclusion
                                (lambda () (pg::physical-promotion-publish-intent)))))
                  (ok (= 20 (getf pending :promotion-child)))
                  (ok (= 1 (pg::physical-recovery-replay-timeline pending)))))
           (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))
     :applied 152 :system-id 42))

(deftest source-observer-runs-only-after-same-system-complete-history-validation
    (dolist (mode '(:wrong-system :bad-history :unavailable :rewritten :bad-child))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root))
         (let* ((two (store-history-text 1 216))
                (policy (when (eq mode :unavailable)
                          (list :target 2 :history (pg::timeline-parse-history 2 two))))
                (observed nil) (fetched nil)
                (identity (symbol-function 'pg::physical-follow-client-identity))
                (history (symbol-function 'pg::pg-client-native-history)))
           (when (eq mode :rewritten)
             (pg::physical-store-select-history store 2 (store-history-text 1 152)
                                                (store-history-eor 152 2 1)))
           (let ((before (pg::physical-store-control-tuple store)))
             (unwind-protect
                  (progn
                    (setf (symbol-function 'pg::physical-follow-client-identity)
                          (lambda (client) (declare (ignore client))
                            (values (if (eq mode :wrong-system) 43 42) 19))
                          (symbol-function 'pg::pg-client-native-history)
                          (lambda (client timeline)
                            (declare (ignore client))
                            (push timeline fetched)
                            (when (= timeline 2) (error "child history unavailable"))
                            (pg::string->bytes
                             (case mode
                               (:bad-history "invalid history")
                               (:bad-child (concatenate 'string two (store-history-text 2 288)))
                               (t (store-history-text 1 240))))))
                    (flet ((discover ()
                             (pg::physical-follow-controller-source-view
                              nil policy (lambda (timeline system-id)
                                           (assert (= system-id 42))
                                           (push timeline observed)))))
                      (if (eq mode :unavailable)
                          (ok (eq :unavailable (getf (discover) :action)))
                          (ok (signals (discover) 'error))))
                    (if (member mode '(:wrong-system :bad-history))
                        (ok (null observed))
                        (ok (equal '(19) observed)))
                    (when (eq mode :wrong-system) (ok (null fetched)))
                    (when (eq mode :bad-child) (ok (equal '(2 19) fetched)))
                    (ok (equal before (pg::physical-store-control-tuple store))))
               (setf (symbol-function 'pg::physical-follow-client-identity) identity
                     (symbol-function 'pg::pg-client-native-history) history)))))
       :applied 152 :system-id 42)))

(deftest source-observation-save-failures-and-cancellation-preserve-the-old-runtime
    (dolist (mode '(:before-follow-config-fsync :after-follow-config-fsync
                    :before-follow-config-rename :after-follow-config-rename
                    :after-follow-config-directory-fsync :cancel-before
                    :cancel-after-rename :cancel-after-sync))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (let* ((old '(1 "127.0.0.1" 5432 "accepted_source"))
                (runtime (pg::make-physical-follow-runtime :config old))
                (operation (pg::make-physical-follow-operation
                            :host "127.0.0.1" :port 5433 :prefix "observed" :runtime runtime
                            :context (list :automatic-p t)))
                (pg::*physical-follow-runtime* runtime)
                (pg::*physical-follow-operation* operation)
                (pg::*physical-follow-controller-blocked-p* nil)
                (pg::*physical-follow-controller-stopping-p* nil)
                (pg::*physical-follow-auto-error* nil)
                (pg::*physical-follow-auto-retry-after* 0)
                (pg::*physical-replay-paused-p* t)
                (names '(pg::physical-follow-operation-connect pg::physical-follow-client-identity
                         pg::pg-client-native-history pg::pg-client-native-prepare))
                (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
                (before (pg::physical-store-control-tuple store))
                (prepares 0))
           (setf (getf pg::*physical-recovery-state* :timeline-policy)
                 (list :target 2 :history (pg::timeline-parse-history 2 (store-history-text 1 216))))
           (pg::physical-follow-save-config old)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-operation-connect)
                        (lambda (op) (declare (ignore op)) nil)
                        (symbol-function 'pg::physical-follow-client-identity)
                        (lambda (client)
                          (declare (ignore client))
                          (when (eq mode :cancel-before)
                            (setf (pg::physical-follow-operation-cancelled-p operation) t))
                          (values 42 19))
                        (symbol-function 'pg::pg-client-native-history)
                        (lambda (client timeline)
                          (declare (ignore client timeline))
                          (pg::string->bytes (store-history-text 1 240)))
                        (symbol-function 'pg::pg-client-native-prepare)
                        (lambda (&rest args) (declare (ignore args)) (incf prepares)
                                (error "observation failure must not prepare")))
                  (let ((pg::*physical-follow-test-hook*
                          (lambda (at)
                            (cond ((eq at mode) (error "observation save cut"))
                                  ((or (and (eq mode :cancel-after-rename)
                                            (eq at :after-follow-config-rename))
                                       (and (eq mode :cancel-after-sync)
                                            (eq at :after-follow-config-directory-fsync)))
                                   (setf (pg::physical-follow-operation-cancelled-p operation) t))))))
                    (pg::physical-follow-operation-run operation))
                  (if (member mode '(:cancel-before :cancel-after-rename :cancel-after-sync))
                      (ok (typep (pg::physical-follow-operation-error operation)
                                 'pg::physical-follow-controller-cancelled))
                      (ok (typep (pg::physical-follow-operation-error operation)
                                 'pg::physical-follow-observation-save-failed)))
                  (ok (null pg::*physical-follow-auto-error*))
                  (ok (> pg::*physical-follow-auto-retry-after* (get-internal-real-time)))
                  (ok (eq runtime pg::*physical-follow-runtime*))
                  (ok (not (pg::physical-follow-runtime-stopped-p runtime)))
                  (ok (not (pg::physical-follow-runtime-receiver-stopped-p runtime)))
                  (ok pg::*physical-replay-paused-p*)
                  (ok (zerop prepares))
                  (ok (equal before (pg::physical-store-control-tuple store)))
                  (ok (not pg::*physical-recovery-failed-p*))
                  (let* ((config (pg::physical-follow-read-config))
                         (known (getf (pg::physical-follow-controller-state config) :known-timeline)))
                    (ok (equal (pg::physical-follow-effective-config old)
                               (pg::physical-follow-effective-config config)))
                    (if (member mode '(:after-follow-config-rename :after-follow-config-directory-fsync
                                       :cancel-after-rename :cancel-after-sync))
                        (ok (eql 19 known))
                        (ok (null known))))
                  (ok (= 19 (pg::physical-promotion-timeline-floor pg::*physical-recovery-state*)))
                  (if (member mode '(:before-follow-config-rename :cancel-before))
                      (progn
                        (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                          (pg::physical-follow-quiesce-runtime))
                        (let ((pending (call-with-promotion-exclusion
                                        (lambda () (pg::physical-promotion-publish-intent)))))
                          (ok (= 20 (getf pending :promotion-child)))
                          (ok (= 19 (getf (pg::physical-follow-controller-state
                                           (pg::physical-follow-read-config)) :known-timeline)))))
                      (let* ((retry (pg::make-physical-follow-operation :runtime runtime))
                             (pg::*physical-follow-operation* retry))
                        (pg::physical-follow-operation-initial-config retry)
                        (pg::physical-follow-operation-remember-source retry 19)
                        (ok (= 19 (getf (pg::physical-follow-controller-state
                                         (pg::physical-follow-read-config)) :known-timeline))))))
             (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))
       :applied 152 :system-id 42)))

(deftest follow-retarget-apis-return-the-not-attached-result
    (let* ((result '(:not-attached :reason :timeline-history-unavailable))
           (operation (pg::make-physical-follow-operation
                       :result result
                       :thread (sb-thread:make-thread (lambda () nil) :name "follow-result-fixture")))
           (names '(pg::physical-follow-ensure-attachment-state
                    pg::physical-follow-controller-start-worker pg::physical-follow-start-operation))
           (saved (mapcar (lambda (name) (cons name (symbol-function name))) names)))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::physical-follow-ensure-attachment-state) (lambda () :ready)
                   (symbol-function 'pg::physical-follow-controller-start-worker)
                   (lambda (&rest args) (declare (ignore args)) nil)
                   (symbol-function 'pg::physical-follow-start-operation)
                   (lambda (&rest args) (declare (ignore args)) operation))
             (ok (equal result (pg::physical-follow-retarget "127.0.0.1" 1 "result")))
             (setf (pg::physical-follow-operation-thread operation)
                   (sb-thread:make-thread (lambda () nil) :name "follow-libpq-result-fixture"))
             (ok (equal result (pg::physical-follow-retarget-conninfo
                                "host=127.0.0.1 port=1" :prefix "result" :fallback "test"))))
        (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))

(deftest cancelling-an-unavailable-fixed-route-does-not-poison-recovery
    (dolist (automatic '(nil t))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (let* ((policy (pg::physical-recovery-resolve-timeline-policy
                         pg::*physical-recovery-state* store nil :current))
                (operation (pg::make-physical-follow-operation
                            :host "127.0.0.1" :port 5433 :prefix "cancel_route"
                            :context (list :automatic-p automatic)))
                (pg::*physical-follow-operation* operation)
                (pg::*physical-follow-controller-blocked-p* nil)
                (pg::*physical-follow-controller-stopping-p* nil)
                (pg::*physical-follow-auto-error* nil)
                (config '(1 "127.0.0.1" 5432 "old_bootstrap"))
                (names '(pg::physical-follow-operation-connect pg::physical-follow-client-identity
                         pg::pg-client-native-history pg::physical-follow-operation-not-attached))
                (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
                (handled 0))
           (setf (getf pg::*physical-recovery-state* :timeline-policy) policy)
           (follow-controller-test-advance store)
           (pg::physical-follow-save-config config)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-operation-connect)
                        (lambda (op) (declare (ignore op)) nil)
                        (symbol-function 'pg::physical-follow-client-identity)
                        (lambda (client) (declare (ignore client)) (values 42 2))
                        (symbol-function 'pg::pg-client-native-history)
                        (lambda (client timeline)
                          (declare (ignore client timeline))
                          (pg::string->bytes (store-history-text 1 216)))
                        (symbol-function 'pg::physical-follow-operation-not-attached)
                        (lambda (op decision)
                          (incf handled)
                          (setf (pg::physical-follow-operation-cancelled-p op) t)
                          (funcall (cdr (assoc 'pg::physical-follow-operation-not-attached saved)) op decision)))
                  (pg::physical-follow-operation-run operation)
                  (ok (= 1 handled))
                  (ok (typep (pg::physical-follow-operation-error operation)
                             'pg::physical-follow-controller-cancelled))
                  (ok (not pg::*physical-recovery-failed-p*))
                  (ok (equal (append config '(nil (:version 1 :enabled t :preparing nil :cleanup nil
                                                   :known-timeline 2)))
                             (pg::physical-follow-read-config)))
                  (ok (= 224 (pg::physical-store-flushed-lsn store))))
             (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))
       :applied 152 :system-id 42)))

(deftest cached-child-fences-receipt-before-network-even-with-full-cleanup
    (dolist (paused '(nil t))
      (call-with-cached-child-follow-config
       (lambda (root store active fork)
         (declare (ignore root fork))
         (let* ((policy (pg::physical-recovery-resolve-timeline-policy
                         (pg::physical-recovery-verify-base-state) store nil :current))
                (state (pg::physical-follow-controller-state active))
                (queue (loop for index below pg::+physical-follow-cleanup-capacity+
                             collect (follow-controller-test-entry (format nil "waiting_~d" index))))
                (config nil)
                (pg::*physical-replay-paused-p* paused)
                (operation (pg::make-physical-follow-operation :host "unused" :port 1 :prefix "no_attach"
                                                               :context '(:requested-slot "new_public")))
                (pg::*physical-follow-operation* operation)
                (pg::*physical-follow-controller-blocked-p* nil)
                (pg::*physical-follow-controller-stopping-p* nil)
                (pg::*physical-follow-auto-error* nil)
                (contacts 0)
                (connect (symbol-function 'pg::physical-follow-operation-connect)))
           (setf (getf state :cleanup) queue (getf state :public-slot) "keep_public"
                 config (pg::physical-follow-with-controller active state)
                 (getf pg::*physical-recovery-state* :timeline-policy) policy)
           (pg::physical-follow-save-config config)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-operation-connect)
                        (lambda (op) (declare (ignore op)) (incf contacts) (error "unexpected network")))
                  (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                    (pg::physical-follow-launch config))
                  (let* ((runtime pg::*physical-follow-runtime*)
                         (replay (pg::physical-follow-runtime-replay-thread runtime)))
                    (pg::physical-follow-operation-run operation)
                    (ok (null (pg::physical-follow-operation-error operation)))
                    (ok (equal '(:not-attached :reason :timeline-target-reached)
                               (pg::physical-follow-operation-result operation)))
                    (ok (zerop contacts))
                    (ok (eq runtime pg::*physical-follow-runtime*))
                    (ok (eq replay (pg::physical-follow-runtime-replay-thread runtime))))
                  (ok (sb-thread:thread-alive-p (pg::physical-follow-runtime-replay-thread pg::*physical-follow-runtime*)))
                  (ok (null (pg::physical-follow-runtime-receiver-thread pg::*physical-follow-runtime*)))
                  (ok (eql paused pg::*physical-replay-paused-p*))
                  (ok (equal config (pg::physical-follow-read-config)))
                  (ok (not pg::*physical-recovery-failed-p*)))
             (setf (symbol-function 'pg::physical-follow-operation-connect) connect)
             (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
               (pg::physical-follow-quiesce-runtime))))))))

(defun follow-policy-test-history (timeline &rest forks)
  (pg::timeline-parse-history
   timeline
   (with-output-to-string (out)
     (dolist (fork forks)
       (format out "~d~c~a~cpromotion~%" (first fork) #\Tab
               (pg::wal-format-lsn (second fork)) #\Tab)))))

(deftest fixed-follow-ancestor-source-wait-preserves-authority-until-source-advances
    (dolist (automatic '(nil t))
      (call-with-promotion-fixture
       (lambda (root store)
         (declare (ignore root store))
         (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
         (let* ((history (store-history-text 1 216))
                (policy (list :target 2 :history (pg::timeline-parse-history 2 history)))
                (old '(1 "127.0.0.1" 5432 "old_bootstrap"))
                (runtime (pg::make-physical-follow-runtime :config old :receiver-stopped-p t))
                (operation (pg::make-physical-follow-operation
                            :host "127.0.0.1" :port 5432 :prefix "waiting"
                            :runtime (when automatic runtime)
                            :context (list :automatic-p automatic)))
                (pg::*physical-follow-runtime* runtime)
                (pg::*physical-follow-operation* operation)
                (pg::*physical-follow-controller-blocked-p* nil)
                (pg::*physical-follow-controller-stopping-p* nil)
                (pg::*physical-follow-auto-error* nil)
                (pg::*physical-follow-auto-retry-after* 0)
                (source 1) (prepared 0)
                (names '(pg::physical-follow-operation-connect pg::physical-follow-client-identity
                         pg::pg-client-native-history pg::pg-client-native-prepare))
                (saved (mapcar (lambda (name) (cons name (symbol-function name))) names)))
           (setf (getf pg::*physical-recovery-state* :timeline-policy) policy)
           (pg::physical-follow-save-config old)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-operation-connect)
                        (lambda (op) (declare (ignore op)) nil)
                        (symbol-function 'pg::physical-follow-client-identity)
                        (lambda (client) (declare (ignore client)) (values 42 source))
                        (symbol-function 'pg::pg-client-native-history)
                        (lambda (client timeline)
                          (declare (ignore client))
                          (assert (= timeline 2))
                          (pg::string->bytes history))
                        (symbol-function 'pg::pg-client-native-prepare)
                        (lambda (&rest args) (declare (ignore args))
                          (incf prepared) (error "unavailable source must not prepare")))
                  (pg::physical-follow-operation-run operation)
                  (ok (null (pg::physical-follow-operation-error operation)))
                  (ok (equal '(:not-attached :reason :timeline-history-unavailable)
                             (pg::physical-follow-operation-result operation)))
                  (ok (zerop prepared))
                  (ok (equal old (pg::physical-follow-read-config)))
                  (ok (eq automatic
                          (not (null (getf (cddr (pg::physical-follow-runtime-transition runtime))
                                           :retry-source)))))
                  (when automatic
                    (ok (> pg::*physical-follow-auto-retry-after* (get-internal-real-time))))
                  (setf source 2)
                  (let ((view (pg::physical-follow-controller-source-view nil policy)))
                    (ok (eq :attach (getf view :action)))
                    (ok (eq :descendant (getf view :proof-kind)))
                    (ok (= 2 (getf view :child))))
                  (ok (not pg::*physical-recovery-failed-p*)))
             (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))
       :applied 152 :system-id 42)))

(deftest fixed-follow-unavailable-source-retries-only-a-target-ancestor
    (let* ((one (follow-policy-test-history 1))
           (two (follow-policy-test-history 2 '(1 64)))
           (other (follow-policy-test-history 3 '(1 96)))
           (policy (list :target 2 :history two)))
      (let ((decision (pg::physical-follow-policy-selection policy one one)))
        (ok (eq :unavailable (getf decision :action)))
        (ok (getf decision :retry-source)))
      (let ((decision (pg::physical-follow-policy-selection policy one other)))
        (ok (eq :unavailable (getf decision :action)))
        (ok (null (getf decision :retry-source))))
      (ok (eq :attach (getf (pg::physical-follow-policy-selection policy one two) :action)))))

(deftest fixed-follow-waiting-source-retry-obeys-deadline-and-accepted-config
    (call-with-retained-follow-config
     (lambda (root store config)
       (declare (ignore root store))
       (let* ((runtime (pg::make-physical-follow-runtime :config config :receiver-stopped-p t))
              (pg::*physical-follow-runtime* runtime)
              (pg::*physical-follow-operation* nil)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (pg::*physical-follow-auto-error* nil)
              (pg::*physical-follow-auto-retry-after* 0)
              (start (symbol-function 'pg::physical-follow-start-operation))
              (attempts nil))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-follow-start-operation)
                      (lambda (&rest args) (push args attempts)))
                (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                  (pg::physical-follow-wait-for-policy runtime :timeline-history-unavailable t))
                (ok (> pg::*physical-follow-auto-retry-after* (get-internal-real-time)))
                (pg::physical-follow-controller-step)
                (ok (null attempts))
                (setf pg::*physical-follow-auto-retry-after* 0)
                (pg::physical-follow-controller-step)
                (ok (equal (list (second config) (third config) "follow" runtime) (first attempts)))
                (ok (= 1 (length attempts)))
                (setf attempts nil)
                (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                  (pg::physical-follow-wait-for-policy runtime :timeline-history-unavailable))
                (pg::physical-follow-controller-step)
                (ok (null attempts))
                (ok (equal config (pg::physical-follow-read-config))))
           (setf (symbol-function 'pg::physical-follow-start-operation) start))))))

(deftest fixed-follow-policy-selects-only-the-path-to-its-target
    (let* ((one (follow-policy-test-history 1))
           (two (follow-policy-test-history 2 '(1 64)))
           (three (follow-policy-test-history 3 '(1 64) '(2 128)))
           (policy (list :target 2 :history two)))
      (dolist (goal (list nil policy))
        (let ((decision (pg::physical-follow-policy-selection goal one three)))
          (ok (eq :attach (getf decision :action)))
          (ok (eq :descendant (getf decision :proof-kind)))
          (ok (= 2 (getf decision :child)))
          (ok (= 64 (getf decision :fork)))))
      (let ((decision (pg::physical-follow-policy-selection policy two three)))
        (ok (eq :attach (getf decision :action)))
        (ok (eq :retained-timeline (getf decision :proof-kind)))
        (ok (= 128 (getf decision :fork)))
        (ok (null (getf decision :child))))
      (let ((decision (pg::physical-follow-policy-selection policy two two)))
        (ok (eq :attach (getf decision :action)))
        (ok (eq :same-timeline (getf decision :proof-kind)))
        (ok (null (getf decision :fork))))
      (let ((decision (pg::physical-follow-policy-selection policy three)))
        (ok (eq :replay-only (getf decision :action)))
        (ok (eq :timeline-target-reached (getf decision :reason))))
      (ok (eq :discover (getf (pg::physical-follow-policy-selection policy one) :action)))
      (dolist (source (list one (follow-policy-test-history 3 '(1 96) '(2 128))))
        (let ((decision (pg::physical-follow-policy-selection policy one source)))
          (ok (eq :unavailable (getf decision :action)))
          (ok (eq :timeline-history-unavailable (getf decision :reason)))))))

(deftest fixed-follow-policy-rejects-equal-timeline-ids-with-different-ancestry
    (let* ((target (follow-policy-test-history 2 '(1 64)))
           (policy (list :target 2 :history target)))
      (dolist (local (list (follow-policy-test-history 2 '(1 32))
                           (follow-policy-test-history 3 '(1 96))))
        (let ((decision (pg::physical-follow-policy-selection policy local)))
          (ok (eq :replay-only (getf decision :action)))
          (ok (eq :timeline-history-unavailable (getf decision :reason)))))))

(cl-spec:defproperty fixed-follow-policy-preserves-gapped-timeline-and-fork-boundaries
    ((target (range integer 2 4294967294))
     (fork (range integer 1 18446744073709551583)))
  (:about pg::physical-follow-policy-selection)
  (:trials (:normal 200))
  (let* ((one (follow-policy-test-history 1))
         (selected (follow-policy-test-history target (list 1 fork)))
         (source (follow-policy-test-history (1+ target) (list 1 fork) (list target (+ fork 32))))
         (policy (list :target target :history selected))
         (toward (pg::physical-follow-policy-selection policy one source))
         (bounded (pg::physical-follow-policy-selection policy selected source))
         (cached (pg::physical-follow-policy-selection policy source)))
    (and (eq :descendant (getf toward :proof-kind))
         (= target (getf toward :child)) (= fork (getf toward :fork))
         (eq :retained-timeline (getf bounded :proof-kind))
         (= (+ fork 32) (getf bounded :fork))
         (eq :replay-only (getf cached :action))
         (eq :timeline-target-reached (getf cached :reason)))))

(deftest generated-fixed-follow-policy-boundaries
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'fixed-follow-policy-preserves-gapped-timeline-and-fork-boundaries :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 200 (getf data :trials)))))

(deftest retained-follow-binding-preserves-the-entire-receive-selection
    (let* ((config (same-timeline-follow-binding-config))
           (binding (fifth config))
           (before (getf binding :before))
           (anchor (retained-timeline-follow-anchor (fourth before) 256 (fifth before))))
      (setf (getf anchor :base) (third before)
            (getf binding :anchor) anchor)
      (ok (pg::physical-follow-binding-check binding))
      (ok (equal config (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))
      (dolist (change '((:after (42 1 64 127 555 64 14695981039346656037 nil nil 77 nil))
                        (:child-history "")))
        (let ((invalid (copy-tree binding)))
          (setf (getf invalid (first change)) (second change))
          (ok (signals (pg::physical-follow-binding-check invalid) 'error))))))

(deftest retained-controller-publication-keeps-parent-history-and-bounds-active-receipt
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((text (store-history-text 1 216))
              (view (list :source 2 :parent 1 :history-text text :fork 216
                          :proof-kind :retained-timeline
                          :history (pg::timeline-parse-history 2 text)))
              (operation (pg::make-physical-follow-operation
                          :host "127.0.0.1" :port 5433 :prefix "bounded"))
              (pg::*physical-follow-operation* operation)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (before (pg::physical-store-control-tuple store)))
         (pg::physical-follow-save-config '(1 "127.0.0.1" 5432 "old_bootstrap"))
         (pg::physical-follow-operation-initial-config operation)
         (multiple-value-bind (entry snapshot) (pg::physical-follow-operation-intent operation view)
           (declare (ignore snapshot))
           (ok (eq :retained-timeline (getf (getf entry :request) :kind)))
           (let* ((anchor (append (copy-list (getf entry :request)) (list :history-text text)))
                  (active (pg::physical-follow-operation-publish operation view entry anchor))
                  (binding (fifth active)))
             (ok (equal before (getf binding :before)))
             (ok (equal before (getf binding :after)))
             (ok (equal before (pg::physical-store-control-tuple store)))
             (ok (null (getf binding :child-history)))
             (ok (null (pg::physical-store-selection store)))
             (ok (pg::physical-follow-check-active-state
                  store (pg::physical-recovery-verify-base-state) active))
             (pg::physical-store-input store 152
                                       (pg::wal-encode-record pg::+wal-record-batch-start+ 0 152 (empty-octets)))
             (pg::physical-store-input store 184 (legacy-checkpoint-record 184))
             (pg::physical-store-flush store)
             (ok (pg::physical-follow-check-active-state
                  store (pg::physical-recovery-verify-base-state) active))
             (let ((candidate (pg::physical-follow-copy-checked-store store)))
               (setf (pg::physical-store-flushed-lsn candidate) 217
                     (pg::physical-store-written-lsn candidate) 217)
               (ok (handler-case
                       (progn (pg::physical-follow-check-active-receive
                               candidate (pg::physical-recovery-verify-base-state) binding)
                              nil)
                     (error (condition)
                       (search "crossed its unselected child fork" (princ-to-string condition))))))))))
     :applied 152 :system-id 42))

(defun call-with-cached-child-follow-config (function &key compact)
  (call-with-promotion-fixture
   (lambda (root store)
     (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
     (pg::wal-atomic-write-bytes (merge-pathnames "PG_VERSION" root)
                                 (pg::string->bytes (format nil "20~%")))
     (when compact
       (let ((initial (pg::physical-recovery-verify-base-state))
             (control (pg::recovery-read-bytes (pg::physical-recovery-control-path root))))
         (let ((frontier (follow-controller-test-advance store)))
           (call-with-promotion-exclusion
            (lambda () (pg::physical-store-compact store frontier #'pg::physical-restartpoint-publish))))
         (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path root) control)
         (setf (getf pg::*physical-recovery-state* :required-applied) (getf initial :required-applied)
               (getf pg::*physical-recovery-state* :required-timeline) (getf initial :required-timeline)
               (getf pg::*physical-recovery-state* :observed-next-xid) (getf initial :observed-next-xid))))
     (let* ((base (pg::physical-recovery-verify-base-state))
            (pending (pg::physical-follow-with-controller
                      (follow-pending-proof-config-for-store store base)
                      (follow-controller-test-state)))
            (fork (pg::physical-store-flushed-lsn store))
            (eor (coerce (getf (getf (fifth pending) :anchor) :eor-octets)
                         '(vector (unsigned-byte 8))))
            (active nil))
       (pg::physical-follow-save-config pending)
       (setf active
             (call-with-promotion-exclusion
              (lambda () (pg::physical-follow-reconcile-pending-store store base pending))))
       (pg::physical-store-input store fork eor)
       (let ((next (+ fork (length eor))))
         (pg::physical-store-input
          store next (pg::wal-encode-record pg::+wal-record-commit+ 3 next
                                            (pg::wal-encode-commit-timestamp 42))))
       (pg::physical-store-flush store)
       (funcall function root store active fork)))
   :applied 152 :system-id 42))

(deftest fixed-follow-active-admission-does-not-interpret-excluded-child-records
    (call-with-cached-child-follow-config
     (lambda (root store active fork)
       (declare (ignore root))
       (let* ((base (pg::physical-recovery-verify-base-state))
              (policy (pg::physical-recovery-resolve-timeline-policy base store nil :current))
              (before (list pg::*next-xid* pg::*last-replay-lsn*
                            (pg::physical-store-control-tuple store))))
         (call-with-promotion-exclusion
          (lambda ()
            (let ((result (pg::physical-follow-check-active-state store base active :policy policy)))
              (ok (= fork (getf result :terminal)))
              (ok (eq :timeline-target-reached (getf result :disposition))))
            (let ((unbound (pg::physical-follow-check-unbound-state
                            store base (list fork 1) :policy policy)))
              (ok (= fork (getf unbound :terminal)))
              (ok (eq :timeline-target-reached (getf unbound :disposition))))
            (ok (signals (pg::physical-follow-check-unbound-state store base (list fork 1) :policy nil) 'error))
            (ok (signals (pg::physical-follow-check-active-state store base active :policy nil) 'error))))
         (ok (equal before (list pg::*next-xid* pg::*last-replay-lsn*
                                 (pg::physical-store-control-tuple store)))))
       (ok (not pg::*physical-recovery-failed-p*)))))

(deftest fixed-follow-fence-preserves-required-xid-and-full-prefix-validation
    (dolist (fault '(:required-xid :child-crc))
      (call-with-cached-child-follow-config
       (lambda (root store active fork)
         (let* ((base (pg::physical-recovery-verify-base-state))
                (policy (pg::physical-recovery-resolve-timeline-policy base store nil :current))
                (path (if (eq fault :required-xid)
                          (pg::physical-recovery-control-path root)
                          (pg::physical-store-segment-path store 0 2)))
                (original (pg::recovery-read-bytes path))
                (visits 0)
                (accept (symbol-function 'pg::physical-history-accept-record)))
           (unwind-protect
                (progn
                  (if (eq fault :required-xid)
                      (pg::physical-recovery-write-control base 2 fork (1+ (getf base :observed-next-xid)))
                      (let ((corrupt (copy-seq original)))
                        (setf (aref corrupt (1- (length corrupt)))
                              (logxor 1 (aref corrupt (1- (length corrupt)))))
                        (pg::wal-atomic-write-bytes path corrupt)
                        (setf (symbol-function 'pg::physical-history-accept-record)
                              (lambda (&rest arguments)
                                (incf visits)
                                (apply accept arguments)))))
                  (call-with-promotion-exclusion
                   (lambda ()
                     (ok (handler-case
                             (progn (pg::physical-follow-check-active-state store base active :policy policy) nil)
                           (error (condition)
                             (or (eq fault :child-crc)
                                 (search "required XID visibility" (princ-to-string condition))))))))
                  (when (eq fault :child-crc) (ok (zerop visits)))
                  (ok (= fork pg::*last-replay-lsn*)))
             (setf (symbol-function 'pg::physical-history-accept-record) accept)
             (pg::wal-atomic-write-bytes path original)))))))

(deftest fixed-follow-fence-validates-archived-required-and-restartpoint-visibility
    (call-with-cached-child-follow-config
     (lambda (root store active fork)
       (declare (ignore root))
       (let ((base (pg::physical-recovery-verify-base-state)))
         (ok (< (getf base :required-applied) (pg::physical-store-floor-lsn store)))
         (ok (= fork (pg::physical-store-floor-lsn store)))
         (multiple-value-bind (metadata catalog prefix digest) (pg::physical-restartpoint-read base)
           (declare (ignore catalog prefix digest))
           (let ((policy (pg::physical-recovery-resolve-timeline-policy base store metadata :current)))
             (call-with-promotion-exclusion
              (lambda ()
                (let ((result (pg::physical-follow-check-active-state store base active :policy policy)))
                  (ok (= fork (getf result :terminal)))
                  (ok (eq :timeline-target-reached (getf result :disposition)))
                  (ok (= 71 (getf result :next-xid)))
                  (ok (= (getf base :observed-next-xid) (getf result :required-next-xid))))))))))
     :compact t))

(deftest cold-fixed-follow-admission-latches-policy-before-semantic-scan
    (dolist (unbound '(nil t))
      (dolist (goal '(:current :latest))
        (call-with-cached-child-follow-config
         (lambda (root store active fork)
           (declare (ignore active))
           (when unbound
             (pg::physical-follow-save-config
              (pg::physical-follow-unattached-config-for-base (pg::physical-recovery-verify-base-state))))
           (let ((control (pg::recovery-read-bytes (pg::physical-recovery-control-path root))))
             (pg::physical-store-close store)
             (sb-posix:rename (namestring (pg::physical-store-directory store))
                              (namestring (pg::physical-recovery-spool-directory)))
             (call-with-current-promotion-owner
              (lambda ()
                (let ((pg::*wal-ready* nil) (pg::*physical-receive-store* nil)
                                            (pg::*physical-recovery-state* nil) (pg::*physical-follow-runtime* nil)
                                            (pg::*physical-restartpoint-worker* nil)
                                            (pg::*wal-checkpoint-snapshot* pg::*wal-checkpoint-snapshot*)
                                            (pg::*wal-poisoned* pg::*wal-poisoned*)
                                            (pg::*active-xids* pg::*active-xids*))
                  (unwind-protect
                       (if (eq goal :latest)
                           (progn
                             (ok (signals (pg::physical-recovery-prepare :timeline-goal goal) 'error))
                             (ok (equalp control (pg::recovery-read-bytes
                                                  (pg::physical-recovery-control-path root)))))
                           (progn
                             (pg::physical-recovery-prepare :timeline-goal goal)
                             (ok (= 1 (getf (getf pg::*physical-recovery-state* :timeline-policy) :target)))
                             (pg::physical-recovery-open-storage)
                             (ok (= fork pg::*last-replay-lsn*))
                             (ok (= 1 (pg::physical-recovery-replay-timeline)))
                             (ok (not pg::*physical-recovery-failed-p*))))
                    (when pg::*clog-stream* (close pg::*clog-stream*) (setf pg::*clog-stream* nil))
                    (when pg::*physical-receive-store*
                      (pg::physical-store-close pg::*physical-receive-store*))))))))))))

(deftest cold-numeric-follow-policy-uses-checked-pending-history-before-publication
    (call-with-promotion-fixture
     (lambda (root store)
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (pg::wal-atomic-write-bytes (merge-pathnames "PG_VERSION" root)
                                   (pg::string->bytes (format nil "20~%")))
       (let* ((base (pg::physical-recovery-verify-base-state))
              (pending (follow-pending-proof-config-for-store store base))
              (fork (pg::physical-store-flushed-lsn store)))
         (pg::physical-follow-save-config pending)
         (ok (null (probe-file (pg::timeline-history-path 2 root))))
         (pg::physical-store-close store)
         (sb-posix:rename (namestring (pg::physical-store-directory store))
                          (namestring (pg::physical-recovery-spool-directory)))
         (call-with-current-promotion-owner
          (lambda ()
            (let ((pg::*wal-ready* nil) (pg::*physical-receive-store* nil)
                                        (pg::*physical-recovery-state* nil) (pg::*physical-follow-runtime* nil)
                                        (pg::*physical-restartpoint-worker* nil)
                                        (pg::*wal-checkpoint-snapshot* pg::*wal-checkpoint-snapshot*)
                                        (pg::*wal-poisoned* pg::*wal-poisoned*)
                                        (pg::*active-xids* pg::*active-xids*))
              (unwind-protect
                   (progn
                     (pg::physical-recovery-prepare :timeline-goal 2)
                     (ok (= 2 (getf (getf pg::*physical-recovery-state* :timeline-policy) :target)))
                     (ok (= 1 (getf (getf pg::*physical-recovery-state* :timeline-policy) :startup-anchor)))
                     (ok (equal (list fork 1)
                                (getf (getf pg::*physical-recovery-state* :timeline-policy) :original-required)))
                     (ok (eq :active (getf (fifth (pg::physical-follow-read-config)) :phase)))
                     (ok (probe-file (pg::timeline-history-path 2 root)))
                     (pg::physical-recovery-open-storage)
                     (ok (= 1 (pg::physical-recovery-replay-timeline)))
                     (ok (= fork pg::*last-replay-lsn*)))
                (when pg::*clog-stream* (close pg::*clog-stream*) (setf pg::*clog-stream* nil))
                (when pg::*physical-receive-store*
                  (pg::physical-store-close pg::*physical-receive-store*))))))))
     :applied 152 :system-id 42))

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
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((runtime (pg::make-physical-follow-runtime))
              (pg::*physical-follow-runtime* runtime)
              (operation (pg::make-physical-follow-operation))
              (pg::*physical-follow-operation* operation)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (pg::*physical-follow-auto-error* nil)
              (pg::*physical-follow-auto-retry-after* 0)
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
         (ok (not pg::*physical-recovery-failed-p*))
         (ok (search "cleanup queue is full"
                     (princ-to-string (pg::physical-follow-operation-error operation))))
         (ok (null (pg::physical-follow-operation-config operation)))
         (ok (null (first pg::*physical-follow-auto-error*)))
         (let* ((state (pg::physical-follow-controller-state config))
                (start (symbol-function 'pg::physical-follow-start-operation))
                (attempts nil))
           (setf (getf state :cleanup) (cdr (getf state :cleanup)))
           (pg::physical-follow-save-config (pg::physical-follow-with-controller config state))
           (setf (pg::physical-follow-runtime-receiver-stopped-p runtime) t
                 (pg::physical-follow-runtime-transition runtime)
                 '(:waiting :timeline-history-unavailable :retry-source t)
                 pg::*physical-follow-auto-retry-after* 0)
           (unwind-protect
                (progn
                  (setf (symbol-function 'pg::physical-follow-start-operation)
                        (lambda (&rest args) (push args attempts)))
                  (pg::physical-follow-controller-step)
                  (ok (= 1 (length attempts)))
                  (ok (equal (list (second config) (third config) "follow" runtime)
                             (first attempts))))
             (setf (symbol-function 'pg::physical-follow-start-operation) start)))))
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

(cl-spec:defproperty follow-known-timeline-survives-controller-roundtrip
    ((known (range integer 1 4294967295))
     (observed (range integer 1 4294967295))
     (enabled (range integer 0 1)))
  (:about pg::physical-follow-decode-config)
  (:trials (:normal 200))
  (let* ((plain '(1 "127.0.0.1" 5432 "known_source"))
         (config (pg::physical-follow-with-controller
                  plain (pg::physical-follow-remember-timeline
                         (append (follow-controller-test-state :enabled (plusp enabled))
                                 (list :known-timeline known)) observed)))
         (decoded (pg::physical-follow-decode-config (pg::physical-follow-config-image config))))
    (and (= (max known observed) (getf (pg::physical-follow-controller-state decoded) :known-timeline))
         (equal (pg::physical-follow-effective-config plain)
                (pg::physical-follow-effective-config decoded)))))

(deftest generated-follow-known-timeline-controller-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'follow-known-timeline-survives-controller-roundtrip :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(cl-spec:defproperty observed-follow-namespace-is-monotonic-and-scoped
    ((seen-a (range integer 1 4294967295))
     (seen-b (range integer 1 4294967295))
     (system-id (range integer 0 18446744073709551614)))
  (:about pg::physical-follow-observe-timeline)
  (:trials (:normal 200))
  (with-physical-store-directory (root)
    (let ((pg::*pgdata* root)
          (pg::*physical-follow-observed-namespace* nil))
      (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
        (pg::physical-follow-observe-timeline system-id seen-a)
        (pg::physical-follow-observe-timeline system-id seen-b)
        (and (= (max seen-a seen-b) (pg::physical-follow-observed-timeline system-id))
             (null (pg::physical-follow-observed-timeline (1+ system-id)))
             (with-physical-store-directory (other)
               (let ((pg::*pgdata* other))
                 (null (pg::physical-follow-observed-timeline system-id)))))))))

(deftest generated-observed-follow-namespace-scope
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'observed-follow-namespace-is-monotonic-and-scoped :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest follow-known-timeline-rejects-invalid-namespace-values
    (dolist (known '(nil 0 -1 4294967296 "2"))
      (ok (signals (pg::physical-follow-controller-copy
                    (append (follow-controller-test-state) (list :known-timeline known)))
                   'error))))

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

(deftest deferred-promotion-resumes-disabled-replay-from-fresh-namespace-metadata
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root store))
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (let* ((old (pg::physical-follow-with-controller
                    '(1 "127.0.0.1" 5432 "accepted_source")
                    (follow-controller-test-state :enabled nil)))
              (fresh (pg::physical-follow-with-controller
                      old (pg::physical-follow-remember-timeline
                           (pg::physical-follow-controller-state old) 19)))
              (runtime (pg::make-physical-follow-runtime :config old))
              (pg::*physical-follow-runtime* runtime)
              (pg::*physical-follow-operation* nil)
              (pg::*physical-follow-controller-blocked-p* nil)
              (pg::*physical-follow-controller-stopping-p* nil)
              (pg::*physical-replay-paused-p* t)
              (names '(pg::physical-promotion-awaiting-child-initialization-p
                       pg::physical-follow-launch-replay-only pg::physical-follow-launch))
              (saved (mapcar (lambda (name) (cons name (symbol-function name))) names))
              (checks 0) (seen nil))
         (pg::physical-follow-save-config fresh)
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::physical-promotion-awaiting-child-initialization-p)
                      (lambda () (> (incf checks) 1))
                      (symbol-function 'pg::physical-follow-launch-replay-only)
                      (lambda (config)
                        (unless (equal config (pg::physical-follow-read-config))
                          (error "replay restore used stale namespace metadata"))
                        (setf seen config)
                        :started)
                      (symbol-function 'pg::physical-follow-launch)
                      (lambda (&rest args) (declare (ignore args))
                        (error "disabled replay must not start a receiver")))
                (call-with-current-promotion-owner
                 (lambda ()
                   (ok (eq :awaiting-child-initialization (pg::physical-recovery-promote)))))
                (ok (equal fresh seen))
                (ok pg::*physical-replay-paused-p*)
                (ok (= 2 (getf pg::*physical-recovery-state* :phase)))
                (ok (not pg::*physical-recovery-failed-p*)))
           (dolist (entry saved) (setf (symbol-function (car entry)) (cdr entry))))))
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

(deftest follow-disabled-failed-preparation-restores-replay-without-reenabling-receipt
    (dolist (bound '(nil t))
      (dolist (paused '(nil t))
        (call-with-promotion-fixture
         (lambda (root store)
           (declare (ignore root))
           (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
           (let* ((base (pg::physical-recovery-verify-base-state))
                  (state (append (follow-controller-test-state
                                  :enabled nil :queue (list (follow-controller-test-entry "previous_cleanup")))
                                 '(:public-slot "held_public")))
                  (config (pg::physical-follow-with-controller
                           (if bound (follow-pending-proof-config-for-store store base)
                               '(1 "127.0.0.1" 5432 "accepted_bootstrap")) state))
                  (operation (pg::make-physical-follow-operation
                              :host "127.0.0.1" :port 5433 :prefix "retry"))
                  (pg::*physical-follow-operation* operation)
                  (pg::*physical-follow-controller-blocked-p* nil)
                  (pg::*physical-follow-controller-stopping-p* nil)
                  (pg::*physical-replay-paused-p* paused)
                  (original (symbol-function 'pg::physical-follow-replay-loop)))
             (unwind-protect
                  (progn
                    (pg::physical-follow-save-config config)
                    (when bound
                      (setf config (call-with-promotion-exclusion
                                    (lambda () (pg::physical-follow-reconcile-pending-store store base config)))))
                    (setf (symbol-function 'pg::physical-follow-replay-loop)
                          (lambda (runtime)
                            (loop until (pg::physical-follow-runtime-stopped-p runtime)
                                  do (sleep 0.005))))
                    (pg::physical-follow-operation-initial-config operation)
                    (let* ((text (if bound (getf (fifth config) :child-history) ""))
                           (source (if bound 2 1))
                           (view (list :source source :parent source :child source
                                       :history-text text :child-history text
                                       :history (pg::timeline-parse-history source text))))
                      (multiple-value-bind (entry snapshot) (pg::physical-follow-operation-intent operation view)
                        (declare (ignore snapshot))
                        (pg::physical-follow-operation-recover operation)
                        (let ((saved (pg::physical-follow-read-config))
                              (runtime pg::*physical-follow-runtime*))
                          (ok (equal (pg::physical-follow-effective-config config)
                                     (pg::physical-follow-effective-config saved)))
                          (ok (not (pg::physical-follow-controller-enabled-p saved)))
                          (ok (null (getf (sixth saved) :preparing)))
                          (ok (equal (append (getf state :cleanup) (list entry))
                                     (getf (sixth saved) :cleanup)))
                          (ok runtime)
                          (when runtime
                            (ok (null (pg::physical-follow-runtime-client runtime)))
                            (ok (null (pg::physical-follow-runtime-receiver-thread runtime)))
                            (ok (sb-thread:thread-alive-p (pg::physical-follow-runtime-replay-thread runtime))))
                          (ok (eql paused pg::*physical-replay-paused-p*))
                          (ok (not pg::*physical-recovery-failed-p*))))))
               (sb-thread:with-mutex (pg::*physical-follow-command-lock*)
                 (pg::physical-follow-quiesce-runtime))
               (setf (symbol-function 'pg::physical-follow-replay-loop) original))))
         :system-id 42))))

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

(deftest cold-current-timeline-policy-is-latched-before-cached-child-redo
    (call-with-promotion-fixture
     (lambda (root store)
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (pg::wal-atomic-write-bytes (merge-pathnames "PG_VERSION" root)
                                   (pg::string->bytes (format nil "20~%")))
       (let* ((base (pg::physical-recovery-verify-base-state))
              (pending (pg::physical-follow-with-controller
                        (follow-pending-proof-config-for-store store base)
                        (follow-controller-test-state :enabled nil)))
              (fork (pg::physical-store-flushed-lsn store))
              (eor (coerce (getf (getf (fifth pending) :anchor) :eor-octets)
                           '(vector (unsigned-byte 8)))))
         (pg::physical-follow-save-config pending)
         (call-with-promotion-exclusion
          (lambda () (pg::physical-follow-reconcile-pending-store store base pending)))
         (pg::physical-store-input store fork eor)
         (pg::physical-store-flush store)
         (let ((flush (pg::physical-store-flushed-lsn store)))
           (pg::physical-store-close store)
           (sb-posix:rename (namestring (pg::physical-store-directory store))
                            (namestring (pg::physical-recovery-spool-directory)))
           (call-with-current-promotion-owner
            (lambda ()
              (let ((pg::*wal-ready* nil) (pg::*physical-receive-store* nil)
                                          (pg::*physical-recovery-state* nil) (pg::*physical-follow-runtime* nil)
                                          (pg::*physical-restartpoint-worker* nil)
                                          (pg::*wal-checkpoint-snapshot* pg::*wal-checkpoint-snapshot*)
                                          (pg::*wal-poisoned* pg::*wal-poisoned*)
                                          (pg::*active-xids* pg::*active-xids*))
                (unwind-protect
                     (progn
                       (pg::physical-recovery-prepare :timeline-goal :current)
                       (let ((policy (getf pg::*physical-recovery-state* :timeline-policy)))
                         (ok (= 1 (getf policy :target) (getf policy :startup-anchor)))
                         (ok (equal (list fork 1) (getf policy :original-required))))
                       (pg::physical-recovery-open-storage)
                       (ok (= fork pg::*last-replay-lsn*))
                       (ok (= 1 (pg::physical-recovery-replay-timeline)))
                       (ok (= 2 (pg::physical-store-selected-timeline pg::*physical-receive-store*)))
                       (ok (= flush (pg::physical-store-flushed-lsn pg::*physical-receive-store*)))
                       (ok (= fork (getf (pg::physical-recovery-read-control root) :applied)))
                       (ok (not pg::*physical-recovery-failed-p*)))
                  (when pg::*clog-stream* (close pg::*clog-stream*) (setf pg::*clog-stream* nil))
                  (when pg::*physical-receive-store* (pg::physical-store-close pg::*physical-receive-store*)))))))))
     :applied 152 :system-id 42))

(deftest fixed-timeline-policy-allows-parent-promotion-with-uninitialized-cached-child
    (call-with-promotion-fixture
     (lambda (root store)
       (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
       (pg::wal-atomic-write-bytes (merge-pathnames "PG_VERSION" root)
                                   (pg::string->bytes (format nil "20~%")))
       (let* ((fork (pg::physical-store-flushed-lsn store))
              (eor (store-history-eor fork 2 1))
              (captured nil)
              (pg::*physical-promotion-stopping-p* nil)
              (pg::*wal-checkpoint-snapshot* pg::*wal-checkpoint-snapshot*)
              (pg::*wal-poisoned* pg::*wal-poisoned*)
              (pg::*active-xids* pg::*active-xids*))
         (pg::physical-store-select-history store 2 (store-history-text 1 fork) eor)
         (setf pg::*physical-recovery-state*
               (pg::physical-recovery-attach-selected-store pg::*physical-recovery-state* store))
         (pg::physical-store-input store fork eor)
         (let ((next (+ fork (length eor))))
           (pg::physical-store-input store next
                                     (pg::wal-encode-record pg::+wal-record-commit+ 3 next (pg::wal-encode-commit-timestamp 42))))
         (pg::physical-store-flush store)
         (pg::physical-recovery-publish-selected-history store)
         (pg::physical-recovery-save-progress fork pg::*next-xid*)
         (multiple-value-bind (metadata catalog prefix digest)
             (pg::physical-restartpoint-read pg::*physical-recovery-state*)
           (declare (ignore catalog prefix digest))
           (setf (getf pg::*physical-recovery-state* :timeline-policy)
                 (pg::physical-recovery-resolve-timeline-policy pg::*physical-recovery-state* store metadata :current)))
         (call-with-current-promotion-owner
          (lambda ()
            (pg::clog-init)
            (unwind-protect
                 (ok (eq :at-intent
                         (block reached
                           (let ((pg::*physical-promotion-test-hook*
                                   (lambda (point)
                                     (when (eq point :after-online-intent)
                                       (setf captured (copy-list pg::*physical-recovery-state*))
                                       (return-from reached :at-intent)))))
                             (pg::physical-recovery-promote)))))
              (when pg::*clog-stream* (close pg::*clog-stream*) (setf pg::*clog-stream* nil)))))
         (ok captured)
         (when captured
           (ok (= 1 (pg::physical-recovery-replay-timeline captured)))
           (ok (= 3 (getf captured :promotion-child)))
           (ok (= fork pg::*last-replay-lsn*)))
         (ok (= 3 (getf (pg::physical-recovery-read-control root) :phase)))
         (pg::physical-store-close store)
         (sb-posix:rename (namestring (pg::physical-store-directory store))
                          (namestring (pg::physical-recovery-spool-directory)))
         (call-with-current-promotion-owner
          (lambda ()
            (let ((pg::*wal-ready* nil) (pg::*physical-recovery-state* nil)
                                        (pg::*physical-receive-store* nil))
              (multiple-value-bind (state metadata prefix generation) (pg::physical-promotion-read-pending-state)
                (ok (null (getf state :timeline-policy)))
                (let ((reopened (pg::physical-promotion-open-pending-store state)))
                  (unwind-protect
                       (ok (eq :validated
                               (call-with-promotion-exclusion
                                (lambda ()
                                  (pg::physical-promotion-validate-cold-state state metadata prefix generation reopened)
                                  (pg::physical-promotion-restore-cold-state state reopened)
                                  (ok (= fork pg::*last-replay-lsn*))
                                  (ok (= 1 (pg::physical-recovery-replay-timeline)))
                                  (ok (= 0 (pg::clog-get 3)))
                                  :validated))))
                    (when pg::*clog-stream* (close pg::*clog-stream*) (setf pg::*clog-stream* nil))
                    (pg::physical-store-close reopened)))))))))
     :applied 152
     :system-id (pg::physical-store-update-digest #xCBF29CE484222325 (pg::string->bytes "history-source"))))

(deftest pending-timeline-policy-still-rejects-later-same-parent-terminal
    (call-with-promotion-fixture
     (lambda (root store)
       (declare (ignore root))
       (pg::clog-init)
       (unwind-protect
            (call-with-current-promotion-owner
             (lambda ()
               (call-with-promotion-exclusion
                (lambda ()
                  (let ((pending (pg::physical-promotion-publish-intent)))
                    (pg::physical-store-input store 152 (sample-physical-record 152))
                    (pg::physical-store-input store 184
                                              (pg::wal-encode-record pg::+wal-record-maintenance+ 0 184 (empty-octets)))
                    (pg::physical-store-flush store)
                    (multiple-value-bind (metadata catalog prefix generation) (pg::physical-restartpoint-read pending)
                      (declare (ignore catalog))
                      (let ((message
                              (handler-case
                                  (progn (pg::physical-promotion-check-progress pending metadata prefix generation 3) nil)
                                (error (condition) (princ-to-string condition)))))
                        (ok (and message (search "promotion must replay complete durable receive backlog" message))))))))))
         (when pg::*clog-stream* (close pg::*clog-stream*) (setf pg::*clog-stream* nil))))
     :applied 152
     :system-id (pg::physical-store-update-digest #xCBF29CE484222325 (pg::string->bytes "history-source"))))

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
