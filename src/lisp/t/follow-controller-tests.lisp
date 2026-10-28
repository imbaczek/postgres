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

(defun call-with-controller-intent (function)
  (call-with-promotion-fixture
   (lambda (root store)
     (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
     (let* ((view (follow-controller-test-view (pg::physical-store-flushed-lsn store)))
            (operation (pg::make-physical-follow-operation :host "127.0.0.1" :port 5433 :prefix "cut"))
            (pg::*physical-follow-operation* operation)
            (pg::*physical-follow-controller-blocked-p* nil)
            (pg::*physical-follow-controller-stopping-p* nil))
       (pg::physical-follow-save-config '(1 "127.0.0.1" 5432 "old_bootstrap"))
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
