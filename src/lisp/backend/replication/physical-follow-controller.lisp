(in-package #:pg)

;;; Native follow transitions and asynchronous source retirement.
;;; Save preparation before remote proof and reconcile pending state before use.
;;; Cancel sockets and join operations outside command, reload and admission locks.

(defstruct (physical-follow-operation (:print-function physical-libpq-print-private-object))
  "One transition and its socket, thread, saved intent and failure context."
  host port prefix runtime config client thread cancelled-p result error context)

(defvar *physical-follow-operation* nil)
(defvar *physical-follow-controller-blocked-p* nil)
(defvar *physical-follow-controller-worker* nil)
(defvar *physical-follow-controller-stopping-p* nil)
(defvar *physical-follow-cleanup-client* nil)
(defvar *physical-follow-controller-errors* (make-hash-table :test #'equal))

(defun physical-follow-overlap-bytes-under-lock (store base index cursor)
  "Reconstruct bytes from the start of the segment containing CURSOR - 1 to CURSOR.
Use the base or checked retained floor prefix and durable spool records. The
caller holds the store mutex and protects the root. The caller hashes these
bytes independently of the carried receive digest."
  (let* ((size +wal-segment-size+) (segment (floor (1- cursor) size))
                                   (start (* segment size)) (length (- cursor start))
                                   (floor (physical-store-floor-lsn store))
                                   (prefix (make-array length :element-type '(unsigned-byte 8)))
                                   (covered 0))
    (unless (and (plusp cursor) (<= floor cursor (physical-store-flushed-lsn store)))
      (error "follow overlap is outside retained receipt"))
    (when (= segment (floor (1- floor) size))
      (let ((anchor
              (if (= floor (getf base :end))
                  (physical-recovery-read-base-overlap base *pgdata*)
                  (let ((entry (car (last (getf index :entries)))))
                    (unless (and entry (= floor (getf (third entry) :end)))
                      (error "follow overlap lacks its checked floor prefix"))
                    (nth-value 1
                               (physical-retained-extent-decode
                                (physical-retained-object-read
                                 (physical-store-retained-directory store)
                                 :extent (first entry) +physical-retained-image-limit+)))))))
        (setf covered (- floor start))
        (unless (= covered (length anchor)) (error "follow floor overlap has the wrong length"))
        (replace prefix anchor)))
    (let ((digest
            (nth-value 1
                       (physical-store-validate-prefix
                        store :digest-cursor cursor
                        :on-record
                        (lambda (record raw)
                          (when (and (= segment (floor (wal-record-start record) size))
                                     (<= (wal-record-end record) cursor))
                            (unless (= covered (- (wal-record-start record) start))
                              (error "follow overlap is not contiguous"))
                            (replace prefix raw :start1 covered)
                            (incf covered (length raw))))))))
      (unless (and digest (= covered length)) (error "follow overlap lacks an exact boundary")))
    prefix))

(defun physical-follow-controller-index (store)
  (when (physical-store-retained-root store)
    (let ((index (physical-retained-inventory-load
                  (physical-store-retained-directory store) (physical-store-retained-root store)
                  (physical-store-retained-identity store) (physical-store-retained-history store))))
      (physical-store-check-retained-coverage store (getf index :entries))
      index)))

(defun physical-follow-controller-snapshot (view)
  "Capture a checked snapshot under a root lease. The caller owns command and
reload exclusion and live-operation quiescence."
  (physical-follow-call-with-receive-root
   *physical-receive-store* (lambda () (physical-follow-controller-snapshot-under-lease view))))

(defun physical-follow-check-unbound-state (store base applied &key (policy (getf *physical-recovery-state* :timeline-policy)))
  "Validate an unbound receive store and replay prefix against the verified base.
The caller excludes mutation; the scan protects retained data with a root lease.
The returned semantic scan disposition does not update replay exhaustion."
  (physical-follow-call-with-receive-root
   store
   (lambda ()
     (let* ((candidate (physical-follow-copy-checked-store store))
            (tuple (physical-store-control-tuple candidate))
            (control (physical-recovery-read-control *pgdata*))
            (required (list (getf control :applied)
                            (getf control :applied-timeline (getf control :timeline)))))
       (unless (and (= (first tuple) (getf base :system-id))
                    (= (second tuple) (getf base :timeline))
                    (= (third tuple) (getf base :end))
                    (= (nth 9 tuple) (getf base :manifest-digest)))
         (error "current receiver differs from its immutable base"))
       (physical-store-validate-prefix candidate)
       (physical-follow-scan-current-state
        candidate base applied required control (physical-follow-controller-index candidate) nil policy)))))

(defun physical-follow-controller-check-current (config)
  "Validate the current recovery and receive state before restoring its receiver.
The caller excludes concurrent mutation and protects the selected store."
  (let ((base (physical-recovery-verify-base-state)))
    (if (fifth config)
        (physical-follow-check-active-state *physical-receive-store* base config)
        (physical-follow-check-unbound-state
         *physical-receive-store* base
         (list *last-replay-lsn* (physical-recovery-replay-timeline))))))

(defun physical-follow-launch-replay-only (config)
  "Start replay without a receiver for reconciled, disabled standby state.
An unattached, accepted bootstrap or active binding may be used; the saved
configuration, base and receive proof are checked before workers start. The
accepted binding and source owner remain intact. Enabled, preparing, pending
and primary cleanup-only states are rejected.
The caller holds the command mutex outside admission; validation also takes
reload exclusion and live-operation quiescence."
  (check-physical-recovery-worker-start)
  (physical-follow-check-config config)
  (unless (and (not (physical-follow-cleanup-only-p config))
               (not (physical-follow-controller-enabled-p config))
               (not (getf (physical-follow-controller-state config) :preparing))
               (or (null (fifth config)) (eq :active (getf (fifth config) :phase)))
               (recovery-in-progress-p) *physical-receive-store*
               (eql 2 (getf *physical-recovery-state* :phase))
               (zerop *live-operation-depth*)
               (sb-thread:holding-mutex-p *physical-follow-command-lock*))
    (error "replay-only launch requires reconciled disabled streaming recovery"))
  (sb-thread:with-mutex (*reload-run-lock*)
    (with-live-operation-quiescence
      (unless (equal config (physical-follow-read-config))
        (error "follow configuration changed before replay-only launch"))
      (physical-follow-check-unowned-bootstrap-base config (physical-recovery-verify-base-state))
      (physical-follow-controller-check-current config)))
  (physical-follow-launch-workers config nil))

(define-condition physical-follow-route-unavailable (error)
  ((reason :initarg :reason :reader physical-follow-route-reason))
  (:report (lambda (condition stream)
             (format stream "native follow route is unavailable: ~a"
                     (physical-follow-route-reason condition)))))

(defun physical-follow-controller-snapshot-under-lease (view)
  "Capture the receive, replay and durable recovery witnesses under root protection.
The receive timeline can differ from the applied replay timeline.  A retained
proof kind is included in the saved receiver coordinates."
  (let* ((base (physical-recovery-verify-base-state))
         (control (physical-recovery-read-control *pgdata*))
         (store (physical-follow-copy-checked-store *physical-receive-store*))
         (before (physical-store-control-tuple store))
         (applied (list *last-replay-lsn* (physical-recovery-replay-timeline)))
         (required (list (getf control :applied)
                         (getf control :applied-timeline (getf control :timeline))))
         (limit (if (= (getf view :source) (getf view :parent))
                    (fourth before) (getf view :fork)))
         (cursor (if (eq :retained-timeline (getf view :proof-kind))
                     (fourth before) (min (fourth before) limit)))
         (index (physical-follow-controller-index store))
         (coordinates nil))
    (unless (and (eql 2 (getf control :phase)) (eql 3 (getf control :control-version)))
      (error "follow transition requires current streaming recovery control"))
    (unless (= (physical-store-selected-timeline store) (getf view :parent))
      (error "receive selection changed during source discovery"))
    (sb-thread:with-mutex ((physical-store-mutex store))
      (when (getf view :fixed-policy)
        (let ((local-history (physical-store-retained-history store))
              (flush (physical-store-flushed-lsn store)))
          (physical-store-check-follow-frontier store applied local-history flush :applied t)
          (physical-store-check-follow-frontier store required local-history flush)
          (when (or (> (max (physical-store-floor-lsn store) (first applied) (first required)) limit)
                    (and (eq :retained-timeline (getf view :proof-kind)) (> flush limit)))
            (error 'physical-follow-route-unavailable :reason :source-history-exhausted))))
      (physical-follow-check-source-history store (getf view :history))
      (physical-store-check-follow-frontier store applied (getf view :history) limit :applied t)
      (physical-store-check-follow-frontier store required (getf view :history) limit)
      (unless (<= (physical-store-floor-lsn store) cursor) (error "source fork precedes receive floor"))
      (setf coordinates
            (list :version 1 :system-id (getf base :system-id)
                  :base-timeline (getf base :timeline) :base (getf base :end)
                  :manifest-digest (getf base :manifest-digest)
                  :source-timeline (getf view :source) :receive-timeline (getf view :parent)
                  :receive-lsn cursor :receive-digest (physical-store-prefix-digest-under-lock store cursor)
                  :overlap-start (* (floor (1- cursor) +wal-segment-size+) +wal-segment-size+)
                  :overlap-digest
                  (physical-store-update-digest
                   #xCBF29CE484222325 (physical-follow-overlap-bytes-under-lock store base index cursor)))))
    (when (eq :retained-timeline (getf view :proof-kind))
      (setf coordinates (physical-follow-request-copy (append '(:kind :retained-timeline) coordinates))))
    (multiple-value-bind (metadata catalog prefix digest) (physical-restartpoint-read base)
      (declare (ignore catalog prefix))
      (list :base base :before before :applied applied :required required :coordinates coordinates
            :recovery (list :phase 2 :cluster-identity (getf base :cluster-identity)
                            :checkpoint (getf base :checkpoint) :observed-next-xid (getf control :next-xid)
                            :history-target (getf control :history-target)
                            :history-digest (getf control :history-digest)
                            :restartpoint (when metadata (list (getf metadata :lsn) digest)))))))

(defun physical-follow-controller-plan (snapshot anchor view)
  "Build a binding; same-timeline and retained follow keep the receive tuple.
A retained binding carries no child history and cannot pass its fork."
  (let* ((before (getf snapshot :before)) (fork (getf view :fork))
                                          (after
                                            (if (or (member (physical-follow-anchor-kind anchor) '(:same-timeline :retained-timeline))
                                                    (< (fourth before) fork))
                                                (copy-list before)
                                                (sb-thread:with-mutex ((physical-store-mutex *physical-receive-store*))
                                                  (physical-store-control-tuple
                                                   (physical-store-follow-candidate
                                                    *physical-receive-store* before (getf snapshot :applied) (getf snapshot :required)
                                                    (getf view :child) (getf view :child-history)
                                                    (coerce (getf anchor :eor-octets) '(vector (unsigned-byte 8))))))))
                                          (binding (list :version 1 :phase :pending :anchor anchor
                                                         :child-history (getf view :child-history) :before before :after after
                                                         :applied (getf snapshot :applied) :required (getf snapshot :required)
                                                         :recovery (getf snapshot :recovery))))
    (physical-follow-binding-check binding)
    binding))

(defun physical-follow-controller-source-view (client &optional (policy (getf *physical-recovery-state* :timeline-policy)) observer)
  "Check source identity and history against the captured policy and local lineage.
Return a non-attach decision when the source cannot serve the selected route;
fetch child history only for a descendant attachment.  After identity and
source history validation, OBSERVER records the timeline before route selection."
  (multiple-value-bind (system-id source) (physical-follow-client-identity client)
    (let* ((local-history
             (with-live-operation
               (unless (= system-id (getf *physical-recovery-state* :system-id))
                 (physical-follow-reject "follow target has a different system identifier"))
               (sb-thread:with-mutex ((physical-store-mutex *physical-receive-store*))
                 (physical-store-retained-history *physical-receive-store*))))
           (parent (timeline-history-entry-timeline (first local-history))))
      (when (and (null (getf policy :target)) (< source parent))
        (error "follow target timeline precedes the receive selection"))
      (let* ((text (if (= source 1) (string->bytes "")
                       (sb-ext:with-timeout 10 (pg-client-native-history client source))))
             (history (timeline-check-lineage (timeline-parse-history source (bytes->string text))))
             (view (progn
                     (when observer (funcall observer source system-id))
                     (physical-follow-policy-selection policy local-history history))))
        (unless (eq :attach (getf view :action))
          (return-from physical-follow-controller-source-view view))
        (setf (getf view :history-text) (bytes->string text))
        (when (getf policy :target) (setf (getf view :fixed-policy) policy))
        (when (eq :descendant (getf view :proof-kind))
          (let* ((child (getf view :child))
                 (child-text (if (= child source) text
                                 (sb-ext:with-timeout 10 (pg-client-native-history client child)))))
            (unless (physical-store-history-extends-p
                     (timeline-check-lineage (timeline-parse-history child (bytes->string child-text))) history)
              (error "follow child history differs from source ancestry"))
            (setf (getf view :child-history) (bytes->string child-text))))
        view))))

(defun physical-follow-current-retirement (config base)
  "Copy the current source identity for later asynchronous retirement."
  (when (physical-follow-no-source-p config)
    (return-from physical-follow-current-retirement nil))
  (let* ((binding (fifth config))
         (request
           (if binding
               (physical-follow-anchor-request (getf binding :anchor))
               (list :version 1 :system-id (getf base :system-id)
                     :source-timeline (getf base :timeline) :base (getf base :end)
                     :manifest-digest (getf base :manifest-digest)))))
    (physical-follow-cleanup-with-owner
     (list :kind (if binding :follow :bootstrap) :host (second config) :port (third config)
           :pin (fourth config) :request request)
     (getf (physical-follow-controller-state config) :source))))

(defun physical-follow-unattached-config-for-base (base)
  "Create a no-source standby configuration retaining the unowned base pin."
  (physical-follow-check-config
   (list 1 nil nil nil nil
         (list :version 1 :enabled nil :preparing nil :cleanup nil
               :mode :standby-unattached
               :unowned-bootstrap (physical-follow-unowned-bootstrap-from-base base)))))

(defun physical-follow-promotion-base ()
  "Check base provenance while the manifest and recovery control still exist.
This also runs after child commit, before recovery artifacts are removed."
  (multiple-value-bind (manifest digest) (recovery-read-manifest *pgdata*)
    (recovery-check-manifest-schema manifest)
    (let ((control (physical-recovery-read-control *pgdata*)))
      (unless (and (= (getf manifest :system-id) (getf control :system-id))
                   (= (getf manifest :timeline) (getf control :timeline))
                   (= digest (getf control :manifest-digest))
                   (= (getf manifest :checkpoint) (getf control :checkpoint)))
        (error "promotion cleanup base identity does not match recovery control")))
    (let ((base (copy-list manifest)))
      (setf (getf base :manifest-digest) digest)
      base)))

(defun physical-follow-promotion-cleanup-config ()
  "Build the post-promotion cleanup image before retiring base provenance.
The caller must save it durably before removing recovery artifacts. Remote
retirement may finish later. Preserve the unowned bootstrap reference even after
base provenance is removed; it does not identify a cleanup endpoint."
  (let* ((base (unless (probe-file (physical-follow-config-path))
                 (physical-follow-promotion-base)))
         (config (if base (physical-follow-unattached-config-for-base base)
                     (physical-follow-read-config)))
         (state (physical-follow-remember-config-timeline config)))
    (when (physical-follow-cleanup-only-p config)
      (return-from physical-follow-promotion-cleanup-config config))
    (when (or (getf state :preparing) (eq :pending (getf (fifth config) :phase)))
      (error "promotion requires a reconciled follow configuration"))
    (when (and (not base)
               (or (not (fifth config)) (getf state :unowned-bootstrap)))
      (setf base (physical-follow-promotion-base)))
    (physical-follow-check-unowned-bootstrap-base config base)
    (let ((retired (physical-follow-current-retirement config base)))
      (setf (getf state :enabled) nil)
      (when retired
        (setf state (physical-follow-enqueue-cleanup state retired))))
    (remf state :mode)
    (remf state :source)
    (remf state :public-slot)
    (physical-follow-check-config (list 1 nil nil nil nil state))))

(defun physical-follow-startup-owner-check ()
  (when (probe-file (physical-follow-config-path))
    (let ((config (physical-follow-read-config)))
      (unless (= 4 (length config))
        (unless (and *postmaster-data-lock* (postmaster-pid-owned-p)
                     (not *wal-ready*) (not *physical-receive-store*)
                     (not *physical-follow-runtime*) (not *physical-restartpoint-worker*)
                     (zerop *live-operation-depth*))
          (error "bound follow startup requires owned cold PGDATA"))
        (when (physical-follow-cleanup-only-p config)
          (error "cleanup-only metadata cannot select standby recovery")))
      config)))

(defun physical-follow-call-with-offline-store (base function)
  (let ((store (physical-store-open
                (physical-recovery-spool-directory) (getf base :system-id) (getf base :timeline)
                :start-lsn (getf base :end) :discard-tail nil :create-lock nil
                :retained-identity (physical-recovery-retained-identity base)
                :retained-directory (physical-recovery-retained-directory))))
    (unwind-protect (funcall function store) (physical-store-close store))))

(defun physical-follow-call-with-startup-candidate (base function)
  "Call FUNCTION with the strictly checked prospective pending store, if any.
Use the offline store when no pending selection exists. This permits numeric
policy resolution before child history is published.  The
pending check holds command and reload exclusion, quiescence and a root lease.
The candidate owns no streams; the original offline store closes on unwind."
  (physical-follow-call-with-offline-store
   base
   (lambda (store)
     (let ((config (when (probe-file (physical-follow-config-path))
                     (physical-follow-read-config))))
       (if (and (fifth config) (eq :pending (getf (fifth config) :phase)))
           (sb-thread:with-mutex (*physical-follow-command-lock*)
             (sb-thread:with-mutex (*reload-run-lock*)
               (with-live-operation-quiescence
                 (physical-follow-call-with-receive-root
                  store
                  (lambda ()
                    (let ((candidate (physical-follow-check-pending-state store base config)))
                      (funcall function candidate)))))))
           (funcall function store))))))

(defun physical-follow-prepare-startup (base &key (policy (getf base :timeline-policy)))
  "Under cold PGDATA ownership, queue unfinished preparation and reconcile pending
state before validating an active binding. For an unattached standby, validate
the unowned base reference and unbound store before recovery control publication
or redo.
The resolved policy bounds active and unbound admission.  Pending reconciliation
uses strict candidate proof; protect the store and retained root."
  (when (probe-file (physical-follow-config-path))
    (let ((config (physical-follow-read-config)))
      (when (= 4 (length config)) (return-from physical-follow-prepare-startup config))
      (physical-follow-check-unowned-bootstrap-base config base)
      (sb-thread:with-mutex (*physical-follow-command-lock*)
        (sb-thread:with-mutex (*reload-run-lock*)
          (with-live-operation-quiescence
            (physical-follow-call-with-offline-store
             base
             (lambda (store)
               (when (physical-follow-unattached-p config)
                 (physical-follow-check-unbound-state
                  store base (list (getf base :required-applied)
                                   (getf base :required-timeline)) :policy policy))
               (when (getf (physical-follow-controller-state config) :preparing)
                 (when (fifth config)
                   (physical-follow-check-active-state store base config :policy policy))
                 (let ((next (physical-follow-abandon-preparation-config config)))
                   (physical-follow-save-config next)
                   (unless (equal next (physical-follow-read-config))
                     (error "startup preparation-abort readback differs"))
                   (setf config next)))
               (when (and (fifth config) (eq :pending (getf (fifth config) :phase)))
                 (setf config (physical-follow-reconcile-pending-store store base config)))
               (when (fifth config)
                 (physical-follow-check-active-state store base config :policy policy))
               config))))))))

(defun physical-follow-check-startup-admission (state)
  (let ((saved (getf state :follow-startup-config))
        (current (when (probe-file (physical-follow-config-path)) (physical-follow-read-config))))
    (unless (or (equal saved current)
                (and (null saved) current (= 4 (length current))))
      (error "follow configuration changed after startup validation"))
    (when current
      (physical-follow-check-config current)
      (when (or (physical-follow-cleanup-only-p current)
                (getf (physical-follow-controller-state current) :preparing)
                (and (fifth current) (not (eq :active (getf (fifth current) :phase)))))
        (error "follow startup phase was not admitted"))))
  t)

(define-condition physical-follow-controller-cancelled (simple-error) ())

(defvar *physical-follow-controller-block-owner* nil)

(defvar *physical-follow-auto-error* nil)

(defvar *physical-follow-auto-retry-after* 0)

(defun physical-follow-operation-check (operation)
  "Require the current uncancelled operation and its captured policy.
A policy replacement cancels the operation before it can publish or launch."
  (sb-thread:with-mutex (*physical-follow-lock*)
    (unless (and (eq operation *physical-follow-operation*)
                 (not (physical-follow-operation-cancelled-p operation))
                 (not *physical-follow-controller-blocked-p*)
                 (not *physical-follow-controller-stopping-p*))
      (error 'physical-follow-controller-cancelled :format-control "native follow transition was canceled")))
  (let ((context (physical-follow-operation-context operation)))
    (when (and (getf context :policy-captured-p)
               (not (eq (getf context :timeline-policy)
                        (getf *physical-recovery-state* :timeline-policy))))
      (error 'physical-follow-controller-cancelled :format-control "native follow timeline policy changed")))
  (check-physical-recovery-worker-start)
  (unless (and (recovery-in-progress-p) *physical-receive-store*
               (eql 2 (getf *physical-recovery-state* :phase)))
    (error "native retarget requires streaming recovery"))
  t)

(defun physical-follow-controller-save-config (config)
  (physical-follow-save-config config)
  (unless (equal config (physical-follow-read-config))
    (error "native follow controller config readback differs"))
  config)

(defun physical-follow-operation-initial-config (operation)
  "Capture the policy and check the local route before contacting the source.
Only a route needing discovery requires cleanup capacity.  The captured policy
object remains the operation's authority until completion or cancellation."
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (with-live-operation
      (physical-follow-operation-check operation)
      (let* ((config (physical-follow-read-config))
             (state (physical-follow-controller-state config))
             (runtime *physical-follow-runtime*))
        (when (or (physical-follow-cleanup-only-p config) (getf state :preparing)
                  (and (fifth config) (not (eq :active (getf (fifth config) :phase)))))
          (error "native follow transition is already pending or has no current source"))
        (physical-follow-check-unowned-bootstrap-base
         config (physical-recovery-verify-base-state))
        (let* ((policy (getf *physical-recovery-state* :timeline-policy))
               (decision (sb-thread:with-mutex ((physical-store-mutex *physical-receive-store*))
                           (physical-follow-policy-selection
                            policy (physical-store-retained-history *physical-receive-store*)))))
          (setf (getf (physical-follow-operation-context operation) :timeline-policy) policy
                (getf (physical-follow-operation-context operation) :policy-captured-p) t
                (getf (physical-follow-operation-context operation) :local-decision) decision)
          (when (and (eq :discover (getf decision :action))
                     (>= (length (getf state :cleanup)) +physical-follow-cleanup-capacity+))
            (error "native follow cleanup queue is full")))
        (when (and (physical-follow-operation-runtime operation)
                   (not (eq runtime (physical-follow-operation-runtime operation))))
          (error "native follow transition refers to a replaced receiver"))
        (setf (physical-follow-operation-runtime operation) runtime
              (physical-follow-operation-config operation) config)
        config))))

(defun physical-follow-operation-register-client (operation client)
  "Publish the child early so cancellation can stop it, checking for late cancellation."
  (sb-thread:with-mutex (*physical-follow-lock*)
    (when (or (physical-follow-operation-cancelled-p operation)
              *physical-follow-controller-blocked-p* *physical-follow-controller-stopping-p*)
      (error 'physical-follow-controller-cancelled
             :format-control "native follow connection was canceled"))
    (setf (physical-follow-operation-client operation) client))
  client)

(defun physical-follow-operation-connect (operation)
  "Connect the requested source and register its client for cancellation."
  (let ((client nil) (accepted nil)
                     (profile (getf (physical-follow-operation-context operation) :requested-profile)))
    (unwind-protect
         (progn
           (if profile
               (progn
                 (setf client
                       (physical-libpq-connect
                        (physical-libpq-helper-path) profile
                        :on-spawn (lambda (channel)
                                    (setf client channel)
                                    (physical-follow-operation-register-client operation channel))))
                 (let* ((info (physical-libpq-channel-info client))
                        (host (physical-libpq-display-octets
                               (physical-libpq-connection-info-host info)))
                        (owner (physical-libpq-frozen-profile-datum
                                (make-physical-libpq-frozen-profile info))))
                   (setf (physical-follow-operation-host operation)
                         (subseq host 0 (min 255 (length host)))
                         (physical-follow-operation-port operation)
                         (physical-libpq-connection-info-port info)
                         (getf (physical-follow-operation-context operation) :accepted-source)
                         (physical-follow-source-copy
                          (list :conninfo (physical-libpq-profile-conninfo-datum profile)
                                :fallback (physical-libpq-profile-fallback profile)
                                :owner owner)))))
               (progn
                 (setf client (sb-ext:with-timeout 10
                                (pg-client-connect (physical-follow-operation-host operation)
                                                   (physical-follow-operation-port operation) "postgres"
                                                   :application-name "pg_lisp_follow_prepare"
                                                   :replication-p t)))
                 (setf (sb-bsd-sockets:sockopt-tcp-nodelay (pg-client-socket client)) t)
                 (physical-follow-operation-register-client operation client)))
           (setf accepted t)
           client)
      (unless accepted
        (physical-client-close client)
        (sb-thread:with-mutex (*physical-follow-lock*)
          (when (eq client (physical-follow-operation-client operation))
            (setf (physical-follow-operation-client operation) nil)))))))

(defun physical-follow-fresh-attempt-name (prefix config)
  "Generate an attempt pin distinct from every retained pin name."
  (unless (and (wal-valid-replication-slot-name-p prefix) (<= (length prefix) 30))
    (error "follow pin prefix must be a valid name of at most 30 characters"))
  (let ((state (physical-follow-controller-state config)))
    (loop for name = (format nil "~a_~(~32,'0x~)" prefix (random (ash 1 128) (make-random-state t)))
          unless (or (equal name (fourth config))
                     (equal name (getf (getf state :preparing) :pin))
                     (equal name (getf (getf state :unowned-bootstrap) :pin))
                     (find name (getf state :cleanup)
                           :key (lambda (entry) (getf entry :pin)) :test #'equal))
            return name)))

(defun physical-follow-preparation-entry (operation slot coordinates)
  (physical-follow-cleanup-with-owner
   (list :kind :follow :host (physical-follow-operation-host operation)
         :port (physical-follow-operation-port operation) :pin slot :request coordinates)
   (getf (physical-follow-operation-context operation) :accepted-source)))

(defun physical-follow-operation-source-state (operation state)
  (let ((copy (physical-follow-controller-copy state))
        (source (getf (physical-follow-operation-context operation) :accepted-source))
        (slot (physical-follow-public-slot-copy
               (getf (physical-follow-operation-context operation) :requested-slot))))
    (if source
        (setf (getf copy :source) (physical-follow-source-copy source))
        (remf copy :source))
    (if slot (setf (getf copy :public-slot) slot) (remf copy :public-slot))
    copy))

(define-condition physical-follow-observation-save-failed (error)
  ((cause :initarg :cause :reader physical-follow-observation-save-cause))
  (:documentation "A retryable failure to persist an observed namespace bound.")
  (:report (lambda (condition stream)
             (format stream "could not save observed follow timeline: ~a"
                     (physical-follow-observation-save-cause condition)))))

(defun physical-follow-operation-current-config (operation)
  "Recheck operation ownership and effective route under command exclusion.
Use the fresh saved config so namespace-only changes are retained."
  (unless (sb-thread:holding-mutex-p *physical-follow-command-lock*)
    (error "follow operation state check requires command exclusion"))
  (physical-follow-operation-check operation)
  (let ((current (physical-follow-read-config))
        (expected (physical-follow-operation-config operation)))
    (unless (and (equal (physical-follow-effective-config current)
                        (physical-follow-effective-config expected))
                 (eq *physical-follow-runtime* (physical-follow-operation-runtime operation)))
      (error "current follow receiver changed during source discovery"))
    current))

(defun physical-follow-operation-remember-source (operation timeline &optional (system-id (getf *physical-recovery-state* :system-id)))
  "Record a checked source ID before attempting its durable config update.
The process-local maximum survives cancellation or a pre-rename save failure.
The saved bound grants no source or WAL authority and creates no PREPARE debt."
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (unless (= system-id (getf *physical-recovery-state* :system-id))
      (error "observed follow source belongs to a different cluster"))
    (when (eql 2 (getf *physical-recovery-state* :phase))
      (physical-follow-observe-timeline system-id timeline))
    (with-live-operation
      (let ((current (physical-follow-operation-current-config operation)))
        (let* ((state (physical-follow-remember-timeline
                       (physical-follow-remember-config-timeline current)
                       (or (physical-follow-observed-timeline system-id) timeline)))
               (known (getf state :known-timeline)))
          (when (> known (or (getf (physical-follow-controller-state current) :known-timeline) 1))
            (setf current (physical-follow-with-controller current state))
            (physical-follow-config-image current)
            (handler-case (physical-follow-controller-save-config current)
              (error (condition)
                (error 'physical-follow-observation-save-failed :cause condition))))
          (setf (physical-follow-operation-config operation) current)
          (physical-follow-operation-check operation)
          known)))))

(defun physical-follow-operation-intent (operation view)
  "Stop the old runtime and save preparation intent before contacting PREPARE.
Hold command exclusion; snapshot under reload and live-operation quiescence.
The preparation entry records the selected owner before the RPC. Once accepted,
that owner stays with the pin until a new publication, not a later reconnect."
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (let ((current (physical-follow-operation-current-config operation)))
      (setf (physical-follow-operation-config operation) current)
      (handler-case (physical-follow-quiesce-runtime)
        (error (condition) (setf *physical-recovery-failed-p* t) (error condition)))
      (setf (getf (physical-follow-operation-context operation) :stopped) t)
      (sb-thread:with-mutex (*reload-run-lock*)
        (with-live-operation-quiescence
          (physical-follow-operation-check operation)
          (let* ((snapshot (physical-follow-controller-snapshot view))
                 (state (physical-follow-controller-state current))
                 (slot (physical-follow-fresh-attempt-name (physical-follow-operation-prefix operation) current))
                 (entry (physical-follow-preparation-entry operation slot (getf snapshot :coordinates)))
                 (intent nil))
            (setf (getf state :preparing) entry
                  intent (physical-follow-with-controller current state))
            (physical-follow-config-image (physical-follow-abandon-preparation-config intent))
            (let ((preview (physical-follow-operation-source-state operation state)))
              (if (physical-follow-unattached-p intent)
                  (progn
                    (remf preview :mode)
                    (setf (getf preview :preparing) nil)
                    (physical-follow-config-image
                     (physical-follow-with-controller
                      (list 1 (getf entry :host) (getf entry :port) (getf entry :pin) nil)
                      preview)))
                  (physical-follow-config-image
                   (physical-follow-with-controller intent preview))))
            (setf (getf (physical-follow-operation-context operation) :intent-write) t)
            (physical-follow-controller-save-config intent)
            (setf (physical-follow-operation-config operation) intent
                  (getf (physical-follow-operation-context operation) :intent) t)
            (physical-follow-test-point :after-controller-intent)
            (values entry snapshot)))))))

(defun physical-follow-operation-publish (operation view entry anchor)
  "Recheck a fresh snapshot and publish pending then active binding under quiescence.
A restartpoint can advance during the RPC; use the final durable witness. Queue
the old source before publication; its remote acknowledgment is not required.
With workers quiesced, strictly validate the candidate before writing pending
state. Receipt resumes only after activation."
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (sb-thread:with-mutex (*reload-run-lock*)
      (with-live-operation-quiescence
        (physical-follow-operation-check operation)
        (unless (equal (physical-follow-operation-config operation) (physical-follow-read-config))
          (error "native follow intent changed during source preparation"))
        (let* ((snapshot (physical-follow-controller-snapshot view))
               (request (getf snapshot :coordinates))
               (old (physical-follow-operation-config operation))
               (binding (physical-follow-controller-plan snapshot anchor view))
               (state (physical-follow-remember-config-timeline old))
               (retired (physical-follow-current-retirement old (getf snapshot :base)))
               (pending nil))
          (unless (and (equal request (getf entry :request))
                       (equal (getf anchor :history-text) (getf view :history-text)))
            (error "native follow prefix or source history changed during preparation"))
          (remf state :mode)
          (setf state (physical-follow-remember-timeline
                       (physical-follow-operation-source-state operation state)
                       (getf anchor :source-timeline)))
          (setf (getf state :preparing) nil
                (getf state :enabled) t
                state (if retired (physical-follow-enqueue-cleanup state retired) state)
                pending (physical-follow-with-controller
                         (list 1 (getf entry :host) (getf entry :port) (getf entry :pin) binding) state))
          (let ((candidate
                  (if (equal (getf binding :before) (getf binding :after))
                      (physical-follow-copy-checked-store *physical-receive-store*)
                      (sb-thread:with-mutex ((physical-store-mutex *physical-receive-store*))
                        (physical-store-follow-candidate
                         *physical-receive-store* (getf binding :before)
                         (getf binding :applied) (getf binding :required) (getf view :child)
                         (getf view :child-history)
                         (coerce (getf anchor :eor-octets) '(vector (unsigned-byte 8))))))))
            (physical-follow-validate-candidate-replay candidate (getf snapshot :base) binding))
          (physical-follow-operation-check operation)
          (setf (getf (physical-follow-operation-context operation) :pending-write) t)
          (physical-follow-controller-save-config pending)
          (physical-follow-test-point :after-controller-pending)
          (let ((active (physical-follow-reconcile-pending-store
                         *physical-receive-store* (getf snapshot :base) pending)))
            (physical-follow-check-active-state *physical-receive-store* (getf snapshot :base) active)
            (setf *physical-recovery-state*
                  (physical-recovery-attach-selected-store *physical-recovery-state* *physical-receive-store*)
                  (getf *physical-recovery-state* :receive-lsn)
                  (physical-store-flushed-lsn *physical-receive-store*)
                  (getf (physical-follow-operation-context operation) :committed) t
                  (physical-follow-operation-result operation) active)
            active))))))

(defun physical-follow-operation-recover (operation)
  "Queue a saved preparation and restore the old workers when safe.
A healthy disabled configuration resumes replay only; an enabled configuration
restores receipt and replay. Cancellation, shutdown and promotion can prevent
restoration. Ambiguous publication failures fence recovery for restart reconciliation."
  (let ((context (physical-follow-operation-context operation)))
    (cond
      ((getf context :committed) nil)
      ((or (getf context :pending-write)
           (and (getf context :intent-write) (not (getf context :intent))))
       (setf *physical-recovery-failed-p* t))
      ((and (getf context :stopped) (not *physical-recovery-failed-p*))
       (sb-thread:with-mutex (*physical-follow-command-lock*)
         (let ((config (physical-follow-operation-config operation)))
           (sb-thread:with-mutex (*reload-run-lock*)
             (with-live-operation-quiescence
               (unless (equal config (physical-follow-read-config))
                 (error "follow configuration changed before transition recovery"))
               (when (getf context :intent)
                 (setf config (physical-follow-abandon-preparation-config config))
                 (physical-follow-controller-save-config config)
                 (setf (physical-follow-operation-config operation) config))
               (physical-follow-controller-check-current config)))
           (unless (or *physical-follow-controller-blocked-p*
                       *physical-follow-controller-stopping-p*
                       (physical-follow-operation-cancelled-p operation)
                       *physical-shutdown-state* *physical-promotion-stopping-p*
                       (shutdown-in-progress-p))
             (if (physical-follow-controller-enabled-p config)
                 (physical-follow-launch config)
                 (physical-follow-launch-replay-only config)))))))))

(declaim (special *physical-follow-prepare-timeout*))

(defun physical-follow-operation-cancelled-condition-p (operation condition)
  "Treat a stopped client as cancellation only when OPERATION was cancelled."
  (or (typep condition 'physical-follow-controller-cancelled)
      (and (typep condition 'physical-client-stopped)
           (sb-thread:with-mutex (*physical-follow-lock*)
             (physical-follow-operation-cancelled-p operation)))))

(defun physical-follow-wait-for-policy (runtime reason &optional retry-source)
  "Stop receipt under command exclusion; preserve replay and its pause state.
Only an eligible source retry schedules another automatic discovery."
  (unless (sb-thread:holding-mutex-p *physical-follow-command-lock*)
    (error "policy wait requires follow command exclusion"))
  (when (and runtime (eq runtime *physical-follow-runtime*))
    (physical-follow-quiesce-receiver)
    (sb-thread:with-mutex (*physical-follow-lock*)
      (when (eq runtime *physical-follow-runtime*)
        (setf (physical-follow-runtime-transition runtime)
              (if retry-source (list :waiting reason :retry-source t) (list :waiting reason)))
        (when retry-source
          (setf *physical-follow-auto-error* nil
                *physical-follow-auto-retry-after*
                (+ (get-internal-real-time) internal-time-units-per-second)))))
    :replay-only))

(defun physical-follow-operation-not-attached (operation decision)
  "Return a non-attach result without granting source or binding authority.
Recheck cancellation before changing runtime state and before publishing the
result; restore stopped workers when needed."
  (when (getf (physical-follow-operation-context operation) :stopped)
    (physical-follow-operation-recover operation))
  (physical-follow-operation-check operation)
  (let ((reason (getf decision :reason)))
    (when (or (eq :replay-only (getf decision :action))
              (getf (physical-follow-operation-context operation) :automatic-p))
      (sb-thread:with-mutex (*physical-follow-command-lock*)
        (physical-follow-operation-check operation)
        (unless *physical-follow-runtime*
          (let ((config (physical-follow-read-config)))
            (if (physical-follow-controller-enabled-p config)
                (physical-follow-launch config)
                (physical-follow-launch-replay-only config))))
        (physical-follow-wait-for-policy
         *physical-follow-runtime* reason
         (and (getf (physical-follow-operation-context operation) :automatic-p)
              (getf decision :retry-source)))))
    (physical-follow-operation-check operation)
    (setf (physical-follow-operation-result operation) (list :not-attached :reason reason))))

(defun physical-follow-operation-run (operation)
  (let ((client nil))
    (unwind-protect
         (handler-case
             (progn
               (physical-follow-operation-initial-config operation)
               (let ((local (getf (physical-follow-operation-context operation) :local-decision)))
                 (when (eq :replay-only (getf local :action))
                   (return-from physical-follow-operation-run
                     (physical-follow-operation-not-attached operation local))))
               (setf client (physical-follow-operation-connect operation))
               (let ((view (physical-follow-controller-source-view
                            client (getf (physical-follow-operation-context operation) :timeline-policy)
                            (lambda (timeline system-id)
                              (physical-follow-operation-remember-source operation timeline system-id)))))
                 (unless (eq :attach (getf view :action))
                   (return-from physical-follow-operation-run
                     (physical-follow-operation-not-attached operation view)))
                 (multiple-value-bind (entry snapshot) (physical-follow-operation-intent operation view)
                   (declare (ignore snapshot))
                   (let ((anchor (sb-ext:with-timeout *physical-follow-prepare-timeout*
                                   (pg-client-native-prepare client (getf entry :pin) (getf entry :request)))))
                     (physical-follow-test-point :after-controller-proof)
                     (physical-follow-operation-publish operation view entry anchor))))
               (sb-thread:with-mutex (*physical-follow-command-lock*)
                 (physical-follow-operation-check operation)
                 (let ((current (physical-follow-read-config)))
                   (unless (equal (physical-follow-effective-config current)
                                  (physical-follow-effective-config (physical-follow-operation-result operation)))
                     (error "active follow configuration changed before worker launch"))
                   (physical-follow-launch current)))
               (setf *physical-follow-auto-error* nil
                     (physical-follow-operation-result operation) :started))
           (physical-follow-route-unavailable (condition)
             (handler-case
                 (physical-follow-operation-not-attached
                  operation (list :action :unavailable :reason (physical-follow-route-reason condition)))
               (error (recovery-error)
                 (setf (physical-follow-operation-error operation) recovery-error)
                 (unless (or (physical-follow-operation-cancelled-condition-p operation recovery-error)
                             (typep recovery-error 'physical-recovery-worker-shutdown))
                   (setf *physical-recovery-failed-p* t)))))
           ((or error sb-ext:timeout) (condition)
             (setf (physical-follow-operation-error operation) condition)
             (when (and (getf (physical-follow-operation-context operation) :committed)
                        (not (or (physical-follow-operation-cancelled-condition-p operation condition)
                                 (typep condition 'physical-recovery-worker-shutdown))))
               (setf *physical-recovery-failed-p* t))
             (handler-case (physical-follow-operation-recover operation)
               (error (recovery-error)
                 (setf *physical-recovery-failed-p* t)
                 (admin-log "native follow transition recovery failed: ~a" recovery-error)))
             (physical-follow-controller-record-error :transition condition)
             (unless (or (physical-client-retryable-condition-p condition)
                         (typep condition 'physical-follow-observation-save-failed)
                         (physical-follow-operation-cancelled-condition-p operation condition))
               (setf *physical-follow-auto-error*
                     (list (and (physical-follow-operation-config operation)
                                (physical-follow-effective-config (physical-follow-operation-config operation)))
                           (princ-to-string condition))))
             (setf *physical-follow-auto-retry-after*
                   (+ (get-internal-real-time) internal-time-units-per-second))))
      (physical-client-close client)
      (sb-thread:with-mutex (*physical-follow-lock*)
        (setf (physical-follow-operation-client operation) nil)
        (when (eq operation *physical-follow-operation*)
          (setf *physical-follow-operation* nil))))))

(defun physical-follow-start-operation (host port prefix &optional runtime profile (slot :inherit))
  "Start a transition with a snapshot of the requested public slot.
:INHERIT copies the runtime's accepted slot for automatic handoff; explicit NIL
requests no slot. Capture the choice before the worker starts."
  (physical-follow-endpoint-check host port prefix)
  (unless (<= (length prefix) 30) (error "follow pin prefix exceeds 30 characters"))
  (let* ((source (when runtime (physical-follow-config-source (physical-follow-runtime-config runtime))))
         (requested (or profile
                        (when source
                          (physical-follow-source-profile source))))
         (public-slot (physical-follow-public-slot-copy
                       (if (eq slot :inherit)
                           (when runtime
                             (physical-follow-config-public-slot (physical-follow-runtime-config runtime)))
                           slot)))
         (ready nil)
         (operation (make-physical-follow-operation
                     :host (copy-seq host) :port port :prefix (copy-seq prefix) :runtime runtime
                     :context (append (list :requested-slot public-slot :automatic-p (not (null runtime)))
                                      (when requested (list :requested-profile requested))))))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (when (or *physical-follow-operation* *physical-follow-controller-blocked-p*
                *physical-follow-controller-stopping-p*)
        (error "native follow controller is busy or stopping"))
      (setf (physical-follow-operation-thread operation)
            (sb-thread:make-thread
             (lambda ()
               (loop until ready do (sleep 0.001))
               (physical-follow-operation-run operation))
             :name "native follow transition")
            *physical-follow-operation* operation
            ready t))
    operation))

(defun physical-follow-controller-cancel-join ()
  "Cancel the operation socket and join its thread outside command, reload and
live-operation admission locks. A failed join fences recovery."
  (let ((operation nil) (client nil))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (setf operation *physical-follow-operation*)
      (when operation
        (setf (physical-follow-operation-cancelled-p operation) t
              client (physical-follow-operation-client operation))))
    (when client
      (ignore-errors (physical-client-stop client)))
    (when operation
      (let ((thread (physical-follow-operation-thread operation)))
        (when (eq thread sb-thread:*current-thread*) (error "follow controller cannot join itself"))
        (sb-thread:join-thread thread :timeout 15 :default :timeout)
        (when (sb-thread:thread-alive-p thread)
          (setf *physical-recovery-failed-p* t)
          (error "native follow transition has not stopped")))))
  :stopped)

(defun physical-follow-call-with-controller-blocked (function)
  "Block new transitions, cancel and join the current operation, then run FUNCTION.
The caller must not hold command, reload or live-operation admission locks."
  (let ((nested nil))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (cond ((eq *physical-follow-controller-block-owner* sb-thread:*current-thread*)
             (setf nested t))
            (*physical-follow-controller-blocked-p* (error "native follow lifecycle operation is already active"))
            (t (setf *physical-follow-controller-blocked-p* t
                     *physical-follow-controller-block-owner* sb-thread:*current-thread*))))
    (if nested (funcall function)
        (unwind-protect
             (progn (physical-follow-controller-cancel-join) (funcall function))
          (sb-thread:with-mutex (*physical-follow-lock*)
            (unless *physical-follow-controller-stopping-p*
              (setf *physical-follow-controller-blocked-p* nil
                    *physical-follow-controller-block-owner* nil)))))))

(defun physical-follow-prepare-managed-startup ()
  "Prepare a signal-managed standby under cold ownership before listeners start.
Enable streaming recovery and durably disable receipt while preserving the saved
endpoint, binding, owner and cleanup debt. Startup can then launch replay only;
an explicit retarget can attach a receiver later."
  (when (getf *physical-recovery-state* :standard-follow-managed-p)
    (unless (and *postmaster-data-lock* (postmaster-pid-owned-p) *wal-ready*
                 (zerop *n-connections*) (zerop *n-backends*)
                 (zerop *live-operation-depth*) (not *physical-follow-runtime*))
      (error "managed follow preparation requires owned startup before listeners"))
    (sb-thread:with-mutex (*physical-follow-command-lock*)
      (check-physical-recovery-worker-start)
      (unless *physical-receive-store*
        (when (eq :deferred (physical-recovery-enable-streaming))
          (error "managed standby streaming setup was deferred during startup")))
      (sb-thread:with-mutex (*reload-run-lock*)
        (with-live-operation-quiescence
          (check-physical-recovery-worker-start)
          (unless (and *physical-receive-store*
                       (eql 2 (getf *physical-recovery-state* :phase)))
            (error "managed follow preparation requires streaming recovery"))
          (physical-follow-check-startup-admission *physical-recovery-state*)
          (let* ((base (physical-recovery-verify-base-state))
                 (config (if (probe-file (physical-follow-config-path))
                             (physical-follow-read-config)
                             (physical-follow-unattached-config-for-base base)))
                 (state (physical-follow-controller-state config)))
            (when (or (physical-follow-cleanup-only-p config)
                      (getf state :preparing)
                      (and (fifth config) (not (eq :active (getf (fifth config) :phase)))))
              (error "managed follow startup requires reconciled standby metadata"))
            (physical-follow-check-unowned-bootstrap-base config base)
            (physical-follow-controller-check-current config)
            (setf (getf state :enabled) nil
                  config (physical-follow-with-controller config state))
            (physical-follow-controller-save-config config)
            (setf (getf *physical-recovery-state* :follow-startup-config) config)))))
    :prepared))

(defun physical-follow-ensure-attachment-state ()
  "Establish checked replay-only state before a first source attachment.
Preserve the unowned bootstrap reference; return :DEFERRED when streaming
recovery cannot yet be enabled. A failed first connection leaves replay running."
  (when (plusp *live-operation-depth*)
    (error "follow initialization requires its own operation admission"))
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (let ((config (when (probe-file (physical-follow-config-path))
                    (physical-follow-read-config))))
      (when (and config (not (physical-follow-unattached-p config)))
        (return-from physical-follow-ensure-attachment-state config))
      (check-physical-recovery-worker-start)
      (unless (recovery-in-progress-p)
        (error "follow initialization requires a verified recovery base"))
      (sb-thread:with-mutex (*physical-follow-lock*)
        (when (or *physical-follow-operation* *physical-follow-controller-blocked-p*
                  *physical-follow-controller-stopping-p*)
          (error "native follow controller is busy or stopping")))
      (when (and config (getf (sixth config) :preparing))
        (error "follow initialization requires reconciled preparation"))
      (unless *physical-receive-store*
        (when (eq :deferred (physical-recovery-enable-streaming))
          (return-from physical-follow-ensure-attachment-state :deferred)))
      (sb-thread:with-mutex (*reload-run-lock*)
        (with-live-operation-quiescence
          (check-physical-recovery-worker-start)
          (unless (and (recovery-in-progress-p) *physical-receive-store*
                       (eql 2 (getf *physical-recovery-state* :phase)))
            (error "follow initialization requires streaming recovery"))
          (unless (equal config (when (probe-file (physical-follow-config-path))
                                  (physical-follow-read-config)))
            (error "follow configuration changed before initialization"))
          (let ((base (physical-recovery-verify-base-state)))
            (setf config (or config (physical-follow-unattached-config-for-base base)))
            (physical-follow-check-unowned-bootstrap-base config base)
            (physical-follow-controller-check-current config)
            (physical-follow-controller-save-config config))))
      (unless *physical-follow-runtime*
        (physical-follow-launch-replay-only config))
      config)))

(defun physical-follow-retarget (host port &optional (prefix "follow"))
  "Attach or retarget to the direct endpoint on a fresh pin, synchronously."
  (when (plusp *live-operation-depth*) (error "follow retarget requires its own operation admission"))
  (physical-follow-endpoint-check host port prefix)
  (unless (<= (length prefix) 30) (error "follow pin prefix exceeds 30 characters"))
  (when (eq :deferred (physical-follow-ensure-attachment-state))
    (return-from physical-follow-retarget :deferred))
  (physical-follow-controller-start-worker :force t)
  (let ((operation (physical-follow-start-operation host port prefix)))
    (sb-thread:join-thread (physical-follow-operation-thread operation))
    (when (physical-follow-operation-error operation)
      (error "native follow transition failed: ~a" (physical-follow-operation-error operation)))
    (physical-follow-operation-result operation)))

(defun physical-follow-retarget-conninfo (conninfo &key (prefix "follow") (fallback (physical-libpq-default-fallback)) slot (mode :database))
  "Attach or retarget native follow using a libpq profile.
MODE defaults to :DATABASE; :PHYSICAL selects physical replication.
Unless supplied, FALLBACK uses cluster_name or walreceiver when it is empty.
SLOT is NIL or a normalized public slot name of 1 to 63 lowercase ASCII letters,
digits or underscores. Omission and explicit NIL both request no public slot.
The source slot must already exist. Preparation and failures before publication
preserve the old accepted slot. After publication, acceptance follows the durable
binding and restart reconciliation; the old slot may not be restored."
  (when (plusp *live-operation-depth*)
    (error "native follow retarget requires its own operation admission"))
  (let ((profile (make-physical-libpq-profile conninfo :fallback fallback :mode mode))
        (slot (physical-follow-public-slot-copy slot)))
    (physical-follow-endpoint-check "libpq" 5432 prefix)
    (unless (<= (length prefix) 30) (error "follow pin prefix exceeds 30 characters"))
    (when (eq :deferred (physical-follow-ensure-attachment-state))
      (return-from physical-follow-retarget-conninfo :deferred))
    (physical-follow-controller-start-worker :force t)
    (let ((operation (physical-follow-start-operation "libpq" 5432 prefix nil profile slot)))
      (sb-thread:join-thread (physical-follow-operation-thread operation))
      (when (physical-follow-operation-error operation)
        (error "native follow transition failed: ~a" (physical-follow-operation-error operation)))
      (physical-follow-operation-result operation))))

(defvar *physical-follow-cleanup-retry-after* (make-hash-table :test #'equal))

(defvar *physical-follow-cleanup-last* nil)

(defvar *physical-follow-prepare-timeout* 60)

(defun physical-follow-controller-record-error (key condition)
  (let ((message (princ-to-string condition)))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (setf (gethash key *physical-follow-controller-errors*) message)))
  nil)

(defun physical-follow-cleanup-register-client (client)
  (sb-thread:with-mutex (*physical-follow-lock*)
    (when *physical-follow-controller-stopping-p*
      (error 'physical-follow-controller-cancelled :format-control "follow cleanup is stopping"))
    (setf *physical-follow-cleanup-client* client))
  client)

(defun physical-follow-cleanup-connect (entry)
  "Reconnect using the retired pin's frozen owner, including its saved credential."
  (let ((client nil) (accepted nil))
    (unwind-protect
         (progn
           (if (getf entry :connection)
               (setf client
                     (physical-libpq-connect-frozen
                      (physical-libpq-helper-path)
                      (physical-libpq-frozen-profile-from-datum (getf entry :connection))
                      :on-spawn (lambda (channel)
                                  (setf client channel)
                                  (physical-follow-cleanup-register-client channel))))
               (progn
                 (setf client
                       (sb-ext:with-timeout 10
                         (pg-client-connect (getf entry :host) (getf entry :port) "postgres"
                                            :application-name "pg_lisp_follow_cleanup"
                                            :replication-p t)))
                 (physical-follow-cleanup-register-client client)))
           (setf accepted t)
           client)
      (unless accepted
        (physical-client-close client)
        (sb-thread:with-mutex (*physical-follow-lock*)
          (when (eq client *physical-follow-cleanup-client*)
            (setf *physical-follow-cleanup-client* nil)))))))

(defun physical-follow-controller-cleanup (entry)
  "Retry exact remote retirement outside command exclusion. Remove the durable
queue entry only after acknowledgment and a fresh local state check. A revoked
old credential leaves the entry queued for a later retry."
  (let ((client nil))
    (unwind-protect
         (handler-case
             (progn
               (setf client (physical-follow-cleanup-connect entry))
               (sb-ext:with-timeout 10
                 (ecase (getf entry :kind)
                   (:follow (pg-client-native-abort client (getf entry :pin) (getf entry :request)))
                   (:bootstrap (pg-client-native-release-bootstrap
                                client (getf entry :pin) (getf entry :request)))))
               (sb-thread:with-mutex (*physical-follow-command-lock*)
                 (with-live-operation
                   (when (probe-file (physical-follow-config-path))
                     (let* ((config (physical-follow-read-config))
                            (state (physical-follow-remember-config-timeline config)))
                       (when (and (not (getf state :preparing))
                                  (or (not (fifth config)) (eq :active (getf (fifth config) :phase)))
                                  (not (equal (getf entry :pin) (fourth config)))
                                  (member entry (getf state :cleanup) :test #'equal))
                         (setf (getf state :cleanup) (remove entry (getf state :cleanup) :test #'equal))
                         (physical-follow-controller-save-config
                          (physical-follow-with-controller config state)))))))
               (sb-thread:with-mutex (*physical-follow-lock*)
                 (remhash entry *physical-follow-controller-errors*))
               (remhash entry *physical-follow-cleanup-retry-after*))
           ((or error sb-ext:timeout) (condition)
             (physical-follow-controller-record-error entry condition)
             (setf (gethash entry *physical-follow-cleanup-retry-after*)
                   (+ (get-internal-real-time) (* 5 internal-time-units-per-second)))))
      (physical-client-close client)
      (sb-thread:with-mutex (*physical-follow-lock*)
        (when (eq client *physical-follow-cleanup-client*)
          (setf *physical-follow-cleanup-client* nil))))))

(defun physical-follow-controller-next-cleanup (config)
  (let* ((state (physical-follow-controller-state config))
         (queue (getf state :cleanup))
         (position (position *physical-follow-cleanup-last* queue :test #'equal))
         (ordered (if position
                      (append (nthcdr (1+ position) queue) (subseq queue 0 (1+ position)))
                      queue)))
    (unless (or (getf state :preparing)
                (and (fifth config) (not (eq :active (getf (fifth config) :phase)))))
      (find-if (lambda (entry)
                 (and (not (equal (getf entry :pin) (fourth config)))
                      (<= (gethash entry *physical-follow-cleanup-retry-after* 0)
                          (get-internal-real-time))))
               ordered))))

(defun physical-follow-controller-loop ()
  (loop until *physical-follow-controller-stopping-p*
        do (handler-case (when (eq :idle (physical-follow-controller-step))
                           (return-from physical-follow-controller-loop :idle))
             ((or error sb-ext:timeout) (condition)
               (physical-follow-controller-record-error :controller condition)))
           (sleep 0.2)))

(defun physical-follow-controller-start-worker (&key force)
  (check-physical-recovery-worker-start)
  (when (or force
            (and (probe-file (physical-follow-config-path))
                 (>= (length (physical-follow-read-config)) 5)))
    (when (shutdown-in-progress-p)
      (error 'physical-recovery-worker-shutdown :format-control "follow controller cannot start during shutdown"))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (unless (and *physical-follow-controller-worker*
                   (sb-thread:thread-alive-p *physical-follow-controller-worker*))
        (setf *physical-follow-controller-stopping-p* nil
              *physical-follow-controller-worker*
              (sb-thread:make-thread #'physical-follow-controller-loop :name "native follow controller")))))
  *physical-follow-controller-worker*)

(defun physical-follow-controller-stop-worker ()
  "Stop cleanup and transitions, cancel sockets and join their workers.
Call outside command, reload and live-operation admission locks."
  (let ((worker nil) (client nil))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (setf *physical-follow-controller-stopping-p* t
            *physical-follow-controller-blocked-p* t
            *physical-follow-controller-block-owner* sb-thread:*current-thread*
            worker *physical-follow-controller-worker*
            client *physical-follow-cleanup-client*))
    (when client
      (ignore-errors (physical-client-stop client)))
    (physical-follow-controller-cancel-join)
    (when worker
      (sb-thread:join-thread worker :timeout 15 :default :timeout)
      (when (sb-thread:thread-alive-p worker) (error "native follow controller has not stopped"))
      (sb-thread:with-mutex (*physical-follow-lock*)
        (when (eq worker *physical-follow-controller-worker*)
          (setf *physical-follow-controller-worker* nil)))))
  :stopped)

(defun physical-follow-controller-status ()
  (let* ((config (when (probe-file (physical-follow-config-path)) (physical-follow-read-config)))
         (state (and config (physical-follow-controller-state config))))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (list :phase (cond ((null config) :none)
                         ((physical-follow-cleanup-only-p config) :cleanup)
                         ((getf state :preparing) :preparing)
                         ((fifth config) (getf (fifth config) :phase))
                         (t :bootstrap))
            :enabled (and config (physical-follow-controller-enabled-p config))
            :transition-running (not (null *physical-follow-operation*))
            :last-error (gethash :transition *physical-follow-controller-errors*)
            :cleanup-count (length (getf state :cleanup))
            :cleanup-capacity +physical-follow-cleanup-capacity+
            :cleanup (loop for entry in (getf state :cleanup)
                           collect (list :kind (getf entry :kind) :host (getf entry :host)
                                         :port (getf entry :port) :pin (getf entry :pin)
                                         :last-error (gethash entry *physical-follow-controller-errors*)))))))

(defun physical-follow-controller-fence-receipt (runtime config decision)
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (unless (and (eq runtime *physical-follow-runtime*)
                 (equal (physical-follow-effective-config config)
                        (physical-follow-effective-config (physical-follow-read-config))))
      (return-from physical-follow-controller-fence-receipt nil))
    (physical-follow-wait-for-policy runtime (getf decision :reason))))

(defun physical-follow-controller-step ()
  "Start an eligible automatic transition or retry one queued retirement.
An empty cleanup-only configuration stops the worker but remains on disk."
  (let ((config (with-live-operation
                  (check-recovery-available)
                  (when (probe-file (physical-follow-config-path)) (physical-follow-read-config))))
        (runtime nil) (busy nil) (blocked nil) (auto-error nil) (decision nil))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (setf runtime *physical-follow-runtime* busy *physical-follow-operation*
            blocked (or *physical-follow-controller-blocked-p* *physical-follow-controller-stopping-p*)
            auto-error *physical-follow-auto-error*))
    (cond
      ((or (null config) blocked busy) nil)
      ((and (physical-follow-cleanup-only-p config)
            (null (getf (physical-follow-controller-state config) :cleanup))) :idle)
      ((and runtime (physical-follow-runtime-transition runtime)
            (not (physical-follow-receiver-stopped-p runtime))
            (eq :replay-only (getf (setf decision (physical-follow-launch-decision config)) :action)))
       (physical-follow-controller-fence-receipt
        runtime config decision))
      ((and runtime (physical-follow-runtime-transition runtime)
            (or (not (physical-follow-receiver-stopped-p runtime))
                (eq :policy-refresh (first (physical-follow-runtime-transition runtime)))
                (let ((transition (physical-follow-runtime-transition runtime)))
                  (and (eq :waiting (first transition))
                       (getf (cddr transition) :retry-source))))
            (physical-follow-controller-enabled-p config)
            (not (equal (first auto-error) (physical-follow-effective-config config)))
            (<= *physical-follow-auto-retry-after* (get-internal-real-time)))
       (physical-follow-start-operation (second config) (third config) "follow" runtime))
      (t
       (let ((entry (physical-follow-controller-next-cleanup config)))
         (when entry
           (setf *physical-follow-cleanup-last* entry)
           (physical-follow-controller-cleanup entry)))))))
