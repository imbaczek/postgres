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

(defun physical-follow-controller-check-current (config)
  "Validate the current recovery and receive state before restoring its receiver.
The caller excludes concurrent mutation and protects the selected store."
  (let ((base (physical-recovery-verify-base-state)))
    (if (fifth config)
        (physical-follow-check-active-state *physical-receive-store* base config)
        (physical-follow-call-with-receive-root
         *physical-receive-store*
         (lambda ()
           (let* ((candidate (physical-follow-copy-checked-store *physical-receive-store*))
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
              candidate base (list *last-replay-lsn* (physical-recovery-replay-timeline))
              required control (physical-follow-controller-index candidate) nil)))))))

(defun physical-follow-controller-snapshot-under-lease (view)
  "Capture the receive, replay and durable recovery witnesses under root protection.
The receive timeline can differ from the applied replay timeline."
  (let* ((base (physical-recovery-verify-base-state))
         (control (physical-recovery-read-control *pgdata*))
         (store (physical-follow-copy-checked-store *physical-receive-store*))
         (before (physical-store-control-tuple store))
         (applied (list *last-replay-lsn* (physical-recovery-replay-timeline)))
         (required (list (getf control :applied)
                         (getf control :applied-timeline (getf control :timeline))))
         (fork (getf view :fork)) (cursor (min (fourth before) fork))
         (index (physical-follow-controller-index store))
         (coordinates nil))
    (unless (and (eql 2 (getf control :phase)) (eql 3 (getf control :control-version)))
      (error "follow transition requires current streaming recovery control"))
    (unless (= (physical-store-selected-timeline store) (getf view :parent))
      (error "receive selection changed during source discovery"))
    (sb-thread:with-mutex ((physical-store-mutex store))
      (physical-follow-check-source-history store (getf view :history))
      (physical-store-check-follow-frontier store applied (getf view :history) fork :applied t)
      (physical-store-check-follow-frontier store required (getf view :history) fork)
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
    (multiple-value-bind (metadata catalog prefix digest) (physical-restartpoint-read base)
      (declare (ignore catalog prefix))
      (list :base base :before before :applied applied :required required :coordinates coordinates
            :recovery (list :phase 2 :cluster-identity (getf base :cluster-identity)
                            :checkpoint (getf base :checkpoint) :observed-next-xid (getf control :next-xid)
                            :history-target (getf control :history-target)
                            :history-digest (getf control :history-digest)
                            :restartpoint (when metadata (list (getf metadata :lsn) digest)))))))

(defun physical-follow-controller-plan (snapshot anchor view)
  (let* ((before (getf snapshot :before)) (fork (getf view :fork))
                                          (after
                                            (if (< (fourth before) fork) (copy-list before)
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

(defun physical-follow-controller-source-view (client)
  "Read source identity and validated remote history; select receipt's next child.
The later snapshot checks exact local ancestry. System identifier and timeline
cannot distinguish endpoint remaps or clones; the direct endpoint must continue
to identify the same PGDATA."
  (multiple-value-bind (system-id source) (physical-follow-client-identity client)
    (let ((parent (with-live-operation
                    (unless (= system-id (getf *physical-recovery-state* :system-id))
                      (physical-follow-reject "follow target has a different system identifier"))
                    (physical-store-selected-timeline *physical-receive-store*))))
      (unless (> source parent) (error "follow target has no descendant timeline to select"))
      (let* ((text (sb-ext:with-timeout 10 (pg-client-native-history client source)))
             (history (timeline-check-lineage (timeline-parse-history source (bytes->string text))))
             (position (position parent history :key #'timeline-history-entry-timeline)))
        (unless (and position (plusp position)) (error "follow target does not contain receive ancestry"))
        (let* ((child (timeline-history-entry-timeline (nth (1- position) history)))
               (child-text (if (= child source) text
                               (sb-ext:with-timeout 10 (pg-client-native-history client child)))))
          (unless (physical-store-history-extends-p
                   (timeline-check-lineage (timeline-parse-history child (bytes->string child-text))) history)
            (error "follow child history differs from source ancestry"))
          (list :source source :parent parent :history history :history-text (bytes->string text)
                :child child :child-history (bytes->string child-text)
                :fork (timeline-history-entry-end (nth position history))))))))

(defun physical-follow-current-retirement (config base)
  "Copy the current source identity for later asynchronous retirement."
  (let* ((binding (fifth config))
         (request
           (if binding
               (loop for key in (cons :version (mapcar #'second *physical-follow-wire-fields*))
                     append (list key (getf (getf binding :anchor) key)))
               (list :version 1 :system-id (getf base :system-id)
                     :source-timeline (getf base :timeline) :base (getf base :end)
                     :manifest-digest (getf base :manifest-digest)))))
    (physical-follow-cleanup-with-owner
     (list :kind (if binding :follow :bootstrap) :host (second config) :port (third config)
           :pin (fourth config) :request request)
     (getf (physical-follow-controller-state config) :source))))

(defun physical-follow-promotion-cleanup-config ()
  "Build the post-promotion cleanup image before retiring base provenance.
The caller must save it durably before removing recovery artifacts. Remote
retirement may finish later."
  (when (probe-file (physical-follow-config-path))
    (let* ((config (physical-follow-read-config))
           (state (physical-follow-controller-state config)))
      (when (physical-follow-cleanup-only-p config)
        (return-from physical-follow-promotion-cleanup-config config))
      (when (or (getf state :preparing) (eq :pending (getf (fifth config) :phase)))
        (error "promotion requires a reconciled follow configuration"))
      (let ((base nil))
        (unless (fifth config)
          (multiple-value-bind (manifest digest) (recovery-read-manifest *pgdata*)
            (recovery-check-manifest-schema manifest)
            (let ((control (physical-recovery-read-control *pgdata*)))
              (unless (and (= (getf manifest :system-id) (getf control :system-id))
                           (= (getf manifest :timeline) (getf control :timeline))
                           (= digest (getf control :manifest-digest))
                           (= (getf manifest :checkpoint) (getf control :checkpoint)))
                (error "promotion cleanup base identity does not match recovery control")))
            (setf base (copy-list manifest) (getf base :manifest-digest) digest)))
        (setf (getf state :enabled) nil
              state (physical-follow-enqueue-cleanup
                     state (physical-follow-current-retirement config base)))
        (remf state :source)
        (physical-follow-check-config (list 1 nil nil nil nil state))))))

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

(defun physical-follow-prepare-startup (base)
  "Under cold PGDATA ownership, queue unfinished preparation and reconcile pending
state before validating an active binding. Protect the store and retained root."
  (when (probe-file (physical-follow-config-path))
    (let ((config (physical-follow-read-config)))
      (when (= 4 (length config)) (return-from physical-follow-prepare-startup config))
      (sb-thread:with-mutex (*physical-follow-command-lock*)
        (sb-thread:with-mutex (*reload-run-lock*)
          (with-live-operation-quiescence
            (physical-follow-call-with-offline-store
             base
             (lambda (store)
               (when (getf (physical-follow-controller-state config) :preparing)
                 (when (fifth config)
                   (physical-follow-check-active-state store base config))
                 (let ((next (physical-follow-abandon-preparation-config config)))
                   (physical-follow-save-config next)
                   (unless (equal next (physical-follow-read-config))
                     (error "startup preparation-abort readback differs"))
                   (setf config next)))
               (when (and (fifth config) (eq :pending (getf (fifth config) :phase)))
                 (setf config (physical-follow-reconcile-pending-store store base config)))
               (when (fifth config)
                 (physical-follow-check-active-state store base config))
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
  (sb-thread:with-mutex (*physical-follow-lock*)
    (unless (and (eq operation *physical-follow-operation*)
                 (not (physical-follow-operation-cancelled-p operation))
                 (not *physical-follow-controller-blocked-p*)
                 (not *physical-follow-controller-stopping-p*))
      (error 'physical-follow-controller-cancelled :format-control "native follow transition was canceled")))
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
  "Check the current receiver and cleanup capacity before disturbing it or PREPARE."
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (with-live-operation
      (physical-follow-operation-check operation)
      (let* ((config (physical-follow-read-config))
             (state (physical-follow-controller-state config))
             (runtime *physical-follow-runtime*))
        (when (or (physical-follow-cleanup-only-p config) (getf state :preparing)
                  (and (fifth config) (not (eq :active (getf (fifth config) :phase)))))
          (error "native follow transition is already pending or has no current source"))
        (unless (< (length (getf state :cleanup)) +physical-follow-cleanup-capacity+)
          (error "native follow cleanup queue is full"))
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
                          (list :conninfo (physical-libpq-profile-conninfo profile)
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
  "Generate an attempt-specific pin name distinct from current and queued names."
  (unless (and (wal-valid-replication-slot-name-p prefix) (<= (length prefix) 30))
    (error "follow pin prefix must be a valid name of at most 30 characters"))
  (loop for name = (format nil "~a_~(~32,'0x~)" prefix (random (ash 1 128) (make-random-state t)))
        unless (or (equal name (fourth config))
                   (find name (getf (physical-follow-controller-state config) :cleanup)
                         :key (lambda (entry) (getf entry :pin)) :test #'equal))
          return name))

(defun physical-follow-preparation-entry (operation slot coordinates)
  (physical-follow-cleanup-with-owner
   (list :kind :follow :host (physical-follow-operation-host operation)
         :port (physical-follow-operation-port operation) :pin slot :request coordinates)
   (getf (physical-follow-operation-context operation) :accepted-source)))

(defun physical-follow-operation-source-state (operation state)
  (let ((copy (physical-follow-controller-copy state))
        (source (getf (physical-follow-operation-context operation) :accepted-source)))
    (if source
        (setf (getf copy :source) (physical-follow-source-copy source))
        (remf copy :source))
    copy))

(defun physical-follow-operation-intent (operation view)
  "Stop the old runtime and save preparation intent before contacting PREPARE.
Hold command exclusion; snapshot under reload and live-operation quiescence.
The preparation entry records the selected owner before the RPC. Once accepted,
that owner stays with the pin until a new publication, not a later reconnect."
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (physical-follow-operation-check operation)
    (let* ((current (physical-follow-read-config))
           (expected (physical-follow-operation-config operation)))
      (unless (and (equal (physical-follow-effective-config current)
                          (physical-follow-effective-config expected))
                   (eq *physical-follow-runtime* (physical-follow-operation-runtime operation)))
        (error "current follow receiver changed during source discovery"))
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
            (physical-follow-config-image
             (physical-follow-with-controller
              intent (physical-follow-operation-source-state operation state)))
            (setf (getf (physical-follow-operation-context operation) :intent-write) t)
            (physical-follow-controller-save-config intent)
            (setf (physical-follow-operation-config operation) intent
                  (getf (physical-follow-operation-context operation) :intent) t)
            (physical-follow-test-point :after-controller-intent)
            (values entry snapshot)))))))

(defun physical-follow-operation-publish (operation view entry anchor)
  "Recheck a fresh snapshot and publish pending then active binding under quiescence.
A restartpoint can advance during the RPC; use the final durable witness. Queue
the old source before publication; its remote acknowledgment is not required."
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
               (state (physical-follow-controller-state old))
               (retired (physical-follow-current-retirement old (getf snapshot :base)))
               (pending nil))
          (unless (and (equal request (getf entry :request))
                       (equal (getf anchor :history-text) (getf view :history-text)))
            (error "native follow prefix or source history changed during preparation"))
          (setf state (physical-follow-operation-source-state operation state))
          (setf (getf state :preparing) nil
                (getf state :enabled) t
                state (physical-follow-enqueue-cleanup state retired)
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
  "Queue a saved preparation and restore the old receiver when safe. Ambiguous
publication failures fence recovery for restart reconciliation."
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
             (when (physical-follow-controller-enabled-p config)
               (physical-follow-launch config)))))))))

(declaim (special *physical-follow-prepare-timeout*))

(defun physical-follow-operation-run (operation)
  (let ((client nil))
    (unwind-protect
         (handler-case
             (progn
               (physical-follow-operation-initial-config operation)
               (setf client (physical-follow-operation-connect operation))
               (let ((view (physical-follow-controller-source-view client)))
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
           ((or error sb-ext:timeout) (condition)
             (setf (physical-follow-operation-error operation) condition)
             (when (and (getf (physical-follow-operation-context operation) :committed)
                        (not (typep condition '(or physical-follow-controller-cancelled
                                                physical-recovery-worker-shutdown))))
               (setf *physical-recovery-failed-p* t))
             (handler-case (physical-follow-operation-recover operation)
               (error (recovery-error)
                 (setf *physical-recovery-failed-p* t)
                 (admin-log "native follow transition recovery failed: ~a" recovery-error)))
             (physical-follow-controller-record-error :transition condition)
             (unless (or (physical-client-retryable-condition-p condition)
                         (typep condition 'physical-follow-controller-cancelled))
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

(defun physical-follow-start-operation (host port prefix &optional runtime profile)
  (physical-follow-endpoint-check host port prefix)
  (unless (<= (length prefix) 30) (error "follow pin prefix exceeds 30 characters"))
  (let* ((source (when runtime (physical-follow-config-source (physical-follow-runtime-config runtime))))
         (requested (or profile
                        (when source
                          (make-physical-libpq-profile (getf source :conninfo)
                                                       :fallback (getf source :fallback)))))
         (ready nil)
         (operation (make-physical-follow-operation
                     :host (copy-seq host) :port port :prefix (copy-seq prefix) :runtime runtime
                     :context (when requested (list :requested-profile requested)))))
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

(defun physical-follow-retarget (host port &optional (prefix "follow"))
  "Synchronously request a transition to the direct endpoint on a fresh pin."
  (when (plusp *live-operation-depth*) (error "follow retarget requires its own operation admission"))
  (physical-follow-controller-start-worker :force t)
  (let ((operation (physical-follow-start-operation host port prefix)))
    (sb-thread:join-thread (physical-follow-operation-thread operation))
    (when (physical-follow-operation-error operation)
      (error "native follow transition failed: ~a" (physical-follow-operation-error operation)))
    (physical-follow-operation-result operation)))

(defun physical-follow-retarget-conninfo (conninfo &key (prefix "follow") (fallback "walreceiver"))
  "Retarget an existing native follow configuration using a libpq profile."
  (when (plusp *live-operation-depth*)
    (error "native follow retarget requires its own operation admission"))
  (let ((profile (make-physical-libpq-profile conninfo :fallback fallback)))
    (physical-follow-controller-start-worker :force t)
    (let ((operation (physical-follow-start-operation "libpq" 5432 prefix nil profile)))
      (sb-thread:join-thread (physical-follow-operation-thread operation))
      (when (physical-follow-operation-error operation)
        (error "native follow transition failed: ~a" (physical-follow-operation-error operation)))
      :started)))

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
                            (state (physical-follow-controller-state config)))
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

(defun physical-follow-controller-step ()
  "Start an eligible automatic transition or retry one queued retirement.
An empty cleanup-only configuration stops the worker but remains on disk."
  (let ((config (with-live-operation
                  (check-recovery-available)
                  (when (probe-file (physical-follow-config-path)) (physical-follow-read-config))))
        (runtime nil) (busy nil) (blocked nil) (auto-error nil))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (setf runtime *physical-follow-runtime* busy *physical-follow-operation*
            blocked (or *physical-follow-controller-blocked-p* *physical-follow-controller-stopping-p*)
            auto-error *physical-follow-auto-error*))
    (cond
      ((or (null config) blocked busy) nil)
      ((and (physical-follow-cleanup-only-p config)
            (null (getf (physical-follow-controller-state config) :cleanup))) :idle)
      ((and runtime (physical-follow-runtime-transition runtime)
            (not (physical-follow-runtime-stopped-p runtime))
            (physical-follow-controller-enabled-p config)
            (not (equal (first auto-error) (physical-follow-effective-config config)))
            (<= *physical-follow-auto-retry-after* (get-internal-real-time)))
       (physical-follow-start-operation (second config) (third config) "follow" runtime))
      (t
       (let ((entry (physical-follow-controller-next-cleanup config)))
         (when entry
           (setf *physical-follow-cleanup-last* entry)
           (physical-follow-controller-cleanup entry)))))))
