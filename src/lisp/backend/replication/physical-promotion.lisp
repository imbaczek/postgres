;;;; Local pending-promotion publication; EOR and writer admission remain separate.
(in-package #:pg)

(defvar *physical-promotion-preserve-readers-p* nil
  "Dynamically select live reader preservation after the ownership census.
The default is NIL, so cold completion retains its strict checks.  This is not
a configuration setting.")

(defun physical-promotion-check-context (phase &key preserve-readers)
  "Check PHASE and the caller's promotion exclusion.

The caller owns follow-command, reload and quiescence in that order; no worker
joins occur here.  PRESERVE-READERS permits retained reader transactions; their
ownership must be checked by the live promotion path."
  (unless (and (zerop *live-operation-depth*)
               (sb-thread:holding-mutex-p *physical-follow-command-lock*)
               (sb-thread:holding-mutex-p *reload-run-lock*)
               (not (sb-thread:holding-mutex-p *physical-restartpoint-start-lock*))
               (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
               (not *live-operation-drain-open*))
    (error "pending promotion requires owned command/reload exclusion and quiescence"))
  (when (and (= phase 2) (shutdown-in-progress-p))
    (error 'physical-promotion-shutdown))
  (if (= phase 2) (check-recovery-available)
      (when *physical-recovery-failed-p* (error "failed recovery cannot install child WAL")))
  (unless (and (member phase '(2 3)) (eql phase (getf *physical-recovery-state* :phase))
               *physical-receive-store* *wal-ready*
               (not *physical-follow-runtime*) (not *physical-shutdown-state*)
               (not (shutdown-in-progress-p)) (not *swank-listener-enabled-p*)
               (or preserve-readers (not (physical-recovery-reader-conflict-p))))
    (error "pending promotion requires stopped workers and no open reader transactions")))

(defun physical-promotion-check-reader-snapshots (backends parked)
  "Check the exact active snapshot census against transaction and parked owners.

Under the snapshot lock, validate identities and parked generations.  Reject
missing or conflicting owners instead of discarding their registrations."
  (let ((owners (make-hash-table :test #'eq))
        (parked-registrations (make-hash-table :test #'eq)))
    (sb-thread:with-mutex (*snapshot-count-lock*)
      (labels ((note (snapshot registration owner &optional generation)
                 (unless (and snapshot registration
                              (snapshot-registration-active-p registration)
                              (not (snapshot-registration-releasing-p registration))
                              (eq snapshot (snapshot-registration-snapshot registration))
                              (or (null generation)
                                  (eql generation (snapshot-registration-lifecycle-generation registration)))
                              (gethash snapshot *active-snapshot-set*))
                   (error "promotion reader snapshot has invalid ownership"))
                 (let ((previous (gethash snapshot owners)))
                   (when (and previous
                              (not (and (eq registration (first previous))
                                        (eq owner (second previous)))))
                     (error "promotion snapshot has conflicting owners")))
                 (setf (gethash snapshot owners) (list registration owner))))
        (dolist (backend backends)
          (let ((snapshot (backend-transaction-snapshot backend))
                (registration (backend-transaction-snapshot-registration backend)))
            (when (or snapshot registration) (note snapshot registration backend))))
        (dolist (entry parked)
          (destructuring-bind (token backend thread registrations) entry
            (unless (and (eq :parked (live-operation-token-state token))
                         (eq thread (live-operation-token-thread token))
                         (eq backend (live-operation-token-backend token))
                         (or (null backend) (member backend backends :test #'eq)))
              (error "promotion parked backend ownership mismatch"))
            (dolist (record registrations)
              (destructuring-bind (registration generation snapshot) record
                (let ((previous (gethash registration parked-registrations)))
                  (when (and previous (not (eq previous token)))
                    (error "statement registration belongs to multiple parked operations")))
                (setf (gethash registration parked-registrations) token)
                (note snapshot registration (or backend token) generation)))))
        (unless (= *active-snapshots* (hash-table-count *active-snapshot-set*)
                   (hash-table-count owners))
          (error "promotion has active snapshots without backend owners"))
        (maphash (lambda (snapshot present)
                   (unless (and present (gethash snapshot owners))
                     (error "promotion snapshot registry has no backend owner")))
                 *active-snapshot-set*))))
  (values))

(defun physical-promotion-enroll-readers ()
  "Check and enroll retained readers before a private promotion.

The caller has closed operation admission and joined recovery workers.  Copy
parked ownership under the admission lock before taking the backend registry
lock.  Closed admission keeps transaction resources stable, including while
normal EOF cleanup waits.  Take the relation lock before the XID lock, and check
snapshots separately under their lock.
Reject orphaned registrations.  Repair the virtual ID counter before allocating
IDs, and keep retained transactions read-only."
  (physical-promotion-check-context 2 :preserve-readers t)
  (let ((parked (live-operation-parked-owners-under-quiescence)))
    (sb-thread:with-mutex (*backend-registry-lock*)
      (let ((backends (remove-duplicates
                       (append (loop for backend being the hash-values of *backend-registry*
                                     collect backend)
                               *shutdown-abandoned-backends*)
                       :test #'eq))
            (owners (make-hash-table :test #'eql)))
        (sb-thread:with-mutex (*relation-lock-mutex*)
          (sb-thread:with-mutex (*xid-lock*)
            (when *active-xids*
              (error "promotion still has active real transaction IDs")))
          (dolist (backend backends)
            (let ((id (backend-virtual-reader-xid backend))
                  (snapshot (backend-transaction-snapshot backend)))
              (when (backend-xid backend)
                (error "promotion reader has a real transaction ID"))
              (when (and (or id snapshot) (eq :idle (backend-state backend)))
                (error "idle promotion backend still owns transaction resources"))
              (when id
                (unless (and (integerp id) (minusp id) (not (gethash id owners)))
                  (error "invalid or duplicate promotion reader identity"))
                (setf (gethash id owners) backend))))
          (maphash (lambda (id present)
                     (declare (ignore present))
                     (unless (gethash id owners)
                       (error "promotion reader registry has no backend owner")))
                   *recovery-reader-xids*)
          (dolist (relations (list *relation-readers* *relation-writers*))
            (maphash (lambda (node ids)
                       (declare (ignore node))
                       (dolist (id ids)
                         (unless (gethash id owners)
                           (error "promotion relation identity has no backend owner"))))
                     relations))
          (maphash (lambda (id backend)
                     (declare (ignore backend))
                     (setf *recovery-reader-xid-counter*
                           (max *recovery-reader-xid-counter* (- id))))
                   owners))
        (physical-promotion-check-reader-snapshots backends parked)
        (dolist (backend backends)
          (when (or (eq :in-progress (backend-state backend))
                    (backend-transaction-snapshot backend)
                    (backend-virtual-reader-xid backend))
            (ensure-recovery-reader-identity backend)
            (setf (backend-read-only backend) t)))))
    (values)))

(defmacro with-physical-promotion-exclusion ((phase) &body body)
  "Require owned command/reload quiescence before registration/history exclusion."
  `(progn
     (physical-promotion-check-context ,phase
                                       :preserve-readers *physical-promotion-preserve-readers-p*)
     (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
       (when (and *physical-restartpoint-worker*
                  (sb-thread:thread-alive-p *physical-restartpoint-worker*))
         (error "promotion restartpoint worker has not been joined"))
       (sb-thread:with-mutex (*timeline-history-lock*) ,@body))))

(defun physical-promotion-can-yield-to-shutdown-p ()
  "Return true if shutdown can take over healthy recovery before intent."
  (and (shutdown-in-progress-p)
       (eql 2 (getf *physical-recovery-state* :phase))
       (not *physical-recovery-failed-p*)))

(defun call-with-physical-promotion-readers (function)
  "Enroll retained readers and call FUNCTION with live preservation enabled.

The caller owns closed admission and has joined the recovery workers.  Within
the scoped transition, an incomplete exit fences recovery, except when healthy
phase-2 recovery can yield to shutdown."
  (physical-promotion-enroll-readers)
  (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
    (when (and *physical-restartpoint-worker*
               (sb-thread:thread-alive-p *physical-restartpoint-worker*))
      (error "promotion restartpoint worker has not been joined")))
  (let ((*physical-promotion-preserve-readers-p* t)
        (finished nil))
    (unwind-protect
         (handler-case
             (multiple-value-prog1
                 (progn
                   (physical-promotion-test-point :after-live-reader-enrollment)
                   (funcall function))
               (setf finished t))
           (physical-promotion-shutdown (condition)
             (when (physical-promotion-can-yield-to-shutdown-p)
               (setf finished t))
             (error condition)))
      (unless finished (setf *physical-recovery-failed-p* t)))))

(defun physical-promotion-select-child (parent)
  "Probe consecutive local history files above the namespace floor, as C does.
Select the first free ID; the floor does not change the replay parent or fork."
  (unless (typep parent '(integer 1 4294967295)) (error "invalid promotion parent timeline"))
  (loop for child from (1+ parent) to 4294967295
        unless (wal-namespace-file-stat (timeline-history-path child *pgdata*) t)
          do (return-from physical-promotion-select-child child))
  (error "no representable child timeline remains"))

(defun physical-promotion-timeline-floor (state)
  "Return the promotion namespace floor without changing replay ancestry.
Phase 2 combines the checked known IDs, receive selection, parent and policy
target. Phase 3 uses the frozen floor and rejects a child colliding with
observed IDs. Legacy pending state without a floor probes from the parent."
  (let* ((parent (physical-recovery-replay-timeline state))
         (config (when (probe-file (physical-follow-config-path)) (physical-follow-read-config)))
         (controller (when config (physical-follow-controller-state config)))
         (known (getf controller :known-timeline))
         (observed (when config
                     (getf (physical-follow-remember-config-timeline config) :known-timeline)))
         (binding (fifth config))
         (selected (if *physical-receive-store*
                       (physical-store-selected-timeline *physical-receive-store*) parent)))
    (when (= 3 (getf state :phase))
      (unless (> (getf state :promotion-child) (max parent selected (or observed 1)))
        (error "pending child reuses an observed timeline namespace"))
      (when known
        (unless (and (not (getf controller :preparing))
                     (or (null binding) (eq :active (getf binding :phase)))
                     (>= known (max parent selected (or observed 1)))
                     (< known (getf state :promotion-child)))
          (error "pending promotion has inconsistent known timeline metadata")))
      (return-from physical-promotion-timeline-floor (or known parent)))
    (max parent (or observed 1) selected
         (or (physical-follow-observed-timeline (getf state :system-id)) 1)
         (or (getf (getf state :timeline-policy) :target) 1))))

(defun physical-promotion-freeze-timeline-floor (floor)
  "Save and read back the phase 2 namespace floor before publishing intent.
Keep this bound fixed through phase 3; do not add the new child until pending
promotion has been retired."
  (let* ((config (if (probe-file (physical-follow-config-path))
                     (physical-follow-read-config)
                     (physical-follow-unattached-config-for-base (physical-follow-promotion-base))))
         (state (physical-follow-remember-timeline (physical-follow-remember-config-timeline config) floor))
         (frozen (physical-follow-with-controller config state)))
    (unless (equal frozen config) (physical-follow-controller-save-config frozen))
    frozen))

(defun physical-promotion-check-base-control (state)
  "Recheck immutable base control and parent history files before promotion intent."
  (let ((entry (find "global/pg_wal_control" (getf state :files) :key #'first :test #'equal)))
    (unless entry (error "promotion lost immutable base WAL control provenance"))
    (recovery-check-manifest-files
     (list :files (remove-if-not
                   (lambda (file)
                     (or (equal (first file) "global/pg_wal_control")
                         (recovery-history-file-timeline (first file))))
                   (getf state :files)))
     *pgdata*))
  (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline namespace)
      (wal-read-control-fields)
    (declare (ignore snapshot))
    (unless (and (= checkpoint (getf state :checkpoint)) (= durable (getf state :end))
                 (= insertion checkpoint-timeline *wal-insert-timeline* *wal-checkpoint-timeline*
                    (getf state :timeline))
                 (eq namespace (getf state :wal-namespace))
                 (eq namespace *wal-segment-namespace*))
      (error "promotion base WAL identity or namespace changed"))
    namespace))

(defun physical-promotion-pending-timeline-policy (state metadata store)
  "Resolve policy for a pending promotion after the caller validates its owner.
The caller must already have checked durable pending control, generation and
parent.  The phase check here alone does not establish that ownership."
  (unless (eql 3 (getf state :phase))
    (error "pending timeline policy requires durable promotion intent"))
  (physical-recovery-resolve-timeline-policy
   state store metadata (physical-recovery-replay-timeline state)))

(defun physical-promotion-check-progress (state metadata prefix generation &optional (phase 2))
  (let* ((fork *last-replay-lsn*) (next *next-xid*)
                                  (control (physical-recovery-read-control *pgdata*))
                                  (snapshot (getf metadata :source-checkpoint))
                                  (reservation (getf metadata :source-reservation)))
    (unless (and metadata prefix (eql 4 (getf metadata :version))
                 (typep generation '(unsigned-byte 64))
                 (= fork (getf metadata :lsn) (getf control :applied)
                    (getf state :required-applied))
                 (= next (getf metadata :next-xid) (getf control :next-xid)
                    (getf state :observed-next-xid))
                 (eql phase (getf control :phase))
                 (every (lambda (key) (eql (getf state key) (getf control key)))
                        '(:system-id :timeline :manifest-digest :checkpoint))
                 (= (physical-recovery-replay-timeline state)
                    (getf control :applied-timeline (getf control :timeline))
                    (getf metadata :replay-timeline (getf metadata :timeline)))
                 (equal (getf control :history-target) (getf state :history-target))
                 (equal (getf control :history-digest) (getf state :history-digest))
                 snapshot (wal-checkpoint-snapshot-valid-p snapshot)
                 (= (getf snapshot :timeline) (physical-recovery-replay-timeline state))
                 reservation (physical-source-reservation-valid-p reservation)
                 (getf reservation :complete) (= fork (getf reservation :through))
                 (<= next (1+ (getf reservation :bound))))
      (error "promotion lacks exact durable restartpoint, checkpoint or allocation proof"))
    (unless (and (= (physical-store-system-id *physical-receive-store*) (getf state :system-id))
                 (= (physical-store-timeline *physical-receive-store*) (getf state :timeline))
                 (= (physical-store-base-lsn *physical-receive-store*) (getf state :end))
                 (= (physical-store-written-lsn *physical-receive-store*)
                    (physical-store-flushed-lsn *physical-receive-store*)))
      (error "promotion requires matching receive identity and flushed complete writes"))
    (unless (= (getf metadata :prefix-digest)
               (physical-store-prefix-digest *physical-receive-store* fork))
      (error "promotion restartpoint differs from durable receive history"))
    ;; Include all acknowledged receive records. A complete later terminal must
    ;; be applied first; an unfinished native batch does not move the data fork.
    (multiple-value-bind (removed terminal)
        (physical-recovery-proven-drops
         *physical-receive-store* (getf state :cluster-identity) :required-lsn fork
         :policy (if (= phase 3)
                     (physical-promotion-pending-timeline-policy state metadata *physical-receive-store*)
                     (getf state :timeline-policy)))
      (declare (ignore removed))
      (unless (= fork terminal)
        (error "promotion must replay complete durable receive backlog")))
    (values fork next)))

(declaim (ftype function physical-promotion-preflight-runtime))

(declaim (ftype function physical-promotion-publish-retained-intent))

(defun physical-promotion-make-pending (state metadata prefix generation namespace child time reference)
  "Build pending control bytes and runtime state for the selected archive.
The caller publishes the bytes and installs the state after owner handoff."
  (let* ((fork (getf metadata :lsn))
         (next (getf metadata :next-xid))
         (bytes (physical-recovery-control-image
                 3 (getf state :system-id) (getf state :timeline)
                 (getf state :manifest-digest) (getf state :checkpoint) fork next
                 :state-time time :promotion-child child :promotion-generation generation
                 :applied-timeline (physical-recovery-replay-timeline state)
                 :history-target (getf state :history-target) :history-digest (getf state :history-digest)
                 :promotion-source-namespace namespace :retained-reference reference))
         (pending (copy-list state)))
    (loop for (key value) on (physical-recovery-decode-control bytes) by #'cddr
          do (setf (getf pending key) value))
    (setf (getf pending :required-applied) fork (getf pending :observed-next-xid) next
          (getf pending :wal-prefix-lsn) fork (getf pending :wal-prefix-bytes) prefix
          (getf pending :source-checkpoint) (copy-list (getf metadata :source-checkpoint))
          (getf pending :source-reservation) (copy-list (getf metadata :source-reservation)))
    (values bytes pending)))

(defun physical-promotion-publish-intent (&optional requested-child)
  "Privately bind a chosen child to verified local recovery without granting writer authority.

Before publishing intent, check the saved catalog against any live catalog and
preflight the runtime allocation and retention state. A preflight rejection
leaves phase 2 unchanged. The caller owns PGDATA and follow-command, reload
and quiescence in that order; worker shutdown and join occur outside this call.
After preflight, freeze the namespace floor before publishing pending intent;
the selected child still forks from the actual replay parent.
Publish the retained parent prefix and pending control under the receive-store
mutex.  Install pending runtime state only after control readback and owner
handoff.  Failure during publication fences recovery."
  (with-physical-promotion-exclusion (2)
    (physical-follow-promotion-cleanup-config)
    (physical-recovery-publish-selected-history *physical-receive-store*)
    (let* ((state *physical-recovery-state*)
           (namespace (physical-promotion-check-base-control state))
           (floor (physical-promotion-timeline-floor state))
           (child (physical-promotion-select-child floor)))
      (when (and requested-child (not (eql requested-child child)))
        (error "requested promotion child is not the next local timeline"))
      (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read state)
        (when (and *catalog* (not (equalp catalog (catalog-image-bytes))))
          (error "promotion retained catalog differs from its restartpoint"))
        (multiple-value-bind (fork next) (physical-promotion-check-progress state metadata prefix generation)
          (physical-promotion-preflight-runtime
           next (getf (getf metadata :source-reservation) :bound) fork)
          (physical-promotion-freeze-timeline-floor floor)
          (let ((time (- (get-universal-time) 2208988800)) (finished nil))
            (unwind-protect
                 (let ((pending
                         (physical-promotion-publish-retained-intent
                          state metadata fork
                          (lambda (reference)
                            (physical-promotion-make-pending
                             state metadata prefix generation namespace child time reference)))))
                   (setf *physical-recovery-state* pending)
                   (physical-recovery-control-test-point :after-write +db-in-archive-recovery+)
                   (setf finished t)
                   pending)
              (unless finished (setf *physical-recovery-failed-p* t)))))))))

(defun physical-promotion-open-file (path &optional writable)
  "Open an existing bounded regular inode without following symlinks."
  (open-bounded-regular-file path +wal-segment-size+ writable))

(defun physical-promotion-read-file (path)
  (read-bounded-regular-file path +wal-segment-size+))

(defun physical-promotion-history-image (state)
  "Canonical native reason, fixed for retry; C history bytes carry no authority."
  (let ((parent (physical-recovery-replay-timeline state)))
    (timeline-encode-history (getf state :promotion-child) parent
                             (getf state :applied) "pg-lisp promotion"
                             (unless (= 1 parent) (timeline-history-bytes parent)))))

(defun physical-promotion-check-history (state allow-history)
  "Check the pending child against the frozen floor and local history files.
A published child must match the saved history and the intervening probe."
  (let* ((floor (physical-promotion-timeline-floor state)) (child (getf state :promotion-child))
                                                           (path (timeline-history-path child *pgdata*)))
    (unless (> child floor) (error "pending child does not exceed known timeline floor"))
    (if (wal-namespace-file-stat path t)
        (unless (and allow-history
                     (loop for timeline from (1+ floor) below child
                           always (wal-namespace-file-stat (timeline-history-path timeline *pgdata*) t))
                     (equalp (physical-promotion-history-image state) (physical-promotion-read-file path)))
          (error "pending child history is already published or differs"))
        (unless (= child (physical-promotion-select-child floor))
          (error "pending child selection changed")))))

(declaim (ftype function physical-promotion-check-pending-slots))

(defun physical-promotion-check-pending (state metadata prefix generation &key allow-history)
  "Revalidate durable promotion ownership and persistent slot floors before the
child state is published. History publication ends prefix-install eligibility."
  (let ((control (physical-recovery-read-control *pgdata*)))
    (unless (equal (getf state :retained-reference) (getf control :retained-reference))
      (error "retained promotion archive pending reference changed"))
    (physical-promotion-check-pending-retained
     control metadata (when *physical-receive-store* (physical-store-retained-history *physical-receive-store*)))
    (physical-promotion-check-history state allow-history)
    (unless (and
             (every (lambda (key) (eql (getf state key) (getf control key)))
                    '(:control-version :phase :system-id :timeline :manifest-digest
                      :checkpoint :applied :next-xid :db-state :state-time
                      :promotion-child :promotion-generation :promotion-source-namespace))
             (= generation (getf control :promotion-generation))
             (eq (physical-promotion-check-base-control state)
                 (getf control :promotion-source-namespace))
             (= (getf state :wal-prefix-lsn) (getf control :applied))
             (equalp prefix (getf state :wal-prefix-bytes))
             (equal (getf metadata :source-checkpoint) (getf state :source-checkpoint))
             (equal (getf metadata :source-reservation) (getf state :source-reservation)))
      (error "pending promotion ownership or generation proof changed"))
    (multiple-value-prog1
        (physical-promotion-check-progress state metadata prefix generation 3)
      (physical-promotion-check-pending-slots state))))

(defun physical-promotion-install-prefix ()
  "Install the verified pending child prefix before history/EOR; never admit writes.
Retry only while child history is absent. Later transition recovery must inspect
history/EOR instead of calling this helper and overwriting child WAL."
  (with-physical-promotion-exclusion (3)
    (let ((state *physical-recovery-state*))
      (multiple-value-bind (metadata catalog prefix generation) (physical-restartpoint-read state)
        (declare (ignore catalog))
        (physical-promotion-check-pending state metadata prefix generation)
        (physical-promotion-ensure-pending-retained state metadata)
        (let ((fork (getf state :applied)) (child (getf state :promotion-child)) (finished nil))
          (wal-namespace-file-stat (wal-segment-path (floor fork +wal-segment-size+) child :timeline) t)
          (unwind-protect
               (prog1
                   (wal-install-timeline-prefix (physical-recovery-replay-timeline state) child fork prefix
                                                (physical-promotion-parent-namespace state))
                 (setf finished t))
            (unless finished (setf *physical-recovery-failed-p* t))))))))
