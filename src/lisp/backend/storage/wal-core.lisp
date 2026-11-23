;;;; wal.lisp -- small physical WAL with full-page after-images.

(in-package #:pg)

(defconstant +wal-control-maximum-bytes+ 1024
  "Maximum control line characters, excluding its terminating newline.")

(declaim (ftype function logical-slot-continuation-gc-fenced-p))

(defconstant +wal-max-page-images-per-record+ 512)

(declaim (ftype function wal-fpw-rebind-incarnation))

(declaim (ftype function wal-logical-decoding-status-record-info
                wal-logical-status-from-checkpoint wal-logical-status-next))

(declaim (ftype function wal-current-full-page-writes-fields
                wal-fpw-note-published-checkpoint wal-fpw-state-from-checkpoint
                wal-fpw-state-next wal-fpw-install-recovered-state wal-fpw-prepare-recovery))

(declaim (ftype function wal-full-page-writes-change-record-info))

(declaim (ftype function wal-parameter-change-record-info wal-recover-resource-parameters))

(declaim (ftype function wal-running-xacts-record-info
                wal-append-running-xacts-under-lock))

(declaim (ftype function wal-end-of-recovery-record-info))

(defvar *wal-keep-size-mb* 0
  "Published wal_keep_size mirror in MB; retention uses a captured configuration.")

(defparameter *max-slot-wal-keep-bytes* (* 1024 1024 1024)
  "Published slot limit mirror in bytes; NIL means unlimited.
Retention uses the policy captured from an installed configuration.")

(defstruct wal-slot-retention-policy
  "MODE is :bytes or :segments; LIMIT is the selected byte cap, or NIL for
unlimited. Segment mode rounds the cap down to whole segments for recycling.
MAX-WAL-BYTES sets the normal WAL window used by catalog status."
  mode limit (max-wal-bytes 1073741824))

(defvar *wal-slot-retention-policy* nil
  "Published slot policy mirror; NIL before configuration is installed.
Runtime retention requires a captured installed policy.")

(defvar *wal-replication-slot-loss-collector* nil
  "Dynamically bound list receiving durably invalidated slots during WAL maintenance.")

(defvar *wal-slot-recycling-pending* nil
  "One process-local pending recycling ticket, retained across cancellation.")

(defvar *wal-shutdown-defer-recycling* nil)

(declaim (ftype function wal-capture-slot-recycling-work))

(declaim (ftype function cleanup-invalidated-logical-replication-state))

(defstruct (commit-policy
            (:constructor %make-commit-policy (local-durability remote-wait)))
  "Requested local durability and remote wait, captured before publication.
This policy is a choice, distinct from a WAL receipt's historical facts."
  (local-durability :flush :read-only t)
  (remote-wait nil :read-only t))

(defun wal-require-local-commit-durability (mode)
  "Validate the local durability policy before publication."
  (unless (member mode '(:flush :async))
    (ereport "XX000" "invalid local commit durability policy"))
  mode)

;; Historical facts for one accepted commit: XID, immutable source identity,
;; that transaction's terminal end, and whether WAL durability was observed.
;; NIL is a valid explicit source.  Without WAL the end and proof are NIL;
;; the direct CLOG commit can nevertheless be durable.

(defstruct (wal-commit-receipt
            (:constructor %make-wal-commit-receipt (xid source-id terminal-end wal-durable-p)))
  ;; This data-only snapshot owns no backend, spool or callback and is never
  ;; upgraded later.
  (xid nil :read-only t)
  (source-id nil :read-only t)
  (terminal-end nil :read-only t)
  (wal-durable-p nil :read-only t))

(defstruct wal-page-image
  ;; XIDS lists actual writer FullXIDs, excluding internal markers 0 and 1.
  ;; Provenance describes the whole image: :unknown, :row, or :maintenance.
  relfilenode (fork :main) block page (xids '()) (provenance :unknown))

(defstruct wal-replication-slot
  "KIND distinguishes logical and physical slots, including unbound logical slots.
Active owners are process-local. The child-owner import token is durable and
read-only; the appropriate codec stores durable slot fields.
Startup checks known logical provenance system IDs against the local cluster.
Logical updates guard identity, progress and the first terminal cause."
  name database-oid restart-lsn consistent-lsn confirmed-flush-lsn active-owner
  active-owner-backend
  ;; Process-local publication and retirement state. An uncertain logical slot
  ;; keeps its old pin and requires a cold load before further use.
  (drop-claim nil) (creation-failed-p nil) (state-uncertain-p nil)
  (catalog-xmin nil)
  (logical-provenance nil :read-only t)
  (child-owner nil :read-only t)
  confirmed-timeline invalidation-cause
  (plugin "pg_lisp")
  (kind :logical) system-id restart-timeline (temporary-p nil) temporary-owner
  (temporary-lifetime nil :read-only t)
  (inactive-since 0)
  (pending-xids (make-hash-table :test #'eql))
  (wal-status :reserved))

(declaim (special *wal-replication-slots*))

(defvar *wal-slot-profiles* (make-hash-table :test #'eq :weakness :key :synchronized t)
  "Immutable transport profiles; synchronized for readers outside *WAL-LOCK*.")

(defvar *wal-new-slot-profile* :text
  "Transport profile selected for the current slot-creation operation.")

(defun wal-slot-profile (slot)
  (gethash slot *wal-slot-profiles* :text))

(defun wal-slot-set-initial-profile (slot profile)
  "Set a profile before SLOT is published; never mutate an existing profile."
  (unless (member profile '(:text :binary-bytea))
    (error "invalid replication slot transport profile ~s" profile))
  (when (or (nth-value 1 (gethash slot *wal-slot-profiles*))
            (and (boundp '*wal-replication-slots*)
                 (eq slot (gethash (wal-replication-slot-name slot) *wal-replication-slots*))))
    (error "replication slot transport profile is already initialized or published"))
  (setf (gethash slot *wal-slot-profiles*) profile)
  slot)

(defvar *physical-slot-horizons*
  (make-hash-table :test #'eq :weakness :key :synchronized t)
  "Optional durable data and catalog horizons, keyed by physical slot generation.")

(defun physical-slot-horizons (slot)
  "Return the stored (data catalog) FullTransactionIds, or NIL when both are absent."
  (gethash slot *physical-slot-horizons*))

(defun (setf physical-slot-horizons) (horizons slot)
  "Store HORIZONS as supplied, or clear the stored pair when HORIZONS is NIL."
  (if horizons
      (setf (gethash slot *physical-slot-horizons*) horizons)
      (remhash slot *physical-slot-horizons*))
  horizons)

(defun wal-copy-replication-slot-with-profile (slot)
  "Copy a slot while preserving its durable side metadata."
  (let ((copy (copy-wal-replication-slot slot)))
    (unless (eq (wal-slot-profile slot) :text)
      (wal-slot-set-initial-profile copy (wal-slot-profile slot)))
    (let ((horizons (physical-slot-horizons slot)))
      (when horizons (setf (physical-slot-horizons copy) (copy-list horizons))))
    copy))

(defun wal-logical-slot-p (slot)
  "Test the explicit slot kind; a nullable database alone does not identify it."
  (eq :logical (wal-replication-slot-kind slot)))

(declaim (ftype function logical-slot-continuation-check-slot))

(declaim (ftype function physical-slot-check-temporary-live-under-lock
                physical-slot-check-temporary-candidate-under-lock
                physical-slot-temporary-drop-under-lock
                physical-slot-lifetime-call-under-lock))

(defun wal-require-known-replication-slot (slot)
  "Reject failed creation or uncertain state until a cold load."
  ;; A failed publication may still be visible on disk and must retain its pin.
  (when (wal-replication-slot-creation-failed-p slot)
    (ereport "55000" "replication slot ~s creation failed; restart required"
             (wal-replication-slot-name slot)))
  (when (wal-replication-slot-state-uncertain-p slot)
    (ereport "55000" "replication slot ~s state is uncertain; restart required"
             (wal-replication-slot-name slot)))
  slot)

(defun wal-require-usable-replication-slot (slot)
  ;; Pending retirement permits only a retry with the stored claim.
  (wal-require-known-replication-slot slot)
  (when (wal-replication-slot-drop-claim slot)
    (ereport "55000" "replication slot ~s has a pending drop"
             (wal-replication-slot-name slot)))
  slot)

(declaim (special *wal-lock*))

(declaim (ftype function physical-slot-check-name-under-lock physical-slot-finish-name-removal-under-lock))

(declaim (ftype function physical-slot-check-file-operation-name))

(defun wal-find-usable-replication-slot-under-lock (name)
  ;; Look up and check the generation while the registry is locked.
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  ;; Group admission follows the actual registry lookup, before callers use it.
  (physical-slot-check-name-under-lock name)
  (let ((slot (gethash name *wal-replication-slots*)))
    (when slot
      (wal-require-usable-replication-slot slot)
      (when (wal-physical-slot-p slot)
        (physical-slot-check-temporary-live-under-lock slot))
      (logical-slot-continuation-check-slot slot))))

(defun wal-require-logical-slot (slot)
  "Reject physical slots before logical decoding or acknowledgment."
  (wal-require-usable-replication-slot slot)
  (unless (wal-logical-slot-p slot)
    (ereport "55000" "physical replication slot cannot use logical acknowledgment or decoding"))
  slot)

(defun wal-physical-slot-p (slot)
  "Test the explicit physical kind, not the database field."
  (eq :physical (wal-replication-slot-kind slot)))

(defvar *wal-physical-sender-xmins* (make-hash-table :test #'eq)
  "Transient data horizons of slotless senders, guarded by *WAL-LOCK*.")

(defun wal-logical-slot-covered-p (slot end-lsn)
  "Check that all positions in SLOT are covered by a WAL prefix ending at END-LSN."
  (let ((confirmed (wal-replication-slot-confirmed-flush-lsn slot)))
    (and (typep end-lsn '(unsigned-byte 64))
         (<= (wal-replication-slot-restart-lsn slot) end-lsn)
         (<= (wal-replication-slot-consistent-lsn slot) end-lsn)
         (or (null confirmed) (<= confirmed end-lsn)))))

(defun wal-standby-logical-catalog-xmin (slot system-id replay-lsn next-xid history policy)
  "Return the catalog pin of a usable logical slot covered by this replay boundary."
  (declare (notinline logical-slot-provenance-system-id))
  (let ((provenance (wal-replication-slot-logical-provenance slot))
        (horizon (wal-replication-slot-catalog-xmin slot)))
    (when (and system-id provenance (running-xacts-normal-xid-p horizon)
               (eql system-id (logical-slot-provenance-system-id provenance))
               (eq :reserved (wal-replication-slot-wal-status slot))
               (not (or (wal-replication-slot-drop-claim slot)
                        (wal-replication-slot-creation-failed-p slot)
                        (wal-replication-slot-state-uncertain-p slot)))
               (wal-logical-slot-covered-p slot replay-lsn)
               (<= (xid-epoch-start (min next-xid +max-xid+)) horizon next-xid)
               (<= (- next-xid horizon) #x80000000)
               (logical-slot-history-positions-p slot history)
               (logical-slot-standby-policy-covers-p slot policy replay-lsn))
      horizon)))

(defun wal-physical-feedback-horizons (xmin &key include-senders logical-system-id replay-lsn next-xid
                                              logical-history logical-policy)
  "Return separate data and catalog minima under the slot registry lock.
Registered physical slots contribute their durable data and catalog horizons
unless their WAL status is lost. INCLUDE-SENDERS adds transient data horizons
from slotless senders. With LOGICAL-SYSTEM-ID and LOGICAL-HISTORY, also consider
logical catalog horizons covered by replay."
  (let ((catalog-xmin nil))
    (flet ((note-catalog (catalog)
             (when catalog
               (setf catalog-xmin (if catalog-xmin (min catalog-xmin catalog) catalog)))))
      (sb-thread:with-mutex (*wal-lock*)
        (loop for slot being the hash-values of *wal-replication-slots*
              do (cond ((and (wal-physical-slot-p slot)
                             (not (eq :lost (wal-replication-slot-wal-status slot))))
                        (destructuring-bind (&optional data catalog) (physical-slot-horizons slot)
                          (when data (setf xmin (min xmin data)))
                          (note-catalog catalog)))
                       ((and logical-system-id logical-history (wal-logical-slot-p slot))
                        (note-catalog (wal-standby-logical-catalog-xmin
                                       slot logical-system-id replay-lsn next-xid logical-history logical-policy)))))
        (when include-senders
          (loop for sender-xmin being the hash-values of *wal-physical-sender-xmins*
                do (setf xmin (min xmin sender-xmin))))))
    (values xmin catalog-xmin)))

(defun wal-limit-xmin-by-physical-feedback (xmin &key include-senders include-catalog)
  "Lower XMIN to retained physical feedback. Include catalog pins only on request."
  (multiple-value-bind (data catalog) (wal-physical-feedback-horizons xmin :include-senders include-senders)
    (if (and include-catalog catalog) (min data catalog) data)))

(defun wal-valid-slot-plugin-state-p (plugin database-oid confirmed-flush-lsn)
  "A standard slot must be database-bound and confirmed in its first durable image."
  (and (stringp plugin)
       (member plugin '("pg_lisp" "pgoutput") :test #'string=)
       (or (string= plugin "pg_lisp")
           (and database-oid confirmed-flush-lsn))))

(defvar *wal-lock* (sb-thread:make-mutex :name "pg wal and page cache lock"))
(defvar *wal-page-cache* (make-hash-table :test #'equal))
(defvar *wal-page-cache-xids* (make-hash-table :test #'equal))

(defparameter *wal-dirty-resident-limit-pages* 512
  "Maximum dirty page vectors across the shared cache, private batch and pending owner.")

(defstruct wal-dirty-store spool (next-slot 0) (free-slots nil) (resident 0) (disk-refs 0))

(defstruct wal-dirty-reference store key page slot checksum)

(defvar *wal-dirty-store* nil)

(declaim (notinline logical-spool-path logical-spool-stream))

(defvar *wal-page-batch* nil)
(defvar *wal-page-batch-xids* nil)

(defvar *wal-row-page-write-p* nil
  "True only while executing certified SQL row writes.")

(defvar *wal-page-batch-split* nil)
(defvar *wal-xid-first-lsn* (make-hash-table :test #'eql))
(defvar *wal-snapshot-retention-pins* (make-hash-table :test #'eq))
(defvar *wal-replication-slots* (make-hash-table :test #'equal))

(defvar *standby-logical-slots-awaiting-replay* nil
  "Copied slot entries held out of the public registry until receive WAL proves them.")

(defvar *standby-logical-slots-rejected* nil
  "Copied slots whose proof failed; their durable files still fence promotion.")

(defvar *max-replication-slots* 10
  "Shared capacity for all public slot names, including temporary and lost slots.")

(declaim (ftype function logical-slot-continuation-cold-check-admission))

;; Check a supplied count and maximum, including the zero-capacity case.

(defun wal-check-replication-slot-capacity-for-count (count maximum)
  (unless (and (typep count '(integer 0 *)) (typep maximum '(integer 0 *)))
    (error "replication slot capacity requires nonnegative counts"))
  (when (zerop maximum)
    (ereport "55000" "replication slots require max_replication_slots greater than zero"))
  (when (>= count maximum)
    (ereport "53400" "all replication slots are in use"))
  (values))

;; Public slot creation checks cold admission before shared capacity.

(defun wal-check-replication-slot-capacity ()
  "Check shared capacity under WAL, counting temporary and lost slots too."
  (logical-slot-continuation-cold-check-admission)
  (wal-check-replication-slot-capacity-for-count
   (hash-table-count *wal-replication-slots*) *max-replication-slots*))

(defvar *wal-logical-catalog-event-interests* (make-hash-table :test #'eq))
(defvar *wal-logical-slot-owner* nil)
(defvar *wal-pending-btree-split* nil)
(defvar *wal-pending-deletes* '())
(defvar *wal-catalog-flush-generation* 0)
(defvar *wal-poisoned* nil)
(defvar *wal-end-lsn* 0)
(defvar *wal-checkpoint-lsn* 0)

(defvar *wal-insert-timeline* 1
  "Committed insertion identity; a pending recovery child must not overwrite it.")

(defvar *wal-checkpoint-timeline* 1
  "Timeline of the checkpoint record, independently of current insertion.")

(defvar *wal-prev-timeline* 1
  "Previous recovery timeline, as C XLogCtl PrevTimeLineID; persists across checkpoints.")

(defvar *wal-segment-namespace* :timeline
  "Explicit filename grammar; changing insertion TLI never reinterprets old names.")

(defvar *wal-checkpoint-snapshot* nil
  "Facts belonging to the published checkpoint, never a merely flushed candidate.")

(defvar *wal-control-metadata* nil)

(defvar *wal-xid-reserved-through* nil)

(defvar *wal-durable-lsn* 0)

(defvar *wal-storage-incarnation* (gensym "WAL-STORE-")
  "WAL-owned token for the local insertion domain, retained across reload.
It is separate from the source identity used for remote acknowledgments.")

(defvar *wal-fpw-state* nil
  "Primary policy evidence under WAL lock: NIL is unknown, :UNRECOVERED awaits redo.
A state with ENABLED NIL is known OFF. It belongs to the current storage
incarnation and is reconstructed from checkpoint and WAL, not a control extension.")

(defvar *wal-logical-policy* nil
  "WAL-owned logical status evidence; NIL is unknown, :UNRECOVERED awaits redo.")

(defvar *wal-logical-info* nil
  "Current incarnation-owned logical info, protected by the XID lock.")

(defvar *wal-logical-info-generation* 0
  "Monotone generation under XID lock; CLOG rebind retains the same generation.")

(defvar *wal-logical-info-activations* (make-hash-table :test #'eq)
  "Activation interest tokens, protected by the XID lock.")

(defvar *wal-logical-info-disable-pending* nil
  "Deferred logical info disable request, protected by the WAL lock.")

(defvar *logical-capture-force* nil
  "Capture mode for new XIDs, protected by the XID lock and changed under WAL.")

(declaim (ftype function wal-reset-logical-policy-under-lock
                wal-current-logical-decoding-fields wal-prepare-logical-policy-recovery
                wal-install-recovered-logical-policy wal-logical-note-published-checkpoint
                wal-logical-capture-required-under-lock-p
                wal-process-pending-logical-disable-under-lock))

(defstruct (wal-writer-position
            (:constructor make-wal-writer-position (incarnation timeline end)))
  "An LSN bound to the storage incarnation and insertion timeline that own it."
  (incarnation nil :read-only t) (timeline nil :read-only t) (end 0 :read-only t))

(defvar *wal-explicit-write-frontier* nil)

(defvar *wal-async-flush-frontier* nil)

(defstruct (wal-page-publication (:constructor %make-wal-page-publication))
  ;; Before FIFO handoff the stack normally closes SPOOL on exit. A transaction's
  ;; CLOG reservation retains the owner after its terminal attempt starts, so
  ;; cleanup leaves the spool even if handoff fails. The FIFO retains the sealed
  ;; spool, dirty maps, CLOG completion and WAL identity through head retirement. A transaction
  ;; completion points back to this owner; maintenance has XID zero and no
  ;; completion. NEXT links owners in terminal order.
  spool (state :stack) prepared-p pages contributors resets split-p xid
  next completion pressure-ticket (kind :transaction)
  incarnation timeline terminal-end replacement-pages replacement-contributors)

(defvar *wal-pending-page-publication* nil
  "Head of the page publication FIFO, protected with its tail by *WAL-LOCK*.")

(defvar *wal-pending-page-publication-tail* nil)

(defvar *wal-page-publication-appender* nil)

(defun wal-check-page-publication-queue-under-lock ()
  "Validate FIFO links, identity and increasing terminal ends under WAL lock."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((last nil) (previous-end 0))
    (loop for owner = *wal-pending-page-publication* then (wal-page-publication-next owner)
          while owner
          do (unless (and (wal-page-publication-p owner)
                          (member (wal-page-publication-state owner) '(:pending :applied :retiring))
                          (or (null last) (eq :pending (wal-page-publication-state owner)))
                          (eq *wal-storage-incarnation* (wal-page-publication-incarnation owner))
                          (eql *wal-insert-timeline* (wal-page-publication-timeline owner))
                          (typep (wal-page-publication-terminal-end owner)
                                 '(integer 1 18446744073709551615))
                          (< previous-end (wal-page-publication-terminal-end owner))
                          (<= (wal-page-publication-terminal-end owner) *wal-end-lsn*))
               (wal-invalid-input "invalid pending page publication queue"))
             (setf last owner previous-end (wal-page-publication-terminal-end owner)))
    (unless (eq last *wal-pending-page-publication-tail*)
      (wal-invalid-input "pending page publication tail differs from its chain")))
  (values))

(defun wal-require-no-pending-page-publication ()
  "Reject page debt at a storage identity or lifecycle boundary.
Transaction and maintenance publishers enqueue and settle before return;
synchronous barriers settle predecessors first. Caller holds *WAL-LOCK* or has
lifecycle quiescence."
  (when (or *wal-pending-page-publication* *wal-pending-page-publication-tail*)
    (wal-invalid-input "pending page publication prevents another storage publisher"))
  (values))

(defun wal-require-no-pending-storage ()
  "Require both page and CLOG debt to be absent before changing storage."
  (wal-require-no-pending-page-publication)
  (clog-require-no-pending-commits))

(defun wal-check-page-publication-owner-under-lock (owner)
  (wal-check-page-publication-queue-under-lock)
  (unless (loop for entry = *wal-pending-page-publication* then (wal-page-publication-next entry)
                while entry thereis (eq owner entry))
    (wal-invalid-input "page publication is not installed in this WAL store"))
  owner)

(defun wal-pending-page-reference (key)
  "Return the newest queued page, presence flag and matching contributors under WAL lock."
  (when *wal-pending-page-publication*
    (wal-check-page-publication-queue-under-lock)
    (let ((reference nil) (present nil) (contributors nil))
      (loop for owner = *wal-pending-page-publication* then (wal-page-publication-next owner)
            while owner
            do (multiple-value-bind (candidate found)
                   (gethash key (wal-page-publication-pages owner))
                 (when found
                   (setf reference candidate present t
                         contributors (gethash key (wal-page-publication-contributors owner))))))
      (values reference present contributors))))

(defun wal-pending-page-contributors (key)
  (nth-value 2 (wal-pending-page-reference key)))

(declaim (ftype function clog-require-no-pending-commits))

(declaim (special *query-temp-file-limit-bytes*)
         (notinline query-temp-cleanup-domain-identity query-temp-cleanup-domain-lock
                    query-temp-cleanup-owner-domain query-temp-cleanup-owner-state
                    logical-spool-owner logical-spool-closed-p logical-spool-failed-p))

(defvar *wal-temp-cleanup-domain* nil
  "Cleanup authority for primary WAL spools, tied to the WAL lock and store identity.
Publication and shared scratch references still determine each spool's lifetime.")

(defun wal-temp-cleanup-domain-under-lock ()
  "Return this store's domain under the WAL lock; reject stale authority.
An empty previous domain can be replaced after the store incarnation changes."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((domain *wal-temp-cleanup-domain*))
    (when (and domain (not (eq *wal-lock* (query-temp-cleanup-domain-lock domain))))
      (wal-invalid-input "temporary cleanup belongs to another WAL lock"))
    (when (and domain
               (not (eq *wal-storage-incarnation* (query-temp-cleanup-domain-identity domain))))
      (unless (query-temp-cleanup-domain-empty-p domain)
        (wal-invalid-input "temporary cleanup belongs to the previous WAL store"))
      (setf domain nil))
    (or domain
        (setf *wal-temp-cleanup-domain*
              (make-query-temp-cleanup-domain *wal-lock* *wal-storage-incarnation*)))))

(defun call-with-wal-temp-cleanup-domain (function)
  (let ((domain (wal-temp-cleanup-domain-under-lock)))
    (call-with-query-temp-cleanup-domain domain *wal-storage-incarnation* function)))

(defun make-wal-owned-spool ()
  "Create and adopt a private spool before it can be used or published.
The caller must keep allocation, adoption and publication uninterruptible."
  (call-with-wal-temp-cleanup-domain
   (lambda ()
     (let* ((*query-temp-file-limit-bytes* nil)
            (spool (make-logical-spool)))
       (adopt-query-temp-owner-by-domain (logical-spool-owner spool) *wal-temp-cleanup-domain*)
       spool))))

(defun call-with-wal-spool-owner (spool function)
  "Rebind SPOOL's owner under the current WAL domain for later use."
  (call-with-wal-temp-cleanup-domain
   (lambda ()
     (unless (eq *wal-temp-cleanup-domain*
                 (query-temp-cleanup-owner-domain (logical-spool-owner spool)))
       (wal-invalid-input "temporary spool does not belong to this WAL store"))
     (call-with-logical-spool-owner spool function))))

(defun wal-close-owned-spool (spool)
  "Close SPOOL or retry its closed owner's cleanup, requiring resolution."
  (call-with-wal-spool-owner
   spool
   (lambda ()
     (if (logical-spool-closed-p spool)
         (retry-query-temp-owner-cleanup (logical-spool-owner spool))
         (close-logical-spool spool))
     (unless (eq :resolved (query-temp-cleanup-owner-state (logical-spool-owner spool)))
       (error "WAL temporary spool cleanup failed"))))
  (values))

(defun wal-retry-pending-temp-cleanup-under-lock ()
  "Retry WAL temporary cleanup even when WAL is poisoned.
Shutdown calls this before health-checked finalization. It does not settle
pending page publications or CLOG debt."
  (call-with-wal-temp-cleanup-domain
   (lambda () (retry-pending-query-temp-cleanup *wal-temp-cleanup-domain*)))
  (values))

(defun wal-require-no-temp-cleanup-under-lock ()
  "Require the domain to be empty at a storage identity or shutdown boundary.
This is separate from the checks for pending page and CLOG debt."
  (let ((domain (wal-temp-cleanup-domain-under-lock)))
    (unless (query-temp-cleanup-domain-empty-p domain)
      (wal-invalid-input "temporary cleanup prevents WAL storage replacement")))
  (values))

(declaim (ftype function wal-logical-info-publish-under-lock))

(defun wal-replace-storage-incarnation-under-lock ()
  "Rotate local WAL store identity only after pending page and CLOG debt is gone.
This identity is separate from the source used for remote acknowledgments."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (require-primary-wal-writer-retired-under-lock)
  (wal-require-no-pending-storage)
  (wal-require-no-temp-cleanup-under-lock)
  (let ((incarnation (gensym "WAL-STORE-")))
    (wal-logical-info-publish-under-lock nil :rebase t :incarnation incarnation)
    (setf *wal-fpw-state* nil)
    (wal-reset-logical-policy-under-lock)
    incarnation))

(defun wal-replace-storage-incarnation ()
  "Check debt and rotate identity under WAL lock.
The caller must already exclude writers for the whole storage adoption; this
short check alone cannot do so."
  (sb-thread:with-mutex (*wal-lock*)
    (wal-replace-storage-incarnation-under-lock)))

(defvar *wal-ready* nil)
(defvar *wal-output-stream* nil)
(defvar *wal-output-segment* nil)
(defvar *wal-new-segments* '())
(defvar *wal-test-pause-at* nil)
(defvar *wal-test-pause-relfilenode* nil)
(defvar *wal-test-pause-fired* nil)
(defvar *wal-vacuum-full-publication* nil)
(defvar *wal-test-before-vacuum-full-catalog-record-appended-hook* nil)
(defvar *wal-test-before-vacuum-full-catalog-flush-hook* nil)
(defvar *wal-test-after-vacuum-full-catalog-flush-hook* nil)
(defvar *wal-test-pause-lock*
  (sb-thread:make-mutex :name "pg WAL test pause"))

;; BACKEND is defined later in the ASDF component order; its accessor cannot be
;; inlined safely into this earlier storage component.
(declaim (special *backend* *xid-lock* *active-xids*)
         (notinline backend-xid))
(defvar *wal-recovery-group-active* nil)

(defvar *wal-recovery-reservation-batch-clean-p* nil)

(defvar *wal-recovery-page-images* '())

(defvar *wal-recovery-relation-resets* nil)

(defvar *wal-recovery-page-records-seen-p* nil)

(defvar *wal-recovery-page-spool-enabled* nil)

(defvar *wal-recovery-page-spool* nil
  "Recovery page spool, with an ordinary cleanup owner rather than WAL's domain.")

(defvar *wal-recovery-catalog-spool* nil
  "Owns catalog chunks until the batch terminal or recovery cleanup.")

(defvar *wal-recovery-catalog-chunk-count* nil)
(defvar *wal-recovery-catalog-next-chunk* 0)
(defvar *wal-recovery-logical-xid* nil)
(defvar *wal-recovery-logical-chunk-count* nil)
(defvar *wal-recovery-logical-next-chunk* 0)
(defvar *wal-recovery-logical-bytes* 0)
(defvar *wal-recovery-logical-catalog-event-p* nil)

(declaim (ftype function wal-test-pause wal-test-pause-once
                wal-checkpoint
                wal-drop-replication-slot
                wal-release-replication-slot-owner
                wal-invalidate-lagging-replication-slots-under-wal-lock
                wal-remove-checkpointed-segments
                wal-remove-checkpointed-segments-under-wal-lock
                wal-atomic-write-bytes
                wal-append-logical-changes
                wal-recovery-accept-logical-change-chunk
                wal-recovery-finish-logical-change-batch
                wal-recovery-reset-logical-change-batch
                clog-get clog-set clog-reset clog-fsync clog-file-extent))
(declaim (notinline pg-class-relfilenode pg-index-relfilenode
                    database-tables database-indexes catalog-databases))

(defun wal-directory ()
  (pgdata-resolve-path "pg_wal/" *pgdata*))

;; Private staging observation hook; OBSERVER may be NIL.  Otherwise it receives
;; (event operation name fd &optional destination).  Events are :created,
;; :opened-existing, :written, :before-unlink, :unlinked, :before-install and
;; :installed.  NAME identifies the path involved in the event; DESTINATION names
;; the installed path for install events.  :written reports synced bytes, which
;; may already have matched without a write.
;;
;; FD is borrowed and valid only during the callback.  It may be NIL after a
;; prefix stream closes; an observer needing the inode later must dup it at
;; :created.  File creation, file sync plus :written, and namespace changes are
;; paired with their events while interrupts are masked.
;; A callback may exit nonlocally; the caller's cleanup still owns its resources.
;; An event does not itself certify directory durability or grant source-file authority.
(defun wal-staging-observe (observer event operation name fd &optional destination)
  (when observer (funcall observer event operation name fd destination)))

(defun wal-parent-directory (directory)
  (let* ((path (pathname directory))
         (parts (pathname-directory path))
         (parent-parts (if (and parts
                                (not (eq (car (last parts)) :absolute)))
                           (butlast parts)
                           parts)))
    (if (equal parts parent-parts)
        path
        (make-pathname :directory parent-parts
                       :name nil :type nil :version nil
                       :defaults path))))

(defun wal-ensure-directory-durable (directory &optional borrowed-fd)
  "Create DIRECTORY's path and sync each parent entry. A borrowed FD instead
names an existing context directory; check its binding and sync it in place."
  (when borrowed-fd
    (unless (and (typep borrowed-fd '(integer 0 2147483647))
                 (physical-image-context-p *physical-image-context*)
                 (member borrowed-fd
                         (cons (physical-image-context-root-fd *physical-image-context*)
                               (mapcar #'cdr (physical-image-context-children *physical-image-context*))))
                 (equal (pathname directory) (physical-image-fd-path borrowed-fd)))
      (error "existing WAL directory differs from its borrowed descriptor"))
    (physical-image-check-context *physical-image-context*)
    (physical-image-check-children *physical-image-context*)
    (physical-image-check-directory borrowed-fd)
    (sb-posix:fsync borrowed-fd)
    (return-from wal-ensure-directory-durable (pathname directory)))
  (let* ((path (pathname directory))
         (parent (wal-parent-directory path)))
    (unless (equal path parent)
      (wal-ensure-directory-durable parent)
      (ensure-directories-exist (merge-pathnames "wal-directory-dummy" path))
      (fsync-directory parent))
    path))

(defun wal-segment-filename (segment timeline namespace)
  "C's 24-hex timeline/log/segment identity, with the native .wal suffix.
Legacy 16-hex segment names always belong to timeline 1."
  (unless (and (typep segment '(integer 0 1099511627775))
               (typep timeline '(integer 1 4294967295))
               (member namespace '(:legacy :timeline))
               (or (eq namespace :timeline) (= timeline 1)))
    (error "invalid native WAL segment identity"))
  (if (eq namespace :legacy)
      (format nil "~16,'0X.wal" segment)
      (let ((per-id (floor (ash 1 32) +wal-segment-size+)))
        (format nil "~8,'0X~8,'0X~8,'0X.wal"
                timeline (floor segment per-id) (mod segment per-id)))))

(defun wal-parse-segment-name (name namespace)
  "Decode one canonical filename stem without guessing its timeline or grammar."
  (when (and (stringp name)
             (= (length name) (ecase namespace (:legacy 16) (:timeline 24)))
             (every (lambda (char) (find char "0123456789ABCDEF")) name))
    (if (eq namespace :legacy)
        (let ((segment (parse-integer name :radix 16)))
          (when (<= segment 1099511627775) (values 1 segment)))
        (let* ((timeline (parse-integer name :start 0 :end 8 :radix 16))
               (log (parse-integer name :start 8 :end 16 :radix 16))
               (low (parse-integer name :start 16 :end 24 :radix 16))
               (per-id (floor (ash 1 32) +wal-segment-size+)))
          (when (and (plusp timeline) (< low per-id))
            (values timeline (+ (* log per-id) low)))))))

(defun wal-segment-path (segment &optional (timeline *wal-insert-timeline*)
                                   (namespace *wal-segment-namespace*))
  (merge-pathnames (wal-segment-filename segment timeline namespace) (wal-directory)))

(defun wal-shell-file-command (template filename path setting &key restart-file)
  "Expand the file, path, and literal-percent escapes in a WAL shell command.
The setting supplies shell quoting. Expand each replacement exactly once."
  (with-output-to-string (command)
    (loop for offset below (length template)
          for character = (char template offset)
          do (cond
               ((char/= character #\%) (write-char character command))
               ((= (incf offset) (length template))
                (error "incomplete ~a placeholder" setting))
               (t
                (case (char template offset)
                  (#\% (write-char #\% command))
                  (#\f (write-string filename command))
                  (#\p (write-string path command))
                  (#\r
                   (unless restart-file (error "unsupported ~a placeholder" setting))
                   (write-string restart-file command))
                  (otherwise (error "unsupported ~a placeholder" setting))))))))

(defun wal-run-cancellable-restore-shell (command directory check)
  (let ((process nil) (finished nil))
    (unwind-protect
         (progn
           (funcall check)
           (sb-sys:without-interrupts
             ;; With no inherited stdin, SBCL gives the shell its own process group.
             (setf process (sb-ext:run-program "/bin/sh" (list "-c" command)
                                               :directory directory :input nil :output nil
                                               :error nil :wait nil)))
           (loop while (sb-ext:process-alive-p process)
                 do (funcall check) (sleep 0.05))
           (sb-ext:process-wait process)
           (setf finished t)
           (+ (if (eq (sb-ext:process-status process) :signaled) 128 0)
              (sb-ext:process-exit-code process)))
      (when process
        (unless finished
          (sb-ext:process-kill process sb-posix:sigkill :process-group)
          (sb-ext:process-wait process))
        (sb-ext:process-close process)))))

(defun wal-run-restore-command (template name directory-fd &key directory check restart-file
                                                             (setting "restore_command"))
  "Run a restore command with an absolute output path through the held descriptor.
When CHECK signals cancellation, terminate and reap the owned shell."
  (let* ((output (format nil "/proc/~d/fd/~d/~a" (sb-posix:getpid) directory-fd name))
         (command (wal-shell-file-command template name output setting :restart-file restart-file)))
    (if check
        (wal-run-cancellable-restore-shell command directory check)
        (nth-value 2
                   (uiop:run-program (list "/bin/sh" "-c" command) :directory directory
                                                                   :output nil :error-output nil :ignore-error-status t)))))

(defvar *wal-offline-segment-fallback* nil)

(defun wal-read-segment-path (segment &optional (timeline *wal-insert-timeline*)
                                        (namespace *wal-segment-namespace*))
  ;; The offline fallback applies only when the local segment is absent.
  (let ((path (wal-segment-path segment timeline namespace)))
    (if (and *wal-offline-segment-fallback* (not *physical-image-context*)
             (handler-case (progn (sb-posix:lstat (namestring path)) nil)
               (sb-posix:syscall-error (condition)
                 (if (= (sb-posix:syscall-errno condition) sb-posix:enoent)
                     t
                     (error condition)))))
        (or (funcall *wal-offline-segment-fallback* path) path)
        path)))

(defun wal-control-path ()
  (pgdata-resolve-path "global/pg_wal_control" *pgdata*))

(defun wal-replication-slot-directory ()
  (merge-pathnames "pg_replslot/" *pgdata*))

(defun wal-replication-slot-path (name)
  (slot-state-path (wal-replication-slot-directory) name))

(defun wal-page-fork-id (fork)
  (case fork
    (:main 0)
    (:visibility 1)
    (otherwise (error "unknown relation fork ~a" fork))))

(defun wal-page-fork-from-id (fork-id)
  (case fork-id
    (0 :main)
    (1 :visibility)
    (otherwise (error "unknown WAL relation fork id ~a" fork-id))))

(defun wal-page-checksum (page relfilenode block &optional (fork :main))
  "Return a location-bound nonzero CRC16 for PAGE, reserving legacy values."
  (let* ((main-p (eq fork :main))
         (identity-size (if main-p 8 12))
         (copy (make-array (+ identity-size (length page))
                           :element-type '(unsigned-byte 8)
                           :initial-element 0)))
    (page-set-u32 copy 0 relfilenode)
    (if main-p
        (page-set-u32 copy 4 block)
        (progn
          (page-set-u32 copy 4 (wal-page-fork-id fork))
          (page-set-u32 copy 8 block)))
    (replace copy page :start1 identity-size)
    (page-set-u16 copy (+ identity-size +pd-checksum-off+) 0)
    (let ((checksum (logand (wal-crc32 copy) #xFFFF)))
      (if (<= checksum 3) (+ checksum 4) checksum))))

(defun wal-stamp-page (page lsn relfilenode block &optional (fork :main))
  (page-set-u64 page +pd-lsn-off+ lsn)
  (page-set-u16 page +pd-checksum-off+ 0)
  (page-set-u16 page +pd-checksum-off+
                (wal-page-checksum page relfilenode block fork))
  page)

(defun wal-verify-page (page relfilenode block &optional (fork :main))
  (let ((lsn (page-u64 page +pd-lsn-off+))
        (checksum (page-u16 page +pd-checksum-off+)))
    (if (zerop lsn)
        ;; Unstamped pages have no WAL LSN or checksum.
        ;; Persisted WAL pages always carry a nonzero LSN and must have a CRC.
        (zerop checksum)
        (= checksum (wal-page-checksum page relfilenode block fork)))))

(defun wal-check-read-page (page relfilenode block)
  "Reject an unstamped page read from disk when durable checksum mode is enabled.
Return PAGE for the remaining read checks."
  (when (and page (getf *wal-control-metadata* :page-checksum-version)
             (zerop (page-u64 page +pd-lsn-off+)))
    (ereport "XX001" "unstamped page in qualified relation ~a block ~a"
             relfilenode block))
  page)

(defun wal-check-persisted-page (stream relfilenode block page fork)
  "In checksum mode, require a complete stamped page with a valid location checksum.
Permit overwrite or append without leaving a storage gap."
  (when (getf *wal-control-metadata* :page-checksum-version)
    (unless (and (typep block '(unsigned-byte 32))
                 (<= block (relation-nblocks stream))
                 (= (length page) +blcksz+)
                 (plusp (page-u64 page +pd-lsn-off+))
                 (wal-verify-page page relfilenode block fork))
      (error "qualified relation write requires a complete, stamped page without a storage gap"))))

(defun wal-page-key (relfilenode block &optional (fork :main))
  (list relfilenode fork block))

(defun wal-copy-dirty-page (page)
  "Copy PAGE without its persisted LSN/checksum until it is WAL-logged."
  (let ((copy (copy-seq page)))
    (page-set-u64 copy +pd-lsn-off+ 0)
    (page-set-u16 copy +pd-checksum-off+ 0)
    copy))

(define-condition wal-dirty-store-error (error)
  ((cause :initarg :cause :reader wal-dirty-store-error-cause))
  (:report (lambda (condition stream)
             (format stream "dirty page scratch failure: ~a"
                     (wal-dirty-store-error-cause condition)))))

(defun wal-dirty-checksum (key page)
  "Bind scratch bytes to the exact relation, fork and block."
  (let ((bytes (make-array (+ 12 +blcksz+) :element-type '(unsigned-byte 8))))
    (wal-set-u32 bytes 0 (first key))
    (wal-set-u32 bytes 4 (wal-page-fork-id (second key)))
    (wal-set-u32 bytes 8 (third key))
    (replace bytes page :start1 12)
    (wal-crc32 bytes)))

(defun wal-dirty-ensure-spool (store)
  "Adopt scratch before publishing it in STORE or opening its file."
  (unless (wal-dirty-store-spool store)
    (sb-sys:without-interrupts
      (setf (wal-dirty-store-spool store) (make-wal-owned-spool))))
  (let ((spool (wal-dirty-store-spool store)))
    (when (or (logical-spool-closed-p spool) (logical-spool-failed-p spool))
      (error "dirty page scratch is not writable"))
    (call-with-wal-spool-owner spool (lambda () (logical-spool-ensure-file spool)))
    spool))

(defun wal-dirty-new-reference (key page)
  "Own PAGE in memory or an immutable scratch slot before publishing it."
  (unless (= (length page) +blcksz+)
    (error 'wal-dirty-store-error :cause "invalid dirty page size"))
  (sb-sys:without-interrupts
    (when (and *wal-dirty-store*
               (wal-dirty-store-spool *wal-dirty-store*)
               (logical-spool-closed-p (wal-dirty-store-spool *wal-dirty-store*)))
      (wal-dirty-close-empty-store))
    (let ((store (or *wal-dirty-store* (setf *wal-dirty-store* (make-wal-dirty-store)))))
      (if (< (wal-dirty-store-resident store) *wal-dirty-resident-limit-pages*)
          (progn
            (incf (wal-dirty-store-resident store))
            (make-wal-dirty-reference :store store :key key :page page))
          (handler-case
              (let* ((spool (wal-dirty-ensure-spool store))
                     (free (wal-dirty-store-free-slots store))
                     (slot (if free (pop (wal-dirty-store-free-slots store))
                               (prog1 (wal-dirty-store-next-slot store)
                                 (incf (wal-dirty-store-next-slot store)))))
                     (checksum (wal-dirty-checksum key page)))
                (call-with-wal-spool-owner
                 spool
                 (lambda ()
                   (unless free
                     (query-temp-account-space (logical-spool-path spool) +blcksz+))
                   (unless (file-position (logical-spool-stream spool) (* slot +blcksz+))
                     (error "cannot seek dirty page scratch"))
                   (write-sequence page (logical-spool-stream spool))
                   (finish-output (logical-spool-stream spool))))
                (incf (wal-dirty-store-disk-refs store))
                (make-wal-dirty-reference :store store :key key :slot slot :checksum checksum))
            (error (condition) (error 'wal-dirty-store-error :cause condition)))))))

(defun wal-dirty-read-reference (key reference)
  "Read a validated transient page without retaining another resident copy."
  (if (not (wal-dirty-reference-p reference))
      reference
      (progn
        (unless (equal key (wal-dirty-reference-key reference))
          (error 'wal-dirty-store-error :cause "scratch page key mismatch"))
        (or (wal-dirty-reference-page reference)
            (handler-case
                (let* ((store (wal-dirty-reference-store reference))
                       (spool (wal-dirty-store-spool store))
                       (page (make-array +blcksz+ :element-type '(unsigned-byte 8))))
                  (call-with-wal-spool-owner
                   spool
                   (lambda ()
                     (unless (file-position (logical-spool-stream spool)
                                            (* (wal-dirty-reference-slot reference) +blcksz+))
                       (error "cannot seek dirty page scratch"))
                     (unless (= +blcksz+ (read-sequence page (logical-spool-stream spool)))
                       (error "truncated dirty page scratch"))))
                  (unless (= (wal-dirty-checksum key page) (wal-dirty-reference-checksum reference))
                    (error "dirty page scratch checksum mismatch"))
                  page)
              (error (condition) (error 'wal-dirty-store-error :cause condition)))))))

(defun wal-dirty-release-reference (reference)
  (when (wal-dirty-reference-p reference)
    (let ((store (wal-dirty-reference-store reference)))
      (if (wal-dirty-reference-page reference)
          (decf (wal-dirty-store-resident store))
          (progn
            (decf (wal-dirty-store-disk-refs store))
            (push (wal-dirty-reference-slot reference) (wal-dirty-store-free-slots store))))))
  (values))

(declaim (ftype function wal-note-page-xid-under-lock))

(defun wal-dirty-install-page (key page)
  "Publish a complete reference and transfer its ownership without interruption."
  (sb-sys:without-interrupts
    (handler-case
        (let* ((reference (wal-dirty-new-reference key page))
               (map (if *wal-page-batch* *wal-page-batch* *wal-page-cache*))
               (old (gethash key map))
               (installed nil))
          (unwind-protect
               (progn
                 (wal-note-page-xid-under-lock key)
                 (setf (gethash key map) reference
                       installed t)
                 (wal-dirty-release-reference old))
            (unless installed (wal-dirty-release-reference reference))))
      (wal-dirty-store-error (condition) (error condition))
      (error (condition) (error 'wal-dirty-store-error :cause condition))))
  (values))

(defun wal-dirty-remove-page (key pages contributors)
  "Retire a map entry and its owned reference as one uninterruptible operation."
  (sb-sys:without-interrupts
    (handler-case
        (multiple-value-bind (reference present) (gethash key pages)
          (when present
            (remhash key pages)
            (when (hash-table-p contributors) (remhash key contributors))
            (wal-dirty-release-reference reference)))
      (error (condition) (error 'wal-dirty-store-error :cause condition))))
  (values))

(defun wal-dirty-close-empty-store ()
  "Release scratch after the last shared, private or pending reference disappears.
Scratch can span publications. Keep the store until its spool cleanup resolves,
so a closed spool is retried before a new store can be used."
  (sb-sys:without-interrupts
    (when (and *wal-dirty-store*
               (zerop (wal-dirty-store-resident *wal-dirty-store*))
               (zerop (wal-dirty-store-disk-refs *wal-dirty-store*)))
      (handler-case
          (let ((spool (wal-dirty-store-spool *wal-dirty-store*)))
            (when spool
              (wal-close-owned-spool spool))
            (setf *wal-dirty-store* nil))
        (error (condition) (error 'wal-dirty-store-error :cause condition)))))
  (values))

(defun wal-visible-page-under-lock (relfilenode block fork)
  "Read the newest private, shared or pending page, falling back to disk.
Caller holds *WAL-LOCK*.  The key includes the exact fork."
  (let* ((key (wal-page-key relfilenode block fork))
         (reference (or (and *wal-page-batch* (gethash key *wal-page-batch*))
                        (gethash key *wal-page-cache*)
                        (wal-pending-page-reference key))))
    (if reference (wal-dirty-read-reference key reference)
        (smgr-read relfilenode block fork))))

(defun wal-note-page-xid-under-lock (key)
  "Carry contributors from the visible dirty page into the new version.
The zero marker remains sticky for uncertified writes; XIDs are full XIDs."
  (let* ((local (and *wal-page-batch* (nth-value 1 (gethash key *wal-page-batch*))))
         (shared (nth-value 1 (gethash key *wal-page-cache*)))
         (pending (and (not local) (not shared) (nth-value 1 (wal-pending-page-reference key))))
         (dirty (or local shared pending))
         (prior (cond (local (gethash key *wal-page-batch-xids*))
                      (shared (gethash key *wal-page-cache-xids*))
                      (pending (wal-pending-page-contributors key))))
         (contributors (if *wal-page-batch* *wal-page-batch-xids* *wal-page-cache-xids*))
         (xids (copy-list prior))
         (xid (and *backend* (backend-xid *backend*))))
    (when (and dirty (not (member 1 xids :test #'eql))) (pushnew 0 xids :test #'eql))
    (pushnew 1 xids :test #'eql)
    (unless (and *wal-row-page-write-p* xid (>= xid 3) (>= (logand xid #xffffffff) 3))
      (pushnew 0 xids :test #'eql))
    (when xid (pushnew xid xids :test #'eql))
    (setf (gethash key contributors) xids))
  (values))

(defun wal-merge-page-xids (source destination)
  ;; Union includes marker 0, so a tainted batch stays tainted.
  (when source
    (maphash (lambda (key xids)
               (dolist (xid xids)
                 (pushnew xid (gethash key destination) :test #'eql)))
             source))
  (values))

(defun wal-dirty-batch-contributors ()
  "Build merged metadata for changed keys before publishing batch pages."
  (let ((merged (make-hash-table :test #'equal)))
    (maphash (lambda (key xids)
               (setf (gethash key merged)
                     (copy-list (if (nth-value 1 (gethash key *wal-page-cache*))
                                    (gethash key *wal-page-cache-xids*)
                                    (wal-pending-page-contributors key))))
               (dolist (xid xids) (pushnew xid (gethash key merged) :test #'eql)))
             *wal-page-batch-xids*)
    merged))

(defmacro with-wal-page-state-lock (&body body)
  "Use the current page batch, or lock and fail-stop a damaged dirty store."
  `(if *wal-page-batch*
       (progn ,@body)
       (handler-case
           (sb-thread:with-mutex (*wal-lock*)
             (handler-case (progn (wal-check-healthy) ,@body)
               (wal-dirty-store-error (condition)
                 (setf *wal-poisoned* t)
                 (error condition))))
         (wal-dirty-store-error (condition) (wal-fail-stop condition)))))

(defun wal-buffer-read (relfilenode block &optional (fork :main))
  (if (not *wal-ready*)
      (smgr-read relfilenode block fork)
      (let ((page
              (with-wal-page-state-lock
                (wal-visible-page-under-lock relfilenode block fork))))
        (when page (copy-seq page)))))

(defun wal-buffer-write (relfilenode block page &optional (fork :main))
  (unless *wal-ready*
    (return-from wal-buffer-write (smgr-write relfilenode block page fork)))
  (let ((key (wal-page-key relfilenode block fork))
        (copy (wal-copy-dirty-page page)))
    (with-wal-page-state-lock
      (wal-dirty-install-page key copy)))
  (wal-test-pause :after-page-stage)
  (values))

(defun wal-visible-nblocks-under-lock (relfilenode fork)
  "Return the block count, including dirty pages, for this exact relation and fork."
  (let ((nblocks (smgr-nblocks relfilenode fork)))
    (flet ((count-map (map)
             (when map
               (maphash (lambda (key page)
                          (declare (ignore page))
                          (when (and (eql (first key) relfilenode)
                                     (eq (second key) fork))
                            (setf nblocks (max nblocks (1+ (third key))))))
                        map))))
      (when *wal-pending-page-publication*
        (wal-check-page-publication-queue-under-lock)
        (loop for owner = *wal-pending-page-publication* then (wal-page-publication-next owner)
              while owner do (count-map (wal-page-publication-pages owner))))
      (count-map *wal-page-cache*)
      (count-map *wal-page-batch*))
    nblocks))

(defun wal-buffer-nblocks (relfilenode &optional (fork :main))
  (if (not *wal-ready*)
      (smgr-nblocks relfilenode fork)
      (with-wal-page-state-lock
        (wal-visible-nblocks-under-lock relfilenode fork))))

(defun wal-buffer-extend (relfilenode page &optional (fork :main))
  (if (not *wal-ready*)
      (smgr-extend relfilenode page fork)
      (with-wal-page-state-lock
        (let* ((block (wal-visible-nblocks-under-lock relfilenode fork))
               (key (wal-page-key relfilenode block fork))
               (copy (wal-copy-dirty-page page)))
          (wal-dirty-install-page key copy)
          (unless *wal-page-batch* (wal-test-pause :after-page-stage))
          block))))

(defmacro with-wal-page-batch (&body body)
  "Stage related page edits privately and publish them together on success."
  `(if *wal-page-batch*
       (progn ,@body)
       (handler-case
           (sb-thread:with-mutex (*wal-lock*)
             (handler-case
                 (let ((*wal-page-batch* (make-hash-table :test #'equal))
                       (*wal-page-batch-xids* (make-hash-table :test #'equal))
                       (*wal-page-batch-split* nil)
                       (published nil))
                   (wal-check-healthy)
                   (unwind-protect
                        (multiple-value-prog1
                            (progn ,@body)
                          (sb-sys:without-interrupts
                            (handler-case
                                (let ((merged (wal-dirty-batch-contributors))
                                      (replaced nil))
                                  ;; Contributor lists are ready before shared entries change.
                                  (maphash
                                   (lambda (key reference)
                                     (let ((old (gethash key *wal-page-cache*)))
                                       (when old (push old replaced))
                                       ;; An exceptional exit after this point fences WAL;
                                       ;; keep both old and new handles owned until shutdown.
                                       (setf published t
                                             (gethash key *wal-page-cache*) reference)
                                       (multiple-value-bind (xids present) (gethash key merged)
                                         (when present (setf (gethash key *wal-page-cache-xids*) xids)))))
                                   *wal-page-batch*)
                                  (clrhash *wal-page-batch*)
                                  (dolist (old replaced) (wal-dirty-release-reference old))
                                  (when *wal-page-batch-split*
                                    (setf *wal-pending-btree-split* t)))
                              (error (condition)
                                (setf *wal-poisoned* t)
                                (error 'wal-dirty-store-error :cause condition)))))
                     (sb-sys:without-interrupts
                       (handler-case
                           (progn
                             (unless published
                               (maphash (lambda (key reference)
                                          (declare (ignore key))
                                          (wal-dirty-release-reference reference))
                                        *wal-page-batch*))
                             (wal-dirty-close-empty-store))
                         (wal-dirty-store-error (condition) (error condition))
                         (error (condition)
                           (error 'wal-dirty-store-error :cause condition))))))
               (wal-dirty-store-error (condition)
                 (setf *wal-poisoned* t)
                 (error condition))))
         (wal-dirty-store-error (condition) (wal-fail-stop condition)))))

(defun wal-mark-btree-split ()
  (when *wal-page-batch*
    (setf *wal-page-batch-split* t))
  (values))

(defun wal-forget-relation (relfilenode)
  (when *wal-ready*
    (with-wal-page-state-lock
      (wal-require-no-pending-page-publication)
      (let ((keys nil))
        (maphash (lambda (key reference)
                   (declare (ignore reference))
                   (when (eql (first key) relfilenode) (push key keys)))
                 *wal-page-cache*)
        (dolist (key keys)
          (wal-dirty-remove-page key *wal-page-cache* *wal-page-cache-xids*))
        (wal-dirty-close-empty-store))))
  (values))

(defun wal-page-images (&optional excluded-relfilenodes)
  (let ((images '()))
    (maphash (lambda (key page)
               (unless (member (first key) excluded-relfilenodes :test #'eql)
                 (push (make-wal-page-image :relfilenode (first key)
                                            :fork (second key)
                                            :block (third key)
                                            :page (copy-seq (wal-dirty-read-reference key page))
                                            :xids (remove-if (lambda (xid) (< xid 3))
                                                             (gethash key *wal-page-cache-xids*))
                                            :provenance
                                            (let ((xids (gethash key *wal-page-cache-xids*)))
                                              (cond ((member 0 xids :test #'eql) :maintenance)
                                                    ((member 1 xids :test #'eql) :row)
                                                    (t :unknown))))
                       images)))
             *wal-page-cache*)
    (sort images
          (lambda (a b)
            (cond ((/= (wal-page-image-relfilenode a)
                       (wal-page-image-relfilenode b))
                   (< (wal-page-image-relfilenode a)
                      (wal-page-image-relfilenode b)))
                  ((/= (wal-page-fork-id (wal-page-image-fork a))
                       (wal-page-fork-id (wal-page-image-fork b)))
                   (< (wal-page-fork-id (wal-page-image-fork a))
                      (wal-page-fork-id (wal-page-image-fork b))))
                  (t (< (wal-page-image-block a)
                        (wal-page-image-block b))))))))

(defun wal-primary-page-keys (&optional excluded-relfilenodes)
  "Plan a deterministic cache traversal without copying page contents."
  (let ((keys nil))
    (maphash (lambda (key page)
               (declare (ignore page))
               (unless (member (first key) excluded-relfilenodes :test #'eql)
                 (push key keys)))
             *wal-page-cache*)
    (sort keys
          (lambda (a b)
            (cond ((/= (first a) (first b)) (< (first a) (first b)))
                  ((/= (wal-page-fork-id (second a)) (wal-page-fork-id (second b)))
                   (< (wal-page-fork-id (second a)) (wal-page-fork-id (second b))))
                  (t (< (third a) (third b))))))))

(defun wal-primary-page-image (key)
  "Copy one page and its contributor metadata while the WAL lock is held."
  (let* ((reference (gethash key *wal-page-cache*))
         (page (and reference (wal-dirty-read-reference key reference)))
         (xids (gethash key *wal-page-cache-xids*)))
    (unless page (error "planned WAL page disappeared"))
    (make-wal-page-image :relfilenode (first key) :fork (second key) :block (third key)
                         :page (copy-seq page)
                         :xids (remove-if (lambda (xid) (< xid 3)) xids)
                         :provenance (cond ((member 0 xids :test #'eql) :maintenance)
                                           ((member 1 xids :test #'eql) :row)
                                           (t :unknown)))))

(defun wal-primary-page-key-image-size (key)
  (let* ((xids (gethash key *wal-page-cache-xids*))
         (size (+ 12 +blcksz+ 4 (* 8 (count-if (lambda (xid) (>= xid 3)) xids)))))
    (when (> (+ +wal-header-size+ 4 size) +wal-segment-size+)
      (error "WAL page image has too many transaction contributors"))
    size))

(defun map-wal-primary-page-key-chunks (keys function)
  "Visit page keys in the same bounded chunks used for WAL and pressure sizing."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((chunk nil) (count 0) (bytes 0))
    (dolist (key keys)
      (let ((size (wal-primary-page-key-image-size key)))
        (when (and chunk
                   (or (>= count +wal-max-page-images-per-record+)
                       (> (+ +wal-header-size+ 4 bytes size) +wal-segment-size+)))
          (funcall function (nreverse chunk) bytes)
          (setf chunk nil count 0 bytes 0))
        (push key chunk)
        (incf count)
        (incf bytes size)))
    (when chunk (funcall function (nreverse chunk) bytes)))
  (values))

(defun wal-primary-preflight-page-keys (keys)
  "Reject a too-large individual image before the batch begins."
  (dolist (key keys) (wal-primary-page-key-image-size key))
  (values))

(defun wal-page-image-encoded-size (image)
  (+ 12 +blcksz+ 4 (* 8 (length (wal-page-image-xids image)))))

(defun wal-page-payload-length (images)
  (+ 4 (loop for image in images sum (wal-page-image-encoded-size image))))

(defun wal-encode-page-images (images lsn)
  ;; The contributor count uses low 30 bits; high bits encode provenance:
  ;; 00 unknown, 10 row, 11 maintenance. 01 is reserved and rejected.
  (let ((payload (make-array (wal-page-payload-length images)
                             :element-type '(unsigned-byte 8)
                             :initial-element 0))
        (offset 4))
    (wal-set-u32 payload 0 (length images))
    (dolist (image images)
      (wal-stamp-page (wal-page-image-page image) lsn
                      (wal-page-image-relfilenode image)
                      (wal-page-image-block image)
                      (wal-page-image-fork image))
      (wal-set-u32 payload offset (wal-page-image-relfilenode image))
      (wal-set-u32 payload (+ offset 4)
                   (wal-page-fork-id (wal-page-image-fork image)))
      (wal-set-u32 payload (+ offset 8) (wal-page-image-block image))
      (replace payload (wal-page-image-page image) :start1 (+ offset 12))
      (let* ((xids (wal-page-image-xids image))
             (xid-count-offset (+ offset 12 +blcksz+))
             (xid-offset (+ xid-count-offset 4)))
        (unless (<= (length xids) #x3fffffff) (error "too many page contributors"))
        (wal-set-u32 payload xid-count-offset
                     (logior (length xids)
                             (ecase (wal-page-image-provenance image)
                               (:unknown 0) (:row #x80000000) (:maintenance #xc0000000))))
        (loop for xid in xids
              do (wal-set-u64 payload xid-offset xid)
                 (incf xid-offset 8))
        (setf offset xid-offset)))
    payload))

(defun wal-decode-page-images (payload lsn)
  (when (< (length payload) 4)
    (error "WAL page-image record is too short"))
  (let ((offset 4)
        (images nil))
    (loop repeat (wal-u32 payload 0)
          do (let* ((page-offset (+ offset 12))
                    (page-end (+ page-offset +blcksz+))
                    (xids-start (+ page-end 4)))
               (unless (<= page-end (length payload))
                 (error "WAL page-image record has a truncated page"))
               (unless (<= xids-start (length payload))
                 (error "WAL page-image record has a truncated contributor count"))
               (let* ((relfilenode (wal-u32 payload offset))
                      (fork (wal-page-fork-from-id (wal-u32 payload (+ offset 4))))
                      (block (wal-u32 payload (+ offset 8)))
                      (tagged-count (wal-u32 payload page-end))
                      (flags (ldb (byte 2 30) tagged-count))
                      (xids-end (+ xids-start (* (logand tagged-count #x3fffffff) 8))))
                 (when (= flags 1)
                   (error "invalid page provenance flags"))
                 (unless (<= xids-end (length payload))
                   (error "WAL page-image record has truncated contributors"))
                 (let ((page (subseq payload page-offset page-end)))
                   (unless (and (= (page-u64 page +pd-lsn-off+) lsn)
                                (wal-verify-page page relfilenode block fork))
                     (error "WAL page image has an invalid LSN or checksum"))
                   (push (make-wal-page-image
                          :relfilenode relfilenode :fork fork :block block :page page
                          :xids (loop for xid-offset from xids-start below xids-end by 8
                                      collect (wal-u64 payload xid-offset))
                          :provenance (ecase flags (0 :unknown) (2 :row) (3 :maintenance)))
                         images))
                 (setf offset xids-end))))
    (unless (= offset (length payload))
      (error "WAL page-image record has an invalid length"))
    (nreverse images)))

(defun wal-segment-and-offset (lsn)
  (floor lsn +wal-segment-size+))

(define-condition wal-input-validation-error (simple-error) ())

(defun wal-invalid-input (control &rest arguments)
  (error 'wal-input-validation-error
         :format-control control
         :format-arguments arguments))

(defun wal-require-ordinary-record-type (type)
  "EOR needs a verified timeline transition, not ordinary batch/progress handling."
  (when (= type +wal-record-end-of-recovery+)
    (wal-invalid-input "end-of-recovery requires a verified timeline transition")))

(defvar *wal-fail-stop-cause-cell* nil
  "Dynamically bound cell retaining the first fail-stop cause in an operation.")

(defun wal-fail-stop (condition)
  "Poison WAL, retain CONDITION on this backend, and request fast shutdown.
Re-signal CONDITION after requesting shutdown."
  (when (and *wal-fail-stop-cause-cell* (not (car *wal-fail-stop-cause-cell*)))
    (setf (car *wal-fail-stop-cause-cell*) condition))
  (setf *wal-poisoned* t)
  (when (fboundp 'note-backend-wal-failure)
    (funcall 'note-backend-wal-failure condition))
  (when (fboundp 'request-fast-shutdown)
    (ignore-errors (funcall (symbol-function 'request-fast-shutdown))))
  (error condition))

(defvar *physical-slot-cold-cleanup-owner* nil
  "First backend requiring cold slot reconciliation. Set under the WAL mutex
and observed by admission without taking the WAL mutex.")

(defun wal-check-healthy ()
  (when *wal-poisoned*
    (error "WAL I/O previously failed; refusing further WAL operations"))
  (values))

(defmacro with-wal-critical (&body body)
  "Hold the WAL mutex while BODY publishes shared state.
Waiting for the mutex is interruptible unless the caller already masks interrupts.
Once acquired, mask interrupts through body execution, error recording and mutex
release. Clean preflight validation errors propagate without poisoning. If WAL
is already poisoned, validation also triggers fail-stop after release; other
errors do likewise. Nonlocal exits need a publication fence. Legacy append and
flush I/O can themselves call fail-stop while the mutex is held."
  (let ((failure (gensym "FAILURE-"))
        (condition (gensym "CONDITION-"))
        (mutex (gensym "MUTEX-"))
        (held (gensym "HELD-")))
    `(let ((,failure nil) (,mutex *wal-lock*) (,held nil))
       (sb-sys:without-interrupts
         (unwind-protect
              (unwind-protect
                   (progn
                     (sb-sys:allow-with-interrupts
                       (setf ,held (sb-thread:grab-mutex ,mutex)))
                     (handler-case
                         (progn (wal-check-healthy) ,@body)
                       (wal-input-validation-error (,condition)
                         (when *wal-poisoned* (setf ,failure ,condition))
                         (error ,condition))
                       (error (,condition)
                         (setf ,failure ,condition *wal-poisoned* t)
                         (error ,condition))))
                (when ,held (sb-thread:release-mutex ,mutex)))
           (when ,failure (wal-fail-stop ,failure)))))))

(defmacro with-wal-publication-fence ((begin-publication) &body body)
  "Fence publication begun by BEGIN-PUBLICATION through completion of BODY.
After publication begins, an incomplete body poisons WAL before releasing its
lock. Fence cleanup fail-stops after release on an error or nonlocal exit.
Legacy append and flush I/O handlers may fail-stop earlier
while WAL is held. Retain the first error even if cleanup fails. Mask interrupts
during cleanup, and keep backend termination from replacing a latched WAL failure."
  (let ((started (gensym "STARTED-"))
        (complete (gensym "COMPLETE-"))
        (body-complete (gensym "BODY-COMPLETE-"))
        (failure (gensym "FAILURE-"))
        (condition (gensym "CONDITION-"))
        (record (gensym "RECORD-")))
    `(let ((,started nil) (,complete nil) (,body-complete nil) (,failure nil))
       (flet ((,begin-publication () (setf ,started t))
              (,record (,condition)
                (when (and ,started (not ,failure))
                  (setf ,failure ,condition))))
         (sb-sys:without-interrupts
           (unwind-protect
                (handler-bind ((error #',record))
                  (multiple-value-prog1
                      (sb-sys:allow-with-interrupts
                        (with-wal-critical
                          (unwind-protect
                               (handler-bind ((error #',record))
                                 (multiple-value-prog1 (progn ,@body)
                                   (setf ,body-complete t)))
                            (when (and ,started (not ,body-complete))
                              (setf *wal-poisoned* t)))))
                    (setf ,complete t)))
             (when (and ,started (not ,complete))
               (wal-fail-stop
                (or ,failure
                    (make-condition 'simple-error
                                    :format-control "WAL publication exited before completion"
                                    :format-arguments nil))))))))))

(defun wal-record-start-after (cursor length)
  "Plan native rotation without changing insertion state."
  (unless (and (typep cursor '(unsigned-byte 64))
               (typep length (list 'integer 1 +wal-segment-size+)))
    (error "invalid native WAL record position or length"))
  (multiple-value-bind (segment offset) (wal-segment-and-offset cursor)
    (if (> (+ offset length) +wal-segment-size+)
        (* (1+ segment) +wal-segment-size+) cursor)))

(defun wal-next-record-start (length)
  (wal-record-start-after *wal-end-lsn* length))

(declaim (ftype function wal-record-primary-archive-rotation))

(defun wal-note-primary-archive-rotation (next-segment)
  (when (and (plusp *wal-end-lsn*) (startup-primary-wal-archive-enabled-p))
    (let ((previous (floor (1- *wal-end-lsn*) +wal-segment-size+)))
      (when (< previous next-segment)
        (handler-case (wal-record-primary-archive-rotation previous *wal-end-lsn*)
          (error (condition)
            (warn "could not record archive proof for WAL segment ~a: ~a" previous condition)))))))

(defun wal-open-output (segment)
  (unless (eql segment *wal-output-segment*)
    (when *wal-output-stream*
      (finish-output *wal-output-stream*)
      ;; Rotation can happen in the middle of a commit batch. Make every
      ;; previous segment and its directory entry durable before opening a
      ;; later segment. The final segment is flushed once the batch's
      ;; commit/catalog record has been appended.
      (fsync-stream *wal-output-stream*)
      (when *wal-new-segments*
        (fsync-directory (wal-directory))
        (setf *wal-new-segments* '()))
      (close *wal-output-stream*)
      (setf *wal-output-stream* nil))
    (wal-note-primary-archive-rotation segment)
    (let* ((path (wal-segment-path segment))
           (newp (not (probe-file path))))
      (setf *wal-output-stream*
            (open path :direction :output
                       :element-type '(unsigned-byte 8)
                       :if-exists :append :if-does-not-exist :create)
            *wal-output-segment* segment)
      (when newp (pushnew segment *wal-new-segments* :test #'eql))))
  *wal-output-stream*)

(defun wal-append-record (type xid payload)
  (when (or *wal-pending-page-publication* *wal-pending-page-publication-tail*)
    (if (and (sb-thread:holding-mutex-p *wal-lock*)
             (wal-page-publication-p *wal-page-publication-appender*)
             (eq :stack (wal-page-publication-state *wal-page-publication-appender*))
             (eq *wal-storage-incarnation*
                 (wal-page-publication-incarnation *wal-page-publication-appender*))
             (eql *wal-insert-timeline*
                  (wal-page-publication-timeline *wal-page-publication-appender*)))
        (wal-check-page-publication-queue-under-lock)
        (wal-require-no-pending-page-publication)))
  (wal-require-ordinary-record-type type)
  (wal-check-healthy)
  (handler-case
      (let* ((length (+ +wal-header-size+ (length payload)))
             (start (wal-next-record-start length))
             (segment (floor start +wal-segment-size+))
             (offset (mod start +wal-segment-size+))
             (record (wal-encode-record type xid start payload))
             (stream (wal-open-output segment)))
        (unless (= (or (file-position stream) offset) offset)
          (error "WAL segment ~a is not positioned at offset ~a" segment offset))
        (write-sequence record stream)
        (setf *wal-end-lsn* (+ start length))
        *wal-end-lsn*)
    (error (condition)
      (wal-fail-stop condition))))

(defun wal-sync-output-bytes (stream sync-names)
  "Make WAL bytes/names durable without advancing any control-file claim."
  (when stream (finish-output stream) (fsync-stream stream))
  (when sync-names (fsync-directory (wal-directory))))

(declaim (ftype function notify-primary-wal-writer-async-under-lock))

(defun wal-writer-position-end-under-lock (position)
  "Return the current-epoch end LSN, or zero for an absent or stale position."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (if (and position
           (eq *wal-storage-incarnation* (wal-writer-position-incarnation position))
           (eql *wal-insert-timeline* (wal-writer-position-timeline position)))
      (progn
        (unless (<= (wal-writer-position-end position) *wal-end-lsn*)
          (wal-invalid-input "writer position is past inserted WAL"))
        (wal-writer-position-end position))
      0))

(defun wal-explicit-written-lsn ()
  "Return the explicit write frontier, at least as far as durable WAL."
  (max *wal-durable-lsn*
       (wal-writer-position-end-under-lock *wal-explicit-write-frontier*)))

(defun wal-async-flush-lsn ()
  (wal-writer-position-end-under-lock *wal-async-flush-frontier*))

(defun wal-request-async-flush-under-lock (end)
  "Advance the current epoch's async flush request and check whether to wake the writer."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (and (typep end '(unsigned-byte 64)) (<= end *wal-end-lsn*))
    (wal-invalid-input "async flush request is past inserted WAL"))
  (when (> end (wal-async-flush-lsn))
    (setf *wal-async-flush-frontier*
          (make-wal-writer-position *wal-storage-incarnation* *wal-insert-timeline* end))
    (notify-primary-wal-writer-async-under-lock end))
  (values))

(defun wal-note-explicit-write ()
  (setf *wal-explicit-write-frontier*
        (make-wal-writer-position
         *wal-storage-incarnation* *wal-insert-timeline* *wal-end-lsn*))
  (values))

(defun wal-write-core ()
  "Finish output through the current inserted end without claiming durability.
The whole stream is completed, which can exceed the selected background request."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless *wal-output-stream*
    (wal-invalid-input "background WAL write has no output stream"))
  (finish-output *wal-output-stream*)
  (wal-note-explicit-write))

(defun wal-flush-core ()
  "Sync WAL bytes and names, then durable control; caller supplies failure fences.
The sync covers the whole current inserted stream, possibly beyond the request."
  (wal-sync-output-bytes *wal-output-stream* *wal-new-segments*)
  (wal-note-explicit-write)
  (setf *wal-new-segments* '())
  (wal-test-pause :after-wal-bytes-flush)
  (wal-save-durable-lsn *wal-end-lsn*)
  (values))

(defun wal-flush ()
  (wal-check-healthy)
  (handler-case (wal-flush-core)
    (error (condition) (wal-fail-stop condition))))

(defun wal-append-page-images (images)
  (labels ((append-chunk (chunk)
             (let* ((length (+ +wal-header-size+ (wal-page-payload-length chunk)))
                    (lsn (+ (wal-next-record-start length) length))
                    (payload (wal-encode-page-images chunk lsn)))
               (unless (= lsn (wal-append-record +wal-record-pages+ 0 payload))
                 (error "WAL page-image LSN changed while appending")))))
    (let ((chunk nil)
          (chunk-count 0)
          (chunk-bytes (+ +wal-header-size+ 4)))
      (dolist (image images)
        (let ((image-size (wal-page-image-encoded-size image)))
          (when (> (+ +wal-header-size+ 4 image-size) +wal-segment-size+)
            (error "WAL page image has too many transaction contributors"))
          (when (and chunk
                     (or (>= chunk-count +wal-max-page-images-per-record+)
                         (> (+ chunk-bytes image-size) +wal-segment-size+)))
            (append-chunk (nreverse chunk))
            (setf chunk nil chunk-count 0
                  chunk-bytes (+ +wal-header-size+ 4)))
          (push image chunk)
          (incf chunk-count)
          (incf chunk-bytes image-size)))
      (when chunk
        (append-chunk (nreverse chunk)))))
  images)

(defun wal-append-batch-start ()
  (let ((start (wal-next-record-start +wal-header-size+)))
    (wal-append-record +wal-record-batch-start+ 0
                       (make-array 0 :element-type '(unsigned-byte 8)))
    start))

(defun wal-encode-relation-reset (relfilenode fork)
  (unless (typep relfilenode '(integer 1 4294967295))
    (error "invalid relation reset identity"))
  (let ((payload (make-array 8 :element-type '(unsigned-byte 8) :initial-element 0)))
    (wal-set-u32 payload 0 relfilenode)
    (wal-set-u32 payload 4 (ecase fork (:main 0) (:visibility 1)))
    payload))

(defun wal-decode-relation-reset (record)
  (let ((payload (wal-record-payload record)))
    (unless (and (= (wal-record-version record) +wal-version+)
                 (= (length payload) 8)
                 (plusp (wal-u32 payload 0))
                 (<= (wal-u32 payload 4) 1)
                 (typep (wal-record-xid record) '(integer 3 18446744073709551615))
                 (>= (logand (wal-record-xid record) #xffffffff) 3))
      (error "invalid relation reset WAL record"))
    (list (wal-u32 payload 0) (if (zerop (wal-u32 payload 4)) :main :visibility))))

(defun wal-recovery-stage-relation-reset (record)
  ;; Keep resets ahead of page images and retain their XIDs for terminal checks.
  ;; An incomplete batch is discarded without applying any of its resets.
  (unless (and *wal-recovery-group-active*
               (not *wal-recovery-page-records-seen-p*)
               (null *wal-recovery-catalog-chunk-count*)
               (null *wal-recovery-logical-chunk-count*)
               (not *wal-recovery-logical-catalog-event-p*))
    (error "relation reset must precede page images in a WAL batch"))
  (let ((reset (wal-decode-relation-reset record)))
    (when (find reset *wal-recovery-relation-resets* :key #'cdr :test #'equal)
      (error "duplicate relation reset in a WAL batch"))
    (push (cons (wal-record-xid record) reset) *wal-recovery-relation-resets*))
  (values))

(defun wal-apply-relation-resets (resets)
  (dolist (reset resets)
    (smgr-reset-fork (first reset) (second reset)))
  (values))

(defun wal-discard-relation-pages-under-lock (relfilenodes)
  (dolist (pair (list (cons *wal-page-cache* *wal-page-cache-xids*)
                      (cons *wal-page-batch* *wal-page-batch-xids*)))
    (let ((pages (car pair)) (contributors (cdr pair)) (keys nil))
      (when (hash-table-p pages)
        (maphash (lambda (key reference)
                   (declare (ignore reference))
                   (when (member (first key) relfilenodes :test #'eql) (push key keys)))
                 pages)
        (dolist (key keys) (wal-dirty-remove-page key pages contributors)))))
  (values))

(defun wal-forget-xid-under-xid-lock (xid)
  "Remove XID's retained WAL position while the caller holds *XID-LOCK*."
  (remhash xid *wal-xid-first-lsn*)
  (values))

(defun wal-slot-record-xid-batch-under-wal-lock (xid first-lsn)
  (maphash (lambda (name slot)
             (declare (ignore name))
             (when (and (wal-logical-slot-p slot) (not (eq (wal-replication-slot-wal-status slot) :lost)))
               (let ((pending (wal-replication-slot-pending-xids slot)))
                 (unless (gethash xid pending)
                   (setf (gethash xid pending) (cons first-lsn nil))))))
           *wal-replication-slots*)
  (values))

(defun wal-slot-seed-active-xids-under-wal-lock (slot active-xids)
  (unless (wal-logical-slot-p slot) (return-from wal-slot-seed-active-xids-under-wal-lock (values)))
  (sb-thread:with-mutex (*xid-lock*)
    (dolist (xid active-xids)
      (let ((first-lsn (gethash xid *wal-xid-first-lsn*)))
        (when first-lsn
          (setf (gethash xid (wal-replication-slot-pending-xids slot))
                (cons first-lsn nil))))))
  (values))

(defun wal-slot-record-xid-commit-under-wal-lock (xid commit-lsn)
  (maphash (lambda (name slot)
             (declare (ignore name))
             (when (wal-logical-slot-p slot)
               (let ((entry (gethash xid (wal-replication-slot-pending-xids slot))))
                 (when entry (setf (cdr entry) commit-lsn)))))
           *wal-replication-slots*)
  (values))

(defun wal-record-page-xids-at-batch-start (images batch-start-lsn)
  "Retain the first batch position for each active page contributor in IMAGES.

Call with *WAL-LOCK* held. The WAL->XID order matches commit publication and
the snapshot boundary capture path. PostgreSQL's logical decoding similarly
keeps transaction state until commit records establish which changes belong to
the transaction (see SnapBuildCommitTxn and CreateInitDecodingContext)."
  (let ((xids (remove-duplicates
               (loop for image in images
                     append (wal-page-image-xids image))
               :test #'eql)))
    (when xids
      (sb-thread:with-mutex (*xid-lock*)
        (dolist (xid xids)
          (when (member xid *active-xids* :test #'eql)
            (multiple-value-bind (first-lsn present-p)
                (gethash xid *wal-xid-first-lsn*)
              (unless present-p
                (setf first-lsn batch-start-lsn
                      (gethash xid *wal-xid-first-lsn*) first-lsn))
              (wal-slot-record-xid-batch-under-wal-lock xid first-lsn))))))
    (values)))

(defun wal-primary-record-page-xids (keys batch-start-lsn)
  "Record each contributor once, without constructing a page-image list."
  (let ((xids (make-hash-table :test #'eql)))
    (dolist (key keys)
      (dolist (xid (gethash key *wal-page-cache-xids*))
        (when (>= xid 3) (setf (gethash xid xids) t))))
    (when (plusp (hash-table-count xids))
      (sb-thread:with-mutex (*xid-lock*)
        (maphash
         (lambda (xid present)
           (declare (ignore present))
           (when (member xid *active-xids* :test #'eql)
             (multiple-value-bind (first-lsn present-p) (gethash xid *wal-xid-first-lsn*)
               (unless present-p
                 (setf first-lsn batch-start-lsn
                       (gethash xid *wal-xid-first-lsn*) first-lsn))
               (wal-slot-record-xid-batch-under-wal-lock xid first-lsn))))
         xids))))
  (values))

(defun wal-apply-page-images (images &key split-p fault-on-first-write)
  (let ((paused nil))
    (loop for image in images
          do (smgr-write (wal-page-image-relfilenode image)
                         (wal-page-image-block image)
                         (wal-page-image-page image)
                         (wal-page-image-fork image))
             (when (and (not paused)
                        fault-on-first-write split-p
                        (or (and *wal-test-pause-relfilenode*
                                 (eql *wal-test-pause-relfilenode*
                                      (wal-page-image-relfilenode image))
                                 (plusp (wal-page-image-block image)))
                            (null *wal-test-pause-relfilenode*)))
               (setf paused t)
               (wal-test-pause :after-btree-split-page-write))))
  (values))

(defun wal-close-primary-page-spool (spool)
  "Close SPOOL and require its temporary-file cleanup to finish."
  (wal-close-owned-spool spool))

(defun call-with-wal-primary-page-spool (function)
  "Call FUNCTION with a stack-owned spool and its publication owner.
The spool is adopted before it is published to the owner. Cleanup closes an
untransferred spool unless a transaction's terminal attempt has begun. Its CLOG
reservation then retains the owner even if queue handoff fails."
  (call-with-wal-temp-cleanup-domain
   (lambda ()
     (when (or *wal-pending-page-publication* *wal-pending-page-publication-tail*)
       (wal-check-page-publication-queue-under-lock)
       (when (wal-page-publication-resets *wal-pending-page-publication-tail*)
         (wal-invalid-input "relation reset publication must finish before another publisher")))
     (let* ((owner (%make-wal-page-publication :incarnation *wal-storage-incarnation*
                                               :timeline *wal-insert-timeline*))
            (*wal-page-publication-appender* owner))
       (unwind-protect
            (progn
              (sb-sys:without-interrupts
                (setf (wal-page-publication-spool owner) (make-wal-owned-spool)))
              (funcall function (wal-page-publication-spool owner) owner))
         (when (and (eq :stack (wal-page-publication-state owner))
                    (or (null (wal-page-publication-completion owner))
                        (clog-release-unattempted-publication-under-wal-lock
                         (wal-page-publication-completion owner) owner)))
           (when (wal-page-publication-spool owner)
             (wal-close-primary-page-spool (wal-page-publication-spool owner)))
           (setf (wal-page-publication-state owner) :released)))))))

(defmacro with-wal-primary-page-spool ((spool &optional publication) &body body)
  "Close an abandoned SPOOL before a terminal attempt; retain attempted transaction owners."
  (let ((owner (or publication (gensym "PAGE-PUBLICATION-"))))
    `(call-with-wal-primary-page-spool
      (lambda (,spool ,owner)
        (declare (ignorable ,owner))
        ,@body))))

(defun wal-primary-append-pages (keys spool)
  "Append and stage bounded WAL page records in sorted key order."
  (map-wal-primary-page-key-chunks
   keys
   (lambda (chunk image-bytes)
     (let* ((images (mapcar #'wal-primary-page-image chunk))
            (length (+ +wal-header-size+ 4 image-bytes))
            (lsn (+ (wal-next-record-start length) length))
            (payload (wal-encode-page-images images lsn))
            (event (make-array (+ 12 (length payload)) :element-type '(unsigned-byte 8))))
       (unless (= (length payload) (+ 4 image-bytes))
         (error "WAL page-image size changed after planning"))
       (unless (= lsn (wal-append-record +wal-record-pages+ 0 payload))
         (error "WAL page-image LSN changed while appending"))
       (wal-set-u32 event 0 +wal-version+)
       (wal-set-u64 event 4 lsn)
       (replace event payload :start1 12)
       (logical-spool-append spool event))))
  (values))

(defun wal-primary-map-page-spool (spool function)
  "Validate and visit staged page records in their original order."
  (call-with-wal-spool-owner
   spool
   (lambda ()
     (logical-spool-map-events
      spool
      (lambda (event)
        (unless (>= (length event) 12) (error "invalid primary WAL page spool record"))
        (let ((version (wal-u32 event 0)) (lsn (wal-u64 event 4)))
          (unless (= version +wal-version+) (error "invalid primary WAL page spool version"))
          (funcall function (wal-decode-page-images (subseq event 12) lsn)))))))
  (values))

(defun wal-primary-seal-page-spool (spool)
  "Preflight every staged record before publishing a terminal WAL record."
  (call-with-wal-spool-owner spool (lambda () (logical-spool-seal spool)))
  (wal-primary-map-page-spool spool (lambda (images) (declare (ignore images))))
  (values))

(defun wal-primary-apply-page-spool (spool &key split-p fault-on-first-write)
  "Apply all records, keeping the split fault point global to the batch."
  (let ((paused nil))
    (wal-primary-map-page-spool
     spool
     (lambda (images)
       (wal-apply-page-images images :split-p split-p
                                     :fault-on-first-write (and fault-on-first-write (not paused)))
       (when (and fault-on-first-write split-p (not paused)
                  (some (lambda (image)
                          (or (null *wal-test-pause-relfilenode*)
                              (and (eql *wal-test-pause-relfilenode*
                                        (wal-page-image-relfilenode image))
                                   (plusp (wal-page-image-block image)))))
                        images))
         (setf paused t)))))
  (values))

(defun wal-clear-page-cache (&optional excluded-relfilenodes)
  (let ((keys '()))
    (maphash (lambda (key page)
               (declare (ignore page))
               (when (or (null excluded-relfilenodes)
                         (member (first key) excluded-relfilenodes :test #'eql))
                 (push key keys)))
             *wal-page-cache*)
    (dolist (key keys)
      (wal-dirty-remove-page key *wal-page-cache* *wal-page-cache-xids*)))
  (wal-dirty-close-empty-store)
  (values))

(defun wal-discard-relation-pages (relfilenodes)
  "Forget staged images for unpublished detached relations in RELFILENODES."
  (with-wal-page-state-lock
    (wal-discard-relation-pages-under-lock relfilenodes)
    (wal-dirty-close-empty-store))
  (values))

(defun postgres-current-timestamp ()
  "Microseconds since PostgreSQL's 2000-01-01 epoch, as GetCurrentTimestamp."
  (multiple-value-bind (seconds microseconds) (sb-ext:get-time-of-day)
    (+ (* (- seconds 946684800) 1000000) microseconds)))

;; TODO: Stream catalog serialization into a temporary file and write bounded
;; WAL chunks from it. Recovery already spills incomplete batches and borrows
;; the catalog bytes from the validated contiguous terminal payload.

(defun wal-segment-entry-names (&optional (directory (wal-directory)))
  "Return raw WAL entry names, including dangling links.

Follow a symlink at DIRECTORY itself. Enumeration errors are reported."
  (remove-if-not (lambda (name)
                   (and (> (length name) 4) (uiop:string-suffix-p name ".wal")))
                 (directory-entry-names directory :follow-symlinks t)))

(defun wal-discover-segments (&optional (timeline *wal-insert-timeline*)
                                (namespace *wal-segment-namespace*))
  "Select only the explicit timeline and grammar; never pick the newest branch."
  (sort (loop for name in (wal-segment-entry-names)
              append (multiple-value-bind (candidate segment)
                         (wal-parse-segment-name (subseq name 0 (- (length name) 4)) namespace)
                       (when (eql candidate timeline) (list segment))))
        #'<))

(defun wal-truncate-tail (path offset)
  (with-open-file (stream path :direction :io
                               :element-type '(unsigned-byte 8)
                               :if-exists :overwrite)
    (sb-posix:ftruncate (sb-sys:fd-stream-fd stream) offset)
    (fsync-stream stream)))

(defun wal-truncate-unflushed-tail (segment last-segment offset lsn reason)
  (unless (and (eql segment last-segment)
               (>= lsn *wal-durable-lsn*))
    (error "~a at LSN ~a before durable LSN ~a"
           reason lsn *wal-durable-lsn*))
  (wal-truncate-tail (wal-segment-path segment) offset)
  (values))

(defun wal-read-scan-record (stream segment last-segment offset size)
  "Read and decode the record at OFFSET, truncating only an unflushed tail."
  (let* ((record-lsn (+ (* segment +wal-segment-size+) offset))
         (remaining (- size offset)))
    (when (< remaining +wal-header-size+)
      (wal-truncate-unflushed-tail segment last-segment offset record-lsn
                                   "incomplete WAL header")
      (return-from wal-read-scan-record (values nil nil)))
    (file-position stream offset)
    (let ((header (make-array +wal-header-size+
                              :element-type '(unsigned-byte 8))))
      (unless (= (read-sequence header stream) +wal-header-size+)
        (error "could not read WAL record header"))
      ;; Recognized timeline-transition bytes must not enter ordinary tail repair,
      ;; even if an interrupted write left their body incomplete or malformed.
      (when (and (equalp (subseq header 0 4) #(80 71 87 76))
                 (>= (wal-u16 header 4) 7))
        (wal-require-ordinary-record-type (wal-u16 header 6)))
      (let ((record-length (wal-u32 header 8)))
        (unless (and (>= record-length +wal-header-size+)
                     (<= record-length +wal-segment-size+)
                     (<= (+ offset record-length) +wal-segment-size+))
          (wal-truncate-unflushed-tail segment last-segment offset record-lsn
                                       "invalid WAL record length")
          (return-from wal-read-scan-record (values nil nil)))
        (when (> record-length remaining)
          (wal-truncate-unflushed-tail segment last-segment offset record-lsn
                                       "incomplete durable WAL record")
          (return-from wal-read-scan-record (values nil nil)))
        (let ((bytes (make-array record-length
                                 :element-type '(unsigned-byte 8))))
          (file-position stream offset)
          (unless (= (read-sequence bytes stream) record-length)
            (error "could not read WAL record at LSN ~a" record-lsn))
          (values
           (handler-case
               (wal-decode-record bytes record-lsn)
             (error (condition)
               (if (and (eql segment last-segment)
                        (>= record-lsn *wal-durable-lsn*))
                   (progn
                     (wal-truncate-tail (wal-segment-path segment) offset)
                     nil)
                   (error condition))))
           record-length))))))

(defun wal-scan-checkpoint-record (record record-lsn scan-from-lsn checkpoint-record-start)
  "Validate the published checkpoint against its independent durable control copy."
  (when (and (not scan-from-lsn) (plusp *wal-checkpoint-lsn*)
             (or (= record-lsn checkpoint-record-start)
                 (= (wal-record-end record) *wal-checkpoint-lsn*)))
    (unless (and (= record-lsn checkpoint-record-start)
                 (= (wal-record-end record) *wal-checkpoint-lsn*)
                 (equal (wal-checkpoint-record-snapshot record) *wal-checkpoint-snapshot*))
      (error "WAL checkpoint differs from its durable control copy"))
    t))

(defun wal-scan-segment (function segment last-segment scan-start-offset
                         scan-end-lsn scanned-end-lsn scan-from-lsn
                         checkpoint-record-start checkpoint-found)
  "Visit one segment, preserving WAL tail-truncation and checkpoint checks."
  (let ((stop-p nil)
        (checkpoint-found checkpoint-found))
    (with-open-file (stream (wal-segment-path segment)
                            :direction :input
                            :element-type '(unsigned-byte 8))
      (let ((size (file-length stream))
            (offset scan-start-offset))
        (loop while (and (< offset size) (not stop-p)
                         (or (null scan-end-lsn)
                             (< (+ (* segment +wal-segment-size+) offset)
                                scan-end-lsn)))
              do (multiple-value-bind (record record-length)
                     (wal-read-scan-record stream segment last-segment offset size)
                   (unless record (return))
                   (wal-require-ordinary-record-type (wal-record-type record))
                   ;; Records stay within a segment; rotate only when this
                   ;; record cannot fit after the preceding scanned record.
                   ;; Check before accepting a checkpoint or invoking the callback.
                   (unless (= (wal-record-start record)
                              (wal-record-start-after scanned-end-lsn record-length))
                     (error "invalid WAL segment rotation gap after ~d" scanned-end-lsn))
                   (when (and scan-end-lsn
                              (> (wal-record-end record) scan-end-lsn))
                     (error "WAL scan end ~a is not a record boundary"
                            scan-end-lsn))
                   (when (wal-scan-checkpoint-record
                          record (wal-record-start record) scan-from-lsn
                          checkpoint-record-start)
                     (setf checkpoint-found t))
                   (setf scanned-end-lsn (wal-record-end record))
                   (if (or (eq (funcall function record) :stop)
                           (and scan-end-lsn
                                (= scanned-end-lsn scan-end-lsn)))
                       (setf stop-p t)
                       (incf offset record-length))))))
    (values scanned-end-lsn stop-p checkpoint-found)))

(defun wal-scan (function &optional scan-from-lsn &rest options)
  "Visit durable records from the checkpoint or an exact retained LSN.
FUNCTION may return :STOP to end the scan after the current record.

Stopping at each decoded record mirrors the XLogReadRecord() consumer loop in
src/backend/access/transam/xlogreader.c."
  (let* ((segments (wal-discover-segments))
         (last-segment (car (last segments)))
         (checkpoint-record-start
           (wal-checkpoint-record-start))
         (scan-start-lsn (or scan-from-lsn checkpoint-record-start))
         (scan-start-segment (floor scan-start-lsn +wal-segment-size+))
         (scan-start-offset (mod scan-start-lsn +wal-segment-size+))
         (scan-segments
           (if (zerop scan-start-lsn)
               segments
               (member scan-start-segment segments)))
         (scan-end-lsn (getf options :end-lsn))
         (scanned-end-lsn scan-start-lsn)
         (checkpoint-found (or scan-from-lsn (zerop *wal-checkpoint-lsn*)))
         (stop-p nil))
    (when (and scan-from-lsn (> scan-from-lsn *wal-durable-lsn*))
      (error "WAL scan start ~a is past durable WAL ~a"
             scan-from-lsn *wal-durable-lsn*))
    (when (and scan-end-lsn
               (not (and (integerp scan-end-lsn)
                         (<= scan-start-lsn scan-end-lsn
                             *wal-durable-lsn*))))
      (error "WAL scan end ~a is outside the durable scan range"
             scan-end-lsn))
    (when (and (not scan-from-lsn) (plusp *wal-checkpoint-lsn*)
               (< *wal-checkpoint-lsn* +wal-header-size+))
      (error "WAL checkpoint LSN ~a is invalid" *wal-checkpoint-lsn*))
    (when (and (not scan-from-lsn) (zerop *wal-checkpoint-lsn*) segments
               (not (zerop (first segments))))
      (error "WAL segment 0 is missing before the first checkpoint"))
    (when (and (< scan-start-lsn *wal-durable-lsn*)
               (null scan-segments))
      (error "WAL scan start segment ~a is missing" scan-start-segment))
    (loop for (left right) on scan-segments
          while right
          unless (= right (1+ left))
            do (error "missing WAL segment between ~a and ~a" left right))
    (loop for segment in scan-segments
          while (not stop-p)
          do (multiple-value-setq (scanned-end-lsn stop-p checkpoint-found)
               (wal-scan-segment
                function segment last-segment
                (if (= segment scan-start-segment) scan-start-offset 0)
                scan-end-lsn scanned-end-lsn scan-from-lsn
                checkpoint-record-start checkpoint-found)))
    (when (and scan-end-lsn (not stop-p)
               (/= scanned-end-lsn scan-end-lsn))
      (error "WAL scan stopped at ~a before requested end ~a"
             scanned-end-lsn scan-end-lsn))
    (unless (or stop-p checkpoint-found)
      (error "WAL checkpoint LSN ~a is not present in retained WAL"
             *wal-checkpoint-lsn*))
    (when (and (not stop-p) (not scan-from-lsn)
               (> *wal-checkpoint-lsn* scanned-end-lsn))
      (error "WAL checkpoint LSN ~a is past the end of WAL ~a"
             *wal-checkpoint-lsn* scanned-end-lsn))
    (when (and (not stop-p) (not scan-end-lsn)
               (> *wal-durable-lsn* scanned-end-lsn))
      (error "WAL durable LSN ~a is past the end of WAL ~a"
             *wal-durable-lsn* scanned-end-lsn))
    (values scanned-end-lsn scan-segments)))

(defun wal-read-u64-payload (payload)
  (unless (= (length payload) 8)
    (error "WAL XID reset record has an invalid length"))
  (wal-u64 payload 0))

(defun wal-empty-metadata-terminal-type-p (type)
  "Recognize terminals that must be the only record after a batch marker."
  (member type (list +wal-record-running-xacts+ +wal-record-xid-reservation+
                     +wal-record-parameter-change+ +wal-record-full-page-writes-change+
                     +wal-record-logical-decoding-status-change+) :test #'=))

(defun wal-note-recovery-batch-record (record)
  "Track whether a metadata terminal can follow the batch marker alone."
  (wal-require-ordinary-record-type (wal-record-type record))
  (let ((type (wal-record-type record)))
    (cond
      ((= type +wal-record-batch-start+)
       (unless (and (zerop (wal-record-xid record)) (zerop (length (wal-record-payload record))))
         (error "invalid WAL batch-start marker"))
       (setf *wal-recovery-reservation-batch-clean-p* t))
      ((not (wal-empty-metadata-terminal-type-p type))
       (setf *wal-recovery-reservation-batch-clean-p* nil)))))

(defun wal-validate-empty-metadata-terminal (record clean-p)
  "Decode a metadata terminal without publishing it.
CLEAN-P must certify that only a valid batch marker precedes the terminal."
  (unless clean-p
    (error "metadata terminal must be the sole record after its batch marker"))
  (let ((type (wal-record-type record)))
    (cond
      ((= type +wal-record-running-xacts+) (wal-running-xacts-record-info record))
      ((= type +wal-record-parameter-change+) (wal-parameter-change-record-info record))
      ((= type +wal-record-full-page-writes-change+) (wal-full-page-writes-change-record-info record))
      ((= type +wal-record-logical-decoding-status-change+) (wal-logical-decoding-status-record-info record))
      ((= type +wal-record-xid-reservation+) (wal-xid-reservation-record-bound record))
      (t (error "unsupported empty metadata terminal")))))

(defun wal-atomic-write-bytes (path bytes &key mode)
  "Durably replace PATH with BYTES. Reserve the tmp type for staging."
  (when (equal (pathname-type path) "tmp")
    (error "WAL atomic write destination uses reserved temporary file type: ~a" path))
  (with-atomic-output (stream
                       path
                       (make-pathname :type "tmp" :defaults path)
                       (make-pathname :name nil :type nil :defaults path)
                       :element-type '(unsigned-byte 8) :mode mode)
    (write-sequence bytes stream)))

(defun wal-read-control-fields ()
  "Read one bounded regular control image, returning NIL only when absent.
Reject extra lines before decoding its CRC and checkpoint metadata."
  (let ((path (wal-control-path)))
    (handler-case
        (with-input-from-string
            (stream (bytes->string (read-bounded-regular-file path (1+ +wal-control-maximum-bytes+) :minimum 1)))
          (let ((line (read-line stream nil nil)))
            (unless (and line (<= (length line) +wal-control-maximum-bytes+) (null (read-line stream nil nil)))
              (error "WAL control file ~a is corrupt" path))
            ;; Keep asynchronous interrupts out of the bounded decode and its
            ;; error converter; they can run again once the handler is gone.
            (sb-sys:without-interrupts
              (handler-case-noninterrupt (wal-decode-control-line line)
                                         (condition
                                          (error "WAL control file ~a is corrupt: ~a" path condition))))))
      (sb-posix:syscall-error (condition)
        (unless (= sb-posix:enoent (sb-posix:syscall-errno condition))
          (error condition))))))

(defun wal-check-serving-namespace-before-pid ()
  "Refuse legacy primary WAL under the startup flock before publishing PID state."
  (when (or (some (lambda (name)
                    (and (= 20 (length name))
                         (wal-parse-segment-name (subseq name 0 16) :legacy)))
                  (wal-segment-entry-names))
            (eq :legacy (nth-value 5 (wal-read-control-fields))))
    (error "startup requires timeline-qualified WAL; migrate the data with the previous build")))

(defun wal-save-control-fields (checkpoint-lsn durable-lsn)
  "Save WAL positions and retain any known primary identity and lifecycle state.
Refresh its modification time. Update memory only after atomic publication."
  (let ((metadata (when *wal-control-metadata*
                    (wal-refresh-control-metadata
                     *wal-control-metadata* (getf *wal-control-metadata* :db-state)))))
    (wal-atomic-write-bytes
     (wal-control-path)
     (wal-encode-control-image checkpoint-lsn durable-lsn *wal-checkpoint-snapshot*
                               :timeline *wal-insert-timeline*
                               :checkpoint-timeline *wal-checkpoint-timeline*
                               :namespace *wal-segment-namespace* :metadata metadata))
    (setf *wal-control-metadata* metadata))
  (values))

(defun wal-save-durable-lsn (lsn)
  (when (< lsn *wal-durable-lsn*)
    (error "WAL durable LSN cannot move backwards from ~a to ~a"
           *wal-durable-lsn* lsn))
  (wal-save-control-fields *wal-checkpoint-lsn* lsn)
  (setf *wal-durable-lsn* lsn)
  (values))

(defun wal-save-checkpoint-control (lsn &optional snapshot clog-extent)
  (when (> lsn *wal-durable-lsn*)
    (error "WAL checkpoint LSN ~a is past durable LSN ~a" lsn *wal-durable-lsn*))
  ;; WAL-FLUSH retains the old copy. Publish a candidate only after page/CLOG sync.
  (let ((metadata
          (let ((*wal-checkpoint-snapshot* snapshot)
                (*wal-checkpoint-timeline* (if snapshot (getf snapshot :timeline) *wal-insert-timeline*))
                (*wal-control-metadata* (copy-list *wal-control-metadata*)))
            (when *wal-control-metadata*
              (if clog-extent
                  (setf (getf *wal-control-metadata* :checkpoint-clog-bytes) clog-extent)
                  (remf *wal-control-metadata* :checkpoint-clog-bytes)))
            (wal-save-control-fields lsn *wal-durable-lsn*)
            *wal-control-metadata*)))
    (setf *wal-control-metadata* metadata))
  (setf *wal-checkpoint-lsn* lsn
        *wal-checkpoint-snapshot* (copy-list snapshot)
        *wal-checkpoint-timeline* (if snapshot (getf snapshot :timeline) *wal-insert-timeline*))
  (values))

(defun wal-sync-segments (segments)
  (dolist (segment segments)
    (with-open-file (stream (wal-segment-path segment)
                            :direction :io
                            :element-type '(unsigned-byte 8)
                            :if-exists :append)
      (fsync-stream stream)))
  (values))

(declaim (ftype function wal-reconcile-namespace-at-startup))

(defun wal-init ()
  "Initialize WAL under startup or equivalent quiescence.
Reject surviving page and CLOG debt and a nonempty cleanup domain before
replacement. Establish a fresh local identity; cleanup owners are released only
after cleanup is confirmed. Policy evidence is unavailable pending recovery when
the checkpoint or scanned WAL contains a policy witness."
  (require-primary-wal-writer-retired)
  (wal-require-no-pending-storage)
  (sb-thread:with-mutex (*wal-lock*)
    (when (and *wal-dirty-store*
               (zerop (wal-dirty-store-resident *wal-dirty-store*))
               (zerop (wal-dirty-store-disk-refs *wal-dirty-store*)))
      (wal-dirty-close-empty-store))
    (wal-require-no-temp-cleanup-under-lock))
  ;; Control identities must be accepted before even syncing directories: a child
  ;; control cannot cause legacy segment scanning, truncation, or tail adoption.
  (multiple-value-bind (checkpoint-lsn durable-lsn snapshot insertion checkpoint-timeline namespace metadata)
      (wal-read-control-fields)
    (when checkpoint-lsn (wal-require-supported-timelines insertion checkpoint-timeline))
    (wal-replace-storage-incarnation)
    (setf *wal-control-metadata* metadata
          *wal-segment-namespace* (or namespace :timeline)
          *wal-insert-timeline* (or insertion 1)
          *wal-checkpoint-timeline* (or checkpoint-timeline 1)
          *wal-prev-timeline* (getf snapshot :prev-timeline (or insertion 1)))
    (wal-ensure-directory-durable *pgdata*)
    (wal-ensure-directory-durable (merge-pathnames "global/" *pgdata*))
    (wal-ensure-directory-durable (merge-pathnames "base/" *pgdata*))
    (wal-ensure-directory-durable (merge-pathnames "pg_wal/" *pgdata*))
    (wal-ensure-directory-durable (wal-replication-slot-directory))
    ;; Persist segment entries left visible before an earlier directory sync.
    (fsync-directory (wal-directory))
    (progn
      (when (and (null checkpoint-lsn) (wal-segment-entry-names))
        (error "WAL control file is missing while WAL segments exist"))
      (when checkpoint-lsn (wal-reconcile-namespace-at-startup))
      (when *wal-dirty-store*
        (wal-clear-page-cache)
        (wal-dirty-close-empty-store)
        (when *wal-dirty-store* (error "dirty page references remain at WAL initialization")))
      (setf *wal-insert-timeline* (or insertion 1)
            *wal-checkpoint-timeline* (or checkpoint-timeline 1)
            *wal-xid-reserved-through* (getf snapshot :xid-reserved-through)
            *wal-checkpoint-snapshot* snapshot
            *wal-checkpoint-lsn* (or checkpoint-lsn 0)
            *wal-durable-lsn* (or durable-lsn 0)
            *wal-end-lsn* 0
            *wal-ready* t
            *wal-poisoned* nil
            *wal-page-cache* (make-hash-table :test #'equal)
            *wal-page-cache-xids* (make-hash-table :test #'equal)
            *wal-xid-first-lsn* (make-hash-table :test #'eql)
            *wal-snapshot-retention-pins* (make-hash-table :test #'eq)
            *wal-replication-slots* (make-hash-table :test #'equal)
            *wal-pending-deletes* '()
            *wal-pending-btree-split* nil
            *wal-output-stream* nil
            *wal-output-segment* nil
            *wal-explicit-write-frontier* nil
            *wal-async-flush-frontier* nil
            *wal-new-segments* '()
            *wal-test-pause-at* nil
            *wal-test-pause-relfilenode* nil
            *wal-test-pause-fired* nil)
      (unless checkpoint-lsn (wal-save-control-fields 0 0))
      (let ((fpw-marker-p nil) (logical-marker-p nil))
        (multiple-value-bind (end-lsn found-segments)
            (wal-scan (lambda (record)
                        (when (= (wal-record-type record) +wal-record-full-page-writes-change+)
                          (setf fpw-marker-p t))
                        (when (= (wal-record-type record) +wal-record-logical-decoding-status-change+)
                          (setf logical-marker-p t))))
          (setf *wal-end-lsn* end-lsn)
          (when (> end-lsn *wal-durable-lsn*)
            ;; Adopt only a complete, synced tail belonging to this insertion identity.
            (wal-sync-segments found-segments)
            (wal-save-durable-lsn end-lsn)))
        (with-wal-critical
          (wal-fpw-prepare-recovery snapshot fpw-marker-p)
          (wal-prepare-logical-policy-recovery snapshot logical-marker-p)))
      (setf *wal-test-pause-at*
            (let ((value (sb-ext:posix-getenv "PG_WAL_TEST_PAUSE_AT")))
              (and value (string-downcase value)))
            *wal-test-pause-relfilenode*
            (let ((value (sb-ext:posix-getenv "PG_WAL_TEST_PAUSE_RELATION")))
              (and value (parse-integer value :junk-allowed t)))))
    (values)))

(defun wal-test-pause (point)
  (when (and *wal-test-pause-at*
             (string= *wal-test-pause-at* (string-downcase (symbol-name point))))
    (let ((marker (merge-pathnames
                   (format nil "test-paused-~a"
                           (string-downcase (symbol-name point)))
                   (wal-directory)))
          (resume (merge-pathnames
                   (format nil "test-resume-~a"
                           (string-downcase (symbol-name point)))
                   (wal-directory))))
      (with-open-file (stream marker :direction :output :if-exists :supersede
                                     :if-does-not-exist :create)
        (format stream "paused~%")
        (finish-output stream)
        (fsync-stream stream))
      (loop until (probe-file resume) do (sleep 0.1))
      (ignore-errors (delete-file resume))
      (ignore-errors (delete-file marker))))
  (values))

(defun wal-test-pause-once (point)
  "Pause once at POINT when the test environment selected that hook."
  (let ((pause-p
          (sb-thread:with-mutex (*wal-test-pause-lock*)
            (when (and *wal-test-pause-at*
                       (not *wal-test-pause-fired*)
                       (string= *wal-test-pause-at*
                                (string-downcase (symbol-name point))))
              (setf *wal-test-pause-fired* t)
              t))))
    (when pause-p
      (wal-test-pause point)))
  (values))

(defun wal-relation-filennodes ()
  (let ((relfilenodes '()))
    (maphash (lambda (database-name database)
               (declare (ignore database-name))
               (maphash (lambda (table-name table)
                          (declare (ignore table-name))
                          (push (pg-class-relfilenode table) relfilenodes))
                        (database-tables database))
               (maphash (lambda (index-name index)
                          (declare (ignore index-name))
                          (push (pg-index-relfilenode index) relfilenodes))
                        (database-indexes database)))
             (catalog-databases (symbol-value '*catalog*)))
    relfilenodes))

(defun wal-remove-orphan-relations ()
  (let ((known (wal-relation-filennodes))
        (base (merge-pathnames "base/" *pgdata*))
        (removed nil))
    (dolist (path (directory (merge-pathnames "*" base)))
      (let ((relfilenode (ignore-errors
                          (and (member (pathname-type path) '(nil "vm")
                                       :test #'equal)
                               (parse-integer (pathname-name path))))))
        (when (and relfilenode (not (member relfilenode known :test #'eql)))
          (smgr-forget relfilenode :delete t)
          (setf removed t))))
    (when removed (fsync-directory base)))
  (values))

(declaim (ftype function startup-checkpoint-commit-ts-facts))

(declaim (ftype function wal-settle-page-publications-under-lock))

(defun wal-checkpoint-core ()
  "Write all cached pages, sync durable state, and advance the redo pointer.
Capture installed commit timestamp facts before taking storage locks. Under the
WAL lock, freeze resource facts before planning placement and rotation. Check the
encoded size against that plan before appending the checkpoint. Include known
full page writes policy, including OFF, and advance its checkpoint witness only
after checkpoint control publication. Include known logical status and advance
its witness at the same boundary. Extra full page images remain native format
when the policy is OFF.
Settle pending pages, then require page and CLOG debt to be empty before appending
any checkpoint batch records. Shutdown can defer recycling until its senders
release their claims to the retained WAL."
  (let ((commit-ts-facts (startup-checkpoint-commit-ts-facts)))
    ;; CHECKPOINT follows the global write-lock -> WAL-lock order.
    (with-write-lock
      (with-wal-critical
        (wal-settle-page-publications-under-lock)
        (wal-require-no-pending-storage)
        (wal-process-pending-logical-disable-under-lock #'values)
        (let ((keys (wal-primary-page-keys))
              (fpw-facts (wal-current-full-page-writes-fields))
              (logical-facts (wal-current-logical-decoding-fields)))
          (wal-primary-preflight-page-keys keys)
          (with-wal-primary-page-spool (spool)
            (wal-primary-record-page-xids keys (wal-append-batch-start))
            (wal-primary-append-pages keys spool)
            (wal-primary-seal-page-spool spool)
            (let* ((checkpoint-facts
                     (append (wal-copy-control-resource-fields *wal-control-metadata*)
                             commit-ts-facts fpw-facts logical-facts))
                   (size (wal-checkpoint-record-size checkpoint-facts))
                   (start (wal-next-record-start size))
                   (snapshot (wal-capture-checkpoint-snapshot start checkpoint-facts))
                   (payload (wal-encode-checkpoint-snapshot snapshot))
                   (checkpoint-end
                     (progn
                       (unless (= size (+ +wal-header-size+ (length payload)))
                         (error "checkpoint encoding differs from planned size"))
                       (wal-append-record +wal-record-checkpoint+ 0 payload))))
              (wal-flush)
              (wal-test-pause :after-checkpoint-wal-flush)
              (wal-primary-apply-page-spool spool)
              (wal-clear-page-cache)
              (smgr-fsync-all)
              (clog-fsync)
              (fsync-directory *pgdata*)
              (fsync-directory (merge-pathnames "base/" *pgdata*))
              (fsync-directory (merge-pathnames "global/" *pgdata*))
              (wal-save-checkpoint-control checkpoint-end snapshot (clog-file-extent))
              (wal-fpw-note-published-checkpoint snapshot)
              (wal-logical-note-published-checkpoint snapshot)
              (wal-test-pause :after-checkpoint-control)
              (wal-remove-checkpointed-segments-under-wal-lock checkpoint-end)))))))
  (values))

(declaim (ftype function wal-prepare-slot-recycling-under-lock))

(defun wal-checkpoint-recycling-segment (checkpoint-lsn)
  "Return the segment containing the checkpoint's last byte; zero for LSN zero."
  (if (zerop checkpoint-lsn) 0 (floor (1- checkpoint-lsn) +wal-segment-size+)))

(declaim (ftype function startup-primary-wal-archive-enabled-p query-temp-lstat))

(declaim (ftype function query-temp-open-directory query-temp-fd-child-path))

(declaim (ftype function wal-archive-try-reconcile-ready-under-lock wal-archive-recycle-segment))

(defun wal-remove-checkpointed-segments-under-wal-lock (checkpoint-lsn &optional (current *wal-durable-lsn*))
  "Plan and remove a contiguous WAL prefix under WAL exclusion.
Active standard-policy owners defer removal in a process-local ticket; the caller
retries outside locks after stopping them. Independent XID and snapshot floors
also constrain removal. CURRENT freezes the endpoint across that retry, while
each calculation captures the current retention settings."
  ;; Publish ready markers before other recycling fences.
  (when (startup-primary-wal-archive-enabled-p)
    (wal-archive-try-reconcile-ready-under-lock))
  (when (logical-slot-continuation-gc-fenced-p *pgdata*)
    (return-from wal-remove-checkpointed-segments-under-wal-lock (values)))
  (when *wal-shutdown-defer-recycling*
    (return-from wal-remove-checkpointed-segments-under-wal-lock (values)))
  (wal-settle-page-publications-under-lock)
  (wal-require-no-pending-page-publication)
  (let ((checkpoint-segment (wal-checkpoint-recycling-segment checkpoint-lsn)))
    (multiple-value-bind (keep-segment requests)
        (wal-prepare-slot-recycling-under-lock checkpoint-segment current)
      (when requests
        (setf *wal-slot-recycling-pending*
              (wal-capture-slot-recycling-work checkpoint-lsn current requests))
        (return-from wal-remove-checkpointed-segments-under-wal-lock
          (values nil requests)))
      (logical-test-failpoint :before-slot-wal-removal keep-segment)
      (setf *wal-slot-recycling-pending* nil)
      (let ((removed nil)
            (archiving (startup-primary-wal-archive-enabled-p)))
        (dolist (segment (wal-discover-segments))
          (when (< segment keep-segment)
            (if archiving
                (unless (wal-archive-recycle-segment segment) (return))
                (progn
                  (when (and *wal-output-stream* (eql segment *wal-output-segment*))
                    (close *wal-output-stream*)
                    (setf *wal-output-stream* nil *wal-output-segment* nil))
                  (handler-case
                      (progn
                        (delete-file (wal-segment-path segment))
                        (setf removed t))
                    (file-error () (return)))))))
        (when removed (fsync-directory (wal-directory))))))
  (values))

(defun wal-log-clog-reset-under-lock ()
  "Append and flush the reset record with staged pages under WAL lock.
The caller keeps WAL locked through subsequent CLOG file replacement; this
function does not replace the file itself."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-require-no-pending-storage)
  (let* ((next-xid (symbol-value '*next-xid*))
         (keys (wal-primary-page-keys))
         (payload (make-array 8 :element-type '(unsigned-byte 8) :initial-element 0)))
    (wal-primary-preflight-page-keys keys)
    (with-wal-primary-page-spool (spool)
      (wal-primary-record-page-xids keys (wal-append-batch-start))
      (wal-primary-append-pages keys spool)
      (wal-primary-seal-page-spool spool)
      (wal-set-u64 payload 0 next-xid)
      (wal-append-record +wal-record-clog-reset+ next-xid payload)
      (wal-flush)
      (wal-primary-apply-page-spool spool)
      (wal-clear-page-cache)))
  (values))

(defun wal-log-clog-reset ()
  "Log a CLOG reset under WAL exclusion after checking pending debt."
  (when *wal-ready*
    (with-wal-critical
      (wal-settle-page-publications-under-lock)
      (wal-require-no-pending-storage)
      (wal-log-clog-reset-under-lock)))
  (values))
