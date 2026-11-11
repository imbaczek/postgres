;;;; wal.lisp -- small physical WAL with full-page after-images.

(in-package #:pg)

(defconstant +wal-segment-size+ (* 16 1024 1024))
(defconstant +wal-header-size+ 32)

(defconstant +wal-control-maximum-bytes+ 1024
  "Maximum control line characters, excluding its terminating newline.")

(defconstant +wal-max-page-images-per-record+ 512)
(defconstant +wal-record-pages+ 1)
(defconstant +wal-record-commit+ 2)
(defconstant +wal-record-catalog+ 3)
(defconstant +wal-record-checkpoint+ 4)
(defconstant +wal-record-clog-reset+ 5)
(defconstant +wal-record-batch-start+ 6)
(defconstant +wal-record-catalog-chunk+ 7)
(defconstant +wal-record-catalog-commit+ 8)
(defconstant +wal-record-maintenance+ 9)
(defconstant +wal-record-logical-changes+ 10)
(defconstant +wal-record-logical-catalog-event+ 11)

(defconstant +wal-record-xid-reservation+ 12)

(defconstant +wal-record-running-xacts+ 14)

(defconstant +wal-record-parameter-change+ 16
  "Native version 8 empty-batch terminal carrying a resource tuple.")

(defconstant +wal-record-full-page-writes-change+ 17)

(declaim (ftype function wal-fpw-rebind-incarnation))

(defconstant +wal-record-logical-decoding-status-change+ 18)

(declaim (ftype function wal-logical-decoding-status-record-info
                wal-logical-status-from-checkpoint wal-logical-status-next))

(declaim (ftype function wal-current-full-page-writes-fields
                wal-fpw-note-published-checkpoint wal-fpw-state-from-checkpoint
                wal-fpw-state-next wal-fpw-install-recovered-state wal-fpw-prepare-recovery))

(declaim (ftype function wal-full-page-writes-change-record-info))

(declaim (ftype function wal-parameter-change-record-info wal-recover-resource-parameters))

(declaim (ftype function wal-running-xacts-record-info
                wal-append-running-xacts-under-lock))

(defconstant +wal-catalog-chunk-header-size+ 8)
(defconstant +wal-version-v2+ 2)
(defconstant +wal-version-v3+ 3)
(defconstant +wal-version-v4+ 4)

(defconstant +wal-version-v5+ 5)

(defconstant +wal-version-v6+ 6)

(defconstant +wal-version-v7+ 7)

(defconstant +wal-record-relation-reset+ 15)

(defconstant +wal-record-end-of-recovery+ 13)

(declaim (ftype function wal-end-of-recovery-record-info))

(defconstant +wal-version+ 8)

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

(defstruct wal-record
  version type xid start end payload)

(defstruct wal-page-image
  ;; XIDS lists actual writer FullXIDs, excluding internal markers 0 and 1.
  ;; Provenance describes the whole image: :unknown, :row, or :maintenance.
  relfilenode (fork :main) block page (xids '()) (provenance :unknown))

(defstruct wal-replication-slot
  "KIND distinguishes logical and physical slots, including unbound logical slots.
Owners are process-local; the appropriate codec stores durable slot fields."
  name database-oid restart-lsn consistent-lsn confirmed-flush-lsn active-owner
  active-owner-backend
  ;; Process-local only: DROP-CLAIM retains the directory identity across retries.
  ;; CREATION-FAILED-P fences a possibly published slot until startup validates it.
  ;; Neither field is encoded in durable slot state.
  (drop-claim nil) (creation-failed-p nil)
  ;; PGSLOT1-3 were the private protocol even though the old wire name was pgoutput.
  (plugin "pg_lisp")
  (kind :logical) system-id restart-timeline (temporary-p nil) temporary-owner
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

(defun wal-copy-replication-slot-with-profile (slot)
  "Copy a slot while preserving its immutable durable transport profile."
  (let ((copy (copy-wal-replication-slot slot)))
    (unless (eq (wal-slot-profile slot) :text)
      (wal-slot-set-initial-profile copy (wal-slot-profile slot)))
    copy))

(defun wal-logical-slot-p (slot)
  "Test the explicit slot kind; a nullable database alone does not identify it."
  (eq :logical (wal-replication-slot-kind slot)))

(defun wal-require-known-replication-slot (slot)
  ;; A failed publication may still be visible on disk and must retain its pin.
  (when (wal-replication-slot-creation-failed-p slot)
    (ereport "55000" "replication slot ~s creation failed; restart required"
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

(defun wal-find-usable-replication-slot-under-lock (name)
  ;; Look up and check the generation while the registry is locked.
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((slot (gethash name *wal-replication-slots*)))
    (when slot (wal-require-usable-replication-slot slot))))

(defun wal-require-logical-slot (slot)
  "Reject physical slots before logical decoding or acknowledgment."
  (wal-require-usable-replication-slot slot)
  (unless (wal-logical-slot-p slot)
    (ereport "55000" "physical replication slot cannot use logical acknowledgment or decoding"))
  slot)

(defun wal-physical-slot-p (slot)
  "Test the explicit physical kind, not the database field."
  (eq :physical (wal-replication-slot-kind slot)))

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

(defvar *max-replication-slots* 10
  "Shared capacity for all public slot names, including temporary and lost slots.")

(defun wal-check-replication-slot-capacity ()
  "Check shared capacity under WAL, counting temporary and lost slots too."
  (when (zerop *max-replication-slots*)
    (ereport "55000" "replication slots require max_replication_slots greater than zero"))
  (when (>= (hash-table-count *wal-replication-slots*) *max-replication-slots*)
    (ereport "53400" "all replication slots are in use"))
  (values))

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

(defvar *wal-segment-namespace* :legacy
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

(defvar *wal-recovery-catalog-chunks* '())
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
                clog-get clog-set clog-reset clog-fsync))
(declaim (notinline pg-class-relfilenode pg-index-relfilenode
                    database-tables database-indexes catalog-databases))

(defun wal-directory ()
  (pgdata-resolve-path "pg_wal/" *pgdata*))

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

(defun wal-ensure-directory-durable (directory)
  "Create DIRECTORY's path one level at a time and sync each parent entry."
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

(defun wal-control-path ()
  (pgdata-resolve-path "global/pg_wal_control" *pgdata*))

(defun wal-replication-slot-directory ()
  (merge-pathnames "pg_replslot/" *pgdata*))

(defun wal-replication-slot-path (name)
  (slot-state-path (wal-replication-slot-directory) name))

(defun wal-u16 (bytes offset)
  (logior (aref bytes offset)
          (ash (aref bytes (+ offset 1)) 8)))

(defun wal-u32 (bytes offset)
  (logior (aref bytes offset)
          (ash (aref bytes (+ offset 1)) 8)
          (ash (aref bytes (+ offset 2)) 16)
          (ash (aref bytes (+ offset 3)) 24)))

(defun wal-u64 (bytes offset)
  (logior (wal-u32 bytes offset)
          (ash (wal-u32 bytes (+ offset 4)) 32)))

(defun wal-i64 (bytes offset)
  (let ((raw (wal-u64 bytes offset)))
    (if (logbitp 63 raw) (- raw #x10000000000000000) raw)))

(defun wal-set-u16 (bytes offset value)
  (setf (aref bytes offset) (logand value #xFF)
        (aref bytes (+ offset 1)) (logand (ash value -8) #xFF))
  value)

(defun wal-set-u32 (bytes offset value)
  (setf (aref bytes offset) (logand value #xFF)
        (aref bytes (+ offset 1)) (logand (ash value -8) #xFF)
        (aref bytes (+ offset 2)) (logand (ash value -16) #xFF)
        (aref bytes (+ offset 3)) (logand (ash value -24) #xFF))
  value)

(defun wal-set-u64 (bytes offset value)
  (wal-set-u32 bytes offset (logand value #xFFFFFFFF))
  (wal-set-u32 bytes (+ offset 4) (logand (ash value -32) #xFFFFFFFF))
  value)

(defun wal-set-i64 (bytes offset value)
  (unless (typep value '(signed-byte 64)) (error "unrepresentable signed WAL integer"))
  (wal-set-u64 bytes offset (ldb (byte 64 0) value))
  value)

(defun make-wal-crc32-table ()
  (let ((table (make-array 256 :element-type '(unsigned-byte 32))))
    (dotimes (i 256 table)
      (let ((crc i))
        (loop repeat 8
              do (setf crc (if (oddp crc)
                               (logxor (ash crc -1) #x82F63B78)
                               (ash crc -1))))
        (setf (aref table i) (logand crc #xFFFFFFFF))))))

(defparameter *wal-crc32-table* (make-wal-crc32-table)
  "Native storage CRC32C table; reinitialized on reload when the format changes.")

#+(or x86-64 arm64)
(defparameter *wal-crc32c-reference-table* (make-wal-crc32-table))

(defun wal-crc32-typed-range (bytes start end table &optional (seed #xffffffff))
  "Return final CRC32C for a validated slice and raw SEED.
Canonical tables permit CPU-qualified native kernels; custom tables retain
their scalar recurrence. Inputs are read only."
  (declare (type (simple-array (unsigned-byte 8) (*)) bytes)
           (type (simple-array (unsigned-byte 32) (256)) table)
           (type fixnum start end)
           (type (unsigned-byte 32) seed))
  #+x86-64
  (when (and *native-crc32c-sse42-available-p*
             (equalp table *wal-crc32c-reference-table*))
    (return-from wal-crc32-typed-range
      (if (and *native-crc32c-pclmul-available-p* (>= (- end start) 64))
          (%wal-crc32c-pclmul-octets bytes start end table
                                     *native-crc32c-constants* seed)
          (%wal-crc32c-sse42-octets bytes start end nil nil seed))))
  #+arm64
  (when (and *native-arm-crc32-available-p*
             (equalp table *wal-crc32c-reference-table*))
    (return-from wal-crc32-typed-range
      (%wal-crc32c-arm-octets bytes start end nil nil seed)))
  (%wal-crc32-octets bytes start end table seed))

(defun wal-crc32 (bytes &optional (start 0) (end (length bytes)))
  "Return final CRC32C (Castagnoli) of BYTES from START inclusive to END exclusive.
The initial raw state and final XOR are #xffffffff, as in PostgreSQL C.
Validated octet slices use available native instructions with a canonical table;
other inputs and modified tables use the scalar recurrence. BYTES is read only."
  (let ((table (and (boundp '*wal-crc32-table*) *wal-crc32-table*)))
    (if (and (typep bytes '(simple-array (unsigned-byte 8) (*)))
             (typep table '(simple-array (unsigned-byte 32) (256)))
             (typep start 'fixnum) (typep end 'fixnum)
             (<= 0 start end (length bytes)))
        (wal-crc32-typed-range bytes start end table)
        (let ((crc #xFFFFFFFF))
          (loop for i from start below end
                for slot = (logand (logxor crc (aref bytes i)) #xFF)
                do (setf crc (logxor (ash crc -8) (aref *wal-crc32-table* slot))))
          (logxor crc #xFFFFFFFF)))))

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
        ;; New pages staged in memory and legacy pages both have no WAL LSN.
        ;; Persisted WAL pages always carry a nonzero LSN and must have a CRC.
        (zerop checksum)
        (= checksum (wal-page-checksum page relfilenode block fork)))))

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

(defun wal-decode-page-images (payload lsn &optional (version +wal-version+))
  ;; Unflagged older images remain readable but cannot pass retained readers.
  ;; Flags are valid only in version 7 or later.
  (when (< (length payload) 4)
    (error "WAL page-image record is too short"))
  (let* ((count (wal-u32 payload 0))
         (header-size (if (= version +wal-version-v2+) 8 12))
         (offset 4)
         (images '()))
    (loop repeat count
          do (let* ((relfilenode (wal-u32 payload offset))
                    (fork (if (= version +wal-version-v2+)
                              :main
                              (wal-page-fork-from-id
                               (wal-u32 payload (+ offset 4)))))
                    (block (wal-u32 payload (+ offset (if (= version +wal-version-v2+) 4 8))))
                    (page-offset (+ offset header-size))
                    (page-end (+ page-offset +blcksz+)))
               (unless (<= page-end (length payload))
                 (error "WAL page-image record has a truncated page"))
               (let* ((page (subseq payload page-offset page-end))
                      (xids nil) (provenance :unknown)
                      (next-offset page-end))
                 (when (>= version +wal-version-v4+)
                   (unless (<= (+ next-offset 4) (length payload))
                     (error "WAL page-image record has a truncated contributor count"))
                   (let* ((tagged-count (wal-u32 payload next-offset))
                          (flags (ldb (byte 2 30) tagged-count))
                          (xid-count (logand tagged-count #x3fffffff))
                          (xids-start (+ next-offset 4))
                          (xids-end (+ xids-start (* xid-count 8))))
                     (unless (and (/= flags 1) (or (zerop flags) (>= version 7)))
                       (error "invalid page provenance flags"))
                     (setf provenance (case flags (0 :unknown) (2 :row) (3 :maintenance)))
                     (unless (<= xids-end (length payload))
                       (error "WAL page-image record has truncated contributors"))
                     (setf xids
                           (loop for xid-offset from xids-start below xids-end by 8
                                 collect (wal-u64 payload xid-offset))
                           next-offset xids-end)))
                 (unless (and (= (page-u64 page +pd-lsn-off+) lsn)
                              (wal-verify-page page relfilenode block fork))
                   (error "WAL page image has an invalid LSN or checksum"))
                 (push (make-wal-page-image :relfilenode relfilenode
                                            :fork fork :block block :page page
                                            :xids xids :provenance provenance)
                       images)
                 (setf offset next-offset))))
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

(defun wal-fail-stop (condition)
  "Poison WAL, retain CONDITION on this backend, and request fast shutdown.
Re-signal CONDITION after requesting shutdown."
  (setf *wal-poisoned* t)
  (when (fboundp 'note-backend-wal-failure)
    (funcall 'note-backend-wal-failure condition))
  (when (fboundp 'request-fast-shutdown)
    (ignore-errors (funcall (symbol-function 'request-fast-shutdown))))
  (error condition))

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
    (let* ((path (wal-segment-path segment))
           (newp (not (probe-file path))))
      (setf *wal-output-stream*
            (open path :direction :output
                       :element-type '(unsigned-byte 8)
                       :if-exists :append :if-does-not-exist :create)
            *wal-output-segment* segment)
      (when newp (pushnew segment *wal-new-segments* :test #'eql))))
  *wal-output-stream*)

(defun wal-encode-record (type xid start payload)
  (let* ((length (+ +wal-header-size+ (length payload)))
         (record (make-array length :element-type '(unsigned-byte 8)
                                    :initial-element 0)))
    (setf (aref record 0) (char-code #\P)
          (aref record 1) (char-code #\G)
          (aref record 2) (char-code #\W)
          (aref record 3) (char-code #\L))
    (wal-set-u16 record 4 +wal-version+)
    (wal-set-u16 record 6 type)
    (wal-set-u32 record 8 length)
    (wal-set-u64 record 12 start)
    (wal-set-u64 record 20 xid)
    (replace record payload :start1 +wal-header-size+)
    (wal-set-u32 record 28 (wal-crc32 record))
    record))

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
    (unless (and (>= (wal-record-version record) 8)
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

(defun wal-prepare-page-publication-under-lock (owner xid resets split-p &key (kind :transaction))
  "Seal the spool and prepare empty replacement maps before the terminal record.
Record the current maps and WAL identity while the caller holds *WAL-LOCK*."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-healthy)
  (wal-check-page-publication-queue-under-lock)
  (unless (and *wal-ready* (wal-page-publication-p owner)
               (eq :stack (wal-page-publication-state owner))
               (not (wal-page-publication-prepared-p owner))
               (wal-page-publication-spool owner)
               (or (and (eq kind :transaction)
                        (typep xid '(integer 3 18446744073709551615))
                        (>= (logand xid #xffffffff) 3))
                   (and (eq kind :maintenance) (eql xid 0) (null resets)))
               (null (wal-page-publication-next owner))
               (or (null resets) (null *wal-pending-page-publication*)))
    (wal-invalid-input "invalid page publication preparation"))
  (call-with-wal-spool-owner
   (wal-page-publication-spool owner)
   (lambda () (logical-spool-seal (wal-page-publication-spool owner))))
  (let ((replacement-pages (make-hash-table :test #'equal))
        (replacement-contributors (make-hash-table :test #'equal)))
    (setf (wal-page-publication-pages owner) *wal-page-cache*
          (wal-page-publication-contributors owner) *wal-page-cache-xids*
          (wal-page-publication-resets owner) resets
          (wal-page-publication-split-p owner) split-p
          (wal-page-publication-xid owner) xid
          (wal-page-publication-kind owner) kind
          (wal-page-publication-incarnation owner) *wal-storage-incarnation*
          (wal-page-publication-timeline owner) *wal-insert-timeline*
          (wal-page-publication-replacement-pages owner) replacement-pages
          (wal-page-publication-replacement-contributors owner) replacement-contributors
          (wal-page-publication-prepared-p owner) t))
  (setf (wal-page-publication-pressure-ticket owner) (wal-actual-publication-pressure owner))
  owner)

(defun wal-publish-page-publication-under-lock (owner end)
  "Transfer both whole dirty maps to OWNER at the terminal record's END.
Install the preallocated empty maps by pointer, without changing references."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-sys:without-interrupts
    (wal-check-healthy)
    (wal-check-page-publication-queue-under-lock)
    (unless (and (wal-page-publication-p owner)
                 (eq :stack (wal-page-publication-state owner))
                 (wal-page-publication-prepared-p owner)
                 (eq *wal-page-cache* (wal-page-publication-pages owner))
                 (eq *wal-page-cache-xids* (wal-page-publication-contributors owner))
                 (eq *wal-storage-incarnation* (wal-page-publication-incarnation owner))
                 (eql *wal-insert-timeline* (wal-page-publication-timeline owner))
                 (typep end '(integer 1 18446744073709551615))
                 (<= end *wal-end-lsn*)
                 (null (wal-page-publication-next owner))
                 (or (null *wal-pending-page-publication-tail*)
                     (> end (wal-page-publication-terminal-end *wal-pending-page-publication-tail*)))
                 (hash-table-p (wal-page-publication-replacement-pages owner))
                 (zerop (hash-table-count (wal-page-publication-replacement-pages owner)))
                 (hash-table-p (wal-page-publication-replacement-contributors owner))
                 (zerop (hash-table-count (wal-page-publication-replacement-contributors owner))))
      (wal-invalid-input "page publication preparation changed before handoff"))
    (clog-check-publication-handoff-under-wal-lock owner end)
    (setf (wal-page-publication-terminal-end owner) end
          (wal-page-publication-state owner) :pending
          *wal-page-cache* (wal-page-publication-replacement-pages owner)
          *wal-page-cache-xids* (wal-page-publication-replacement-contributors owner)
          *wal-pending-btree-split* nil
          (wal-page-publication-replacement-pages owner) nil
          (wal-page-publication-replacement-contributors owner) nil)
    (if *wal-pending-page-publication-tail*
        (setf (wal-page-publication-next *wal-pending-page-publication-tail*) owner)
        (setf *wal-pending-page-publication* owner))
    (setf *wal-pending-page-publication-tail* owner))
  owner)

(defun wal-drain-page-publication-under-lock (owner &key fault-on-first-write)
  "Complete CLOG and apply a durable owner, then retire its spool and references.
Only the FIFO head can retire, and it is removed last. Failure leaves debt for
fail-stop handling."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-sys:without-interrupts
    (wal-check-healthy)
    (wal-check-page-publication-owner-under-lock owner)
    (unless (eq owner *wal-pending-page-publication*)
      (wal-invalid-input "page publication drain must start at the queue head"))
    (unless (<= (wal-page-publication-terminal-end owner) *wal-durable-lsn*)
      (wal-invalid-input "pending page publication WAL is not durable"))
    (wal-complete-publication-status-under-lock owner)
    (when (eq :pending (wal-page-publication-state owner))
      (wal-apply-relation-resets (wal-page-publication-resets owner))
      (when (wal-page-publication-resets owner) (wal-test-pause :after-relation-resets))
      (wal-primary-apply-page-spool
       (wal-page-publication-spool owner)
       :split-p (wal-page-publication-split-p owner) :fault-on-first-write fault-on-first-write)
      (setf (wal-page-publication-state owner) :applied))
    (when (eq :applied (wal-page-publication-state owner))
      (wal-close-primary-page-spool (wal-page-publication-spool owner))
      (setf (wal-page-publication-state owner) :retiring))
    (maphash (lambda (key reference)
               (wal-dirty-release-reference reference)
               (remhash key (wal-page-publication-pages owner))
               (remhash key (wal-page-publication-contributors owner)))
             (wal-page-publication-pages owner))
    (wal-dirty-close-empty-store)
    (let ((next (wal-page-publication-next owner)))
      (setf (wal-page-publication-state owner) :released
            *wal-pending-page-publication* next
            (wal-page-publication-next owner) nil)
      (unless next (setf *wal-pending-page-publication-tail* nil))))
  (values))

(defun wal-drain-page-publication-prefix-under-lock (target &key include-target-p fault-on-first-write)
  "Validate and drain the durable FIFO prefix before TARGET, optionally including it."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-sys:without-interrupts
    (wal-check-healthy)
    (wal-check-page-publication-owner-under-lock target)
    (clog-check-queued-commits-under-wal-lock)
    (unless (<= (wal-page-publication-terminal-end target) *wal-durable-lsn*)
      (wal-invalid-input "captured page publication prefix is not durable"))
    (loop for head = *wal-pending-page-publication*
          until (and (eq head target) (not include-target-p))
          do (wal-drain-page-publication-under-lock head :fault-on-first-write fault-on-first-write)
          when (eq head target) return nil))
  (values))

(defun wal-drain-page-publications-through-under-lock (target &key fault-on-first-write)
  "Validate TARGET and its durable prefix before effects; retire through TARGET only."
  (wal-drain-page-publication-prefix-under-lock
   target :include-target-p t :fault-on-first-write fault-on-first-write))

(defmacro with-wal-poison-on-incomplete ((message) &body body)
  "Run BODY under the held WAL lock with interrupts masked.
On incomplete exit, poison WAL before unlock and preserve the first error.
Signal MESSAGE for a bare nonlocal exit. This macro does not request shutdown;
callers must do that after releasing locks."
  (let ((complete (gensym "COMPLETE-"))
        (failure (gensym "FAILURE-"))
        (condition (gensym "CONDITION-")))
    `(progn
       (assert (sb-thread:holding-mutex-p *wal-lock*))
       (let ((,complete nil) (,failure nil))
         (sb-sys:without-interrupts
           (unwind-protect
                (handler-bind ((error (lambda (,condition)
                                        (unless ,failure (setf ,failure ,condition)))))
                  (multiple-value-prog1 (progn ,@body)
                    (setf ,complete t)))
             (unless ,complete
               (setf *wal-poisoned* t)
               (unless ,failure (error ,message)))))))))

(defun wal-settle-page-publications-under-lock ()
  "Flush and drain the captured FIFO prefix under WAL exclusion.
Incomplete settlement poisons WAL. Preserve ordinary errors; convert a bare
nonlocal exit to an error so the caller can fence failure after unlocking."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-healthy)
  (wal-check-page-publication-queue-under-lock)
  (clog-check-queued-commits-under-wal-lock)
  (let ((target *wal-pending-page-publication-tail*))
    (when target
      (with-wal-poison-on-incomplete ("WAL predecessor drain exited before completion")
        (wal-flush-core)
        (wal-drain-page-publications-through-under-lock target :fault-on-first-write t)))
    target))

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
          (funcall function (wal-decode-page-images (subseq event 12) lsn version)))))))
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

(defun wal-encode-commit-timestamp (timestamp)
  (unless (and (integerp timestamp) (<= (- (ash 1 63)) timestamp (1- (ash 1 63))))
    (error "invalid WAL commit timestamp"))
  (let ((payload (make-array 8 :element-type '(unsigned-byte 8))))
    (dotimes (index 8 payload)
      (setf (aref payload index) (ldb (byte 8 (* index 8)) timestamp)))))

(defun wal-decode-commit-timestamp (payload)
  ;; Earlier commit records have an empty payload. An eight-byte payload adds
  ;; signed microseconds without changing the record header or row-change codec.
  (case (length payload)
    (0 nil)
    (8 (let ((value (loop for index below 8
                          sum (ash (aref payload index) (* index 8)))))
         (if (logbitp 63 value) (- value (ash 1 64)) value)))
    (otherwise (error "WAL commit record has an invalid payload"))))

(declaim (ftype function wal-validate-logical-binary-capability-under-lock))

(defun wal-admit-transaction-under-lock (xid logical-changes)
  "Prepare and queue a transaction's pages and CLOG completion under WAL lock.
Reserve against the planned terminal end before attempting insertion. Publish
only after the actual end matches, then hand the owner to the FIFO. An
unattempted reservation can be cancelled; an ambiguous attempt retains its owner
for fail-stop handling. Return the owner and actual terminal end.
Caller masks interrupts through handoff and fences incomplete publication.
Pressure may settle predecessors before this batch starts. The third value
requires synchronous completion of this owner before any asynchronous acceptance.
This helper does not flush the new owner or provide an acceptance receipt."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-page-publication-queue-under-lock)
  (clog-check-queued-commits-under-wal-lock)
  (wal-validate-logical-binary-capability-under-lock xid)
  (when (and (fboundp 'transaction-has-relation-replacements-p)
             (funcall 'transaction-has-relation-replacements-p
                      (when (boundp '*backend*) (symbol-value '*backend*))))
    (wal-settle-page-publications-under-lock)
    (wal-require-no-pending-storage))
  (let* ((resets
           (when (fboundp 'publish-transaction-replacements-under-wal-lock)
             (funcall 'publish-transaction-replacements-under-wal-lock
                      (when (boundp '*backend*) (symbol-value '*backend*)))))
         (keys (wal-primary-page-keys))
         (split-p *wal-pending-btree-split*))
    (wal-primary-preflight-page-keys keys)
    (multiple-value-bind (projection forced-p) (wal-preflight-publication-pressure keys resets)
      (with-wal-primary-page-spool (spool publication)
        (wal-append-running-xacts-under-lock xid)
        (wal-primary-record-page-xids keys (wal-append-batch-start))
        (dolist (reset resets)
          (wal-append-record +wal-record-relation-reset+ xid
                             (wal-encode-relation-reset (first reset) (second reset))))
        (wal-primary-append-pages keys spool)
        (wal-primary-seal-page-spool spool)
        (wal-prepare-page-publication-under-lock publication xid resets split-p)
        (setf forced-p
              (wal-publication-pressure-requires-flush-p publication projection forced-p))
        (when logical-changes (wal-append-logical-changes xid logical-changes))
        (when *wal-output-stream* (finish-output *wal-output-stream*))
        (wal-test-pause :after-page-images)
        (let* ((payload (wal-encode-commit-timestamp (postgres-current-timestamp)))
               (size (+ +wal-header-size+ (length payload)))
               (planned-end (+ (wal-next-record-start size) size))
               (completion (clog-reserve-commit-under-wal-lock xid planned-end publication)))
          (assert (eq completion (wal-page-publication-completion publication)))
          (wal-test-pause :after-clog-reservation)
          (clog-begin-reserved-commit-under-wal-lock completion publication)
          (wal-test-pause :before-terminal-append)
          (let ((end (wal-append-record +wal-record-commit+ xid payload)))
            (clog-publish-reserved-commit-under-wal-lock completion publication end)
            (wal-test-pause :before-page-publication-transfer)
            (wal-publish-page-publication-under-lock publication end)
            (wal-test-pause :after-page-publication-transfer)
            (values publication end forced-p)))))))

(defun wal-complete-publication-status-under-lock (owner)
  "Complete the durable FIFO head's CLOG obligation, if it has one.
This may be called again after materialization while page retirement is pending."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-page-publication-owner-under-lock owner)
  (unless (eq owner *wal-pending-page-publication*)
    (wal-invalid-input "CLOG completion must start at the queue head"))
  (clog-check-queued-commits-under-wal-lock)
  (let ((completion (wal-page-publication-completion owner)))
    (when completion
      (wal-slot-record-xid-commit-under-wal-lock
       (wal-page-publication-xid owner) (wal-page-publication-terminal-end owner))
      (clog-complete-reserved-commit-under-wal-lock completion owner)))
  (values))

(defun wal-commit-transaction
    (xid after-durable-function &key logical-changes terminal-durable-function
                                  (source-id nil source-id-supplied-p) accepted-function
                                  (local-durability :flush))
  "Publish XID's commit and return its end and immutable historical receipt.
LOCAL-DURABILITY defaults to :FLUSH. :ASYNC can accept a queued owner
without flushing when pressure, resets and writer availability permit it. The
FIFO retains the owner's pages, spool and CLOG completion independent of the
backend. Otherwise flush and apply through this owner before acceptance; reset
descriptors remain owned through AFTER-DURABLE-FUNCTION.

A receipt records whether durability was observed at acceptance; it is not
upgraded later. The async path calls neither durable callback. On the flush path
TERMINAL-DURABLE-FUNCTION observes the exact end after flush and before
AFTER-DURABLE-FUNCTION. Without WAL, the latter must write the commit status and the
receipt has no WAL end or proof. Incomplete publication is fail-stopped by the
publication fence.

SOURCE-ID must be an explicit immutable symbol or integer when WAL is ready;
NIL is valid. ACCEPTED-FUNCTION receives the returned receipt under the interrupt
mask and must only latch scalar ownership and receipt state, without allocation,
I/O or locks. With WAL it runs under the publication fence and WAL mutex."
  (wal-require-local-commit-durability local-durability)
  (when (and *wal-ready* (not source-id-supplied-p))
    (error "WAL commit requires an explicit source identity"))
  (unless (typep source-id '(or symbol integer))
    (error "WAL commit source identity must be immutable"))
  (unless *wal-ready*
    (sb-thread:with-mutex (*wal-lock*) (wal-require-no-pending-storage))
    (sb-sys:without-interrupts
      (when (fboundp 'publish-transaction-replacements-under-wal-lock)
        (with-wal-critical
          (funcall 'publish-transaction-replacements-under-wal-lock
                   (when (boundp '*backend*) (symbol-value '*backend*)) t)))
      (funcall after-durable-function nil)
      (let ((receipt (%make-wal-commit-receipt xid source-id nil nil)))
        (when accepted-function (funcall accepted-function receipt))
        (return-from wal-commit-transaction (values nil receipt)))))
  (let ((commit-lsn nil) (receipt nil))
    (sb-sys:without-interrupts
      (with-wal-publication-fence (begin-publication)
        (wal-check-page-publication-queue-under-lock)
        (clog-check-queued-commits-under-wal-lock)
        (wal-validate-logical-binary-capability-under-lock xid)
        (when (and (fboundp 'transaction-has-relation-replacements-p)
                   (funcall 'transaction-has-relation-replacements-p
                            (when (boundp '*backend*) (symbol-value '*backend*))))
          (wal-settle-page-publications-under-lock)
          (wal-require-no-pending-storage))
        (begin-publication)
        (multiple-value-bind (publication end forced-p)
            (wal-admit-transaction-under-lock xid logical-changes)
          (setf commit-lsn end)
          (if (and (eq :async local-durability)
                   (not forced-p)
                   (null (wal-page-publication-resets publication))
                   (primary-wal-writer-available-under-lock-p))
              (progn
                (setf receipt (%make-wal-commit-receipt xid source-id commit-lsn nil))
                (wal-slot-record-xid-commit-under-wal-lock xid commit-lsn)
                (clog-accept-publication-under-wal-lock publication)
                (wal-request-async-flush-under-lock commit-lsn))
              (progn
                (wal-flush)
                (when terminal-durable-function (funcall terminal-durable-function commit-lsn))
                (wal-drain-page-publication-prefix-under-lock publication :fault-on-first-write t)
                (wal-slot-record-xid-commit-under-wal-lock xid commit-lsn)
                (wal-test-pause :after-wal-flush)
                (wal-complete-publication-status-under-lock publication)
                (funcall after-durable-function commit-lsn)
                (wal-drain-page-publications-through-under-lock publication :fault-on-first-write t)
                (setf receipt (%make-wal-commit-receipt xid source-id commit-lsn t)))))
        (when accepted-function (funcall accepted-function receipt))))
    (values commit-lsn receipt)))

(defun wal-commit-maintenance ()
  "Durably publish staged storage maintenance without committing a user XID."
  (unless *wal-ready*
    (return-from wal-commit-maintenance (values)))
  (with-wal-publication-fence (begin-publication)
    (wal-check-page-publication-queue-under-lock)
    (clog-check-queued-commits-under-wal-lock)
    (let ((keys (wal-primary-page-keys)) (split-p *wal-pending-btree-split*))
      (wal-primary-preflight-page-keys keys)
      (with-wal-primary-page-spool (spool publication)
        (begin-publication)
        (wal-primary-record-page-xids keys (wal-append-batch-start))
        (wal-primary-append-pages keys spool)
        (wal-primary-seal-page-spool spool)
        (wal-prepare-page-publication-under-lock publication 0 nil split-p :kind :maintenance)
        (let ((end (wal-append-record +wal-record-maintenance+ 0
                                      (make-array 0 :element-type '(unsigned-byte 8)))))
          (wal-publish-page-publication-under-lock publication end))
        (wal-test-pause :after-page-publication-transfer)
        (wal-flush)
        (wal-test-pause :after-wal-flush)
        (wal-drain-page-publications-through-under-lock publication :fault-on-first-write t))))
  (values))

;; TODO: Stream catalog serialization into a temporary file and write bounded
;; WAL chunks from it. Recovery should spool chunks, validate terminal metadata,
;; then publish from the spool; this path currently holds several full-image
;; byte arrays in memory.
(defun wal-encode-catalog-payload (bytes deletes)
  (let* ((payload (make-array (+ 8 (* 4 (length deletes)) (length bytes))
                              :element-type '(unsigned-byte 8)
                              :initial-element 0))
         (offset 0))
    (wal-set-u32 payload offset (length deletes))
    (incf offset 4)
    (dolist (relfilenode deletes)
      (wal-set-u32 payload offset relfilenode)
      (incf offset 4))
    (wal-set-u32 payload offset (length bytes))
    (incf offset 4)
    (replace payload bytes :start1 offset)
    payload))

(defun wal-decode-catalog-payload (payload)
  (when (< (length payload) 8)
    (error "WAL catalog record is too short"))
  (let* ((count (wal-u32 payload 0))
         (offset 4))
    (when (> (+ offset (* count 4) 4) (length payload))
      (error "WAL catalog record has an invalid delete list"))
    (let ((deletes '()))
      (loop repeat count
            do (push (wal-u32 payload offset) deletes)
               (incf offset 4))
      (setf deletes (nreverse deletes))
      (let ((catalog-length (wal-u32 payload offset)))
        (incf offset 4)
        (unless (= (+ offset catalog-length) (length payload))
          (error "WAL catalog record has an invalid catalog image length (offset ~a, image ~a, payload ~a)"
                 offset catalog-length (length payload)))
        (values deletes (subseq payload offset))))))

(defun wal-encode-catalog-chunk-payload (bytes index count)
  (let ((payload (make-array (+ +wal-catalog-chunk-header-size+
                                (length bytes))
                             :element-type '(unsigned-byte 8)
                             :initial-element 0)))
    (wal-set-u32 payload 0 index)
    (wal-set-u32 payload 4 count)
    (replace payload bytes :start1 +wal-catalog-chunk-header-size+)
    payload))

(defun wal-decode-catalog-chunk-payload (payload)
  (when (< (length payload) (1+ +wal-catalog-chunk-header-size+))
    (error "WAL catalog chunk is too short"))
  (let ((index (wal-u32 payload 0))
        (count (wal-u32 payload 4)))
    (unless (and (plusp count) (< index count))
      (error "WAL catalog chunk has an invalid index or count"))
    (values index count
            (subseq payload +wal-catalog-chunk-header-size+))))

(defun wal-catalog-chunk-count (payload)
  (let* ((chunk-size (- +wal-segment-size+ +wal-header-size+
                        +wal-catalog-chunk-header-size+))
         (count (ceiling (length payload) chunk-size)))
    (unless (<= count #xFFFFFFFF)
      (error "WAL catalog image requires too many chunks"))
    count))

(defun wal-encode-catalog-commit-payload (payload chunk-count)
  (let ((commit (make-array 12 :element-type '(unsigned-byte 8)
                               :initial-element 0)))
    (wal-set-u32 commit 0 chunk-count)
    (wal-set-u64 commit 4 (length payload))
    commit))

(defun wal-decode-catalog-commit-payload (payload)
  (unless (= (length payload) 12)
    (error "WAL catalog commit record has an invalid length"))
  (let ((count (wal-u32 payload 0))
        (length (wal-u64 payload 4)))
    (unless (plusp count)
      (error "WAL catalog commit record has an invalid chunk count"))
    (values count length)))

(defun wal-append-catalog-chunks (payload count)
  (let ((chunk-size (- +wal-segment-size+ +wal-header-size+
                       +wal-catalog-chunk-header-size+)))
    (loop for index from 0 below count
          for start = (* index chunk-size)
          for stop = (min (length payload) (+ start chunk-size))
          for chunk = (subseq payload start stop)
          do (wal-append-record
              +wal-record-catalog-chunk+ 0
              (wal-encode-catalog-chunk-payload chunk index count))))
  (values))

(defun wal-reassemble-catalog-chunks (expected-count expected-length)
  (unless (and (eql expected-count *wal-recovery-catalog-chunk-count*)
               (= expected-count *wal-recovery-catalog-next-chunk*)
               (= expected-count (length *wal-recovery-catalog-chunks*)))
    (error "WAL catalog chunk sequence is incomplete"))
  (let* ((chunks (nreverse *wal-recovery-catalog-chunks*))
         (actual-length (reduce #'+ chunks :key #'length :initial-value 0)))
    (unless (= actual-length expected-length)
      (error "WAL catalog chunks do not match their declared length"))
    (let ((payload (make-array actual-length
                               :element-type '(unsigned-byte 8)))
          (offset 0))
      (dolist (chunk chunks)
        (replace payload chunk :start1 offset)
        (incf offset (length chunk)))
      payload)))

(defun wal-defer-relation-delete (relfilenode)
  (when *wal-ready*
    (sb-thread:with-mutex (*wal-lock*)
      (pushnew relfilenode *wal-pending-deletes* :test #'eql)))
  (values))

(defun wal-catalog-flush-generation ()
  "Return the number of catalog WAL commits known to have reached durable WAL."
  (sb-thread:with-mutex (*wal-lock*)
    *wal-catalog-flush-generation*))

(defun wal-logical-catalog-event-needed-under-lock-p (database-oid)
  "Whether a live slot or in-flight snapshot needs DATABASE-OID catalog events.

Call while holding *WAL-LOCK*. Provisional interests close the gap between
snapshot capture under *WRITE-LOCK* and durable slot publication."
  (or (wal-logical-capture-required-under-lock-p)
      (loop for interested-database being the hash-values of
            *wal-logical-catalog-event-interests*
              thereis (= database-oid interested-database))
      (loop for slot being the hash-values of *wal-replication-slots*
              thereis (and (wal-logical-slot-p slot) (= database-oid
                                                        (or (wal-replication-slot-database-oid slot) -1))
                           (not (eq (wal-replication-slot-wal-status slot)
                                    :lost))))))

(defun wal-register-logical-catalog-event-interest-under-writer-lock
    (registration database-oid)
  "Protect sequence/DDL events after a snapshot boundary until slot publish."
  ;; WAL loads before the snapshot structure is defined, so keep validation
  ;; independent of its generated predicates. Callers pass the active
  ;; registration created by snapshot capture.
  (unless (and registration
               (integerp database-oid) (<= 1 database-oid #xFFFFFFFF))
    (error "logical catalog event interest requires a snapshot and database"))
  ;; The capture caller still holds *WRITE-LOCK*: taking *WAL-LOCK* here follows
  ;; the write -> WAL order and makes the interest visible before writers resume.
  (sb-thread:with-mutex (*wal-lock*)
    (setf (gethash registration *wal-logical-catalog-event-interests*)
          database-oid))
  registration)

(defun wal-release-logical-catalog-event-interest (registration)
  (when registration
    (sb-thread:with-mutex (*wal-lock*)
      (remhash registration *wal-logical-catalog-event-interests*)))
  (values))

(defun wal-prepare-logical-catalog-event-under-lock (event)
  "Validate EVENT and return it only if a slot or in-flight snapshot needs it.
Call with the WAL lock held, before beginning publication."
  (when event
    (when (< (length event) 4)
      (wal-invalid-input "invalid WAL logical catalog event envelope"))
    (when (wal-logical-catalog-event-needed-under-lock-p (wal-u32 event 0))
      (when (< (length event) 8)
        (wal-invalid-input "invalid WAL logical catalog event envelope"))
      (when (> (+ +wal-header-size+ (length event)) +wal-segment-size+)
        (wal-invalid-input "WAL logical catalog event exceeds the segment size"))
      event)))

(defun wal-commit-catalog (catalog-bytes write-function
                           &key relation-deletes logical-event
                             on-record-appended on-durable)
  "Durably publish a full catalog image and any deferred relation drops.
With WAL ready, return the exact end LSN of the terminal catalog record after
local apply. ON-RECORD-APPENDED runs after that record is appended; ON-DURABLE
receives its end LSN after the flush and before local page, catalog and drop
application. An incomplete publication fail-stops, retaining the first error.
Without WAL, write the catalog, call ON-DURABLE with NIL, and return no values."
  (unless *wal-ready*
    (sb-thread:with-mutex (*wal-lock*) (wal-require-no-pending-storage))
    (funcall write-function catalog-bytes)
    (sb-thread:with-mutex (*wal-lock*)
      (incf *wal-catalog-flush-generation*))
    (when on-durable (funcall on-durable nil))
    (return-from wal-commit-catalog (values)))
  (let ((commit-lsn nil))
    (with-wal-publication-fence (begin-publication)
      (wal-settle-page-publications-under-lock)
      (wal-require-no-pending-storage)
      (let* ((logical-event (wal-prepare-logical-catalog-event-under-lock logical-event))
             (deletes (sort (remove-duplicates
                             (append (copy-list *wal-pending-deletes*)
                                     (copy-list relation-deletes))
                             :test #'eql)
                            #'<))
             (keys (wal-primary-page-keys deletes))
             (catalog-payload (wal-encode-catalog-payload catalog-bytes deletes))
             (chunked-p (> (+ +wal-header-size+ (length catalog-payload)) +wal-segment-size+))
             (chunk-count (when chunked-p (wal-catalog-chunk-count catalog-payload)))
             (commit-payload (when chunked-p
                               (wal-encode-catalog-commit-payload catalog-payload chunk-count))))
        (wal-primary-preflight-page-keys keys)
        (with-wal-primary-page-spool (spool)
          (begin-publication)
          (wal-primary-record-page-xids keys (wal-append-batch-start))
          (wal-primary-append-pages keys spool)
          (wal-primary-seal-page-spool spool)
          (when chunked-p
            (wal-append-catalog-chunks catalog-payload chunk-count)
            (when *wal-output-stream* (finish-output *wal-output-stream*))
            (wal-test-pause :after-catalog-chunks))
          (when logical-event
            (wal-append-record +wal-record-logical-catalog-event+ 0 logical-event))
          (when (and *wal-vacuum-full-publication*
                     *wal-test-before-vacuum-full-catalog-record-appended-hook*)
            (funcall *wal-test-before-vacuum-full-catalog-record-appended-hook*))
          (setf commit-lsn
                (if chunked-p
                    (wal-append-record +wal-record-catalog-commit+ 0 commit-payload)
                    (wal-append-record +wal-record-catalog+ 0 catalog-payload)))
          (when on-record-appended (funcall on-record-appended))
          (when (and *wal-vacuum-full-publication*
                     *wal-test-pause-at*
                     (string= *wal-test-pause-at* "after-vacuum-full-catalog-wal-append"))
            (finish-output *wal-output-stream*)
            (fsync-stream *wal-output-stream*)
            (when *wal-new-segments*
              (fsync-directory (wal-directory))
              (setf *wal-new-segments* '()))
            (wal-test-pause :after-vacuum-full-catalog-wal-append))
          (when (and *wal-vacuum-full-publication*
                     *wal-test-before-vacuum-full-catalog-flush-hook*)
            (funcall *wal-test-before-vacuum-full-catalog-flush-hook*))
          (wal-flush)
          (incf *wal-catalog-flush-generation*)
          (when on-durable (funcall on-durable commit-lsn))
          (wal-test-pause :after-catalog-wal-flush)
          (when *wal-vacuum-full-publication*
            (wal-test-pause :after-vacuum-full-catalog-wal-flush))
          (when (and *wal-vacuum-full-publication*
                     *wal-test-after-vacuum-full-catalog-flush-hook*)
            (funcall *wal-test-after-vacuum-full-catalog-flush-hook*))
          (wal-primary-apply-page-spool spool)
          (funcall write-function catalog-bytes)
          (dolist (relfilenode deletes) (smgr-forget relfilenode :delete t))
          (setf *wal-pending-deletes*
                (set-difference *wal-pending-deletes* deletes :test #'eql))
          (when deletes (fsync-directory (merge-pathnames "base/" *pgdata*)))
          (wal-clear-page-cache)
          (setf *wal-pending-btree-split* nil))))
    commit-lsn))

(defun wal-segment-entry-names ()
  "Return raw WAL entry names, including dangling links.

Follow a symlink at the WAL directory itself. Enumeration errors are reported."
  (remove-if-not (lambda (name)
                   (and (> (length name) 4) (uiop:string-suffix-p name ".wal")))
                 (directory-entry-names (wal-directory) :follow-symlinks t)))

(defun wal-discover-segments (&optional (timeline *wal-insert-timeline*)
                                (namespace *wal-segment-namespace*))
  "Select only the explicit timeline and grammar; never pick the newest branch."
  (sort (loop for name in (wal-segment-entry-names)
              append (multiple-value-bind (candidate segment)
                         (wal-parse-segment-name (subseq name 0 (- (length name) 4)) namespace)
                       (when (eql candidate timeline) (list segment))))
        #'<))

(defun wal-valid-type-p (type version)
  (and (or (member type (list +wal-record-pages+ +wal-record-commit+
                              +wal-record-catalog+ +wal-record-checkpoint+
                              +wal-record-clog-reset+ +wal-record-batch-start+
                              +wal-record-catalog-chunk+
                              +wal-record-catalog-commit+
                              +wal-record-maintenance+))
           (and (= type +wal-record-relation-reset+) (>= version 8))
           (and (= type +wal-record-parameter-change+) (>= version 8))
           (and (= type +wal-record-full-page-writes-change+) (>= version 8))
           (and (= type +wal-record-logical-decoding-status-change+) (>= version 8))
           (and (= type +wal-record-xid-reservation+) (>= version +wal-version-v7+))
           (and (= type +wal-record-running-xacts+) (>= version 7))
           (and (= type +wal-record-end-of-recovery+) (>= version 7))
           (and (= type +wal-record-logical-changes+)
                (>= version +wal-version-v5+))
           (and (= type +wal-record-logical-catalog-event+)
                (>= version +wal-version-v5+)))
       (or (/= type +wal-record-maintenance+)
           ;; Maintenance terminals were already emitted by v3. Keep them
           ;; readable after the v4/v5 page-image extensions, as PostgreSQL's
           ;; version-aware xlogreader must decode WAL written by older servers.
           (>= version +wal-version-v3+))))

(defun wal-record-crc32 (bytes)
  "Return the final CRC of a WAL record with its checksum field treated as zero.
For a simple octet record of at least 32 bytes and a typed table, process bytes
0 through 27, invert that result back to raw state, apply four zero bytes, then
process bytes 32 onward.  This read-only path allocates no heap storage.
Other vectors or tables use the copy, zero and public CRC fallback, preserving
its behavior and allocation.  The record format uses CRC32C (Castagnoli)."
  (let ((table (and (boundp '*wal-crc32-table*) *wal-crc32-table*)))
    (when (and (typep bytes '(simple-array (unsigned-byte 8) (*)))
               (>= (length bytes) +wal-header-size+)
               (typep table '(simple-array (unsigned-byte 32) (256))))
      (let ((crc (logxor #xffffffff (%wal-crc32-octets bytes 0 28 table))))
        (declare (type (unsigned-byte 32) crc))
        (loop repeat 4
              do (setf crc (logxor (ash crc -8) (aref table (logand crc #xff)))))
        (return-from wal-record-crc32
          (wal-crc32-typed-range bytes +wal-header-size+ (length bytes) table crc)))))
  (let ((copy (copy-seq bytes)))
    (wal-set-u32 copy 28 0)
    (wal-crc32 copy)))

(defun wal-decode-record (bytes expected-lsn)
  "Validate the record header and checksum at EXPECTED-LSN and decode it.
The returned record owns a separate payload subsequence of BYTES."
  (let ((version (and (>= (length bytes) +wal-header-size+)
                      (wal-u16 bytes 4))))
    (unless (and (>= (length bytes) +wal-header-size+)
                 (= (aref bytes 0) (char-code #\P))
                 (= (aref bytes 1) (char-code #\G))
                 (= (aref bytes 2) (char-code #\W))
                 (= (aref bytes 3) (char-code #\L))
                 (member version (list +wal-version-v2+
                                       +wal-version-v3+
                                       +wal-version-v4+
                                       +wal-version-v5+
                                       +wal-version-v6+ +wal-version-v7+
                                       +wal-version+))
                 (= (wal-u32 bytes 8) (length bytes))
                 (= (wal-u64 bytes 12) expected-lsn)
                 (wal-valid-type-p (wal-u16 bytes 6) version))
      (error "invalid WAL record at LSN ~a" expected-lsn))
    (let ((saved-crc (wal-u32 bytes 28)))
      (unless (= saved-crc (wal-record-crc32 bytes))
        (error "WAL checksum mismatch at LSN ~a" expected-lsn)))
    (let* ((type (wal-u16 bytes 6))
           (xid (wal-u64 bytes 20))
           (payload (subseq bytes +wal-header-size+))
           (end (+ expected-lsn (length bytes))))
      (let ((record (make-wal-record :version version :type type :xid xid
                                     :start expected-lsn :end end :payload payload)))
        (when (= type +wal-record-end-of-recovery+)
          (wal-end-of-recovery-record-info record))
        record))))

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

(defun wal-apply-catalog-image (deletes catalog-bytes)
  (wal-atomic-write-bytes (merge-pathnames "global/pg_catalog.dat" *pgdata*)
                          catalog-bytes)
  (dolist (relfilenode deletes)
    (smgr-forget relfilenode :delete t))
  (when deletes (fsync-directory (merge-pathnames "base/" *pgdata*))))

(defun wal-prepare-catalog-record (payload)
  "Validate a catalog payload and return the operation that publishes it."
  (multiple-value-bind (deletes catalog-bytes)
      (wal-decode-catalog-payload payload)
    (lambda () (wal-apply-catalog-image deletes catalog-bytes))))

(defun wal-recovery-close-page-spool ()
  (when *wal-recovery-page-spool*
    (close-logical-spool *wal-recovery-page-spool*)
    (unless (eq :resolved (query-temp-cleanup-owner-state
                           (logical-spool-owner *wal-recovery-page-spool*)))
      (error "recovery page spool cleanup failed"))
    (setf *wal-recovery-page-spool* nil))
  (values))

(defun wal-recovery-stage-page-record (record)
  "Validate one page record before retaining its bounded raw payload."
  (let* ((payload (wal-record-payload record))
         (size (length payload))
         (event (make-array (+ 12 size) :element-type '(unsigned-byte 8))))
    (wal-decode-page-images payload (wal-record-end record) (wal-record-version record))
    (wal-set-u32 event 0 (wal-record-version record))
    (wal-set-u64 event 4 (wal-record-end record))
    (replace event payload :start1 12)
    (unless *wal-recovery-page-spool*
      ;; Publish ownership before the first append can acquire a file.
      (sb-sys:without-interrupts
        (let ((*query-temp-file-limit-bytes* nil))
          (setf *wal-recovery-page-spool* (make-logical-spool)))))
    (logical-spool-append *wal-recovery-page-spool* event))
  (values))

(defun wal-recovery-map-page-spool (function)
  "Visit validated records, one at a time, in original WAL order."
  (when *wal-recovery-page-spool*
    (logical-spool-map-events
     *wal-recovery-page-spool*
     (lambda (event)
       (unless (>= (length event) 12) (error "invalid recovery page spool record"))
       (let ((version (wal-u32 event 0))
             (lsn (wal-u64 event 4)))
         (unless (<= +wal-version-v2+ version +wal-version+)
           (error "invalid recovery page spool version"))
         (funcall function (wal-decode-page-images (subseq event 12) lsn version))))))
  (values))

(defun wal-replay-finish-batch (terminal-preparer &optional catalog-chunks-p)
  (unless *wal-recovery-group-active*
    (error "WAL terminal record appears outside a batch"))
  (when *wal-recovery-logical-chunk-count*
    (error "logical WAL chunks appear before a non-commit terminal"))
  (when (and *wal-recovery-catalog-chunk-count*
             (not catalog-chunks-p))
    (error "WAL catalog chunks lack a catalog commit record"))
  ;; Validate the terminal and every spooled page before destructive resets.
  ;; Chunked catalog metadata must be reassembled and decoded first. Apply
  ;; resets before the batch's page images; an incomplete batch applies neither.
  (let ((terminal-function (funcall terminal-preparer)))
    (unless (functionp terminal-function)
      (error "WAL terminal validation did not produce an apply operation"))
    (when *wal-recovery-page-spool*
      (logical-spool-seal *wal-recovery-page-spool*)
      (wal-recovery-map-page-spool (lambda (images) (declare (ignore images)))))
    (wal-apply-relation-resets (mapcar #'cdr (reverse *wal-recovery-relation-resets*)))
    (if *wal-recovery-page-spool-enabled*
        (wal-recovery-map-page-spool #'wal-apply-page-images)
        (wal-apply-page-images (nreverse *wal-recovery-page-images*)))
    (funcall terminal-function))
  (wal-recovery-close-page-spool)
  (setf *wal-recovery-reservation-batch-clean-p* nil
        *wal-recovery-group-active* nil
        *wal-recovery-page-images* '()
        *wal-recovery-relation-resets* nil
        *wal-recovery-page-records-seen-p* nil
        *wal-recovery-catalog-chunks* '()
        *wal-recovery-catalog-chunk-count* nil
        *wal-recovery-catalog-next-chunk* 0)
  (wal-recovery-reset-logical-change-batch)
  (setf *wal-recovery-logical-catalog-event-p* nil)
  (values))

(defun wal-recovery-validate-logical-terminal (kind &optional xid)
  ;; A reset belongs only to a commit terminal for the same transaction.
  (when *wal-recovery-relation-resets*
    (unless (and (eq kind :commit)
                 (every (lambda (reset) (eql xid (car reset))) *wal-recovery-relation-resets*))
      (error "relation resets require their transaction's commit terminal")))
  (ecase kind
    (:commit
     (when *wal-recovery-logical-catalog-event-p*
       (error "logical catalog events are attached to a row commit"))
     (wal-recovery-finish-logical-change-batch xid))
    (:catalog
     (when *wal-recovery-logical-chunk-count*
       (error "logical row changes are attached to a catalog WAL batch")))
    (:other
     (when (or *wal-recovery-logical-catalog-event-p*
               *wal-recovery-logical-chunk-count*)
       (error "logical changes are attached to an unsupported WAL terminal"))))
  (values))

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

(defun wal-replay-empty-metadata-terminal (record)
  "Validate and finish an empty metadata batch.
Primary XID reservations restore their allocation floor; parameter facts are
published separately after primary redo completes."
  (wal-recovery-validate-logical-terminal :other)
  (let* ((decoded (wal-validate-empty-metadata-terminal
                   record *wal-recovery-reservation-batch-clean-p*))
         (apply-function
           (if (= (wal-record-type record) +wal-record-xid-reservation+)
               (lambda ()
                 (unless (recovery-in-progress-p) (xid-restore-reservation-floor decoded)))
               #'values)))
    (wal-replay-finish-batch (lambda () apply-function))))

(defun wal-replay-record (record)
  (when (> (wal-record-end record) *wal-checkpoint-lsn*)
    (wal-note-recovery-batch-record record)
    (cond
      ((= (wal-record-type record) +wal-record-batch-start+)
       ;; A prior group without a terminal record is a crash tail. Ignore its
       ;; accumulated images and begin staging the next post-restart batch.
       (wal-recovery-close-page-spool)
       (setf *wal-recovery-group-active* t
             *wal-recovery-page-images* '()
             *wal-recovery-relation-resets* nil
             *wal-recovery-page-records-seen-p* nil
             *wal-recovery-catalog-chunks* '()
             *wal-recovery-catalog-chunk-count* nil
             *wal-recovery-catalog-next-chunk* 0)
       (wal-recovery-reset-logical-change-batch)
       (setf *wal-recovery-logical-catalog-event-p* nil))
      ((= (wal-record-type record) +wal-record-relation-reset+)
       (wal-recovery-stage-relation-reset record))
      ((= (wal-record-type record) +wal-record-pages+)
       (unless *wal-recovery-group-active*
         (error "WAL page images appear outside a batch"))
       (when *wal-recovery-catalog-chunk-count*
         (error "WAL page images follow catalog chunks in a batch"))
       (setf *wal-recovery-page-records-seen-p* t)
       (if *wal-recovery-page-spool-enabled*
           (wal-recovery-stage-page-record record)
           (setf *wal-recovery-page-images*
                 (nconc (reverse
                         (wal-decode-page-images (wal-record-payload record)
                                                 (wal-record-end record)
                                                 (wal-record-version record)))
                        *wal-recovery-page-images*))))
      ((= (wal-record-type record) +wal-record-logical-changes+)
       (when *wal-recovery-logical-catalog-event-p*
         (error "logical row changes follow a catalog event in the same batch"))
       (when *wal-recovery-catalog-chunk-count*
         (error "logical WAL changes follow catalog chunks in a batch"))
       (wal-recovery-accept-logical-change-chunk record))
      ((= (wal-record-type record) +wal-record-logical-catalog-event+)
       (unless *wal-recovery-group-active*
         (error "logical catalog event appears outside a WAL batch"))
       (when (/= (wal-record-xid record) 0)
         (error "logical catalog event must not carry a transaction ID"))
       (when (or *wal-recovery-logical-catalog-event-p*
                 *wal-recovery-logical-chunk-count*)
         (error "logical catalog event is mixed with other changes in a WAL batch"))
       (setf *wal-recovery-logical-catalog-event-p* t)
       (let ((payload (wal-record-payload record)))
         (unless (and (>= (length payload) 8)
                      (plusp (wal-u32 payload 0))
                      (= (+ 8 (wal-u32 payload 4)) (length payload)))
           (error "logical catalog event has an invalid payload"))))
      ((= (wal-record-type record) +wal-record-commit+)
       (wal-decode-commit-timestamp (wal-record-payload record))
       (wal-recovery-validate-logical-terminal
        :commit (wal-record-xid record))
       (wal-replay-finish-batch
        (lambda ()
          (lambda ()
            (clog-set (wal-record-xid record) 1)))))
      ((= (wal-record-type record) +wal-record-catalog+)
       (wal-recovery-validate-logical-terminal :catalog)
       (wal-replay-finish-batch
        (lambda ()
          (wal-prepare-catalog-record (wal-record-payload record)))))
      ((= (wal-record-type record) +wal-record-catalog-chunk+)
       (unless *wal-recovery-group-active*
         (error "WAL catalog chunks appear outside a batch"))
       (when (or *wal-recovery-logical-catalog-event-p*
                 *wal-recovery-logical-chunk-count*)
         (error "WAL catalog chunks are mixed with row or catalog events"))
       (multiple-value-bind (index count bytes)
           (wal-decode-catalog-chunk-payload (wal-record-payload record))
         (unless (= index *wal-recovery-catalog-next-chunk*)
           (error "WAL catalog chunk index ~a follows chunk ~a"
                  index *wal-recovery-catalog-next-chunk*))
         (when (and *wal-recovery-catalog-chunk-count*
                    (/= count *wal-recovery-catalog-chunk-count*))
           (error "WAL catalog chunk count changes within a batch"))
         (setf *wal-recovery-catalog-chunk-count* count)
         (push bytes *wal-recovery-catalog-chunks*)
         (incf *wal-recovery-catalog-next-chunk*)))
      ((= (wal-record-type record) +wal-record-catalog-commit+)
       (wal-recovery-validate-logical-terminal :catalog)
       (multiple-value-bind (count length)
           (wal-decode-catalog-commit-payload (wal-record-payload record))
         (wal-replay-finish-batch
          (lambda ()
            (wal-prepare-catalog-record
             (wal-reassemble-catalog-chunks count length)))
          t)))
      ((= (wal-record-type record) +wal-record-checkpoint+)
       (wal-recovery-validate-logical-terminal :other)
       (wal-checkpoint-record-snapshot record)
       (wal-replay-finish-batch (lambda () #'values)))
      ((wal-empty-metadata-terminal-type-p (wal-record-type record))
       (wal-replay-empty-metadata-terminal record))
      ((= (wal-record-type record) +wal-record-maintenance+)
       (wal-recovery-validate-logical-terminal :other)
       (unless (zerop (wal-record-xid record))
         (error "WAL maintenance record must not carry a transaction ID"))
       (unless (zerop (length (wal-record-payload record)))
         (error "WAL maintenance record has an invalid payload"))
       (wal-replay-finish-batch (lambda () #'values)))
      ((= (wal-record-type record) +wal-record-clog-reset+)
       (wal-recovery-validate-logical-terminal :other)
       (wal-read-u64-payload (wal-record-payload record))
       (wal-replay-finish-batch
        (lambda () (lambda () (clog-reset :wal-reset-p nil))))))))

(defmacro with-wal-replay-staging (() &body body)
  "Isolate one native batch decoder; primary redo and history share this state."
  `(let ((*wal-recovery-reservation-batch-clean-p* nil)
         (*wal-recovery-group-active* nil)
         (*wal-recovery-page-images* '())
         (*wal-recovery-relation-resets* nil)
         (*wal-recovery-page-records-seen-p* nil)
         (*wal-recovery-page-spool* nil)
         (*wal-recovery-catalog-chunks* '())
         (*wal-recovery-catalog-chunk-count* nil)
         (*wal-recovery-catalog-next-chunk* 0)
         (*wal-recovery-logical-xid* nil)
         (*wal-recovery-logical-chunk-count* nil)
         (*wal-recovery-logical-next-chunk* 0)
         (*wal-recovery-logical-bytes* 0)
         (*wal-recovery-logical-catalog-event-p* nil))
     (unwind-protect
          (progn ,@body)
       (unwind-protect (wal-recovery-close-page-spool)
         (wal-recovery-reset-logical-change-batch)))))

(defun wal-recovery-policy-incarnation (state incarnation record completed-p)
  "Carry private FPW evidence across a completed CLOG reset's identity change.
The caller supplies COMPLETED-P only after a validated terminal closes its batch.
A completed type-5 reset must rotate storage; any other rotation is refused.
Return the rebound evidence and current token, or the unchanged pair."
  (let ((current *wal-storage-incarnation*))
    (cond
      ((and completed-p (= (wal-record-type record) +wal-record-clog-reset+))
       (when (eq current incarnation)
         (error "completed CLOG reset did not replace its storage identity"))
       (values (wal-fpw-rebind-incarnation state incarnation current) current))
      ((not (eq current incarnation))
       (error "WAL recovery storage changed outside a completed CLOG reset"))
      (t (values state incarnation)))))

(defun wal-recover ()
  "Redo durable complete batches before loading the catalog.
Validate logical status privately before each new terminal's redo. Carry private
FPW evidence to a new token only after a validated CLOG reset terminal closes its
batch; reject other storage rotations. Publish FPW and logical status only after
the full scan and CLOG sync succeed. The caller must exclude storage lifecycle
changes throughout."
  (let* ((incarnation *wal-storage-incarnation*)
         (fpw-state (wal-fpw-state-from-checkpoint *wal-checkpoint-snapshot* incarnation))
         (logical-status (wal-logical-status-from-checkpoint *wal-checkpoint-snapshot*))
         (*wal-recovery-page-spool-enabled* t)
         (resource-record nil))
    (with-wal-replay-staging ()
      (wal-scan
       (lambda (record)
         (unless (eq incarnation *wal-storage-incarnation*)
           (error "WAL recovery storage changed before replay"))
         (let* ((new-p (> (wal-record-end record) *wal-checkpoint-lsn*))
                (terminal-p (wal-terminal-record-type-p (wal-record-type record)))
                (next-logical-status
                  (if (and new-p terminal-p)
                      (wal-logical-status-next logical-status record *wal-insert-timeline*)
                      logical-status)))
           (wal-replay-record record)
           (multiple-value-setq (fpw-state incarnation)
             (wal-recovery-policy-incarnation
              fpw-state incarnation record
              (and new-p terminal-p (not *wal-recovery-group-active*))))
           (when new-p
             (when (= (wal-record-type record) +wal-record-parameter-change+)
               (setf resource-record record))
             (when (and terminal-p (not *wal-recovery-group-active*))
               (setf fpw-state (wal-fpw-state-next
                                fpw-state record *wal-insert-timeline* incarnation)
                     logical-status next-logical-status)))))))
    (when resource-record
      (with-wal-critical (wal-recover-resource-parameters resource-record)))
    (clog-fsync)
    (with-wal-critical
      (wal-fpw-install-recovered-state fpw-state incarnation)
      (wal-install-recovered-logical-policy logical-status incarnation)))
  (values))

(defun wal-atomic-write-bytes (path bytes)
  (with-atomic-output (stream
                       path
                       (make-pathname :type "tmp" :defaults path)
                       (make-pathname :name nil :type nil :defaults path)
                       :element-type '(unsigned-byte 8))
    (write-sequence bytes stream)))

(defun wal-slot-retention-mode (policy)
  "Return POLICY's mode."
  (wal-slot-retention-policy-mode policy))

(defun wal-slot-retention-limit (policy)
  "Return POLICY's byte limit, or NIL if the cap is disabled."
  (wal-slot-retention-policy-limit policy))

(defun wal-slot-reserved-p (slot)
  "Return the healthy internal restart floor, if any.
This is distinct from the advisory public reserved, extended or unreserved status."
  (and (eq :reserved (wal-replication-slot-wal-status slot))
       (wal-replication-slot-restart-lsn slot)))

(defun wal-keep-retention-segment (current keep-mb)
  "Return the wal_keep_size floor at CURRENT, rounding down to whole segments.
Native WAL starts at segment zero, so clamp there rather than C's segment one."
  (max 0 (- (floor current +wal-segment-size+)
            (floor (* keep-mb 1024 1024) +wal-segment-size+))))

(defun wal-independent-retention-segment (redo-segment current keep-mb)
  "Combine redo, wal_keep_size, active XID and snapshot floors independent of slots.
The caller holds WAL; take the XID lock to copy active positions. CURRENT is the
checkpoint endpoint used for both the initial calculation and later rechecks."
  (let ((positions
          (append
           (sb-thread:with-mutex (*xid-lock*)
             (loop for lsn being the hash-values of *wal-xid-first-lsn* collect lsn))
           (loop for lsn being the hash-values of *wal-snapshot-retention-pins* collect lsn))))
    (min redo-segment
         (if (plusp keep-mb) (wal-keep-retention-segment current keep-mb) redo-segment)
         (if positions (floor (reduce #'min positions) +wal-segment-size+) redo-segment))))

(defun wal-slot-removal-segment (redo-segment independent-segment current policy)
  "Combine independent retention with reserved slot floors under the WAL lock.
The standard policy rounds its limit down to whole WAL segments."
  (let* ((positions (loop for slot being the hash-values of *wal-replication-slots*
                          when (wal-slot-reserved-p slot)
                            collect (wal-replication-slot-restart-lsn slot)))
         (limit (wal-slot-retention-limit policy))
         (slot-segment (when positions (floor (reduce #'min positions) +wal-segment-size+))))
    (when (and slot-segment limit (eq :segments (wal-slot-retention-mode policy)))
      (setf slot-segment
            (max slot-segment (- (floor current +wal-segment-size+)
                                 (floor limit +wal-segment-size+)))))
    (min redo-segment independent-segment (or slot-segment redo-segment))))

(declaim (ftype function physical-slot-recovery-write-lsn))

(declaim (ftype function physical-slot-recovery-durable-lsn))

(defun wal-slot-safe-wal-bytes (slot policy keep-mb)
  "Report remaining safe bytes from the write endpoint, which can be negative.
For the standard segment policy use the greater of slot cap and wal_keep_size,
plus one segment. The native byte policy retains its own exact-byte calculation.
The caller holds WAL and supplies POLICY and KEEP-MB from one configuration."
  (let* ((restart (wal-replication-slot-restart-lsn slot))
         (limit (wal-slot-retention-limit policy))
         (mode (wal-slot-retention-mode policy))
         (write (if (recovery-in-progress-p) (physical-slot-recovery-write-lsn) *wal-end-lsn*)))
    (if (or (null limit) (eq :lost (wal-replication-slot-wal-status slot))
            (and (eq mode :bytes) (null restart)))
        +null+
        (- (ecase mode
             (:segments (* (+ (floor (or restart 0) +wal-segment-size+)
                              (max (floor limit +wal-segment-size+)
                                   (floor (* keep-mb 1024 1024) +wal-segment-size+)) 1)
                           +wal-segment-size+))
             (:bytes (+ restart limit 1)))
           write))))

(defun wal-slot-byte-limit-exceeded-p (slot current limit)
  "Test the native byte cap at CURRENT; the caller chooses write or durable WAL."
  (and (integerp limit)
       (> (- current (wal-replication-slot-restart-lsn slot)) limit)))

(defun wal-slot-within-normal-window-p (restart write policy)
  "Test RESTART against the normal max_wal_size window in POLICY."
  (let* ((maximum (wal-slot-retention-policy-max-wal-bytes policy))
         (oldest (max 0 (- (floor write +wal-segment-size+)
                           (1+ (floor maximum +wal-segment-size+))))))
    (>= (floor restart +wal-segment-size+) oldest)))

(defun wal-slot-catalog-status (slot policy keep-mb)
  "Compute advisory public WAL status at the write endpoint; do not invalidate.
The standard horizon includes wal_keep_size but excludes XID and snapshot floors,
as in C's metadata calculation. Lost slots remain lost. The caller holds WAL
and supplies POLICY and KEEP-MB from the same captured configuration."
  (when (eq :lost (wal-replication-slot-wal-status slot))
    (return-from wal-slot-catalog-status "lost"))
  (let ((restart (wal-replication-slot-restart-lsn slot)))
    (unless restart (return-from wal-slot-catalog-status +null+))
    (let* ((write (if (recovery-in-progress-p) (physical-slot-recovery-write-lsn) *wal-end-lsn*))
           (current (floor write +wal-segment-size+))
           (unreserved
             (ecase (wal-slot-retention-mode policy)
               (:bytes (wal-slot-byte-limit-exceeded-p slot write (wal-slot-retention-policy-limit policy)))
               (:segments (< (floor restart +wal-segment-size+)
                             (wal-slot-removal-segment current (wal-keep-retention-segment write keep-mb)
                                                       write policy))))))
      (cond (unreserved "unreserved")
            ((wal-slot-within-normal-window-p restart write policy) "reserved")
            (t "extended")))))

(defun wal-valid-replication-slot-name-p (name)
  (slot-state-valid-name-p name))

(declaim (ftype function physical-slot-encode physical-slot-decode physical-slot-image-p))

(defun wal-encode-replication-slot (slot)
  "Encode using the slot's explicit kind; reject unknown kinds."
  (when (eq :physical (wal-replication-slot-kind slot))
    (return-from wal-encode-replication-slot (physical-slot-encode slot)))
  (unless (wal-logical-slot-p slot)
    (error "unknown WAL replication slot kind ~s" (wal-replication-slot-kind slot)))
  (let* ((name (wal-replication-slot-name slot))
         (database-oid (wal-replication-slot-database-oid slot))
         (restart-lsn (wal-replication-slot-restart-lsn slot))
         (consistent-lsn (wal-replication-slot-consistent-lsn slot))
         (confirmed-flush-lsn (wal-replication-slot-confirmed-flush-lsn slot))
         (wal-status (wal-replication-slot-wal-status slot))
         (plugin (wal-replication-slot-plugin slot))
         (profile (wal-slot-profile slot)))
    (unless (and (wal-valid-replication-slot-name-p name)
                 (wal-valid-slot-plugin-state-p plugin database-oid confirmed-flush-lsn)
                 (or (null database-oid)
                     (and (integerp database-oid) (<= 1 database-oid #xffffffff)))
                 (integerp restart-lsn) (<= 0 restart-lsn #xffffffffffffffff)
                 (integerp consistent-lsn) (<= 0 consistent-lsn #xffffffffffffffff)
                 (or (null confirmed-flush-lsn)
                     (and (integerp confirmed-flush-lsn)
                          (<= consistent-lsn confirmed-flush-lsn #xffffffffffffffff)
                          (<= restart-lsn confirmed-flush-lsn)))
                 (or confirmed-flush-lsn (<= restart-lsn consistent-lsn))
                 (member wal-status '(:reserved :lost))
                 (member profile '(:text :binary-bytea))
                 (or (eq profile :text) (and database-oid (string= plugin "pg_lisp"))))
      (error "invalid WAL replication slot state ~s" slot))
    (let* ((prefix (format nil "~a ~a ~a ~a ~a ~a ~a ~a~a"
                           (if (eq profile :text) "PGSLOT4" "PGSLOT5")
                           name (or database-oid "-") restart-lsn consistent-lsn
                           (or confirmed-flush-lsn "-")
                           (string-downcase (symbol-name wal-status)) plugin
                           (if (eq profile :text) "" " binary-bytea")))
           (crc (wal-crc32 (string->bytes prefix))))
      (string->bytes (format nil "~a ~8,'0X~%" prefix crc)))))

(defun wal-finish-replication-slot-creation-under-lock (slot receipt completed)
  ;; Remove only our unpublished candidate. An uncertain or published failure
  ;; keeps its WAL pin and is fenced until startup reads fresh durable state.
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (when (and (not completed)
             (eq slot (gethash (wal-replication-slot-name slot) *wal-replication-slots*)))
    (if (eq (car receipt) :unpublished)
        (remhash (wal-replication-slot-name slot) *wal-replication-slots*)
        (setf (wal-replication-slot-creation-failed-p slot) t
              (wal-replication-slot-active-owner slot) nil
              (wal-replication-slot-active-owner-backend slot) nil)))
  (values))

(defun wal-create-replication-slot-file (slot &optional publication-receipt)
  ;; CREATE publishes a staged directory without replacing an existing name.
  (wal-require-usable-replication-slot slot)
  (slot-state-create (wal-replication-slot-directory)
                     (wal-replication-slot-name slot)
                     (wal-encode-replication-slot slot)
                     :publication-receipt publication-receipt)
  (values))

(defun wal-save-replication-slot (slot)
  ;; UPDATE requires an existing directory and cannot recreate a dropped slot.
  (wal-require-usable-replication-slot slot)
  (slot-state-update (wal-replication-slot-directory)
                     (wal-replication-slot-name slot)
                     (wal-encode-replication-slot slot))
  (values))

(defun wal-delete-replication-slot-file (name &optional expected-slot)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((slot (gethash name *wal-replication-slots*)))
    (unless (and slot (or (null expected-slot) (eq slot expected-slot)))
      (error "replication slot deletion requires its exact registered object"))
    (wal-require-known-replication-slot slot)
    ;; Save the numeric directory identities before removal. On failure the
    ;; claim and WAL pin remain, and only this generation may retry the drop.
    (unless (wal-replication-slot-drop-claim slot)
      (setf (wal-replication-slot-drop-claim slot)
            (slot-state-claim-drop (wal-replication-slot-directory) name)))
    (slot-state-drop (wal-replication-slot-directory) name
                     (wal-replication-slot-drop-claim slot)))
  (values))

(defun wal-split-fields (line)
  (loop with start = 0
        for end = (position #\Space line :start start)
        collect (subseq line start end)
        while end
        do (setf start (1+ end))))

(defun wal-decode-replication-slot (bytes expected-name)
  ;; Decode a fresh object; process-local creation and drop fences are not stored.
  (unless (and (typep bytes '(vector (unsigned-byte 8))) (<= 1 (length bytes) 2048))
    (error "invalid replication slot state bytes"))
  (when (physical-slot-image-p bytes)
    (return-from wal-decode-replication-slot
      (physical-slot-decode bytes expected-name)))
  (with-input-from-string (stream (bytes->string bytes))
    (let* ((line (read-line stream nil nil))
           (fields (and line (wal-split-fields line)))
           (version (first fields))
           (legacy-v1-p (equal version "PGSLOT1"))
           (legacy-v2-p (equal version "PGSLOT2"))
           (v3-p (equal version "PGSLOT3"))
           (v4-p (equal version "PGSLOT4"))
           (v5-p (equal version "PGSLOT5"))
           (expected-count (cond (legacy-v1-p 5) (legacy-v2-p 7) (v3-p 8) (v4-p 9) (v5-p 10)))
           (database-index (when (or v3-p v4-p v5-p) 2))
           (restart-index (if database-index 3 2))
           (consistent-index (1+ restart-index))
           (confirmed-index (unless legacy-v1-p (+ restart-index 2)))
           (status-index (unless legacy-v1-p (+ restart-index 3))))
      (unless (and expected-count (= (length fields) expected-count)
                   (every (lambda (field) (plusp (length field))) fields)
                   (null (read-line stream nil nil)))
        (error "replication slot file ~a is malformed" expected-name))
      (let* ((name (second fields))
             (database-text (when database-index (nth database-index fields)))
             (database-oid (when (and database-text (not (string= database-text "-")))
                             (ignore-errors (parse-integer database-text))))
             (database-valid-p (or (null database-index) (string= database-text "-")
                                   (and database-oid (<= 1 database-oid #xffffffff))))
             (restart-lsn (ignore-errors (parse-integer (nth restart-index fields))))
             (consistent-lsn (ignore-errors (parse-integer (nth consistent-index fields))))
             (confirmed-text (when confirmed-index (nth confirmed-index fields)))
             (confirmed-flush-lsn (when (and confirmed-text (not (string= confirmed-text "-")))
                                    (ignore-errors (parse-integer confirmed-text))))
             (status-text (if legacy-v1-p "reserved" (nth status-index fields)))
             (wal-status (cond ((string= status-text "reserved") :reserved)
                               ((string= status-text "lost") :lost)))
             ;; Every older format was the private extension protocol.
             (plugin (if (or v4-p v5-p) (nth 7 fields) "pg_lisp"))
             (profile (if v5-p (nth 8 fields) "text"))
             (prefix (format nil "~{~a~^ ~}" (butlast fields)))
             (crc (ignore-errors (parse-integer (car (last fields)) :radix 16))))
        (unless (and (wal-valid-replication-slot-name-p name)
                     (string= name expected-name)
                     restart-lsn consistent-lsn crc wal-status database-valid-p
                     (or legacy-v1-p (string= confirmed-text "-") confirmed-flush-lsn)
                     (wal-valid-slot-plugin-state-p plugin database-oid confirmed-flush-lsn)
                     (or (and (not v5-p) (string= profile "text"))
                         (and v5-p database-oid (string= profile "binary-bytea") (string= plugin "pg_lisp")))
                     (<= 0 restart-lsn *wal-durable-lsn* #xffffffffffffffff)
                     (<= 0 consistent-lsn *wal-durable-lsn* #xffffffffffffffff)
                     (or (null confirmed-flush-lsn)
                         (and (<= consistent-lsn confirmed-flush-lsn *wal-durable-lsn*)
                              (<= restart-lsn confirmed-flush-lsn)))
                     (or confirmed-flush-lsn (<= restart-lsn consistent-lsn))
                     (= crc (wal-crc32 (string->bytes prefix))))
          (error "replication slot file ~a is corrupt or outside durable WAL" expected-name))
        (let ((slot (make-wal-replication-slot
                     :name name :database-oid database-oid :restart-lsn restart-lsn
                     :consistent-lsn consistent-lsn :confirmed-flush-lsn confirmed-flush-lsn
                     :wal-status wal-status :plugin plugin)))
          (when v5-p (wal-slot-set-initial-profile slot :binary-bytea))
          slot)))))

(defun wal-read-replication-slot (path &key stabilize)
  ;; Canonical state is read through the held slot directory descriptor.
  (if (equal (file-namestring path) "state")
      (let* ((directory (uiop:pathname-directory-pathname path))
             (name (car (last (pathname-directory directory))))
             (root (uiop:pathname-parent-directory-pathname directory)))
        (slot-state-read root name #'wal-decode-replication-slot :stabilize stabilize))
      (wal-decode-replication-slot
       (read-bounded-regular-file path 2048 :stabilize stabilize)
       (pathname-name path))))

(defun wal-validate-replication-slot-wal (slot segments)
  (let ((restart-lsn (wal-replication-slot-restart-lsn slot)))
    ;; A slot at the current durable end does not require a segment yet. Any
    ;; earlier restart point requires every segment through the durable tail.
    (when (and (eq (wal-replication-slot-wal-status slot) :reserved)
               (< restart-lsn *wal-durable-lsn*))
      (let ((first (floor restart-lsn +wal-segment-size+))
            (last (floor (1- *wal-durable-lsn*) +wal-segment-size+)))
        (loop for segment from first to last
              unless (member segment segments :test #'=)
                do (error "WAL required by replication slot ~a is missing segment ~a"
                          (wal-replication-slot-name slot) segment)))))
  slot)

(defun wal-terminal-record-type-p (type)
  (member type (list +wal-record-commit+ +wal-record-catalog+
                     +wal-record-catalog-commit+ +wal-record-checkpoint+
                     +wal-record-maintenance+ +wal-record-clog-reset+ +wal-record-xid-reservation+
                     +wal-record-running-xacts+ +wal-record-parameter-change+
                     +wal-record-full-page-writes-change+ +wal-record-logical-decoding-status-change+)
          :test #'=))

(defun wal-slot-terminal-ack-p (slot target-lsn)
  "Check a terminal acknowledgment, validating metadata batches without applying them."
  (let ((*wal-recovery-reservation-batch-clean-p* nil)
        (batch-open nil)
        (found nil)
        (scan-from-lsn (or (wal-replication-slot-confirmed-flush-lsn slot)
                           (wal-replication-slot-consistent-lsn slot)
                           (wal-replication-slot-restart-lsn slot))))
    (wal-scan
     (lambda (record)
       (wal-note-recovery-batch-record record)
       (when (wal-empty-metadata-terminal-type-p (wal-record-type record))
         (wal-validate-empty-metadata-terminal
          record (and batch-open *wal-recovery-reservation-batch-clean-p*)))
       (cond
         ((= (wal-record-type record) +wal-record-batch-start+)
          (setf batch-open t))
         (t
          (when (and batch-open
                     (wal-terminal-record-type-p (wal-record-type record)))
            (when (= (wal-record-end record) target-lsn)
              (setf found t))
            (setf batch-open nil *wal-recovery-reservation-batch-clean-p* nil))))
       (when (or found (>= (wal-record-end record) target-lsn))
         :stop))
     ;; LogicalConfirmReceivedLocation() in src/backend/replication/logical/logical.c
     ;; advances confirmed_flush monotonically. Validate forward from that point,
     ;; even when an older in-flight XID keeps restart_lsn behind it.
     scan-from-lsn)
    found))

(defun wal-slot-rebuild-progress (slot)
  "Rebuild pending transaction WAL from retained records after restart."
  (unless (wal-logical-slot-p slot) (return-from wal-slot-rebuild-progress slot))
  (let ((first-lsns (make-hash-table :test #'eql))
        (committed (make-hash-table :test #'eql))
        (legacy-first-lsn nil)
        (batch-start-lsn nil)
        (pending (make-hash-table :test #'eql)))
    (unless (or (eq (wal-replication-slot-wal-status slot) :lost)
                (and (wal-replication-slot-confirmed-flush-lsn slot)
                     (>= (wal-replication-slot-confirmed-flush-lsn slot)
                         *wal-durable-lsn*)))
      (wal-scan
       (lambda (record)
         (cond
           ((= (wal-record-type record) +wal-record-batch-start+)
            (setf batch-start-lsn (wal-record-start record)))
           ((= (wal-record-type record) +wal-record-pages+)
            (let ((images (wal-decode-page-images
                           (wal-record-payload record) (wal-record-end record)
                           (wal-record-version record))))
              (if (>= (wal-record-version record) +wal-version-v4+)
                  (dolist (image images)
                    (dolist (xid (wal-page-image-xids image))
                      (unless (gethash xid first-lsns)
                        (setf (gethash xid first-lsns) batch-start-lsn))))
                  (when (and batch-start-lsn
                             (null legacy-first-lsn))
                    (setf legacy-first-lsn batch-start-lsn)))))
           ((= (wal-record-type record) +wal-record-commit+)
            (let* ((xid (wal-record-xid record))
                   (first-lsn (or (gethash xid first-lsns)
                                  legacy-first-lsn)))
              (when (and (plusp xid) first-lsn)
                (setf (gethash xid committed)
                      (cons first-lsn (wal-record-end record))))))))
       (wal-replication-slot-restart-lsn slot))
      (maphash
       (lambda (xid entry)
         (let ((confirmed (wal-replication-slot-confirmed-flush-lsn slot)))
           (when (or (null confirmed) (> (cdr entry) confirmed))
             (setf (gethash xid pending) entry))))
       committed)
      (setf (wal-replication-slot-pending-xids slot) pending))
    slot))

(defun wal-slot-pending-after-ack (slot acknowledged-lsn)
  (let ((pending (make-hash-table :test #'eql)))
    (maphash (lambda (xid entry)
               (let ((commit-lsn (cdr entry)))
                 (unless (or (and commit-lsn (<= commit-lsn acknowledged-lsn))
                             ;; CLOG stores the aborted state as the two-bit
                             ;; value 2 (see access/transam/clog.lisp).
                             (= (clog-get xid) 2))
                   (setf (gethash xid pending) (cons (car entry) commit-lsn)))))
             (wal-replication-slot-pending-xids slot))
    pending))

(defun wal-slot-restart-for-ack (acknowledged-lsn pending)
  (let ((restart-lsn acknowledged-lsn))
    (maphash (lambda (xid entry)
               (declare (ignore xid))
               (setf restart-lsn (min restart-lsn (car entry))))
             pending)
    restart-lsn))

(defun wal-persist-replication-slot-ack-under-lock (slot acknowledged-lsn
                                                    &optional (restart-floor acknowledged-lsn))
  "Persist an already-validated slot acknowledgment while *WAL-LOCK* is held."
  (wal-require-logical-slot slot)
  (let* ((pending (wal-slot-pending-after-ack slot acknowledged-lsn))
         (restart-lsn (wal-slot-restart-for-ack restart-floor pending))
         (candidate (wal-copy-replication-slot-with-profile slot)))
    (setf (wal-replication-slot-confirmed-flush-lsn candidate) acknowledged-lsn
          (wal-replication-slot-restart-lsn candidate) restart-lsn
          (wal-replication-slot-pending-xids candidate) pending)
    ;; Persist first: checkpoint recycling cannot observe the new horizon until
    ;; the durable slot file records the consumer's acknowledgment.
    (wal-save-replication-slot candidate)
    (setf (wal-replication-slot-confirmed-flush-lsn slot) acknowledged-lsn
          (wal-replication-slot-restart-lsn slot) restart-lsn
          (wal-replication-slot-pending-xids slot) pending)
    slot))

(defun wal-acknowledge-replication-slot
    (name acknowledged-lsn &optional expected-owner)
  "Durably advance a slot after a consumer completes a terminal WAL batch."
  (unless (and (integerp acknowledged-lsn)
               (<= 0 acknowledged-lsn #xFFFFFFFFFFFFFFFF))
    (ereport "22023" "invalid replication slot acknowledgment LSN"))
  (sb-thread:with-mutex (*wal-lock*)
    (wal-check-healthy)
    (let* ((slot (wal-find-usable-replication-slot-under-lock name))
           (confirmed (and slot
                           (wal-replication-slot-confirmed-flush-lsn slot))))
      (unless slot
        (ereport "42704" "replication slot \"~a\" does not exist" name))
      (wal-require-logical-slot slot)
      (when (and expected-owner
                 (not (eq (wal-replication-slot-active-owner slot)
                          expected-owner)))
        (ereport "55006" "replication slot \"~a\" is not owned by this reader"
                 name))
      (when (and (wal-replication-slot-active-owner slot)
                 (not (eq (wal-replication-slot-active-owner slot)
                          *wal-logical-slot-owner*)))
        (ereport "55006" "replication slot \"~a\" is active" name))
      (when (eq (wal-replication-slot-wal-status slot) :lost)
        (ereport "55000" "replication slot \"~a\" has lost its WAL" name))
      ;; Initial copy completion has a separate owner-checked finalizer below.
      ;; Keeping it out of this general path prevents an unowned client from
      ;; advancing a snapshot slot after its runtime owner was released.
      (unless confirmed
        (ereport "55000" "initial replication slot acknowledgment requires its owner"))
      (when (and confirmed (< acknowledged-lsn confirmed))
        (ereport "22023" "replication slot acknowledgment cannot move backwards"))
      (when (> acknowledged-lsn *wal-durable-lsn*)
        (ereport "22023" "replication slot acknowledgment is past durable WAL"))
      (when (= (or confirmed -1) acknowledged-lsn)
        (return-from wal-acknowledge-replication-slot slot))
      (unless (wal-slot-terminal-ack-p slot acknowledged-lsn)
        (ereport "22023"
                 "replication slot acknowledgment must name a terminal WAL batch LSN"))
      (wal-persist-replication-slot-ack-under-lock slot acknowledged-lsn))))

(defun wal-finalize-replication-slot-copy (name boundary-lsn expected-owner)
  "Confirm an initial snapshot boundary for its exact active slot owner."
  (unless (and (integerp boundary-lsn)
               (<= 0 boundary-lsn #xFFFFFFFFFFFFFFFF))
    (ereport "22023" "invalid initial replication slot boundary LSN"))
  (sb-thread:with-mutex (*wal-lock*)
    (wal-check-healthy)
    (let ((slot (wal-find-usable-replication-slot-under-lock name)))
      (unless slot
        (ereport "42704" "replication slot \"~a\" does not exist" name))
      (wal-require-logical-slot slot)
      (unless (and expected-owner
                   (eq (wal-replication-slot-active-owner slot) expected-owner))
        (ereport "55006" "initial replication slot finalization requires its active owner"))
      (when (eq (wal-replication-slot-wal-status slot) :lost)
        (ereport "55000" "replication slot \"~a\" has lost its WAL" name))
      (when (wal-replication-slot-confirmed-flush-lsn slot)
        (ereport "55000" "replication slot initial copy is already finalized"))
      (unless (= boundary-lsn (wal-replication-slot-consistent-lsn slot))
        (ereport "22023" "initial replication acknowledgment must name the consistent LSN"))
      (when (> boundary-lsn *wal-durable-lsn*)
        (ereport "22023" "initial replication acknowledgment is past durable WAL"))
      ;; A snapshot boundary is captured under the writer gate and need not be
      ;; rediscovered by scanning a terminal record. Match SnapBuild's one-time
      ;; consistent-point confirmation while retaining exact owner validation.
      (wal-persist-replication-slot-ack-under-lock slot boundary-lsn))))

(defun wal-format-lsn (lsn)
  (format nil "~X/~8,'0X" (ash lsn -32) (logand lsn #xFFFFFFFF)))

(defun wal-replication-slot-rows ()
  "Return the private eight-column logical-only snapshot for existing consumers."
  (sb-thread:with-mutex (*wal-lock*)
    (loop for slot being the hash-values of *wal-replication-slots*
          when (wal-logical-slot-p slot)
            collect
            (vector (wal-replication-slot-name slot)
                    (if (wal-replication-slot-active-owner slot) t nil)
                    (wal-format-lsn (wal-replication-slot-restart-lsn slot))
                    (if (wal-replication-slot-confirmed-flush-lsn slot)
                        (wal-format-lsn
                         (wal-replication-slot-confirmed-flush-lsn slot))
                        +null+)
                    (if (eq (wal-replication-slot-wal-status slot) :lost)
                        0
                        (max 0 (- *wal-durable-lsn*
                                  (wal-replication-slot-restart-lsn slot))))
                    (string-downcase
                     (symbol-name (wal-replication-slot-wal-status slot)))
                    (wal-replication-slot-plugin slot)
                    (or (wal-replication-slot-database-oid slot) +null+)))))

(declaim (ftype function physical-slot-restore physical-slot-check-standby-slots physical-slot-startup-system-id))

(defun wal-load-replication-slots ()
  "Validate a complete startup inventory before publishing its registry.
Logical restoration during native recovery remains unsupported."
  (let ((segments (wal-discover-segments))
        (inactive-since (postgres-current-timestamp))
        (restored (make-hash-table :test #'equal))
        (candidates (slot-state-map (wal-replication-slot-directory)
                                    #'wal-decode-replication-slot :cleanup t)))
    (let ((*wal-replication-slots* restored))
      (dolist (slot candidates)
        (let ((name (wal-replication-slot-name slot)))
          (when (gethash name restored) (error "duplicate WAL replication slot ~a" name))
          (when (if (wal-physical-slot-p slot)
                    (physical-slot-restore slot inactive-since)
                    (progn
                      (when (recovery-in-progress-p)
                        (error "logical slot restoration during native recovery is unsupported"))
                      (wal-validate-replication-slot-wal slot segments)))
            (wal-check-replication-slot-capacity)
            (setf (gethash name restored) slot))))
      (when (and (recovery-in-progress-p) (plusp (hash-table-count restored)))
        (physical-slot-check-standby-slots
         (loop for slot being the hash-values of restored collect slot)
         (physical-slot-startup-system-id)))
      (maphash (lambda (name slot)
                 (declare (ignore name))
                 (wal-slot-rebuild-progress slot))
               restored))
    ;; Replace the registry only after every candidate has been checked.
    (setf *wal-replication-slots* restored))
  (values))

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
            (handler-case (wal-decode-control-line line)
              (error (condition)
                (error "WAL control file ~a is corrupt: ~a" path condition)))))
      (sb-posix:syscall-error (condition)
        (unless (= sb-posix:enoent (sb-posix:syscall-errno condition))
          (error condition))))))

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

(defun wal-save-checkpoint-control (lsn &optional snapshot)
  (when (> lsn *wal-durable-lsn*)
    (error "WAL checkpoint LSN ~a is past durable LSN ~a" lsn *wal-durable-lsn*))
  ;; WAL-FLUSH retains the old copy. Publish a candidate only after page/CLOG sync.
  (let ((*wal-checkpoint-snapshot* snapshot)
        (*wal-checkpoint-timeline* (if snapshot (getf snapshot :timeline) *wal-insert-timeline*)))
    (wal-save-control-fields lsn *wal-durable-lsn*))
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
          *wal-segment-namespace* (or namespace :legacy)
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
              (wal-save-checkpoint-control checkpoint-end snapshot)
              (wal-fpw-note-published-checkpoint snapshot)
              (wal-logical-note-published-checkpoint snapshot)
              (wal-test-pause :after-checkpoint-control)
              (wal-remove-checkpointed-segments-under-wal-lock checkpoint-end)))))))
  (values))

(declaim (ftype function wal-prepare-slot-recycling-under-lock))

(defun wal-checkpoint-recycling-segment (checkpoint-lsn)
  "Return the segment containing the checkpoint's last byte; zero for LSN zero."
  (if (zerop checkpoint-lsn) 0 (floor (1- checkpoint-lsn) +wal-segment-size+)))

(defun wal-remove-checkpointed-segments-under-wal-lock (checkpoint-lsn &optional (current *wal-durable-lsn*))
  "Plan and remove a contiguous WAL prefix under WAL exclusion.
Active standard-policy owners defer removal in a process-local ticket; the caller
retries outside locks after stopping them. Independent XID and snapshot floors
also constrain removal. CURRENT freezes the endpoint across that retry, while
each calculation captures the current retention settings."
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
      (let ((removed nil))
        (dolist (segment (wal-discover-segments))
          (when (< segment keep-segment)
            (when (and *wal-output-stream* (eql segment *wal-output-segment*))
              (close *wal-output-stream*)
              (setf *wal-output-stream* nil *wal-output-segment* nil))
            (handler-case
                (progn
                  (delete-file (wal-segment-path segment))
                  (setf removed t))
              (file-error () (return)))))
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
