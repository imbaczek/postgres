;;;; logical-retention.lisp -- runtime cleanup after slot invalidation.

(in-package #:pg)

;;; Standard slot recycling stops active owners before saving loss and removing WAL.
;;; Consumer cleanup also retires logical snapshot exports and importers.

(defstruct wal-slot-recycling-work
  "Process-local recycling ticket and captured (slot owner backend) requests.
The registry identity and storage context fence retries after storage replacement
or role changes. DURABLE-LSN freezes the checkpoint's current endpoint for the removal horizon."
  slots directory system-id control-system-id timeline namespace checkpoint-lsn durable-lsn retained-reference requests)

(defun wal-capture-slot-recycling-work (checkpoint-lsn durable-lsn requests)
  "Capture the storage incarnation and owners under WAL for a later retry."
  (make-wal-slot-recycling-work
   :slots *wal-replication-slots* :directory (namestring *pgdata*)
   :system-id (catalog-system-identifier)
   :control-system-id (getf *wal-control-metadata* :system-id)
   :timeline *wal-insert-timeline* :namespace *wal-segment-namespace*
   :checkpoint-lsn checkpoint-lsn :durable-lsn durable-lsn
   :retained-reference (copy-tree (getf *wal-control-metadata* :retained-reference))
   :requests requests))

(defun wal-slot-recycling-work-current-p (work)
  "Check the ticket's registry and storage context. Authoritative callers hold WAL;
checks outside WAL are advisory."
  (and *wal-ready* (not *wal-poisoned*)
       (eq :segments (wal-slot-retention-mode))
       (not (recovery-in-progress-p)) (not *physical-recovery-failed-p*)
       (not *live-operation-terminal-shutdown*)
       (not (shutdown-in-progress-p))
       (eq (wal-slot-recycling-work-slots work) *wal-replication-slots*)
       (equal (wal-slot-recycling-work-directory work) (namestring *pgdata*))
       (= (wal-slot-recycling-work-system-id work) (catalog-system-identifier))
       (eql (wal-slot-recycling-work-control-system-id work)
            (getf *wal-control-metadata* :system-id))
       (= (wal-slot-recycling-work-timeline work) *wal-insert-timeline*)
       (eq (wal-slot-recycling-work-namespace work) *wal-segment-namespace*)
       (= (wal-slot-recycling-work-checkpoint-lsn work) *wal-checkpoint-lsn*)
       (<= (wal-slot-recycling-work-durable-lsn work) *wal-durable-lsn*)
       (equal (wal-slot-recycling-work-retained-reference work)
              (getf *wal-control-metadata* :retained-reference))))

(defun wal-slot-recycling-can-wait-p ()
  "Defer cleanup where waiting or parking would retain incompatible resources."
  (and (not (sb-thread:holding-mutex-p *wal-lock*))
       (not (sb-thread:holding-mutex-p *write-lock*))
       (not (sb-thread:holding-mutex-p *reload-run-lock*))
       (not (live-operation-lock-owned-p))
       (not (sb-thread:holding-mutex-p *physical-retained-inventory-publish-lock*))
       (not (and *physical-receive-store*
                 (sb-thread:holding-mutex-p (physical-store-mutex *physical-receive-store*))))
       (not *exported-snapshot-backend*)
       (not *schema-read-lock-held*) (not *schema-write-lock-held*)
       (not (backend-holds-schema-read-lock-p))
       (not (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*))
       (not *live-operation-terminal-shutdown*)
       (not (shutdown-in-progress-p))))

(defun wal-slot-recycling-owner-current-p (work slot owner backend)
  "Recheck the owner and current removal predicate under WAL before signaling.
Use the ticket's frozen endpoint for wal_keep_size and slot-cap calculations."
  (and (wal-slot-recycling-work-current-p work)
       (eq :segments (wal-slot-retention-mode))
       (eq slot (gethash (wal-replication-slot-name slot) *wal-replication-slots*))
       (wal-slot-reserved-p slot)
       (eq owner (wal-replication-slot-active-owner slot))
       (eq backend (wal-replication-slot-active-owner-backend slot))
       (let* ((redo (wal-checkpoint-recycling-segment (wal-slot-recycling-work-checkpoint-lsn work)))
              (independent (wal-independent-retention-segment redo (wal-slot-recycling-work-durable-lsn work)))
              (keep (wal-slot-removal-segment
                     redo independent (wal-slot-recycling-work-durable-lsn work))))
         (< (wal-replication-slot-restart-lsn slot) (* keep +wal-segment-size+)))))

(defvar *wal-slot-recycling-termination-requests*
  (make-hash-table :test #'eq :weakness :key)
  "Remember termination requests by slot generation and captured owner pair.")

(defvar *wal-slot-owner-wait-seconds* 5
  "Duration of each owner wait slice, not a total checkpoint deadline.")

(defun wal-request-recycling-owner-termination (work request)
  "Claim a current owner request under WAL, then signal outside the lock.
A self-owner terminates before loss is published; release after a claim does not
revoke the already authorized signal."
  (destructuring-bind (slot owner backend) request
    (when backend
      (logical-test-failpoint :before-recycling-owner-termination-claim slot owner backend)
      (let ((current nil) (claimed nil))
        (sb-sys:without-interrupts
          (sb-thread:with-mutex (*wal-lock*)
            (when (wal-slot-recycling-owner-current-p work slot owner backend)
              (setf current t)
              (unless (equal (gethash slot *wal-slot-recycling-termination-requests*)
                             (list owner backend))
                (setf (gethash slot *wal-slot-recycling-termination-requests*)
                      (list owner backend)
                      claimed t))))
          (when claimed
            (if (eq backend *backend*)
                (terminate-backend backend)
                (request-invalidated-backend-termination backend))))
        (when current backend)))))

(defun wal-finish-slot-recycling-work (work &key (wait t))
  "Stop owners outside storage locks, then recheck and retry WAL removal.
WAIT parks admission in bounded slices until owners release or work becomes stale.
With WAIT NIL, never park; leave unresolved work for another safe retry."
  (when (wal-slot-recycling-can-wait-p)
    (with-live-operation
      (loop
        (unless (and (eq :normal *live-operation-current-lane*)
                     (wal-slot-recycling-can-wait-p)
                     (wal-slot-recycling-work-current-p work))
          (return))
        (dolist (request (wal-slot-recycling-work-requests work))
          (let ((backend (wal-request-recycling-owner-termination work request)))
            (when (and wait backend (not (eq backend *backend*)))
              (call-with-live-operation-parked
               (lambda ()
                 (wait-for-invalidated-backend
                  backend *wal-slot-owner-wait-seconds*))))))
        (let ((next nil))
          (when (and (eq :normal *live-operation-current-lane*)
                     (wal-slot-recycling-can-wait-p))
            (with-write-lock
              (with-wal-critical
                (when (wal-slot-recycling-work-current-p work)
                  (wal-remove-checkpointed-segments-under-wal-lock
                   (wal-slot-recycling-work-checkpoint-lsn work)
                   (wal-slot-recycling-work-durable-lsn work))
                  (setf next *wal-slot-recycling-pending*)))))
          (unless (and wait next
                       (some (lambda (request)
                               (let* ((backend (third request))
                                      (thread (when backend (backend-thread backend))))
                                 (and thread (sb-thread:thread-alive-p thread))))
                             (wal-slot-recycling-work-requests next)))
            (return))
          (setf work next)))))
  (values))

(defun wal-finish-pending-slot-recycling (&key (wait nil))
  "Retry the current ticket or discard stale work; optional callers never wait
for admission or park. Unsafe resource and quiescence contexts defer the work."
  (when (and *wal-slot-recycling-pending* (wal-slot-recycling-can-wait-p))
    (flet ((drain ()
             (when (eq :normal *live-operation-current-lane*)
               (let ((work (sb-thread:with-mutex (*wal-lock*)
                             (let ((pending *wal-slot-recycling-pending*))
                               (when pending
                                 (if (wal-slot-recycling-work-current-p pending)
                                     pending
                                     (setf *wal-slot-recycling-pending* nil)))))))
                 (when work (wal-finish-slot-recycling-work work :wait wait))))))
      (if wait
          (with-live-operation (drain))
          (with-live-operation-if-admitted (drain)))))
  (values))

(defun wal-invalidate-replication-slots-matching-under-lock (predicate)
  "Save loss before publishing it on each matching reserved slot under WAL.
The saved restart floor remains internal; consumer cleanup runs after locks drain."
  (maphash
   (lambda (name slot)
     (declare (ignore name))
     (when (and (wal-slot-reserved-p slot) (funcall predicate slot))
       (let ((candidate (wal-copy-replication-slot-with-profile slot)))
         (setf (wal-replication-slot-wal-status candidate) :lost
               (wal-replication-slot-pending-xids candidate) (make-hash-table :test #'eql))
         (wal-save-replication-slot candidate)
         (setf (wal-replication-slot-wal-status slot) :lost
               (wal-replication-slot-pending-xids slot) (wal-replication-slot-pending-xids candidate))
         (when *wal-replication-slot-loss-collector*
           (push slot (car *wal-replication-slot-loss-collector*))))))
   *wal-replication-slots*)
  (wal-refresh-logical-capture-slot-mirror-under-wal-lock)
  (values))

(defun wal-prepare-standard-slot-recycling-under-lock (redo-segment current)
  "Save loss for inactive slots and collect active owner triples under WAL.
Combine independent retention, including wal_keep_size, before judging loss.
Active slots retain WAL until release and a recheck at the same endpoint."
  (let* ((independent (wal-independent-retention-segment redo-segment current))
         (keep (wal-slot-removal-segment redo-segment independent current))
         (requests nil))
    (wal-invalidate-replication-slots-matching-under-lock
     (lambda (slot)
       (when (< (wal-replication-slot-restart-lsn slot) (* keep +wal-segment-size+))
         (if (wal-replication-slot-active-owner slot)
             (progn
               (push (list slot (wal-replication-slot-active-owner slot)
                           (wal-replication-slot-active-owner-backend slot))
                     requests)
               nil)
             t))))
    (values (wal-slot-removal-segment redo-segment independent current) (nreverse requests))))

(defun wal-prepare-slot-recycling-under-lock (redo-segment &optional (current *wal-durable-lsn*))
  "Apply the selected policy under WAL and return the floor and owner requests.
Standard segments stop active owners first; native bytes keep their existing path."
  (ecase (wal-slot-retention-mode)
    (:bytes
     (wal-invalidate-lagging-replication-slots-under-wal-lock)
     (wal-slot-removal-segment
      redo-segment (wal-independent-retention-segment redo-segment current) current))
    (:segments
     (physical-source-invalidate-lagging-pins-under-wal-lock)
     (wal-prepare-standard-slot-recycling-under-lock redo-segment current))))

(defun wal-invalidate-lagging-replication-slots-under-wal-lock ()
  "Apply the native byte cap at durable WAL, including private source pins.
The recycling dispatcher selects the standard segment policy separately."
  (wal-invalidate-replication-slots-matching-under-lock
   (lambda (slot)
     (wal-slot-byte-limit-exceeded-p slot *wal-durable-lsn*)))
  (physical-source-invalidate-lagging-pins-under-wal-lock)
  (values))

(defun wal-lost-replication-slots ()
  "Return current lost generations so a later maintenance pass can retry cleanup."
  (sb-thread:with-mutex (*wal-lock*)
    (loop for slot being the hash-values of *wal-replication-slots*
          when (eq (wal-replication-slot-wal-status slot) :lost)
            collect slot)))

(defun call-with-invalidated-slot-cleanup (function &key (wait t))
  "Run maintenance, retry pending recycling and clean lost consumers on exit.
Preserve FUNCTION's values; callers may choose a nonwaiting retry."
  (let ((*wal-replication-slot-loss-collector* (list nil)))
    (unwind-protect
         (multiple-value-prog1 (funcall function)
           (wal-finish-pending-slot-recycling :wait wait))
      (cleanup-invalidated-logical-replication-state
       (remove-duplicates
        (append (nreverse (car *wal-replication-slot-loss-collector*))
                (wal-lost-replication-slots))
        :test #'eq)))))

(defun wal-checkpoint ()
  "Write cached pages and then clean up any slots lost by this checkpoint."
  (prevent-recovery-primary-operation "CHECKPOINT")
  (call-with-invalidated-slot-cleanup #'wal-checkpoint-core)
  (values))

(defun wal-remove-checkpointed-segments (checkpoint-lsn &key (wait t))
  (call-with-invalidated-slot-cleanup
   (lambda ()
     (with-wal-critical
       (wal-remove-checkpointed-segments-under-wal-lock checkpoint-lsn)))
   :wait wait)
  (values))

(defun detach-exported-logical-snapshots-for-slots-under-registry-lock (slots)
  "Transfer matching exports to retryable cleanup while the registry is held."
  (let ((entries nil))
    (sb-sys:without-interrupts
      (labels ((collect-matching (token state)
                 (when (member (exported-logical-snapshot-slot-object state)
                               slots :test #'eq)
                   (pushnew (cons token state) entries
                            :test (lambda (left right)
                                    (eq (cdr left) (cdr right)))))))
        (maphash #'collect-matching *exported-logical-snapshots*)
        (maphash #'collect-matching *expired-exported-logical-snapshots*))
      (dolist (entry entries)
        (let ((token (car entry))
              (state (cdr entry)))
          (sb-thread:with-mutex (*exported-logical-snapshot-importer-lock*)
            (setf (exported-logical-snapshot-expired-p state) t))
          (when (eq state (gethash token *exported-logical-snapshots*))
            (remhash token *exported-logical-snapshots*))
          (setf (gethash token *expired-exported-logical-snapshots*) state))))
    (mapcar #'cdr entries)))

(defun wal-drop-replication-slot (name &optional expected-slot)
  "Durably remove NAME and adopt its exported snapshots for retryable cleanup."
  (unless (wal-valid-replication-slot-name-p name)
    (error "invalid replication slot name ~s" name))
  (let ((removed-slot nil))
    ;; Registry -> WAL matches snapshot publication. Adopt the exact slot's
    ;; exports before releasing either lock, so a dropped slot is never the only
    ;; remaining owner of cleanup work.
    (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
      (sb-sys:without-interrupts
        (sb-thread:with-mutex (*wal-lock*)
          (let ((slot (gethash name *wal-replication-slots*)))
            (when (and expected-slot (not (eq expected-slot slot)))
              (return-from wal-drop-replication-slot (values)))
            (unless slot
              (ereport "42704" "replication slot \"~a\" does not exist" name))
            (when (and (wal-replication-slot-active-owner slot)
                       (not (physical-slot-owned-by-backend-p slot *backend*)))
              (ereport "55006" "replication slot \"~a\" is active" name))
            (wal-delete-replication-slot-file name)
            (setf removed-slot slot)
            (remhash name *wal-replication-slots*)
            (wal-refresh-logical-capture-slot-mirror-under-wal-lock)))
        (when (and removed-slot (wal-logical-slot-p removed-slot))

          (detach-exported-logical-snapshots-for-slots-under-registry-lock
           (list removed-slot))
          (logical-test-failpoint :after-logical-slot-drop-detach name))))
    (when (and removed-slot (wal-logical-slot-p removed-slot))
      (cleanup-invalidated-logical-replication-state (list removed-slot))))
  (values))

(defvar *logical-slot-owner-termination-lock*
  (sb-thread:make-mutex :name "logical slot owner termination requests"))

(defvar *logical-slot-owner-termination-requests*
  (make-hash-table :test #'eq))

(defun wal-release-replication-slot-owner (name owner &optional expected-slot)
  "Release OWNER's claim, optionally requiring the exact slot generation.
A temporary physical slot returns to its creator and records inactivity time."
  (sb-sys:without-interrupts
    (sb-thread:with-mutex (*wal-lock*)
      (let ((slot (gethash name *wal-replication-slots*)))
        (when (and slot (or (null expected-slot) (eq expected-slot slot))
                   (eq owner (wal-replication-slot-active-owner slot)))
          (let ((creator (when (wal-physical-slot-p slot)
                           (wal-replication-slot-temporary-owner slot))))
            (setf (wal-replication-slot-active-owner slot) creator
                  (wal-replication-slot-active-owner-backend slot) creator)
            (when (wal-physical-slot-p slot)
              (setf (wal-replication-slot-inactive-since slot) (postgres-current-timestamp))))
          (sb-thread:with-mutex (*logical-slot-owner-termination-lock*)
            (remhash slot *logical-slot-owner-termination-requests*))))))
  (values))

(defun call-with-logical-replication-slot (name function &key allow-initial-copy)
  "Hold an exclusive runtime claim and remember its backend for invalidation."
  (let ((owner (gensym "LOGICAL-SLOT-"))
        (slot nil))
    (unwind-protect
         (progn
           ;; Establish cleanup before the owner becomes visible. Publication is
           ;; interrupt-atomic so invalidation sees both owner fields or neither.
           (sb-sys:without-interrupts
             (sb-thread:with-mutex (*wal-lock*)
               (setf slot (gethash name *wal-replication-slots*))
               (unless slot
                 (ereport "42704" "replication slot \"~a\" does not exist" name))
               (wal-require-logical-slot slot)
               (unless (wal-replication-slot-database-oid slot)
                 (ereport "55000" "replication slot has no database binding"))
               (when (eq (wal-replication-slot-wal-status slot) :lost)
                 (ereport "55000" "replication slot \"~a\" has lost its WAL" name))
               (unless (or (wal-replication-slot-confirmed-flush-lsn slot)
                           allow-initial-copy)
                 (ereport "55000" "replication slot initial copy is not acknowledged"))
               (when (wal-replication-slot-active-owner slot)
                 (ereport "55006" "replication slot \"~a\" is active" name))
               (when (and *backend*
                          (/= (database-oid (current-database))
                              (wal-replication-slot-database-oid slot)))
                 (ereport "55000" "replication slot belongs to another database"))
               (setf (wal-replication-slot-active-owner slot) owner
                     (wal-replication-slot-active-owner-backend slot) *backend*)))
           (logical-test-failpoint :after-logical-slot-owner-publication name)
           (let ((*wal-logical-slot-owner* owner)
                 (*wal-logical-slot-read-progress*
                   (or (wal-replication-slot-confirmed-flush-lsn slot)
                       (wal-replication-slot-consistent-lsn slot))))
             (funcall function slot)))
      ;; Safe even if claim validation failed or the slot was replaced: release
      ;; only clears the exact owner token published above.
      (wal-release-replication-slot-owner name owner))))

(defun register-exported-logical-snapshot (state)
  "Publish STATE only while its exact slot is still durably reserved."
  (let ((token (exported-logical-snapshot-token state))
        (slot-name (exported-logical-snapshot-slot-name state))
        (slot-object (exported-logical-snapshot-slot-object state)))
    (unless slot-object
      (error "exported logical snapshot has no slot identity"))
    ;; Registry -> WAL is intentional. Invalidation releases WAL before it
    ;; enters the registry, so token publication cannot race a lost generation.
    (sb-sys:without-interrupts
      (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
        (when (or (gethash token *exported-logical-snapshots*)
                  (gethash token *expired-exported-logical-snapshots*)
                  (gethash token *transaction-exported-snapshots*))
          (error "duplicate exported logical snapshot token"))
        (sb-thread:with-mutex (*wal-lock*)
          (unless (and (eq slot-object (gethash slot-name *wal-replication-slots*))
                       (eq (wal-replication-slot-wal-status slot-object) :reserved))
            (ereport "55000"
                     "replication slot was lost before snapshot publication"))
          (setf (gethash token *exported-logical-snapshots*) state))))
    state))

(defun remove-exported-logical-snapshot-token (token)
  "Move TOKEN to the pending-cleanup registry before releasing its owner."
  (let ((state nil))
    (unwind-protect
         (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
           (setf state (or (gethash token *exported-logical-snapshots*)
                           (gethash token *expired-exported-logical-snapshots*)))
           (when (and state (gethash token *exported-logical-snapshots*))
             ;; Imports hold this registry lock through importer publication.
             ;; Expire and transfer ownership before a new importer can look up
             ;; the token or an interrupt can abandon cleanup.
             (sb-sys:without-interrupts
               (sb-thread:with-mutex (*exported-logical-snapshot-importer-lock*)
                 (setf (exported-logical-snapshot-expired-p state) t))
               (remhash token *exported-logical-snapshots*)
               (setf (gethash token *expired-exported-logical-snapshots*) state))))
      (when state
        (finish-expired-exported-logical-snapshot state)))
    state))

(defun release-exported-logical-snapshot-for-slot (slot-name &optional slot-object)
  "Retire only the matching slot generation's exported snapshot."
  (let ((tokens nil))
    (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
      (maphash
       (lambda (token state)
         (when (and (string= slot-name
                             (exported-logical-snapshot-slot-name state))
                    (or (null slot-object)
                        (eq slot-object
                            (exported-logical-snapshot-slot-object state))))
           (push token tokens)))
       *exported-logical-snapshots*))
    (dolist (token tokens)
      (remove-exported-logical-snapshot-token token)))
  (values))

(defun detach-exported-logical-snapshots-for-slots (slots)
  "Expire matching tokens and transfer them to retryable pending cleanup."
  (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
    (detach-exported-logical-snapshots-for-slots-under-registry-lock slots)))

(defun copy-exported-logical-snapshot-importers (state)
  (sb-thread:with-mutex (*exported-logical-snapshot-importer-lock*)
    (copy-list (exported-logical-snapshot-importers state))))

(defvar *logical-test-invalidated-backend-hook* nil)

(defun request-invalidated-backend-termination (backend)
  (if *logical-test-invalidated-backend-hook*
      (funcall *logical-test-invalidated-backend-hook* backend)
      (let ((socket (backend-socket backend)))
        ;; A peer may have stopped reading. Break socket I/O before delivering
        ;; the single graceful termination, like slot.c does outside shared locks.
        (when socket
          (ignore-errors (sb-bsd-sockets:socket-shutdown socket :direction :io)))
        (ignore-errors (terminate-backend backend))))
  (values))

(defun wait-for-invalidated-backend (backend &optional (timeout 5))
  (let ((thread (backend-thread backend)))
    (when (and (or (null timeout) (plusp timeout))
               thread (not (eq thread sb-thread:*current-thread*))
               (sb-thread:thread-alive-p thread))
      (flet ((join ()
               (handler-case
                   (sb-thread:join-thread thread :timeout timeout)
                 (sb-thread:join-thread-error () nil))))
        (cond
          (*live-operation-parked-p* (join))
          ((wal-slot-recycling-can-wait-p)
           (with-live-operation
             (when (eq :normal *live-operation-current-lane*)
               (call-with-live-operation-parked #'join))))))))
  (values))

(defun terminate-invalidated-backend (backend &optional (timeout 5))
  (request-invalidated-backend-termination backend)
  (wait-for-invalidated-backend backend timeout))

(defun wal-current-lost-slot-owner-p (slot backend)
  "Check the current lost generation and backend while the caller holds WAL."
  (and (eq slot (gethash (wal-replication-slot-name slot) *wal-replication-slots*))
       (eq :lost (wal-replication-slot-wal-status slot))
       (eq backend (wal-replication-slot-active-owner-backend slot))))

(defun request-lost-slot-owner-termination (slot backend)
  "Claim one termination for the current lost slot generation under WAL.
The captured backend is signaled after unlocking. A later release does not
revoke a request already authorized by the generation and owner check."
  (let ((claimed nil) (current nil))
    (logical-test-failpoint
     :before-lost-slot-owner-termination-claim slot backend)
    (sb-sys:without-interrupts
      ;; Cleanup can hold a stale owner/backend pair captured before another
      ;; pass let the owner release. Revalidate under WAL, then claim in WAL ->
      ;; termination-registry order, matching owner release's lock order.
      (sb-thread:with-mutex (*wal-lock*)
        (when (wal-current-lost-slot-owner-p slot backend)
          (setf current t)
          (sb-thread:with-mutex (*logical-slot-owner-termination-lock*)
            (unless (gethash slot *logical-slot-owner-termination-requests*)
              (setf (gethash slot *logical-slot-owner-termination-requests*)
                    backend
                    claimed t)))))
      ;; Signal after both locks are released, but before interrupts resume.
      (when claimed
        (request-invalidated-backend-termination backend)))
    (values claimed current)))

(defun logical-pending-expired-snapshot-states ()
  (sb-thread:with-mutex (*exported-logical-snapshot-lock*)
    (loop for state being the hash-values
            of *expired-exported-logical-snapshots*
          collect state)))

(defun request-exported-snapshot-importer-termination (state)
  (let ((requested-backends nil))
    (dolist (backend (copy-exported-logical-snapshot-importers state))
      (unless (eq backend *backend*)
        ;; Record each backend separately before its one graceful request. Keep
        ;; claim and request uninterruptible, but release the registry lock
        ;; before touching its socket or thread.
        (sb-sys:without-interrupts
          (let ((claimed nil))
            (sb-thread:with-mutex (*exported-logical-snapshot-importer-lock*)
              (unless (member
                       backend
                       (exported-logical-snapshot-termination-requested-backends
                        state)
                       :test #'eq)
                (push backend
                      (exported-logical-snapshot-termination-requested-backends
                       state))
                (setf claimed t)))
            (when claimed
              (request-invalidated-backend-termination backend)
              (push backend requested-backends))))))
    (nreverse requested-backends)))

(defun slot-cleanup-current-backend-p (owner-requests states)
  "Recheck whether this backend still participates in cleanup before self-exit.
An earlier authorized termination request is not revoked by a later release."
  (and *backend*
       (or (sb-thread:with-mutex (*wal-lock*)
             (some (lambda (request)
                     (and (eq (cdr request) *backend*)
                          (wal-current-lost-slot-owner-p (car request) *backend*)))
                   owner-requests))
           (some (lambda (state)
                   (member *backend* (copy-exported-logical-snapshot-importers state) :test #'eq))
                 states))))

(defun cleanup-invalidated-replication-state (slots)
  "Retire lost slot owners and logical snapshot consumers outside WAL locks."
  (let* ((new-states (detach-exported-logical-snapshots-for-slots
                      (remove-if-not #'wal-logical-slot-p slots)))
         (pending-states (logical-pending-expired-snapshot-states))
         (states (remove-duplicates (append new-states pending-states) :test #'eq))
         (importer-backends
           (loop for state in states append (copy-exported-logical-snapshot-importers state)))
         (owner-requests
           (loop for slot in slots
                 for backend = (wal-replication-slot-active-owner-backend slot)
                 when (and backend (not (member backend importer-backends :test #'eq)))
                   collect (cons slot backend)))
         (owner-backends
           (loop for (slot . backend) in owner-requests
                 when (nth-value 1 (request-lost-slot-owner-termination slot backend))
                   collect backend))
         (newly-requested-importers
           (loop for state in states append (request-exported-snapshot-importer-termination state)))
         (backends (remove-duplicates (append owner-backends newly-requested-importers) :test #'eq)))
    (unwind-protect
         (progn
           (dolist (backend backends)
             (unless (eq backend *backend*)
               (wait-for-invalidated-backend backend)))
           (dolist (state states) (finish-expired-exported-logical-snapshot state))
           (when (slot-cleanup-current-backend-p owner-requests states)
             (terminate-backend *backend*)))
      (dolist (state states)
        (ignore-errors (finish-expired-exported-logical-snapshot state)))))
  (values))

(defun cleanup-invalidated-logical-replication-state (slots)
  "Retain the existing cleanup entry point for both public slot kinds."
  (cleanup-invalidated-replication-state slots))
