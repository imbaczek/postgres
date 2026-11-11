;;; Maintain the primary's durable logical decoding status and capture policy.
;;; An ON record is flushed before a logical slot can be published. OFF can be
;;; deferred while capture is armed. Unknown status is distinct from known OFF.
;;; Policy is protected by the WAL lock; take it before the XID lock when both
;;; are needed.

;;; An existing durable ON checkpoint also satisfies the slot publication check.

(in-package #:pg)

(declaim (special *xid-lock* *wal-level* *logical-capture-arming*
                  *logical-capture-live-slots* *logical-capture-xid-modes* *active-xids*))
(declaim (ftype function call-with-logical-capture-armed
                notify-primary-wal-writer-under-lock admin-log))

(defstruct (wal-logical-info
            (:constructor %make-wal-logical-info (incarnation generation enabled)))
  "Incarnation-owned bit and generation published under the XID lock.
ON is also a positive reason for native capture."
  (incarnation nil :read-only t)
  (generation 0 :read-only t)
  (enabled nil :read-only t))

(defstruct (wal-logical-info-cache
            (:constructor %make-wal-logical-info-cache (generation enabled pending held-xid)))
  "Backend reporting cache; pending delays refresh until its real XID ends.
HELD-XID retains logical ownership in a failed block after resource cleanup."
  (generation 0 :read-only t)
  (enabled nil :read-only t)
  (pending nil :read-only t)
  (held-xid nil :read-only t))

(defun wal-logical-info-valid-p (info)
  (and (wal-logical-info-p info)
       (symbolp (wal-logical-info-incarnation info))
       (wal-logical-info-incarnation info)
       (typep (wal-logical-info-generation info) '(integer 1 *))
       (member (wal-logical-info-enabled info) '(nil t))))

(defun wal-logical-info-real-xid-p (xid)
  (and (typep xid '(integer 3 18446744073709551615))
       (>= (xid32-of xid) 3)))

(defun wal-logical-info-cache-valid-p (cache)
  (and (wal-logical-info-cache-p cache)
       (typep (wal-logical-info-cache-generation cache) '(integer 1 *))
       (member (wal-logical-info-cache-enabled cache) '(nil t))
       (member (wal-logical-info-cache-pending cache) '(nil t))
       (or (null (wal-logical-info-cache-held-xid cache))
           (wal-logical-info-real-xid-p (wal-logical-info-cache-held-xid cache)))))

(defun wal-logical-info-check-cache-input (cache info xid)
  (unless (and (wal-logical-info-valid-p info)
               (or (null cache)
                   (and (wal-logical-info-cache-valid-p cache)
                        (<= (wal-logical-info-cache-generation cache)
                            (wal-logical-info-generation info))
                        (or (null xid) (null (wal-logical-info-cache-held-xid cache))
                            (eql xid (wal-logical-info-cache-held-xid cache)))))
               (or (null xid) (wal-logical-info-real-xid-p xid)))
    (error "invalid logical info cache transition"))
  (values))

(defun wal-logical-info-deliver-cache (cache info xid)
  "Acknowledge INFO now, retaining the cached bit while a real XID is held."
  (wal-logical-info-check-cache-input cache info xid)
  (when (and (null cache) xid)
    (error "logical info cache must be initialized before XID assignment"))
  (let ((held (and cache (wal-logical-info-cache-held-xid cache)))
        (generation (wal-logical-info-generation info)))
    (if (or xid held)
        (if (and (= generation (wal-logical-info-cache-generation cache))
                 (wal-logical-info-cache-pending cache))
            cache
            (%make-wal-logical-info-cache
             generation (wal-logical-info-cache-enabled cache) t held))
        (if (and cache (= generation (wal-logical-info-cache-generation cache))
                 (not (wal-logical-info-cache-pending cache))
                 (eq (wal-logical-info-cache-enabled cache) (wal-logical-info-enabled info)))
            cache
            (%make-wal-logical-info-cache generation (wal-logical-info-enabled info) nil nil)))))

(defun wal-logical-info-end-cache (cache info xid preserve-failed)
  "Refresh at transaction cleanup, or latch the XID through a failed block."
  (wal-logical-info-check-cache-input cache info xid)
  (unless (member preserve-failed '(nil t)) (error "invalid logical info cleanup mode"))
  (let ((held (or xid (and cache (wal-logical-info-cache-held-xid cache)))))
    (if (and preserve-failed held)
        (progn
          (unless cache (error "failed logical transaction lacks its initial cache"))
          (if (eql held (wal-logical-info-cache-held-xid cache))
              cache
              (%make-wal-logical-info-cache
               (wal-logical-info-cache-generation cache) (wal-logical-info-cache-enabled cache)
               (wal-logical-info-cache-pending cache) held)))
        (wal-logical-info-deliver-cache
         (unless (and cache (wal-logical-info-cache-held-xid cache)) cache) info nil))))

(declaim (special *backend-registry* *backend-registry-lock* *shutdown-abandoned-backends*
                  *backend* *active-xids* *preserve-failed-logical-info-backend*)
         (ftype function backend-p backend-xid backend-logical-info-cache
                (setf backend-xid) (setf backend-logical-info-cache))
         (notinline backend-p backend-xid backend-logical-info-cache
                    (setf backend-xid) (setf backend-logical-info-cache)))

(defun wal-logical-info-current-under-xid-lock (&optional allow-uninitialized)
  (assert (sb-thread:holding-mutex-p *xid-lock*))
  (when (and allow-uninitialized (null *wal-logical-info*))
    (return-from wal-logical-info-current-under-xid-lock nil))
  (unless (and (wal-logical-info-valid-p *wal-logical-info*)
               (eq *wal-storage-incarnation* (wal-logical-info-incarnation *wal-logical-info*))
               (<= (wal-logical-info-generation *wal-logical-info*) *wal-logical-info-generation*))
    (error "logical info does not belong to current WAL storage"))
  *wal-logical-info*)

(defun wal-logical-info-initialize-backend-under-xid-lock (backend)
  (assert (sb-thread:holding-mutex-p *xid-lock*))
  (unless (and (backend-p backend) (null (backend-xid backend))
               (not (and (backend-logical-info-cache backend)
                         (wal-logical-info-cache-held-xid (backend-logical-info-cache backend)))))
    (error "backend registration already owns logical transaction state"))
  (let ((info (wal-logical-info-current-under-xid-lock t)))
    (setf (backend-logical-info-cache backend)
          (when info (wal-logical-info-deliver-cache (backend-logical-info-cache backend) info nil))))
  backend)

(defun wal-logical-info-finish-backend-under-xid-lock (backend preserve-failed)
  "Finish cache ownership under the leaf XID lock before later admission work."
  (assert (sb-thread:holding-mutex-p *xid-lock*))
  (let* ((info (wal-logical-info-current-under-xid-lock t))
         (cache (backend-logical-info-cache backend))
         (next (when info
                 (wal-logical-info-end-cache cache info (backend-xid backend) preserve-failed))))
    (setf (backend-xid backend) nil
          (backend-logical-info-cache backend) next))
  (values))

(defun wal-logical-info-backend-enabled (backend)
  "Read the backend reporting cache without taking the WAL lock.
Native row capture uses a separately pinned, conservative XID decision."
  (wal-check-healthy)
  (unless *wal-ready* (error "logical info requires initialized WAL"))
  (sb-thread:with-mutex (*xid-lock*)
    (let ((info (wal-logical-info-current-under-xid-lock))
          (cache (and backend (backend-logical-info-cache backend))))
      (unless (and (wal-logical-info-cache-valid-p cache)
                   (= (wal-logical-info-cache-generation cache) (wal-logical-info-generation info))
                   (or (wal-logical-info-cache-pending cache)
                       (eq (wal-logical-info-cache-enabled cache) (wal-logical-info-enabled info))))
        (error "backend has not acknowledged current logical info"))
      (wal-logical-info-cache-enabled cache))))

(defun wal-logical-info-participants-under-registry-lock (rebase extra)
  (assert (sb-thread:holding-mutex-p *backend-registry-lock*))
  (remove-duplicates
   (append (loop for backend being the hash-values of *backend-registry* collect backend)
           (when rebase *shutdown-abandoned-backends*) extra)
   :test #'eq))

(defun wal-logical-info-prepare-caches-under-xid-lock (backends info rebase)
  (assert (sb-thread:holding-mutex-p *xid-lock*))
  (loop for backend in backends
        collect
        (progn
          (unless (backend-p backend) (error "invalid logical info participant"))
          (let ((cache (backend-logical-info-cache backend)))
            (when (and rebase
                       (or (backend-xid backend)
                           (and cache (wal-logical-info-cache-held-xid cache))))
              (error "logical info rebase still has real transaction ownership"))
            (cons backend
                  (wal-logical-info-deliver-cache (unless rebase cache) info
                                                  (unless rebase (backend-xid backend))))))))

(defun wal-logical-info-publish-under-lock (enabled &key rebase incarnation extra-backends)
  "Publish and acknowledge a reporting generation under WAL, registry, then XID locks.
A full storage replacement requires all real XIDs and activation arms to drain."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (and (member enabled '(nil t)) (member rebase '(nil t))
               (or (null incarnation) (and (symbolp incarnation) rebase)))
    (error "invalid logical info publication"))
  (sb-thread:with-mutex (*backend-registry-lock*)
    (let ((backends (wal-logical-info-participants-under-registry-lock rebase extra-backends)))
      (sb-thread:with-mutex (*xid-lock*)
        (let ((prior (wal-logical-info-current-under-xid-lock rebase)))
          (when (and incarnation
                     (or *active-xids*
                         (plusp (hash-table-count *logical-capture-arming*))
                         (plusp (hash-table-count *wal-logical-info-activations*))))
            (error "WAL replacement still owns logical activation or real XIDs"))
          (when (and prior (not rebase) (eq enabled (wal-logical-info-enabled prior)))
            (return-from wal-logical-info-publish-under-lock prior))
          (let* ((generation (1+ *wal-logical-info-generation*))
                 (info (%make-wal-logical-info (or incarnation *wal-storage-incarnation*)
                                               generation enabled))
                 (updates (wal-logical-info-prepare-caches-under-xid-lock backends info rebase)))
            (sb-sys:without-interrupts
              (when incarnation
                (setf *wal-storage-incarnation* incarnation
                      *wal-logical-info-disable-pending* nil))
              (setf *wal-logical-info* info
                    *wal-logical-info-generation* generation)
              (dolist (entry updates)
                (setf (backend-logical-info-cache (car entry)) (cdr entry))))
            info))))))

(defun wal-logical-info-rebinding-valid-p (prior replacement old new)
  (if (null prior)
      (null replacement)
      (and (wal-logical-info-valid-p prior) (wal-logical-info-valid-p replacement)
           (eq old (wal-logical-info-incarnation prior))
           (eq new (wal-logical-info-incarnation replacement))
           (= (wal-logical-info-generation prior) (wal-logical-info-generation replacement))
           (eq (wal-logical-info-enabled prior) (wal-logical-info-enabled replacement)))))

(defun wal-logical-info-rebind-incarnation (prior old new)
  "Prepare the same bit and generation for a CLOG rebind. This object grants no
writer authority; activation tokens remain separate."
  (when prior
    (unless (and (wal-logical-info-valid-p prior)
                 (eq old (wal-logical-info-incarnation prior))
                 (symbolp new) new (not (eq old new)))
      (error "invalid logical info storage transfer"))
    (%make-wal-logical-info new (wal-logical-info-generation prior)
                            (wal-logical-info-enabled prior))))

(defstruct wal-logical-info-activation
  "Preflight request and interest token, without writer authority."
  (incarnation nil :read-only t)
  (slot-name nil :read-only t))

(defun call-with-logical-info-wal-lock (function)
  (if (sb-thread:holding-mutex-p *wal-lock*)
      (funcall function)
      (sb-thread:with-mutex (*wal-lock*) (funcall function))))

(defun wal-logical-info-begin-activation (token activation)
  "Publish provisional logical-info ON before old XIDs drain and durable status."
  (call-with-logical-info-wal-lock
   (lambda ()
     (wal-check-healthy)
     (prevent-recovery-primary-operation "logical info activation")
     (unless (and *wal-ready* (wal-logical-info-activation-p activation)
                  (or (null (wal-logical-info-activation-incarnation activation))
                      (eq *wal-storage-incarnation*
                          (wal-logical-info-activation-incarnation activation))))
       (wal-invalid-input "logical activation belongs to unavailable or replaced WAL"))
     (let ((name (wal-logical-info-activation-slot-name activation)))
       (when name
         (unless (wal-valid-replication-slot-name-p name)
           (ereport "42602" "invalid replication slot name ~s" name))
         (when (gethash name *wal-replication-slots*)
           (ereport "42710" "replication slot ~s already exists" name))
         (wal-check-replication-slot-capacity)))
     (sb-thread:with-mutex (*xid-lock*)
       (wal-logical-info-current-under-xid-lock)
       (unless (and (gethash token *logical-capture-arming*)
                    (not (gethash token *wal-logical-info-activations*)))
         (error "logical activation lacks its capture arm"))
       (setf (gethash token *wal-logical-info-activations*) activation))
     (wal-logical-info-publish-under-lock t))))

(defun wal-logical-info-capture-under-xid-lock-p ()
  (let ((info (wal-logical-info-current-under-xid-lock t)))
    (and info (wal-logical-info-enabled info))))

(defstruct (wal-logical-policy
            (:constructor %make-wal-logical-policy (incarnation state pending-disable)))
  "Logical status evidence owned by one WAL incarnation.
STATE NIL means unknown, not OFF; PENDING-DISABLE requests a later OFF
transition while an enabled state is still in force."
  (incarnation nil :read-only t)
  (state nil :read-only t)
  (pending-disable nil :read-only t))

(defun wal-logical-info-end-activation (token failed)
  "Release activation interest and queue disable after failure when needed."
  (call-with-logical-info-wal-lock
   (lambda ()
     (let ((removed (sb-thread:with-mutex (*xid-lock*)
                      (remhash token *wal-logical-info-activations*))))
       (when (and removed *wal-ready* (not *wal-poisoned*)
                  (eq *wal-level* :replica))
         (let ((state (and (wal-logical-policy-p *wal-logical-policy*)
                           (wal-logical-policy-state *wal-logical-policy*))))
           (when (or failed (not (and state (wal-logical-status-enabled state))))
             (setf *wal-logical-info-disable-pending* t)
             (notify-primary-wal-writer-under-lock)))))))
  (values))

(defvar *wal-logical-policy-test-hook* nil)

(defun wal-logical-policy-test-point (point policy)
  (when *wal-logical-policy-test-hook*
    (funcall *wal-logical-policy-test-hook* point policy)))

(defun wal-logical-policy-valid-p (policy)
  (and (wal-logical-policy-p policy)
       (symbolp (wal-logical-policy-incarnation policy))
       (wal-logical-policy-incarnation policy)
       (member (wal-logical-policy-pending-disable policy) '(nil t))
       (or (null (wal-logical-policy-state policy))
           (wal-logical-status-valid-p (wal-logical-policy-state policy)))
       (or (null (wal-logical-policy-pending-disable policy))
           (and (wal-logical-policy-state policy)
                (wal-logical-status-enabled (wal-logical-policy-state policy))))))

(defun wal-logical-policy-rebinding-valid-p (prior replacement old new)
  (and (symbolp old) old (symbolp new) new
       (if (member prior '(nil :unrecovered))
           (eq prior replacement)
           (and (wal-logical-policy-valid-p prior)
                (wal-logical-policy-valid-p replacement)
                (eq old (wal-logical-policy-incarnation prior))
                (eq new (wal-logical-policy-incarnation replacement))
                (eq (wal-logical-policy-state prior) (wal-logical-policy-state replacement))
                (eq (wal-logical-policy-pending-disable prior)
                    (wal-logical-policy-pending-disable replacement))))))

(defun wal-logical-policy-rebind-incarnation (policy old new)
  "Prepare policy evidence for transfer to a new incarnation.
Reset-plan validation governs publication."
  (unless (and (symbolp old) old (symbolp new) new
               (or (member policy '(nil :unrecovered))
                   (and (wal-logical-policy-valid-p policy)
                        (eq old (wal-logical-policy-incarnation policy)))))
    (error "invalid logical policy storage transfer"))
  (if (member policy '(nil :unrecovered)) policy
      (%make-wal-logical-policy new (wal-logical-policy-state policy)
                                (wal-logical-policy-pending-disable policy))))

(defun wal-set-logical-capture-under-lock (enabled)
  "Change the capture force under WAL then XID lock."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-thread:with-mutex (*xid-lock*)
    (setf *logical-capture-force* (not (null enabled))))
  (values))

(defun wal-reset-logical-policy-under-lock ()
  "Discard policy and capture force when WAL storage ownership changes."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (setf *wal-logical-policy* nil)
  (wal-set-logical-capture-under-lock nil))

(defun wal-checked-logical-policy-under-lock ()
  "Return current policy after checking its owner, durability and capture mode.
Reject an unrecovered witness rather than treating it as an OFF state."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-healthy)
  (unless *wal-ready* (wal-invalid-input "logical status requires initialized WAL"))
  (when (eq :unrecovered *wal-logical-policy*)
    (wal-invalid-input "logical status recovery has not completed"))
  (when *wal-logical-policy*
    (let ((state (wal-logical-policy-state *wal-logical-policy*)))
      (unless (and (wal-logical-policy-valid-p *wal-logical-policy*)
                   (eq *wal-storage-incarnation*
                       (wal-logical-policy-incarnation *wal-logical-policy*))
                   (or (null state)
                       (and (= *wal-insert-timeline* (wal-logical-status-timeline state))
                            (<= (wal-logical-status-end state) *wal-durable-lsn* *wal-end-lsn*)
                            (sb-thread:with-mutex (*xid-lock*)
                              (eq *logical-capture-force* (wal-logical-status-enabled state))))))
        (error "logical status differs from WAL storage or capture mode"))))
  *wal-logical-policy*)

(defun wal-current-logical-decoding-fields ()
  "Return checkpoint fields for known status, omitting unknown status."
  (let* ((policy (wal-checked-logical-policy-under-lock))
         (state (and policy (wal-logical-policy-state policy))))
    (when state (list :logical-decoding-enabled (wal-logical-status-enabled state)))))

(defun wal-prepare-logical-policy-recovery (snapshot marker-p)
  "Mark policy unrecovered when checkpoint or WAL supplies a witness."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (setf *wal-logical-policy*
        (when (or marker-p (get-properties snapshot '(:logical-decoding-enabled)))
          :unrecovered)))

(defun wal-install-recovered-logical-policy (state incarnation)
  "Install recovered evidence and its capture mode for the current incarnation."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (and (eq incarnation *wal-storage-incarnation*)
               (or (null state)
                   (and (wal-logical-status-valid-p state)
                        (= *wal-insert-timeline* (wal-logical-status-timeline state))
                        (<= (wal-logical-status-end state) *wal-durable-lsn* *wal-end-lsn*))))
    (error "invalid recovered logical policy"))
  (setf *wal-logical-policy* (%make-wal-logical-policy incarnation state nil)
        *wal-logical-info-disable-pending* nil)
  (wal-set-logical-capture-under-lock (and state (wal-logical-status-enabled state)))
  (wal-logical-info-publish-under-lock
   (and state (wal-logical-status-enabled state)) :rebase t
   :extra-backends (when *backend* (list *backend*))))

(defun wal-logical-valid-slot-count-under-lock ()
  "Count logical slots that have not lost their WAL under the WAL lock."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (loop for slot being the hash-values of *wal-replication-slots*
        count (and (wal-logical-slot-p slot)
                   (not (eq :lost (wal-replication-slot-wal-status slot))))))

(defun wal-logical-interest-present-under-lock-p ()
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (or (eq *wal-level* :logical)
      (plusp (wal-logical-valid-slot-count-under-lock))))

(defun wal-logical-capture-required-under-lock-p ()
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (or (eq *wal-level* :logical)
      (sb-thread:with-mutex (*xid-lock*)
        (or *logical-capture-force* (wal-logical-info-capture-under-xid-lock-p)))))

(defun wal-logical-arm-count-under-lock ()
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-thread:with-mutex (*xid-lock*) (hash-table-count *logical-capture-arming*)))

(defun wal-logical-set-pending-under-lock (policy pending)
  "Replace the current policy with an updated deferred-disable request."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (eq policy *wal-logical-policy*) (wal-invalid-input "stale logical pending request"))
  (unless (eq pending (wal-logical-policy-pending-disable policy))
    (let ((candidate (%make-wal-logical-policy
                      (wal-logical-policy-incarnation policy)
                      (wal-logical-policy-state policy) pending)))
      (unless (wal-logical-policy-valid-p candidate)
        (error "invalid logical pending request"))
      (setf *wal-logical-policy* candidate)))
  (values))

(defun wal-note-logical-slot-count-under-lock (count)
  "Request a writer disable when replica mode loses its last valid slot."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (when (and *wal-ready* (not *wal-poisoned*))
    (let* ((policy (wal-checked-logical-policy-under-lock))
           (state (and policy (wal-logical-policy-state policy)))
           (no-interest (and (eq *wal-level* :replica) (zerop count)))
           (pending (and no-interest state (wal-logical-status-enabled state)))
           (info-pending
             (and no-interest
                  (sb-thread:with-mutex (*xid-lock*)
                    (wal-logical-info-capture-under-xid-lock-p)))))
      (when policy (wal-logical-set-pending-under-lock policy (not (null pending))))
      (setf *wal-logical-info-disable-pending* (not (null info-pending)))
      (when (or pending info-pending) (notify-primary-wal-writer-under-lock))))
  (values))

(defun wal-request-logical-disable-after-arm ()
  "Reconsider deferred disable after a capture arm has been released."
  (when (and *wal-ready* (not *wal-poisoned*)
             (wal-logical-policy-p *wal-logical-policy*))
    (flet ((request ()
             (wal-note-logical-slot-count-under-lock
              (wal-logical-valid-slot-count-under-lock))))
      (if (sb-thread:holding-mutex-p *wal-lock*) (request)
          (sb-thread:with-mutex (*wal-lock*) (request)))))
  (values))

(defun wal-check-logical-policy-transition-under-lock (enabled)
  "Require an armed, drained capture gate for ON; defer OFF while armed."
  (if enabled
      (progn
        (sb-thread:with-mutex (*xid-lock*)
          (unless (plusp (hash-table-count *logical-capture-arming*))
            (wal-invalid-input "logical activation requires an armed capture gate"))
          (when (loop for xid in *active-xids*
                        thereis (member (gethash xid *logical-capture-xid-modes*) '(:skip :skip-written)))
            (wal-invalid-input "logical activation requires uncaptured XIDs to drain")))
        t)
      (cond
        ((wal-logical-interest-present-under-lock-p)
         (wal-invalid-input "logical disable conflicts with active logical interest"))
        ((plusp (wal-logical-arm-count-under-lock)) :deferred)
        (t t))))

(defvar *wal-logical-policy-publication-reason* :slot)

(defun wal-logical-policy-log-change (enabled)
  (if (eq *wal-logical-policy-publication-reason* :startup)
      (admin-log "update logical decoding status to ~d at end of recovery" (if enabled 1 0))
      (admin-log (if enabled
                     "logical decoding is enabled upon creating a new logical replication slot"
                     "logical decoding is disabled because there are no valid logical replication slots"))))

(defun wal-report-logical-decoding-status-under-lock (enabled incarnation begin-publication)
  "Publish a durable logical status transition for INCARNATION under WAL lock.
ON forces capture before writing the record; slot publication must wait for
its flush. OFF requires no logical interest and is deferred while armed; it
removes the force only after flush. An incomplete publication poisons WAL."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (member enabled '(nil t)) (error "invalid logical decoding status"))
  (prevent-recovery-primary-operation "logical decoding status publication")
  (unless (eq incarnation *wal-storage-incarnation*)
    (wal-invalid-input "logical status request belongs to replaced storage"))
  (let* ((policy (wal-checked-logical-policy-under-lock))
         (prior (and policy (wal-logical-policy-state policy))))
    (if (and prior (eq enabled (wal-logical-status-enabled prior)))
        (progn
          (when (and (not enabled)
                     (sb-thread:with-mutex (*xid-lock*)
                       (wal-logical-info-capture-under-xid-lock-p)))
            (when (eq :deferred (wal-check-logical-policy-transition-under-lock nil))
              (return-from wal-report-logical-decoding-status-under-lock :deferred)))
          (wal-logical-set-pending-under-lock policy nil)
          (wal-logical-info-publish-under-lock enabled)
          (setf *wal-logical-info-disable-pending* nil)
          (wal-logical-status-end prior))
        (progn
          (when (eq :deferred (wal-check-logical-policy-transition-under-lock enabled))
            (return-from wal-report-logical-decoding-status-under-lock :deferred))
          (wal-settle-page-publications-under-lock)
          (wal-require-no-pending-storage)
          (let* ((payload (wal-encode-logical-decoding-status-change enabled))
                 (size (+ +wal-header-size+ (length payload)))
                 (batch (wal-next-record-start +wal-header-size+))
                 (start (wal-record-start-after (+ batch +wal-header-size+) size))
                 (end (+ start size))
                 (record (make-wal-record :version +wal-version+
                                          :type +wal-record-logical-decoding-status-change+
                                          :xid 0 :start start :end end :payload payload))
                 (candidate (%make-wal-logical-policy
                             incarnation (wal-logical-status-next prior record *wal-insert-timeline*) nil)))
            (funcall begin-publication)
            (with-wal-poison-on-incomplete ("logical status publication did not complete")
              (when enabled
                (wal-set-logical-capture-under-lock t)
                (wal-logical-info-publish-under-lock t))
              (wal-logical-policy-test-point :before-record candidate)
              (unless (= batch (wal-append-batch-start))
                (error "logical status batch placement changed"))
              (unless (= end (wal-append-record +wal-record-logical-decoding-status-change+ 0 payload))
                (error "logical status record placement changed"))
              (wal-logical-policy-test-point :after-record candidate)
              (wal-flush)
              (unless (<= end *wal-durable-lsn*) (error "logical status is not durable"))
              (wal-logical-policy-test-point :after-flush candidate)
              (setf *wal-logical-policy* candidate)
              (wal-set-logical-capture-under-lock enabled)
              (unless enabled (wal-logical-info-publish-under-lock nil))
              (setf *wal-logical-info-disable-pending* nil)
              (wal-logical-policy-test-point :after-state candidate)
              (wal-logical-policy-log-change enabled)
              end))))))

(defun wal-report-logical-decoding-status (enabled &optional (incarnation *wal-storage-incarnation*))
  "Report status, holding an arm through the whole ON publication."
  (unless (member enabled '(nil t)) (error "invalid logical decoding status"))
  (flet ((publish ()
           (with-wal-publication-fence (begin-publication)
             (wal-report-logical-decoding-status-under-lock enabled incarnation #'begin-publication))))
    (if enabled
        (call-with-logical-capture-armed
         #'publish :activation (make-wal-logical-info-activation :incarnation incarnation))
        (publish))))

(defun wal-process-pending-logical-disable-under-lock (begin-publication)
  "Process queued disable without waiting. Clear it if interest returns; defer
while an arm or activation remains. Otherwise publish durable OFF, or clear
provisional logical info without an OFF record if ON was never durable."
  (let* ((policy (wal-checked-logical-policy-under-lock))
         (state (and policy (wal-logical-policy-state policy)))
         (pending (or *wal-logical-info-disable-pending*
                      (and policy (wal-logical-policy-pending-disable policy)))))
    (when (and pending (not (recovery-in-progress-p)))
      (cond
        ((wal-logical-interest-present-under-lock-p)
         (when policy (wal-logical-set-pending-under-lock policy nil))
         (setf *wal-logical-info-disable-pending* nil))
        ((or (plusp (wal-logical-arm-count-under-lock))
             (sb-thread:with-mutex (*xid-lock*)
               (plusp (hash-table-count *wal-logical-info-activations*))))
         :deferred)
        ((and state (wal-logical-status-enabled state))
         (wal-report-logical-decoding-status-under-lock
          nil *wal-storage-incarnation* begin-publication))
        (t
         (funcall begin-publication)
         (with-wal-poison-on-incomplete ("logical info disable did not complete")
           (wal-logical-info-publish-under-lock nil)
           (when policy (wal-logical-set-pending-under-lock policy nil))
           (setf *wal-logical-info-disable-pending* nil)
           :disabled))))))

(defun wal-reconcile-configured-logical-policy ()
  "Report the startup status after recovery and slot loading."
  (let ((*wal-logical-policy-publication-reason* :startup)
        (enabled (sb-thread:with-mutex (*wal-lock*)
                   (wal-logical-interest-present-under-lock-p))))
    (wal-report-logical-decoding-status (not (null enabled)))))

(defun wal-logical-note-published-checkpoint (snapshot)
  "Advance policy evidence only after checkpoint control is published.
An established status must be present in the checkpoint witness."
  (let* ((policy (wal-checked-logical-policy-under-lock))
         (state (and policy (wal-logical-policy-state policy))))
    (when (or state (get-properties snapshot '(:logical-decoding-enabled)))
      (unless (get-properties snapshot '(:logical-decoding-enabled))
        (error "checkpoint omitted established logical status"))
      (setf *wal-logical-policy*
            (%make-wal-logical-policy
             *wal-storage-incarnation*
             (wal-logical-status-next
              state (make-wal-record :version +wal-version+ :type +wal-record-checkpoint+
                                     :xid 0 :start (getf snapshot :start) :end (getf snapshot :end)
                                     :payload (wal-encode-checkpoint-snapshot snapshot))
              *wal-insert-timeline*)
             (and policy (wal-logical-policy-pending-disable policy)))))))
