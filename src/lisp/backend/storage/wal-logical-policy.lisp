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

(defstruct (wal-logical-policy
            (:constructor %make-wal-logical-policy (incarnation state pending-disable)))
  "Logical status evidence owned by one WAL incarnation.
STATE NIL means unknown, not OFF; PENDING-DISABLE requests a later OFF
transition while an enabled state is still in force."
  (incarnation nil :read-only t)
  (state nil :read-only t)
  (pending-disable nil :read-only t))

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
  (setf *wal-logical-policy* (%make-wal-logical-policy incarnation state nil))
  (wal-set-logical-capture-under-lock (and state (wal-logical-status-enabled state))))

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
      (sb-thread:with-mutex (*xid-lock*) *logical-capture-force*)))

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
  (when (and *wal-ready* (not *wal-poisoned*) (wal-logical-policy-p *wal-logical-policy*))
    (let* ((policy (wal-checked-logical-policy-under-lock))
           (state (wal-logical-policy-state policy))
           (pending (and (eq *wal-level* :replica) (zerop count)
                         state (wal-logical-status-enabled state))))
      (wal-logical-set-pending-under-lock policy (not (null pending)))
      (when pending (notify-primary-wal-writer-under-lock))))
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
          (wal-logical-set-pending-under-lock policy nil)
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
              (when enabled (wal-set-logical-capture-under-lock t))
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
              (wal-logical-policy-test-point :after-state candidate)
              (wal-logical-policy-log-change enabled)
              end))))))

(defun wal-report-logical-decoding-status (enabled &optional (incarnation *wal-storage-incarnation*))
  "Report status, holding an arm through the whole ON publication."
  (unless (member enabled '(nil t)) (error "invalid logical decoding status"))
  (flet ((publish ()
           (with-wal-publication-fence (begin-publication)
             (wal-report-logical-decoding-status-under-lock enabled incarnation #'begin-publication))))
    (if enabled (call-with-logical-capture-armed #'publish) (publish))))

(defun wal-process-pending-logical-disable-under-lock (begin-publication)
  "Cancel, defer or publish a pending OFF after rechecking current interest."
  (let* ((policy (wal-checked-logical-policy-under-lock))
         (pending (and policy (wal-logical-policy-pending-disable policy))))
    (when (and pending (not (recovery-in-progress-p)))
      (cond
        ((wal-logical-interest-present-under-lock-p)
         (wal-logical-set-pending-under-lock policy nil))
        ((plusp (wal-logical-arm-count-under-lock)) :deferred)
        (t (wal-report-logical-decoding-status-under-lock
            nil *wal-storage-incarnation* begin-publication))))))

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
