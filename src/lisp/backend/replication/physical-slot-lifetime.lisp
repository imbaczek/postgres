(in-package #:pg)

;; Track temporary physical slots for their owning backend generation.  The
;; weak tables bind slots to capabilities and capabilities to their state.
;; Operations below run under WAL exclusion unless noted otherwise.
(defvar *physical-slot-lifetimes*
  (make-hash-table :test #'eq :synchronized t :weakness :key))
(defvar *physical-slot-lifetime-seals*
  (make-hash-table :test #'eq :synchronized t :weakness :key))
(defvar *physical-slot-lifetime-create* nil)

(define-condition physical-slot-cold-cleanup-refusal (pg-error) ()
  (:default-initargs
   :sqlstate "55000"
   :message "continuation group is uncertain; cold reconciliation required"))

(defvar *physical-slot-cold-cleanup-refusals*
  (make-hash-table :test #'eq :synchronized t :weakness :key))

(declaim (ftype function physical-slot-lifetime-check-under-lock
                physical-slot-check-temporary-owner-under-lock))

(defun physical-slot-refuse-cold-cleanup-under-lock (slot check-group)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (physical-slot-lifetime-check-under-lock slot)
  (physical-slot-check-temporary-owner-under-lock slot *backend*)
  (funcall check-group)
  (let ((condition (make-condition 'physical-slot-cold-cleanup-refusal))
        (backend *backend*)
        (capability (wal-replication-slot-temporary-lifetime slot)))
    (setf (gethash condition *physical-slot-cold-cleanup-refusals*)
          (lambda (candidate creator)
            (unless (and (eq candidate slot) (eq creator backend)
                         (eq capability (wal-replication-slot-temporary-lifetime slot)))
              (error "cold cleanup refusal changed its slot or creator"))
            (physical-slot-lifetime-check-under-lock slot)
            (physical-slot-check-temporary-owner-under-lock slot creator)
            (funcall check-group)
            t))
    (error condition)))

(defun physical-slot-cold-cleanup-refusal-check-under-lock (condition slot backend)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((check (and (typep condition 'physical-slot-cold-cleanup-refusal)
                    (gethash condition *physical-slot-cold-cleanup-refusals*))))
    (unless check
      (error "unissued temporary physical cleanup refusal"))
    (funcall check slot backend)))

;; Callers carry this read-only capability; its sealed state checks the exact
;; slot and context before allowing an operation.
(defstruct (physical-slot-lifetime (:constructor %make-physical-slot-lifetime))
  (state nil :read-only t))

;; Keep the backend, registry and storage identities captured at creation,
;; together with the slot, phase and any outstanding drop claim.
(defstruct (physical-slot-lifetime-context
            (:constructor %make-physical-slot-lifetime-context))
  (backend nil :read-only t) (backend-token nil :read-only t)
  (registry nil :read-only t) (issued nil :read-only t)
  (name-index nil :read-only t)
  (incarnation nil :read-only t) (mutex nil :read-only t)
  (root nil :read-only t) (name nil :read-only t) (system-id nil :read-only t)
  slot (phase :unbound) claim claim-image)

(defvar *physical-slot-lifetime-names*
  (make-hash-table :test #'eq :synchronized t :weakness :key))

;; Keep a name bound to its original context and capability until removal has
;; finished, even if the registry entry is disturbed.
(defstruct (physical-slot-lifetime-name (:constructor %make-physical-slot-lifetime-name))
  (context nil :read-only t) (capability nil :read-only t))

(declaim (ftype function physical-slot-lifetime-check-context))

(defun physical-slot-lifetime-name-table (registry &optional create)
  (or (gethash registry *physical-slot-lifetime-names*)
      (when create
        (setf (gethash registry *physical-slot-lifetime-names*)
              (make-hash-table :test #'equal)))))

;; Check the name anchor before using the registry entry.  A lost binding or
;; failed context check makes the original slot uncertain.
(defun physical-slot-check-name-under-lock (name)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-sys:without-interrupts
    (let* ((names (physical-slot-lifetime-name-table *wal-replication-slots*))
           (anchor (and names (gethash name names))))
      (when anchor
        (let* ((context (physical-slot-lifetime-name-context anchor))
               (capability (physical-slot-lifetime-name-capability anchor))
               (slot (physical-slot-lifetime-context-slot context)))
          (handler-case
              (progn
                (unless (and (eq slot (gethash name *wal-replication-slots*))
                             (eq (physical-slot-lifetime-state capability)
                                 (gethash capability *physical-slot-lifetime-seals*)))
                  (error "temporary physical name lost its original generation"))
                (if (eq (physical-slot-lifetime-context-phase context) :cold-cleanup-required)
                    (progn
                      (physical-slot-lifetime-check-storage context)
                      (physical-slot-lifetime-check-registration context capability))
                    (physical-slot-lifetime-check-context context capability)))
            (error (condition)
              (setf (physical-slot-lifetime-context-phase context) :uncertain
                    (wal-replication-slot-state-uncertain-p slot) t)
              (error condition)))))))
  t)

(defun physical-slot-check-names-under-lock ()
  (let ((names (physical-slot-lifetime-name-table *wal-replication-slots*)))
    (when names
      (maphash (lambda (name anchor)
                 (declare (ignore anchor))
                 (physical-slot-check-name-under-lock name))
               names)))
  t)

(defun physical-slot-lifetime-retire-name (context capability)
  (let* ((names (physical-slot-lifetime-name-table
                 (physical-slot-lifetime-context-registry context)))
         (name (physical-slot-lifetime-context-name context))
         (anchor (and names (gethash name names))))
    (unless (and anchor
                 (eq context (physical-slot-lifetime-name-context anchor))
                 (eq capability (physical-slot-lifetime-name-capability anchor)))
      (error "temporary physical retirement lost its name anchor"))
    (remhash name names)))

;; Release the name only after retirement and removal from the registry.
(defun physical-slot-finish-name-removal-under-lock (slot)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let* ((names (physical-slot-lifetime-name-table *wal-replication-slots*))
         (name (wal-replication-slot-name slot))
         (anchor (and names (gethash name names))))
    (when anchor
      (let ((context (physical-slot-lifetime-name-context anchor))
            (capability (physical-slot-lifetime-name-capability anchor)))
        (unless (and (eq slot (physical-slot-lifetime-context-slot context))
                     (eq :retired (physical-slot-lifetime-context-phase context))
                     (null (gethash name *wal-replication-slots*)))
          (error "temporary physical name was not durably removed"))
        (physical-slot-lifetime-retire-name context capability))))
  t)

(declaim (ftype function physical-slot-state-valid-p))

(defun physical-slot-lifetime-root ()
  (let* ((path (namestring (uiop:ensure-directory-pathname *pgdata*)))
         (facts (physical-image-file-facts -100 path)))
    (list path (first facts) (second facts) (ninth facts))))

(defun physical-slot-lifetime-candidate-matches-p (context capability slot)
  (and (wal-replication-slot-p slot)
       (physical-slot-state-valid-p slot)
       (wal-replication-slot-temporary-p slot)
       (eq capability (wal-replication-slot-temporary-lifetime slot))
       (string= (physical-slot-lifetime-context-name context) (wal-replication-slot-name slot))
       (eql (physical-slot-lifetime-context-system-id context) (wal-replication-slot-system-id slot))
       (eq (physical-slot-lifetime-context-backend context)
           (wal-replication-slot-temporary-owner slot))))

(defun physical-slot-lifetime-check-storage (context)
  (assert (and (eq *wal-lock* (physical-slot-lifetime-context-mutex context))
               (sb-thread:holding-mutex-p *wal-lock*)))
  (unless (and (eq *wal-replication-slots* (physical-slot-lifetime-context-registry context))
               (eq *physical-slot-lifetimes* (physical-slot-lifetime-context-issued context))
               (eq *physical-slot-lifetime-names* (physical-slot-lifetime-context-name-index context))
               (eq *wal-storage-incarnation* (physical-slot-lifetime-context-incarnation context))
               (equal (physical-slot-lifetime-root) (physical-slot-lifetime-context-root context)))
    (error "temporary physical slot changed its storage context")))

(defun physical-slot-lifetime-check-registration (context capability)
  (let ((slot (physical-slot-lifetime-context-slot context)))
    (when slot
      (let* ((names (physical-slot-lifetime-name-table *wal-replication-slots*))
             (anchor (and names (gethash (physical-slot-lifetime-context-name context) names))))
        (unless (and anchor
                     (eq context (physical-slot-lifetime-name-context anchor))
                     (eq capability (physical-slot-lifetime-name-capability anchor)))
          (error "temporary physical slot lost its name anchor")))
      (unless (and (eq slot (gethash (physical-slot-lifetime-context-name context)
                                     *wal-replication-slots*))
                   (eq capability (gethash slot *physical-slot-lifetimes*))
                   (not (wal-replication-slot-state-uncertain-p slot))
                   (physical-slot-lifetime-candidate-matches-p context capability slot))
        (error "temporary physical slot lost its exact registered generation"))))
  t)

;; Recheck captured storage and backend identities and, once bound, the exact
;; slot and name anchor.  The caller holds the original WAL mutex.
(defun physical-slot-lifetime-check-context (context capability)
  (physical-slot-lifetime-check-storage context)
  (backend-lifetime-check-under-wal-lock (physical-slot-lifetime-context-backend-token context))
  (physical-slot-lifetime-check-registration context capability))

;; Check that the creator is still registered on its owning thread.
(defun physical-slot-lifetime-check-owner (context backend)
  (unless (eq backend (physical-slot-lifetime-context-backend context))
    (ereport "55006" "temporary physical slot belongs to another backend"))
  (unless (eq (physical-slot-lifetime-context-backend-token context)
              (backend-lifetime-capture-under-wal-lock backend))
    (error "temporary physical slot changed its backend registration")))

;; Bind the new slot and reserve its name before publishing the slot state.
(defun physical-slot-lifetime-bind (context capability slot)
  (unless (and (eq :unbound (physical-slot-lifetime-context-phase context))
               (null (physical-slot-lifetime-context-slot context))
               (null (gethash (physical-slot-lifetime-context-name context) *wal-replication-slots*))
               (physical-slot-lifetime-candidate-matches-p context capability slot))
    (error "temporary physical lifetime cannot bind this slot"))
  (physical-slot-lifetime-check-owner context *backend*)
  (physical-slot-lifetime-check-context context capability)
  (setf (physical-slot-lifetime-context-slot context) slot
        (physical-slot-lifetime-context-phase context) :creating
        (gethash slot *physical-slot-lifetimes*) capability)
  (let ((names (physical-slot-lifetime-name-table *wal-replication-slots* t)))
    (when (gethash (physical-slot-lifetime-context-name context) names)
      (error "temporary physical name is still reserved"))
    (setf (gethash (copy-seq (physical-slot-lifetime-context-name context)) names)
          (%make-physical-slot-lifetime-name :context context :capability capability)))
  slot)

;; The publication receipt distinguishes an unpublished failure from one that
;; may have reached disk.  Only the former can release the name immediately;
;; an uncertain publication remains fenced for recovery.
(defun physical-slot-lifetime-finish-create (context capability slot receipt completed)
  (assert (and (eq *wal-lock* (physical-slot-lifetime-context-mutex context))
               (sb-thread:holding-mutex-p *wal-lock*)))
  (unless (and (eq capability *physical-slot-lifetime-create*)
               (eq slot (physical-slot-lifetime-context-slot context)))
    (error "temporary physical creation lost its exact scope"))
  (if completed
      (progn
        (unless (and (member (physical-slot-lifetime-context-phase context) '(:creating :live))
                     (eq (car receipt) :published))
          (error "temporary physical creation did not publish its state"))
        (handler-case
            (progn
              (physical-slot-lifetime-check-context context capability)
              (setf (physical-slot-lifetime-context-phase context) :live))
          (error (condition)
            (setf (physical-slot-lifetime-context-phase context) :uncertain
                  (wal-replication-slot-state-uncertain-p slot) t)
            (error condition))))
      (if (eq (car receipt) :unpublished)
          (progn
            (physical-slot-lifetime-retire-name context capability)
            (setf (physical-slot-lifetime-context-phase context) :retired)
            (remhash slot (physical-slot-lifetime-context-issued context))
            (remhash capability *physical-slot-lifetime-seals*))
          (setf (physical-slot-lifetime-context-phase context) :uncertain)))
  slot)

(defun physical-slot-lifetime-check-candidate (context capability candidate)
  (unless (physical-slot-lifetime-candidate-matches-p context capability candidate)
    (error "physical candidate did not retain its temporary lifetime")))

;; Enforce the creation, live and retirement phases under WAL exclusion.
;; Retirement holds the exact drop claim until completion; a failed context
;; check marks the slot uncertain.
(defun physical-slot-lifetime-dispatch (context capability operation slot value)
  (assert (and (eq *wal-lock* (physical-slot-lifetime-context-mutex context))
               (sb-thread:holding-mutex-p *wal-lock*)))
  (unless (eq slot (physical-slot-lifetime-context-slot context))
    (error "copied slot cannot use a temporary physical lifetime"))
  (let ((phase (physical-slot-lifetime-context-phase context)))
    (when (eq operation :cold-cleanup-retained-p)
      (return-from physical-slot-lifetime-dispatch (eq phase :cold-cleanup-required)))
    (when (eq operation :adopt-cold-cleanup)
      (unless (and (eq phase :live) (null (wal-replication-slot-drop-claim slot)))
        (error "cold cleanup requires a live temporary slot without a drop claim"))
      (physical-slot-cold-cleanup-refusal-check-under-lock value slot *backend*)
      (sb-sys:without-interrupts
        (request-primary-wal-writer-shutdown)
        (setf (physical-slot-lifetime-context-phase context) :cold-cleanup-required)
        (unless *physical-slot-cold-cleanup-owner*
          (setf *physical-slot-cold-cleanup-owner* *backend*)))
      (return-from physical-slot-lifetime-dispatch slot))
    (ecase operation
      ((:live :candidate)
       (unless (and (eq phase :live) (null (wal-replication-slot-drop-claim slot)))
         (error "temporary physical slot is not live"))
       (when (eq operation :candidate)
         (physical-slot-lifetime-check-candidate context capability value)))
      (:creating
       (unless (and (eq phase :creating) (eq capability *physical-slot-lifetime-create*))
         (error "temporary physical creation requires its exact scope")))
      ((:owner :drop-check :begin-drop :finish-drop)
       (unless (member phase '(:live :retiring))
         (error "temporary physical slot cannot be retired in its current phase"))
       (physical-slot-lifetime-check-owner context (if (eq operation :owner) value *backend*))
       (when (eq phase :retiring)
         (unless (and (eq (physical-slot-lifetime-context-claim context)
                          (wal-replication-slot-drop-claim slot))
                      (equal (physical-slot-lifetime-context-claim-image context)
                             (wal-replication-slot-drop-claim slot)))
           (error "temporary physical drop changed its exact claim")))
       (when (and (eq operation :finish-drop) (not (eq phase :retiring)))
         (error "temporary physical drop did not enter retirement"))))
    (handler-case
        (progn
          (physical-slot-lifetime-check-context context capability)
          (case operation
            (:begin-drop
             (unless (wal-replication-slot-drop-claim slot)
               (error "temporary physical drop requires an inode claim"))
             (when (eq phase :live)
               (setf (physical-slot-lifetime-context-claim-image context)
                     (let ((claim (wal-replication-slot-drop-claim slot)))
                       (cons (copy-seq (first claim)) (copy-list (rest claim))))
                     (physical-slot-lifetime-context-claim context)
                     (wal-replication-slot-drop-claim slot)
                     (physical-slot-lifetime-context-phase context) :retiring)))
            (:finish-drop
             (setf (physical-slot-lifetime-context-phase context) :retired)
             (remhash slot *physical-slot-lifetimes*)
             (remhash capability *physical-slot-lifetime-seals*)))
          slot)
      (error (condition)
        (setf (physical-slot-lifetime-context-phase context) :uncertain
              (wal-replication-slot-state-uncertain-p slot) t)
        (error condition)))))

(declaim (ftype function physical-slot-check-live-admission))

(defun physical-slot-registered-creator-p (backend)
  (and backend
       (sb-ext:hash-table-synchronized-p *backend-registry*)
       (eq backend (gethash (backend-pid backend) *backend-registry*))
       (eq (backend-thread backend) sb-thread:*current-thread*)
       (eql (backend-pid backend) (sb-thread:thread-os-tid sb-thread:*current-thread*))))

(defun physical-slot-check-temporary-creation-admission ()
  (when (physical-slot-registered-creator-p *backend*)
    (physical-slot-check-live-admission)))

;; Create a sealed capability and callbacks for a registered backend.  An
;; unregistered creator gets no lifetime here, preserving the legacy fixture
;; path. Strict runtime validation rejects temporary slots without a lifetime.
(defun physical-slot-make-lifetime-under-lock (name system-id receipt)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((backend *backend*))
    (when (physical-slot-registered-creator-p backend)
      (physical-slot-check-live-admission)
      (unless (and (consp receipt) (eq (car receipt) :unpublished) (null (cdr receipt)))
        (error "temporary physical creation requires a fresh publication receipt"))
      (let ((context (%make-physical-slot-lifetime-context
                      :backend backend :backend-token (backend-lifetime-capture-under-wal-lock backend)
                      :registry *wal-replication-slots* :issued *physical-slot-lifetimes*
                      :name-index *physical-slot-lifetime-names*
                      :incarnation *wal-storage-incarnation* :mutex *wal-lock*
                      :root (physical-slot-lifetime-root) :name (copy-seq name) :system-id system-id))
            (capability nil))
        (setf capability
              (%make-physical-slot-lifetime
               :state (lambda (object operation slot value)
                        (unless (eq object capability)
                          (error "copied temporary physical lifetime"))
                        (physical-slot-lifetime-dispatch context capability operation slot value))))
        (setf (gethash capability *physical-slot-lifetime-seals*) (physical-slot-lifetime-state capability))
        (values capability
                (lambda (slot)
                  (sb-sys:without-interrupts
                    (physical-slot-lifetime-bind context capability slot)))
                (lambda (slot completed)
                  (sb-sys:without-interrupts
                    (physical-slot-lifetime-finish-create context capability slot receipt completed))))))))

;; Check the independent seal before dispatch.  If the live slot lost its
;; issued capability, mark it uncertain rather than allowing further use.
(defun physical-slot-lifetime-call-under-lock (capability operation slot &optional value)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-sys:without-interrupts
    (let ((state (and (physical-slot-lifetime-p capability)
                      (physical-slot-lifetime-state capability))))
      (unless (and (functionp state) (eq state (gethash capability *physical-slot-lifetime-seals*)))
        (when (and capability (eq slot (gethash (wal-replication-slot-name slot) *wal-replication-slots*))
                   (eq capability (gethash slot *physical-slot-lifetimes*))
                   (eq capability (wal-replication-slot-temporary-lifetime slot)))
          (setf (wal-replication-slot-state-uncertain-p slot) t))
        (error "unissued temporary physical lifetime"))
      (funcall state capability operation slot value))))

(defun physical-slot-lifetime-check-under-lock (slot)
  (physical-slot-lifetime-call-under-lock
   (wal-replication-slot-temporary-lifetime slot) :live slot))

(defun physical-slot-check-temporary-live-under-lock (slot)
  (when (or (wal-replication-slot-temporary-lifetime slot)
            (gethash slot *physical-slot-lifetimes*))
    (physical-slot-lifetime-check-under-lock slot))
  slot)

;; A copied candidate must carry the same lifetime as the registered slot;
;; the capability then checks the candidate's temporary identity.
(defun physical-slot-check-temporary-candidate-under-lock (candidate)
  (let ((slot (gethash (wal-replication-slot-name candidate) *wal-replication-slots*)))
    (when (or (wal-replication-slot-temporary-lifetime candidate)
              (and slot (gethash slot *physical-slot-lifetimes*)))
      (unless (and slot (eq (wal-replication-slot-temporary-lifetime candidate)
                            (wal-replication-slot-temporary-lifetime slot)))
        (error "temporary physical candidate changed its live lifetime"))
      (physical-slot-lifetime-call-under-lock
       (wal-replication-slot-temporary-lifetime slot) :candidate slot candidate)))
  candidate)

(defun physical-slot-check-temporary-owner-under-lock (slot backend)
  (when (or (wal-replication-slot-temporary-lifetime slot)
            (gethash slot *physical-slot-lifetimes*))
    (physical-slot-lifetime-call-under-lock
     (wal-replication-slot-temporary-lifetime slot) :owner slot backend))
  t)

(defun physical-slot-temporary-drop-under-lock (slot operation)
  (when (or (wal-replication-slot-temporary-lifetime slot)
            (gethash slot *physical-slot-lifetimes*))
    (physical-slot-lifetime-call-under-lock
     (wal-replication-slot-temporary-lifetime slot) operation slot))
  slot)
