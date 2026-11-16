(in-package #:pg)

(declaim (ftype function check-primary-storage-startup-context))

(declaim (ftype function call-with-logical-slot-imported-startup-principal
                logical-slot-imported-startup-principal-under-lock))

(declaim (ftype function call-with-logical-slot-imported-startup-capture
                logical-slot-imported-startup-capture-p
                logical-slot-imported-startup-capture-call
                %make-logical-slot-imported-startup-source
                logical-slot-copy-descriptor-list-count))

(defvar *logical-slot-startup-sources* nil)

(defvar *logical-slot-startup-inventories*
  (make-hash-table :test #'eq :weakness :key :synchronized t))

(defun logical-slot-startup-inventory-dispatch (inventory operation &optional source)
  ;; Dispatch only the registered capability. Loss of registration must not
  ;; fall back to the ordinary disk inventory path.
  (let ((state (and (logical-slot-startup-inventory-p inventory)
                    (gethash inventory *logical-slot-startup-inventories*))))
    (unless (and (functionp state) (eq state (logical-slot-startup-inventory-state inventory)))
      (when (and (logical-slot-startup-inventory-p inventory)
                 (logical-slot-startup-source-p source)
                 (eq inventory (logical-slot-startup-source-inventory source))
                 (eq (logical-slot-startup-source-thread source) sb-thread:*current-thread*)
                 (eq (logical-slot-startup-source-mutex source) *wal-lock*)
                 (member source *logical-slot-startup-sources* :test #'eq))
        (setf (logical-slot-startup-source-active-p source) nil))
      (error "startup inventory requires its registered capability"))
    (funcall state operation inventory source)))

(defun logical-slot-startup-inventory-call (source operation)
  (unless (logical-slot-imported-startup-source-p source)
    (error "selected inventory requires its typed imported startup source"))
  (logical-slot-startup-inventory-dispatch
   (logical-slot-startup-source-inventory source) operation source))

(defun logical-slot-startup-known-p (slot)
  (and (wal-logical-slot-p slot) (wal-replication-slot-logical-provenance slot)))

(defun logical-slot-startup-candidate-image (slot)
  (if (wal-physical-slot-p slot)
      (let ((copy (wal-copy-replication-slot-with-profile slot)))
        (setf (wal-replication-slot-inactive-since copy) 0)
        (wal-encode-replication-slot copy))
      (wal-encode-replication-slot slot)))

(defun logical-slot-startup-entry-from-image (bytes name)
  (let ((slot (wal-decode-replication-slot bytes name)))
    (%make-logical-slot-startup-entry
     :slot slot :bytes (copy-seq bytes) :image (logical-slot-startup-candidate-image slot)
     :profile (wal-slot-profile slot) :provenance (wal-replication-slot-logical-provenance slot))))

(defun logical-slot-startup-read-inventory (&key cleanup root parent-fd local-workspace)
  "Read canonical slot state into startup entries.
Inventory is strict by default.  Local startup, capture, recovery and
promotion may explicitly skip the local workspace; incoming images stay strict."
  (apply #'slot-state-map
         (or root (wal-replication-slot-directory))
         #'logical-slot-startup-entry-from-image
         :cleanup cleanup :local-workspace local-workspace
         (when parent-fd (list :parent-fd parent-fd))))

(defun logical-slot-startup-control-image ()
  (read-bounded-regular-file (wal-control-path) (1+ +wal-control-maximum-bytes+) :minimum 1))

(defun logical-slot-startup-check-live (source)
  (unless (and (logical-slot-startup-source-p source)
               (logical-slot-startup-source-active-p source)
               (eq (logical-slot-startup-source-thread source) sb-thread:*current-thread*)
               (eq (logical-slot-startup-source-mutex source) *wal-lock*)
               (member source *logical-slot-startup-sources* :test #'eq))
    (error "logical slot startup source scope ended or changed thread"))
  (when (logical-slot-startup-source-inventory source)
    (logical-slot-startup-inventory-call source :check))
  source)

(defun logical-slot-startup-check-authority (source)
  ;; Check the drained gate and postmaster ownership outside the WAL mutex.
  ;; When needed, the read-only system ID lookup parses the full catalog on
  ;; each check; avoiding repeated parses is a performance followup.
  (logical-slot-startup-check-live source)
  (when (sb-thread:holding-mutex-p *wal-lock*)
    (error "startup authority must be checked before taking the WAL mutex"))
  (check-primary-storage-startup-context
   (logical-slot-startup-source-gate-owner source) (logical-slot-startup-source-ownership source))
  (let ((system-id (logical-slot-startup-source-system-id source)))
    (when (and system-id (not (eql system-id (physical-slot-startup-system-id))))
      (error "logical slot startup durable catalog identity changed")))
  t)

(defun logical-slot-startup-check-source-facts (source)
  ;; Under the WAL mutex, compare the frozen source and candidates with the
  ;; current post-replay state.  This does not prove slot lifetime or import.
  (unless (and *wal-ready* (not (recovery-in-progress-p))
               (eq (logical-slot-startup-source-incarnation source) *wal-storage-incarnation*)
               (eql (logical-slot-startup-source-timeline source) *wal-insert-timeline*)
               (eq (logical-slot-startup-source-namespace source) *wal-segment-namespace*)
               (eq (logical-slot-startup-source-wal-level source) *wal-level*)
               (eql (logical-slot-startup-source-maximum source) *max-replication-slots*)
               (eq (logical-slot-startup-source-policy source) *wal-logical-policy*)
               (eql (logical-slot-startup-source-end source) *wal-durable-lsn*)
               (or (null (logical-slot-startup-source-system-id source))
                   (eql (logical-slot-startup-source-system-id source)
                        (physical-slot-startup-system-id)))
               (equal (logical-slot-startup-source-root source) (logical-wal-current-root-identity))
               (equalp (logical-slot-startup-source-control source) (logical-slot-startup-control-image))
               (or (null (logical-slot-startup-source-history source))
                   (equalp (logical-slot-startup-source-history-bytes source)
                           (unless (= *wal-insert-timeline* 1)
                             (timeline-history-bytes *wal-insert-timeline*)))))
    (error "logical slot startup source changed"))
  (wal-check-healthy)
  (dolist (entry (logical-slot-startup-source-entries source))
    (let ((slot (logical-slot-startup-entry-slot entry)))
      (unless (and (eq (logical-slot-startup-entry-profile entry) (wal-slot-profile slot))
                   (eq (logical-slot-startup-entry-provenance entry)
                       (wal-replication-slot-logical-provenance slot))
                   (equalp (logical-slot-startup-entry-image entry)
                           (logical-slot-startup-candidate-image slot)))
        (error "logical slot startup candidate changed"))))
  t)

(defun logical-slot-startup-check-source (source)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (logical-slot-startup-check-live source)
  (sb-sys:without-interrupts
    (let ((result (sb-sys:with-local-interrupts
                    (handler-case-noninterrupt (logical-slot-startup-check-source-facts source)
                                               (condition condition)))))
      (unless (eq t result)
        (setf (logical-slot-startup-source-active-p source) nil)
        (error result))
      (logical-slot-startup-check-live source)))
  t)

(defun %call-with-logical-slot-startup-source (gate-owner ownership entries function &optional capture inventory)
  "Capture startup source facts after independently checking the context.
Child-owned candidates require history even when all are lost. Capture alone
publishes nothing and does not authorize imported use."
  ;; Preflight before binding this one-use inventory to the imported source.
  (when inventory
    (unless capture (error "selected inventory requires an imported capture"))
    (logical-slot-startup-inventory-dispatch inventory :preflight))
  (check-primary-storage-startup-context gate-owner ownership)
  (when (and capture (not (logical-slot-imported-startup-capture-p capture)))
    (error "startup capture requires its typed imported purpose"))
  (let* ((slots (mapcar #'logical-slot-startup-entry-slot entries))
         (system-id (logical-slot-check-startup-candidate-identities slots))
         (history-required
           (or (some #'wal-replication-slot-child-owner slots)
               (some (lambda (slot)
                       (and (logical-slot-startup-known-p slot)
                            (eq :reserved (wal-replication-slot-wal-status slot))))
                     slots)))
         (source
           (sb-thread:with-mutex (*wal-lock*)
             (let* ((fields (multiple-value-list (wal-read-control-fields)))
                    (timeline *wal-insert-timeline*)
                    (bytes (when (and history-required (/= timeline 1)) (timeline-history-bytes timeline)))
                    (history (when history-required
                               (timeline-check-lineage
                                (timeline-parse-history timeline (if bytes (bytes->string bytes) ""))))))
               (unless (and *wal-ready* (not (recovery-in-progress-p))
                            (integerp (first fields)) (eql *wal-durable-lsn* (second fields))
                            (eql timeline (fourth fields)) (eq *wal-segment-namespace* (sixth fields))
                            (equalp *wal-control-metadata* (seventh fields)))
                 (error "logical slot startup requires accepted post-replay primary control"))
               (when history
                 (dolist (slot slots)
                   (when (and (logical-slot-startup-known-p slot)
                              (eq :reserved (wal-replication-slot-wal-status slot)))
                     (logical-slot-check-primary-history slot history))))
               (apply (if capture #'%make-logical-slot-imported-startup-source
                          #'%make-logical-slot-startup-source)
                      (append (when capture (list :capture capture))
                              (list :gate-owner gate-owner :ownership ownership :entries (copy-list entries) :inventory inventory
                                    :system-id system-id :root (logical-wal-current-root-identity)
                                    :history history :history-bytes bytes :control (logical-slot-startup-control-image)
                                    :timeline timeline :namespace *wal-segment-namespace* :end *wal-durable-lsn*
                                    :incarnation *wal-storage-incarnation* :wal-level *wal-level*
                                    :registry *wal-replication-slots* :maximum *max-replication-slots* :policy *wal-logical-policy*
                                    :thread sb-thread:*current-thread* :mutex *wal-lock* :active-p t)))))))
    (let ((*logical-slot-startup-sources* (cons source *logical-slot-startup-sources*)))
      (unwind-protect
           (progn
             (when capture (logical-slot-imported-startup-capture-call source :bind))
             (when inventory (logical-slot-startup-inventory-call source :bind))
             (logical-slot-startup-check-authority source)
             (sb-thread:with-mutex (*wal-lock*) (logical-slot-startup-check-source source))
             (funcall function source))
        (setf (logical-slot-startup-source-active-p source) nil)))))

(defun call-with-logical-slot-startup-source (gate-owner ownership entries function)
  "Capture ordinary startup facts after rejecting child-owned candidates."
  (check-primary-storage-startup-context gate-owner ownership)
  (logical-slot-check-startup-local-owners (mapcar #'logical-slot-startup-entry-slot entries))
  (%call-with-logical-slot-startup-source gate-owner ownership entries function))

(defun logical-slot-imported-startup-check-under-lock (source)
  "Check imported purpose, lifetime and captured in-memory source and candidate
facts under WAL exclusion. This does not recheck disk inventory bytes."
  (logical-slot-imported-startup-principal-under-lock source)
  t)

(defun call-with-logical-slot-imported-startup-source (gate-owner ownership entries function)
  "Observe child-owned candidates in a registered imported startup scope.
Check disk inventory at entry and normal exit. Under-WAL checks compare the
private snapshot and current source, but do not reread slot inventory."
  (check-primary-storage-startup-context gate-owner ownership)
  (logical-slot-copy-descriptor-list-count entries *max-replication-slots*)
  (unless (some (lambda (entry)
                  (wal-replication-slot-child-owner (logical-slot-startup-entry-slot entry)))
                entries)
    (error "imported logical startup candidates require a child-owned slot"))
  (call-with-logical-slot-imported-startup-capture
   (lambda (capture)
     (%call-with-logical-slot-startup-source
      gate-owner ownership entries
      (lambda (source)
        (call-with-logical-slot-imported-startup-principal source function))
      capture))))

(defun logical-slot-startup-start-position (slot history)
  ;; A restart may name a record start at a fork or the end of a prefix.
  (let ((start (wal-replication-slot-restart-lsn slot))
        (owner (wal-replication-slot-restart-timeline slot)))
    (cond
      ((zerop start)
       (unless (eql owner (timeline-at-point 0 history))
         (error "zero startup restart lost its record owner"))
       (values owner :record-start))
      ((not (timeline-prefix-boundary-p owner start history))
       (error "startup restart is outside selected history"))
      ((and (eql owner (timeline-at-point start history))
            (not (eql owner (timeline-at-point (1- start) history))))
       (values owner :record-start))
      (t (values owner :prefix-end)))))

(defun logical-slot-startup-local-anchor-p (slot history)
  ;; Only current-timeline anchors are eligible for this startup path.
  (let* ((tip (timeline-history-entry-timeline (first history)))
         (begin (timeline-history-entry-begin (first history)))
         (provenance (wal-replication-slot-logical-provenance slot))
         (confirmed (wal-replication-slot-confirmed-flush-lsn slot)))
    (and provenance
         (eql tip (logical-slot-provenance-birth-timeline provenance))
         (> (wal-replication-slot-consistent-lsn slot) begin)
         (eql tip (wal-replication-slot-restart-timeline slot))
         (>= (wal-replication-slot-restart-lsn slot) begin)
         (or (null confirmed)
             (and (eql tip (wal-replication-slot-confirmed-timeline slot)) (>= confirmed begin))))))

(defun call-with-logical-slot-startup-archive (source function)
  ;; Use only a retained root already restored in committed primary control.
  ;; The archive reader lease must cover preparation and publication; readable
  ;; archive bytes alone do not establish a slot's eligibility.
  (logical-slot-startup-check-authority source)
  (flet ((guard () (logical-slot-startup-check-source source)))
    (let ((present
            (sb-thread:with-mutex (*wal-lock*)
              (guard)
              (not (null (getf (seventh (multiple-value-list (wal-read-control-fields)))
                               :retained-reference))))))
      (if present
          (call-with-logical-wal-archive-core
           (logical-slot-startup-source-system-id source) (logical-slot-startup-source-history source)
           (logical-slot-startup-source-namespace source) #'guard
           (lambda ()
             (guard)
             (logical-slot-startup-source-system-id source))
           function)
          (funcall function nil)))))
