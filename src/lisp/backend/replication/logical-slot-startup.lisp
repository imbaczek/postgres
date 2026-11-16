(in-package #:pg)

(defun logical-slot-startup-prepare-known-candidates (candidates prepare-known)
  "Preflight names, then prepare known candidates in snapshot order.
The snapshot pairs each exact slot with its encoded image. Child-owned results
must be that same candidate. Recheck list topology and images before cleanup."
  (let ((names (make-hash-table :test #'equal))
        (prepared (make-hash-table :test #'eq)))
    (dolist (slot candidates)
      (let ((name (wal-replication-slot-name slot)))
        (when (gethash name names) (error "duplicate WAL replication slot ~a" name))
        (setf (gethash name names) t)))
    (let ((images (mapcar (lambda (slot)
                            (cons slot (logical-slot-startup-candidate-image slot)))
                          candidates)))
      (loop for entry in images for slot = (car entry)
            do (when (logical-slot-startup-known-p slot)
                 (when (recovery-in-progress-p)
                   (error "logical slot restoration during native recovery is unsupported"))
                 (unless prepare-known (error "known logical slot startup requires an owned source"))
                 (let ((result (funcall prepare-known slot)))
                   (when (and (wal-replication-slot-child-owner slot) (not (eq slot result)))
                     (error "imported startup preparation must return the exact captured candidate"))
                   (setf (gethash slot prepared) result))))
      (loop with remaining = candidates
            for (slot . image) in images
            do (unless (and (consp remaining) (eq slot (car remaining))
                            (equalp image (logical-slot-startup-candidate-image slot)))
                 (error "startup preparation changed a persistent candidate"))
               (setf remaining (cdr remaining))
            finally (unless (null remaining)
                      (error "startup preparation changed the candidate list"))))
    prepared))

(defun logical-slot-prepare-startup-inventory (candidates segments inactive-since &optional prepare-known)
  "Prepare derived state while the caller's source registry is visible.
After the callbacks and checks, build a private replacement with duplicate,
capacity, standby and progress checks. Publication is separate."
  (let ((prepared (logical-slot-startup-prepare-known-candidates candidates prepare-known))
        (restored (make-hash-table :test #'equal)))
    (let ((*wal-replication-slots* restored))
      (dolist (slot candidates)
        (let ((name (wal-replication-slot-name slot)))
          (when (gethash name restored) (error "duplicate WAL replication slot ~a" name))
          (when (if (wal-physical-slot-p slot)
                    (physical-slot-restore slot inactive-since)
                    (progn
                      (when (recovery-in-progress-p)
                        (error "logical slot restoration during native recovery is unsupported"))
                      (if (logical-slot-startup-known-p slot)
                          (gethash slot prepared)
                          (wal-validate-replication-slot-wal slot segments))))
            (wal-check-replication-slot-capacity)
            (setf (gethash name restored) slot))))
      (when (and (recovery-in-progress-p) (plusp (hash-table-count restored)))
        (physical-slot-check-standby-slots
         (loop for slot being the hash-values of restored collect slot) (physical-slot-startup-system-id)))
      (maphash (lambda (name slot)
                 (declare (ignore name))
                 (unless (logical-slot-startup-known-p slot) (wal-slot-rebuild-progress slot)))
               restored))
    restored))

(defun logical-slot-startup-check-inventory (source)
  ;; A selected inventory observes its own captured rows; ordinary startup
  ;; compares disk bytes after allowed temporary physical slot cleanup. Both
  ;; retain startup authority throughout the check.
  (logical-slot-startup-check-authority source)
  (when (logical-slot-startup-source-inventory source)
    (logical-slot-startup-inventory-call source :observe)
    (logical-slot-startup-check-authority source)
    (return-from logical-slot-startup-check-inventory t))
  (let ((expected
          (loop for entry in (logical-slot-startup-source-entries source)
                for slot = (logical-slot-startup-entry-slot entry)
                unless (and (wal-physical-slot-p slot) (wal-replication-slot-temporary-p slot))
                  collect (cons (wal-replication-slot-name slot) (logical-slot-startup-entry-bytes entry))))
        (actual (slot-state-map (wal-replication-slot-directory)
                                (lambda (bytes name) (cons name bytes)) :local-workspace t)))
    (unless (equalp (sort expected #'string< :key #'car) (sort actual #'string< :key #'car))
      (error "logical slot startup disk inventory changed")))
  (logical-slot-startup-check-authority source)
  t)

(defun logical-slot-startup-prepare-mirrors (restored)
  "Compute replacement mirrors in dynamic bindings without waking the writer.
The caller publishes the results before waking the writer."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((*wal-replication-slots* restored)
        (*logical-capture-live-slots* *logical-capture-live-slots*)
        (*wal-logical-policy* *wal-logical-policy*)
        (*wal-logical-info-disable-pending* *wal-logical-info-disable-pending*))
    (wal-refresh-logical-capture-slot-mirror-under-wal-lock :notify nil)
    (values *logical-capture-live-slots* *wal-logical-policy* *wal-logical-info-disable-pending*)))

(defun logical-slot-startup-publish (source restored &optional archive)
  ;; Finish all fallible preparation and checks before fixed publication under
  ;; WAL then XID lock. The archive lease, when present, still spans this step.
  (when (logical-slot-imported-startup-source-p source)
    (error "imported startup publication requires its one-shot publication owner"))
  (logical-slot-startup-check-inventory source)
  (sb-thread:with-mutex (*wal-lock*)
    (logical-slot-startup-check-source source)
    (unless (and (eq *wal-replication-slots* (logical-slot-startup-source-registry source))
                 (<= (hash-table-count restored) *max-replication-slots*))
      (error "logical slot startup publication context changed"))
    (multiple-value-bind (count policy disable-pending)
        (logical-slot-startup-prepare-mirrors restored)
      (logical-slot-startup-check-source source)
      (when archive (logical-wal-archive-check archive))
      (sb-thread:with-mutex (*xid-lock*)
        (sb-sys:without-interrupts
          (setf *wal-replication-slots* restored
                *logical-capture-live-slots* count
                *wal-logical-policy* policy
                *wal-logical-info-disable-pending* disable-pending)))
      (when (or disable-pending
                (and policy (wal-logical-policy-pending-disable policy)))
        (notify-primary-wal-writer-under-lock))))
  (values))

(defun logical-slot-startup-restore (source segments inactive-since)
  ;; Known lost slots still undergo identity checks in the source scope, but
  ;; only reserved slots need eligibility checks and pending-XID reconstruction.
  (let ((candidates (mapcar #'logical-slot-startup-entry-slot
                            (logical-slot-startup-source-entries source))))
    (flet ((prepare (archive)
             (let ((restored
                     (logical-slot-prepare-startup-inventory
                      candidates segments inactive-since
                      (lambda (slot)
                        (when (eq :reserved (wal-replication-slot-wal-status slot))
                          (logical-slot-startup-check-eligibility source slot)
                          (let ((pending (logical-slot-startup-derive-pending source slot archive)))
                            (setf (wal-replication-slot-pending-xids slot) pending)))
                        slot))))
               (logical-slot-startup-publish source restored archive))))
      (if (logical-slot-startup-source-history source)
          (call-with-logical-slot-startup-archive source #'prepare)
          (prepare nil)))))

(declaim (ftype function logical-slot-continuation-restore-startup))

(defun logical-slot-load-startup-inventory (gate-owner ownership)
  (when (or gate-owner ownership)
    (check-primary-storage-startup-context gate-owner ownership))
  (when (and gate-owner ownership (logical-slot-continuation-gc-fenced-p *pgdata*))
    (unless (logical-slot-continuation-restore-startup gate-owner ownership)
      (call-with-logical-slot-startup-source
       gate-owner ownership nil
       (lambda (source)
         (logical-slot-startup-restore source (wal-discover-segments)
                                       (postgres-current-timestamp)))))
    (return-from logical-slot-load-startup-inventory (values)))
  (let* ((entries (logical-slot-startup-read-inventory :cleanup (and gate-owner ownership)
                                                       :local-workspace t))
         (candidates (mapcar #'logical-slot-startup-entry-slot entries))
         (segments (wal-discover-segments))
         (inactive-since (postgres-current-timestamp)))
    (cond
      ((or gate-owner ownership (some #'logical-slot-startup-known-p candidates))
       (call-with-logical-slot-startup-source
        gate-owner ownership entries
        (lambda (source) (logical-slot-startup-restore source segments inactive-since))))
      (t
       (logical-slot-check-startup-identities candidates)
       (logical-slot-check-startup-history candidates)
       (let ((restored (logical-slot-prepare-startup-inventory candidates segments inactive-since)))
         (setf *wal-replication-slots* restored)))))
  (values))
