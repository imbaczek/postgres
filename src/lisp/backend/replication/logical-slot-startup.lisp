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

(defun logical-slot-prepare-startup-inventory (candidates segments inactive-since &optional prepare-known
                                                                                    (check-capacity #'wal-check-replication-slot-capacity))
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
            (funcall check-capacity)
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

(declaim (ftype function logical-slot-continuation-cold-empty-call))

(defun logical-slot-startup-publish (source restored &optional archive empty-handoff)
  ;; Finish preparation before fixed publication under WAL then XID exclusion.
  ;; The archive lease, when present, still spans the publication.
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
      (let ((commit (when empty-handoff
                      (logical-slot-continuation-cold-empty-call
                       empty-handoff :prepare (list source restored)))))
        (flet ((install ()
                 (logical-slot-startup-check-source source)
                 (when archive (logical-wal-archive-check archive))
                 (sb-thread:with-mutex (*xid-lock*)
                   (sb-sys:without-interrupts
                     (when commit (funcall commit))
                     (setf *wal-replication-slots* restored
                           *logical-capture-live-slots* count
                           *wal-logical-policy* policy
                           *wal-logical-info-disable-pending* disable-pending)))))
          (if empty-handoff
              (sb-sys:without-interrupts
                (logical-slot-continuation-cold-empty-call empty-handoff :before-commit)
                (install))
              (install)))
        (when empty-handoff
          (logical-slot-continuation-cold-empty-call empty-handoff :after-commit))
        (when (or disable-pending
                  (and policy (wal-logical-policy-pending-disable policy)))
          (notify-primary-wal-writer-under-lock)))))
  (values))

(defun logical-slot-startup-restore (source segments inactive-since &optional empty-handoff)
  (when empty-handoff
    (sb-thread:with-mutex (*wal-lock*)
      (logical-slot-continuation-cold-empty-call empty-handoff :authorize-source source)))
  (let ((candidates (mapcar #'logical-slot-startup-entry-slot
                            (logical-slot-startup-source-entries source))))
    (flet ((prepare (archive)
             (let ((restored
                     (apply
                      #'logical-slot-prepare-startup-inventory
                      candidates segments inactive-since
                      (lambda (slot)
                        (when (eq :reserved (wal-replication-slot-wal-status slot))
                          (logical-slot-startup-check-eligibility source slot)
                          (let ((pending (logical-slot-startup-derive-pending source slot archive)))
                            (setf (wal-replication-slot-pending-xids slot) pending)))
                        slot)
                      (when empty-handoff
                        (list (lambda ()
                                (wal-check-replication-slot-capacity-for-count
                                 (hash-table-count *wal-replication-slots*)
                                 (logical-slot-startup-source-maximum source))))))))
               (if empty-handoff
                   (logical-slot-startup-publish source restored archive empty-handoff)
                   (logical-slot-startup-publish source restored archive)))))
      (if (logical-slot-startup-source-history source)
          (call-with-logical-slot-startup-archive source #'prepare)
          (prepare nil)))))

(declaim (ftype function logical-slot-continuation-restore-startup))

(defun logical-slot-check-standby-candidate (slot state history replay)
  "Check a copied logical slot against the applied source identity and policy."
  (let* ((provenance (wal-replication-slot-logical-provenance slot))
         (pin (wal-replication-slot-catalog-xmin slot))
         (policy (getf state :source-logical-fact)))
    (unless (and provenance
                 (eql (logical-slot-provenance-system-id provenance) (getf state :system-id))
                 (not (wal-replication-slot-child-owner slot))
                 (eq :reserved (wal-replication-slot-wal-status slot))
                 (wal-replication-slot-database-oid slot)
                 (wal-replication-slot-confirmed-flush-lsn slot)
                 (running-xacts-normal-xid-p pin)
                 (<= (xid-epoch-start (min *next-xid* +max-xid+)) pin *next-xid*)
                 (wal-logical-slot-covered-p slot replay)
                 (logical-slot-history-positions-p slot history)
                 (logical-slot-standby-policy-covers-p slot policy replay))
      (error "logical slot ~a is outside supported standby recovery coverage"
             (wal-replication-slot-name slot)))))

(defun logical-slot-prove-standby-prefix (store slots)
  "Prove slot anchors on the retained receive chain under startup or quiescence."
  (let* ((state *physical-recovery-state*)
         (replay *last-replay-lsn*)
         (system-id (physical-slot-startup-system-id))
         (history (physical-store-retained-history store))
         (control (physical-slot-check-standby-control state system-id replay)))
    (dolist (slot slots)
      (logical-slot-check-standby-candidate slot state history replay)
      (when (< (wal-replication-slot-consistent-lsn slot) (physical-store-base-lsn store))
        (let ((provenance (wal-replication-slot-logical-provenance slot))
              (policy (getf state :source-logical-fact)))
          (unless (and (>= (wal-replication-slot-restart-lsn slot) (physical-store-base-lsn store))
                       (logical-slot-provenance-on-start-lsn provenance)
                       (eql (logical-slot-provenance-on-start-lsn provenance)
                            (getf policy :logical-on-start-lsn))
                       (eql (logical-slot-provenance-on-start-timeline provenance)
                            (getf policy :logical-on-start-timeline)))
            (error "pre-base logical slot ~a lacks its retained restart or original ON interval"
                   (wal-replication-slot-name slot))))))
    (physical-follow-call-with-receive-root
     store
     (lambda ()
       (physical-slot-check-standby-receive
        store state replay
        :restart-positions (mapcar #'wal-replication-slot-restart-lsn slots)
        :prefix-positions (loop for slot in slots
                                for birth = (wal-replication-slot-consistent-lsn slot)
                                when (>= birth (physical-store-base-lsn store))
                                  collect birth
                                ;; pgoutput confirmation is a filter position.
                                unless (equal (wal-replication-slot-plugin slot) "pgoutput")
                                  collect (wal-replication-slot-confirmed-flush-lsn slot)))))
    (unless (and (eq state *physical-recovery-state*)
                 (= replay *last-replay-lsn*)
                 (equalp control (physical-recovery-read-control *pgdata*)))
      (error "logical slot recovery control changed during validation"))
    control))

(defun logical-slot-recheck-standby-entry (entry)
  "Check that a proved slot still has the exact bytes captured before the scan."
  (let* ((slot (logical-slot-startup-entry-slot entry))
         (name (wal-replication-slot-name slot)))
    (slot-state-read
     (wal-replication-slot-directory) name
     (lambda (bytes expected-name)
       (declare (ignore expected-name))
       (unless (equalp bytes (logical-slot-startup-entry-bytes entry))
         (error "logical slot ~a changed during receive validation" name))))))

(defun logical-slot-publish-standby-entries (entries registry)
  (dolist (entry entries)
    (let* ((slot (logical-slot-startup-entry-slot entry))
           (name (wal-replication-slot-name slot)))
      (logical-slot-recheck-standby-entry entry)
      (when (gethash name registry) (error "duplicate WAL replication slot ~a" name))))
  (dolist (entry entries)
    (let ((slot (logical-slot-startup-entry-slot entry)))
      (setf (gethash (wal-replication-slot-name slot) registry) slot))))

(defun logical-slot-standby-entry-covered-p (entry)
  (wal-logical-slot-covered-p (logical-slot-startup-entry-slot entry) *last-replay-lsn*))

(defun logical-slot-admit-replayed-standby-entries (store)
  "Admit copied entries after replay catches up, while the receive chain is quiescent."
  (let ((ready (remove-if-not #'logical-slot-standby-entry-covered-p
                              *standby-logical-slots-awaiting-replay*)))
    (when ready
      (handler-case
          (progn
            (logical-slot-prove-standby-prefix store (mapcar #'logical-slot-startup-entry-slot ready))
            (sb-thread:with-mutex (*wal-lock*)
              (wal-check-replication-slot-capacity-for-count
               (+ (hash-table-count *wal-replication-slots*) (1- (length ready)))
               *max-replication-slots*)
              (logical-slot-publish-standby-entries ready *wal-replication-slots*)))
        (error (condition)
          (setf *standby-logical-slots-rejected*
                (nconc ready *standby-logical-slots-rejected*))
          (admin-log "copied logical slot admission deferred until restart: ~a" condition)))
      (setf *standby-logical-slots-awaiting-replay*
            (set-difference *standby-logical-slots-awaiting-replay* ready)))))

(defun logical-slot-restore-standby-inventory (store)
  "Restore covered copied slots and defer slots that are ahead of replay."
  (let* ((entries (logical-slot-startup-read-inventory :cleanup t :local-workspace t
                                                       :decoder #'wal-decode-replication-slot-image))
         (slots (mapcar #'logical-slot-startup-entry-slot entries))
         (physical (remove-if-not #'wal-physical-slot-p slots))
         (logical (remove-if-not (lambda (entry)
                                   (wal-logical-slot-p (logical-slot-startup-entry-slot entry)))
                                 entries))
         (restored (logical-slot-prepare-startup-inventory
                    physical nil (postgres-current-timestamp))))
    (when logical
      (wal-check-replication-slot-capacity-for-count (+ (hash-table-count restored) (1- (length logical))) *max-replication-slots*))
    (setf *standby-logical-slots-awaiting-replay* logical
          *standby-logical-slots-rejected* nil
          *wal-replication-slots* restored)
    (logical-slot-admit-replayed-standby-entries store)
    (values)))

(defun logical-slot-load-startup-inventory (gate-owner ownership)
  (when (or gate-owner ownership)
    (check-primary-storage-startup-context gate-owner ownership))
  ;; Fenced startup owns cleanup and publication, including an empty inventory.
  (when (and gate-owner ownership (logical-slot-continuation-gc-fenced-p *pgdata*))
    (logical-slot-continuation-restore-startup gate-owner ownership)
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
