(in-package #:pg)

(defstruct (logical-slot-continuation-protection
            (:constructor %make-logical-slot-continuation-protection)
            (:print-function physical-libpq-print-private-object))
  (origin nil :read-only t) (root-identity nil :read-only t)
  (fork nil :read-only t) (names nil :read-only t) (state nil :read-only t))

(defun logical-slot-continuation-protection-call (protection operation &optional value)
  ;; Dispatch a registered group under WAL exclusion.  :FAIL marks all
  ;; members uncertain and retains the persistent fork pin.
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (logical-slot-continuation-protection-p protection)
    (error "continuation protection requires its registered record"))
  (let ((state (logical-slot-continuation-protection-state protection)))
    (unless (and (functionp state)
                 (eq state (gethash protection *logical-slot-continuation-protection-identities*))
                 (eq protection (gethash (logical-slot-continuation-protection-root-identity protection)
                                         *logical-slot-continuation-protections*)))
      (error "continuation protection has no registered identity"))
    (funcall state :identity protection)
    (funcall state operation protection value)))

;; The selected children are a subset of the registry. Check their saved
;; identities and validate every ordinary entry under its own slot contract.
(defun logical-slot-continuation-protection-check-registry
    (registry members identities system-id history frontier &optional permitted operation)
  (unless (and (<= (length members) (hash-table-count registry))
               (= (length members) (length identities))
               (every (lambda (entry expected)
                        (let ((slot (cdr entry)))
                          (and (eq slot (gethash (car entry) registry))
                               (logical-slot-state-identity-equal-p slot (car expected))
                               (eq (wal-replication-slot-logical-provenance slot)
                                   (cdr expected)))))
                      members identities))
    (error "continuation protection registry differs from its saved group"))
  ;; Other entries may include live temporary physical slots as well as
  ;; persistent ordinary slots.
  (logical-slot-continuation-check-runtime-ordinary-registry
   registry members system-id history frontier permitted operation)
  t)

(defun logical-slot-continuation-protection-check-bindings
    (protection pins registry incarnation namespace child members identities origin system-id history
     &optional permitted operation)
  (unless (and (eq incarnation *wal-storage-incarnation*)
               (eq pins *wal-snapshot-retention-pins*)
               (eql (logical-slot-continuation-protection-fork protection) (gethash protection pins))
               (eq namespace *wal-segment-namespace*) (eql child *wal-insert-timeline*))
    (error "continuation protection lost its fork reservation"))
  (unless (and (eq registry *wal-replication-slots*)
               (eq protection (gethash registry *logical-slot-continuation-protection-registries*)))
    (error "continuation protection lost its published registry"))
  (logical-slot-continuation-protection-check-registry
   registry members identities system-id history *wal-durable-lsn* permitted operation)
  (logical-slot-imported-host-origin-call origin :check-root)
  t)

(defun logical-slot-continuation-check-physical-operation
    (operation slot protection registry control incarnation pins check-identity)
  (unless (and (wal-replication-slot-p slot)
               (wal-physical-slot-p slot)
               (wal-replication-slot-temporary-p slot)
               (eq slot (gethash (wal-replication-slot-name slot) registry))
               (not (member (wal-replication-slot-name slot)
                            (logical-slot-continuation-protection-names protection)
                            :test #'string=)))
    (error "temporary physical operation is outside its ordinary registry"))
  (when (and (eq operation :physical-drop) (svref control 1)
             (eq :published (svref control 0)))
    (physical-slot-refuse-cold-cleanup-under-lock
     slot
     (lambda ()
       (funcall check-identity protection)
       (unless (and (svref control 1)
                    (eq :published (svref control 0))
                    (eq registry *wal-replication-slots*)
                    (eq protection
                        (gethash registry *logical-slot-continuation-protection-registries*))
                    (eq protection
                        (gethash (logical-slot-continuation-protection-root-identity protection)
                                 *logical-slot-continuation-protections*))
                    (eq incarnation *wal-storage-incarnation*)
                    (eq pins *wal-snapshot-retention-pins*)
                    (eql (logical-slot-continuation-protection-fork protection)
                         (gethash protection pins)))
         (error "cold cleanup refusal lost its uncertain group"))
       (logical-slot-imported-host-origin-call
        (logical-slot-continuation-protection-origin protection) :check-root)))))

;; A staged group holds candidate identities for cold publication but cannot
;; serve as the published protection until the installer changes its phase.
(defun logical-slot-continuation-build-protection-under-lock
    (registry image binding root origin namespace &optional (phase :published))
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (member phase '(:staged :published)) (error "invalid continuation group phase"))
  (let* ((root (copy-list root)) (binding (copy-seq binding))
                                 (saved (copy-seq image)) (selection (cons saved nil))
                                 (snapshot (logical-slot-continuation-decode saved)) (rows (getf snapshot :slots))
                                 (system-id (getf snapshot :system-id))
                                 (history (logical-slot-continuation-history (getf snapshot :history-target)
                                                                             (getf snapshot :history)))
                                 (fork (second (getf snapshot :origin))) (child (first (getf snapshot :origin)))
                                 (pins *wal-snapshot-retention-pins*) (incarnation *wal-storage-incarnation*)
                                 (mutex *wal-lock*) (name-index *physical-slot-lifetime-names*)
                                 (control (vector phase nil))
                                 (members (mapcar (lambda (row) (cons (first row) (gethash (first row) registry))) rows))
                                 (identities
                                   (mapcar (lambda (row)
                                             (cons (logical-slot-state-decode (second row) (first row))
                                                   (wal-replication-slot-logical-provenance (gethash (first row) registry)))) rows))
                                 (names (mapcar (lambda (entry) (copy-seq (car entry))) members))
                                 (protection nil))
    (unless (and (<= (length members) (hash-table-count registry))
                 (every (lambda (entry) (wal-replication-slot-p (cdr entry))) members))
      (error "continuation group requires its complete candidate registry"))
    (logical-slot-continuation-check-runtime-ordinary-registry
     registry members system-id history *wal-durable-lsn*)
    (labels ((fail ()
               (assert (sb-thread:holding-mutex-p mutex))
               (sb-sys:without-interrupts
                 (setf (svref control 1) t)
                 (dolist (entry members)
                   (setf (wal-replication-slot-state-uncertain-p (cdr entry)) t)
                   (when (and (eq mutex *wal-lock*) (eq registry *wal-replication-slots*))
                     (let ((current (gethash (car entry) registry)))
                       (when (wal-replication-slot-p current)
                         (setf (wal-replication-slot-state-uncertain-p current) t))))))
               t)
             (check-identity (object)
               (assert (and (eq mutex *wal-lock*) (sb-thread:holding-mutex-p mutex)))
               (unless (and (eq name-index *physical-slot-lifetime-names*)
                            (eq object protection)
                            (equal root (logical-slot-continuation-protection-root-identity protection))
                            (equal names (logical-slot-continuation-protection-names protection))
                            (eql fork (logical-slot-continuation-protection-fork protection))
                            (eq origin (logical-slot-continuation-protection-origin protection)))
                 (error "continuation protection changed its identity")))
             (ready (&optional permitted operation)
               (when (svref control 1)
                 (ereport "55000" "continuation group is uncertain; cold reconciliation required"))
               (unless (eq :published (svref control 0))
                 (ereport "55000" "continuation group is staged; publication required"))
               (logical-slot-continuation-protection-check-bindings
                protection pins registry incarnation namespace child members identities origin system-id history
                permitted operation))
             (staged ()
               (check-identity protection)
               (unless (and (eq :staged (svref control 0)) (not (svref control 1))
                            (eq pins *wal-snapshot-retention-pins*)
                            (eq incarnation *wal-storage-incarnation*)
                            (eq namespace *wal-segment-namespace*) (eql child *wal-insert-timeline*)
                            (eql fork (gethash protection pins)))
                 (error "staged continuation protection changed its candidates or fork pin"))
               (logical-slot-continuation-protection-check-registry
                registry members identities system-id history *wal-durable-lsn*)
               (logical-slot-imported-host-origin-call origin :check-root)
               t)
             (state (operation object &optional value)
               (check-identity object)
               (ecase operation
                 (:identity t)
                 (:uncertain (svref control 1))
                 (:fail (fail))
                 (:failure-cleanup #'fail)
                 (:check (ready))
                 (:ordinary
                  (ready)
                  (logical-slot-continuation-check-runtime-ordinary-candidate
                   registry members value system-id history *wal-durable-lsn*))
                 ((:physical-create :physical-drop)
                  (logical-slot-continuation-check-physical-operation
                   operation value protection registry control incarnation pins #'check-identity)
                  (ready (wal-replication-slot-temporary-lifetime value) operation))
                 (:binding (copy-seq binding))
                 (:image (copy-seq (car selection)))
                 (:owns-cell
                  (unless (eq :published (svref control 0)) (error "continuation group is staged"))
                  (and (cdr selection) (consp value) (eq (car value) (cdr selection))
                       (eq (cdr value) selection)))
                 (:begin
                  (ready)
                  (logical-slot-continuation-writer-bind-check-under-lock value protection (car selection))
                  (when (cdr selection) (error "continuation already has a writer"))
                  (sb-sys:without-interrupts
                    (let ((complete nil))
                      (unwind-protect
                           (progn
                             (setf (cdr selection) value)
                             (logical-slot-continuation-writer-bind-cell-under-lock value protection selection)
                             (setf complete t))
                        (unless complete
                          (when (eq value (cdr selection)) (setf (cdr selection) nil)))))))
                 (:cancel
                  (unless (eq :published (svref control 0)) (error "continuation group is staged"))
                  (logical-slot-continuation-writer-call value :cancel-ready protection)
                  (cond ((eq value (cdr selection)) (setf (cdr selection) nil))
                        ((null (cdr selection)) t)
                        (t (error "continuation cancellation changed its writer")))))))
      (setf protection (%make-logical-slot-continuation-protection
                        :origin origin :root-identity (copy-list root)
                        :fork fork :names (mapcar #'copy-seq names) :state #'state))
      (values protection control #'fail #'staged))))

(defun logical-slot-continuation-make-protection-under-lock (proof)
  (multiple-value-bind (record candidate ledger selected-facts)
      (logical-slot-continuation-native-preflight-under-lock proof)
    (let ((root (logical-slot-continuation-native-proof-root-identity proof))
          (registry *wal-replication-slots*))
      (when (or (gethash root *logical-slot-continuation-protections*)
                (gethash registry *logical-slot-continuation-protection-registries*))
        (error "continuation protection already has a registered group"))
      (let ((protection
              (logical-slot-continuation-build-protection-under-lock
               registry (logical-slot-continuation-native-proof-old-image proof)
               (logical-slot-continuation-native-proof-binding proof) root
               (logical-slot-continuation-native-proof-origin proof)
               (logical-wal-source-namespace (logical-slot-continuation-native-proof-source proof)))))
        (sb-sys:without-interrupts
          (setf (gethash protection *wal-snapshot-retention-pins*)
                (logical-slot-continuation-protection-fork protection)
                (gethash protection *logical-slot-continuation-protection-identities*)
                (logical-slot-continuation-protection-state protection)
                (gethash (copy-list root) *logical-slot-continuation-protections*) protection
                (gethash registry *logical-slot-continuation-protection-registries*) protection))
        (values protection record candidate ledger t selected-facts)))))

(defun logical-slot-continuation-ensure-native-protection-under-lock (proof)
  ;; Reuse a group only for the exact saved image, binding and origin, including
  ;; a no-op preflight.  The returned plan is not an ACK, cold dispatcher,
  ;; writer authority or permission to release retained WAL.
  (logical-slot-continuation-native-proof-check-under-lock proof)
  (let* ((key (logical-slot-continuation-native-proof-root-identity proof))
         (image (logical-slot-continuation-native-proof-old-image proof))
         (prior (gethash key *logical-slot-continuation-protections*)))
    (multiple-value-bind (protection record candidate ledger added selected-facts)
        (if prior
            (multiple-value-bind (record candidate ledger selected-facts)
                (logical-slot-continuation-native-preflight-under-lock proof)
              (values prior record candidate ledger nil selected-facts))
            (logical-slot-continuation-make-protection-under-lock proof))
      (logical-slot-continuation-protection-call protection :check)
      (unless (and (equalp image (logical-slot-continuation-protection-call protection :image))
                   (equalp (logical-slot-continuation-native-proof-binding proof)
                           (logical-slot-continuation-protection-call protection :binding))
                   (eq (logical-slot-continuation-native-proof-origin proof)
                       (logical-slot-continuation-protection-origin protection)))
        (error "continuation protection differs from its selected durable binding"))
      (values protection record candidate ledger added selected-facts))))

(defun logical-slot-continuation-check-slot (slot &optional mutation)
  (logical-slot-continuation-cold-check-admission)
  (let ((registered (gethash *wal-replication-slots* *logical-slot-continuation-protection-registries*)))
    (when (or registered (plusp (hash-table-count *logical-slot-continuation-protections*)))
      (let* ((facts (physical-image-file-facts
                     -100 (namestring (uiop:ensure-directory-pathname *pgdata*))))
             (key (list (first facts) (second facts) (ninth facts)))
             (protection (gethash key *logical-slot-continuation-protections*)))
        (when (and registered
                   (not (and (eq protection registered)
                             (equal key (logical-slot-continuation-protection-root-identity registered)))))
          (error "protected continuation changed its PGDATA identity"))
        (when protection
          (cond
            ((member (wal-replication-slot-name slot)
                     (logical-slot-continuation-protection-names protection) :test #'string=)
             (logical-slot-continuation-protection-call protection :check)
             (when mutation
               (ereport "55000" "protected imported slot mutation requires continuation publication")))
            ((member mutation '(:physical-create :physical-drop))
             (logical-slot-continuation-protection-call protection mutation slot))
            (t
             (logical-slot-continuation-protection-call protection :check)
             (logical-slot-continuation-protection-call protection :ordinary slot)))))))
  slot)
