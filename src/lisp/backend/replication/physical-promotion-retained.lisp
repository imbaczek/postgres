(in-package #:pg)

;; Retained archive protection across promotion.
;;
;; A pending request and the committed primary each own a process-local root.
;; These roots protect objects from local collection; they provide no cross-process
;; coordination or promotion authority.  The durable controls select the root.
;; Validate the complete archive before registering an owner or changing files.
;; Startup holds command and reload quiescence, then restartpoint-start and
;; history locks for pending restoration.  Root reconciliation reads selected
;; control outside the archive mutex, then rechecks owner state under the mutex
;; before installing protection.
;;
;; Register the primary owner before removing recovery controls.  Remove pending
;; control last and sync the directory, then retire the pending owner only after
;; reading back the complete primary tuple and confirming pending control is gone.
;; The archive endpoint is the fixed child fork, even after later checkpoints.
;; Read-only checks do not restore the process-local registry.

(defvar *physical-promotion-retained-owners* (make-hash-table :test #'equal))

(defun physical-promotion-retained-identity (system-id reference)
  "Return the archive identity after checking its control reference."
  (unless (and (typep system-id '(unsigned-byte 64)) (wal-retained-reference-valid-p reference))
    (error "invalid retained promotion archive reference"))
  (list :system-id system-id :base-timeline (getf reference :base-timeline)
        :base (getf reference :base) :manifest-digest (getf reference :manifest-digest)))

(defun physical-promotion-retained-parent-history (history)
  "Project committed child history back to its parent and fixed fork."
  (timeline-check-lineage history)
  (unless (second history) (error "retained promotion archive requires a committed child history"))
  (let* ((parent (timeline-history-entry-timeline (second history)))
         (fork (timeline-history-entry-begin (first history)))
         (projected (mapcar #'copy-timeline-history-entry (rest history))))
    (unless (eql fork (timeline-history-entry-end (first projected)))
      (error "retained promotion archive has a contradictory child fork"))
    (setf (timeline-history-entry-end (first projected)) nil)
    (timeline-check-lineage projected)
    (values parent fork projected)))

(defun physical-promotion-check-retained-reference (system-id reference parent history &key end digest)
  "Validate the root, ancestry, base overlap and extent coverage without mutation."
  (handler-case
      (let* ((identity (physical-promotion-retained-identity system-id reference))
             (fork (getf reference :end)) (base (getf reference :base))
             (directory (physical-recovery-retained-directory)))
        (unless (and (eql parent (timeline-history-entry-timeline (first history)))
                     (or (null end) (= end fork))
                     (or (null digest) (= digest (getf reference :end-digest)))
                     (timeline-prefix-boundary-p parent fork history)
                     (if (= fork base) (= parent (getf reference :base-timeline))
                         (= parent (timeline-at-point (1- fork) history))))
          (error "reference differs from its parent or applied endpoint"))
        (let ((index (physical-retained-inventory-read-index
                      directory (getf reference :root-id) identity history)))
          (unless (and (eql parent (getf index :history-target))
                       (= (length history) (length (physical-retained-inventory-check index)))
                       (getf index :base-overlap))
            (error "root differs from its parent ancestry or lacks base overlap"))
          (physical-store-check-retained-coverage
           (%make-physical-store :base-lsn base :floor-lsn fork :floor-digest (getf reference :end-digest))
           (getf index :entries))
          (physical-retained-inventory-load directory (getf reference :root-id) identity history)))
    (error (condition) (error "invalid retained promotion archive: ~a" condition))))

(defun physical-promotion-check-primary-retained (snapshot history metadata)
  "Validate an optional primary archive against the child history fork.
A later checkpoint does not move that fork."
  (let ((reference (getf metadata :retained-reference)))
    (when reference
      (multiple-value-bind (parent fork projected) (physical-promotion-retained-parent-history history)
        (unless (eql parent (getf snapshot :prev-timeline))
          (error "retained promotion archive differs from checkpoint parent"))
        (physical-promotion-check-retained-reference
         (getf metadata :system-id) reference parent projected :end fork)))))

(defun physical-promotion-check-pending-retained (control metadata &optional source-history)
  "Validate an optional pending archive against the applied boundary and digest.
This reads objects but does not register a root or change files."
  (let ((reference (getf control :retained-reference)))
    (when reference
      (unless (and (eql 3 (getf control :phase))
                   (every (lambda (pair) (eql (getf control (first pair)) (getf reference (second pair))))
                          '((:timeline :base-timeline) (:checkpoint :base) (:manifest-digest :manifest-digest)
                            (:applied :end))))
        (error "invalid retained promotion archive pending boundary"))
      (let* ((parent (getf control :applied-timeline (getf control :timeline)))
             (text (if (= parent 1) "" (bytes->string (timeline-history-bytes parent))))
             (history (timeline-check-lineage (timeline-parse-history parent text))))
        (when source-history
          (physical-retained-project-history source-history
                                             (physical-promotion-retained-identity (getf control :system-id) reference)
                                             parent text (getf control :applied)))
        (values (physical-promotion-check-retained-reference
                 (getf control :system-id) reference parent history
                 :end (getf control :applied) :digest (getf metadata :prefix-digest))
                history)))))

(defun physical-promotion-retained-owner (kind)
  "Return the process-local owner for KIND and this data directory."
  (unless (member kind '(:pending :primary)) (error "invalid promotion archive owner kind"))
  (let ((key (list kind (namestring (physical-retained-root-directory *pgdata*)))))
    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
      (or (gethash key *physical-promotion-retained-owners*)
          (setf (gethash key *physical-promotion-retained-owners*) (gensym "PROMOTION-ARCHIVE"))))))

(defun physical-promotion-pending-root-selection (expected)
  "Recheck the complete pending control before selecting its root."
  (let ((control (physical-recovery-read-control *pgdata*)))
    (unless (equalp control expected) (error "retained promotion archive pending control changed"))
    (getf (getf control :retained-reference) :root-id)))

(defun physical-promotion-restore-pending-retained (control metadata &optional source-history)
  "Validate and reconcile the pending owner with the selected durable root."
  (when (getf control :retained-reference)
    (multiple-value-bind (index history)
        (physical-promotion-check-pending-retained control metadata source-history)
      (declare (ignore index))
      (physical-retained-root-reconcile
       (physical-promotion-retained-owner :pending) (physical-recovery-retained-directory)
       (physical-promotion-retained-identity (getf control :system-id) (getf control :retained-reference))
       history (lambda () (physical-promotion-pending-root-selection control)))
      (physical-promotion-test-point :after-pending-retained-registration))))

(defun physical-promotion-ensure-pending-retained (state metadata &optional (store *physical-receive-store*))
  "Restore pending protection only for the reference selected by STATE."
  (when (getf state :retained-reference)
    (let ((control (physical-recovery-read-control *pgdata*)))
      (unless (equal (getf state :retained-reference) (getf control :retained-reference))
        (error "retained promotion archive pending reference changed"))
      (physical-promotion-restore-pending-retained
       control metadata (when store (physical-store-retained-history store))))))

(defun physical-promotion-primary-root-selection (expected)
  "Recheck the complete primary control tuple before selecting its root."
  (let ((fields (multiple-value-list (wal-read-control-fields))))
    (unless (equalp fields expected) (error "retained promotion archive primary control changed"))
    (getf (getf (seventh fields) :retained-reference) :root-id)))

(defun physical-promotion-restore-primary-retained ()
  "Validate committed child control and register its primary root owner.
Return the complete control tuple for the subsequent retirement check."
  (let* ((fields (multiple-value-list (wal-read-control-fields)))
         (metadata (seventh fields)) (reference (getf metadata :retained-reference)))
    (when reference
      (multiple-value-bind (snapshot history)
          (physical-promotion-check-child-control (fourth fields) (fifth fields))
        (declare (ignore snapshot))
        (multiple-value-bind (parent fork projected) (physical-promotion-retained-parent-history history)
          (declare (ignore parent fork))
          (physical-retained-root-reconcile
           (physical-promotion-retained-owner :primary) (physical-recovery-retained-directory)
           (physical-promotion-retained-identity (getf metadata :system-id) reference)
           projected (lambda () (physical-promotion-primary-root-selection fields))))))
    fields))

(defun physical-promotion-retire-pending-retained (fields)
  "Retire pending protection after primary readback and pending control removal."
  (let* ((metadata (seventh fields)) (reference (getf metadata :retained-reference)))
    (when reference
      (physical-retained-root-retire
       (physical-promotion-retained-owner :pending) (physical-recovery-retained-directory)
       (getf reference :root-id)
       (physical-promotion-retained-identity (getf metadata :system-id) reference)
       (lambda ()
         (physical-promotion-primary-root-selection fields)
         (when (wal-namespace-file-stat (physical-recovery-control-path) t)
           (error "retained promotion archive pending control has not been retired"))
         nil)))))

(defun physical-promotion-retained-parent-selection (state store)
  (let* ((parent (physical-recovery-replay-timeline state))
         (text (if (= parent 1) "" (bytes->string (timeline-history-bytes parent))))
         (selection (unless (= parent (physical-store-timeline store))
                      (or (loop for item = (physical-store-selection store) then (getf item :previous)
                                while item when (eql parent (getf item :target)) return item)
                          (error "promotion parent is absent from receive history")))))
    (when (and selection (not (string= text (getf selection :history-text))))
      (error "promotion parent history differs from receive selection"))
    (values parent text (getf selection :digest))))

(defun physical-promotion-retained-source (state metadata fork store)
  (physical-store-check-retained-identity store)
  (wal-ensure-directory-durable (physical-recovery-retained-directory))
  (let* ((identity (physical-store-retained-identity store))
         (expected (physical-store-control-tuple store))
         (digest (getf metadata :prefix-digest))
         (root (physical-store-retained-root store))
         (history (physical-store-retained-history store)))
    (unless (and (equal identity
                        (list :system-id (getf state :system-id) :base-timeline (getf state :timeline)
                              :base (getf state :end) :manifest-digest (getf state :manifest-digest)))
                 (equal (physical-retained-root-directory (physical-store-retained-directory store))
                        (physical-retained-root-directory (physical-recovery-retained-directory)))
                 (= fork (physical-store-floor-lsn store))
                 (= digest (physical-store-floor-digest store))
                 (= (physical-store-written-lsn store) (physical-store-flushed-lsn store))
                 (or (= fork (physical-store-base-lsn store)) root)
                 (= digest (physical-store-prefix-digest-under-lock store fork)))
      (error "promotion lacks retained receive provenance at its applied fork; reseed required"))
    (physical-store-read-retained-control store expected)
    (physical-store-check-retained-root store)
    (let ((index (when root
                   (physical-retained-inventory-read-index
                    (physical-store-retained-directory store) root identity history))))
      (values identity expected history
              (unless (getf index :base-overlap)
                (multiple-value-bind (overlap bytes) (physical-retained-overlap-capture state *pgdata*)
                  (physical-retained-overlap-encode overlap bytes)))))))

(defun physical-promotion-publish-retained-intent (state metadata fork make-pending)
  (let ((store *physical-receive-store*))
    (with-physical-store (store)
      (multiple-value-bind (identity source-control history overlap-image)
          (physical-promotion-retained-source state metadata fork store)
        (multiple-value-bind (parent text parent-digest)
            (physical-promotion-retained-parent-selection state store)
          (let ((expected nil) (pending nil))
            (physical-retained-project-history history identity parent text fork)
            (call-with-physical-retained-root-update
             (lambda (initial hold)
               (declare (ignore initial))
               (let* ((overlap (when overlap-image
                                 (physical-retained-root-stage-overlap hold parent text overlap-image)))
                      (root (physical-retained-root-project-prefix
                             hold (physical-store-retained-root store) history parent text parent-digest
                             fork (getf metadata :prefix-digest) :base-overlap overlap
                             :source-owner (physical-store-retained-owner store)))
                      (reference (list :root-id root :base-timeline (getf identity :base-timeline)
                                       :base (getf identity :base) :manifest-digest (getf identity :manifest-digest)
                                       :end fork :end-digest (getf metadata :prefix-digest))))
                 (physical-promotion-test-point :after-retained-intent-publication)
                 (multiple-value-bind (bytes selected) (funcall make-pending reference)
                   (setf expected (physical-recovery-decode-control bytes) pending selected)
                   (physical-recovery-persist-control-bytes bytes +db-in-archive-recovery+)
                   (physical-promotion-test-point :after-retained-intent-control))
                 root))
             (physical-promotion-retained-owner :pending) (physical-recovery-retained-directory)
             identity parent text nil
             :read-selected
             (lambda ()
               (physical-store-read-retained-control store source-control)
               (physical-promotion-pending-root-selection expected)))
            (physical-promotion-test-point :after-retained-intent-handoff)
            pending))))))
