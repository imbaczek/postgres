(in-package #:pg)

(defvar *physical-promotion-retained-owners* (make-hash-table :test #'equal))

(defun physical-promotion-retained-identity (system-id reference)
  (unless (and (typep system-id '(unsigned-byte 64)) (wal-retained-reference-valid-p reference))
    (error "invalid retained promotion archive reference"))
  (list :system-id system-id :base-timeline (getf reference :base-timeline)
        :base (getf reference :base) :manifest-digest (getf reference :manifest-digest)))

(defun physical-promotion-retained-parent-history (history)
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
  (let ((reference (getf metadata :retained-reference)))
    (when reference
      (multiple-value-bind (parent fork projected) (physical-promotion-retained-parent-history history)
        (unless (eql parent (getf snapshot :prev-timeline))
          (error "retained promotion archive differs from checkpoint parent"))
        (physical-promotion-check-retained-reference
         (getf metadata :system-id) reference parent projected :end fork)))))

(defun physical-promotion-check-pending-retained (control metadata &optional source-history)
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
  (unless (member kind '(:pending :primary)) (error "invalid promotion archive owner kind"))
  (let ((key (list kind (namestring (physical-retained-root-directory *pgdata*)))))
    (sb-thread:with-mutex (*physical-retained-inventory-publish-lock*)
      (or (gethash key *physical-promotion-retained-owners*)
          (setf (gethash key *physical-promotion-retained-owners*) (gensym "PROMOTION-ARCHIVE"))))))

(defun physical-promotion-pending-root-selection (expected)
  (let ((control (physical-recovery-read-control *pgdata*)))
    (unless (equalp control expected) (error "retained promotion archive pending control changed"))
    (getf (getf control :retained-reference) :root-id)))

(defun physical-promotion-restore-pending-retained (control metadata &optional source-history)
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
  (when (getf state :retained-reference)
    (let ((control (physical-recovery-read-control *pgdata*)))
      (unless (equal (getf state :retained-reference) (getf control :retained-reference))
        (error "retained promotion archive pending reference changed"))
      (physical-promotion-restore-pending-retained
       control metadata (when store (physical-store-retained-history store))))))

(defun physical-promotion-primary-root-selection (expected)
  (let ((fields (multiple-value-list (wal-read-control-fields))))
    (unless (equalp fields expected) (error "retained promotion archive primary control changed"))
    (getf (getf (seventh fields) :retained-reference) :root-id)))

(defun physical-promotion-restore-primary-retained ()
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
