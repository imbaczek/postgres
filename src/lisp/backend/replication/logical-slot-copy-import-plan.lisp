(in-package #:pg)

(defstruct (logical-slot-copy-import-pending-plan
            (:constructor %make-logical-slot-copy-import-pending-plan)
            (:print-function physical-libpq-print-private-object))
  "Opaque facts for a pending attempt; possession does not grant authority.
Encoded evidence is independent of the minted child owner."
  (value nil :read-only t) (state nil :read-only t) (state-image nil :read-only t)
  (metadata nil :read-only t) (prefix nil :read-only t) (generation nil :read-only t)
  (control nil :read-only t) (child-plan nil :read-only t)
  (descriptor nil :read-only t) (observations nil :read-only t)
  (selected-history nil :read-only t) (evidence nil :read-only t))

(defun logical-slot-copy-import-digest (bytes)
  (physical-store-update-digest #xCBF29CE484222325 bytes))

(defun logical-slot-copy-import-selected-history (state)
  (let ((target (or (getf state :history-target) (getf state :timeline))))
    (if (= 1 target) (make-array 0 :element-type '(unsigned-byte 8))
        (timeline-history-bytes target))))

(defun logical-slot-copy-import-plan-image-digest (plan history)
  "Digest the child plan and history used by both planning and evidence binding."
  (logical-slot-copy-import-digest
   (string->bytes
    (physical-bootstrap-manifest-text
     (list :fork (getf plan :fork) :child (getf plan :child) :end (getf plan :end)
           :snapshot (getf plan :snapshot) :history (logical-slot-copy-import-digest history)
           :frames (loop for (start bytes) in (getf plan :frames)
                         collect (list start (length bytes)
                                       (logical-slot-copy-import-digest bytes))))))))

(defun logical-slot-copy-import-child-plan-digest (state plan)
  "Digest the deterministic child checkpoint plan and its frame contents."
  (logical-slot-copy-import-plan-image-digest plan (physical-promotion-history-image state)))

(defun logical-slot-copy-import-check-capacity (owner context source)
  "Reject excess slots, deferred rows and collisions with local physical names."
  (let* ((slots (physical-promotion-read-physical-slots
                 :parent (cons (physical-adoption-owner-directory owner)
                               (physical-image-context-root-fd context))))
         (rows (getf source :slots)))
    (when (> (+ (length slots) (length rows)) *max-replication-slots*)
      (error "logical import exceeds max_replication_slots"))
    (dolist (row rows)
      (when (eq :deferred (fifth row))
        (error "logical import source is not eligible for a frozen promotion"))
      (when (find (first row) slots :key #'wal-replication-slot-name :test #'equal)
        (error "logical import name conflicts with a local physical slot")))
    t))

(defun logical-slot-copy-import-excluded-owners (source)
  "Collect copied identities that a new or retried child owner must not equal."
  (let ((excluded (list (logical-slot-parse-generation (getf source :copy-id)))))
    (dolist (row (getf source :slots) excluded)
      (let ((slot (logical-slot-state-decode (string->bytes (fourth row)) (first row))))
        (push (logical-slot-parse-generation (second row)) excluded)
        (when (wal-replication-slot-child-owner slot)
          (push (wal-replication-slot-child-owner slot) excluded))))))

(defun logical-slot-copy-import-new-owner (source)
  "Mint a child owner distinct from the verified source identities."
  (let ((excluded (logical-slot-copy-import-excluded-owners source)))
    (loop repeat 32 for owner = (logical-slot-new-generation)
          unless (member owner excluded :test #'eql)
            do (return-from logical-slot-copy-import-new-owner owner))
    (error "cannot choose a distinct logical import owner")))

(defun logical-slot-copy-import-pending-value (source descriptor state plan selected owner)
  (logical-slot-copy-import-check
   (list :phase :pending :copy-id (getf source :copy-id)
         :device (getf source :device) :inode (getf source :inode)
         :manifest-digest (getf source :manifest-digest)
         :descriptor-digest (getf source :descriptor-digest)
         :base-proof-digest (getf source :base-proof-digest)
         :system-id (getf source :system-id) :base-end (getf source :base-end)
         :base-timeline (getf source :base-timeline)
         :source-history-digest
         (logical-slot-copy-import-digest (logical-slot-copy-descriptor-history descriptor))
         :selected-history-digest (logical-slot-copy-import-digest selected)
         :promotion-generation (getf state :promotion-generation)
         :parent-timeline (getf source :parent-timeline)
         :child-timeline (getf state :promotion-child) :fork (getf source :fork)
         :child-end (getf plan :end)
         :pending-control-digest (logical-slot-copy-import-digest (getf source :control-bytes))
         :child-plan-digest (logical-slot-copy-import-child-plan-digest state plan)
         :owner (logical-slot-generation-token owner)
         :retained-reference (copy-list (getf state :retained-reference))
         :slots
         (loop for (name generation digest image disposition) in (getf source :slots)
               for slot = (logical-slot-state-decode (string->bytes image) name)
               collect (list name generation digest
                             (bytes->string
                              (logical-slot-copy-import-child-image
                               slot owner (if (eq disposition :eligible) :none disposition)))
                             disposition)))))

(defun logical-slot-copy-import-check-new-child (state)
  "Reject child WAL or history before a fresh pending certificate exists."
  (let ((prefix (format nil "~8,'0X" (getf state :promotion-child))))
    (when (some (lambda (name) (and (>= (length name) 8)
                                    (string= prefix name :end2 8)))
                (directory-entry-names (wal-directory) :follow-symlinks nil
                                                       :maximum-entries 65536))
      (error "child WAL or history exists without a pending import certificate"))))

(defun logical-slot-copy-import-plan-from-source
    (owner context existing source descriptor observations control)
  "Build source-bound pending facts and a deterministic child plan.
On retry, reuse the certificate owner only after checking source exclusions and
requiring the reconstructed value to match."
  ;; Reconciliation uses this same source-bound plan to authenticate the
  ;; staged child and the expected control temporary.
  (let ((*pgdata* (physical-image-context-root-path context))
        (*physical-image-context* nil))
    (logical-slot-copy-import-check-capacity owner context source)
    (multiple-value-bind (state metadata prefix generation) (physical-promotion-read-pending-state)
      (let* ((plan (physical-promotion-current-checkpoint-plan state))
             (selected (logical-slot-copy-import-selected-history state))
             (token (if existing
                        (progn
                          (unless (eq :pending (getf existing :phase))
                            (error "active import certificate cannot select parent control"))
                          (let ((owner (logical-slot-parse-generation (getf existing :owner))))
                            (when (member owner (logical-slot-copy-import-excluded-owners source)
                                          :test #'eql)
                              (error "pending import owner aliases a copied identity"))
                            owner))
                        (progn
                          (logical-slot-copy-import-check-new-child state)
                          (logical-slot-copy-verification-test-point :before-import-owner-mint context)
                          (logical-slot-copy-import-new-owner source))))
             (value (logical-slot-copy-import-pending-value
                     source descriptor state plan selected token)))
        (unless (equal control (physical-recovery-decode-control (getf source :control-bytes)))
          (error "logical import pending control changed while planning"))
        (when (and existing (not (equal existing value)))
          (error "existing pending import certificate differs from verified source or child plan"))
        (%make-logical-slot-copy-import-pending-plan
         :value value :state state :state-image (logical-slot-copy-import-state-image state)
         :metadata metadata :prefix prefix :generation generation
         :control (getf source :control-bytes) :child-plan plan
         :descriptor descriptor :observations observations :selected-history selected
         :evidence (logical-slot-copy-import-build-evidence
                    context descriptor state plan selected (getf source :control-bytes)))))))

(defun logical-slot-copy-import-build-pending (owner context existing)
  "Build source-bound pending facts and a deterministic child plan.
On retry, reuse the certificate owner only after checking source exclusions and
requiring the reconstructed value to match."
  (multiple-value-bind (descriptor observations manifest control)
      (logical-slot-copy-check-import-receipt owner context)
    (declare (ignore manifest))
    (logical-slot-copy-import-plan-from-source
     owner context existing (logical-slot-copy-prepare-import-source owner context)
     descriptor observations control)))

(defun logical-slot-copy-import-check-pending-plan (owner context attempt)
  "Recheck the source and selected plan before publication."
  (logical-slot-copy-check-input owner context)
  (let* ((state (logical-slot-copy-import-pending-plan-state attempt))
         (observations (logical-slot-copy-import-pending-plan-observations attempt))
         (*pgdata* (physical-image-context-root-path context))
         (*physical-image-context* nil))
    (unless (equalp (logical-slot-copy-import-pending-plan-control attempt)
                    (logical-slot-copy-read-file context "physical-recovery.control"
                                                 +physical-recovery-control-maximum-bytes+))
      (error "logical import pending control changed before publication"))
    (logical-slot-copy-import-recheck-plan
     owner (physical-recovery-decode-control (logical-slot-copy-import-pending-plan-control attempt))
     state (logical-slot-copy-import-pending-plan-state-image attempt)
     (logical-slot-copy-import-pending-plan-metadata attempt)
     (logical-slot-copy-import-pending-plan-prefix attempt)
     (logical-slot-copy-import-pending-plan-generation attempt))
    (unless (and (equalp (logical-slot-copy-import-pending-plan-child-plan attempt)
                         (physical-promotion-current-checkpoint-plan state))
                 (equalp (logical-slot-copy-import-pending-plan-selected-history attempt)
                         (logical-slot-copy-import-selected-history state)))
      (error "logical import child plan or selected history changed before publication"))
    (unless (equalp (logical-slot-copy-import-pending-plan-evidence attempt)
                    (logical-slot-copy-import-build-evidence
                     context (logical-slot-copy-import-pending-plan-descriptor attempt)
                     state (logical-slot-copy-import-pending-plan-child-plan attempt)
                     (logical-slot-copy-import-pending-plan-selected-history attempt)
                     (logical-slot-copy-import-pending-plan-control attempt)))
      (error "logical import evidence changed before publication"))
    (logical-slot-copy-input-check-observations owner context observations)
    (dolist (row observations)
      (unless (equal row (logical-slot-copy-file-row context (first row)))
        (error "logical import source bytes changed before publication")))
    (logical-slot-copy-input-sidecar-inventory
     context (logical-slot-copy-import-pending-plan-descriptor attempt))
    (logical-slot-copy-import-check-capacity
     owner context (list :slots (getf (logical-slot-copy-import-pending-plan-value attempt) :slots)))
    t))
