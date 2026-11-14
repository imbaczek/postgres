(in-package #:pg)

(defstruct (logical-slot-copy-install-input-owner
            (:include logical-slot-copy-committed-owner)
            (:constructor %make-logical-slot-copy-install-input-owner)
            (:print-function physical-libpq-print-private-object))
  (operation nil :read-only t))

(defstruct (logical-slot-copy-install-input
            (:constructor %make-logical-slot-copy-install-input)
            (:print-function physical-libpq-print-private-object))
  (owner nil :read-only t) (context nil :read-only t) (store nil :read-only t)
  (image nil :read-only t) (certificate nil :read-only t) (plan nil :read-only t)
  (fact nil :read-only t) (history nil :read-only t) (proof nil :read-only t))

(defstruct (logical-slot-copy-install-input-state
            (:constructor %make-logical-slot-copy-install-input-state)
            (:print-function physical-libpq-print-private-object))
  (operation nil :read-only t) (image nil :read-only t)
  (origin nil :read-only t) (handoff nil :read-only t)
  (registry nil :read-only t) (engine nil :read-only t) (shutdown-lock nil :read-only t)
  owner context context-fd context-children input seal
  phase prefix physical-images physical-facts physical-witnesses failed ready)

(defvar *logical-slot-copy-install-inputs* nil)

(defun logical-slot-copy-install-input-owner-record (owner)
  (let ((record (find owner *logical-slot-copy-install-inputs*
                      :key #'logical-slot-copy-install-input-state-owner :test #'eq)))
    (unless (and record (logical-slot-copy-install-input-owner-p owner))
      (error "logical installation input requires its registered owner"))
    (logical-slot-copy-install-operation-record
     (logical-slot-copy-install-input-state-operation record))
    (when (logical-slot-copy-install-input-state-failed record)
      (error "logical installation input scope ended"))
    record))

(defun logical-slot-copy-install-revoke-input (record)
  (let* ((operation (logical-slot-copy-install-input-state-operation record))
         (operation-record (find operation *logical-slot-copy-install-operations*
                                 :key #'logical-slot-copy-install-operation-state-operation :test #'eq))
         (owner (logical-slot-copy-install-input-state-owner record)))
    (setf (logical-slot-copy-install-input-state-failed record) t
          (physical-adoption-owner-active-p operation) nil)
    (when owner (setf (physical-adoption-owner-active-p owner) nil))
    (when operation-record
      (setf (logical-slot-copy-install-operation-state-failed operation-record) t))))

(defun logical-slot-copy-install-check-input-lifecycle (operation &optional (shutdown-lock *physical-shutdown-lock*))
  (let ((record (logical-slot-copy-install-operation-record operation)))
    (when (eq :live (logical-slot-copy-install-operation-state-mode record))
      (unless (and *physical-follow-controller-blocked-p*
                   (eq *physical-follow-controller-block-owner* sb-thread:*current-thread*)
                   (eq shutdown-lock *physical-shutdown-lock*)
                   (sb-thread:holding-mutex-p shutdown-lock))
        (error "live installation input requires controller and shutdown exclusion")))
    t))

(defmethod physical-adoption-check-owner-role ((owner logical-slot-copy-install-input-owner))
  (let* ((record (logical-slot-copy-install-input-owner-record owner))
         (operation (logical-slot-copy-install-input-state-operation record))
         (completed nil))
    (unwind-protect
         (let ((outer (logical-slot-copy-install-operation-record operation)))
           (logical-slot-copy-install-check-operation operation)
           (logical-slot-copy-install-check-input-lifecycle
            operation (logical-slot-copy-install-input-state-shutdown-lock record))
           (unless (and (physical-adoption-owner-active-p owner)
                        (eq operation (logical-slot-copy-install-input-owner-operation owner))
                        (eq (logical-slot-copy-committed-owner-mode owner)
                            (logical-slot-copy-install-operation-state-mode outer))
                        (eq (logical-slot-copy-committed-owner-state owner)
                            (logical-slot-copy-install-operation-state-state outer))
                        (eq (logical-slot-copy-committed-owner-store owner)
                            (logical-slot-copy-install-operation-state-store outer))
                        (eq *wal-replication-slots*
                            (logical-slot-copy-install-input-state-registry record)))
             (error "logical installation input ownership binding changed"))
           (logical-slot-copy-check-postmaster-owner
            owner (logical-slot-copy-committed-owner-ownership owner))
           (let ((context (logical-slot-copy-install-input-state-context record)))
             (when context
               (unless (and (eql (physical-image-context-root-fd context)
                                 (logical-slot-copy-install-input-state-context-fd record))
                            (equal (physical-image-context-children context)
                                   (logical-slot-copy-install-input-state-context-children record)))
                 (error "logical installation input descriptor binding changed"))))
           (setf completed t)
           owner)
      (unless completed (logical-slot-copy-install-revoke-input record)))))

(defmethod physical-adoption-check-owner ((owner logical-slot-copy-install-input-owner))
  (declare (ignore owner))
  (error "logical installation input does not authorize offline adoption"))

(defun logical-slot-copy-install-input-record (owner context input)
  (let ((record (logical-slot-copy-install-input-owner-record owner)))
    (unless (and (logical-slot-copy-install-input-p input)
                 (eq input (logical-slot-copy-install-input-state-input record))
                 (eq owner (logical-slot-copy-install-input-owner input))
                 (eq context (logical-slot-copy-install-input-context input))
                 (eq context (logical-slot-copy-install-input-state-context record)))
      (error "logical installation input requires its registered owner and context"))
    record))

(defun logical-slot-copy-install-input-view (input)
  (let ((proof (logical-slot-copy-install-input-proof input)))
    (list
     (logical-slot-copy-install-input-image input)
     (logical-slot-copy-import-image (logical-slot-copy-install-input-certificate input))
     (logical-slot-copy-import-evidence-encode (logical-slot-copy-committed-proof-evidence proof))
     (logical-slot-copy-descriptor-encode (logical-slot-copy-committed-proof-descriptor proof))
     (list (logical-slot-copy-install-input-plan input)
           (logical-slot-copy-install-input-fact input)
           (logical-slot-copy-install-input-history input)
           (logical-slot-copy-committed-proof-manifest proof)
           (logical-slot-copy-committed-proof-control proof)
           (logical-slot-copy-committed-proof-state proof)
           (logical-slot-copy-committed-proof-metadata proof)
           (logical-slot-copy-committed-proof-prefix proof)
           (logical-slot-copy-committed-proof-generation proof)
           (logical-slot-copy-committed-proof-tuple proof)
           (logical-slot-copy-committed-proof-observations proof)))))

(defun logical-slot-copy-install-check-input-seal (owner context input record)
  (logical-slot-copy-check-input owner context)
  (logical-slot-copy-install-check-operation-input
   (logical-slot-copy-install-input-state-operation record) context)
  (unless (and (logical-slot-copy-committed-image-equal-p
                (logical-slot-copy-install-input-state-seal record)
                (logical-slot-copy-install-input-view input))
               (logical-slot-copy-committed-image-equal-p
                (logical-slot-copy-install-input-state-engine record)
                *physical-recovery-state*))
    (error "logical installation input proof image or engine state changed"))
  t)
