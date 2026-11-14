(in-package #:pg)

;;; A receipt records row durability only within the current input scope.
;;; It moves from available to spent and is never adopted from an older scope.
(defstruct (logical-slot-copy-install-row-receipt
            (:constructor %make-logical-slot-copy-install-row-receipt)
            (:print-function physical-libpq-print-private-object))
  "Scoped evidence of completed row syncs, tied to the current input."
  (owner nil :read-only t) (context nil :read-only t) (input nil :read-only t)
  (operation nil :read-only t) (image nil :read-only t)
  (phase nil :read-only t) (prefix nil :read-only t) (facts nil :read-only t))

(defun logical-slot-copy-install-row-parent-facts (operation image)
  "Capture full facts for the recorded slot parents under inspection."
  (let ((value (logical-slot-copy-install-decode image)))
    (call-with-logical-slot-copy-install-inspection
     operation
     (lambda (root)
       (let* ((slot-root (logical-slot-copy-install-inspection-open root "pg_replslot" :slot-root nil))
              (work (logical-slot-copy-install-inspection-open slot-root ".logical-slot-copy.work" :directory slot-root))
              (slots (logical-slot-copy-install-inspection-open work (getf value :stage-id) :directory slot-root)))
         (loop for node in (list slot-root work slots) for key in '(:slot-root :slot-work :slot-container)
               collect (progn
                         (logical-slot-copy-install-inspection-identity node (getf value key))
                         (list key (physical-image-file-facts (logical-slot-copy-install-node-fd node))))))))))

(defun logical-slot-copy-install-row-durable-facts (operation image)
  "Capture parent and row facts for the selected layout."
  (let ((layout (logical-slot-copy-install-read-layout operation image)))
    (list (logical-slot-copy-install-row-parent-facts operation image)
          (remove-if-not (lambda (row) (= 3 (length row))) (getf layout :observations)))))

(defun logical-slot-copy-install-row-receipt-view (receipt)
  "Build the receipt image checked against its private seal."
  (list (logical-slot-copy-install-row-receipt-image receipt)
        (logical-slot-copy-install-row-receipt-phase receipt)
        (logical-slot-copy-install-row-receipt-prefix receipt)
        (logical-slot-copy-install-row-receipt-facts receipt)))

(defun logical-slot-copy-install-check-row-receipt (owner context input receipt &key (state :available))
  "Require the current input's exact receipt, state, seal, and unchanged row facts.
A receipt is scoped to its input; an old receipt cannot be adopted.  Failure
after registration revokes the receipt and input."
  (let ((record (logical-slot-copy-install-input-record owner context input)))
    (unless (and (logical-slot-copy-install-row-receipt-p receipt)
                 (eq receipt (logical-slot-copy-install-input-state-row-receipt record)))
      (error "logical installation requires its registered row receipt"))
    (let ((completed nil))
      (unwind-protect
           (progn
             (logical-slot-copy-install-check-input-seal owner context input record)
             (unless (and (eq state (logical-slot-copy-install-input-state-row-receipt-state record))
                          (eq owner (logical-slot-copy-install-row-receipt-owner receipt))
                          (eq context (logical-slot-copy-install-row-receipt-context receipt))
                          (eq input (logical-slot-copy-install-row-receipt-input receipt))
                          (equalp (logical-slot-copy-install-row-receipt-image receipt)
                                  (logical-slot-copy-install-input-state-image record))
                          (eq (logical-slot-copy-install-input-state-operation record)
                              (logical-slot-copy-install-row-receipt-operation receipt))
                          (logical-slot-copy-committed-image-equal-p
                           (logical-slot-copy-install-input-state-row-receipt-seal record)
                           (logical-slot-copy-install-row-receipt-view receipt)))
               (error "logical installation row receipt changed or was consumed"))
             (unless (logical-slot-copy-committed-image-equal-p
                      (logical-slot-copy-install-row-receipt-facts receipt)
                      (logical-slot-copy-install-row-durable-facts
                       (logical-slot-copy-install-input-state-operation record)
                       (logical-slot-copy-install-input-state-image record)))
               (error "logical installation row durability observations changed"))
             (setf completed t)
             receipt)
        (unless completed
          (setf (logical-slot-copy-install-input-state-row-receipt-state record) :revoked)
          (logical-slot-copy-install-revoke-input record))))))
