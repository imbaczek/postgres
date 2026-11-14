(in-package #:pg)

;;; Exchange the two certificate names or resync the already active cold state.
;;; The fixed journal remains the startup fence.
(defun logical-slot-copy-install-exchange-rebind (writer)
  "Rebind both held certificate identities after exchange, allowing only ctime drift."
  (let* ((old-pending (logical-slot-copy-install-exchange-writer-pending writer))
         (old-active (logical-slot-copy-install-publisher-active writer))
         (pending (%make-logical-slot-copy-install-node
                   :fd (logical-slot-copy-install-node-fd old-pending)
                   :parent (logical-slot-copy-install-publisher-container writer) :name "active" :kind :file
                   :identity (logical-slot-copy-install-node-identity old-pending)))
         (active (%make-logical-slot-copy-install-node
                  :fd (logical-slot-copy-install-node-fd old-active)
                  :parent (logical-slot-copy-install-publisher-root writer) :name "logical-slot-copy.import" :kind :file
                  :identity (logical-slot-copy-install-node-identity old-active))))
    (setf (logical-slot-copy-install-publisher-nodes writer)
          (mapcar (lambda (node) (cond ((eq node old-pending) pending) ((eq node old-active) active) (t node)))
                  (logical-slot-copy-install-publisher-nodes writer))
          (logical-slot-copy-install-exchange-writer-pending writer) pending
          (logical-slot-copy-install-publisher-active writer) active)
    (dolist (node (list pending active))
      (let* ((fd (logical-slot-copy-install-node-fd node))
             (entry (assoc fd (logical-slot-copy-install-publisher-files writer)))
             (after (physical-image-file-facts fd)))
        (unless (and entry (logical-slot-copy-install-exchange-facts-p (second entry) after))
          (error "logical installation certificate changed beyond rename ctime"))
        (setf (second entry) after)))
    (setf (logical-slot-copy-install-exchange-writer-expected-phase writer) :active)))

(defun logical-slot-copy-install-consume-row-receipt (writer)
  "Spend the current available receipt and arm the exchange writer."
  (let ((record (logical-slot-copy-install-publisher-record writer)))
    (unless (and (eq :available (logical-slot-copy-install-input-state-row-receipt-state record))
                 (eq (logical-slot-copy-install-exchange-writer-receipt writer)
                     (logical-slot-copy-install-input-state-row-receipt record)))
      (error "logical installation row receipt is not available"))
    (setf (logical-slot-copy-install-input-state-row-receipt-state record) :spent
          (logical-slot-copy-install-exchange-writer-armed writer) t)))

(defun logical-slot-copy-install-exchange-rename (writer)
  "Exchange certificates only from pending phase after spending the receipt."
  (declare (notinline physical-adoption-renameat2))
  (logical-slot-copy-install-check-exchange-writer writer)
  (unless (and (eq :pending (logical-slot-copy-install-exchange-writer-expected-phase writer))
               (not (logical-slot-copy-install-exchange-writer-armed writer)))
    (error "logical installation exchange rename requires pending phase"))
  (logical-slot-copy-install-exchange-point writer :before-exchange)
  (logical-slot-copy-install-publisher-proof writer)
  (when *logical-slot-copy-install-inspection*
    (error "logical installation exchange cannot span readonly inspection"))
  (sb-sys:without-interrupts
    (logical-slot-copy-install-consume-row-receipt writer)
    (unless (zerop (physical-adoption-renameat2
                    (logical-slot-copy-install-node-fd (logical-slot-copy-install-publisher-root writer))
                    "logical-slot-copy.import"
                    (logical-slot-copy-install-node-fd (logical-slot-copy-install-publisher-container writer))
                    "active" +logical-slot-copy-install-exchange+))
      (error "cannot exchange logical installation certificates: errno ~d" (sb-alien:get-errno)))
    (logical-slot-copy-install-exchange-rebind writer))
  (logical-slot-copy-install-exchange-point writer :after-exchange))

(defun logical-slot-copy-install-exchange-certificate (owner context input receipt)
  "Exchange pending certificates or resync an already active cold layout.
Sync both certificate files, root, and container, then reprove the input and
receipt facts through cleanup while the source lease remains held.  Cold active
resync never exchanges the files.  The immutable journal containing the original pending certificate remains
the authority and startup fence; registry activation, admission, and retirement
are not implemented here."
  (let ((record (logical-slot-copy-install-input-record owner context input)))
    (logical-slot-copy-install-check-row-receipt owner context input receipt)
    (call-with-logical-slot-copy-install-publisher
     owner context input
     (lambda (writer)
       (logical-slot-copy-install-exchange-bind writer)
       (if (eq :pending (logical-slot-copy-install-exchange-writer-expected-phase writer))
           (logical-slot-copy-install-exchange-rename writer)
           (sb-sys:without-interrupts (logical-slot-copy-install-consume-row-receipt writer)))
       (logical-slot-copy-install-publisher-sync writer (logical-slot-copy-install-publisher-active writer)
                                                 :after-exchange-active-sync)
       (logical-slot-copy-install-publisher-sync writer (logical-slot-copy-install-exchange-writer-pending writer)
                                                 :after-exchange-pending-sync)
       (logical-slot-copy-install-publisher-sync writer (logical-slot-copy-install-publisher-root writer)
                                                 :after-exchange-root-sync)
       (logical-slot-copy-install-publisher-sync writer (logical-slot-copy-install-publisher-container writer)
                                                 :after-exchange-container-sync)
       (logical-slot-copy-install-exchange-point writer :before-exchange-final-proof)
       (logical-slot-copy-install-publisher-proof writer)
       (logical-slot-copy-install-exchange-point writer :after-exchange-final-proof)
       (logical-slot-copy-install-publisher-proof writer)
       :active)
     :constructor (lambda (&rest arguments)
                    (apply #'%make-logical-slot-copy-install-exchange-writer
                           :receipt receipt :expected-phase (logical-slot-copy-install-input-state-phase record)
                           arguments)))))

(defun logical-slot-copy-install-fresh-active ()
  "Publish the journal and rows, then exchange the pending certificate."
  (call-with-logical-slot-copy-install-fresh-input
   (lambda (owner context input)
     (logical-slot-copy-install-publish-journal owner context input)
     (multiple-value-bind (count receipt) (logical-slot-copy-install-rows owner context input)
       (declare (ignore count))
       (logical-slot-copy-install-exchange-certificate owner context input receipt)))))

(defun logical-slot-copy-install-resume-active ()
  "Resume a fixed cold layout through certificate exchange or active resync."
  (call-with-logical-slot-copy-install-cold-input
   (lambda (owner context input)
     (logical-slot-copy-install-publish-journal owner context input)
     (let ((phase (logical-slot-copy-install-input-state-phase
                   (logical-slot-copy-install-input-record owner context input))))
       (multiple-value-bind (count receipt) (logical-slot-copy-install-rows owner context input :phase phase)
         (declare (ignore count))
         (logical-slot-copy-install-exchange-certificate owner context input receipt))))))
