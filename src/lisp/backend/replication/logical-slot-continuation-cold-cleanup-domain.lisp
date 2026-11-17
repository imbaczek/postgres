(in-package #:pg)

;; Validate the actual certificate child and fork with the pure empty-snapshot
;; metadata check; this does not assert that the filesystem is empty.  Build
;; the domain from canonical non-NIL fourth fields, including historical slots
;; now missing from disk.  A NIL fourth field is deferred, not a child slot.
;; Each image's logical system ID comes from its provenance.
(defun logical-slot-continuation-cold-certified-domain (binding certificate host)
  (let ((history (logical-slot-continuation-origin-input-shape binding certificate host))
        (domain nil))
    (logical-slot-continuation-check-empty-snapshot
     (logical-slot-continuation-empty-snapshot binding certificate host) binding certificate host)
    (dolist (row (getf certificate :slots))
      (when (fourth row)
        (let* ((name (first row)) (bytes (string->bytes (fourth row)))
                                  (slot (logical-slot-state-decode bytes name)))
          (logical-slot-imported-check-state slot slot history (sixth host))
          (unless (and (equalp bytes (logical-slot-state-encode slot))
                       (eql (first host)
                            (logical-slot-provenance-system-id
                             (wal-replication-slot-logical-provenance slot))))
            (error "cold child certificate has a foreign or noncanonical image"))
          (push (list (copy-seq name) slot) domain))))
    (values (sort domain #'string< :key #'first) history)))

;; Classify only certified child rows, excluding ordinary and temporary slot
;; eligibility.  Nonempty proofs require the original child; empty descendant
;; history is allowed.  This pure check does no I/O and grants no effects,
;; source or publication authority.
(defun logical-slot-continuation-cold-check-child-selection
    (kind model rows binding certificate host)
  (let ((domain (logical-slot-continuation-cold-certified-domain binding certificate host)))
    (logical-slot-copy-descriptor-list-count rows +logical-slot-continuation-obligation-limit+)
    (dolist (row rows)
      (logical-slot-continuation-list row 2)
      (unless (assoc (first row) domain :test #'string=)
        (error "cold child selection contains an uncertified slot")))
    (ecase kind
      (:absent
       (when model (error "absent cold selection cannot carry a selected model"))
       (let ((snapshot (if rows
                           (logical-slot-continuation-active-child-snapshot binding certificate host rows)
                           (logical-slot-continuation-empty-snapshot binding certificate host))))
         (unless rows
           (logical-slot-continuation-check-empty-snapshot snapshot binding certificate host))
         (values snapshot :bootstrap)))
      (:stable
       (if rows
           (logical-slot-continuation-cold-classify-child-rows
            kind model rows binding certificate host)
           (progn (logical-slot-continuation-check-empty-snapshot model binding certificate host)
                  (values model :stable))))
      (:prepared
       (logical-slot-continuation-cold-classify-child-rows
        kind model rows binding certificate host)))))
