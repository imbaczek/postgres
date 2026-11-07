(in-package #:pg)

;; Copy and classify a source lease observation against the frozen request and
;; installed base.  Return the detached parent and its classification.  NIL is
;; unknown; a lease without a base is unbound.  Neither settles cleanup debt.
;; A bound observation must equal any frozen closed settlement receipt exactly.
;; A closing or closed handoff must name the selected generation and match its
;; anchor digest when that proof is known.  Without the anchor, the matching
;; handoff remains unproved.
;;
;; A closed receipt is structural evidence, not proof that an operation ran.
;; Source-closed records that observation; a cleaned acknowledgment requires a
;; future caller to establish the corresponding typed operation.

(defun physical-reseed-follow-cancellation-source-observation-copy (predecessor observation)
  (let* ((record (physical-reseed-follow-core-copy
                  (physical-reseed-follow-core-projection predecessor)))
         (parent (when observation (physical-backup-lease-copy observation))))
    (unless parent
      (return-from physical-reseed-follow-cancellation-source-observation-copy
        (values nil :unknown)))
    (unless (equal (getf parent :request) (getf (getf record :attempt) :request))
      (error "source cleanup lease differs from the frozen request"))
    (let ((base (getf parent :base)))
      (unless base
        (return-from physical-reseed-follow-cancellation-source-observation-copy
          (values parent :unbound)))
      (unless (equal base (physical-reseed-follow-settlement-base record))
        (error "source cleanup lease differs from the installed base")))
    (let ((receipt (getf (getf predecessor :settlement) :receipt)))
      (when (and receipt (not (equal receipt parent)))
        (error "source cleanup lease differs from its frozen closed receipt")))
    (if (member (getf parent :phase) '(:closing :closed))
        (if (eq :aborted (getf parent :outcome))
            (values parent :aborting)
            (let* ((generation (car (last (getf record :generations))))
                   (successor (getf parent :successor))
                   (anchor (when generation
                             (physical-reseed-follow-generation-anchor generation))))
              (unless (and generation (equal (getf generation :name) (getf successor :name)))
                (error "source cleanup selected an unrecorded allocation"))
              (when (and anchor
                         (/= (physical-follow-anchor-digest anchor) (getf successor :anchor-digest)))
                (error "source cleanup successor differs from its saved proof"))
              (values parent (if anchor :handed-off :unproved))))
        (values parent :open))))

;; Before a future connection or STATUS request, check the image size needed
;; for the possible terminal receipts and both later phases.  A known receipt
;; is reused; otherwise consider abort and, when a generation exists, handoff.
;; An unknown anchor uses the maximum 64-bit digest for the size bound.  A
;; cleaned record needs only its existing image.  This preflight checks journal
;; capacity; it does not perform cleanup or source I/O.

(defun physical-reseed-follow-cancellation-check-cleanup-capacity
    (value &optional (maximum +physical-reseed-follow-maximum-bytes+))
  (let* ((record (physical-reseed-follow-copy value))
         (group (getf record :cancellation))
         (phase (getf group :phase))
         (required (length (physical-reseed-follow-image record))))
    (unless group
      (error "cleanup capacity requires durable cancellation intent"))
    (unless (eq :cleaned phase)
      (let* ((frozen (or (getf group :receipt)
                         (getf (getf record :settlement) :receipt)))
             (generation (car (last (getf record :generations))))
             (anchor (when generation
                       (physical-reseed-follow-generation-anchor generation)))
             (request (getf (getf record :attempt) :request))
             (base (physical-reseed-follow-settlement-base record))
             (receipts
               (if frozen
                   (list frozen)
                   (loop for outcome in (if generation '(:aborted :handed-off) '(:aborted))
                         collect
                         (physical-backup-lease-copy
                          (list :version 1 :request request :phase :closed :outcome outcome
                                :base base :successor
                                (when (eq :handed-off outcome)
                                  (list :name (getf generation :name)
                                        :anchor-digest
                                        (if anchor (physical-follow-anchor-digest anchor)
                                            #xffffffffffffffff)))))))))
        (dolist (receipt receipts)
          (dolist (next-phase '(:source-closed :cleaned))
            (let* ((candidate (copy-list record))
                   (next-group (list :phase next-phase :receipt receipt)))
              (when (eq :cleaned next-phase)
                (setf next-group
                      (append next-group
                              (list :ack (if (eq :aborted (getf receipt :outcome))
                                             :parent-abort :selected-release)))))
              (setf (getf candidate :cancellation) next-group)
              (setf required (max required (length (physical-reseed-follow-image candidate)))))))))
    (unless (<= required maximum)
      (error "target follow journal lacks room for its source cleanup outcomes"))
    required))
