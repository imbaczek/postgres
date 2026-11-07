;;; Scope source-closed cancellation publication to its exact predecessor and
;;; a stable local attempt, using the shared one-use publication claims.

(in-package #:pg)

(defstruct (physical-reseed-follow-cancellation-cleanup-authority
            (:include physical-reseed-follow-cancellation-authority)
            (:constructor %make-physical-reseed-follow-cancellation-cleanup-authority)
            (:copier nil)
            ;; This subtype uses shared publication claims for source-closed cleanup.
            (:print-function physical-libpq-print-private-object)))

(defun physical-reseed-follow-cancellation-cleanup-candidate (candidate previous)
  ;; Accept only the exact pending to source-closed transition and its receipt.
  (let* ((before (physical-reseed-follow-cancellation-preconfiguration-record (getf previous :record)))
         (after (physical-reseed-follow-cancellation-preconfiguration-record candidate))
         (group (getf after :cancellation)))
    (unless (and (eq :pending (getf (getf before :cancellation) :phase))
                 (eq :source-closed (getf group :phase))
                 (equal after
                        (physical-reseed-follow-cancellation-source-closed-record
                         before (getf group :receipt))))
      (error "cleanup publication requires the exact source-closed successor"))
    after))

(defun call-with-physical-reseed-follow-cancellation-cleanup-authority
    (owner root-fd candidate previous bytes mode check function &optional fd temporary-identity)
  ;; Freeze the owner, root, thread, candidate and attempt for this scope.
  ;; The attempt may already be aborted and must stay unchanged during the
  ;; operation.  Initial pending publication requires an active attempt.
  ;; Keep claims unavailable during initial proof and revoke them on exit.
  (when *physical-reseed-follow-cancellation-authority*
    (error "cancellation authority cannot reenter acquisition or publication"))
  (let* ((frozen (physical-reseed-follow-cancellation-cleanup-candidate candidate previous))
         (before (physical-adoption-attempt-value-copy previous))
         (image (copy-seq bytes))
         (thread sb-thread:*current-thread*)
         (authority nil)
         (live t)
         (attempt (physical-reseed-follow-attempt-snapshot-at root-fd)))
    (unless (equalp image (physical-reseed-follow-image frozen))
      (error "cancellation authority requires its canonical source-closed image"))
    (unless (and (physical-reseed-follow-owner-busy-p owner) (member mode '(:write :recover))
                 (eq :pending (getf (getf (getf before :record) :cancellation) :phase)))
      (error "cancellation authority requires its exact source-closed successor"))
    (labels ((scope (value actual-owner actual-root actual-candidate actual-previous actual-bytes)
               (unless (and live (physical-reseed-follow-cancellation-cleanup-authority-p value)
                            (eq value authority)
                            (eq owner (physical-reseed-follow-cancellation-authority-owner value))
                            (eql root-fd (physical-reseed-follow-cancellation-authority-root-fd value))
                            (eq actual-owner owner) (eql actual-root root-fd)
                            (eq thread sb-thread:*current-thread*)
                            (physical-reseed-follow-cancellation-authority-active-p value)
                            (equal frozen actual-candidate) (equal before actual-previous)
                            (or (null actual-bytes) (equalp image actual-bytes))
                            (equal frozen (physical-reseed-follow-cancellation-authority-candidate value))
                            (equal before (physical-reseed-follow-cancellation-authority-previous value))
                            (equalp image (physical-reseed-follow-cancellation-authority-bytes value)))
                 (error "cancellation authority changed candidate, predecessor or scope")))
             (verify ()
               (scope authority owner root-fd frozen before image)
               (physical-reseed-cleanup-check-binding owner (getf attempt :record))
               (physical-reseed-follow-check-attempt-snapshot-at root-fd attempt)
               (funcall check)
               (physical-reseed-follow-cancellation-check-publication-state
                authority root-fd temporary-identity)))
      (setf authority
            (%make-physical-reseed-follow-cancellation-cleanup-authority
             :owner owner :root-fd root-fd :thread thread :mode mode :fd fd
             :candidate (physical-reseed-follow-copy frozen)
             :previous (physical-adoption-attempt-value-copy before) :bytes (copy-seq image)
             :scope #'scope :check #'verify :phase :acquiring :active-p t))
      (let ((*physical-reseed-follow-cancellation-authority* authority))
        (unwind-protect
             (progn
               (verify)
               (sb-sys:without-interrupts
                 (setf (physical-reseed-follow-cancellation-authority-phase authority) :ready))
               (funcall function #'verify))
          (setf live nil (physical-reseed-follow-cancellation-authority-active-p authority) nil))))))
