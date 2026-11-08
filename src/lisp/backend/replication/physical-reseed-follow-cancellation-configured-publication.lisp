(in-package #:pg)

;; Connect configured cancellation publication to its cleanup owner and
;; the installation context held by that owner.
(defmethod physical-reseed-follow-cancellation-publication-record
    ((owner physical-reseed-follow-cancellation-owner) value)
  (physical-reseed-follow-cancellation-check-owner-history owner value))

(defmethod physical-reseed-follow-cancellation-publication-context
    ((owner physical-reseed-follow-cancellation-owner))
  (physical-reseed-follow-installation-context owner))

(defmethod physical-reseed-follow-cancellation-local-proof
    ((owner physical-reseed-follow-cancellation-owner) root-fd value)
  (let* ((record (physical-reseed-follow-cancellation-publication-record owner value))
         (context (physical-reseed-follow-installation-context owner)))
    (unless (and (= root-fd (physical-reseed-follow-configuration-role-fd
                             (physical-reseed-follow-installation-context-descriptors context) :root))
                 (equal (physical-reseed-follow-cancellation-configured-history record)
                        (physical-reseed-follow-cancellation-configured-history
                         (physical-reseed-follow-installation-context-record context))))
      (error "configured cancellation proof changed its root or predecessor"))
    (physical-reseed-follow-installation-verify context)
    record))

(defmethod physical-reseed-follow-configuration-publication-candidate
    ((owner physical-reseed-follow-cancellation-owner) context candidate)
  ;; Adoption belongs to the active promotion for this owner and mapped
  ;; context.  The authority also checks the candidate and predecessor.
  (let ((authority *physical-reseed-follow-cancellation-authority*))
    (unless (and (physical-reseed-follow-cancellation-authority-p authority)
                 (physical-reseed-follow-cancellation-authority-active-p authority)
                 (eq owner (physical-reseed-follow-cancellation-authority-owner authority))
                 (eq :promoting (physical-reseed-follow-cancellation-authority-phase authority))
                 (eq context (physical-reseed-follow-cancellation-publication-context owner)))
      (error "configured cancellation adoption requires its live publication authority"))
    (funcall (physical-reseed-follow-cancellation-authority-scope authority)
             authority owner
             (physical-reseed-follow-configuration-role-fd
              (physical-reseed-follow-installation-context-descriptors context) :root)
             candidate (physical-reseed-follow-cancellation-authority-previous authority) nil)
    (physical-reseed-follow-cancellation-publication-record owner candidate)))

(defmethod physical-reseed-follow-published-snapshot
    ((owner physical-reseed-follow-cancellation-owner) root-fd candidate snapshot previous)
  (physical-reseed-follow-installation-adopt-snapshot owner root-fd candidate snapshot previous))

(defmethod physical-reseed-follow-cancellation-cleanup-stamp
    ((owner physical-reseed-follow-cancellation-owner) root-fd check)
  (getf (physical-reseed-follow-cancellation-configured-observation-at owner root-fd check) :stamp))

(defmethod physical-reseed-follow-cancellation-audit-cleanup-stamp
    ((owner physical-reseed-follow-cancellation-owner) stamp)
  (physical-reseed-follow-retirement-check-stamp stamp))
