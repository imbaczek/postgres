(in-package #:pg)

(defun physical-reseed-follow-inert-settlement-candidate (context value)
  (let* ((before (physical-reseed-follow-installation-context-record context))
         (candidate (physical-reseed-follow-copy value))
         (configuration (getf before :configuration))
         (maximum (length (physical-reseed-follow-configuration-moves
                           before (physical-reseed-follow-installation-context-binding context)))))
    (physical-reseed-follow-check-transition before candidate)
    (unless (and (eq :inert (getf configuration :phase))
                 (= maximum (getf configuration :step)
                    (physical-reseed-follow-installation-context-prefix context)))
      (error "settlement candidate requires the exact completed inert prefix"))
    (physical-reseed-follow-image candidate)
    candidate))

(defun physical-reseed-follow-pending-settlement-candidate (context value)
  (let ((before (physical-reseed-follow-installation-context-record context))
        (candidate (physical-reseed-follow-inert-settlement-candidate context value)))
    (unless (and (null (getf before :settlement))
                 (eq :handoff-pending (getf (getf candidate :settlement) :phase)))
      (error "pending settlement requires its exact unselected inert predecessor"))
    candidate))

(defmethod physical-reseed-follow-configuration-publication-candidate :around
    ((owner physical-reseed-follow-configuration-owner) context candidate)
  (let ((pending *physical-reseed-follow-settlement-candidate*))
    (if pending
        (progn
          (when *physical-reseed-follow-installation-candidate*
            (error "configuration publication has conflicting operation candidates"))
          (unless (equal candidate pending)
            (error "settlement publication differs from its frozen candidate"))
          (unless (eq owner (physical-reseed-follow-installation-context-owner context))
            (error "settlement publication has a different owner"))
          (physical-reseed-follow-pending-settlement-candidate context candidate))
        (call-next-method))))

(defun physical-reseed-follow-publish-pending-settlement (context candidate check)
  (let* ((owner (physical-reseed-follow-installation-context-owner context))
         (candidate (physical-reseed-follow-pending-settlement-candidate context candidate))
         (bytes (physical-reseed-follow-image candidate))
         (*physical-reseed-follow-settlement-candidate* candidate))
    (physical-reseed-follow-check-active-attempt owner)
    (funcall check)
    (physical-reseed-follow-write-installed-intent context candidate bytes check)))

(defun physical-reseed-follow-begin-settlement (owner)
  (call-with-physical-reseed-follow-installation-operation
   owner
   (lambda (root check)
     (let* ((context (physical-reseed-follow-installation-context owner))
            (record (physical-reseed-follow-copy
                     (physical-reseed-follow-installation-context-record context))))
       (funcall check)
       (physical-reseed-follow-check-active-attempt owner)
       (physical-adoption-require-absent
        (merge-pathnames "physical-reseed.follow.tmp" (physical-image-fd-path root))
        "settlement temporary requires explicit recovery")
       (unless (eq :inert (getf (getf record :configuration) :phase))
         (error "settlement requires installed inert configuration"))
       (if (getf record :settlement)
           (values record (physical-reseed-follow-installation-context-snapshot context))
           (progn
             (setf (getf record :settlement)
                   (list :phase :handoff-pending
                         :successor (physical-reseed-follow-settlement-successor record) :receipt nil))
             (physical-reseed-follow-publish-pending-settlement context record check)))))))
