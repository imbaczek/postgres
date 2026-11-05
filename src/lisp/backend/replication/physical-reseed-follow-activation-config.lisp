(in-package #:pg)

(defun physical-reseed-follow-activation-config (value binding)
  (physical-reseed-install-check-literal (list value binding))
  (multiple-value-bind (record selected)
      (physical-reseed-follow-configuration-plan-record value binding)
    (declare (ignore selected))
    (unless (and (eq :inert (getf (getf record :configuration) :phase))
                 (eq :handed-off (getf (getf record :settlement) :phase)))
      (error "activation configuration requires settled inert handoff"))
    (let ((config (physical-adoption-attempt-value-copy
                   (physical-reseed-follow-pending-config record binding))))
      (setf (getf (fifth config) :phase) :active
            (getf (sixth config) :enabled) t)
      (physical-follow-check-config config))))
