(in-package #:pg)

(defun physical-adoption-startup-follow-state ()
  (let* ((path (physical-adoption-follow-path))
         (before (physical-adoption-handoff-file-fact path +physical-adoption-follow-maximum-bytes+))
         (state (physical-adoption-follow-decode
                 (read-bounded-regular-file path +physical-adoption-follow-maximum-bytes+)))
         (plan (getf state :plan))
         (identity (getf plan :journal)))
    (unless (and (= (getf state :step) (length (physical-adoption-follow-moves plan)))
                 (not (getf state :partial))
                 (= (getf identity :device) (getf before :device))
                 (= (getf identity :inode) (getf before :inode))
                 (= (getf state :file-size) (getf before :size))
                 (equal before (physical-adoption-handoff-file-fact
                                path +physical-adoption-follow-maximum-bytes+)))
      (error "adoption startup requires an unchanged terminal follow journal"))
    state))

(defun physical-adoption-startup-preflight ()
  (if (physical-adoption-path-present-p (physical-adoption-follow-path))
      (physical-adoption-startup-follow-state)
      (physical-adoption-check-startup)))

(defun physical-adoption-reconcile-startup ()
  (if (physical-adoption-path-present-p (physical-adoption-follow-path))
      (call-with-physical-adoption-startup-owner #'physical-adoption-retire-resume)
      (physical-adoption-check-startup)))
