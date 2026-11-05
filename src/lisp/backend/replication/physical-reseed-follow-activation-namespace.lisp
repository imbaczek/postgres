(in-package #:pg)

(defun physical-reseed-follow-activation-active-row (descriptors record binding positions mount)
  (let* ((position (assoc "follow.active" positions :test #'equal))
         (parent (physical-reseed-follow-configuration-role-fd descriptors (second position)))
         (leaf (third position))
         (bytes (physical-follow-config-image
                 (physical-reseed-follow-activation-config record binding)))
         (before (physical-reseed-install-old-observe parent leaf mount))
         (row (physical-reseed-follow-configuration-file-row parent leaf "follow.active" bytes)))
    (unless (and (equal row (getf (getf record :activation) :entry))
                 (equal before (physical-reseed-install-old-observe parent leaf mount)))
      (error "active configuration lost its captured identity or stable metadata"))
    (list (copy-seq "follow.active") parent (copy-seq leaf) before)))

(defun physical-reseed-follow-activation-layout-state (owner descriptors value binding images prefix check)
  (unless (physical-reseed-follow-configuration-owner-p owner)
    (error "mapped layout requires configuration ownership"))
  (physical-reseed-check-owner owner)
  (funcall check)
  (multiple-value-bind (plan record)
      (physical-reseed-follow-configuration-install-plan (physical-reseed-install-owner-plan owner) value)
    (let* ((root (physical-reseed-follow-configuration-role-fd descriptors :root))
           (mount (physical-adoption-install-mount-id nil :directory-fd root))
           (positions (physical-reseed-follow-activation-positions record binding prefix))
           (extra (physical-reseed-install-extra-entries owner root))
           (expected (physical-reseed-follow-activation-inventories plan record binding prefix extra)))
      (physical-reseed-follow-layout-proof
       owner descriptors plan record positions images extra expected mount check
       (lambda () (physical-reseed-follow-activation-active-row descriptors record binding positions mount))))))

(defun physical-reseed-follow-activation-output-state (owner descriptors value binding images check)
  (unless (physical-reseed-follow-configuration-owner-p owner)
    (error "activation output requires configuration ownership"))
  (physical-reseed-check-owner owner)
  (funcall check)
  (let* ((record (physical-reseed-follow-copy value))
         (activation (getf record :activation))
         (bytes (physical-follow-config-image
                 (physical-reseed-follow-activation-config record binding)))
         (image (list (length bytes) (physical-store-update-digest #xCBF29CE484222325 bytes)))
         (terminal (length (physical-reseed-follow-configuration-moves record binding)))
         (staged (physical-reseed-follow-configuration-role-fd descriptors :staged)))
    (unless (and (= terminal (getf (getf record :configuration) :step))
                 (or (null activation)
                     (and (eq :building (getf activation :phase))
                          (equal image (getf activation :image)))))
      (error "activation output requires absent intent or its exact Building image"))
    (if (physical-adoption-path-present-p
         (merge-pathnames "follow.active" (physical-image-fd-path staged)))
        (progn
          (unless activation (error "active output exists without Building intent"))
          (let* ((row (physical-reseed-follow-configuration-file-row
                       staged "follow.active" "follow.active" bytes))
                 (candidate (physical-reseed-follow-copy record)))
            (setf (getf candidate :activation)
                  (physical-reseed-follow-activation-copy
                   (list :phase :staged :step 0 :image image :entry row) record))
            (physical-reseed-follow-check-transition record candidate)
            (list :entry (getf (getf candidate :activation) :entry)
                  :layout (physical-reseed-follow-activation-layout-state
                           owner descriptors candidate binding images 0 check))))
        (list :entry nil
              :layout (physical-reseed-follow-configuration-layout-state
                       owner descriptors record binding images terminal check)))))
