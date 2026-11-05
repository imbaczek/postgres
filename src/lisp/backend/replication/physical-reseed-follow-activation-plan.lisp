(in-package #:pg)

(defun physical-reseed-follow-activation-check-prefix (value prefix)
  ;; Absent, building and staged intent require observed prefix 0.  Active
  ;; requires 2.  While activating, the observed prefix may equal the recorded
  ;; step or be one ahead, allowing a move completed before its journal update.
  ;; The caller must apply this check to the observed layout before acting.
  (let* ((record (physical-reseed-follow-copy value))
         (activation (getf record :activation))
         (phase (getf activation :phase))
         (step (getf activation :step)))
    (unless (and (typep prefix '(integer 0 2))
                 (case phase
                   ((nil :building :staged) (zerop prefix))
                   (:activating (or (= prefix step) (= prefix (min 2 (1+ step)))))
                   (:active (= prefix 2))))
      (error "observed activation prefix disagrees with recorded progress"))
    prefix))

(defun physical-reseed-follow-activation-plan-record (value binding)
  ;; Check the historical pending binding against its frozen image, then
  ;; derive and check the exact active image.  Building has no captured entry
  ;; and cannot be planned.  This check gives no filesystem or startup authority.
  (let* ((record (physical-reseed-follow-copy value))
         (activation (getf record :activation))
         (bytes (physical-follow-config-image
                 (physical-reseed-follow-activation-config record binding)))
         (terminal (length (physical-reseed-follow-configuration-moves record binding))))
    (unless (and (member (getf activation :phase) '(:staged :activating :active))
                 (getf activation :entry)
                 (= terminal (getf (getf record :configuration) :step))
                 (equal (getf activation :image)
                        (list (length bytes) (physical-store-update-digest #xCBF29CE484222325 bytes))))
      (error "activation plan requires a captured exact active image and terminal inert predecessor"))
    (values record terminal)))

(defun physical-reseed-follow-activation-moves (value binding)
  ;; Move the pending follow control aside before placing the active image.
  ;; Recovery controls, history, and the receive directory are unchanged.
  (physical-reseed-follow-activation-plan-record value binding)
  (physical-adoption-attempt-value-copy
   '(("follow.pending" :root "physical-follow.control" :saved "follow.pending")
     ("follow.active" :staged "follow.active" :root "physical-follow.control"))))

(defun physical-reseed-follow-activation-positions (value binding prefix)
  ;; Describe any static prefix from 0 through 2, independent of the recorded
  ;; activation phase.  A driver must require staged/0 at observed prefix 0,
  ;; activating/N at N or one move ahead (up to 2), and active/2 at prefix 2.
  ;; The returned positions describe expected names, not observed disk state.
  (multiple-value-bind (record terminal)
      (physical-reseed-follow-activation-plan-record value binding)
    (unless (typep prefix '(integer 0 2)) (error "invalid activation move prefix"))
    (let ((positions (append (physical-reseed-follow-configuration-positions record binding terminal)
                             (list (list (copy-seq "follow.active") :staged (copy-seq "follow.active"))))))
      (physical-reseed-follow-apply-position-moves
       positions (subseq (physical-reseed-follow-activation-moves record binding) 0 prefix)
       "activation move lacks a unique source or empty destination"))))

(defun physical-reseed-follow-activation-inventories (value-plan value binding prefix extra)
  ;; Extend the terminal installation inventories with the active image and
  ;; apply the requested static prefix.  This does not inspect directories.
  (multiple-value-bind (record terminal)
      (physical-reseed-follow-activation-plan-record value binding)
    (unless (typep prefix '(integer 0 2)) (error "invalid activation inventory prefix"))
    (let ((inventories (physical-reseed-follow-configuration-inventories
                        value-plan record binding terminal extra)))
      (when (member "follow.active" (getf inventories :staged) :test #'equal)
        (error "historical staged inventory already contains the active image"))
      (push (copy-seq "follow.active") (getf inventories :staged))
      (dolist (move (subseq (physical-reseed-follow-activation-moves record binding) 0 prefix))
        (destructuring-bind (logical from-role from-leaf to-role to-leaf) move
          (declare (ignore logical))
          (unless (and (= 1 (count from-leaf (getf inventories from-role) :test #'equal))
                       (not (member to-leaf (getf inventories to-role) :test #'equal)))
            (error "activation inventory move lacks a unique source or empty destination"))
          (setf (getf inventories from-role)
                (remove from-leaf (getf inventories from-role) :test #'equal :count 1))
          (push (copy-seq to-leaf) (getf inventories to-role))))
      (physical-reseed-follow-configuration-sort-inventories inventories))))
