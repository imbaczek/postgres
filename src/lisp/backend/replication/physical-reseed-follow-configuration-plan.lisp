(in-package #:pg)

;;; Derive configuration installation moves and positions from a saved record
;;; and binding.  These functions only describe a plan; they do not inspect or
;;; change files, or authorize installation or startup.  Building and Staged
;;; have drivers today; Installing and Inert are for later work.

;; Copy and check the record against the pending binding and staged generation.
;; The returned selection flag says whether the binding chooses a child.
(defun physical-reseed-follow-configuration-plan-record (value binding)
  (let* ((record (physical-reseed-follow-copy value))
         (configuration (getf record :configuration))
         (generation (car (last (getf record :generations))))
         (selected (not (equal (getf binding :before) (getf binding :after)))))
    (physical-follow-binding-check binding)
    (unless (and (member (getf configuration :phase) '(:staged :installing :inert))
                 (eq :pending (getf binding :phase))
                 (equal (physical-reseed-follow-generation-anchor generation) (getf binding :anchor))
                 (eq (not (null (physical-reseed-follow-configuration-selected-p
                                 (physical-reseed-follow-core-projection record))))
                     selected))
      (error "configuration move plan differs from its proved staged generation"))
    (let* ((pending (physical-follow-config-image (physical-reseed-follow-pending-config record binding)))
           (specification (assoc "follow.pending" (getf configuration :images) :test #'equal)))
      (unless (and (= (length pending) (second specification))
                   (= (physical-store-update-digest #xCBF29CE484222325 pending) (third specification)))
        (error "configuration move binding differs from its saved pending image")))
    (values record selected)))

;; Return ordered (logical-name from-role from-leaf to-role to-leaf) moves.
;; Roles name directories, while logical names identify images independently of
;; their current leaves.  A selected immediate child adds its WAL history.
;; The original root control moves aside before the replacement moves in, so
;; root control is temporarily absent between those moves.
(defun physical-reseed-follow-configuration-moves (value binding)
  (multiple-value-bind (record selected) (physical-reseed-follow-configuration-plan-record value binding)
    (declare (ignore record))
    (let ((recovery (if selected "recovery.selected" "recovery.initial")))
      (mapcar
       (lambda (move) (mapcar (lambda (item) (if (stringp item) (copy-seq item) item)) move))
       (append
        (list (list :receive-directory :workspace "receive" :root "pg_physical_receive"))
        (when selected
          (list (list "history.wal" :staged "history.wal" :wal
                      (timeline-history-filename (eighth (getf binding :after))))))
        (list (list "follow.pending" :staged "follow.pending" :root "physical-follow.control")
              (list :original-recovery :root "physical-recovery.control" :saved "recovery.original")
              (list recovery :staged recovery :root "physical-recovery.control")))))))

;; Return (logical-name role leaf) positions after PREFIX moves.  Include all
;; image positions, including unused images, so a later check can preserve and
;; account for them.  Each move needs one source and an empty destination.
(defun physical-reseed-follow-configuration-positions (value binding prefix)
  (let* ((record (physical-reseed-follow-configuration-plan-record value binding))
         (moves (physical-reseed-follow-configuration-moves record binding))
         (positions
           (append
            (list (list :receive-directory :workspace (copy-seq "receive"))
                  (list :original-recovery :root (copy-seq "physical-recovery.control")))
            (loop for (name size digest) in (getf (getf record :configuration) :images)
                  collect (multiple-value-bind (directory leaf)
                              (physical-reseed-follow-configuration-layout-name name binding)
                            (list (copy-seq name) (ecase (intern (string-upcase directory) :keyword)
                                                    (:receive :receive) (:staged :staged))
                                  (copy-seq leaf)))))))
    (unless (typep prefix `(integer 0 ,(length moves))) (error "invalid configuration installation prefix"))
    (dolist (move (subseq moves 0 prefix))
      (destructuring-bind (logical from-role from-leaf to-role to-leaf) move
        (let ((sources (remove-if-not
                        (lambda (entry) (and (equal logical (first entry)) (eq from-role (second entry))
                                             (equal from-leaf (third entry))))
                        positions)))
          (unless (and (= 1 (length sources))
                       (not (find (list to-role to-leaf) positions :key #'rest :test #'equal)))
            (error "configuration move lacks a unique source or empty destination"))
          (setf (second (first sources)) to-role (third (first sources)) (copy-seq to-leaf)))))
    positions))
