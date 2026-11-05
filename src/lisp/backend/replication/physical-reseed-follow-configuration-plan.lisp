(in-package #:pg)

;;; Derive configuration installation moves and positions from a saved record
;;; and binding.  These functions only describe a plan; they do not inspect or
;;; change files, or authorize installation or startup.  Local drivers use these
;;; plans for Building, Staged, Installing and Inert.  Handoff and activation
;;; are separate work.

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

(defun physical-reseed-follow-apply-position-moves (positions moves error-message)
  (dolist (move moves)
    (destructuring-bind (logical from-role from-leaf to-role to-leaf) move
      (let ((sources (remove-if-not
                      (lambda (entry) (and (equal logical (first entry)) (eq from-role (second entry))
                                           (equal from-leaf (third entry))))
                      positions)))
        (unless (and (= 1 (length sources))
                     (not (find (list to-role to-leaf) positions :key #'rest :test #'equal)))
          (error error-message))
        (setf (second (first sources)) to-role (third (first sources)) (copy-seq to-leaf)))))
  positions)

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
    (physical-reseed-follow-apply-position-moves
     positions (subseq moves 0 prefix)
     "configuration move lacks a unique source or empty destination")))

;; Check the saved terminal installation plan against the configuration record.
;; Configuration planning must not rewrite the original base or predecessor.
(defun physical-reseed-follow-configuration-install-plan (value-plan value)
  (let* ((plan (physical-reseed-install-copy value-plan))
         (record (physical-reseed-follow-copy value))
         (step (physical-reseed-install-last-step plan))
         (reference (getf record :install))
         (identity (getf reference :identity))
         (image (physical-reseed-install-image plan step))
         (decoded (physical-reseed-install-decode image)))
    (unless (and (equal (getf plan :attempt) (getf record :attempt))
                 (= (getf plan :manifest-digest) (getf (getf record :base) :manifest-digest))
                 (= step (getf reference :step))
                 (= (getf (getf plan :journal) :device) (first identity))
                 (= (getf (getf plan :journal) :inode) (second identity))
                 (= (length image) (sixth identity))
                 (= (getf decoded :body-crc) (getf reference :body-crc))
                 (= (physical-store-update-digest #xCBF29CE484222325 image) (getf reference :digest)))
      (error "configuration inventories differ from their original install plan"))
    (values plan record)))

(defun physical-reseed-follow-configuration-sort-inventories (inventories)
  (loop for (role names) on inventories by #'cddr
        do (unless (= (length names) (length (remove-duplicates names :test #'equal)))
             (error "configuration inventory has duplicate names in ~a" role))
           (setf (getf inventories role) (sort (mapcar #'copy-seq names) #'string<)))
  inventories)

(defun physical-reseed-follow-configuration-inventories (value-plan value binding prefix extra)
  (multiple-value-bind (plan record) (physical-reseed-follow-configuration-install-plan value-plan value)
    (physical-reseed-follow-configuration-plan-record record binding)
    (let* ((positions (physical-reseed-follow-configuration-positions record binding prefix))
           (terminal (1- (physical-reseed-install-last-step plan)))
           (original (physical-reseed-install-expected-names plan terminal extra))
           (inventories (list :root (remove "physical-recovery.control" (getf original :root) :test #'equal)
                              :incoming (getf original :incoming) :quarantine (getf original :quarantine)
                              :base nil :global nil :wal nil
                              :workspace (list ".pg_lisp.lock" "physical-reseed.install" "staged" "saved")
                              :staged nil :saved nil :receive nil)))
      (dolist (directory '((:base "base/") (:global "global/") (:wal "pg_wal/")))
        (destructuring-bind (role path) directory
          (setf (getf inventories role)
                (loop for row in (getf plan :new)
                      for name = (first row)
                      when (uiop:string-prefix-p path name)
                        collect (subseq name (length path))))))
      (dolist (position positions)
        (destructuring-bind (logical role leaf) position
          (declare (ignore logical))
          (unless (member role '(:root :workspace :staged :saved :receive :wal))
            (error "configuration position has an unknown directory role"))
          (push leaf (getf inventories role))))
      (physical-reseed-follow-configuration-sort-inventories inventories))))

(defun physical-reseed-follow-configuration-prefix-signature (value-plan value prefix extra)
  (multiple-value-bind (plan record) (physical-reseed-follow-configuration-install-plan value-plan value)
    (let* ((configuration (getf record :configuration))
           (generation (car (last (getf record :generations))))
           (selected (physical-reseed-follow-configuration-selected-p
                      (physical-reseed-follow-core-projection record)))
           (maximum (if selected 5 4))
           (pending (if selected 3 2))
           (original (physical-reseed-install-expected-names
                      plan (1- (physical-reseed-install-last-step plan)) extra))
           (root (remove "physical-recovery.control" (getf original :root) :test #'equal))
           (staged (append (list "follow.pending" "recovery.initial")
                           (when selected (list "history.wal" "receive.initial" "recovery.selected")))))
      (unless (and (member (getf configuration :phase) '(:staged :installing :inert))
                   (eq :proved (getf generation :phase))
                   (typep prefix `(integer 0 ,maximum)))
        (error "invalid configuration prefix signature"))
      (when (plusp prefix) (push "pg_physical_receive" root))
      (unless (= prefix (1- maximum)) (push "physical-recovery.control" root))
      (when (>= prefix pending)
        (push "physical-follow.control" root)
        (setf staged (remove "follow.pending" staged :test #'equal)))
      (when (and selected (>= prefix 2))
        (setf staged (remove "history.wal" staged :test #'equal)))
      (when (= prefix maximum)
        (setf staged (remove (if selected "recovery.selected" "recovery.initial") staged :test #'equal)))
      (physical-reseed-follow-configuration-sort-inventories
       (list :root root
             :workspace (append (list ".pg_lisp.lock" "physical-reseed.install" "staged" "saved")
                                (when (zerop prefix) (list "receive")))
             :staged staged :saved (when (>= prefix (1- maximum)) (list "recovery.original")))))))
