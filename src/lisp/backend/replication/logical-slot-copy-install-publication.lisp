(in-package #:pg)

;;; Publish the journal container or resync its existing fixed form.
;;; This does not install slot rows or activate the registry.
(defun logical-slot-copy-install-publisher-check-phase (publisher phase prefix)
  "Reprove the input and require the exact expected phase and prefix."
  (logical-slot-copy-install-publisher-proof publisher)
  (let ((record (logical-slot-copy-install-publisher-record publisher)))
    (unless (and (eq phase (logical-slot-copy-install-input-state-phase record))
                 (= prefix (logical-slot-copy-install-input-state-prefix record)))
      (error "logical installation journal publication changed its phase or prefix")))
  (logical-slot-copy-install-check-publisher publisher))

(defun logical-slot-copy-install-publisher-bind (publisher)
  "Bind the selected private or fixed container and its journal files."
  (let* ((record (logical-slot-copy-install-publisher-record publisher))
         (value (logical-slot-copy-install-decode (logical-slot-copy-install-publisher-image publisher)))
         (root (logical-slot-copy-install-publisher-root publisher))
         (fresh (eq :fresh (logical-slot-copy-install-input-state-origin record)))
         (phase (logical-slot-copy-install-input-state-phase record))
         (prefix (logical-slot-copy-install-input-state-prefix record))
         (work (logical-slot-copy-install-publisher-open publisher root "logical-slot-copy.work" :directory :missing (not fresh)))
         (container (logical-slot-copy-install-publisher-open
                     publisher (if fresh work root)
                     (if fresh (getf value :stage-id) "logical-slot-copy.install") :directory)))
    (unless (if fresh (and (eq phase :private) (zerop prefix))
                (member phase '(:pending :active)))
      (error "logical installation journal publication requires its original layout"))
    (unless (equal (subseq (getf value :container) 0 2)
                   (subseq (logical-slot-copy-install-node-identity container) 0 2))
      (error "logical installation publisher container differs from its journal"))
    (setf (logical-slot-copy-install-publisher-work publisher) work
          (logical-slot-copy-install-publisher-container publisher) container
          (logical-slot-copy-install-publisher-journal publisher)
          (logical-slot-copy-install-publisher-open publisher container "journal" :file)
          (logical-slot-copy-install-publisher-active publisher)
          (logical-slot-copy-install-publisher-open publisher container "active" :file)
          (logical-slot-copy-install-publisher-phase publisher) (list phase prefix))
    (logical-slot-copy-install-publisher-check-phase publisher phase prefix)))

(defun logical-slot-copy-install-publisher-rebind (publisher)
  "Bind the held container and children to their fixed names after rename."
  (let* ((old (logical-slot-copy-install-publisher-container publisher))
         (new (%make-logical-slot-copy-install-node
               :fd (logical-slot-copy-install-node-fd old)
               :parent (logical-slot-copy-install-publisher-root publisher)
               :name "logical-slot-copy.install" :kind :directory
               :identity (logical-slot-copy-install-node-identity old))))
    (labels ((rebind (node)
               (cond ((eq node old) new)
                     ((eq (logical-slot-copy-install-node-parent node) old)
                      (%make-logical-slot-copy-install-node
                       :fd (logical-slot-copy-install-node-fd node) :parent new
                       :name (logical-slot-copy-install-node-name node)
                       :kind (logical-slot-copy-install-node-kind node)
                       :identity (logical-slot-copy-install-node-identity node)))
                     (t node))))
      (setf (logical-slot-copy-install-publisher-nodes publisher)
            (mapcar #'rebind (logical-slot-copy-install-publisher-nodes publisher))
            (logical-slot-copy-install-publisher-container publisher) new
            (logical-slot-copy-install-publisher-journal publisher)
            (find "journal" (logical-slot-copy-install-publisher-nodes publisher)
                  :key #'logical-slot-copy-install-node-name :test #'equal)
            (logical-slot-copy-install-publisher-active publisher)
            (find "active" (logical-slot-copy-install-publisher-nodes publisher)
                  :key #'logical-slot-copy-install-node-name :test #'equal)))))

(defun logical-slot-copy-install-publisher-rename (publisher)
  "Publish the fresh container exclusively using its original handoff.
No read-only inspection may span the rename.  Consume the registered handoff
immediately before NOREPLACE, then rebind held identities to the new path."
  (declare (notinline physical-adoption-renameat2))
  (let* ((record (logical-slot-copy-install-publisher-record publisher))
         (operation (logical-slot-copy-install-publisher-operation publisher))
         (handoff (logical-slot-copy-install-input-state-handoff record))
         (value (logical-slot-copy-install-decode (logical-slot-copy-install-publisher-image publisher))))
    (logical-slot-copy-install-publisher-point publisher :before-journal-rename)
    (logical-slot-copy-install-publisher-check-phase publisher :private 0)
    (when *logical-slot-copy-install-inspection*
      (error "logical installation publication cannot span readonly inspection"))
    (unless (equalp (logical-slot-copy-install-publisher-image publisher)
                    (logical-slot-copy-install-handoff-image operation handoff))
      (error "logical installation publisher handoff image changed"))
    (sb-sys:without-interrupts
      (logical-slot-copy-install-consume-handoff operation handoff)
      (unless (zerop (physical-adoption-renameat2
                      (logical-slot-copy-install-node-fd (logical-slot-copy-install-publisher-work publisher))
                      (getf value :stage-id)
                      (logical-slot-copy-install-node-fd (logical-slot-copy-install-publisher-root publisher))
                      "logical-slot-copy.install" +physical-adoption-rename-noreplace+))
        (error "cannot publish exclusive logical installation journal: errno ~d" (sb-alien:get-errno)))
      (logical-slot-copy-install-publisher-rebind publisher))
    (logical-slot-copy-install-publisher-point publisher :after-journal-rename)))

(defun logical-slot-copy-install-publisher-sync-journal (publisher)
  "Sync journal, active file, container, root, then source parent when present."
  (logical-slot-copy-install-publisher-sync
   publisher (logical-slot-copy-install-publisher-journal publisher) :after-journal-file-sync)
  (logical-slot-copy-install-publisher-sync
   publisher (logical-slot-copy-install-publisher-active publisher) :after-journal-active-sync)
  (logical-slot-copy-install-publisher-sync
   publisher (logical-slot-copy-install-publisher-container publisher) :after-journal-container-sync)
  (logical-slot-copy-install-publisher-sync
   publisher (logical-slot-copy-install-publisher-root publisher) :after-journal-target-parent-sync)
  (when (logical-slot-copy-install-publisher-work publisher)
    (logical-slot-copy-install-publisher-sync
     publisher (logical-slot-copy-install-publisher-work publisher) :after-journal-source-parent-sync)))

(defun logical-slot-copy-install-publish-journal (owner context input)
  "Publish a fresh journal or resync a fixed one under a proved input.
Cold resync preserves its validated pending prefix from zero through N, or
active phase with prefix N.  It certifies journal and container durability,
not moved-row durability, registry activation, or admission."
  (call-with-logical-slot-copy-install-publisher
   owner context input
   (lambda (publisher)
     (logical-slot-copy-install-publisher-bind publisher)
     (let* ((record (logical-slot-copy-install-publisher-record publisher))
            (fresh (eq :fresh (logical-slot-copy-install-input-state-origin record)))
            (phase (if fresh :pending (first (logical-slot-copy-install-publisher-phase publisher))))
            (prefix (second (logical-slot-copy-install-publisher-phase publisher))))
       (when fresh (logical-slot-copy-install-publisher-rename publisher))
       (logical-slot-copy-install-publisher-sync-journal publisher)
       (logical-slot-copy-install-publisher-point publisher :before-journal-final-proof)
       (logical-slot-copy-install-publisher-check-phase publisher phase prefix)
       (logical-slot-copy-install-publisher-point publisher :after-journal-final-proof)
       (logical-slot-copy-install-publisher-check-phase publisher phase prefix)
       phase))))

(defun logical-slot-copy-install-publish-fresh-journal ()
  "Prove a fresh handoff and publish its journal."
  (call-with-logical-slot-copy-install-fresh-input #'logical-slot-copy-install-publish-journal))

(defun logical-slot-copy-install-resync-fixed-journal ()
  "Reprove and resync an existing fixed journal during cold startup."
  (call-with-logical-slot-copy-install-cold-input #'logical-slot-copy-install-publish-journal))
