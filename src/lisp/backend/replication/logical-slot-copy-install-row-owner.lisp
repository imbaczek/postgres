(in-package #:pg)

;;; Row publication extends the publisher scope with pending-prefix checks.
;;; The same constructor-based scope performs role checks before and after cleanup.
(defstruct (logical-slot-copy-install-row-writer
            (:include logical-slot-copy-install-publisher)
            (:constructor %make-logical-slot-copy-install-row-writer)
            (:print-function physical-libpq-print-private-object))
  "Publisher role holding the expected pending prefix and closed row facts."
  (value nil :read-only t) (expected-phase :pending :read-only t)
  prefix slot-root slot-work slots closed)

(defvar *logical-slot-copy-install-row-test-hook* nil)

(defun logical-slot-copy-install-check-fixed-origin (writer)
  "Require the actual spent fresh handoff or cold fixed-journal authority."
  (let* ((record (logical-slot-copy-install-publisher-record writer))
         (operation (logical-slot-copy-install-publisher-operation writer)))
    (ecase (logical-slot-copy-install-input-state-origin record)
      (:fresh
       (let* ((handoff (logical-slot-copy-install-input-state-handoff record))
              (entry (find handoff
                           (logical-slot-copy-install-operation-state-handoffs
                            (logical-slot-copy-install-operation-record operation))
                           :key #'logical-slot-copy-install-handoff-record-handoff :test #'eq)))
         (unless (and entry (logical-slot-copy-install-handoff-p handoff)
                      (eq operation (logical-slot-copy-install-handoff-operation handoff))
                      (eq :spent (logical-slot-copy-install-handoff-record-state entry))
                      (equalp (logical-slot-copy-install-publisher-image writer)
                              (logical-slot-copy-install-handoff-record-image entry)))
           (error "logical installation rows require the spent fresh handoff"))))
      (:cold
       (unless (eq :cold (logical-slot-copy-install-operation-state-mode
                          (logical-slot-copy-install-operation-record operation)))
         (error "logical installation rows require cold fixed-journal authority"))))))

(defun logical-slot-copy-install-check-row-closed (writer)
  "Reopen closed rows without following links through held parents and compare facts."
  (let ((closed (logical-slot-copy-install-row-writer-closed writer)))
    (when closed
      (logical-slot-copy-descriptor-list-count closed +logical-slot-copy-descriptor-maximum-slots+)
      (call-with-logical-slot-copy-install-inspection
       (logical-slot-copy-install-publisher-operation writer)
       (lambda (root)
         (let* ((value (logical-slot-copy-install-row-writer-value writer))
                (slot-root (logical-slot-copy-install-inspection-open root "pg_replslot" :slot-root nil))
                (rows (getf (logical-slot-copy-import-decode (getf value :certificate)) :slots)))
           (logical-slot-copy-install-inspection-identity slot-root (getf value :slot-root))
           (loop for saved in (reverse closed) for row in rows for expected in (getf value :entries)
                 do (unless (equal saved (logical-slot-copy-install-layout-row slot-root slot-root row expected))
                      (error "logical installation closed row facts changed")))))))))

(defmethod logical-slot-copy-install-publisher-check-role ((writer logical-slot-copy-install-row-writer))
  "Require the origin, exact pending prefix, and closed row observations."
  (logical-slot-copy-install-check-fixed-origin writer)
  (let* ((layout (logical-slot-copy-install-read-layout
                  (logical-slot-copy-install-publisher-operation writer)
                  (logical-slot-copy-install-publisher-image writer)))
         (prefix (logical-slot-copy-install-row-writer-prefix writer)))
    (unless (and (member (logical-slot-copy-install-row-writer-expected-phase writer) '(:pending :active))
                 (eq (logical-slot-copy-install-row-writer-expected-phase writer) (getf layout :phase))
                 (integerp prefix) (= prefix (getf layout :prefix))
                 (or (eq :pending (getf layout :phase))
                     (and (eq :cold (logical-slot-copy-install-input-state-origin
                                     (logical-slot-copy-install-publisher-record writer)))
                          (= prefix (length (getf (logical-slot-copy-install-row-writer-value writer) :entries))))))
      (error "logical installation row phase or expected prefix changed")))
  (logical-slot-copy-install-check-row-closed writer)
  writer)

(defmethod logical-slot-copy-install-publisher-check-closed-role ((writer logical-slot-copy-install-row-writer))
  "Recheck origin and closed rows after publisher descriptor cleanup."
  (logical-slot-copy-install-check-fixed-origin writer)
  (logical-slot-copy-install-check-row-closed writer))

(defun logical-slot-copy-install-check-row-writer (writer)
  "Check the typed row role and its registered publisher bindings."
  (unless (logical-slot-copy-install-row-writer-p writer)
    (error "logical installation requires a row writer"))
  (logical-slot-copy-install-check-publisher writer))

(defun logical-slot-copy-install-row-point (writer point name)
  (when *logical-slot-copy-install-row-test-hook*
    (funcall *logical-slot-copy-install-row-test-hook* point writer name))
  (logical-slot-copy-install-check-row-writer writer))

(defun logical-slot-copy-install-row-proof (writer)
  "Recheck row authority and the complete retained source proof."
  (logical-slot-copy-install-check-row-writer writer)
  (logical-slot-copy-install-publisher-proof writer))

(defun logical-slot-copy-install-row-bind (writer)
  "Bind the recorded slot parents in their current filesystem group.
The slot root may be on a different filesystem from the operation root."
  (let* ((value (logical-slot-copy-install-row-writer-value writer))
         (root (logical-slot-copy-install-publisher-root writer))
         (slot-root (logical-slot-copy-install-publisher-open writer root "pg_replslot" :slot-root :group nil))
         (work (logical-slot-copy-install-publisher-open writer slot-root ".logical-slot-copy.work" :directory :group slot-root))
         (slots (logical-slot-copy-install-publisher-open writer work (getf value :stage-id) :directory :group slot-root)))
    (loop for node in (list slot-root work slots) for key in '(:slot-root :slot-work :slot-container)
          do (unless (equal (subseq (logical-slot-copy-install-node-identity node) 0 2)
                            (subseq (getf value key) 0 2))
               (error "logical installation row parents differ from their journal")))
    (setf (logical-slot-copy-install-row-writer-slot-root writer) slot-root
          (logical-slot-copy-install-row-writer-slot-work writer) work
          (logical-slot-copy-install-row-writer-slots writer) slots)
    (logical-slot-copy-install-row-proof writer)))
