(in-package #:pg)

;;; Reconcile durable backup lease ownership before admitting server work.
;;; Record loading has already interrupted requests and captures left by a
;;; previous process.  Pin and origin inventories must be loaded before the
;;; remaining READY and CLOSING records can be reconciled here.

;; Under write and WAL exclusion, verify both the indexed owner and its disk
;; image before using it to change a pin or publish another lease phase.
(defun physical-backup-lease-current-record (request)
  (physical-backup-lease-require-locks)
  (let ((record (physical-backup-lease-record-under-lock (getf request :name))))
    (unless (and record (equal request (getf record :request)))
      (wal-invalid-input "backup lease request has no matching owner"))
    (physical-backup-lease-check-source request)
    (unless (equal record (physical-backup-lease-read (physical-backup-lease-path request)))
      (error "backup lease ownership changed on disk"))
    record))

;; A standby does not install primary source retention.  Validate all pins
;; first, then durably mark newly lost pins before installing the candidates.
;; The publication fence covers disk and memory changes, including failures.
(defun physical-backup-lease-load-standby-pins ()
  (physical-backup-lease-load-startup-pins t))

(declaim (ftype function physical-backup-lease-selected-observation-under-lock))

(declaim (ftype function physical-backup-lease-startup-source-context
                physical-backup-lease-check-startup-source-context
                physical-backup-lease-startup-selected-state))

(defun physical-backup-lease-closing-bootstrap-pin (record)
  (let* ((request (getf record :request))
         (name (getf request :name))
         (current (physical-backup-lease-current-record request))
         (selected-name (getf (getf current :successor) :name)))
    (unless (and (eq :closing (getf current :phase)) (equal current record))
      (wal-invalid-input "backup lease has no matching closing intent"))
    (when (or (gethash name *physical-backup-lease-capture-owners*)
              (gethash name *physical-source-senders*)
              (gethash selected-name *physical-source-senders*))
      (wal-invalid-input "backup lease closing still has an active source owner"))
    (physical-backup-lease-pin-directory-stat)
    (multiple-value-bind (pin disk) (physical-source-retirement-pins name)
      (declare (ignore disk))
      (when pin (physical-backup-lease-check-pin-unlink pin request))
      pin)))

;; Validate the closing intent and its successor before finding the bootstrap
;; pin to remove. The successor may be present only on disk during startup.
(defun physical-backup-lease-closing-pin (record)
  (let* ((request (getf record :request))
         (current (physical-backup-lease-current-record request)))
    (unless (and (eq :closing (getf current :phase)) (equal current record))
      (wal-invalid-input "backup lease has no matching closing intent"))
    (when (eq :handed-off (getf record :outcome))
      ;; The observation establishes the selected pin under the locks.  Closing
      ;; still requires the strict unused-pin and source WAL checks below.
      (let ((successor
              (nth-value 1 (physical-backup-lease-selected-observation-under-lock record))))
        (physical-backup-lease-check-successor-pin record successor)
        (physical-follow-pin-check-wal successor)))
    (physical-backup-lease-check-successor-assignment record)
    (physical-backup-lease-closing-bootstrap-pin record)))

(declaim (ftype function physical-backup-lease-selected-cleanup-claim
                physical-backup-lease-finish-aborted-allocations-under-lock
                physical-backup-lease-allocation-abort-cleanup-plan
                physical-backup-lease-allocation-canceled-cleanup-plan
                physical-backup-lease-finish-canceled-allocations-under-lock))

;; Finish allocation cleanup for an aborted parent, including a CLOSED retry.
;; Retire and sync the bootstrap pin before publishing CLOSED. Normal handoff
;; closing checks the selected successor and its WAL. Explicit selected cleanup
;; uses scoped authority to finish a committed handoff. The caller holds write
;; and WAL exclusion and supplies the publication fence callback.
(defun physical-backup-lease-finish-closing-under-lock (request begin-publication &optional cleanup-authority)
  ;; A supplied cleanup authority claims retirement before any terminal branch.
  (physical-backup-lease-selected-cleanup-check-entry cleanup-authority)
  (let* ((record (physical-backup-lease-current-record request))
         (name (getf request :name))
         (cleanup-pin (when cleanup-authority
                        (physical-backup-lease-selected-cleanup-claim
                         cleanup-authority record :retire))))
    (when (gethash name *physical-backup-lease-capture-owners*)
      (wal-invalid-input "backup lease still has an active capture owner"))
    (when (eq :aborted (getf record :outcome))
      (physical-backup-lease-finish-aborted-allocations-under-lock record begin-publication))
    (when (eq :closed (getf record :phase))
      (when (or (gethash name *physical-source-pins*)
                (physical-backup-lease-lstat (physical-source-pin-path name)))
        (error "closed backup lease still has a source pin"))
      (return-from physical-backup-lease-finish-closing-under-lock
        (physical-backup-lease-copy record)))
    (let ((pin (if cleanup-authority cleanup-pin
                   (physical-backup-lease-closing-pin record))))
      (funcall begin-publication)
      (if pin
          (physical-source-pin-unlink pin request)
          (if (physical-backup-lease-pin-directory-stat)
              (fsync-directory (physical-source-pin-directory))
              (fsync-directory *pgdata*)))
      ;; Unlink and sync finish before the publication claim.  Completion follows
      ;; the checked record readback and installation in the memory registry.
      (when cleanup-authority
        (physical-backup-lease-selected-cleanup-claim cleanup-authority record :retired))
      (let ((next (copy-list record)))
        (setf (getf next :phase) :closed)
        (let ((result (physical-backup-lease-publish-checked-under-lock
                       record (physical-backup-lease-transition record next)
                       begin-publication cleanup-authority)))
          (when cleanup-authority
            (physical-backup-lease-selected-cleanup-claim cleanup-authority result :done))
          result)))))

(defun physical-backup-lease-finish-closing (request)
  (let ((request (physical-backup-lease-request-copy request)))
    (with-write-lock
      (with-wal-publication-fence (begin-publication)
        (physical-backup-lease-finish-closing-under-lock request #'begin-publication)))))

;; Startup and STATUS share this READY check under write and WAL exclusion.
;; Missing or mismatched pin, origin, or required WAL is an error.  An explicit
;; pin loss, changed timeline, or standby source instead requests interruption.
(defun physical-backup-lease-ready-action (record)
  (let* ((request (getf record :request))
         (base (getf record :base))
         (identity (list (getf request :system-id) (getf request :timeline)
                         (getf base :base) (getf base :manifest-digest)))
         (entry (gethash identity *physical-export-origins*)))
    (physical-backup-lease-pin-directory-stat)
    (multiple-value-bind (pin disk)
        (handler-case (physical-source-retirement-pins (getf request :name))
          (wal-input-validation-error () (error "ready backup lease pin registry and disk disagree")))
      (declare (ignore disk))
      (unless pin (error "ready backup lease has no source pin"))
      (physical-backup-lease-check-pin-record pin record)
      (unless (and entry
                   (equal identity (physical-export-origin-identity
                                    (physical-export-origin-entry-origin entry)))
                   (equalp (physical-export-origin-entry-origin entry)
                           (physical-export-origin-read
                            (physical-export-origin-path (physical-export-origin-entry-origin entry))
                            :stabilize t)))
        (error "ready backup lease lacks its durable origin receipt"))
      (if (or (recovery-in-progress-p) (physical-source-pin-lost-p pin)
              (/= (getf request :timeline) *wal-insert-timeline*))
          :interrupt
          (progn (physical-source-pin-check-wal pin) nil)))))

(defun physical-backup-lease-check-successor-assignment (record)
  (let* ((name (getf (getf record :successor) :name))
         (owner (and name (gethash name (physical-backup-lease-successor-records)))))
    (when (and owner
               (not (equal (getf (getf record :request) :name)
                           (getf (getf owner :request) :name))))
      (wal-invalid-input "backup lease successor already belongs to another lease")))
  record)

;; Normal handoff publication checks the successor and retained WAL. A completed
;; base on an INTERRUPTED lease can be handed to a descendant source through a
;; matching successor. Explicit selected cleanup claims its own authority.
(defun physical-backup-lease-check-handoff-publication (before after &optional cleanup-authority)
  (when cleanup-authority
    (physical-backup-lease-check-successor-assignment after)
    (physical-backup-lease-selected-cleanup-claim cleanup-authority before :publish after)
    (return-from physical-backup-lease-check-handoff-publication after))
  (when (eq :handed-off (getf after :outcome))
    (physical-backup-lease-check-successor-assignment after)
    (unless (equal before after)
      (let* ((request (getf after :request))
             (name (getf request :name)))
        (when (or (gethash name *physical-backup-lease-capture-owners*)
                  (gethash name *physical-source-senders*))
          (wal-invalid-input "backup lease handoff still has an active bootstrap owner"))
        (ecase (getf after :phase)
          (:closing
           (unless (and (member (getf before :phase) '(:ready :interrupted))
                        (getf before :base)
                        (not (recovery-in-progress-p)))
             (wal-invalid-input "backup lease handoff requires a completed primary backup"))
           (physical-backup-lease-ready-action before)
           (let ((successor-name (getf (getf after :successor) :name)))
             (multiple-value-bind (pin disk) (physical-source-retirement-pins successor-name)
               (declare (ignore disk))
               (physical-backup-lease-check-successor-pin after pin)
               (physical-follow-pin-check-wal pin))))
          (:closed
           (physical-backup-lease-closing-pin before)
           (when (or (gethash name *physical-source-pins*)
                     (physical-backup-lease-lstat (physical-source-pin-path name)))
             (wal-invalid-input "backup lease handoff still has its bootstrap pin")))))))
  after)

;; Durable CLOSING is the handoff commit point. Finishing removes and syncs the
;; bootstrap pin before publishing CLOSED. An exact CLOSED retry returns the
;; saved outcome even after the successor has been retired normally.
(defun physical-backup-lease-handoff (request successor)
  (let* ((request (physical-backup-lease-request-copy request))
         (successor (physical-backup-lease-successor-copy successor request)))
    (with-live-operation
      (with-write-lock
        (with-wal-publication-fence (begin-publication)
          (let* ((record (physical-backup-lease-current-record request))
                 (phase (getf record :phase)))
            (if (member phase '(:closing :closed))
                (unless (and (eq :handed-off (getf record :outcome))
                             (equal successor (getf record :successor)))
                  (wal-invalid-input "backup lease already has a different terminal outcome"))
                (let ((next (copy-list record)))
                  (setf (getf next :phase) :closing
                        (getf next :outcome) :handed-off
                        (getf next :successor) successor)
                  (physical-backup-lease-publish-checked-under-lock
                   record (physical-backup-lease-transition record next) #'begin-publication)))
            (physical-backup-lease-finish-closing-under-lock request #'begin-publication)))))))

(defun physical-backup-lease-startup-action (record)
  ;; A closing handoff keeps any bootstrap pin until its successor is usable.
  ;; Unavailable successors leave cleanup pending.  Normal closing still uses
  ;; the strict successor and WAL checks before removing bootstrap retention.
  (let* ((request (getf record :request))
         (name (getf request :name)))
    (physical-backup-lease-current-record request)
    (case (getf record :phase)
      (:ready (physical-backup-lease-ready-action record))
      (:closing
       (if (eq :aborted (getf record :outcome))
           (progn (physical-backup-lease-allocation-abort-cleanup-plan record) :close)
           (multiple-value-bind (presence pin coordinates row)
               (physical-backup-lease-selected-observation-under-lock record)
             (declare (ignore presence coordinates row))
             (let* ((context (physical-backup-lease-startup-source-context))
                    (state (physical-backup-lease-startup-selected-state
                            record pin (recovery-in-progress-p) context)))
               (physical-backup-lease-check-startup-source-context context)
               (physical-backup-lease-closing-bootstrap-pin record)
               (when (eq state :usable)
                 (physical-backup-lease-check-successor-pin record pin)
                 :close)))))
      (:closed
       (when (or (gethash name *physical-source-pins*)
                 (physical-backup-lease-lstat (physical-source-pin-path name)))
         (error "closed backup lease still has a source pin"))
       (when (eq :aborted (getf record :outcome))
         (when (physical-backup-lease-allocation-abort-cleanup-plan record) :close)))
      (:interrupted
       (physical-backup-lease-pin-directory-stat)
       (multiple-value-bind (pin disk) (physical-source-retirement-pins name)
         (declare (ignore disk))
         (when pin (physical-backup-lease-check-pin-record pin record)))
       nil)
      (t (error "backup lease startup found an active capture")))))

;; Preflight every startup action and cleanup plan before publishing any of
;; them, then recheck each cleanup at its mutation boundary.  Under write and
;; WAL exclusion, fence durable changes and their memory publication.  Allocation
;; revocation precedes pin unlink and directory sync, then retention release.
(defun physical-backup-lease-reconcile-startup ()
  (with-write-lock
    (with-wal-publication-fence (begin-publication)
      (physical-backup-lease-check-successor-inventory)
      (let ((actions nil)
            (records (sort (loop for record being the hash-values of *physical-backup-lease-records*
                                 collect (physical-backup-lease-copy record))
                           #'string< :key (lambda (record) (getf (getf record :request) :name)))))
        (dolist (record records)
          (let ((action (physical-backup-lease-startup-action record)))
            (unless (eq :aborted (getf record :outcome))
              (when (physical-backup-lease-allocation-canceled-cleanup-plan record)
                (push (list :cleanup record) actions)))
            (when action (push (list action record) actions))))
        (dolist (item (nreverse actions))
          (destructuring-bind (action record) item
            (ecase action
              (:cleanup
               (physical-backup-lease-finish-canceled-allocations-under-lock record #'begin-publication))
              (:interrupt
               (let ((next (copy-list record)))
                 (setf (getf next :phase) :interrupted)
                 (physical-backup-lease-publish-checked-under-lock
                  record (physical-backup-lease-transition record next) #'begin-publication)))
              (:close
               (physical-backup-lease-finish-closing-under-lock
                (getf record :request) #'begin-publication))))))))
  (values))
