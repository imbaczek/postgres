(in-package #:pg)

;;; Plan and finish allocation cleanup owed by canceled intents and aborted
;;; leases.  Durable identities and owners are checked before any pin is retired.

;; Preflight the whole history under write and WAL exclusion, including durable
;; identity and sender checks, before retiring any allocation.  Canceled rows
;; can still have pins: revocation alone does not prove unlink completed.

(defun physical-backup-lease-allocation-cleanup-history (parent)
  (physical-backup-lease-require-locks)
  (let* ((request (getf parent :request))
         (name (getf request :name))
         (history (gethash name (physical-backup-lease-allocation-histories))))
    (unless (equal parent (physical-backup-lease-current-record request))
      (wal-invalid-input "backup lease allocation cleanup parent changed"))
    (when (gethash name *physical-backup-lease-capture-owners*)
      (wal-invalid-input "backup lease still has an active capture owner"))
    (when history (physical-backup-lease-allocation-check-history parent history))
    (dolist (row history)
      (physical-backup-lease-allocation-check-parent row *physical-backup-lease-records*)
      (unless (equal row (gethash (getf row :name) *physical-backup-lease-allocations*))
        (error "backup lease allocation history differs from its registered owner"))
      (physical-backup-lease-allocation-check-disk row)
      (when (gethash (getf row :name) *physical-source-senders*)
        (error "backup lease cleanup allocation still has an active sender"))
      (physical-backup-lease-allocation-cleanup-pin row))
    history))

;; Retire a preflighted history under write and WAL exclusion and the caller's
;; publication fence.  Each revocation rechecks its boundary; this helper does
;; not settle the parent lease.

(defun physical-backup-lease-revoke-allocations-under-lock (history begin-publication)
  (dolist (row history)
    (let ((canceled (physical-backup-lease-allocation-copy row)))
      (setf (getf canceled :state) :canceled)
      (when (eq :busy (physical-backup-lease-allocation-revoke-under-lock
                       row canceled begin-publication))
        (error "backup lease cleanup allocation acquired a sender"))))
  (values))

(defun physical-backup-lease-allocation-canceled-cleanup-plan (parent)
  (remove-if-not (lambda (row) (eq :canceled (getf row :state)))
                 (physical-backup-lease-allocation-cleanup-history parent)))

(defun physical-backup-lease-finish-canceled-allocations-under-lock (parent begin-publication)
  (physical-backup-lease-revoke-allocations-under-lock
   (physical-backup-lease-allocation-canceled-cleanup-plan parent) begin-publication))

;; An aborted closing or closed parent owes cleanup of its allocations.  Check
;; the entire history and bootstrap pin before starting; cleanup uses exact pin
;; identity and does not need the old source WAL.

(defun physical-backup-lease-allocation-abort-cleanup-plan (parent)
  (physical-backup-lease-require-locks)
  (unless (and (eq :aborted (getf parent :outcome))
               (member (getf parent :phase) '(:closing :closed)))
    (wal-invalid-input "backup lease allocation cleanup lacks its aborted parent"))
  (let* ((history (physical-backup-lease-allocation-cleanup-history parent))
         (name (getf (getf parent :request) :name))
         (bootstrap
           (if (eq :closed (getf parent :phase))
               (progn
                 (when (or (gethash name *physical-source-pins*)
                           (physical-backup-lease-lstat (physical-source-pin-path name)))
                   (error "closed backup lease still has a source pin"))
                 nil)
               (physical-backup-lease-closing-pin parent))))
    (values history bootstrap)))

(defun physical-backup-lease-finish-aborted-allocations-under-lock (parent begin-publication)
  (multiple-value-bind (history bootstrap)
      (physical-backup-lease-allocation-abort-cleanup-plan parent)
    (physical-backup-lease-revoke-allocations-under-lock history begin-publication)
    bootstrap))
