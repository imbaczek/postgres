(in-package #:pg)

(declaim (special *physical-follow-lock*))

;;; Desired source settings and the durable claim to manage native follow.
;;; Snapshots own their setting bytes; opaque tokens fence old asynchronous work.
;;; Live claims are durable before publication. Cold startup establishes the
;;; verified base and claim before receipt is admitted.

(define-condition physical-follow-generation-changed (error) ()
  (:report (lambda (condition stream)
             (declare (ignore condition))
             (write-string "native follow source generation changed" stream))))

(defun physical-follow-print-desired (object stream depth)
  (declare (ignore depth))
  (print-unreadable-object (object stream :type t :identity t)))

(defstruct (physical-follow-desired
            (:constructor %make-physical-follow-desired (conninfo slot fallback policy policy-token claimed-p predecessor))
            (:conc-name %physical-follow-desired-)
            (:print-function physical-follow-print-desired))
  "One private desired generation with owned copies of its settings.
IDENTITY and PREDECESSOR are opaque tokens used to reject stale installs.
The complete conninfo bytes distinguish generations; printing hides credentials."
  (identity (list nil) :read-only t)
  (predecessor nil :read-only t)
  (conninfo #() :read-only t)
  (slot nil :read-only t)
  (fallback "" :read-only t)
  (policy nil :read-only t)
  (policy-token nil :read-only t)
  (claimed-p nil :read-only t))

(defvar *physical-follow-desired* nil)

(defvar *physical-follow-manual-reservation* nil)

(defvar *physical-follow-desired-wake* (sb-thread:make-semaphore :name "follow desired settings"))

(defun physical-follow-desired-conninfo (desired)
  (copy-seq (%physical-follow-desired-conninfo desired)))

(defun physical-follow-desired-receiver-conninfo (desired)
  "Copy the first 1023 bytes for receiver transport, retaining full input in DESIRED."
  (let ((bytes (%physical-follow-desired-conninfo desired)))
    (subseq bytes 0 (min 1023 (length bytes)))))

(defun physical-follow-desired-slot (desired)
  (let ((slot (%physical-follow-desired-slot desired)))
    (when slot (copy-seq slot))))

(defun physical-follow-desired-fallback (desired)
  (copy-seq (%physical-follow-desired-fallback desired)))

(defun physical-follow-copy-desired-policy (policy)
  (let ((copy (copy-tree policy)))
    (when (getf policy :history)
      (setf (getf copy :history) (mapcar #'copy-timeline-history-entry (getf policy :history))))
    copy))

(defun physical-follow-desired-policy (desired)
  (physical-follow-copy-desired-policy (%physical-follow-desired-policy desired)))

(defun physical-follow-desired-claimed-p (desired)
  (and desired (%physical-follow-desired-claimed-p desired)))

(defun physical-follow-configured-slot (slot)
  (unless (and (stringp slot)
               (<= (length slot) 63)
               (every (lambda (char) (find char "abcdefghijklmnopqrstuvwxyz0123456789_")) slot)
               (not (string= slot "pg_conflict_detection")))
    (error "invalid primary_slot_name"))
  (when (plusp (length slot)) (copy-seq slot)))

(defun physical-follow-next-desired (previous conninfo slot fallback policy claimed-p)
  "Return the existing generation if unchanged, or an owned successor snapshot."
  (let* ((bytes (etypecase conninfo
                  (string (string->bytes conninfo))
                  ((vector (unsigned-byte 8)) (copy-seq conninfo))))
         (slot (physical-follow-configured-slot slot))
         (saved-policy (physical-follow-copy-desired-policy policy))
         (claimed (or claimed-p (physical-follow-desired-claimed-p previous))))
    (unless (and (not (find 0 bytes)) (stringp fallback)
                 (not (find (code-char 0) fallback)) (member claimed '(nil t)))
      (error "invalid native follow desired settings"))
    (if (and previous
             (equalp bytes (%physical-follow-desired-conninfo previous))
             (equal slot (%physical-follow-desired-slot previous))
             (equal fallback (%physical-follow-desired-fallback previous))
             (eq policy (%physical-follow-desired-policy-token previous))
             (equalp saved-policy (%physical-follow-desired-policy previous))
             (eq claimed (%physical-follow-desired-claimed-p previous)))
        previous
        (%make-physical-follow-desired bytes slot (copy-seq fallback) saved-policy policy claimed
                                       (when previous (%physical-follow-desired-identity previous))))))

(defun physical-follow-managed-path ()
  (merge-pathnames "physical-follow-managed.control" *pgdata*))

(defun physical-follow-managed-marker-bytes (base)
  (unless (and (typep (getf base :system-id) '(unsigned-byte 64))
               (typep (getf base :manifest-digest) '(unsigned-byte 64)))
    (error "managed follow claim lacks verified base identity"))
  (string->bytes (format nil "PGFOLLOWMANAGED1 ~d ~d~%"
                         (getf base :system-id) (getf base :manifest-digest))))

(defun physical-follow-managed-marker-p (base)
  (let ((path (physical-follow-managed-path)))
    (when (wal-namespace-file-stat path t)
      (unless (equalp (read-bounded-regular-file path 128 :minimum 1)
                      (physical-follow-managed-marker-bytes base))
        (error "managed follow marker differs from verified base"))
      t)))

(defun physical-follow-persist-management (base)
  "Durably claim management for the verified base identity before publication.
A mismatch or write failure fails recovery closed; atomic publication includes
file and directory synchronization."
  (let ((bytes (physical-follow-managed-marker-bytes base)))
    (handler-case
        (progn
          (unless (physical-follow-managed-marker-p base)
            (wal-atomic-write-bytes (physical-follow-managed-path) bytes))
          (unless (physical-follow-managed-marker-p base)
            (error "managed follow marker was not published"))
          t)
      (error (condition)
        (setf *physical-recovery-failed-p* t)
        (error condition)))))

(defun physical-follow-prepare-managed-claim (desired)
  (when (and (physical-follow-desired-claimed-p desired)
             (member (getf *physical-recovery-state* :phase) '(1 2))
             *postmaster-data-lock* (postmaster-pid-owned-p))
    (physical-follow-persist-management *physical-recovery-state*)))

(defun physical-follow-check-desired-predecessor (desired)
  (unless (or (eq desired *physical-follow-desired*)
              (and (eq (%physical-follow-desired-predecessor desired)
                       (when *physical-follow-desired*
                         (%physical-follow-desired-identity *physical-follow-desired*)))
                   (or (not (physical-follow-desired-claimed-p *physical-follow-desired*))
                       (physical-follow-desired-claimed-p desired))))
    (error 'physical-follow-generation-changed)))

(defun physical-follow-call-with-desired-install (desired function &optional preflight)
  "Install settings and their generation together under quiescence.
Check the follow predecessor and optional preflight before and after the durable
claim. Under the follow lock and interrupt mask, FUNCTION publishes settings,
then the desired generation is swapped. Notify after unlock; a notification
error is logged without undoing the installed configuration."
  (check-type desired physical-follow-desired)
  (let ((changed nil))
    (flet ((install ()
             (sb-thread:with-mutex (*physical-follow-lock*)
               (when preflight (funcall preflight))
               (physical-follow-check-desired-predecessor desired))
             (physical-follow-prepare-managed-claim desired)
             (sb-thread:with-mutex (*physical-follow-lock*)
               (when preflight (funcall preflight))
               (physical-follow-check-desired-predecessor desired)
               (sb-sys:without-interrupts
                 (funcall function)
                 (unless (eq desired *physical-follow-desired*)
                   (setf *physical-follow-desired* desired changed t))))
             (when changed
               (handler-case (sb-thread:signal-semaphore *physical-follow-desired-wake*)
                 (error (condition)
                   (ignore-errors
                    (admin-log "configuration installed; follow wake failed: ~a" condition)))))))
      (if (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
          (install)
          (with-live-operation-quiescence (install)))))
  desired)

(defun physical-follow-install-desired (desired)
  (physical-follow-call-with-desired-install desired (constantly nil)))

(defun physical-follow-generation-current-p (generation)
  (or (not (physical-follow-desired-claimed-p *physical-follow-desired*))
      (eq generation *physical-follow-desired*)))

(defun physical-follow-require-generation (generation)
  (unless (physical-follow-generation-current-p generation)
    (error 'physical-follow-generation-changed))
  t)
