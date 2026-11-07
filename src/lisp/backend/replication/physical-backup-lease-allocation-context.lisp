(in-package #:pg)

(defun print-physical-backup-lease-allocation-context (context stream depth)
  (declare (ignore depth))
  (print-unreadable-object (context stream :type t :identity t)))

(defstruct (physical-backup-lease-allocation-context
            (:constructor %make-physical-backup-lease-allocation-context
                (operation record thread backend incarnation))
            (:print-function print-physical-backup-lease-allocation-context))
  (operation nil :read-only t) (record nil :read-only t)
  (thread nil :read-only t) (backend nil :read-only t) (incarnation nil :read-only t)
  (active t))

(defvar *physical-backup-lease-allocation-context* nil)

;; The dynamic context authorizes only this operation on this saved record.
;; Bind it to the thread, backend and storage incarnation, and revoke it on
;; every exit so an escaped context cannot authorize a later operation.

(defun call-with-physical-backup-lease-allocation-context (operation record function)
  (unless (member operation '(:prepare :retire))
    (wal-invalid-input "invalid backup lease allocation operation"))
  (let ((context (%make-physical-backup-lease-allocation-context
                  operation (physical-backup-lease-allocation-copy record)
                  sb-thread:*current-thread* *backend* *wal-storage-incarnation*)))
    (unwind-protect
         (let ((*physical-backup-lease-allocation-context* context))
           (funcall function))
      (sb-sys:without-interrupts
        (setf (physical-backup-lease-allocation-context-active context) nil)))))

(defun physical-backup-lease-allocation-record-under-lock (name)
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (error "backup lease allocation lookup requires WAL exclusion"))
  (gethash name *physical-backup-lease-allocations*))

(defun physical-backup-lease-allocation-check-unowned-name (name)
  (when (physical-backup-lease-allocation-record-under-lock name)
    (wal-invalid-input "backup lease allocation permanently reserves this name")))

(defun physical-backup-lease-allocation-current-context (operation name)
  (let* ((context *physical-backup-lease-allocation-context*)
         (row (physical-backup-lease-allocation-record-under-lock name)))
    (unless (and context row
                 (physical-backup-lease-allocation-context-active context)
                 (eq operation (physical-backup-lease-allocation-context-operation context))
                 (eq sb-thread:*current-thread* (physical-backup-lease-allocation-context-thread context))
                 (eq *backend* (physical-backup-lease-allocation-context-backend context))
                 (eq *wal-storage-incarnation* (physical-backup-lease-allocation-context-incarnation context))
                 (equal row (physical-backup-lease-allocation-context-record context)))
      (wal-invalid-input "backup lease allocation lacks its current operation owner"))
    (handler-case
        (let* ((request (getf row :request))
               (parent (physical-backup-lease-record-under-lock (getf request :name))))
          (unless (and parent (equal request (getf parent :request)))
            (error "backup lease allocation parent ownership changed"))
          (physical-backup-lease-check-source request)
          (unless (equal parent (physical-backup-lease-read (physical-backup-lease-path request)))
            (error "backup lease allocation parent changed on disk"))
          (physical-backup-lease-allocation-check-disk row)
          (values row parent))
      (wal-input-validation-error (condition) (error condition))
      (error (condition) (wal-fail-stop condition)))))

;; A live allocation is selected only by the handed-off parent and matching
;; successor.  PIN may be absent after retirement; when present its ready anchor
;; must match the saved coordinates and the parent's digest.

(defun physical-backup-lease-allocation-selected-pin-p (row parent pin &optional closing)
  (and (eq :live (getf row :state))
       (equal (getf row :request) (getf parent :request))
       (member (getf parent :phase) (if closing '(:closing :closed) '(:closed)))
       (eq :handed-off (getf parent :outcome))
       (equal (getf row :name) (getf (getf parent :successor) :name))
       (or (null pin)
           (and (physical-backup-lease-successor-pin-identity-p parent pin)
                (physical-follow-request-matches-anchor-p
                 (getf row :coordinates) (physical-follow-pin-anchor pin))))))

(defun physical-backup-lease-selected-observation-under-lock (record)
  ;; With write and WAL exclusion, check the exact durable parent, successor
  ;; index, local allocation history, and registered and durable pin pair.
  ;; Ownership remains checkable after old WAL is unavailable. Cleanup requires
  ;; separate operation authority.
  ;; Return :PRESENT or :ABSENT, the pin for use under these locks, detached
  ;; coordinates, and a detached allocation row. Allocated absence retains saved
  ;; coordinates. Legacy absence returns NIL coordinates and a NIL row.
  (physical-backup-lease-require-locks)
  (let* ((checked (physical-backup-lease-copy record))
         (request (getf checked :request))
         (current (physical-backup-lease-current-record request))
         (name (getf (getf current :successor) :name)))
    (unless (and (equal checked current)
                 (member (getf current :phase) '(:closing :closed))
                 (eq :handed-off (getf current :outcome))
                 (eq current (gethash name (physical-backup-lease-successor-records))))
      (wal-invalid-input "backup lease selection has no exact durable owner"))
    (let* ((history (gethash (getf request :name) (physical-backup-lease-allocation-histories)))
           (row (gethash name *physical-backup-lease-allocations*)))
      (when history
        (physical-backup-lease-allocation-check-history current history))
      (unless (equal row (car (last history)))
        (wal-invalid-input "backup lease selection differs from its allocation history"))
      (dolist (entry history)
        (physical-backup-lease-allocation-check-parent entry *physical-backup-lease-records*)
        (unless (equal entry (gethash (getf entry :name) *physical-backup-lease-allocations*))
          (error "backup lease allocation history differs from its registry"))
        (physical-backup-lease-allocation-check-disk entry))
      (multiple-value-bind (pin disk) (physical-source-retirement-pins name)
        (declare (ignore disk))
        (when pin
          (physical-backup-lease-check-selected-pin-fields current pin)
          (when (and row
                     (not (physical-backup-lease-allocation-selected-pin-p row current pin t)))
            (wal-invalid-input "backup lease selection differs from its allocated coordinates")))
        (values (if pin :present :absent)
                pin
                (cond (row (physical-follow-request-copy (getf row :coordinates)))
                      (pin (physical-follow-anchor-request (physical-follow-pin-anchor pin))))
                (when row (physical-backup-lease-allocation-copy row)))))))

(defun physical-backup-lease-allocation-check-prepare (name request &optional anchor-p observation-p)
  (when (physical-backup-lease-allocation-record-under-lock name)
    (multiple-value-bind (row parent) (physical-backup-lease-allocation-current-context :prepare name)
      (unless (and (eq :live (getf row :state))
                   (equal (getf row :coordinates)
                          (if anchor-p (physical-follow-anchor-request request)
                              (physical-follow-request-copy request))))
        (wal-invalid-input "backup lease allocation preparation differs from its saved coordinates"))
      (let* ((pin (gethash name *physical-source-pins*))
             (observation (and observation-p pin
                               (physical-backup-lease-allocation-selected-pin-p row parent pin t))))
        (unless (or observation
                    (and (member (getf parent :phase) '(:ready :interrupted)) (getf parent :base)
                         (not (recovery-in-progress-p))
                         (= (getf (getf row :coordinates) :source-timeline) *wal-insert-timeline*)
                         (null (gethash (getf (getf parent :request) :name) *physical-source-senders*))
                         (null (gethash (getf (getf parent :request) :name)
                                        *physical-backup-lease-capture-owners*))))
          (wal-invalid-input "backup lease allocation parent no longer permits preparation"))
        (values row observation)))))

(defun physical-backup-lease-allocation-check-stream (name)
  (let ((row (physical-backup-lease-allocation-record-under-lock name)))
    (when row
      (let ((parent (physical-backup-lease-record-under-lock (getf (getf row :request) :name))))
        (unless (physical-backup-lease-allocation-selected-pin-p
                 row parent (gethash name *physical-source-pins*))
          (wal-invalid-input "backup lease allocation has no completed handoff owner"))))))

(defun physical-backup-lease-allocation-check-retirement-pin (row pin)
  (physical-backup-lease-require-locks)
  (let* ((name (getf row :name))
         (registered (gethash name *physical-source-pins*)))
    (unless (and (physical-follow-pin-p pin)
                 (physical-follow-request-matches-anchor-p
                  (getf row :coordinates) (physical-follow-pin-anchor pin))
                 (or (eq registered pin) (and (null registered) (recovery-in-progress-p))))
      (wal-invalid-input "backup lease allocation retirement pin generation changed"))
    (handler-case
        (multiple-value-bind (verified disk) (physical-source-retirement-pins name)
          (unless (and verified disk
                       (equalp (physical-source-pin-encode pin) (physical-source-pin-encode disk)))
            (error "backup lease allocation retirement pin changed on disk")))
      (wal-input-validation-error ()
        (error "backup lease allocation retirement registry and disk disagree"))))
  pin)

;; Cancellation may retire its own unselected pin after durable revocation.
;; Without that operation owner, retirement requires a completed handoff.

(defun physical-backup-lease-allocation-check-retirement (name &optional pin)
  (let ((row (physical-backup-lease-allocation-record-under-lock name)))
    (when row
      (if *physical-backup-lease-allocation-context*
          (multiple-value-bind (owned parent) (physical-backup-lease-allocation-current-context :retire name)
            (unless (and (eq :canceled (getf owned :state))
                         (null (gethash name *physical-source-senders*))
                         (not (equal name (getf (getf parent :successor) :name)))
                         (or (null pin)
                             (and (physical-follow-pin-p pin)
                                  (physical-follow-request-matches-anchor-p
                                   (getf owned :coordinates) (physical-follow-pin-anchor pin)))))
              (wal-invalid-input "backup lease allocation cancellation cannot retire this pin")))
          (physical-backup-lease-allocation-check-stream name))
      (when pin (physical-backup-lease-allocation-check-retirement-pin row pin)))))

(defun physical-backup-lease-allocation-check-handoff (after)
  (when (eq :handed-off (getf after :outcome))
    (let* ((name (getf (getf after :successor) :name))
           (row (physical-backup-lease-allocation-record-under-lock name))
           (history (gethash (getf (getf after :request) :name)
                             (physical-backup-lease-allocation-histories))))
      (when (or row history)
        (handler-case (physical-backup-lease-allocation-check-history after history)
          (error () (wal-invalid-input "backup lease handoff lacks its live allocation")))
        (unless (and row
                     (physical-backup-lease-allocation-selected-pin-p
                      row after (gethash name *physical-source-pins*) t))
          (wal-invalid-input "backup lease handoff differs from its allocated preparation"))
        (dolist (record history) (physical-backup-lease-allocation-check-disk record)))))
  after)
