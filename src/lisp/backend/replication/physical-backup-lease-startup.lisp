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

;; A standby cannot retain source WAL for these leases.  Validate all pins
;; first, then durably mark newly lost pins before installing the candidates.
;; The publication fence covers disk and memory changes, including failures.
(defun physical-backup-lease-load-standby-pins ()
  (unless (recovery-in-progress-p)
    (error "backup lease standby pin load requires recovery"))
  (with-write-lock
    (with-wal-publication-fence (begin-publication)
      (physical-backup-lease-pin-directory-stat)
      (let ((candidates nil) (invalidated nil))
        (maphash
         (lambda (name record)
           (let ((path (physical-source-pin-path name))
                 (old (gethash name *physical-source-pins*)))
             (when old
               (when (eq :closed (getf record :phase))
                 (error "closed backup lease still has a registered source pin"))
               (physical-backup-lease-check-pin-record old record))
             (if (physical-backup-lease-lstat path)
                 (multiple-value-bind (pin invalidate) (physical-backup-lease-startup-pin path t)
                   (unless pin (error "backup lease lost its indexed pin owner"))
                   (when invalidate (push pin invalidated))
                   (push pin candidates))
                 (when (eq :ready (getf record :phase))
                   (error "ready backup lease has no source pin")))))
         *physical-backup-lease-records*)
        (when invalidated
          (begin-publication)
          (dolist (pin invalidated) (physical-source-pin-save pin)))
        (when (plusp (hash-table-count *physical-backup-lease-records*))
          (begin-publication))
        (maphash (lambda (name record)
                   (declare (ignore record))
                   (let ((old (gethash name *physical-source-pins*)))
                     (when old (remhash old *wal-snapshot-retention-pins*)))
                   (remhash name *physical-source-pins*))
                 *physical-backup-lease-records*)
        (dolist (pin candidates)
          (setf (gethash (physical-source-pin-name pin) *physical-source-pins*) pin)))))
  (values))

(defun physical-backup-lease-closing-pin (record)
  (let* ((request (getf record :request))
         (current (physical-backup-lease-current-record request)))
    (unless (and (eq :closing (getf current :phase)) (equal current record))
      (wal-invalid-input "backup lease has no matching closing intent"))
    (physical-backup-lease-pin-directory-stat)
    (multiple-value-bind (pin disk) (physical-source-retirement-pins (getf request :name))
      (declare (ignore disk))
      (when pin (physical-backup-lease-check-pin-unlink pin request))
      pin)))

;; Complete an existing closing intent without requiring the old WAL.  Remove
;; and sync the pin first, then publish CLOSED.  A restart can repeat this after
;; a failure at either step.  The caller holds both locks and supplies the
;; publication fence callback.
(defun physical-backup-lease-finish-closing-under-lock (request begin-publication)
  (let* ((record (physical-backup-lease-current-record request))
         (name (getf request :name)))
    (when (gethash name *physical-backup-lease-capture-owners*)
      (wal-invalid-input "backup lease still has an active capture owner"))
    (when (eq :closed (getf record :phase))
      (when (or (gethash name *physical-source-pins*)
                (physical-backup-lease-lstat (physical-source-pin-path name)))
        (error "closed backup lease still has a source pin"))
      (return-from physical-backup-lease-finish-closing-under-lock
        (physical-backup-lease-copy record)))
    (let ((pin (physical-backup-lease-closing-pin record)))
      (funcall begin-publication)
      (if pin
          (physical-source-pin-unlink pin request)
          (if (physical-backup-lease-pin-directory-stat)
              (fsync-directory (physical-source-pin-directory))
              (fsync-directory *pgdata*)))
      (let ((next (copy-list record)))
        (setf (getf next :phase) :closed)
        (physical-backup-lease-publish-checked-under-lock
         record (physical-backup-lease-transition record next) begin-publication)))))

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

(defun physical-backup-lease-startup-action (record)
  (let* ((request (getf record :request))
         (name (getf request :name)))
    (physical-backup-lease-current-record request)
    (case (getf record :phase)
      (:ready (physical-backup-lease-ready-action record))
      (:closing (physical-backup-lease-closing-pin record) :close)
      (:closed
       (when (or (gethash name *physical-source-pins*)
                 (physical-backup-lease-lstat (physical-source-pin-path name)))
         (error "closed backup lease still has a source pin")))
      (:interrupted
       (physical-backup-lease-pin-directory-stat)
       (multiple-value-bind (pin disk) (physical-source-retirement-pins name)
         (declare (ignore disk))
         (when pin (physical-backup-lease-check-pin-record pin record)))
       nil)
      (t (error "backup lease startup found an active capture")))))

;; Validate every action before publishing any of them.  Hold write and WAL
;; exclusion throughout, and fence each durable transition with its memory
;; publication so a failed publication cannot admit unsafe work.
(defun physical-backup-lease-reconcile-startup ()
  (with-write-lock
    (with-wal-publication-fence (begin-publication)
      (let ((actions nil)
            (records (sort (loop for record being the hash-values of *physical-backup-lease-records*
                                 collect (physical-backup-lease-copy record))
                           #'string< :key (lambda (record) (getf (getf record :request) :name)))))
        (dolist (record records)
          (let ((action (physical-backup-lease-startup-action record)))
            (when action (push (list action record) actions))))
        (dolist (item (nreverse actions))
          (destructuring-bind (action record) item
            (ecase action
              (:interrupt
               (let ((next (copy-list record)))
                 (setf (getf next :phase) :interrupted)
                 (physical-backup-lease-publish-checked-under-lock
                  record (physical-backup-lease-transition record next) #'begin-publication)))
              (:close
               (physical-backup-lease-finish-closing-under-lock
                (getf record :request) #'begin-publication))))))))
  (values))
