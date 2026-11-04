(in-package #:pg)

;;; Validate the durable parent, allocation and raw pin graph before record
;;; loading interrupts captures or removes temporary files.  Source WAL and
;;; role policy are applied later when startup loads the pins.

(defun physical-backup-lease-check-parent-pin-inventory (record pins)
  (let* ((name (getf (getf record :request) :name))
         (pin (gethash name pins)))
    (when pin
      (when (eq :closed (getf record :phase))
        (error "closed backup lease still has a source pin"))
      (physical-backup-lease-check-pin-record pin record))
    (when (and (eq :ready (getf record :phase)) (null pin))
      (error "ready backup lease has no source pin"))
    (when (eq :handed-off (getf record :outcome))
      (let ((selected (gethash (getf (getf record :successor) :name) pins)))
        (if (eq :closing (getf record :phase))
            (physical-backup-lease-check-successor-pin record selected)
            (when selected
              (unless (physical-backup-lease-successor-pin-identity-p record selected)
                (error "closed backup lease successor differs from its immutable selection"))
              (physical-follow-pin-check-fields selected)))))))

(defun physical-backup-lease-allocation-check-pin-inventory-row (row parent pins)
  (let ((pin (gethash (getf row :name) pins)))
    (when pin
      (unless (and (physical-follow-pin-p pin)
                   (physical-follow-request-matches-anchor-p
                    (getf row :coordinates) (physical-follow-pin-anchor pin)))
        (error "backup lease allocated pin differs from its immutable ownership"))
      (physical-follow-pin-check-fields pin)
      (unless (or (eq :canceled (getf row :state))
                  (eq :aborted (getf parent :outcome))
                  (physical-backup-lease-allocation-selected-pin-p row parent pin t))
        (when (and (physical-follow-pin-ready-p pin)
                   (/= (physical-source-pin-confirmed-lsn pin)
                       (getf (getf row :coordinates) :receive-lsn)))
          (error "unselected live backup lease allocation has advanced progress"))))))

;; Check ownership across the complete parent, allocation and raw pin inventory.
;; A closing handoff requires its unused successor; a closed handoff may have
;; advanced or retired it.  Unselected live preparations cannot have advanced.

(defun physical-backup-lease-allocation-check-pin-inventory (parents allocations pins)
  (let ((histories (physical-backup-lease-allocation-check-graph parents allocations)))
    (maphash
     (lambda (name pin)
       (unless (and (physical-source-pin-p pin) (equal name (physical-source-pin-name pin)))
         (error "backup lease pin inventory key differs from its owner")))
     pins)
    (maphash (lambda (name record)
               (declare (ignore name))
               (physical-backup-lease-check-parent-pin-inventory record pins))
             parents)
    (maphash
     (lambda (name row)
       (declare (ignore name))
       (physical-backup-lease-allocation-check-pin-inventory-row
        row (gethash (getf (getf row :request) :name) parents) pins))
     allocations)
    histories))

(defun physical-backup-lease-pin-inventory ()
  (physical-backup-lease-require-locks)
  (physical-backup-lease-pin-directory-stat)
  (let ((pins (make-hash-table :test #'equal)))
    (dolist (path (durable-state-file-paths (physical-source-pin-directory) "pin"))
      (let* ((pin (physical-source-pin-read-raw path :stabilize t))
             (name (physical-source-pin-name pin)))
        (when (gethash name pins) (error "duplicate physical source pin in startup inventory"))
        (setf (gethash name pins) pin)))
    pins))

;; Under write and WAL exclusion, validate both the original graph and the
;; graph with interrupted parents before rewriting records or removing temporary
;; files.  WAL availability and role policy are checked during the later pin
;; load, before pin loss is written or the pin registry is replaced.

(defun physical-backup-lease-load-records-with-pin-preflight ()
  (with-write-lock
    (with-wal-publication-fence (begin-publication)
      (unless (and (zerop (hash-table-count *physical-source-senders*))
                   (zerop (hash-table-count *physical-backup-lease-capture-owners*)))
        (wal-invalid-input "cannot reload backup lease records with active source owners"))
      (multiple-value-bind (paths temporaries) (physical-backup-lease-inventory)
        (let ((candidates (make-hash-table :test #'equal))
              (interrupted nil))
          (dolist (path paths)
            (let* ((record (physical-backup-lease-read path :stabilize t))
                   (request (getf record :request))
                   (name (getf request :name)))
              (physical-backup-lease-check-source request)
              (when (gethash name candidates) (error "duplicate backup lease request"))
              (setf (gethash name candidates) record)))
          ;; Check the original parent and allocation graph before constructing
          ;; interrupted parents. Check again before cleanup or record rewrites.
          (multiple-value-bind (allocations allocation-temporaries)
              (physical-backup-lease-allocation-load-candidates candidates)
            (let ((pin-candidates (physical-backup-lease-pin-inventory)))
              (physical-backup-lease-allocation-check-pin-inventory candidates allocations pin-candidates)
              (maphash
               (lambda (name record)
                 (when (member (getf record :phase) '(:requested :capturing))
                   (let ((next (copy-list record)))
                     (setf (getf next :phase) :interrupted
                           next (physical-backup-lease-transition record next))
                     (push next interrupted)
                     (setf (gethash name candidates) next))))
               candidates)
              (physical-backup-lease-allocation-check-pin-inventory candidates allocations pin-candidates)
              (begin-publication)
              (when (or interrupted temporaries)
                (dolist (path temporaries) (sb-posix:unlink (namestring path)))
                (dolist (record interrupted) (physical-backup-lease-write record))
                (fsync-directory (physical-backup-lease-directory)))
              (when allocation-temporaries
                (dolist (path allocation-temporaries) (sb-posix:unlink (namestring path)))
                (fsync-directory (physical-backup-lease-allocation-directory)))
              (physical-backup-lease-allocation-install-records allocations)
              (setf *physical-backup-lease-successor-cache* nil
                    *physical-backup-lease-records* candidates)))))))
  (values))
