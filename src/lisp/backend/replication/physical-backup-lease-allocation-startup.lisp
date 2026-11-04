(in-package #:pg)

(defun physical-backup-lease-startup-pin-class (name)
  (let ((parent (physical-backup-lease-record-under-lock name))
        (row (physical-backup-lease-allocation-record-under-lock name)))
    (cond
      (parent (values :bootstrap parent parent))
      (row
       (let ((owner (physical-backup-lease-record-under-lock (getf (getf row :request) :name))))
         (cond
           ((or (eq :canceled (getf row :state)) (eq :aborted (getf owner :outcome)))
            (values :cleanup row owner))
           ((and (eq :handed-off (getf owner :outcome))
                 (equal name (getf (getf owner :successor) :name)))
            (values :selected row owner))
           (t (values :preparation row owner)))))
      (t (values :ordinary nil nil)))))

(defun physical-backup-lease-startup-pin-candidate (pin standby)
  (multiple-value-bind (kind owner parent)
      (physical-backup-lease-startup-pin-class (physical-source-pin-name pin))
    (declare (ignore owner))
    (case kind
      ((:bootstrap :preparation)
       (cond
         ((physical-source-pin-lost-p pin) (values pin nil t))
         ((or standby (/= (physical-source-pin-timeline pin) *wal-insert-timeline*)
              (and (eq kind :bootstrap) (eq :closing (getf parent :phase))))
          (let ((lost (copy-structure pin)))
            (setf (physical-source-pin-lost-p lost) t)
            (values lost t t)))
         (t (physical-source-pin-check-wal pin) (values pin nil t))))
      (:cleanup (values pin nil t))
      (:selected
       (unless (or standby (eq :closing (getf parent :phase)))
         (physical-source-pin-check-wal pin))
       (values pin nil (not standby)))
      (:ordinary
       (unless standby (physical-source-pin-check-wal pin))
       (values pin nil (not standby)))
      (t (error "unknown backup lease startup pin class")))))

(defun physical-backup-lease-check-startup-selected-sources (pins)
  (maphash
   (lambda (name record)
     (declare (ignore name))
     (when (and (eq :closing (getf record :phase)) (eq :handed-off (getf record :outcome)))
       (physical-follow-pin-check-wal (gethash (getf (getf record :successor) :name) pins))))
   *physical-backup-lease-records*)
  (values))

(defun physical-backup-lease-startup-managed-pin-p (name)
  (or (gethash name *physical-backup-lease-records*)
      (gethash name *physical-backup-lease-allocations*)))

(defun physical-backup-lease-check-standby-registered-pins ()
  (maphash
   (lambda (name record)
     (let ((old (gethash name *physical-source-pins*)))
       (when old
         (when (eq :closed (getf record :phase))
           (error "closed backup lease still has a registered source pin"))
         (physical-backup-lease-check-pin-record old record))))
   *physical-backup-lease-records*)
  (values))

(defun physical-backup-lease-startup-pin-plan (raw standby)
  (let ((candidates (make-hash-table :test #'equal))
        (invalidated nil))
    (when standby
      (maphash (lambda (name pin)
                 (unless (physical-backup-lease-startup-managed-pin-p name)
                   (setf (gethash name candidates) pin)))
               *physical-source-pins*))
    (maphash
     (lambda (name pin)
       (multiple-value-bind (candidate invalidate install)
           (physical-backup-lease-startup-pin-candidate pin standby)
         (when invalidate (push candidate invalidated))
         (when install (setf (gethash name candidates) candidate))))
     raw)
    (values candidates invalidated)))

(defun physical-backup-lease-install-startup-pins (candidates standby)
  (maphash
   (lambda (name pin)
     (when (or (not standby) (physical-backup-lease-startup-managed-pin-p name))
       (remhash pin *wal-snapshot-retention-pins*)))
   *physical-source-pins*)
  (unless standby
    (maphash (lambda (name pin)
               (declare (ignore name))
               (unless (physical-source-pin-lost-p pin)
                 (setf (gethash pin *wal-snapshot-retention-pins*)
                       (physical-source-pin-restart-lsn pin))))
             candidates))
  (setf *physical-source-pins* candidates)
  (values))

(defun physical-backup-lease-load-startup-pins (&optional standby)
  (when (and standby (not (recovery-in-progress-p)))
    (error "backup lease standby pin load requires recovery"))
  (with-write-lock
    (with-wal-publication-fence (begin-publication)
      (let ((raw (physical-backup-lease-pin-inventory)))
        (physical-backup-lease-allocation-check-pin-inventory
         *physical-backup-lease-records* *physical-backup-lease-allocations* raw)
        (when standby (physical-backup-lease-check-standby-registered-pins))
        (physical-backup-lease-check-startup-selected-sources raw)
        (multiple-value-bind (candidates invalidated)
            (physical-backup-lease-startup-pin-plan raw standby)
          (when (or (not standby) (plusp (hash-table-count *physical-backup-lease-records*))
                    (plusp (hash-table-count *physical-backup-lease-allocations*)))
            (begin-publication)
            (dolist (pin invalidated) (physical-source-pin-save pin))
            (physical-backup-lease-install-startup-pins candidates standby))))))
  (values))
