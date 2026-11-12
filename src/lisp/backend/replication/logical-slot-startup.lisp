(in-package #:pg)

(defun logical-slot-prepare-startup-inventory (candidates segments inactive-since &optional prepare-known)
  (let ((restored (make-hash-table :test #'equal)))
    (let ((*wal-replication-slots* restored))
      (dolist (slot candidates)
        (let ((name (wal-replication-slot-name slot)))
          (when (gethash name restored) (error "duplicate WAL replication slot ~a" name))
          (when (if (wal-physical-slot-p slot)
                    (physical-slot-restore slot inactive-since)
                    (progn
                      (when (recovery-in-progress-p)
                        (error "logical slot restoration during native recovery is unsupported"))
                      (if (logical-slot-startup-known-p slot)
                          (if prepare-known (funcall prepare-known slot)
                              (error "known logical slot startup requires an owned source"))
                          (wal-validate-replication-slot-wal slot segments))))
            (wal-check-replication-slot-capacity)
            (setf (gethash name restored) slot))))
      (when (and (recovery-in-progress-p) (plusp (hash-table-count restored)))
        (physical-slot-check-standby-slots
         (loop for slot being the hash-values of restored collect slot) (physical-slot-startup-system-id)))
      (maphash (lambda (name slot)
                 (declare (ignore name))
                 (unless (logical-slot-startup-known-p slot) (wal-slot-rebuild-progress slot)))
               restored))
    restored))

(defun logical-slot-startup-check-inventory (source)
  (logical-slot-startup-check-authority source)
  (let ((expected
          (loop for entry in (logical-slot-startup-source-entries source)
                for slot = (logical-slot-startup-entry-slot entry)
                unless (and (wal-physical-slot-p slot) (wal-replication-slot-temporary-p slot))
                  collect (cons (wal-replication-slot-name slot) (logical-slot-startup-entry-bytes entry))))
        (actual (slot-state-map (wal-replication-slot-directory)
                                (lambda (bytes name) (cons name bytes)))))
    (unless (equalp (sort expected #'string< :key #'car) (sort actual #'string< :key #'car))
      (error "logical slot startup disk inventory changed")))
  (logical-slot-startup-check-authority source)
  t)

(defun logical-slot-startup-prepare-mirrors (restored)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((*wal-replication-slots* restored)
        (*logical-capture-live-slots* *logical-capture-live-slots*)
        (*wal-logical-policy* *wal-logical-policy*)
        (*wal-logical-info-disable-pending* *wal-logical-info-disable-pending*))
    (wal-refresh-logical-capture-slot-mirror-under-wal-lock)
    (values *logical-capture-live-slots* *wal-logical-policy* *wal-logical-info-disable-pending*)))

(defun logical-slot-startup-publish (source restored &optional archive)
  (logical-slot-startup-check-inventory source)
  (sb-thread:with-mutex (*wal-lock*)
    (logical-slot-startup-check-source source)
    (unless (and (eq *wal-replication-slots* (logical-slot-startup-source-registry source))
                 (<= (hash-table-count restored) *max-replication-slots*))
      (error "logical slot startup publication context changed"))
    (multiple-value-bind (count policy disable-pending) (logical-slot-startup-prepare-mirrors restored)
      (logical-slot-startup-check-source source)
      (when archive (logical-wal-archive-check archive))
      (sb-thread:with-mutex (*xid-lock*)
        (sb-sys:without-interrupts
          (setf *wal-replication-slots* restored
                *logical-capture-live-slots* count
                *wal-logical-policy* policy
                *wal-logical-info-disable-pending* disable-pending)))))
  (values))

(defun logical-slot-startup-restore (source segments inactive-since)
  (let ((candidates (mapcar #'logical-slot-startup-entry-slot
                            (logical-slot-startup-source-entries source))))
    (flet ((prepare (archive)
             (let ((restored
                     (logical-slot-prepare-startup-inventory
                      candidates segments inactive-since
                      (lambda (slot)
                        (when (eq :reserved (wal-replication-slot-wal-status slot))
                          (logical-slot-startup-check-eligibility source slot)
                          (let ((pending (logical-slot-startup-derive-pending source slot archive)))
                            (setf (wal-replication-slot-pending-xids slot) pending)))
                        slot))))
               (logical-slot-startup-publish source restored archive))))
      (if (logical-slot-startup-source-history source)
          (call-with-logical-slot-startup-archive source #'prepare)
          (prepare nil)))))

(defun logical-slot-load-startup-inventory (gate-owner ownership)
  (when (or gate-owner ownership)
    (check-primary-storage-startup-context gate-owner ownership))
  (let* ((entries (logical-slot-startup-read-inventory :cleanup (and gate-owner ownership)))
         (candidates (mapcar #'logical-slot-startup-entry-slot entries))
         (segments (wal-discover-segments))
         (inactive-since (postgres-current-timestamp)))
    (cond
      ((or gate-owner ownership (some #'logical-slot-startup-known-p candidates))
       (call-with-logical-slot-startup-source
        gate-owner ownership entries
        (lambda (source) (logical-slot-startup-restore source segments inactive-since))))
      (t
       (logical-slot-check-startup-identities candidates)
       (logical-slot-check-startup-history candidates)
       (let ((restored (logical-slot-prepare-startup-inventory candidates segments inactive-since)))
         (setf *wal-replication-slots* restored)))))
  (values))
