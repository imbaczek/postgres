(in-package #:pg)

(defun logical-slot-publication-pending-image (pending)
  (unless (and (hash-table-p pending) (eq (hash-table-test pending) 'eql))
    (error "startup publication requires an EQL pending-XID table"))
  (sort (loop for xid being the hash-keys of pending using (hash-value positions)
              collect
              (progn
                (unless (and (typep xid '(integer 0 *)) (consp positions)
                             (typep (car positions) '(integer 0 *))
                             (or (null (cdr positions)) (typep (cdr positions) '(integer 0 *))))
                  (error "startup publication has malformed pending-XID positions"))
                (list xid (car positions) (cdr positions))))
        #'< :key #'first))

(defun logical-slot-publication-slot-transients (slot)
  (list (wal-replication-slot-active-owner slot)
        (wal-replication-slot-active-owner-backend slot)
        (wal-replication-slot-drop-claim slot)
        (wal-replication-slot-creation-failed-p slot)
        (wal-replication-slot-state-uncertain-p slot)
        (wal-replication-slot-temporary-owner slot)
        (wal-replication-slot-inactive-since slot)))

(defun logical-slot-publication-registry-image (registry)
  (unless (and (hash-table-p registry) (eq (hash-table-test registry) 'equal))
    (error "startup publication requires its EQUAL slot registry"))
  (sort
   (loop for name being the hash-keys of registry using (hash-value slot)
         collect
         (progn
           (unless (and (stringp name) (wal-replication-slot-p slot)
                        (equal name (wal-replication-slot-name slot)))
             (error "startup publication registry changed its slot binding"))
           (list (copy-seq name) slot (logical-slot-startup-candidate-image slot)
                 (wal-slot-profile slot) (wal-replication-slot-logical-provenance slot)
                 (wal-replication-slot-pending-xids slot)
                 (logical-slot-publication-pending-image (wal-replication-slot-pending-xids slot))
                 (logical-slot-publication-slot-transients slot))))
   #'string< :key #'first))

(defun logical-slot-publication-check-registry (registry image)
  (unless (and (hash-table-p registry) (= (hash-table-count registry) (length image))
               (every
                (lambda (row)
                  (destructuring-bind (name slot bytes profile provenance pending positions transients) row
                    (and (eq slot (gethash name registry))
                         (equal name (wal-replication-slot-name slot))
                         (equalp bytes (logical-slot-startup-candidate-image slot))
                         (eq profile (wal-slot-profile slot))
                         (eq provenance (wal-replication-slot-logical-provenance slot))
                         (eq pending (wal-replication-slot-pending-xids slot))
                         (equal positions (logical-slot-publication-pending-image pending))
                         (equal transients (logical-slot-publication-slot-transients slot)))))
                image))
    (error "startup publication registry or derived pending state changed"))
  t)

(defun logical-slot-publication-mirrors ()
  (list *wal-replication-slots* *logical-capture-live-slots*
        *wal-logical-policy* *wal-logical-info-disable-pending*))

(defun logical-slot-publication-fence (registry)
  (flet ((fence ()
           (maphash (lambda (name slot)
                      (declare (ignore name))
                      (setf (wal-replication-slot-state-uncertain-p slot) t))
                    registry)))
    (if (sb-thread:holding-mutex-p *wal-lock*)
        (fence)
        (sb-thread:with-mutex (*wal-lock*) (fence))))
  (values))
