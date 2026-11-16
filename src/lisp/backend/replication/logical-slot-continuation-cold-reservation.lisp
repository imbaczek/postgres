(in-package #:pg)

(defun logical-slot-continuation-ensure-cold-reservation (storage &optional failure-cell)
  ;; Install conservative protection before classification or reconstruction.
  ;; A private failure receipt remains usable if later registrations change.
  (when (and failure-cell
             (not (and (consp failure-cell) (null (car failure-cell)) (null (cdr failure-cell)))))
    (error "cold reservation requires an empty private failure receipt"))
  (unless (and (logical-slot-continuation-startup-storage-p storage)
               (not (logical-slot-continuation-runtime-storage-p storage)))
    (error "cold reservation requires actual startup storage"))
  (logical-slot-continuation-storage-check storage)
  (let* ((root (logical-slot-continuation-storage-call storage :root-identity))
         (binding (logical-slot-continuation-storage-call storage :binding))
         (facts (logical-slot-continuation-storage-call storage :facts))
         (host (logical-slot-continuation-storage-call storage :host))
         (origin (logical-slot-continuation-storage-call storage :startup-origin))
         (fork (getf (getf facts :certificate) :fork))
         (child (getf (getf facts :certificate) :child-timeline)))
    (sb-thread:with-mutex (*wal-lock*)
      (logical-slot-continuation-storage-call storage :check-under-lock)
      (unless (and (zerop (hash-table-count *wal-replication-slots*))
                   (eql child (fifth host)) (eql child *wal-insert-timeline*)
                   (eq :timeline *wal-segment-namespace*))
        (error "cold reservation requires unpublished original-child startup"))
      (when (gethash root *logical-slot-continuation-protections*)
        (error "cold reservation cannot replace an existing runtime group"))
      (let ((prior (gethash root *logical-slot-continuation-cold-reservations*)))
        (when prior
          (when failure-cell
            (sb-sys:without-interrupts
              (setf (car failure-cell)
                    (logical-slot-continuation-cold-reservation-call prior :failure-cleanup))))
          (logical-slot-continuation-cold-reservation-call prior :check)
          (unless (and (equalp binding (logical-slot-continuation-cold-reservation-call prior :binding))
                       (eq origin (logical-slot-continuation-cold-reservation-call prior :origin)))
            (error "cold reservation changed its permanent binding or origin"))
          (return-from logical-slot-continuation-ensure-cold-reservation prior)))
      (when (gethash *wal-replication-slots* *logical-slot-continuation-cold-registries*)
        (error "cold reservation lost its registered root"))
      (multiple-value-bind (reservation failure)
          (logical-slot-continuation-build-cold-reservation-under-lock root binding origin fork child)
        (sb-sys:without-interrupts
          (when failure-cell (setf (car failure-cell) failure))
          (setf (gethash reservation *wal-snapshot-retention-pins*) fork
                (gethash reservation *logical-slot-continuation-cold-identities*)
                (logical-slot-continuation-cold-reservation-state reservation)
                (gethash (copy-list root) *logical-slot-continuation-cold-reservations*) reservation
                (gethash *wal-replication-slots* *logical-slot-continuation-cold-registries*) reservation))
        reservation))))
