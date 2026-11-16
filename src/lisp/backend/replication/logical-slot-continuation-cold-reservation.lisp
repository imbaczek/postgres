(in-package #:pg)

(defun logical-slot-continuation-ensure-cold-reservation (storage &optional failure-cell)
  ;; Require actual original-child startup, its restored origin and an empty
  ;; registry.  Install a persistent fork pin and root/registry anchors; no
  ;; slot admission follows from this reservation.  A prior reservation is
  ;; reusable only with the same binding and origin.
  ;;
  ;; Populate the private failure cell before installing the pin and maps, so
  ;; later failure or close can mark the reservation uncertain and retain F.
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
      (let ((pins *wal-snapshot-retention-pins*) (incarnation *wal-storage-incarnation*)
                                                 (registry *wal-replication-slots*)
                                                 (mutex *wal-lock*) (private-root (copy-list root)) (private-binding (copy-seq binding))
                                                 (reservation nil) (uncertain nil))
        (labels ((fail ()
                   (assert (sb-thread:holding-mutex-p mutex))
                   (setf uncertain t))
                 (state (operation object)
                   (assert (and (eq mutex *wal-lock*) (sb-thread:holding-mutex-p mutex)))
                   (unless (and (eq object reservation)
                                (eq reservation
                                    (gethash private-root *logical-slot-continuation-cold-reservations*))
                                (eq reservation (gethash registry *logical-slot-continuation-cold-registries*)))
                     (error "cold reservation changed its identity or root registration"))
                   (ecase operation
                     (:identity t)
                     (:uncertain uncertain)
                     (:failure-cleanup #'fail)
                     (:binding (copy-seq private-binding))
                     (:origin origin)
                     (:fork fork)
                     (:require-published
                      (ereport "55000" "continuation cold reconciliation has not published its group"))
                     (:check
                      (when uncertain (error "cold continuation reservation is uncertain"))
                      (unless (and (eq registry *wal-replication-slots*)
                                   (zerop (hash-table-count registry))
                                   (eq pins *wal-snapshot-retention-pins*)
                                   (eql fork (gethash reservation pins))
                                   (eq incarnation *wal-storage-incarnation*)
                                   (eql child *wal-insert-timeline*) (eq :timeline *wal-segment-namespace*))
                        (error "cold reservation lost its storage or fork pin"))
                      (let ((f (physical-image-file-facts
                                -100 (namestring (uiop:ensure-directory-pathname *pgdata*)))))
                        (unless (equal private-root (list (first f) (second f) (ninth f)))
                          (error "cold reservation lost its original PGDATA root")))
                      (logical-slot-imported-host-origin-call origin :check-root)
                      t))))
          (setf reservation (%make-logical-slot-continuation-cold-reservation :state #'state))
          (sb-sys:without-interrupts
            (when failure-cell (setf (car failure-cell) #'fail))
            (setf (gethash reservation pins) fork
                  (gethash reservation *logical-slot-continuation-cold-identities*) #'state
                  (gethash (copy-list private-root) *logical-slot-continuation-cold-reservations*) reservation
                  (gethash registry *logical-slot-continuation-cold-registries*) reservation))
          reservation)))))
