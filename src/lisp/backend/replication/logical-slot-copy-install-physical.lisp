(in-package #:pg)

(defun logical-slot-copy-install-physical-registry (mode physical logical-names)
  (dolist (slot physical) (physical-slot-encode slot))
  (ecase mode
    (:cold
     (unless (zerop (hash-table-count *wal-replication-slots*))
       (error "cold installation physical registry requires unopened storage"))
     (when (some #'wal-replication-slot-temporary-p physical)
       (error "cold installation cannot retain temporary physical images"))
     physical)
    (:live
     (dolist (name logical-names)
       (when (nth-value 1 (gethash name *wal-replication-slots*))
         (error "logical installation row already occupies the live registry")))
     (unless (= (length physical) (hash-table-count *wal-replication-slots*))
       (error "installation physical registry count differs from canonical slots"))
     (loop for disk in physical
           for live = (gethash (wal-replication-slot-name disk) *wal-replication-slots*)
           collect
           (progn
             (unless (and (wal-replication-slot-p live) (wal-physical-slot-p live))
               (error "installation physical registry changed its object kind"))
             (wal-require-usable-replication-slot live)
             (let ((normalized (copy-wal-replication-slot disk)))
               (setf (wal-replication-slot-inactive-since normalized)
                     (wal-replication-slot-inactive-since live))
               (unless (equalp (physical-slot-encode normalized) (physical-slot-encode live))
                 (error "installation physical registry differs from its durable image")))
             live)))))

(defun logical-slot-copy-install-physical-witnesses (slots)
  (mapcar (lambda (slot)
            (list slot (physical-slot-encode slot)
                  (wal-replication-slot-active-owner slot)
                  (wal-replication-slot-active-owner-backend slot)
                  (wal-replication-slot-temporary-owner slot)))
          slots))

(defun logical-slot-copy-install-check-physical-witnesses (expected slots)
  (unless (and (= (length expected) (length slots))
               (loop for witness in expected for slot in slots
                     always (and (eq (first witness) slot)
                                 (equalp (second witness) (physical-slot-encode slot))
                                 (eq (third witness) (wal-replication-slot-active-owner slot))
                                 (eq (fourth witness) (wal-replication-slot-active-owner-backend slot))
                                 (eq (fifth witness) (wal-replication-slot-temporary-owner slot)))))
    (error "installation physical registry ownership changed"))
  t)
