(in-package #:pg)

(cl-spec:defproperty startup-entry-detaches-the-selected-image
    ((generation (range integer 0 1024)))
  (:about logical-slot-startup-entry-from-image)
  (:trials (:normal 100))
  (let* ((slot (unit-imported-state-baseline :generation generation))
         (*wal-durable-lsn* (wal-replication-slot-confirmed-flush-lsn slot))
         (bytes (logical-slot-state-encode slot))
         (entry (logical-slot-startup-entry-from-image bytes (wal-replication-slot-name slot))))
    (and (equalp bytes (logical-slot-startup-entry-bytes entry))
         (not (eq bytes (logical-slot-startup-entry-bytes entry)))
         (not (eq slot (logical-slot-startup-entry-slot entry)))
         (eq (logical-slot-startup-entry-profile entry)
             (wal-slot-profile (logical-slot-startup-entry-slot entry)))
         (eq (logical-slot-startup-entry-provenance entry)
             (wal-replication-slot-logical-provenance (logical-slot-startup-entry-slot entry))))))

(rove:deftest startup-selected-entry-generated-contract
    (let ((result (cl-spec:result-data
                   (cl-spec:run-property 'startup-entry-detaches-the-selected-image
                                         :seed 11604 :profile :normal))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))
      (rove:ok (= 100 (getf result :trials)))))

(rove:deftest startup-selected-inventory-forgeries-refuse-before-io
    (let* ((calls 0) (reads 0) (original (symbol-function 'physical-image-file-facts))
                     (inventory (%make-logical-slot-startup-inventory
                                 :state (lambda (&rest ignored) (declare (ignore ignored)) (incf calls))))
                     (source (%make-logical-slot-imported-startup-source :inventory inventory)))
      (unwind-protect
           (progn
             (setf (symbol-function 'physical-image-file-facts)
                   (lambda (&rest args) (incf reads) (apply original args)))
             (dolist (action
                      (list (lambda () (logical-slot-startup-inventory-dispatch inventory :preflight))
                            (lambda () (logical-slot-startup-inventory-dispatch
                                        (copy-logical-slot-startup-inventory inventory) :preflight))
                            (lambda () (logical-slot-startup-inventory-call source :observe))
                            (lambda () (logical-slot-startup-inventory-call
                                        (copy-logical-slot-imported-startup-source source) :check))))
               (rove:ok (handler-case (progn (funcall action) nil) (error () t))))
             (rove:ok (zerop reads))
             (rove:ok (zerop calls)))
        (setf (symbol-function 'physical-image-file-facts) original))))
