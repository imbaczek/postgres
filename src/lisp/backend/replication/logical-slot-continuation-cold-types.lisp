(in-package #:pg)

(defstruct (logical-slot-continuation-cold-reservation
            (:constructor %make-logical-slot-continuation-cold-reservation)
            (:print-function physical-libpq-print-private-object))
  (state nil :read-only t))

(defvar *logical-slot-continuation-cold-reservations* (make-hash-table :test #'equal :synchronized t))

(defvar *logical-slot-continuation-cold-registries* (make-hash-table :test #'eq :synchronized t))

(defvar *logical-slot-continuation-cold-identities*
  (make-hash-table :test #'eq :weakness :key :synchronized t))

(defvar *logical-slot-continuation-cold-test-hook* nil)

(defun logical-slot-continuation-cold-reservation-call (reservation operation)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((state (and (logical-slot-continuation-cold-reservation-p reservation)
                    (gethash reservation *logical-slot-continuation-cold-identities*))))
    (unless (and (functionp state)
                 (eq state (logical-slot-continuation-cold-reservation-state reservation)))
      (error "cold continuation requires its registered reservation"))
    (funcall state operation reservation)))

(defun logical-slot-continuation-cold-check-admission ()
  (let ((registered (gethash *wal-replication-slots* *logical-slot-continuation-cold-registries*)))
    (when (or registered (plusp (hash-table-count *logical-slot-continuation-cold-reservations*)))
      (let* ((facts (physical-image-file-facts
                     -100 (namestring (uiop:ensure-directory-pathname *pgdata*))))
             (key (list (first facts) (second facts) (ninth facts)))
             (reservation (gethash key *logical-slot-continuation-cold-reservations*)))
        (when (and registered (not (eq registered reservation)))
          (error "cold continuation changed its root registration"))
        (when reservation
          (logical-slot-continuation-cold-reservation-call reservation :require-published)))))
  t)
