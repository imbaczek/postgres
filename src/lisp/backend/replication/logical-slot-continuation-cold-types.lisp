(in-package #:pg)

(defstruct (logical-slot-continuation-cold-reservation
            (:constructor %make-logical-slot-continuation-cold-reservation)
            (:print-function physical-libpq-print-private-object))
  ;; Registered startup reservation; classification does not publish a group.
  (state nil :read-only t))

(defvar *logical-slot-continuation-cold-identities*
  (make-hash-table :test #'eq :weakness :key :synchronized t))

(defvar *logical-slot-continuation-cold-test-hook* nil)

(defmacro with-logical-slot-continuation-observation-check
    ((active failed thread label) &body body)
  (unless (stringp label) (error "observation check requires a literal label"))
  (let ((complete (gensym "COMPLETE")))
    `(sb-sys:without-interrupts
       (unless (and ,active (eq ,thread sb-thread:*current-thread*))
         (error ,(concatenate 'string label " expired or changed thread")))
       (when ,failed (error ,(concatenate 'string label " failed")))
       (let ((,complete nil))
         (unwind-protect
              (multiple-value-prog1
                  (sb-sys:with-local-interrupts (progn ,@body))
                (setf ,complete t))
           (unless ,complete (setf ,failed t)))))))

(defun logical-slot-continuation-cold-reservation-call (reservation operation &optional (value nil supplied-p))
  ;; Check the registered reservation under WAL exclusion before dispatch.
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((state (and (logical-slot-continuation-cold-reservation-p reservation)
                    (gethash reservation *logical-slot-continuation-cold-identities*))))
    (unless (and (functionp state)
                 (eq state (logical-slot-continuation-cold-reservation-state reservation)))
      (error "cold continuation requires its registered reservation"))
    (if supplied-p (funcall state operation reservation value)
        (funcall state operation reservation))))

(defun logical-slot-continuation-cold-check-admission ()
  ;; Anchor admission to both the actual root and registry while the group is
  ;; unpublished, including when either identity has changed.
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
