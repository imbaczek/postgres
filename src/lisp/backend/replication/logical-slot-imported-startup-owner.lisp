(in-package #:pg)

(defstruct (logical-slot-imported-startup-owner
            (:include physical-adoption-owner)
            (:constructor %make-logical-slot-imported-startup-owner)
            (:print-function physical-libpq-print-private-object))
  ;; Borrow the real postmaster owner for one unpublished child candidate.
  (ownership nil :read-only t) (source nil :read-only t)
  (entry nil :read-only t) (principal nil :read-only t)
  (state nil :read-only t))

(defvar *logical-slot-imported-startup-owners* nil)

(defun call-with-logical-slot-imported-startup-owner-check (owner function)
  "Check identity before sticky failure handling; the owner expires with its scope."
  (unless (logical-slot-imported-startup-owner-p owner)
    (error "imported startup observation requires its typed owner"))
  (let ((state (logical-slot-imported-startup-owner-state owner)))
    (funcall state :identity owner)
    (sb-sys:without-interrupts
      (let ((complete nil))
        (unwind-protect
             (multiple-value-prog1
                 (sb-sys:with-local-interrupts
                   (funcall state :check owner)
                   (unless (and (physical-adoption-owner-active-p owner)
                                (member owner *logical-slot-imported-startup-owners* :test #'eq))
                     (error "imported startup owner is no longer registered"))
                   (funcall function))
               (setf complete t))
          (unless complete
            (funcall state :fail owner)
            (setf (physical-adoption-owner-active-p owner) nil)))))))

(defun logical-slot-imported-startup-owner-under-lock (owner)
  "Recheck the exact private principal and captured child entry under WAL."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (call-with-logical-slot-imported-startup-owner-check
   owner
   (lambda ()
     (let* ((source (logical-slot-imported-startup-owner-source owner))
            (entry (logical-slot-imported-startup-owner-entry owner))
            (principal (logical-slot-imported-startup-owner-principal owner)))
       (unless (and (eq principal (logical-slot-imported-startup-principal-under-lock source))
                    (member entry (logical-slot-imported-startup-bounded-entries source) :test #'eq)
                    (wal-replication-slot-child-owner (logical-slot-startup-entry-slot entry)))
         (error "imported startup owner lost its captured candidate"))
       owner))))

(defmethod physical-adoption-check-owner-role ((owner logical-slot-imported-startup-owner))
  (call-with-logical-slot-imported-startup-owner-check
   owner
   (lambda ()
     (when (sb-thread:holding-mutex-p *wal-lock*)
       (error "imported startup ownership requires the WAL mutex to be released"))
     (let ((source (logical-slot-imported-startup-owner-source owner)))
       (logical-slot-copy-check-postmaster-owner
        owner (logical-slot-imported-startup-owner-ownership owner))
       (logical-slot-startup-check-authority source)
       (logical-slot-startup-check-inventory source)
       (sb-thread:with-mutex (*wal-lock*)
         (logical-slot-imported-startup-owner-under-lock owner)))
     owner)))

(defun logical-slot-imported-startup-check-owner (owner)
  (call-with-logical-slot-imported-startup-owner-check
   owner (lambda () (physical-directory-check-owner owner))))

(defmethod physical-adoption-check-owner ((owner logical-slot-imported-startup-owner))
  (error "imported startup observation does not authorize offline adoption"))

(defun call-with-logical-slot-imported-startup-owner (source entry function)
  "Observe one captured child entry under borrowed postmaster ownership.
The entry is not published by this scope."
  (when (sb-thread:holding-mutex-p *wal-lock*)
    (error "imported startup owner acquisition requires the WAL mutex to be released"))
  (let ((principal (sb-thread:with-mutex (*wal-lock*)
                     (logical-slot-imported-startup-principal-under-lock source)))
        (ownership (logical-slot-startup-source-ownership source))
        (owner nil) (active t) (failed nil)
        (thread sb-thread:*current-thread*) (mutex *wal-lock*))
    (unless (and (member entry (logical-slot-imported-startup-bounded-entries source) :test #'eq)
                 (wal-replication-slot-child-owner (logical-slot-startup-entry-slot entry)))
      (error "imported startup owner requires an exact captured child candidate"))
    (flet ((state (operation candidate)
             (unless (and (eq candidate owner) (eq thread sb-thread:*current-thread*)
                          (eq mutex *wal-lock*))
               (error "imported startup owner changed identity, thread or WAL mutex"))
             (ecase operation
               (:identity t)
               (:check (unless (and active (not failed))
                         (error "imported startup owner ended or failed")))
               (:fail (setf failed t))
               (:release (setf active nil)))))
      (setf owner (apply #'%make-logical-slot-imported-startup-owner
                         :source source :entry entry :principal principal :state #'state
                         (logical-slot-copy-owner-arguments ownership)))
      (let ((*logical-slot-imported-startup-owners*
              (cons owner *logical-slot-imported-startup-owners*)))
        (unwind-protect
             (progn
               (logical-slot-imported-startup-check-owner owner)
               (multiple-value-prog1 (funcall function owner)
                 (logical-slot-imported-startup-check-owner owner)))
          (state :release owner)
          (setf (physical-adoption-owner-active-p owner) nil))))))
