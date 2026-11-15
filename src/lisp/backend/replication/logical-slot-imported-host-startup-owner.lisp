(in-package #:pg)

;; Observe an imported host at primary startup, even when it has no slots.
;; The private baseline is a coherent post-replay control image and its
;; associated root, timeline, and durable end, captured under the WAL lock.
;; Each check compares against that same image; history facts are copied.
;; The startup gate and postmaster ownership must remain valid for the scope.
;; A failed check is sticky, and release ends the owner's lifetime.
;; This observation grants no adoption, admission, or retention authority.

(defstruct (logical-slot-imported-host-startup-owner
            (:include physical-adoption-owner)
            (:constructor %make-logical-slot-imported-host-startup-owner)
            (:print-function physical-libpq-print-private-object))
  (ownership nil :read-only t) (state nil :read-only t))

(defvar *logical-slot-imported-host-startup-owners* nil)

(defun logical-slot-imported-host-startup-image ()
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let* ((control (logical-slot-startup-control-image))
         (fields
           (with-input-from-string (stream (bytes->string control))
             (let ((line (read-line stream nil nil)))
               (unless (and line (<= (length line) +wal-control-maximum-bytes+)
                            (null (read-line stream nil nil)))
                 (error "imported host startup control is not one bounded line"))
               (multiple-value-list (wal-decode-control-line line)))))
         (system-id (physical-slot-startup-system-id))
         (timeline *wal-insert-timeline*)
         (history-bytes (unless (= timeline 1) (timeline-history-bytes timeline))))
    (unless (and *wal-ready* (not (recovery-in-progress-p))
                 (typep system-id '(integer 1 18446744073709551615))
                 (eql system-id (getf (seventh fields) :system-id))
                 (eql *wal-durable-lsn* (second fields))
                 (eql timeline (fourth fields))
                 (eq *wal-segment-namespace* (sixth fields))
                 (equalp *wal-control-metadata* (seventh fields)))
      (error "imported host startup requires accepted post-replay primary control"))
    (timeline-check-lineage
     (timeline-parse-history timeline (if history-bytes (bytes->string history-bytes) "")))
    (wal-check-healthy)
    (list system-id (logical-wal-current-root-identity) control history-bytes timeline *wal-durable-lsn*)))

(defun call-with-logical-slot-imported-host-startup-check (owner function)
  (unless (logical-slot-imported-host-startup-owner-p owner)
    (error "imported host observation requires its typed startup owner"))
  (let ((state (logical-slot-imported-host-startup-owner-state owner)))
    (funcall state :identity owner)
    (sb-sys:without-interrupts
      (let ((complete nil))
        (unwind-protect
             (multiple-value-prog1
                 (sb-sys:with-local-interrupts
                   (funcall state :check owner)
                   (unless (and (physical-adoption-owner-active-p owner)
                                (member owner *logical-slot-imported-host-startup-owners* :test #'eq))
                     (error "imported host startup owner is no longer registered"))
                   (funcall function))
               (setf complete t))
          (unless complete
            (funcall state :fail owner)
            (setf (physical-adoption-owner-active-p owner) nil)))))))

(defun logical-slot-imported-host-startup-under-lock (owner)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (call-with-logical-slot-imported-host-startup-check
   owner (lambda ()
           (funcall (logical-slot-imported-host-startup-owner-state owner) :under-lock owner)
           owner)))

(defun logical-slot-imported-host-startup-facts-under-lock (owner)
  (logical-slot-imported-host-startup-under-lock owner)
  (funcall (logical-slot-imported-host-startup-owner-state owner) :facts owner))

(defmethod physical-adoption-check-owner-role ((owner logical-slot-imported-host-startup-owner))
  (call-with-logical-slot-imported-host-startup-check
   owner
   (lambda ()
     (when (sb-thread:holding-mutex-p *wal-lock*)
       (error "imported host startup ownership requires the WAL mutex to be released"))
     (funcall (logical-slot-imported-host-startup-owner-state owner) :authority owner)
     (sb-thread:with-mutex (*wal-lock*) (logical-slot-imported-host-startup-under-lock owner))
     owner)))

(defun logical-slot-imported-host-startup-check-owner (owner)
  (call-with-logical-slot-imported-host-startup-check
   owner (lambda () (physical-directory-check-owner owner))))

(defmethod physical-adoption-check-owner ((owner logical-slot-imported-host-startup-owner))
  (error "imported host startup observation does not authorize offline adoption"))

(defun call-with-logical-slot-imported-host-startup-owner (gate ownership function)
  (when (sb-thread:holding-mutex-p *wal-lock*)
    (error "imported host startup acquisition requires the WAL mutex to be released"))
  (check-primary-storage-startup-context gate ownership)
  (multiple-value-bind (image incarnation namespace registry thread mutex)
      (sb-thread:with-mutex (*wal-lock*)
        (values (logical-slot-imported-host-startup-image) *wal-storage-incarnation*
                *wal-segment-namespace* *wal-replication-slots* sb-thread:*current-thread* *wal-lock*))
    (let ((owner nil) (active t) (failed nil))
      (flet ((state (operation candidate)
               (unless (and (eq candidate owner) (eq thread sb-thread:*current-thread*) (eq mutex *wal-lock*))
                 (error "imported host startup owner changed identity, thread or WAL mutex"))
               (ecase operation
                 (:identity t)
                 (:check
                  (unless (and active (not failed)
                               (eq ownership (logical-slot-imported-host-startup-owner-ownership owner)))
                    (error "imported host startup owner ended or failed")))
                 (:authority
                  (check-primary-storage-startup-context gate ownership)
                  (logical-slot-copy-check-postmaster-owner owner ownership))
                 (:under-lock
                  (assert (sb-thread:holding-mutex-p *wal-lock*))
                  (unless (and (eq incarnation *wal-storage-incarnation*)
                               (eq namespace *wal-segment-namespace*)
                               (eq registry *wal-replication-slots*)
                               (equalp image (logical-slot-imported-host-startup-image)))
                    (error "imported host startup source changed")))
                 (:facts
                  (values (first image)
                          (timeline-check-lineage
                           (timeline-parse-history
                            (fifth image) (if (fourth image) (bytes->string (fourth image)) "")))
                          (sixth image)))
                 (:fail (setf failed t))
                 (:release (setf active nil)))))
        (setf owner (apply #'%make-logical-slot-imported-host-startup-owner
                           :state #'state (logical-slot-copy-owner-arguments ownership)))
        (let ((*logical-slot-imported-host-startup-owners*
                (cons owner *logical-slot-imported-host-startup-owners*)))
          (unwind-protect
               (progn
                 (logical-slot-imported-host-startup-check-owner owner)
                 (multiple-value-prog1 (funcall function owner)
                   (logical-slot-imported-host-startup-check-owner owner)))
            (state :release owner)
            (setf (physical-adoption-owner-active-p owner) nil)))))))
