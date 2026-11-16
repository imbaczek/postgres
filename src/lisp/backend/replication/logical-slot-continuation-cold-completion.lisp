(in-package #:pg)

(defvar *logical-slot-continuation-cold-completions*
  (make-hash-table :test #'eq :weakness :key :synchronized t))
(defvar *logical-slot-continuation-cold-completion-scopes* nil)

(defun logical-slot-continuation-cold-publication-point (point completion group registry)
  (when *logical-slot-continuation-cold-publication-test-hook*
    (funcall *logical-slot-continuation-cold-publication-test-hook*
             point completion group registry)))

(defun logical-slot-continuation-cold-completion-call (completion operation)
  (unless (and (logical-slot-continuation-cold-completion-p completion)
               (member completion *logical-slot-continuation-cold-completion-scopes* :test #'eq))
    (error "cold completion requires its scoped registered receipt"))
  (sb-sys:without-interrupts
    (let* ((state (logical-slot-continuation-cold-completion-state completion))
           (binding (gethash completion *logical-slot-continuation-cold-completions*)))
      (unless (and (functionp state) binding (eq state (car binding)))
        (when (functionp state) (funcall state :lost completion))
        (error "cold completion lost its registered receipt"))
      (sb-sys:with-local-interrupts (funcall state operation completion)))))

(defun logical-slot-continuation-cold-completion-check-data (data owner control)
  (if (sb-thread:holding-mutex-p *wal-lock*)
      (logical-slot-imported-host-startup-under-lock owner)
      (logical-slot-imported-host-startup-check-owner owner))
  (flet ((check ()
           (unless (and (eq (getf data :mutex) *wal-lock*)
                        (eq (getf data :thread) sb-thread:*current-thread*)
                        (eq (getf data :incarnation) *wal-storage-incarnation*)
                        (eq (getf data :namespace) *wal-segment-namespace*)
                        (eq (getf data :wal-level) *wal-level*)
                        (eql (getf data :maximum) *max-replication-slots*)
                        (equalp (getf data :host) (logical-slot-imported-host-startup-image))
                        (if (eq :published (svref control 0))
                            (and (eq (getf data :registry) *wal-replication-slots*)
                                 (logical-slot-continuation-protection-p (svref control 2))
                                 (logical-slot-continuation-protection-call (svref control 2) :check))
                            (equal (getf data :mirrors) (logical-slot-publication-mirrors))))
             (error "cold completion changed its host, configuration or publication"))
           (logical-slot-publication-check-registry (getf data :registry) (getf data :image))
           (let ((reservation (getf data :reservation)))
             (logical-slot-continuation-cold-reservation-call reservation :check)
             (unless (and (equalp (getf data :binding)
                                  (logical-slot-continuation-cold-reservation-call reservation :binding))
                          (eq (getf data :origin)
                              (logical-slot-continuation-cold-reservation-call reservation :origin)))
               (error "cold completion changed its permanent binding or origin")))
           t))
    (if (sb-thread:holding-mutex-p *wal-lock*) (check)
        (sb-thread:with-mutex (*wal-lock*) (check)))))

;; Reconciliation produces a scoped receipt for the candidate registry.  The
;; receipt can be claimed once by publication, and is invalid after the callback.
;; On failure, leave the reservation in its failure state.
(defun call-with-logical-slot-continuation-cold-completion
    (owner gate ownership function &optional failure-cell)
  (unless (and (logical-slot-startup-publication-owner-p owner) (functionp function)
               (or (null failure-cell)
                   (and (consp failure-cell) (null (car failure-cell)) (null (cdr failure-cell)))))
    (error "cold completion requires its publication owner, callback and fresh failure cell"))
  (let ((data nil) (completion nil) (active t) (busy nil) (complete nil)
                   (mutex *wal-lock*) (thread sb-thread:*current-thread*)
                   (failure (cons nil nil)) (control (vector :ready nil nil)))
    (labels ((state (operation candidate)
               (unless (and active (eq completion candidate) (eq mutex *wal-lock*)
                            (eq thread sb-thread:*current-thread*))
                 (error "cold completion ended or changed identity, mutex or thread"))
               (sb-sys:without-interrupts
                 (when (eq operation :lost)
                   (setf (svref control 1) t)
                   (error "cold completion lost its registered receipt"))
                 (when (and (eq operation :claim) (not (eq :ready (svref control 0))))
                   (setf (svref control 1) t)
                   (error "cold completion was already claimed"))
                 (when (or busy (svref control 1))
                   (setf (svref control 1) t)
                   (error "cold completion failed or is busy"))
                 (let ((done nil))
                   (setf busy t)
                   (unwind-protect
                        (multiple-value-prog1
                            (sb-sys:with-local-interrupts
                              (logical-slot-continuation-cold-completion-check-data data owner control)
                              (multiple-value-prog1
                                  (ecase operation
                                    (:check t)
                                    (:data (logical-slot-continuation-copy data))
                                    (:claim
                                     (setf (svref control 0) :claimed)
                                     (logical-slot-continuation-copy data)))
                                (unless (and (not (svref control 1))
                                             (eq #'state
                                                 (car (gethash completion
                                                               *logical-slot-continuation-cold-completions*))))
                                  (error "cold completion failed or lost registration during its check"))))
                          (setf done t))
                     (setf busy nil)
                     (unless done (setf (svref control 1) t)))))))
      (sb-sys:without-interrupts
        (unwind-protect
             (multiple-value-prog1
                 (sb-sys:with-local-interrupts
                   (call-with-logical-slot-continuation-cold-preparation
                    gate ownership
                    (lambda (preparation)
                      (let ((reservation
                              (logical-slot-continuation-cold-preparation-call preparation :reservation)))
                        (sb-thread:with-mutex (*wal-lock*)
                          (let ((cleanup (logical-slot-continuation-cold-reservation-call
                                          reservation :failure-cleanup)))
                            (sb-sys:without-interrupts
                              (setf (car failure) cleanup (cdr failure) *wal-lock*)
                              (when failure-cell
                                (setf (car failure-cell) cleanup (cdr failure-cell) *wal-lock*))))))
                      (setf data (nth-value 1
                                            (logical-slot-continuation-cold-reconcile-prepared
                                             owner preparation)))))
                   (logical-slot-continuation-cold-completion-check-data data owner control)
                   (setf completion (%make-logical-slot-continuation-cold-completion :state #'state))
                   (let ((*logical-slot-continuation-cold-completion-scopes*
                           (cons completion *logical-slot-continuation-cold-completion-scopes*)))
                     (setf (gethash completion *logical-slot-continuation-cold-completions*)
                           (cons #'state control))
                     (logical-slot-continuation-cold-publication-point
                      :completion-ready completion nil (getf data :registry))
                     (multiple-value-prog1 (funcall function completion)
                       (state :check completion))))
               (setf complete t))
          (setf active nil)
          (when completion (remhash completion *logical-slot-continuation-cold-completions*))
          (unless complete
            (logical-slot-continuation-cold-preparation-fail (car failure) (cdr failure))))))))
