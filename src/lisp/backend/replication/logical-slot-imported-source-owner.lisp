(in-package #:pg)

(defstruct (logical-slot-imported-scan-owner
            (:include physical-adoption-owner)
            (:constructor %make-logical-slot-imported-scan-owner)
            (:print-function physical-libpq-print-private-object))
  ;; An observational owner borrowing postmaster ownership, not adoption authority.
  (ownership nil :read-only t) (scan nil :read-only t)
  (source nil :read-only t) (principal nil :read-only t)
  (state nil :read-only t))

(defvar *logical-slot-imported-scan-owners* nil)

(defun logical-slot-imported-history-equal-p (expected observed)
  "Compare the complete observed history with the bounded expected history."
  (loop with remaining = observed
        for original in expected
        always (and (consp remaining)
                    (let ((entry (pop remaining)))
                      (and (timeline-history-entry-p entry)
                           (eql (timeline-history-entry-timeline original)
                                (timeline-history-entry-timeline entry))
                           (eql (timeline-history-entry-begin original)
                                (timeline-history-entry-begin entry))
                           (eql (timeline-history-entry-end original)
                                (timeline-history-entry-end entry)))))
        finally (return (null remaining))))

(defun call-with-logical-slot-imported-scan-check (owner function)
  "Check identity before the sticky live-owner checks and FUNCTION.
A foreign or copied owner cannot fail the original owner's scope."
  (unless (logical-slot-imported-scan-owner-p owner)
    (error "imported logical source requires its typed scan owner"))
  (let ((state (logical-slot-imported-scan-owner-state owner)))
    (funcall state :identity owner)
    (sb-sys:without-interrupts
      (let ((complete nil))
        (unwind-protect
             (multiple-value-prog1
                 (sb-sys:with-local-interrupts
                   (funcall state :check owner)
                   (unless (and (physical-adoption-owner-active-p owner)
                                (member owner *logical-slot-imported-scan-owners* :test #'eq))
                     (error "imported logical source has no live registered scan owner"))
                   (funcall function))
               (setf complete t))
          (unless complete
            (funcall state :fail owner)
            (setf (physical-adoption-owner-active-p owner) nil)))))))

(defun logical-slot-imported-scan-check-under-lock (owner)
  "Recheck scan, purpose and source against the registered principal under WAL."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (call-with-logical-slot-imported-scan-check
   owner
   (lambda ()
     (let* ((scan (logical-slot-imported-scan-owner-scan owner))
            (source (logical-slot-imported-scan-owner-source owner))
            (principal (logical-slot-imported-scan-owner-principal owner)))
       (wal-retained-slot-scan-check-scope-under-lock scan)
       (unless (and
                (eq principal (find scan *wal-retained-slot-scan-scopes*
                                    :key #'wal-retained-slot-scan-scope-scan :test #'eq))
                (eq (wal-retained-slot-scan-scope-purpose principal) (logical-wal-source-purpose source))
                (eq (wal-retained-slot-scan-slot scan) (logical-wal-source-slot source))
                (eq (wal-retained-slot-scan-owner scan) (logical-wal-source-owner source))
                (equal (wal-retained-slot-scan-name scan) (logical-wal-source-name source))
                (eql (wal-retained-slot-scan-database scan) (logical-wal-source-database source))
                (equal (wal-retained-slot-scan-plugin scan) (logical-wal-source-plugin source))
                (eql (wal-retained-slot-scan-timeline scan) (logical-wal-source-timeline source))
                (eq (wal-retained-slot-scan-namespace scan) (logical-wal-source-namespace source)))
         (error "imported logical source differs from its registered scan"))
       (logical-wal-check-source source (wal-retained-slot-scan-end scan))))))

(defmethod physical-adoption-check-owner-role ((owner logical-slot-imported-scan-owner))
  (call-with-logical-slot-imported-scan-check
   owner
   (lambda ()
     (when (sb-thread:holding-mutex-p *wal-lock*)
       (error "imported source ownership must be checked outside the WAL mutex"))
     (logical-slot-copy-check-postmaster-owner
      owner (logical-slot-imported-scan-owner-ownership owner))
     (sb-thread:with-mutex (*wal-lock*) (logical-slot-imported-scan-check-under-lock owner))
     owner)))

(defun logical-slot-imported-scan-check-owner (owner)
  "Check the borrowed directory owner outside WAL and its scan under WAL."
  (call-with-logical-slot-imported-scan-check
   owner (lambda () (physical-directory-check-owner owner))))

(defmethod physical-adoption-check-owner ((owner logical-slot-imported-scan-owner))
  (error "imported logical source observation does not authorize offline adoption"))

(defun call-with-logical-slot-imported-scan-owner (scan source function)
  "Borrow postmaster ownership for a purpose-bound observational scan.
This alone does not establish the permanent pair, F proof, lease, provider,
startup or readiness required for imported source use."
  (when (sb-thread:holding-mutex-p *wal-lock*)
    (error "imported source acquisition must run outside the WAL mutex"))
  (let ((principal
          (sb-thread:with-mutex (*wal-lock*)
            (wal-retained-slot-scan-check-scope-under-lock scan)
            (find scan *wal-retained-slot-scan-scopes*
                  :key #'wal-retained-slot-scan-scope-scan :test #'eq))))
    (call-with-postmaster-ownership
     (lambda (ownership)
       (let ((owner nil) (active t) (failed nil)
                         (history (timeline-check-lineage
                                   (timeline-parse-history
                                    (logical-wal-source-timeline source)
                                    (if (logical-wal-source-history-bytes source)
                                        (bytes->string (logical-wal-source-history-bytes source)) ""))))
                         (thread sb-thread:*current-thread*) (mutex *wal-lock*))
         (flet ((state (operation candidate)
                  (unless (and (eq candidate owner) (eq thread sb-thread:*current-thread*)
                               (eq mutex *wal-lock*))
                    (error "imported logical source owner changed identity, thread or WAL mutex"))
                  (ecase operation
                    (:identity t)
                    (:check (unless (and active (not failed)
                                         (logical-slot-imported-history-equal-p history (logical-wal-source-history source)))
                              (error "imported source owner ended or failed")))
                    (:fail (setf failed t))
                    (:release (setf active nil)))))
           (setf owner (apply #'%make-logical-slot-imported-scan-owner
                              :scan scan :source source :principal principal :state #'state
                              (logical-slot-copy-owner-arguments ownership)))
           (let ((*logical-slot-imported-scan-owners* (cons owner *logical-slot-imported-scan-owners*)))
             (unwind-protect
                  (progn
                    (logical-slot-imported-scan-check-owner owner)
                    (multiple-value-prog1 (funcall function owner)
                      (logical-slot-imported-scan-check-owner owner)))
               (state :release owner)
               (setf (physical-adoption-owner-active-p owner) nil)))))))))
