(in-package #:pg)

(defun logical-slot-copy-import-control-new-state (owner context source source-check mark-started)
  ;; Reconstruct the deterministic child image from the leased source and the
  ;; original pending certificate before accepting any temporary file.
  (logical-slot-copy-import-control-fences context)
  (multiple-value-bind (certificate certificate-bytes certificate-facts)
      (logical-slot-copy-import-read-artifact context :pending)
    (unless (and certificate (eq :pending (getf certificate :phase)))
      (error "control temporary reconciliation requires its original pending certificate"))
    (multiple-value-bind (evidence evidence-bytes evidence-facts)
        (logical-slot-copy-import-read-artifact context :evidence)
      (unless evidence (error "control temporary reconciliation requires permanent evidence"))
      (multiple-value-bind (descriptor observations manifest control)
          (logical-slot-copy-check-import-receipt owner context)
        (let* ((plan (logical-slot-copy-import-plan-from-source
                      owner context certificate source descriptor observations control))
               (expected
                 (let ((*physical-recovery-state* (logical-slot-copy-import-pending-plan-state plan)))
                   (let ((child-plan (logical-slot-copy-import-pending-plan-child-plan plan)))
                     (physical-promotion-child-control-image
                      child-plan (getf (getf child-plan :snapshot) :time))))))
          (unless (and (equalp evidence-bytes (logical-slot-copy-import-pending-plan-evidence plan))
                       (<= 1 (length expected) (1+ +wal-control-maximum-bytes+)))
            (error "control temporary evidence or child image differs from its source plan"))
          (logical-slot-copy-import-evidence-check-certificate
           evidence certificate descriptor (logical-slot-copy-import-owner-existing-proof owner))
          (multiple-value-bind (global facts children phase)
              (logical-slot-copy-import-control-global-image context manifest)
            (let ((record
                    (%make-logical-slot-copy-import-control-state
                     :owner owner :context context :source source :source-check source-check :mark-started mark-started :plan plan
                     :certificate certificate :certificate-bytes certificate-bytes :certificate-facts certificate-facts
                     :evidence-bytes evidence-bytes :evidence-facts evidence-facts :manifest manifest
                     :expected expected :root (physical-image-context-root-fd context) :global global
                     :global-facts facts :global-children children :phase phase :active t
                     :child-observations
                     (when (eq phase :present)
                       (logical-slot-copy-check-committed-child
                        context evidence (logical-slot-copy-import-pending-plan-child-plan plan)
                        (logical-slot-copy-import-pending-plan-prefix plan) control :include-control nil)))))
              (setf (logical-slot-copy-import-control-state-seal record)
                    (logical-slot-copy-import-state-image (logical-slot-copy-import-control-image record)))
              record)))))))

(defun logical-slot-copy-import-control-point (point record)
  (logical-slot-copy-verification-test-point point (logical-slot-copy-import-control-state-context record)))

(defun logical-slot-copy-import-control-execute (record)
  ;; Unlink only the authenticated temporary, then sync its parent directory.
  ;; Sync even when absent: a previous unlink may have failed before its sync.
  (declare (notinline logical-slot-copy-unlinkat sb-posix:fsync))
  (let ((complete nil))
    (unwind-protect
         (progn
           (logical-slot-copy-import-control-check-namespace record)
           (logical-slot-copy-import-control-open-temp record)
           (logical-slot-copy-import-control-check-temp record)
           (logical-slot-copy-import-control-point :after-import-control-open record)
           (when (eq :present (logical-slot-copy-import-control-state-phase record))
             (logical-slot-copy-import-control-point :before-import-control-unlink record)
             (logical-slot-copy-import-control-check-proof record)
             (sb-sys:without-interrupts
               (funcall (logical-slot-copy-import-control-state-mark-started record))
               (unless (zerop (logical-slot-copy-unlinkat
                               (logical-slot-copy-import-control-state-global record)
                               "pg_wal_control.tmp" 0))
                 (error "cannot remove authenticated control temporary"))
               (setf (logical-slot-copy-import-control-state-phase record) :removed))
             (logical-slot-copy-import-control-point :after-import-control-unlink record))
           (logical-slot-copy-import-control-point :before-import-control-directory-sync record)
           (logical-slot-copy-import-control-check-proof record)
           (sb-sys:without-interrupts
             (funcall (logical-slot-copy-import-control-state-mark-started record))
             (sb-posix:fsync (logical-slot-copy-import-control-state-global record)))
           (logical-slot-copy-import-control-point :after-import-control-directory-sync record)
           (logical-slot-copy-import-control-point :before-import-control-final-proof record)
           (logical-slot-copy-import-control-check-proof record)
           (logical-slot-copy-import-control-close-temp record)
           (logical-slot-copy-import-control-point :after-import-control-close record)
           (logical-slot-copy-import-control-check-proof record)
           (logical-slot-copy-import-control-point :after-import-control-final-proof record)
           (logical-slot-copy-import-control-check-proof record)
           (setf complete t)
           (logical-slot-copy-import-control-state-phase record))
      (unless complete
        (setf (logical-slot-copy-import-control-state-failed record) t
              (physical-adoption-owner-active-p (logical-slot-copy-import-control-state-owner record)) nil))
      (logical-slot-copy-import-control-close-temp record))))

(defun logical-slot-copy-import-reconcile-parent-control-temp ()
  ;; Hold exclusion and the source lease through removal and the final proof.
  ;; Once mutation starts, any failure through scope exit fences recovery.
  (when *logical-slot-copy-import-control-states*
    (error "control temporary reconciliation is already active"))
  (logical-slot-copy-install-check-input-entry)
  (when *wal-ready* (error "control temporary reconciliation requires unopened storage"))
  (let ((record nil) (complete nil) (started nil))
    (unwind-protect
         (multiple-value-prog1
             (call-with-logical-slot-copy-install-exclusion
              (lambda ()
                (call-with-logical-slot-copy-import-input-kind
                 #'%make-logical-slot-copy-import-control-owner
                 (lambda (owner context)
                   (logical-slot-copy-import-control-fences context)
                   (let ((*logical-slot-copy-import-control-states* *logical-slot-copy-import-control-states*))
                     (unwind-protect
                          (call-with-logical-slot-copy-import-source
                           owner context
                           (lambda (source check)
                             (setf record (logical-slot-copy-import-control-new-state
                                           owner context source check (lambda () (setf started t))))
                             (push (cons owner record) *logical-slot-copy-import-control-states*)
                             (logical-slot-copy-import-control-execute record))
                           :final-check
                           (lambda () (logical-slot-copy-import-control-check-proof record :source nil)))
                       (when record (setf (logical-slot-copy-import-control-state-active record) nil))))))))
           (setf complete t))
      (when (and started (not complete))
        (setf *physical-recovery-failed-p* t)))))
