(in-package #:pg)

(defun logical-slot-continuation-cold-zero-child-startup-p (gate ownership)
  (%call-with-logical-slot-continuation-cold-cleanup-inputs
   gate ownership
   (lambda (plan reservation check)
     (declare (ignore reservation))
     (funcall check)
     (not (null (logical-slot-continuation-cold-zero-child-plan-p plan))))))

(defun logical-slot-continuation-cold-empty-publish-scoped (gate ownership reservation)
  (let ((restored nil))
    (multiple-value-bind (token closed abandon)
        (call-with-logical-slot-continuation-cold-empty-handoff
         gate ownership
         (lambda (handoff actual check entries)
           (unless (eq reservation actual) (error "empty startup changed its cold reservation"))
           (call-with-logical-slot-startup-source
            gate ownership entries
            (lambda (source)
              (logical-slot-startup-restore source (wal-discover-segments)
                                            (postgres-current-timestamp) handoff)
              (setf restored *wal-replication-slots*)))
           (funcall check)))
      (values restored token closed abandon))))

(defun logical-slot-continuation-restore-empty-publication (gate ownership &optional stable-selector)
  (let ((reservation nil) (failure nil) (restored nil) (token nil)
                          (closed nil) (abandon nil) (complete nil))
    (sb-sys:without-interrupts
      (unwind-protect
           (sb-sys:with-local-interrupts
             (call-with-logical-slot-continuation-cold-context
              gate ownership
              (lambda (storage actual archive)
                (declare (ignore storage archive))
                (setf reservation actual)
                (sb-thread:with-mutex (*wal-lock*)
                  (setf failure
                        (logical-slot-continuation-cold-reservation-call actual :failure-cleanup)))
                (unless stable-selector
                  (logical-slot-continuation-restore-empty-startup gate ownership))
                (multiple-value-setq (restored token closed abandon)
                  (logical-slot-continuation-cold-empty-publish-scoped gate ownership reservation))))
             (logical-slot-startup-publication-notify gate ownership restored)
             (funcall closed)
             (sb-sys:without-interrupts
               (sb-thread:with-mutex (*wal-lock*)
                 (logical-slot-continuation-cold-empty-call token :before-retirement)
                 (logical-slot-continuation-cold-reservation-call reservation :retire-empty token)
                 (setf complete t)))
             restored)
        (unless complete
          (when abandon (funcall abandon))
          (when failure
            (if (sb-thread:holding-mutex-p *wal-lock*) (funcall failure)
                (sb-thread:with-mutex (*wal-lock*) (funcall failure)))))))))
