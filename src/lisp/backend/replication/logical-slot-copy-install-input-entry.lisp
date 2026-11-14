(in-package #:pg)

(defun logical-slot-copy-install-new-input-state (operation image origin &optional handoff)
  (logical-slot-copy-install-operation-record operation)
  (%make-logical-slot-copy-install-input-state
   :operation operation :image (copy-seq image) :origin origin :handoff handoff
   :registry *wal-replication-slots*
   :shutdown-lock (when (eq :live (logical-slot-copy-install-operation-state-mode
                                   (logical-slot-copy-install-operation-record operation)))
                    *physical-shutdown-lock*)
   :engine (logical-slot-copy-import-state-image *physical-recovery-state*)))

(defun call-with-logical-slot-copy-install-input (operation handoff function)
  (when *logical-slot-copy-install-inputs* (error "logical installation input is already active"))
  (let ((image (logical-slot-copy-install-handoff-image operation handoff)))
    (logical-slot-copy-install-check-input-lifecycle operation)
    (call-with-logical-slot-copy-install-input-state
     (logical-slot-copy-install-new-input-state operation image :fresh handoff) function)))

(defun logical-slot-copy-install-fixed-image (operation)
  (call-with-logical-slot-copy-install-inspection
   operation
   (lambda (root)
     (let* ((container (logical-slot-copy-install-inspection-open
                        root "logical-slot-copy.install" :directory root))
            (journal (logical-slot-copy-install-inspection-open container "journal" :file root))
            (bytes (logical-slot-copy-install-inspection-read
                    journal +logical-slot-copy-install-maximum-bytes+)))
       (logical-slot-copy-install-decode bytes)
       bytes))))

(defun call-with-logical-slot-copy-install-cold-input-under-locks (function)
  (when *logical-slot-copy-install-inputs* (error "logical installation input is already active"))
  (when *wal-ready* (error "fixed installation input requires cold startup"))
  (call-with-logical-slot-copy-install-operation-scope
   (lambda (operation mark-started)
     (let* ((outer (logical-slot-copy-install-operation-record operation))
            (fd (logical-slot-copy-install-operation-state-root-fd outer))
            (present (handler-case
                         (slot-state-lstat (slot-state-fd-path fd "logical-slot-copy.install"))
                       (error (condition) (funcall mark-started) (error condition)))))
       (when present
         (funcall mark-started)
         (call-with-logical-slot-copy-install-input-state
          (logical-slot-copy-install-new-input-state
           operation (logical-slot-copy-install-fixed-image operation) :cold)
          function))))))

(defun logical-slot-copy-install-check-input-entry ()
  (when (or *logical-slot-copy-install-inputs* *logical-slot-copy-install-operations*)
    (error "logical installation input is already active"))
  (unless (and (zerop *live-operation-depth*)
               (not (sb-thread:holding-mutex-p *physical-follow-command-lock*))
               (not (sb-thread:holding-mutex-p *reload-run-lock*))
               (not (sb-thread:holding-mutex-p *physical-follow-generation-lock*)))
    (error "logical installation input requires an outer lifecycle entry")))

(defun call-with-logical-slot-copy-install-fresh-input (function)
  (logical-slot-copy-install-check-input-entry)
  (when (sb-thread:holding-mutex-p *physical-shutdown-lock*)
    (error "fresh installation input requires an outer lifecycle entry"))
  (physical-follow-call-with-controller-blocked
   (lambda ()
     (sb-thread:with-mutex (*physical-shutdown-lock*)
       (call-with-logical-slot-copy-install-handoff
        (lambda (operation handoff)
          (call-with-logical-slot-copy-install-input operation handoff function)))))))

(defun call-with-logical-slot-copy-install-cold-input (function)
  (logical-slot-copy-install-check-input-entry)
  (when *wal-ready* (error "fixed installation input requires cold startup"))
  (call-with-logical-slot-copy-install-exclusion
   (lambda () (call-with-logical-slot-copy-install-cold-input-under-locks function))))
