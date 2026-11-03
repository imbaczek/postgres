(in-package #:pg)

(defun physical-backup-lease-capture-active (owner)
  (with-live-operation
    (with-write-lock
      (with-wal-critical
        (physical-backup-lease-check-capture-owner owner))))
  (values))

(defun physical-backup-lease-copy-capture (target owner reservation)
  (with-write-lock
    (with-wal-critical
      (physical-backup-lease-check-capture-owner owner)))
  (let ((request (physical-backup-lease-capture-owner-request owner)))
    (physical-export-origin-capture-backup
     target (physical-bootstrap-copy-under-quiescence
             target (getf request :name) :lease-request request)
     reservation)))

(defun physical-backup-lease-publish-capture (owner reservation origin manifest digest)
  (with-live-operation
    (with-write-lock
      (with-wal-publication-fence (begin-publication)
        (let* ((record (physical-backup-lease-check-capture-owner owner))
               (request (getf record :request))
               (next (copy-list record)))
          (setf (getf next :phase) :ready
                (getf next :base) (list :checkpoint-start (physical-manifest-checkpoint-start manifest)
                                        :base (getf manifest :end) :manifest-digest digest)
                next (physical-backup-lease-transition record next))
          (multiple-value-bind (pin disk) (physical-source-retirement-pins (getf request :name))
            (declare (ignore disk))
            (unless (and pin (not (physical-source-pin-lost-p pin)))
              (wal-invalid-input "backup lease capture lost its retained source pin"))
            (physical-backup-lease-check-pin-record pin next)
            (physical-source-pin-check-wal pin))
          (physical-export-origin-install-under-lock reservation origin #'begin-publication)
          (physical-backup-lease-test-point :after-capture-origin next)
          (physical-backup-lease-publish-checked-under-lock record next #'begin-publication))))))

(defun physical-backup-lease-capture (directory request)
  (let ((request (physical-backup-lease-request-copy request)))
    (when (plusp *live-operation-depth*)
      (error "backup lease capture must start outside a live operation"))
    (call-with-invalidated-slot-cleanup
     (lambda ()
       (sb-thread:with-mutex (*reload-run-lock*)
         (call-with-physical-backup-lease-capture
          request
          (lambda (owner)
            (call-with-physical-export-origin-reservation
             (lambda (reservation)
               (let* ((target (with-live-operation
                                (prevent-recovery-primary-operation "leased native backup")
                                (physical-bootstrap-target directory)))
                      (manifest (with-live-operation-quiescence
                                  (physical-backup-lease-copy-capture target owner reservation))))
                 (physical-backup-lease-test-point :after-capture-copy
                                                   (physical-backup-lease-status request))
                 (physical-backup-lease-capture-active owner)
                 (multiple-value-bind (origin actual digest)
                     (physical-export-origin-verify-backup target manifest reservation)
                   (physical-backup-lease-test-point :after-capture-verification
                                                     (physical-backup-lease-status request))
                   (physical-backup-lease-publish-capture owner reservation origin actual digest))
                 manifest))))))))))
