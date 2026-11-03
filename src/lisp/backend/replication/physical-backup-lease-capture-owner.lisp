(in-package #:pg)

(defstruct physical-backup-lease-capture-owner request backend thread namespace incarnation)

(defun physical-backup-lease-status-under-lock (request)
  (physical-backup-lease-require-locks)
  (physical-backup-lease-check-source request)
  (when (physical-backup-lease-record-under-lock (getf request :name))
    (physical-backup-lease-current-record request)))

(defun physical-backup-lease-status (request)
  (let ((request (physical-backup-lease-request-copy request)))
    (with-live-operation
      (with-write-lock
        (with-wal-critical
          (let ((record (physical-backup-lease-status-under-lock request)))
            (when record (physical-backup-lease-copy record))))))))

(defun physical-backup-lease-claim-capture (request)
  (with-live-operation
    (with-write-lock
      (with-wal-publication-fence (begin-publication)
        (let* ((name (getf request :name))
               (record (physical-backup-lease-status-under-lock request)))
          (when record
            (return-from physical-backup-lease-claim-capture
              (values (physical-backup-lease-copy record) nil)))
          (when (gethash name *physical-backup-lease-capture-owners*)
            (error "backup lease has an owner without a request"))
          (unless *backend* (wal-invalid-input "backup lease capture requires a backend"))
          (let* ((requested (list :version 1 :request request :phase :requested
                                  :base nil :outcome nil :successor nil))
                 (owner (make-physical-backup-lease-capture-owner
                         :request (physical-backup-lease-request-copy request)
                         :backend *backend* :thread sb-thread:*current-thread*
                         :namespace *wal-segment-namespace* :incarnation *wal-storage-incarnation*))
                 (published (physical-backup-lease-publish-checked-under-lock
                             nil (physical-backup-lease-transition nil requested) #'begin-publication)))
            (setf (gethash (copy-seq name) *physical-backup-lease-capture-owners*) owner)
            (values published owner)))))))

(defun physical-backup-lease-check-capture-owner (owner &optional (phases '(:capturing)))
  (physical-backup-lease-require-locks)
  (unless (and (physical-backup-lease-capture-owner-p owner)
               (eq owner *physical-backup-lease-capture-owner*)
               (eq *backend* (physical-backup-lease-capture-owner-backend owner))
               (eq sb-thread:*current-thread* (physical-backup-lease-capture-owner-thread owner))
               (eq *wal-segment-namespace* (physical-backup-lease-capture-owner-namespace owner))
               (eq *wal-storage-incarnation* (physical-backup-lease-capture-owner-incarnation owner)))
    (wal-invalid-input "backup lease capture owner changed"))
  (let* ((request (physical-backup-lease-capture-owner-request owner))
         (record (physical-backup-lease-current-record request)))
    (unless (and (eq owner (gethash (getf request :name) *physical-backup-lease-capture-owners*))
                 (member (getf record :phase) phases))
      (wal-invalid-input "backup lease capture is no longer active"))
    (physical-backup-lease-check-source request :current-timeline t)
    record))

(defun physical-backup-lease-start-capture (owner)
  (with-live-operation
    (with-write-lock
      (with-wal-publication-fence (begin-publication)
        (let* ((record (physical-backup-lease-check-capture-owner owner '(:requested)))
               (next (copy-list record)))
          (setf (getf next :phase) :capturing)
          (physical-backup-lease-publish-checked-under-lock
           record (physical-backup-lease-transition record next) #'begin-publication))))))

(defun call-with-physical-backup-lease-capture (request function)
  (let ((request (physical-backup-lease-request-copy request)))
    (when (plusp *live-operation-depth*)
      (error "backup lease capture must start outside a live operation"))
    (sb-sys:without-interrupts
      (multiple-value-bind (record owner) (physical-backup-lease-claim-capture request)
        (unless owner
          (return-from call-with-physical-backup-lease-capture (values record nil)))
        (let ((*physical-backup-lease-capture-owner* owner))
          (labels
              ((finish-owner ()
                 (with-live-operation
                   (with-write-lock
                     (with-wal-publication-fence (begin-publication)
                       (let ((name (getf request :name)))
                         (unless (and (eq sb-thread:*current-thread*
                                          (physical-backup-lease-capture-owner-thread owner))
                                      (eq owner (gethash name *physical-backup-lease-capture-owners*)))
                           (error "backup lease cleanup lost its capture owner"))
                         (unwind-protect
                              (let ((current (physical-backup-lease-current-record request)))
                                (case (getf current :phase)
                                  ((:requested :capturing)
                                   (let ((next (copy-list current)))
                                     (setf (getf next :phase) :interrupted)
                                     (physical-backup-lease-publish-checked-under-lock
                                      current (physical-backup-lease-transition current next)
                                      #'begin-publication)))
                                  (:closing
                                   (remhash name *physical-backup-lease-capture-owners*)
                                   (physical-backup-lease-finish-closing-under-lock
                                    request #'begin-publication))))
                           (remhash name *physical-backup-lease-capture-owners*)))))))
               (release-owner ()
                 (with-write-lock
                   (sb-thread:with-mutex (*wal-lock*)
                     (let ((name (getf request :name)))
                       (when (eq owner (gethash name *physical-backup-lease-capture-owners*))
                         (remhash name *physical-backup-lease-capture-owners*)))))))
            (unwind-protect
                 (sb-sys:with-local-interrupts
                   (physical-backup-lease-test-point :after-capture-claim record)
                   (physical-backup-lease-start-capture owner)
                   (values (funcall function owner) t))
              (unwind-protect (finish-owner)
                (release-owner)))))))))

(defun physical-backup-lease-abort (request)
  (let ((request (physical-backup-lease-request-copy request)))
    (with-live-operation
      (with-write-lock
        (with-wal-publication-fence (begin-publication)
          (let* ((record (physical-backup-lease-status-under-lock request))
                 (phase (getf record :phase)))
            (when (member phase '(:closing :closed))
              (unless (eq :aborted (getf record :outcome))
                (wal-invalid-input "backup lease already has a different terminal outcome")))
            (unless (member phase '(:closing :closed))
              (let ((next (if record (copy-list record)
                              (list :version 1 :request request :base nil :successor nil))))
                (setf (getf next :phase) :closing (getf next :outcome) :aborted
                      record (physical-backup-lease-publish-checked-under-lock
                              record (physical-backup-lease-transition record next) #'begin-publication))))
            (if (gethash (getf request :name) *physical-backup-lease-capture-owners*)
                (physical-backup-lease-copy record)
                (physical-backup-lease-finish-closing-under-lock request #'begin-publication))))))))
