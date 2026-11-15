(in-package #:pg)

(defvar *logical-slot-startup-publication-test-hook* nil)

(defun logical-slot-startup-publication-test-point (point owner token)
  (when *logical-slot-startup-publication-test-hook*
    (let ((binding (gethash token *logical-slot-startup-publication-tokens*)))
      (funcall *logical-slot-startup-publication-test-hook* point owner token (svref binding 1)))))

(defun logical-slot-startup-publication-derive (owner gate ownership read-slot check)
  ;; Under the owned startup gate, prepare and prove the replacement before
  ;; publishing it.  This library does not open admission.
  (multiple-value-bind (restored image)
      (call-with-logical-slot-imported-startup-source
       gate ownership (logical-slot-startup-read-inventory :local-workspace t)
       (lambda (source)
         (let* ((candidates (mapcar #'logical-slot-startup-entry-slot
                                    (logical-slot-startup-source-entries source)))
                (restored
                  (logical-slot-prepare-startup-inventory
                   candidates (wal-discover-segments) (postgres-current-timestamp)
                   (lambda (slot)
                     (when (eq :reserved (wal-replication-slot-wal-status slot))
                       (funcall read-slot (wal-replication-slot-name slot)
                                (lambda (cursor seams)
                                  (declare (ignore seams))
                                  (setf (wal-replication-slot-pending-xids slot)
                                        (logical-slot-startup-derive-pending-with-cursor source slot cursor)))))
                     slot))))
           (funcall check)
           (logical-slot-startup-check-inventory source)
           (values restored (logical-slot-publication-registry-image restored)))))
    ;; The source scope has exited before a token can be registered.  Its
    ;; private EQ binding holds owner, registry, image and phase in slots 0
    ;; through 3.  Slot 4 starts NIL and receives the selector and permanent
    ;; proof binding after the final proof.
    (logical-slot-publication-check-registry restored image)
    (logical-slot-imported-host-startup-check-owner owner)
    (let ((token (%make-logical-slot-startup-publication-token)))
      (setf (gethash token *logical-slot-startup-publication-tokens*)
            (vector owner restored image :derived nil))
      (logical-slot-startup-publication-test-point :derived owner token)
      token)))

(defun logical-slot-startup-publication-finish (owner storage token snapshot check guard)
  (let* ((binding (logical-slot-startup-publication-token-binding owner token))
         (restored (svref binding 1)))
    (setf (svref binding 4)
          (list (logical-slot-continuation-encode snapshot)
                (logical-slot-continuation-storage-call storage :binding)))
    (logical-slot-startup-publication-call owner :prepare token)
    (logical-slot-startup-publication-test-point :prepared owner token)
    ;; Recheck the source while its leases and file descriptors are held.
    ;; The guard repeats the under-lock checks immediately before commit.
    (funcall check)
    (sb-thread:with-mutex (*wal-lock*)
      (logical-slot-startup-publication-test-point :before-commit owner token)
      (funcall guard)
      (logical-slot-startup-publication-call owner :commit token))
    (logical-slot-startup-publication-test-point :after-commit owner token)
    restored))

(defun call-with-logical-slot-startup-publication-storage (owner function)
  (call-with-physical-directory-root
   owner
   (lambda (root path)
     (declare (ignore path))
     (call-with-physical-image-owned-context
      root t
      (lambda (context)
        (call-with-logical-slot-continuation-startup-storage-context owner context function))))))

(defun logical-slot-startup-publication-notify (gate ownership restored)
  (check-primary-storage-startup-context gate ownership)
  (sb-thread:with-mutex (*wal-lock*)
    (unless (eq restored *wal-replication-slots*)
      (error "startup publication registry changed before writer notification"))
    (when (or *wal-logical-info-disable-pending*
              (and *wal-logical-policy* (wal-logical-policy-pending-disable *wal-logical-policy*)))
      (notify-primary-wal-writer-under-lock)))
  restored)

(defun logical-slot-continuation-publish-startup (gate ownership)
  (logical-slot-continuation-prepare-startup-directory gate ownership)
  (let ((receipt (vector nil)))
    (sb-sys:without-interrupts
      (let ((complete nil))
        (unwind-protect
             (multiple-value-prog1
                 (sb-sys:with-local-interrupts
                   (let ((restored
                           (call-with-logical-slot-startup-publication-owner
                            gate ownership
                            (lambda (owner)
                              (call-with-logical-slot-startup-publication-storage
                               owner
                               (lambda (storage facts)
                                 (call-with-logical-slot-continuation-storage-readers
                                  storage facts
                                  (lambda (read-slot check)
                                    (logical-slot-startup-publication-derive
                                     owner gate ownership read-slot check))
                                  (lambda (token snapshot check guard)
                                    (logical-slot-startup-publication-finish
                                     owner storage token snapshot check guard))))))
                            receipt)))
                     ;; All source scopes have exited before notifying the writer.
                     (logical-slot-startup-publication-notify gate ownership restored)))
               (setf complete t))
          (unless complete
            (when (svref receipt 0) (logical-slot-publication-fence (svref receipt 0)))))))))
