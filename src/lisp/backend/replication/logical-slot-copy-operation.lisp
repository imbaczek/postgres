(in-package #:pg)

(defvar *logical-slot-copy-test-hook* nil)

(defun logical-slot-copy-test-point (point context)
  (when *logical-slot-copy-test-hook* (funcall *logical-slot-copy-test-hook* point context)))

(defun logical-slot-copy-authentication-files (policy)
  (list (list "pg_hba.conf" (string->bytes (authentication-policy-hba-text policy)))
        (list "pg_ident.conf" (string->bytes (authentication-policy-ident-text policy)))
        (list "authentication.initialized" (string->bytes *authentication-marker-text*))))

(defun logical-slot-copy-check-authentication (policy files)
  (unless (and (eq policy (authentication-capture-policy))
               (equalp files (logical-slot-copy-authentication-files policy)))
    (error "logical copy installed authentication generation changed"))
  t)

(defun logical-slot-copy-build-base (capture context copy-id pin authentication)
  (let* ((facts (logical-slot-copy-capture-expected capture))
         (source (logical-slot-copy-capture-context capture))
         (derived (physical-bootstrap-derived-wal-image))
         (base-control (or derived (copy-seq (getf facts :control))))
         (recovery-control
           (physical-recovery-control-image
            0 (getf facts :system-id) (getf facts :timeline) 0
            (getf facts :checkpoint) (getf facts :end) (getf facts :next-xid)
            :resource-fact (wal-copy-control-resource-fields *wal-checkpoint-snapshot*)))
         (metadata (append (list (list "recovery.required" (physical-recovery-marker-bytes))
                                 (list "physical-recovery.control" recovery-control))
                           authentication))
         (files nil))
    (dolist (entry metadata)
      (logical-slot-copy-write-bytes context (first entry) (second entry)))
    (dolist (row (getf facts :files))
      (logical-slot-copy-check-capture capture)
      (push (if (equal (first row) "global/pg_wal_control")
                (logical-slot-copy-write-bytes context (first row) base-control)
                (physical-image-copy-file source context (first row) (second row) (third row)
                                          :source-size (sixth (fourth row))))
            files))
    (let* ((manifest
             (list :version 3 :system-id (getf facts :system-id)
                   :cluster-identity (copy-seq (getf facts :cluster-identity))
                   :timeline (getf facts :timeline) :checkpoint (getf facts :checkpoint)
                   :checkpoint-start (getf facts :checkpoint-start) :end (getf facts :end)
                   :next-xid (getf facts :next-xid) :source-pin (copy-seq (physical-source-pin-name pin))
                   :files (nreverse files) :auth-policy-required 1 :logical-slot-copy-id copy-id))
           (image (physical-bootstrap-manifest-image manifest))
           (digest (physical-store-update-digest #xCBF29CE484222325 image)))
      (recovery-check-manifest-schema manifest)
      (physical-source-pin-bind-manifest pin digest)
      (logical-slot-copy-write-bytes context "physical-backup.manifest" image)
      (values manifest digest base-control recovery-control
              (append metadata
                      (list (list "physical-backup.manifest" image)
                            (list "logical-slot-copy.in-progress"
                                  (string->bytes (format nil "PGLOGICALCOPY ~a~%" copy-id)))))))))

(defun logical-slot-copy-finalize
    (capture owner context policy authentication manifest metadata descriptor wal-state)
  (let* ((image (logical-slot-copy-descriptor-encode descriptor))
         (digest (logical-slot-copy-descriptor-manifest-digest descriptor))
         (decoded (logical-slot-copy-descriptor-decode image))
         (observations nil))
    (logical-slot-copy-check-binding decoded manifest digest)
    (logical-slot-copy-check-capture capture)
    (logical-slot-copy-check-authentication policy authentication)
    (setf observations (logical-slot-copy-check-readback owner context manifest metadata descriptor nil))
    (logical-slot-copy-test-point :before-descriptor context)
    (logical-slot-copy-write-bytes context "logical-slot-copy.descriptor" image)
    (unless (equalp image (logical-slot-copy-read-file
                           context "logical-slot-copy.descriptor"
                           +logical-slot-copy-descriptor-maximum-bytes+))
      (error "logical copy descriptor readback changed"))
    (push (logical-slot-copy-file-row context "logical-slot-copy.descriptor") observations)
    (logical-slot-copy-test-point :after-descriptor context)
    (logical-slot-copy-test-point :before-marker-retirement context)
    (logical-slot-copy-check-capture capture)
    (logical-slot-copy-check-authentication policy authentication)
    (maphash (lambda (key info) (declare (ignore key)) (logical-slot-copy-check-piece-source wal-state info))
             (logical-slot-copy-wal-state-pieces wal-state))
    (let ((archive (logical-slot-copy-wal-state-archive wal-state)))
      (when archive (sb-thread:with-mutex (*wal-lock*) (logical-wal-archive-check archive))))
    (logical-slot-copy-target-inventory
     owner context (getf manifest :files) (logical-slot-copy-descriptor-wal-pieces descriptor) t)
    (logical-slot-copy-check-observations owner context observations)
    (logical-slot-copy-retire-marker owner context (logical-slot-copy-descriptor-copy-id descriptor))
    (values manifest descriptor)))

(defun logical-slot-copy-into-target (capture directory pin-name policy authentication archive)
  (call-with-logical-slot-copy-target
   directory
   (lambda (owner context copy-id)
     (let ((pin (physical-source-pin-create pin-name)))
       (logical-slot-copy-open-sidecar owner context)
       (multiple-value-bind (manifest digest base-control recovery-control metadata)
           (logical-slot-copy-build-base capture context copy-id pin authentication)
         (logical-slot-copy-test-point :after-base-copy context)
         (multiple-value-bind (pieces state) (logical-slot-copy-transfer-wal capture owner context archive)
           (logical-slot-copy-test-point :after-wal-copy context)
           (let* ((facts (logical-slot-copy-capture-expected capture))
                  (descriptor (make-logical-slot-copy-descriptor
                               :copy-id copy-id :manifest-digest digest
                               :source-control (copy-seq (getf facts :control))
                               :base-control base-control :recovery-control recovery-control
                               :history (copy-seq (getf facts :history))
                               :slots (logical-slot-copy-clone-facts (getf facts :slots))
                               :wal-pieces pieces)))
             (logical-slot-copy-finalize
              capture owner context policy authentication manifest metadata descriptor state))))))))

(defun logical-slot-base-copy (directory pin-name)
  (unless (and (stringp directory) (plusp (length directory))
               (wal-valid-replication-slot-name-p pin-name))
    (wal-invalid-input "logical slot base copy requires a directory and private pin name"))
  (let ((target (namestring (physical-bootstrap-target directory))))
    (call-with-logical-slot-copy-writer-paused
     (lambda (gate ownership)
       (sb-thread:with-mutex (*wal-lock*)
         (unless (<= 1 (loop for slot being the hash-values of *wal-replication-slots*
                             count (wal-logical-slot-p slot))
                     +logical-slot-copy-descriptor-maximum-slots+)
           (error "logical slot base copy requires a nonempty bounded logical slot inventory")))
       (let* ((policy (authentication-capture-policy))
              (authentication (logical-slot-copy-authentication-files policy)))
         (wal-checkpoint-core)
         (physical-recovery-check-checkpoint-resources *wal-checkpoint-snapshot* *wal-control-metadata*)
         (call-with-logical-slot-copy-source-directory
          gate ownership
          (lambda (context)
            (call-with-logical-slot-copy-capture
             gate ownership context
             (lambda (capture)
               (let* ((facts (logical-slot-copy-capture-expected capture))
                      (history (timeline-check-lineage
                                (timeline-parse-history (getf facts :timeline)
                                                        (bytes->string (getf facts :history))))))
                 (unless (<= 1 (length (getf facts :slots)) +logical-slot-copy-descriptor-maximum-slots+)
                   (error "logical slot base copy requires a nonempty bounded logical slot inventory"))
                 (dolist (entry (logical-slot-startup-source-entries
                                 (logical-slot-copy-capture-source capture)))
                   (let ((slot (logical-slot-startup-entry-slot entry)))
                     (when (and (wal-logical-slot-p slot)
                                (eq :reserved (wal-replication-slot-wal-status slot)))
                       (logical-slot-copy-captured-start capture slot history))))
                 (call-with-optional-logical-wal-archive
                  (getf facts :system-id) history (getf facts :namespace)
                  (lambda () (logical-slot-copy-check-captured-source-under-lock capture))
                  (lambda (archive)
                    (logical-slot-copy-into-target
                     capture target pin-name policy authentication archive)))))))
          :prepare-pins t))))))
