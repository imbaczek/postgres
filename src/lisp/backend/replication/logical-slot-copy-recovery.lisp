(in-package #:pg)

(defun logical-slot-copy-check-observed-bytes (observations name bytes &optional facts)
  "Compare byte size, digest and optional file facts with their recorded observation."
  (let ((row (find name observations :key #'first :test #'equal)))
    (unless (and row (= (length bytes) (second row))
                 (= (physical-store-update-digest #xCBF29CE484222325 bytes) (third row))
                 (or (null facts) (equal facts (fourth row))))
      (error "logical copy recovery metadata changed while binding its input"))))

(defun logical-slot-copy-recovery-observations (owner context manifest descriptor)
  "Observe immutable base files, input metadata and WAL sidecar pieces for rechecking."
  (logical-slot-copy-check-input owner context)
  (logical-slot-copy-input-sidecar-inventory context descriptor)
  (let ((observations nil) (index (make-hash-table :test #'equal)))
    (dolist (row (getf manifest :files))
      (when (physical-recovery-immutable-base-file-p (first row))
        (let ((actual (logical-slot-copy-file-row context (first row))))
          (unless (equal row (subseq actual 0 3))
            (error "logical copy immutable recovery file differs from its base manifest"))
          (push actual observations))))
    (dolist (name '("physical-backup.manifest" "logical-slot-copy.descriptor"
                    "logical-slot-copy.base-verified" "recovery.required" "physical-recovery.control"
                    "pg_hba.conf" "pg_ident.conf" "authentication.initialized"))
      (push (logical-slot-copy-file-row context name) observations))
    (logical-slot-copy-observe-pieces context descriptor observations index)))

(defun logical-slot-copy-check-recovery-slots (owner context system-id)
  "Require local slot images to be physical and match the source system."
  (slot-state-map
   (merge-pathnames "pg_replslot/" (physical-adoption-owner-directory owner))
   (lambda (bytes name)
     (let ((slot (wal-decode-replication-slot-image bytes name)))
       (unless (and (wal-physical-slot-p slot) (physical-slot-state-valid-p slot)
                    (eql system-id (wal-replication-slot-system-id slot)))
         (error "logical copy recovery requires source logical slots to remain descriptor-only"))))
   :cleanup nil :stabilize nil :parent-fd (physical-image-context-root-fd context)))

(defun logical-slot-copy-check-bound-receipt
    (owner context proof-bytes allowed-phases verify-state)
  "Bind receipt and observed input bytes under OWNER and CONTEXT.
Require an allowed phase, call VERIFY-STATE, then recheck the observations."
  (logical-slot-copy-check-input owner context)
  (let* ((root (physical-image-context-root-path context))
         (proof (logical-slot-copy-base-proof-decode proof-bytes))
         (control-bytes (logical-slot-copy-read-file context "physical-recovery.control"
                                                     +physical-recovery-control-maximum-bytes+))
         (control (physical-recovery-decode-control control-bytes)))
    (unless (member (getf control :phase) allowed-phases)
      (error "logical copy recovery requires child-owner import before promotion"))
    (when (or (slot-state-lstat (merge-pathnames "logical-slot-copy.in-progress" root))
              (slot-state-lstat (merge-pathnames "logical-slot-copy.base-verified.tmp" root)))
      (error "unfinished logical copy recovery proof cannot start"))
    (multiple-value-bind (manifest digest) (recovery-read-manifest root)
      (multiple-value-bind (bytes facts)
          (logical-slot-copy-read-file context "logical-slot-copy.descriptor"
                                       +logical-slot-copy-descriptor-maximum-bytes+)
        (let ((descriptor (logical-slot-copy-descriptor-decode bytes)))
          (logical-slot-copy-check-binding descriptor manifest digest)
          (unless (and (equal proof (logical-slot-copy-base-proof-value context manifest descriptor))
                       (equalp bytes (logical-slot-copy-descriptor-encode descriptor))
                       (equalp (logical-slot-copy-descriptor-base-control descriptor)
                               (logical-slot-copy-read-file context "global/pg_wal_control"
                                                            (1+ +wal-control-maximum-bytes+)))
                       (equalp (physical-recovery-marker-bytes)
                               (logical-slot-copy-read-file context "recovery.required" 4096)))
            (error "logical copy recovery receipt differs from its bound base"))
          (multiple-value-bind (observations index)
              (logical-slot-copy-recovery-observations owner context manifest descriptor)
            (declare (ignore index))
            (unless (= digest (third (find "physical-backup.manifest" observations :key #'first :test #'equal)))
              (error "logical copy recovery manifest changed"))
            (logical-slot-copy-check-observed-bytes observations "logical-slot-copy.descriptor" bytes facts)
            (logical-slot-copy-check-observed-bytes observations "logical-slot-copy.base-verified" proof-bytes)
            (logical-slot-copy-check-observed-bytes observations "physical-recovery.control" control-bytes)
            (logical-slot-copy-input-checkpoint context manifest descriptor)
            (logical-slot-copy-check-recovery-slots owner context (getf manifest :system-id))
            (let ((*pgdata* root) (*physical-image-context* context))
              (funcall verify-state control))
            (logical-slot-copy-input-check-observations owner context observations)
            (dolist (row observations)
              (unless (equal row (logical-slot-copy-file-row context (first row)))
                (error "logical copy recovery input failed final byte verification")))
            (logical-slot-copy-input-sidecar-inventory context descriptor)
            (logical-slot-copy-check-input owner context)
            (values descriptor observations manifest control)))))))

(defun logical-slot-copy-check-recovery-receipt (owner context)
  "Check the startup receipt using the cold startup phase allowlist."
  (unless (and (logical-slot-copy-startup-owner-p owner)
               (logical-slot-copy-startup-owner-existing-proof owner))
    (error "logical copy recovery requires base verification receipt"))
  (values
   (logical-slot-copy-check-bound-receipt
    owner context (logical-slot-copy-startup-owner-existing-proof owner)
    '(0 1 2)
    (lambda (control)
      (declare (ignore control))
      (physical-recovery-verify-base-state)))))

(defun logical-slot-copy-verify-recovery-startup ()
  "Verify the receipt within the guarded cold startup input scope."
  (call-with-logical-slot-copy-startup-input #'logical-slot-copy-check-recovery-receipt))
