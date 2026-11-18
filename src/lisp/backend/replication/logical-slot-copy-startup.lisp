(in-package #:pg)

(defun logical-slot-copy-imported-startup-candidate-p ()
  ;; This names-only observation grants no startup or source authority.
  ;; Historical copy evidence is checked later under real PGDATA ownership.
  (and
   (every (lambda (name)
            (null (slot-state-lstat (merge-pathnames name *pgdata*))))
          '("logical-slot-copy.import.tmp" "logical-slot-copy.source-proof.tmp"
            "logical-slot-copy.evidence.tmp" "logical-slot-copy.base-verified.tmp"
            "logical-slot-copy.in-progress"))
   (loop for (name directory) in '(("logical-slot-copy.source-proof" nil)
                                   ("logical-slot-copy.import" nil)
                                   ("logical-slot-copy.install" t))
         for facts = (slot-state-lstat (merge-pathnames name *pgdata*))
         always (and facts
                     (if directory
                         (sb-posix:s-isdir (sb-posix:stat-mode facts))
                         (sb-posix:s-isreg (sb-posix:stat-mode facts)))))))

(defun logical-slot-copy-preflight-startup ()
  ;; Neither a source proof nor its temporary file grants startup.  Import
  ;; artifacts must be reconciled by the child owner first.
  (dolist (name '("logical-slot-copy.install" "logical-slot-copy.import" "logical-slot-copy.import.tmp"
                  "logical-slot-copy.evidence" "logical-slot-copy.evidence.tmp"
                  "logical-slot-copy.source-proof" "logical-slot-copy.source-proof.tmp"))
    (when (slot-state-lstat (merge-pathnames name *pgdata*))
      (error "logical copy requires child-owner import before promotion; import artifacts require reconciliation")))
  (when (slot-state-lstat (merge-pathnames "logical-slot-copy.in-progress" *pgdata*))
    (error "unfinished logical slot copy cannot start"))
  (when (slot-state-lstat (merge-pathnames "logical-slot-copy.base-verified.tmp" *pgdata*))
    (error "logical copy base verification receipt requires child-owner import before startup"))
  (let* ((descriptor-path (merge-pathnames "logical-slot-copy.descriptor" *pgdata*))
         (descriptor-p (slot-state-lstat descriptor-path))
         (proof-p (slot-state-lstat (merge-pathnames "logical-slot-copy.base-verified" *pgdata*)))
         (sidecar (slot-state-lstat (merge-pathnames "logical_slot_wal" *pgdata*))))
    (multiple-value-bind (manifest digest)
        (when (slot-state-lstat (merge-pathnames "physical-backup.manifest" *pgdata*))
          (recovery-read-manifest *pgdata*))
      (let ((copy-p (and manifest (get-properties manifest '(:logical-slot-copy-id)))))
        (when (or copy-p descriptor-p proof-p sidecar)
          (when (and proof-p (not (and copy-p descriptor-p)))
            (logical-slot-copy-check-base-proof-fence))
          (unless (and copy-p descriptor-p sidecar)
            (error "logical copy manifest and descriptor must both be present with their sidecar"))
          (unless (sb-posix:s-isdir (sb-posix:stat-mode sidecar))
            (error "logical copy WAL sidecar is not a directory"))
          (physical-directory-check-reseed-intent *pgdata*)
          (physical-adoption-check-startup)
          (let* ((descriptor (logical-slot-copy-descriptor-decode
                              (read-bounded-regular-file
                               descriptor-path +logical-slot-copy-descriptor-maximum-bytes+)))
                 (bytes (recovery-read-bytes (physical-recovery-control-path)
                                             :maximum +physical-recovery-control-maximum-bytes+))
                 (control (physical-recovery-decode-control bytes)))
            (logical-slot-copy-check-binding descriptor manifest digest)
            (unless (member (getf control :phase) '(0 1 2))
              (error "logical copy requires child-owner import before promotion"))
            (unless (equalp (logical-slot-copy-descriptor-base-control descriptor)
                            (recovery-read-bytes (wal-control-path) :maximum (1+ +wal-control-maximum-bytes+)))
              (error "logical copy base control changed without child-owner import"))
            (if (zerop (getf control :phase))
                (unless (equalp bytes (logical-slot-copy-descriptor-recovery-control descriptor))
                  (error "logical copy initial recovery control changed"))
                (unless proof-p (error "logical copy recovery requires base verification receipt")))
            (when proof-p
              (logical-slot-copy-base-proof-decode
               (read-bounded-regular-file (merge-pathnames "logical-slot-copy.base-verified" *pgdata*)
                                          +logical-slot-copy-base-proof-maximum-bytes+)))
            (values manifest descriptor control)))))))

(defun logical-slot-copy-retire-startup-scratch (directory fd)
  (labels ((check-root ()
             (let ((held (physical-image-check-directory fd))
                   (named (slot-state-lstat directory)))
               (unless (and named (sb-posix:s-isdir (sb-posix:stat-mode named))
                            (= (sb-posix:stat-dev held) (sb-posix:stat-dev named))
                            (= (sb-posix:stat-ino held) (sb-posix:stat-ino named))
                            (= (sb-posix:geteuid) (sb-posix:stat-uid named))
                            (= #o700 (logand #o7777 (sb-posix:stat-mode named))))
                 (error "logical copy verification scratch changed before retirement")))))
    (check-root)
    (unless (every (lambda (name) (equal ".pg_lisp.lock" name))
                   (directory-entry-names (physical-image-fd-path fd)
                                          :follow-symlinks t :maximum-entries 2))
      (error "logical copy verification scratch has unresolved cleanup"))
    (call-with-physical-image-input-at
     fd ".pg_lisp.lock"
     (lambda (lock)
       (when lock
         (let ((lock-fd (sb-sys:fd-stream-fd lock)))
           (unless (and (zerop (sb-posix:stat-size (sb-posix:fstat lock-fd)))
                        (zerop (physical-store-flock lock-fd 6))
                        (equal (physical-image-file-facts lock-fd)
                               (physical-image-file-facts fd ".pg_lisp.lock")))
             (error "logical copy verification scratch lock is still owned or changed"))
           (check-root)
           (unless (zerop (logical-slot-copy-unlinkat fd ".pg_lisp.lock" 0))
             (error "cannot retire logical copy verification scratch lock")))))
     :element-type '(unsigned-byte 8) :if-does-not-exist nil)
    (check-root)
    (sb-posix:rmdir (namestring directory))))

(defun call-with-logical-slot-copy-startup-scratch (function)
  (let* ((parent (uiop:ensure-directory-pathname (truename (uiop:temporary-directory))))
         (target (uiop:ensure-directory-pathname (truename *pgdata*))))
    (when (uiop:subpathp parent target)
      (error "logical copy verification scratch parent must be outside PGDATA"))
    (let* ((directory (uiop:ensure-directory-pathname
                       (sb-posix:mkdtemp (namestring (merge-pathnames "pg-copy-verify-XXXXXX" parent)))))
           (fd nil) (completed nil))
      (unwind-protect
           (progn
             (sb-sys:without-interrupts (setf fd (physical-backup-open-directory directory)))
             (multiple-value-prog1 (funcall function directory) (setf completed t)))
        (unwind-protect
             (when fd
               (handler-case-noninterrupt
                (logical-slot-copy-retire-startup-scratch directory fd)
                (condition
                 (if completed (error condition)
                     (report-query-temp-cleanup-failure directory condition)))))
          (when fd (sb-posix:close fd)))))))

(defun logical-slot-copy-owned-startup ()
  (multiple-value-bind (manifest descriptor control) (logical-slot-copy-preflight-startup)
    (declare (ignore descriptor))
    (when manifest
      (if (zerop (getf control :phase))
          (call-with-logical-slot-copy-startup-scratch #'logical-slot-copy-record-base-verification)
          (logical-slot-copy-verify-recovery-startup)))))

(defun logical-slot-copy-validate-startup ()
  (when (logical-slot-copy-preflight-startup)
    (logical-slot-copy-verify-recovery-startup))
  (values))

(defun logical-slot-promotion-source-entries ()
  "Capture the durable logical slot images matching the standby registry."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((logical-count
          (loop for slot being the hash-values of *wal-replication-slots*
                count (wal-logical-slot-p slot))))
    (when (plusp logical-count)
      (let ((entries
              (slot-state-map
               (wal-replication-slot-directory)
               (lambda (bytes name)
                 (let* ((entry (logical-slot-startup-entry-from-image
                                bytes name #'wal-decode-replication-slot-image))
                        (slot (logical-slot-startup-entry-slot entry)))
                   (when (wal-logical-slot-p slot)
                     (let ((live (gethash name *wal-replication-slots*)))
                       (unless (and live (wal-logical-slot-p live))
                         (error "durable logical slot ~a is missing from the standby registry" name))
                       (wal-require-usable-replication-slot live)
                       (unless (equalp bytes (wal-encode-replication-slot live))
                         (error "durable logical slot ~a differs from its standby registry entry" name))
                       entry))))
               :stabilize t :local-workspace t)))
        (let ((logical (remove nil entries)))
          (unless (= logical-count (length logical))
            (error "standby logical slot registry has unpersisted generations"))
          (sort logical #'string<
                :key (lambda (entry)
                       (wal-replication-slot-name (logical-slot-startup-entry-slot entry)))))))))

(defun logical-slot-copy-check-promotion (&key allow-standby-logical)
  (when (recovery-in-progress-p)
    (let ((phase (getf *physical-recovery-state* :phase)))
      (cond ((and allow-standby-logical (eql phase 2)))
            ((getf *physical-recovery-state* :logical-slot-evidence-digest)
             (logical-slot-promotion-read-evidence *physical-recovery-state*))
            ((if (sb-thread:holding-mutex-p *wal-lock*)
                 (logical-slot-promotion-source-entries)
                 (sb-thread:with-mutex (*wal-lock*) (logical-slot-promotion-source-entries)))
             (error "standby logical slots require promotion reconciliation")))))
  ;; Fence both promotion and destructive recovery retirement until the copied
  ;; import is reconciled.  Proof files, including temporary files, do not
  ;; authorize either operation.
  (when (getf *physical-recovery-state* :logical-slot-copy-id)
    (error "logical copy requires child-owner import before promotion"))
  (dolist (name '("logical-slot-copy.install" "logical-slot-copy.in-progress" "logical-slot-copy.descriptor"
                  "logical-slot-copy.base-verified" "logical-slot-copy.base-verified.tmp" "logical_slot_wal"
                  "logical-slot-copy.import" "logical-slot-copy.import.tmp"
                  "logical-slot-copy.evidence" "logical-slot-copy.evidence.tmp"
                  "logical-slot-copy.source-proof" "logical-slot-copy.source-proof.tmp"))
    (when (slot-state-lstat (merge-pathnames name *pgdata*))
      (error "logical copy requires child-owner import before promotion")))
  ;; Key presence is a backstop even when the other copy artifacts are gone.
  (when (slot-state-lstat (merge-pathnames "physical-backup.manifest" *pgdata*))
    (when (get-properties (recovery-read-manifest *pgdata*) '(:logical-slot-copy-id))
      (error "logical copy requires child-owner import before promotion")))
  (values))
