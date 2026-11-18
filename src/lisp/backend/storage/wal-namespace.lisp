;;;; Primary-only durable filename migration; never changes timeline or role.
(in-package #:pg)

(defvar *wal-namespace-test-hook* nil)

(defun wal-namespace-test-point (point)
  (when *wal-namespace-test-hook* (funcall *wal-namespace-test-hook* point)))

(defun wal-namespace-file-stat (path &optional missing-ok)
  "Never follow a symlink when validating a namespace migration alias."
  (let ((stat (handler-case (sb-posix:lstat (namestring path))
                (sb-posix:syscall-error (condition)
                  (if (and missing-ok (= (sb-posix:syscall-errno condition) sb-posix:enoent))
                      nil (error condition))))))
    (when (and stat (/= (logand (sb-posix:stat-mode stat) sb-posix:s-ifmt) sb-posix:s-ifreg))
      (wal-invalid-input "WAL namespace alias is not a regular file: ~a" path))
    stat))

(defun wal-namespace-same-inode-p (left right)
  (and left right (= (sb-posix:stat-dev left) (sb-posix:stat-dev right))
       (= (sb-posix:stat-ino left) (sb-posix:stat-ino right))))

(defun wal-namespace-migration-plan (&key cleanup)
  "Reject the entire unmatched destination inventory before creating any alias."
  (let ((legacy (wal-discover-segments 1 :legacy))
        (qualified (wal-discover-segments 1 :timeline)))
    (unless cleanup
      (dolist (segment qualified)
        (unless (member segment legacy)
          (wal-invalid-input "unmatched qualified WAL alias; inspect before retry: ~d" segment))))
    (loop for segment in legacy
          for old = (wal-segment-path segment 1 :legacy)
          for new = (wal-segment-path segment 1 :timeline)
          for old-stat = (wal-namespace-file-stat old)
          for new-stat = (wal-namespace-file-stat new t)
          do (when (and cleanup (null new-stat))
               (wal-invalid-input "retired legacy WAL alias requires manual inspection: ~a" old))
             (when (and new-stat (not (wal-namespace-same-inode-p old-stat new-stat)))
               (wal-invalid-input "qualified WAL alias belongs to another inode: ~a" new))
          collect (list old new old-stat new-stat))))

(declaim (ftype function wal-encode-archive-segment-proof wal-verify-archive-segment-proof))

(defun wal-namespace-proof-path (segment-path)
  (merge-pathnames (wal-archive-segment-proof-name (file-namestring segment-path)) segment-path))

(defun wal-namespace-stage-qualified-proof (old new)
  "Bind an existing closed legacy proof to the qualified name before control switches."
  (let* ((segment (nth-value 1 (wal-parse-segment-name (pathname-name old) :legacy)))
         (old-proof (wal-namespace-proof-path old))
         (new-proof (wal-namespace-proof-path new)))
    (unless (and (wal-archive-ready-segment-p segment)
                 (wal-namespace-file-stat old-proof t))
      (when (wal-namespace-file-stat new-proof t)
        (error "qualified WAL proof lacks a closed legacy source: ~a" new-proof))
      (return-from wal-namespace-stage-qualified-proof nil))
    (let* ((bytes (read-bounded-regular-file old +wal-segment-size+))
           (system-id (catalog-system-identifier))
           (proof (read-bounded-regular-file old-proof +wal-archive-segment-proof-size+))
           (end (wal-verify-archive-segment-proof proof system-id 1 segment :legacy bytes)))
      (sb-sys:without-interrupts
        (let ((fd (wal-archive-open-wal-directory)))
          (unwind-protect
               (wal-archive-stage-source
                fd (file-namestring new-proof)
                (wal-encode-archive-segment-proof system-id 1 segment :timeline end bytes)
                #'wal-archive-check-wal-directory)
            (sb-posix:close fd)))))))

(defun wal-namespace-remove-inactive-alias (entry inactive)
  "Recheck both inode identities, and reject any attempt to remove the active name."
  (destructuring-bind (old new expected new-stat) entry
    (declare (ignore new-stat))
    (unless (equal inactive (if (eq *wal-segment-namespace* :legacy) new old))
      (error "refusing to remove active WAL namespace name"))
    (unless (and (wal-namespace-same-inode-p expected (wal-namespace-file-stat old))
                 (wal-namespace-same-inode-p expected (wal-namespace-file-stat new)))
      (error "WAL namespace alias changed before inactive cleanup"))
    (let ((proof (wal-namespace-proof-path inactive)))
      (when (wal-namespace-file-stat proof t)
        (sb-posix:unlink (namestring proof))))
    ;; A previous attempt may have unlinked the proof without syncing.
    (fsync-directory (wal-directory))
    (sb-posix:unlink (namestring inactive))))

(defun wal-reconcile-namespace-at-startup ()
  "Reconcile inactive aliases before normal writes or checkpoint recycling.
Control remains authoritative: legacy rolls staging back; timeline finishes cleanup."
  ;; A promoted child's legacy TLI-1 names are ancestors, not migration aliases.
  (when (> *wal-insert-timeline* 1) (return-from wal-reconcile-namespace-at-startup (values)))
  (let* ((legacy (eq *wal-segment-namespace* :legacy))
         (opposite (if legacy :timeline :legacy)))
    (when (wal-discover-segments 1 opposite)
      (let ((plan (wal-namespace-migration-plan :cleanup (not legacy))))
        ;; Validate the complete plan before deleting even one inactive alias.
        (dolist (entry plan)
          (when (or (not legacy) (fourth entry))
            (wal-namespace-remove-inactive-alias entry (if legacy (second entry) (first entry)))
            (wal-namespace-test-point :after-startup-alias-cleanup)))
        (fsync-directory (wal-directory)))))
  (values))

(defun wal-migrate-namespace-under-lock ()
  "Caller owns quiescence, writer and WAL locks. Validate quiescence and control
preconditions, settle the old namespace prefix, then plan the inventory. Any
uncertain publication fences WAL."
  (unless (and (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
               (not *live-operation-drain-open*))
    (wal-invalid-input "WAL namespace migration requires owned operation quiescence"))
  (wal-require-supported-timelines *wal-insert-timeline* *wal-checkpoint-timeline*)
  (unless (member *wal-segment-namespace* '(:legacy :timeline)) (wal-invalid-input "invalid WAL namespace"))
  (unless *wal-ready* (wal-invalid-input "WAL namespace migration requires initialized WAL"))
  (wal-settle-page-publications-under-lock)
  (wal-require-no-pending-storage)
  (let* ((cleanup-only (eq *wal-segment-namespace* :timeline))
         (plan (wal-namespace-migration-plan :cleanup cleanup-only))
         (finished nil))
    (unwind-protect
         (progn
           (unless cleanup-only
             (wal-flush)
             (dolist (entry plan)
               (destructuring-bind (old new old-stat new-stat) entry
                 (declare (ignore old-stat))
                 (unless new-stat (sb-posix:link (namestring old) (namestring new)))
                 (wal-namespace-test-point :after-link)))
             (fsync-directory (wal-directory))
             (dolist (entry plan)
               (wal-namespace-stage-qualified-proof (first entry) (second entry)))
             (wal-namespace-test-point :after-links-sync)
             (let ((*wal-segment-namespace* :timeline))
               (wal-save-control-fields *wal-checkpoint-lsn* *wal-durable-lsn*))
             (wal-namespace-test-point :after-control-before-memory)
             (setf *wal-segment-namespace* :timeline)
             (wal-namespace-test-point :after-memory))
           ;; Retry verified cleanup even after a previous process committed the switch.
           (dolist (entry plan)
             (wal-namespace-remove-inactive-alias entry (first entry))
             (wal-namespace-test-point :after-cleanup-link))
           (fsync-directory (wal-directory))
           (setf finished t)
           :timeline)
      (unless finished (setf *wal-poisoned* t)))))

(defun wal-migrate-timeline-namespace ()
  "Explicit primary-only storage migration; no promotion or writer authority change."
  (prevent-recovery-primary-operation "WAL filename namespace migration")
  (when (or (plusp *live-operation-depth*) *swank-listener-enabled-p*)
    (error "WAL namespace migration requires outside admission with Swank disabled"))
  (sb-thread:with-mutex (*reload-run-lock*)
    (unwind-protect
         (with-live-operation-quiescence
           (with-write-lock
             (with-wal-critical (wal-migrate-namespace-under-lock))))
      ;; Also cover THROW/RETURN exits after uncertain control publication.
      (when *wal-poisoned* (request-shutdown +fast-shutdown+)))))
