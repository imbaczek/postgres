(in-package #:pg)

(defvar *logical-slot-copy-verification-test-hook* nil)

(defun logical-slot-copy-verification-test-point (point context)
  (when *logical-slot-copy-verification-test-hook*
    (funcall *logical-slot-copy-verification-test-hook* point context)))

(defun call-with-logical-slot-copy-scratch (directory target function)
  (let* ((root (uiop:ensure-directory-pathname (truename directory)))
         (fd nil))
    (when (or (uiop:subpathp root target) (uiop:subpathp target root))
      (error "logical copy verification scratch must be separate from its target"))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (setf fd (physical-backup-open-directory root))
             (let ((initial (physical-image-check-directory fd)))
               (unless (every (lambda (name) (equal name ".pg_lisp.lock"))
                              (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t))
                 (error "logical copy verification scratch must be empty"))
               (sb-sys:with-local-interrupts
                 (let ((*postmaster-pid-directory* (physical-image-fd-path fd)))
                   (with-open-stream (lock (postmaster-open-data-lock)) (fsync-stream lock)))
                 (call-with-physical-adoption-owner
                  root
                  (lambda (owner)
                    (unless (and (= (sb-posix:stat-dev initial) (physical-adoption-owner-device owner))
                                 (= (sb-posix:stat-ino initial) (physical-adoption-owner-inode owner)))
                      (error "logical copy verification scratch changed before ownership"))
                    (logical-slot-copy-verification-test-point :after-scratch-lock owner)
                    (unless (equal (list ".pg_lisp.lock")
                                   (directory-entry-names (physical-image-fd-path fd) :follow-symlinks t))
                      (error "logical copy verification scratch changed before initialization"))
                    (funcall function owner))))))
        (when fd (sb-posix:close fd))))))

(defun logical-slot-copy-clean-scratch (owner)
  (logical-slot-copy-verification-test-point :before-scratch-cleanup owner)
  (physical-directory-check-owner owner)
  (retry-pending-query-temp-cleanup)
  (physical-directory-check-owner owner)
  (dolist (path (list *query-temp-instance-directory* *query-temp-root-directory*))
    (when (and path (query-temp-lstat path))
      (unless (null (directory-entry-names path :follow-symlinks t))
        (error "logical copy verification temporary cleanup remains unresolved"))
      (sb-posix:rmdir (namestring path))))
  (physical-directory-check-owner owner)
  t)

(defun call-with-logical-slot-copy-verification-spools (owner function)
  (let ((*query-temp-directory* nil) (*query-temp-root-directory* nil)
                                     (*query-temp-instance-directory* nil) (*query-temp-scope-directory* nil)
                                     (*query-temp-cleanup-owner* nil) (*query-temp-cleanup-domain* nil)
                                     (*query-temp-file-scope-active-p* nil) (*query-temp-files* nil) (*query-temp-quota* nil)
                                     (*query-temp-session-id* (next-query-temp-session-id))
                                     (clean nil))
    (unwind-protect
         (progn
           (initialize-query-temp-storage (physical-adoption-owner-directory owner))
           (multiple-value-prog1 (funcall function)
             (logical-slot-copy-clean-scratch owner)
             (setf clean t)))
      (unless clean
        (handler-case-noninterrupt
         (logical-slot-copy-clean-scratch owner)
         (condition
          (report-query-temp-cleanup-failure (physical-adoption-owner-directory owner) condition)))))))

(defun logical-slot-copy-verify-metadata (owner context)
  (logical-slot-copy-check-input owner context)
  (let* ((root (physical-image-context-root-path context))
         (*physical-image-context* context))
    (when (slot-state-lstat (merge-pathnames "logical-slot-copy.in-progress" root))
      (error "unfinished logical slot copy cannot be verified"))
    (multiple-value-bind (manifest digest) (recovery-read-manifest root)
      (multiple-value-bind (bytes facts)
          (logical-slot-copy-read-file context "logical-slot-copy.descriptor"
                                       +logical-slot-copy-descriptor-maximum-bytes+)
        (let ((descriptor (logical-slot-copy-descriptor-decode bytes)))
          (logical-slot-copy-check-binding descriptor manifest digest)
          (unless (<= 1 (length (logical-slot-copy-descriptor-slots descriptor))
                      +logical-slot-copy-descriptor-maximum-slots+)
            (error "logical copy verification requires a nonempty bounded slot inventory"))
          (unless (and
                   (equalp (logical-slot-copy-descriptor-base-control descriptor)
                           (logical-slot-copy-read-file context "global/pg_wal_control"
                                                        (1+ +wal-control-maximum-bytes+)))
                   (equalp (logical-slot-copy-descriptor-recovery-control descriptor)
                           (logical-slot-copy-read-file context "physical-recovery.control"
                                                        +physical-recovery-control-maximum-bytes+))
                   (equalp (physical-recovery-marker-bytes)
                           (logical-slot-copy-read-file context "recovery.required" 4096)))
            (error "logical copy input controls differ from their immutable descriptor"))
          (multiple-value-bind (observations index)
              (logical-slot-copy-input-observe owner context manifest descriptor)
            (let ((manifest-row (find "physical-backup.manifest" observations :key #'first :test #'equal))
                  (descriptor-row (find "logical-slot-copy.descriptor" observations :key #'first :test #'equal)))
              (unless (and (= digest (third manifest-row))
                           (= (length bytes) (second descriptor-row))
                           (= (physical-store-update-digest #xCBF29CE484222325 bytes) (third descriptor-row))
                           (equal facts (fourth descriptor-row)))
                (error "logical copy metadata changed while binding its input")))
            (let ((*pgdata* root))
              (recovery-check-image-metadata manifest root)
              (authentication-check-backup-policy root))
            (values manifest descriptor observations index
                    (logical-slot-copy-input-checkpoint context manifest descriptor))))))))

(defun logical-slot-copy-note-coverage (coverage key start end)
  (let ((bounds (gethash key coverage)))
    (if bounds
        (setf (car bounds) (min start (car bounds)) (cdr bounds) (max end (cdr bounds)))
        (setf (gethash key coverage) (cons start end)))))

(defun logical-slot-copy-verify-slot (owner context index coverage manifest descriptor checkpoint slot)
  (let* ((end (getf manifest :end))
         (history (timeline-parse-history (getf manifest :timeline)
                                          (bytes->string (logical-slot-copy-descriptor-history descriptor))))
         (fields (logical-slot-copy-control-fields (logical-slot-copy-descriptor-base-control descriptor)))
         (snapshot (third fields))
         (reader (make-logical-reader-state :database-oid (wal-replication-slot-database-oid slot)))
         (progress (make-logical-slot-startup-progress (wal-replication-slot-confirmed-flush-lsn slot)))
         (*wal-recovery-reservation-batch-clean-p* nil)
         (last nil) (count 0))
    (logical-slot-copy-check-slot-lifetime slot (wal-logical-status-from-checkpoint snapshot) history end)
    (when (eq :reserved (wal-replication-slot-wal-status slot))
      (multiple-value-bind (start-owner role) (logical-slot-startup-start-position slot history)
        (unwind-protect
             (call-with-logical-reader-spools
              (lambda ()
                (call-with-logical-wal-cursor
                 history (sixth fields) (wal-replication-slot-restart-lsn slot) end start-owner role
                 (lambda () (logical-slot-copy-check-input owner context))
                 (lambda (cursor)
                   (loop while (< (logical-wal-cursor-position cursor) end) do
                     (let* ((position (logical-wal-cursor-position cursor))
                            (segment (floor position +wal-segment-size+))
                            (key (list (logical-wal-segment-carrier segment history) segment)))
                       (unless (gethash key index) (error "logical copy restart is missing its selected piece"))
                       (logical-slot-copy-note-coverage coverage key position position))
                     (multiple-value-bind (record bytes record-owner carrier kind)
                         (logical-wal-cursor-next cursor)
                       (declare (ignore record-owner))
                       (unless record (error "logical copy input ended before its checkpoint"))
                       (let* ((key (list carrier (floor (wal-record-start record) +wal-segment-size+)))
                              (piece (first (gethash key index))))
                         (unless (and piece (<= (fifth piece) (wal-record-start record)
                                                (wal-record-end record) (sixth piece)))
                           (error "logical copy record exceeds its declared piece coverage"))
                         (logical-slot-copy-note-coverage coverage key (wal-record-start record)
                                                          (wal-record-end record)))
                       (logical-slot-copy-validate-payload reader record kind)
                       (logical-slot-startup-accept-record progress record kind)
                       (setf last bytes)
                       (incf count)
                       (logical-slot-copy-verification-test-point :after-record context)))
                   (unless (and (= end (logical-wal-cursor-position cursor))
                                (null (logical-slot-startup-progress-batch-start progress))
                                (or (zerop count) (equalp checkpoint last)))
                     (error "logical copy WAL does not finish at its bound checkpoint")))
                 :segment-reader (logical-slot-copy-input-segment-reader owner context index)
                 :batch-policy :logical-reader)))
          (wal-logical-reader-reset-batch reader nil))))
    (values progress count)))

(defun logical-slot-copy-verify-input (owner context scratch)
  (multiple-value-bind (manifest descriptor observations index checkpoint)
      (logical-slot-copy-verify-metadata owner context)
    (logical-slot-copy-verification-test-point :after-input context)
    (let ((coverage (make-hash-table :test #'equal)) (results nil) (records 0)
                                                     (*wal-lock* (sb-thread:make-mutex :name "logical copy verification reader")))
      (call-with-logical-slot-copy-scratch
       scratch (physical-adoption-owner-directory owner)
       (lambda (scratch-owner)
         (call-with-logical-slot-copy-verification-spools
          scratch-owner
          (lambda ()
            (dolist (entry (logical-slot-copy-descriptor-slots descriptor))
              (let ((slot (logical-slot-state-decode (second entry) (first entry))))
                (multiple-value-bind (progress count)
                    (logical-slot-copy-verify-slot owner context index coverage manifest descriptor checkpoint slot)
                  (incf records count)
                  (logical-slot-copy-verification-test-point :slot-verified (list slot progress))
                  (push (list slot progress) results))))))))
      (dolist (piece (logical-slot-copy-descriptor-wal-pieces descriptor))
        (let ((bounds (gethash (subseq piece 0 2) coverage)))
          (unless (and bounds (= (fifth piece) (car bounds)) (= (sixth piece) (cdr bounds)))
            (error "logical copy sidecar coverage differs from its completed scans"))))
      (logical-slot-copy-input-check-observations owner context observations)
      (dolist (row observations)
        (unless (equal row (logical-slot-copy-file-row context (first row)))
          (error "logical copy input failed its final byte verification")))
      (logical-slot-copy-input-inventory owner context manifest descriptor)
      (values manifest descriptor (nreverse results) records))))

(defun logical-slot-copy-verify (directory scratch)
  (call-with-physical-adoption-owner
   directory
   (lambda (owner)
     (call-with-physical-directory-root
      owner
      (lambda (borrowed root)
        (declare (ignore root))
        (call-with-physical-backup-image-directory
         borrowed
         (lambda (path)
           (declare (ignore path))
           (let ((context *physical-image-context*))
             (logical-slot-copy-open-input-sidecar owner context)
             (multiple-value-bind (manifest descriptor results records)
                 (logical-slot-copy-verify-input owner context scratch)
               (declare (ignore manifest))
               (list :copy-id (logical-slot-copy-descriptor-copy-id descriptor)
                     :slots (length (logical-slot-copy-descriptor-slots descriptor))
                     :records records
                     :pending-xids (loop for entry in results
                                         sum (hash-table-count
                                              (logical-slot-startup-progress-pending (second entry))))))))))))))
