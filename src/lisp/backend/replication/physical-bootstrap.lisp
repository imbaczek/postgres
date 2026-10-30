;;;; Coherent native base copy under local operation quiescence.
(in-package #:pg)

(defvar *physical-bootstrap-test-hook* nil)

(defun physical-bootstrap-test-point (point target)
  (when *physical-bootstrap-test-hook*
    (funcall *physical-bootstrap-test-hook* point target)))

(declaim (ftype function physical-promotion-startup-primary-p physical-promotion-startup-pending-p))

(defun physical-bootstrap-standby-control-p ()
  "Return true when existing recovery control requires ordinary standby preparation.
Malformed control is an error, not permission to fall back to primary startup."
  (when (wal-namespace-file-stat (physical-recovery-control-path) t)
    (member (getf (physical-recovery-read-control *pgdata*) :phase) '(0 1 2))))

(defun physical-recovery-signal-found-p (name)
  (let ((path (namestring (merge-pathnames name *pgdata*))))
    (handler-case (sb-posix:stat path)
      (sb-posix:syscall-error () (return-from physical-recovery-signal-found-p nil)))
    (handler-case
        (let ((fd (sb-posix:open path
                                 (logior sb-posix:o-rdwr sb-posix:o-nonblock
                                         +postmaster-o-cloexec+))))
          (unwind-protect
               (when (sb-posix:s-isreg (sb-posix:stat-mode (sb-posix:fstat fd)))
                 (sb-posix:fsync fd))
            (sb-posix:close fd)))
      (sb-posix:syscall-error () nil))
    t))

(defun physical-recovery-read-signal-mode ()
  (let ((standby (physical-recovery-signal-found-p "standby.signal"))
        (archive (physical-recovery-signal-found-p "recovery.signal")))
    (cond (standby :standby) (archive :archive))))

(defun physical-recovery-signal-retirement-stat (name)
  (handler-case
      (let ((stat (sb-posix:lstat (namestring (merge-pathnames name *pgdata*)))))
        (when (sb-posix:s-isdir (sb-posix:stat-mode stat))
          (error "recovery signal cannot be retired as a file: ~a" name))
        stat)
    (sb-posix:syscall-error (condition)
      (unless (= sb-posix:enoent (sb-posix:syscall-errno condition))
        (error condition)))))

(defun physical-recovery-unlink-signal (name)
  (when (physical-recovery-signal-retirement-stat name)
    (sb-posix:unlink (namestring (merge-pathnames name *pgdata*)))))

(defun physical-bootstrap-check-startup ()
  "Choose recovery or primary startup before listeners or ordinary storage setup.
Recovery artifacts without recovery control are an error: pending control is
retired last, so their presence cannot safely select primary startup. Nonpending
standby control requires recovery validation. A pending request is reconciled
against parent or committed child control. Return :PRIMARY-READY when pending
completion already initialized storage. Invalid proof is an error."
  (let ((signal-mode (physical-recovery-read-signal-mode)))
    (when (and (not (wal-namespace-file-stat (physical-recovery-control-path) t))
               (or signal-mode
                   (wal-namespace-file-stat (merge-pathnames "recovery.required" *pgdata*) t)
                   (wal-namespace-file-stat (merge-pathnames "physical-backup.manifest" *pgdata*) t)
                   (and (wal-namespace-file-stat (physical-follow-config-path) t)
                        (not (physical-follow-cleanup-only-p (physical-follow-read-config))))
                   (wal-namespace-file-stat (merge-pathnames "physical-restartpoint.control" *pgdata*) t)))
      (error "recovery metadata is incomplete: recovery control is missing"))
    (cond
      ((physical-bootstrap-standby-control-p) (physical-recovery-prepare :signal-mode signal-mode) (values))
      ((physical-promotion-startup-primary-p) (values))
      ((physical-promotion-startup-pending-p) :primary-ready)
      (t
       (if (or (probe-file (merge-pathnames "recovery.required" *pgdata*))
               (probe-file (merge-pathnames "physical-backup.manifest" *pgdata*))
               (probe-file (merge-pathnames "physical-recovery.control" *pgdata*)))
           (physical-recovery-prepare :signal-mode signal-mode)
           (when (recovery-in-progress-p)
             (error "cannot clear recovery role by removing its metadata")))
       (values)))))

(defun physical-bootstrap-target (directory)
  "Require an existing empty target outside the source directory hierarchy."
  (let ((target (truename (uiop:ensure-directory-pathname directory)))
        (source (truename *pgdata*)))
    (when (or (uiop:subpathp target source) (uiop:subpathp source target)
              (uiop:directory-files target) (uiop:subdirectories target))
      (error "physical backup target must be empty and separate from source"))
    target))

(defun physical-recovery-marker-bytes ()
  "Canonical recovery exclusion marker shared by creation and both startup paths."
  (string->bytes (format nil "PGPHYSICAL-RECOVERY-REQUIRED-1~%")))

(defun physical-bootstrap-mark-target (target)
  "Persist the recovery guard before placing any database files in TARGET."
  (fsync-directory (uiop:pathname-parent-directory-pathname target))
  (with-open-file (stream (merge-pathnames "recovery.required" target)
                          :direction :output :if-exists :error :if-does-not-exist :create)
    (write-string (bytes->string (physical-recovery-marker-bytes)) stream)
    (fsync-stream stream))
  (fsync-directory target))

(defun physical-bootstrap-history-inventory (timeline)
  "Return the selected timeline history and available ancestor histories for a backup.
Validate the selected lineage before constructing the inventory."
  (let ((history (timeline-check-lineage (timeline-read-history timeline))))
    (sort (loop for entry in history
                for ancestor = (timeline-history-entry-timeline entry)
                when (and (> ancestor 1)
                          (or (= ancestor timeline)
                              (probe-file (timeline-history-path ancestor *pgdata*))))
                  collect (concatenate 'string "pg_wal/" (timeline-history-filename ancestor)))
          #'string<)))

(defun physical-bootstrap-inventory (&optional (checkpoint-start (wal-checkpoint-record-start)))
  "Return only durable cluster state and catalog-referenced relation forks."
  (let ((files (list "PG_VERSION" "pg_xact" "global/pg_control"
                     "global/pg_catalog.dat" "global/pg_wal_control")))
    (dolist (relfilenode (remove-duplicates (catalog-physical-relfilenodes)))
      (push (format nil "base/~d" relfilenode) files)
      (when (probe-file (relation-file-path relfilenode :visibility))
        (push (format nil "base/~d.vm" relfilenode) files)))
    (let* ((restart checkpoint-start)
           (last (and (plusp *wal-durable-lsn*) (floor (1- *wal-durable-lsn*) +wal-segment-size+))))
      (when last
        (loop for segment from (floor restart +wal-segment-size+) to last
              do (push (concatenate 'string "pg_wal/" (file-namestring (wal-segment-path segment))) files))))
    (sort (append files (physical-bootstrap-history-inventory *wal-insert-timeline*)) #'string<)))

(defun physical-bootstrap-derived-wal-control (target)
  "Derive target control when the source has a historical archive reference.
Validate child control and require a checkpoint beyond the old fork.  Remove only
the old reference from the target, preserving positions, snapshot, timelines,
namespace and other metadata.  Atomically write and read back the complete tuple,
then hash the actual target bytes.  Leave source control unchanged.  With no
reference, return NIL so the ordinary copy preserves source bytes exactly.
The new base receives its own archive; the old primary archive is not copied."
  (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline namespace metadata)
      (wal-read-control-fields)
    (let ((reference (getf metadata :retained-reference)))
      (when reference
        (wal-require-supported-timelines insertion checkpoint-timeline)
        (unless (and snapshot (> checkpoint (getf reference :end)))
          (error "base backup checkpoint does not supersede its historical archive"))
        (let ((copied (copy-list metadata))
              (path (merge-pathnames "global/pg_wal_control" target)))
          (remf copied :retained-reference)
          (let ((bytes (wal-encode-control-image
                        checkpoint durable snapshot :timeline insertion
                        :checkpoint-timeline checkpoint-timeline :namespace namespace :metadata copied)))
            (wal-ensure-directory-durable (uiop:pathname-directory-pathname path))
            (wal-atomic-write-bytes path bytes)
            (let ((actual (recovery-read-bytes path :maximum 513)))
              (unless (and (equalp bytes actual)
                           (equal (multiple-value-list (wal-decode-control-line (bytes->string actual)))
                                  (list checkpoint durable snapshot insertion checkpoint-timeline namespace copied)))
                (error "base backup control readback differs from its new checkpoint"))
              (list "global/pg_wal_control" (length actual)
                    (physical-store-update-digest #xCBF29CE484222325 actual)))))))))

(defun physical-bootstrap-copy-file (relative target &key limit)
  "Copy and fsync a stable source file or exact prefix, returning its length and checksum."
  (when (equal relative "global/pg_wal_control")
    (let ((derived (physical-bootstrap-derived-wal-control target)))
      (when derived
        (physical-bootstrap-test-point :after-file target)
        (return-from physical-bootstrap-copy-file derived))))
  (let* ((source (merge-pathnames relative *pgdata*))
         (destination (merge-pathnames relative target))
         (parent (uiop:pathname-directory-pathname destination))
         (digest #xCBF29CE484222325)
         (length 0)
         (buffer (make-array 65536 :element-type '(unsigned-byte 8))))
    (wal-ensure-directory-durable parent)
    (with-open-file (input source :element-type '(unsigned-byte 8))
      (with-atomic-output
          (output destination (make-pathname :type "tmp" :defaults destination)
                  parent :element-type '(unsigned-byte 8))
        (loop for count = (read-sequence buffer input
                                         :end (if limit (min (length buffer) (- limit length))
                                                  (length buffer)))
              while (plusp count)
              do (write-sequence buffer output :end count)
                 (incf length count)
                 (setf digest (physical-store-update-digest digest (subseq buffer 0 count))))
        (when (and limit (/= length limit))
          (error "physical base backup file ~a is shorter than its durable prefix" relative))))
    (physical-bootstrap-test-point :after-file target)
    (list relative length digest)))

(defun physical-bootstrap-manifest-text (manifest)
  "Print portable literal strings rather than SBCL's readable BASE-CHAR #A arrays."
  (labels ((literal (value)
             (typecase value
               (string (map 'string #'identity value))
               (cons (mapcar #'literal value))
               (t value))))
    (with-standard-io-syntax (prin1-to-string (literal manifest)))))

(defun physical-bootstrap-copy-under-quiescence (target pin-name)
  "Checkpoint and capture one complete native image while mutation admission is closed."
  (unless (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
    (error "physical base copy requires operation quiescence ownership"))
  ;; An admitted control operation could enable Swank while this caller drains.
  (when *swank-listener-enabled-p*
    (error "gated physical base backup requires the optional Swank listener to be off"))
  (when (or (not (active-xids-empty-p)) *xid-wrap-pending*
            (probe-file (xid-wrap-marker-path)))
    (error "physical base backup requires no active transaction IDs or pending XID wrap"))
  (wal-checkpoint-core)
  (let ((pin (physical-source-pin-create pin-name)))
    ;; Failure after here intentionally retains the durable source pin. The
    ;; operator must inspect the failed target and explicitly release the pin.
    (physical-bootstrap-mark-target target)
    (physical-recovery-mark-target target)
    (physical-bootstrap-test-point :after-checkpoint target)
    (let* ((checkpoint *wal-checkpoint-lsn*)
           (end *wal-durable-lsn*)
           (system-id (catalog-system-identifier))
           (last-segment (and (plusp end) (concatenate 'string "pg_wal/"
                                                       (file-namestring (wal-segment-path
                                                                         (floor (1- end) +wal-segment-size+))))))
           (files (mapcar (lambda (relative)
                            (physical-bootstrap-copy-file
                             relative target
                             :limit (when (equal relative last-segment)
                                      (1+ (mod (1- end) +wal-segment-size+)))))
                          (physical-bootstrap-inventory)))
           (manifest (list :version 3 :system-id system-id
                           :cluster-identity (catalog-cluster-identity *catalog*) :timeline *wal-insert-timeline*
                           :checkpoint checkpoint :checkpoint-start (wal-checkpoint-record-start checkpoint)
                           :end end :next-xid *next-xid*
                           :source-pin (physical-source-pin-name pin) :files files)))
      (physical-bootstrap-test-point :before-manifest target)
      (let* ((*print-readably* t)
             (text (physical-bootstrap-manifest-text manifest))
             (bytes (string->bytes text))
             (crc (wal-crc32 bytes))
             (image (string->bytes (format nil "PGPHYSICALBACKUP1 ~8,'0X~%~a~%" crc text))))
        (physical-source-pin-bind-manifest pin (physical-store-update-digest #xCBF29CE484222325 image))
        (physical-bootstrap-test-point :after-pin-binding target)
        (wal-atomic-write-bytes (merge-pathnames "physical-backup.manifest" target) image))
      (fsync-directory target)
      (physical-bootstrap-test-point :after-manifest target)
      manifest)))

(defun physical-base-backup (directory pin-name)
  "Create a recovery-required native base image while SQL mutations are quiescent.
Invoke as a direct control form with two literal strings, or outside a live
operation. Idle transactions with assigned XIDs cause refusal; new operations
wait until the copy completes. This initial copy pauses the source for its
duration. Cleanup of any deferred slot recycling runs after reload exclusion and
quiescence end. It is not the final online base-backup protocol."
  (prevent-recovery-primary-operation "native base backup")
  (when *swank-listener-enabled-p*
    (error "gated physical base backup requires the optional Swank listener to be off"))
  (when (plusp *live-operation-depth*)
    (error "physical base backup must start outside a live operation"))
  (unless (wal-valid-replication-slot-name-p pin-name)
    (error "invalid physical base backup pin name"))
  (call-with-invalidated-slot-cleanup
   (lambda ()
     (sb-thread:with-mutex (*reload-run-lock*)
       (with-live-operation-quiescence
         (physical-bootstrap-copy-under-quiescence
          (physical-bootstrap-target directory) pin-name))))))
