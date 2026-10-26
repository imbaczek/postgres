;;;; Coherent native base copy under local operation quiescence.
(in-package #:pg)

(defvar *physical-bootstrap-test-hook* nil)

(defun physical-bootstrap-test-point (point target)
  (when *physical-bootstrap-test-hook*
    (funcall *physical-bootstrap-test-hook* point target)))

(declaim (ftype function physical-promotion-startup-primary-p physical-promotion-startup-pending-p))

(defun physical-bootstrap-check-startup ()
  "Classify committed control first, then finish a valid pending request before SQL."
  (cond
    ((physical-promotion-startup-primary-p) (values))
    ((physical-promotion-startup-pending-p) :primary-ready)
    (t
     (if (or (probe-file (merge-pathnames "recovery.required" *pgdata*))
             (probe-file (merge-pathnames "physical-backup.manifest" *pgdata*))
             (probe-file (merge-pathnames "physical-recovery.control" *pgdata*)))
         (physical-recovery-prepare)
         (when (recovery-in-progress-p)
           (error "cannot clear recovery role by removing its metadata")))
     (values))))

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

(defun physical-bootstrap-inventory (&optional (checkpoint-start (wal-checkpoint-record-start)))
  "Return only durable cluster state and catalog-referenced relation forks."
  (let ((files (list "PG_VERSION" "pg_xact" "global/pg_control"
                     "global/pg_catalog.dat" "global/pg_wal_control")))
    (dolist (relfilenode (remove-duplicates (wal-relation-filennodes)))
      (push (format nil "base/~d" relfilenode) files)
      (when (probe-file (relation-file-path relfilenode :visibility))
        (push (format nil "base/~d.vm" relfilenode) files)))
    (let* ((restart checkpoint-start)
           (last (and (plusp *wal-durable-lsn*) (floor (1- *wal-durable-lsn*) +wal-segment-size+))))
      (when last
        (loop for segment from (floor restart +wal-segment-size+) to last
              do (push (concatenate 'string "pg_wal/" (file-namestring (wal-segment-path segment))) files))))
    (sort files #'string<)))

(defun physical-bootstrap-copy-file (relative target &key limit)
  "Copy and fsync a stable source file or exact prefix, returning its length and checksum."
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
                           :cluster-identity (catalog-cluster-identity *catalog*) :timeline 1
                           :checkpoint checkpoint :checkpoint-start (wal-checkpoint-record-start checkpoint)
                           :end end :next-xid *next-xid*
                           :source-pin (physical-source-pin-name pin) :files files)))
      (physical-bootstrap-test-point :before-manifest target)
      (let* ((*print-readably* t)
             (text (physical-bootstrap-manifest-text manifest))
             (bytes (string->bytes text))
             (crc (wal-crc32 bytes)))
        (wal-atomic-write-bytes
         (merge-pathnames "physical-backup.manifest" target)
         (string->bytes (format nil "PGPHYSICALBACKUP1 ~8,'0X~%~a~%" crc text))))
      (fsync-directory target)
      (physical-bootstrap-test-point :after-manifest target)
      manifest)))

(defun physical-base-backup (directory pin-name)
  "Create a recovery-required native base image while SQL mutations are quiescent.
Invoke as a direct control form with two literal strings, or outside a live
operation. Idle transactions with assigned XIDs cause refusal; new operations
wait until the copy completes. This initial copy pauses the source for its
duration. It is not the final online base-backup protocol."
  (prevent-recovery-primary-operation "native base backup")
  (when *swank-listener-enabled-p*
    (error "gated physical base backup requires the optional Swank listener to be off"))
  (when (plusp *live-operation-depth*)
    (error "physical base backup must start outside a live operation"))
  (unless (wal-valid-replication-slot-name-p pin-name)
    (error "invalid physical base backup pin name"))
  (sb-thread:with-mutex (*reload-run-lock*)
    (call-with-invalidated-slot-cleanup
     (lambda ()
       (with-live-operation-quiescence
         (physical-bootstrap-copy-under-quiescence
          (physical-bootstrap-target directory) pin-name))))))
