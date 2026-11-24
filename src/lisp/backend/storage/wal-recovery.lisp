;;;; Native WAL batch validation, staging, and redo.
(in-package #:pg)

(defvar *wal-recovery-page-spool-enabled* nil)

(defvar *wal-recovery-page-spool* nil
  "Recovery page spool, with an ordinary cleanup owner rather than WAL's domain.")

(defvar *wal-recovery-catalog-spool* nil
  "Owns catalog chunks until the batch terminal or recovery cleanup.")

(defun wal-recovery-close-spool (spool kind)
  (when spool
    (if (logical-spool-closed-p spool)
        (retry-query-temp-owner-cleanup (logical-spool-owner spool))
        (close-logical-spool spool))
    (unless (eq :resolved (query-temp-cleanup-owner-state (logical-spool-owner spool)))
      (error "recovery ~a spool cleanup failed" kind)))
  nil)

(defun wal-reassemble-catalog-chunks (expected-count expected-length)
  (let ((spool *wal-recovery-catalog-spool*))
    (multiple-value-bind (bytes count) (when spool (logical-spool-resource-size spool))
      (unless (and count
                   (eql expected-count *wal-recovery-catalog-chunk-count*)
                   (= expected-count *wal-recovery-catalog-next-chunk* count))
        (error "WAL catalog chunk sequence is incomplete"))
      (unless (= expected-length bytes)
        (error "WAL catalog chunks do not match their declared length")))
    (logical-spool-seal spool)
    (let ((payload (make-array expected-length :element-type '(unsigned-byte 8)))
          (offset 0))
      (logical-spool-map-events
       spool (lambda (chunk)
               (replace payload chunk :start1 offset)
               (incf offset (length chunk))))
      (unless (= offset expected-length)
        (error "WAL catalog spool length changed during reassembly"))
      (setf *wal-recovery-catalog-spool* (wal-recovery-close-spool spool "catalog"))
      payload)))

(defun wal-apply-catalog-image (deletes catalog-bytes)
  (wal-atomic-write-bytes (merge-pathnames "global/pg_catalog.dat" *pgdata*)
                          catalog-bytes :mode #o600)
  (dolist (relfilenode deletes)
    (smgr-forget relfilenode :delete t))
  (when deletes (fsync-directory (merge-pathnames "base/" *pgdata*))))

(defun wal-prepare-catalog-record (payload)
  "Validate a catalog payload and return the operation that publishes it."
  (multiple-value-bind (deletes catalog-bytes)
      (wal-decode-catalog-payload payload :borrow-image-p t)
    (lambda () (wal-apply-catalog-image deletes catalog-bytes))))

(defun wal-recovery-close-page-spool ()
  (setf *wal-recovery-page-spool*
        (wal-recovery-close-spool *wal-recovery-page-spool* "page"))
  (values))

(defmacro wal-append-recovery-spool (spool bytes)
  "Publish the recovery spool in SPOOL before the first append can acquire a file."
  `(progn
     (unless ,spool
       (sb-sys:without-interrupts
         (let ((*query-temp-file-limit-bytes* nil))
           (setf ,spool (make-logical-spool)))))
     (logical-spool-append ,spool ,bytes)))

(defun wal-recovery-stage-page-record (record)
  "Validate one page record before retaining its bounded raw payload."
  (let* ((payload (wal-record-payload record))
         (size (length payload))
         (event (make-array (+ 12 size) :element-type '(unsigned-byte 8))))
    (wal-decode-page-images payload (wal-record-end record))
    (wal-set-u32 event 0 (wal-record-version record))
    (wal-set-u64 event 4 (wal-record-end record))
    (replace event payload :start1 12)
    (wal-append-recovery-spool *wal-recovery-page-spool* event))
  (values))

(defun wal-recovery-map-page-spool (function)
  "Visit validated records, one at a time, in original WAL order."
  (when *wal-recovery-page-spool*
    (logical-spool-map-events
     *wal-recovery-page-spool*
     (lambda (event)
       (unless (>= (length event) 12) (error "invalid recovery page spool record"))
       (let ((version (wal-u32 event 0))
             (lsn (wal-u64 event 4)))
         (unless (= version +wal-version+)
           (error "invalid recovery page spool version"))
         (funcall function (wal-decode-page-images (subseq event 12) lsn))))))
  (values))

(defun wal-recovery-close-catalog-spool ()
  (setf *wal-recovery-catalog-spool*
        (wal-recovery-close-spool *wal-recovery-catalog-spool* "catalog"))
  (values))

(defun wal-recovery-discard-batch-spools ()
  "Close staged page and catalog resources before their batch state is replaced."
  (wal-recovery-close-page-spool)
  (wal-recovery-close-catalog-spool)
  (values))

(defun wal-recovery-reset-batch (active)
  "Discard staged batch records. ACTIVE starts a batch after a validated marker."
  (wal-recovery-discard-batch-spools)
  (unless active (setf *wal-recovery-reservation-batch-clean-p* nil))
  (setf *wal-recovery-group-active* active
        *wal-recovery-page-images* nil
        *wal-recovery-relation-resets* nil
        *wal-recovery-page-records-seen-p* nil
        *wal-recovery-catalog-chunk-count* nil
        *wal-recovery-catalog-next-chunk* 0
        *wal-recovery-logical-catalog-event-p* nil)
  (wal-recovery-reset-logical-change-batch)
  (values))

(defun wal-recovery-note-page-record ()
  (wal-check-batch-page-record *wal-recovery-group-active* *wal-recovery-catalog-chunk-count*)
  (setf *wal-recovery-page-records-seen-p* t))

(defun wal-recovery-stage-logical-changes (record)
  (wal-check-batch-logical-changes *wal-recovery-logical-catalog-event-p* *wal-recovery-catalog-chunk-count*)
  (wal-recovery-accept-logical-change-chunk record))

(defun wal-recovery-append-catalog-chunk (bytes)
  "Retain validated catalog bytes in the batch's bounded scratch spool."
  (wal-append-recovery-spool *wal-recovery-catalog-spool* bytes))

(defun wal-recovery-stage-catalog-chunk (record)
  (multiple-value-bind (count bytes)
      (wal-decode-next-catalog-chunk record *wal-recovery-group-active*
                                     *wal-recovery-logical-catalog-event-p*
                                     *wal-recovery-logical-chunk-count*
                                     *wal-recovery-catalog-chunk-count*
                                     *wal-recovery-catalog-next-chunk*)
    (wal-recovery-append-catalog-chunk bytes)
    (setf *wal-recovery-catalog-chunk-count* count)
    (incf *wal-recovery-catalog-next-chunk*)))

(defun wal-recovery-note-logical-catalog-event (record)
  (wal-check-batch-logical-catalog-event record *wal-recovery-group-active*
                                         *wal-recovery-logical-catalog-event-p*
                                         *wal-recovery-logical-chunk-count*)
  (setf *wal-recovery-logical-catalog-event-p* t))

(defun wal-replay-finish-batch (terminal-preparer &optional catalog-chunks-p)
  (unless *wal-recovery-group-active*
    (error "WAL terminal record appears outside a batch"))
  (when *wal-recovery-logical-chunk-count*
    (error "logical WAL chunks appear before a non-commit terminal"))
  (when (and *wal-recovery-catalog-chunk-count*
             (not catalog-chunks-p))
    (error "WAL catalog chunks lack a catalog commit record"))
  ;; Validate the terminal and every spooled page before destructive resets.
  ;; Chunked catalog metadata must be reassembled and decoded first. Apply
  ;; resets before the batch's page images; an incomplete batch applies neither.
  (let ((terminal-function (funcall terminal-preparer))
        (*smgr-redo-p* t))
    (unless (functionp terminal-function)
      (error "WAL terminal validation did not produce an apply operation"))
    (when *wal-recovery-page-spool*
      (logical-spool-seal *wal-recovery-page-spool*)
      (wal-recovery-map-page-spool (lambda (images) (declare (ignore images)))))
    (wal-apply-relation-resets (mapcar #'cdr (reverse *wal-recovery-relation-resets*)))
    (if *wal-recovery-page-spool-enabled*
        (wal-recovery-map-page-spool #'wal-apply-page-images)
        (wal-apply-page-images (nreverse *wal-recovery-page-images*)))
    (funcall terminal-function))
  (wal-recovery-reset-batch nil))

(defun wal-recovery-decode-terminal (record)
  "Validate a complete native batch terminal and finish its staged resources."
  (multiple-value-bind (decoded length)
      (wal-decode-batch-terminal record *wal-recovery-group-active*
                                 *wal-recovery-reservation-batch-clean-p*
                                 *wal-recovery-catalog-chunk-count*
                                 *wal-recovery-relation-resets*
                                 *wal-recovery-logical-catalog-event-p*
                                 *wal-recovery-logical-chunk-count*)
    (let ((type (wal-record-type record)))
      (cond
        ((= type +wal-record-commit+)
         (wal-recovery-finish-logical-change-batch (wal-record-xid record))
         decoded)
        ((= type +wal-record-catalog-commit+)
         (wal-reassemble-catalog-chunks decoded length))
        (t decoded)))))

(defvar *wal-recovery-required-clog-reset* nil
  "First proven replacement required when startup found a short CLOG file.")

(defun wal-replay-terminal (record)
  (let* ((type (wal-record-type record))
         (decoded (wal-recovery-decode-terminal record)))
    (wal-replay-finish-batch
     (lambda ()
       (cond
         ((= type +wal-record-commit+)
          (lambda ()
            ;; Leave short CLOG files short until their proven replacement.
            (unless (and *wal-recovery-required-clog-reset*
                         (< (wal-record-end record) *wal-recovery-required-clog-reset*))
              (clog-set (wal-record-xid record) 1))))
         ((or (= type +wal-record-catalog+) (= type +wal-record-catalog-commit+))
          (wal-prepare-catalog-record decoded))
         ((= type +wal-record-clog-reset+)
          (lambda () (clog-reset :wal-reset-p nil)))
         ((= type +wal-record-xid-reservation+)
          (lambda ()
            (unless (recovery-in-progress-p) (xid-restore-reservation-floor decoded))))
         (t #'values)))
     (= type +wal-record-catalog-commit+))))

(defun wal-recovery-stage-batch-record (record page-handler)
  "Stage a nonterminal batch record, returning NIL for terminal or unknown types.
Redo and history supply their own page handling after the common ordering checks."
  (wal-note-recovery-batch-record record)
  (let ((type (wal-record-type record)))
    (cond
      ((= type +wal-record-batch-start+)
       ;; Discard staged images from a crash tail before opening the next batch.
       (wal-recovery-reset-batch t))
      ((= type +wal-record-relation-reset+)
       (wal-recovery-stage-relation-reset record))
      ((= type +wal-record-pages+)
       (wal-recovery-note-page-record)
       (funcall page-handler record))
      ((= type +wal-record-logical-changes+)
       (wal-recovery-stage-logical-changes record))
      ((= type +wal-record-logical-catalog-event+)
       (wal-recovery-note-logical-catalog-event record))
      ((= type +wal-record-catalog-chunk+)
       (wal-recovery-stage-catalog-chunk record))
      (t (return-from wal-recovery-stage-batch-record nil))))
  t)

(defun wal-recovery-stage-redo-pages (record)
  (if *wal-recovery-page-spool-enabled*
      (wal-recovery-stage-page-record record)
      (setf *wal-recovery-page-images*
            (nconc (reverse (wal-decode-page-images (wal-record-payload record)
                                                    (wal-record-end record)))
                   *wal-recovery-page-images*))))

(defun wal-replay-record (record)
  (when (and (> (wal-record-end record) *wal-checkpoint-lsn*)
             (not (wal-recovery-stage-batch-record record #'wal-recovery-stage-redo-pages))
             (wal-terminal-record-type-p (wal-record-type record)))
    (wal-replay-terminal record)))

(defun wal-recovery-close-staging ()
  "On scan exit, attempt every cleanup even if an earlier resource fails to close."
  (unwind-protect (wal-recovery-close-page-spool)
    (unwind-protect (wal-recovery-close-catalog-spool)
      (wal-recovery-reset-logical-change-batch))))

(defmacro with-wal-replay-staging (() &body body)
  "Isolate one native batch decoder; primary redo and history share this state."
  `(let ((*wal-recovery-reservation-batch-clean-p* nil)
         (*wal-recovery-group-active* nil)
         (*wal-recovery-page-images* '())
         (*wal-recovery-relation-resets* nil)
         (*wal-recovery-page-records-seen-p* nil)
         (*wal-recovery-page-spool* nil)
         (*wal-recovery-catalog-spool* nil)
         (*wal-recovery-catalog-chunk-count* nil)
         (*wal-recovery-catalog-next-chunk* 0)
         (*wal-recovery-logical-xid* nil)
         (*wal-recovery-logical-chunk-count* nil)
         (*wal-recovery-logical-next-chunk* 0)
         (*wal-recovery-logical-bytes* 0)
         (*wal-recovery-logical-catalog-event-p* nil))
     (unwind-protect
          (progn ,@body)
       (wal-recovery-close-staging))))

(defun wal-recovery-policy-incarnation (state incarnation record completed-p)
  "Carry private FPW evidence across a completed CLOG reset's identity change.
The caller supplies COMPLETED-P only after a validated terminal closes its batch.
A completed type-5 reset must rotate storage; any other rotation is refused.
Return the rebound evidence and current token, or the unchanged pair."
  (let ((current *wal-storage-incarnation*))
    (cond
      ((and completed-p (= (wal-record-type record) +wal-record-clog-reset+))
       (when (eq current incarnation)
         (error "completed CLOG reset did not replace its storage identity"))
       (values (wal-fpw-rebind-incarnation state incarnation current) current))
      ((not (eq current incarnation))
       (error "WAL recovery storage changed outside a completed CLOG reset"))
      (t (values state incarnation)))))

(defun wal-verify-checkpoint-clog-extent ()
  (let ((required (getf *wal-control-metadata* :checkpoint-clog-bytes)))
    (when (and required (< (or (clog-file-extent) 0) required))
      ;; An already-installed reset legitimately replaces the checkpoint's CLOG.
      ;; Replay reconstructs all committed statuses in the new epoch.
      (let ((reset-p nil))
        (wal-scan (lambda (record)
                    (when (and (> (wal-record-end record) *wal-checkpoint-lsn*)
                               (= (wal-record-type record) +wal-record-clog-reset+))
                      (setf reset-p t))))
        (unless reset-p
          (error "transaction status file pg_xact is truncated (checkpoint requires ~d bytes)"
                 required))))))

(defun wal-recover ()
  "Redo durable complete batches before loading the catalog.
Validate logical status privately before each new terminal's redo. Carry private
FPW evidence to a new token only after a validated CLOG reset terminal closes its
batch; reject other storage rotations. Publish FPW and logical status only after
the full scan and CLOG sync succeed. The caller must exclude storage lifecycle
changes throughout. Return the greatest CLOG reset boundary replayed after
the checkpoint, or NIL when no reset was replayed."
  (wal-verify-checkpoint-clog-extent)
  (let* ((incarnation *wal-storage-incarnation*)
         (fpw-state (wal-fpw-state-from-checkpoint *wal-checkpoint-snapshot* incarnation))
         (logical-status (wal-logical-status-from-checkpoint *wal-checkpoint-snapshot*))
         (*wal-recovery-page-spool-enabled* t)
         (resource-record nil)
         (reset-boundary nil))
    (with-wal-replay-staging ()
      (wal-scan
       (lambda (record)
         (unless (eq incarnation *wal-storage-incarnation*)
           (error "WAL recovery storage changed before replay"))
         (let* ((new-p (> (wal-record-end record) *wal-checkpoint-lsn*))
                (terminal-p (wal-terminal-record-type-p (wal-record-type record)))
                (next-logical-status
                  (if (and new-p terminal-p)
                      (wal-logical-status-next logical-status record *wal-insert-timeline*)
                      logical-status)))
           (when (and new-p (= (wal-record-type record) +wal-record-checkpoint+))
             (let ((snapshot (wal-checkpoint-record-snapshot record)))
               (when (getf snapshot :logical-on-start-lsn)
                 (timeline-check-resource-origin
                  snapshot (timeline-read-history *wal-insert-timeline*)))))
           (wal-replay-record record)
           (multiple-value-setq (fpw-state incarnation)
             (wal-recovery-policy-incarnation
              fpw-state incarnation record
              (and new-p terminal-p (not *wal-recovery-group-active*))))
           (when new-p
             (when (= (wal-record-type record) +wal-record-parameter-change+)
               (setf resource-record record))
             (when (and terminal-p (not *wal-recovery-group-active*))
               (when (= (wal-record-type record) +wal-record-clog-reset+)
                 (let ((boundary (wal-read-u64-payload (wal-record-payload record))))
                   (setf reset-boundary (max boundary (or reset-boundary 0)))))
               (setf fpw-state (wal-fpw-state-next
                                fpw-state record *wal-insert-timeline* incarnation)
                     logical-status next-logical-status)))))))
    (when resource-record
      (with-wal-critical (wal-recover-resource-parameters resource-record)))
    (clog-fsync)
    (with-wal-critical
      (wal-fpw-install-recovered-state fpw-state incarnation)
      (wal-install-recovered-logical-policy logical-status incarnation))
    reset-boundary))
