(in-package #:pg)

(defstruct (logical-slot-startup-progress (:constructor make-logical-slot-startup-progress (confirmed)))
  ;; FIRST holds first XID contributions from completed batches.  STAGED
  ;; belongs to the current batch and cannot contribute until its terminal.
  confirmed batch-start legacy-first staged-legacy-first
  (first (make-hash-table :test #'eql))
  (staged (make-hash-table :test #'eql))
  (pending (make-hash-table :test #'eql)))

(defun logical-slot-startup-stage-pages (progress record)
  (unless (logical-slot-startup-progress-batch-start progress)
    (error "startup page images appear outside a batch"))
  (let ((images (wal-decode-page-images (wal-record-payload record)
                                        (wal-record-end record) (wal-record-version record)))
        (start (logical-slot-startup-progress-batch-start progress)))
    (if (>= (wal-record-version record) +wal-version-v4+)
        (dolist (image images)
          (dolist (xid (wal-page-image-xids image))
            (unless (gethash xid (logical-slot-startup-progress-staged progress))
              (setf (gethash xid (logical-slot-startup-progress-staged progress)) start))))
        (setf (logical-slot-startup-progress-staged-legacy-first progress) start))))

(defun logical-slot-startup-finish-batch (progress record)
  (unless (logical-slot-startup-progress-batch-start progress)
    (error "startup terminal appears outside a batch"))
  (let ((type (wal-record-type record)))
    (cond
      ((= type +wal-record-commit+) (wal-decode-commit-timestamp (wal-record-payload record)))
      ((wal-empty-metadata-terminal-type-p type)
       (wal-validate-empty-metadata-terminal record *wal-recovery-reservation-batch-clean-p*)))
    (maphash
     (lambda (xid start)
       (unless (gethash xid (logical-slot-startup-progress-first progress))
         (setf (gethash xid (logical-slot-startup-progress-first progress)) start)))
     (logical-slot-startup-progress-staged progress))
    (unless (logical-slot-startup-progress-legacy-first progress)
      (setf (logical-slot-startup-progress-legacy-first progress)
            (logical-slot-startup-progress-staged-legacy-first progress)))
    (when (= type +wal-record-commit+)
      (let* ((xid (wal-record-xid record))
             (first (or (gethash xid (logical-slot-startup-progress-first progress))
                        (logical-slot-startup-progress-legacy-first progress)))
             (confirmed (logical-slot-startup-progress-confirmed progress)))
        (when (and (plusp xid) first (or (null confirmed) (> (wal-record-end record) confirmed)))
          (setf (gethash xid (logical-slot-startup-progress-pending progress))
                (cons first (wal-record-end record))))))
    (setf (logical-slot-startup-progress-batch-start progress) nil)
    (clrhash (logical-slot-startup-progress-staged progress))
    (setf (logical-slot-startup-progress-staged-legacy-first progress) nil)))

(defun logical-slot-startup-accept-record (progress record kind)
  ;; An end-of-recovery record marks a timeline transition, not an ACK.
  (when (eq kind :end-of-recovery)
    (when (logical-slot-startup-progress-batch-start progress)
      (error "startup end-of-recovery interrupts a batch"))
    (return-from logical-slot-startup-accept-record (values)))
  (wal-note-recovery-batch-record record)
  (let ((type (wal-record-type record)))
    (cond
      ((= type +wal-record-batch-start+)
       (setf (logical-slot-startup-progress-batch-start progress) (wal-record-start record)
             (logical-slot-startup-progress-staged-legacy-first progress) nil)
       (clrhash (logical-slot-startup-progress-staged progress)))
      ((not (logical-slot-startup-progress-batch-start progress))
       (error "startup WAL record appears outside a batch"))
      ((= type +wal-record-pages+) (logical-slot-startup-stage-pages progress record))
      ((wal-terminal-record-type-p type) (logical-slot-startup-finish-batch progress record))))
  (values))

(defun logical-slot-startup-next-record (source cursor)
  (logical-slot-startup-check-authority source)
  (multiple-value-prog1 (logical-wal-cursor-next cursor)
    (logical-slot-startup-check-authority source)))

(defun logical-slot-startup-derive-pending (source slot &optional archive)
  ;; Scan through the frozen durable end.  The logical reader checks record
  ;; framing, carrier and CRC, and accepts an open tail of complete records.
  ;; An open or abandoned batch contributes no staged first positions.  This
  ;; scan does not claim full logical payload semantic validation.
  (logical-slot-startup-check-authority source)
  (unless (and (some (lambda (entry) (eq slot (logical-slot-startup-entry-slot entry)))
                     (logical-slot-startup-source-entries source))
               (logical-slot-startup-known-p slot)
               (eq :reserved (wal-replication-slot-wal-status slot)))
    (error "startup cursor requires its captured reserved logical candidate"))
  (let ((history (logical-slot-startup-source-history source))
        (progress (make-logical-slot-startup-progress (wal-replication-slot-confirmed-flush-lsn slot)))
        (*wal-recovery-reservation-batch-clean-p* nil))
    (multiple-value-bind (owner role) (logical-slot-startup-start-position slot history)
      (call-with-logical-wal-cursor
       history (logical-slot-startup-source-namespace source)
       (wal-replication-slot-restart-lsn slot) (logical-slot-startup-source-end source)
       owner role (lambda () (logical-slot-startup-check-source source))
       (lambda (cursor)
         (loop
           (multiple-value-bind (record bytes record-owner carrier kind)
               (logical-slot-startup-next-record source cursor)
             (declare (ignore bytes record-owner carrier))
             (unless record (return))
             (logical-slot-startup-accept-record progress record kind))))
       :archive archive :batch-policy :logical-reader))
    (logical-slot-startup-progress-pending progress)))

(defun logical-slot-on-coverage
    (state history required owner role through through-owner)
  ;; The caller must provide the latest logical status from complete ordered
  ;; replay through the frozen positive THROUGH prefix, with verified source and
  ;; history.  This helper does not establish those facts; STATE's end being within
  ;; THROUGH alone does not prove it is latest.
  ;; Return :proved-on, :insufficient, or :unknown; this is not an admission
  ;; certificate.  Compare the uninterrupted ON origin, not a redundant ON.
  ;; Known OFF or an ON origin after REQUIRED is insufficient; absent state,
  ;; legacy ON without an origin, or REQUIRED beyond replay is unknown.
  ;; REQUIRED may equal THROUGH with the same owner.  A child record start at
  ;; the parent prefix fork is unknown, even with known OFF.  Malformed positions
  ;; or witnesses fail range, timeline, or segment checks with an error.
  ;; A local slot uses birth for lifetime, separately from older restart retention;
  ;; imported or remote anchors require separate provenance authority.
  (timeline-check-lineage history)
  (flet ((check-position (lsn timeline position-role)
           (unless
               (and (logical-slot-position-valid-p lsn timeline)
                    (case position-role
                      (:record-start (eql timeline (timeline-at-point lsn history)))
                      (:prefix-end (and (plusp lsn)
                                        (eql timeline (timeline-at-point (1- lsn) history))))))
             (error "logical ON coverage requires valid selected-history positions"))))
    (check-position through through-owner :prefix-end)
    (check-position required owner role)
    (when state
      (unless (and (wal-logical-status-valid-p state)
                   (<= (wal-logical-status-end state) through))
        (error "logical status witness is outside frozen replay prefix"))
      (physical-policy-check-record-owner
       (wal-logical-status-start state) (wal-logical-status-end state)
       (wal-logical-status-timeline state) history)
      (when (wal-logical-status-on-start-lsn state)
        (physical-policy-check-record-owner
         (- (wal-logical-status-on-start-lsn state) (1+ +wal-header-size+))
         (wal-logical-status-on-start-lsn state)
         (wal-logical-status-on-start-timeline state) history))
      (when (wal-logical-status-last-enable-lsn state)
        (physical-policy-check-record-owner
         (- (wal-logical-status-last-enable-lsn state) (1+ +wal-header-size+))
         (wal-logical-status-last-enable-lsn state)
         (wal-logical-status-last-enable-timeline state) history)))
    (cond
      ((or (> required through)
           (and (= required through) (not (eql owner through-owner))))
       :unknown)
      ((null state) :unknown)
      ((not (wal-logical-status-enabled state)) :insufficient)
      ((null (wal-logical-status-on-start-lsn state)) :unknown)
      ((<= (wal-logical-status-on-start-lsn state) required) :proved-on)
      (t :insufficient))))

(defun logical-slot-startup-check-eligibility (source slot)
  ;; Reserved slots currently require local anchors and current logical ON.
  ;; Ancestral or imported generation and lifetime, and uninterrupted ON/OFF
  ;; evidence, require a later eligibility proof.
  (unless (logical-slot-startup-local-anchor-p slot (logical-slot-startup-source-history source))
    (error "ancestral logical slot startup requires lifetime and import eligibility"))
  (sb-thread:with-mutex (*wal-lock*)
    (logical-slot-startup-check-source source)
    (let* ((policy (wal-checked-logical-policy-under-lock))
           (state (and policy (wal-logical-policy-state policy))))
      (unless (and state (wal-logical-status-enabled state))
        (error "reserved logical slot startup requires current logical ON evidence"))))
  slot)
