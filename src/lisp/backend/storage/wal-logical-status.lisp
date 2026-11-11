(in-package #:pg)

(defstruct (wal-logical-status
            (:constructor %make-wal-logical-status
                (enabled start end timeline last-enable-lsn last-enable-timeline)))
  "Immutable evidence from a checkpoint or status change, not published authority.
START, END and TIMELINE identify its latest witness. LAST-ENABLE-LSN and
LAST-ENABLE-TIMELINE identify the end of the latest observed enable record.
NIL for both means none was observed; it does not prove earlier continuity."
  (enabled nil :read-only t)
  (start 0 :read-only t)
  (end 0 :read-only t)
  (timeline 1 :read-only t)
  (last-enable-lsn nil :read-only t)
  (last-enable-timeline nil :read-only t))

(defun wal-encode-logical-decoding-status-change (enabled)
  "Encode Boolean ENABLED as a newly allocated one-byte status payload."
  (unless (member enabled '(nil t)) (error "invalid logical decoding status"))
  (make-array 1 :element-type '(unsigned-byte 8) :initial-element (if enabled 1 0)))

(defun wal-decode-logical-decoding-status-change (bytes)
  "Validate a one-byte status payload and return NIL for off or T for on."
  (unless (and (= (length bytes) 1) (member (aref bytes 0) '(0 1)))
    (error "invalid native logical decoding status change"))
  (= 1 (aref bytes 0)))

(defun wal-logical-decoding-status-record-info (record)
  "Validate a logical-status terminal and return its decoded Boolean value."
  (wal-require-boolean-change-envelope record +wal-record-logical-decoding-status-change+ 1)
  (wal-decode-logical-decoding-status-change (wal-record-payload record)))

(defun wal-require-logical-status-physical-replay-supported (record)
  "Validate a logical-status terminal for physical replay; accept other types.
This gate does not persist or publish the status fact."
  (when (= (wal-record-type record) +wal-record-logical-decoding-status-change+)
    (wal-logical-decoding-status-record-info record))
  t)

(defun wal-logical-status-valid-p (state)
  "Check evidence shape and numeric ordering; this does not prove ancestry."
  (and (wal-logical-status-p state)
       (member (wal-logical-status-enabled state) '(nil t))
       (typep (wal-logical-status-start state) '(unsigned-byte 64))
       (typep (wal-logical-status-end state) '(unsigned-byte 64))
       (< (wal-logical-status-start state) (wal-logical-status-end state))
       (typep (wal-logical-status-timeline state) '(integer 1 4294967295))
       (let ((lsn (wal-logical-status-last-enable-lsn state))
             (timeline (wal-logical-status-last-enable-timeline state)))
         (or (and (null lsn) (null timeline))
             (and (typep lsn '(integer 1 18446744073709551615))
                  (typep timeline '(integer 1 4294967295))
                  (<= lsn (wal-logical-status-end state))
                  (<= timeline (wal-logical-status-timeline state)))))))

(defun wal-logical-status-from-checkpoint (snapshot)
  "Seed private evidence from a checkpoint, or return NIL when status is absent.
A present NIL is known off. A checkpoint marked on has no observed enable
position, and does not establish earlier continuity."
  (when snapshot
    (unless (wal-checkpoint-snapshot-valid-p snapshot)
      (error "invalid logical status checkpoint seed"))
    (when (get-properties snapshot '(:logical-decoding-enabled))
      (%make-wal-logical-status
       (getf snapshot :logical-decoding-enabled)
       (getf snapshot :start) (getf snapshot :end) (getf snapshot :timeline) nil nil))))

(defun wal-logical-status-next (prior record timeline)
  "Fold a later record into private logical-status evidence without publication.
When PRIOR is present, records must start at or after its witness end. The
caller proves ancestry and complete batch order. An absent seed is unknown;
records without facts preserve prior evidence. A present NIL is known off.
A checkpoint fact must agree with prior known status. Every enable record
updates the last-enable end and timeline, even if status was already on."
  (unless (and (typep timeline '(integer 1 4294967295))
               (or (null prior)
                   (and (wal-logical-status-valid-p prior)
                        (<= (wal-logical-status-end prior) (wal-record-start record))
                        (<= (wal-logical-status-timeline prior) timeline))))
    (error "logical status seed belongs to a later replay position"))
  (let* ((type (wal-record-type record))
         (checkpoint (when (= type +wal-record-checkpoint+)
                       (wal-checkpoint-record-snapshot record)))
         (change-p (= type +wal-record-logical-decoding-status-change+))
         (known-p (or change-p (get-properties checkpoint '(:logical-decoding-enabled)))))
    (if (not known-p) prior
        (let ((enabled (if change-p (wal-logical-decoding-status-record-info record)
                           (getf checkpoint :logical-decoding-enabled))))
          (when (and checkpoint
                     (or (/= timeline (getf checkpoint :timeline))
                         (and prior (not (eq enabled (wal-logical-status-enabled prior))))))
            (error "checkpoint logical status differs from preceding WAL"))
          (%make-wal-logical-status
           enabled (wal-record-start record) (wal-record-end record) timeline
           (if (and change-p enabled) (wal-record-end record)
               (and prior (wal-logical-status-last-enable-lsn prior)))
           (if (and change-p enabled) timeline
               (and prior (wal-logical-status-last-enable-timeline prior))))))))
