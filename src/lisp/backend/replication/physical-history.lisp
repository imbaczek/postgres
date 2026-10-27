;;;; Read-only native catalog history validation before standby redo.
(in-package #:pg)

(defmacro with-physical-replay-staging (() &body body)
  `(with-wal-replay-staging () ,@body))

(defun physical-history-reset (active)
  (unless active (setf *wal-recovery-reservation-batch-clean-p* nil))
  (setf *wal-recovery-page-images* nil
        *wal-recovery-group-active* active
        *wal-recovery-catalog-chunks* nil
        *wal-recovery-catalog-chunk-count* nil
        *wal-recovery-catalog-next-chunk* 0
        *wal-recovery-logical-catalog-event-p* nil)
  (wal-recovery-reset-logical-change-batch))

(defun physical-history-check-plist (value required)
  "Reject ambiguous keys before using serialized catalog entries as drop proof."
  (unless (and (listp value) (evenp (length value)))
    (error "invalid catalog history property list"))
  (let ((seen (make-hash-table :test #'eq)))
    (loop for (key item) on value by #'cddr
          do (unless (and (keywordp key) (not (gethash key seen)))
               (error "duplicate or invalid catalog history key"))
             (setf (gethash key seen) t))
    (unless (every (lambda (key) (gethash key seen)) required)
      (error "catalog history lacks required metadata")))
  value)

(defun physical-history-read-catalog (bytes)
  "Read one catalog form without evaluation or circular reader-label objects.
Native readable array syntax remains accepted, including SBCL string arrays."
  (with-standard-io-syntax
    (let ((*read-eval* nil) (*readtable* (copy-readtable nil)))
      (flet ((reject-label (stream character argument)
               (declare (ignore stream character argument))
               (error "reader labels are not allowed in catalog history")))
        (set-dispatch-macro-character #\# #\= #'reject-label)
        (set-dispatch-macro-character #\# #\# #'reject-label))
      (with-input-from-string (stream (bytes->string bytes))
        (let* ((end (gensym))
               (image (read stream nil end)))
          (unless (eq (read stream nil end) end)
            (error "catalog history has trailing forms"))
          image)))))

(defun physical-history-catalog-relfilenodes (bytes identity)
  "Read exactly one source catalog and retain every serialized fork reference.
Do not round-trip through catalog hash tables: duplicate names must not hide
a still-referenced relfilenode when checking a terminal's delete list."
  (let ((image (physical-history-read-catalog bytes))
        (live (make-hash-table :test #'eql)))
    (physical-history-check-plist image '(:cluster-identity :next-oid :databases))
    (unless (and (equal identity (getf image :cluster-identity))
                 (typep (getf image :next-oid) '(unsigned-byte 32)))
      (error "catalog history has a foreign or invalid identity"))
    (dolist (database (getf image :databases))
      (physical-history-check-plist database '(:name :tables :indexes))
      (dolist (relation (append (getf database :tables) (getf database :indexes)))
        (physical-history-check-plist relation '(:name :relfilenode))
        (let ((node (getf relation :relfilenode)))
          (unless (typep node '(integer 1 4294967295))
            (error "catalog history has an invalid relfilenode"))
          (setf (gethash node live) t))))
    live))

(defun physical-history-accept-catalog (payload identity removed)
  (multiple-value-bind (deletes bytes) (wal-decode-catalog-payload payload)
    (let ((live (physical-history-catalog-relfilenodes bytes identity)))
      (dolist (node deletes)
        (when (or (zerop node) (gethash node live))
          (error "catalog history deletes a referenced or invalid relfilenode")))
      ;; A later reference revokes earlier deletion proof, even though the
      ;; native allocator normally never reuses a published relfilenode.
      (maphash (lambda (node present) (declare (ignore present)) (remhash node removed)) live)
      (dolist (node deletes) (setf (gethash node removed) t)))))

(defun physical-history-accept-chunk (record)
  (unless *wal-recovery-group-active*
    (error "catalog history chunk appears outside a batch"))
  (when (or *wal-recovery-logical-catalog-event-p* *wal-recovery-logical-chunk-count*)
    (error "catalog history chunks are mixed with logical events"))
  (multiple-value-bind (index count bytes)
      (wal-decode-catalog-chunk-payload (wal-record-payload record))
    (unless (and (= index *wal-recovery-catalog-next-chunk*)
                 (or (null *wal-recovery-catalog-chunk-count*)
                     (= count *wal-recovery-catalog-chunk-count*)))
      (error "catalog history chunk sequence is invalid"))
    (setf *wal-recovery-catalog-chunk-count* count)
    (incf *wal-recovery-catalog-next-chunk*)
    (push bytes *wal-recovery-catalog-chunks*)))

(defun physical-history-accept-logical-event (record)
  (unless (and *wal-recovery-group-active* (zerop (wal-record-xid record))
               (not *wal-recovery-logical-catalog-event-p*)
               (not *wal-recovery-logical-chunk-count*))
    (error "invalid logical catalog event in history"))
  (let ((payload (wal-record-payload record)))
    (unless (and (>= (length payload) 8) (plusp (wal-u32 payload 0))
                 (= (+ 8 (wal-u32 payload 4)) (length payload)))
      (error "invalid logical catalog event payload in history")))
  (setf *wal-recovery-logical-catalog-event-p* t))

(defun physical-history-accept-terminal (record identity removed)
  (unless *wal-recovery-group-active*
    (error "catalog history terminal appears outside a batch"))
  (let ((type (wal-record-type record))
        (payload (wal-record-payload record)))
    (when (and *wal-recovery-catalog-chunk-count* (/= type +wal-record-catalog-commit+))
      (error "catalog history chunks lack their terminal"))
    (cond
      ((= type +wal-record-commit+)
       (wal-decode-commit-timestamp payload)
       (wal-recovery-validate-logical-terminal :commit (wal-record-xid record)))
      ((member type (list +wal-record-catalog+ +wal-record-catalog-commit+))
       (wal-recovery-validate-logical-terminal :catalog)
       (physical-history-accept-catalog
        (if (= type +wal-record-catalog+) payload
            (multiple-value-bind (count length) (wal-decode-catalog-commit-payload payload)
              (wal-reassemble-catalog-chunks count length)))
        identity removed))
      (t
       (wal-recovery-validate-logical-terminal :other)
       (cond ((= type +wal-record-running-xacts+)
              (unless *wal-recovery-reservation-batch-clean-p*
                (error "running transaction snapshot cannot finish a nonempty batch"))
              (wal-running-xacts-record-info record))
             ((= type +wal-record-xid-reservation+)
              (unless *wal-recovery-reservation-batch-clean-p*
                (error "reservation cannot finish a nonempty batch in history"))
              (wal-xid-reservation-record-bound record))
             ((= type +wal-record-clog-reset+) (wal-read-u64-payload payload))
             ((= type +wal-record-checkpoint+) (wal-checkpoint-record-snapshot record))
             ((not (and (zerop (length payload)) (zerop (wal-record-xid record))))
              (error "invalid noncatalog terminal in history"))))))
  (physical-history-reset nil))

(defun physical-history-accept-record (record identity removed)
  "Validate native batch ordering and payloads without applying any storage."
  (wal-note-recovery-batch-record record)
  (let ((type (wal-record-type record)))
    (cond
      ((= type +wal-record-batch-start+) (physical-history-reset t))
      ((= type +wal-record-pages+)
       (unless (and *wal-recovery-group-active* (not *wal-recovery-catalog-chunk-count*))
         (error "page images outside a batch or after catalog chunks in history"))
       (wal-decode-page-images (wal-record-payload record)
                               (wal-record-end record) (wal-record-version record)))
      ((= type +wal-record-logical-changes+)
       (when (or *wal-recovery-logical-catalog-event-p* *wal-recovery-catalog-chunk-count*)
         (error "logical row changes after catalog metadata in history"))
       (wal-recovery-accept-logical-change-chunk record))
      ((= type +wal-record-logical-catalog-event+) (physical-history-accept-logical-event record))
      ((= type +wal-record-catalog-chunk+) (physical-history-accept-chunk record))
      ((wal-terminal-record-type-p type)
       (physical-history-accept-terminal record identity removed))
      (t (error "unsupported record in catalog history")))))

(declaim (ftype function physical-replay-check-selected-eor))

(defun physical-recovery-proven-drops (store identity &key
                                                        (start-lsn (physical-store-floor-lsn store)) required-lsn)
  "Certify drops only from complete catalog terminals in validated durable WAL.
An unfinished batch grants no exemption. REQUIRED-LSN must be a complete terminal.
Return the deletion set and last terminal. No database files are written."
  (with-physical-replay-staging ()
    (let ((removed (make-hash-table :test #'eql))
          (terminal start-lsn)
          (timeline (physical-store-prefix-timeline store start-lsn))
          (required-found (eql required-lsn start-lsn)))
      (physical-store-map-durable-records
       store (lambda (record raw)
               (if (= (wal-record-type record) +wal-record-end-of-recovery+)
                   (setf timeline (physical-replay-check-selected-eor store record raw terminal timeline))
                   (physical-history-accept-record record identity removed))
               (unless *wal-recovery-group-active*
                 (setf terminal (wal-record-end record))
                 (when (eql required-lsn terminal) (setf required-found t))))
       :start-lsn start-lsn)
      (when (and required-lsn (not required-found))
        (error "durable WAL lost a previously applied terminal"))
      (values removed terminal))))
