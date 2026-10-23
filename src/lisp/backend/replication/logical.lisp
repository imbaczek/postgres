;;;; logical.lisp -- transactional row-change records and slot reads.

(in-package #:pg)

(defconstant +max-logical-transaction-bytes+ (* 64 1024 1024))
(defconstant +max-logical-read-transactions+ 128)
(defconstant +max-logical-read-bytes+ (* 8 1024 1024))
(defconstant +max-logical-read-scan-bytes+ (* 16 1024 1024))
(defconstant +logical-change-chunk-header-size+ 12)

(defvar *wal-logical-slot-read-progress* nil)

(defstruct logical-change
  kind database-oid relation-oid relation-name columns old-values new-values)

(defstruct logical-transaction
  xid commit-lsn changes)

(defstruct logical-cursor
  bytes (offset 0))

(defstruct logical-reader-state
  database-oid batch-open-p xid chunk-count next-chunk
  (change-bytes 0) (changes '()) (transactions '()))

(defun logical-type-code (type)
  (case type
    (:int4 1)
    (:int8 2)
    (:varchar 3)
    (otherwise (error "logical replication does not support type ~a" type))))

(defun logical-type-from-code (code)
  (case code
    (1 :int4)
    (2 :int8)
    (3 :varchar)
    (otherwise (error "invalid logical column type code ~a" code))))

(defun logical-kind-code (kind)
  (case kind
    (:insert 1)
    (:update 2)
    (:delete 3)
    (otherwise (error "invalid logical row change kind ~a" kind))))

(defun logical-kind-from-code (code)
  (case code
    (1 :insert)
    (2 :update)
    (3 :delete)
    (otherwise (error "invalid logical row change code ~a" code))))

(defun logical-write-u8 (buffer value)
  (unless (and (integerp value) (<= 0 value #xFF))
    (error "logical WAL byte is out of range: ~a" value))
  (vector-push-extend value buffer))

(defun logical-write-u16 (buffer value)
  (unless (and (integerp value) (<= 0 value #xFFFF))
    (error "logical WAL 16-bit value is out of range: ~a" value))
  (vector-push-extend (logand value #xFF) buffer)
  (vector-push-extend (logand (ash value -8) #xFF) buffer))

(defun logical-write-u32 (buffer value)
  (unless (and (integerp value) (<= 0 value #xFFFFFFFF))
    (error "logical WAL 32-bit value is out of range: ~a" value))
  (dotimes (shift 4)
    (vector-push-extend (logand (ash value (* -8 shift)) #xFF) buffer)))

(defun logical-write-u64 (buffer value)
  (unless (and (integerp value) (<= 0 value #xFFFFFFFFFFFFFFFF))
    (error "logical WAL 64-bit value is out of range: ~a" value))
  (dotimes (shift 8)
    (vector-push-extend (logand (ash value (* -8 shift)) #xFF) buffer)))

(defun logical-write-octets (buffer octets)
  (loop for byte across octets do (vector-push-extend byte buffer))
  buffer)

(defun logical-write-string-u32 (buffer string)
  (let ((octets (sb-ext:string-to-octets string :external-format :utf-8)))
    (logical-write-u32 buffer (length octets))
    (logical-write-octets buffer octets)))

(defun logical-write-value (buffer type value)
  (cond
    ((eq value +null+)
     (logical-write-u8 buffer 0))
    ((eq type :int4)
     (unless (and (integerp value) (<= +int4-min+ value +int4-max+))
       (error "invalid int4 logical row value ~s" value))
     (logical-write-u8 buffer 1)
     (logical-write-u32 buffer (logand value #xFFFFFFFF)))
    ((eq type :int8)
     (unless (and (integerp value) (<= +int8-min+ value +int8-max+))
       (error "invalid int8 logical row value ~s" value))
     (logical-write-u8 buffer 2)
     (logical-write-u64 buffer (logand value #xFFFFFFFFFFFFFFFF)))
    ((eq type :varchar)
     (unless (stringp value)
       (error "invalid varchar logical row value ~s" value))
     (let ((octets (sb-ext:string-to-octets value :external-format :utf-8)))
       (logical-write-u8 buffer 3)
       (logical-write-u32 buffer (length octets))
       (logical-write-octets buffer octets)))
    (t (error "logical replication does not support type ~a" type))))

(defun logical-write-row (buffer columns values)
  (unless (and values (= (length columns) (length values)))
    (error "logical row width does not match its relation description"))
  (loop for column in columns
        for value across values
        do (logical-write-value buffer (getf column :type) value)))

(defun logical-encode-change (change)
  (let ((buffer (make-array 0 :element-type '(unsigned-byte 8)
                              :adjustable t :fill-pointer 0))
        (columns (logical-change-columns change)))
    (logical-write-u8 buffer (logical-kind-code (logical-change-kind change)))
    (logical-write-u32 buffer (logical-change-database-oid change))
    (logical-write-u32 buffer (logical-change-relation-oid change))
    (logical-write-string-u32 buffer (logical-change-relation-name change))
    (logical-write-u16 buffer (length columns))
    (dolist (column columns)
      (logical-write-u16 buffer (getf column :attnum))
      (logical-write-string-u32 buffer (getf column :name))
      (logical-write-u8 buffer (logical-type-code (getf column :type)))
      (logical-write-u32 buffer (logand (getf column :typmod) #xFFFFFFFF))
      (logical-write-u8 buffer (if (getf column :notnull) 1 0)))
    (dolist (values (list (logical-change-old-values change)
                          (logical-change-new-values change)))
      (logical-write-u8 buffer (if values 1 0))
      (when values (logical-write-row buffer columns values)))
    (unless (case (logical-change-kind change)
              (:insert (and (null (logical-change-old-values change))
                            (logical-change-new-values change)))
              (:update (and (logical-change-old-values change)
                            (logical-change-new-values change)))
              (:delete (and (logical-change-old-values change)
                            (null (logical-change-new-values change)))))
      (error "logical row change has inconsistent old/new images"))
    buffer))

(defun logical-cursor-require (cursor count)
  (unless (<= (+ (logical-cursor-offset cursor) count)
              (length (logical-cursor-bytes cursor)))
    (error "truncated logical row-change payload")))

(defun logical-read-u8 (cursor)
  (logical-cursor-require cursor 1)
  (prog1 (aref (logical-cursor-bytes cursor) (logical-cursor-offset cursor))
    (incf (logical-cursor-offset cursor))))

(defun logical-read-u16 (cursor)
  (logical-cursor-require cursor 2)
  (prog1 (wal-u16 (logical-cursor-bytes cursor) (logical-cursor-offset cursor))
    (incf (logical-cursor-offset cursor) 2)))

(defun logical-read-u32 (cursor)
  (logical-cursor-require cursor 4)
  (prog1 (wal-u32 (logical-cursor-bytes cursor) (logical-cursor-offset cursor))
    (incf (logical-cursor-offset cursor) 4)))

(defun logical-read-u64 (cursor)
  (logical-cursor-require cursor 8)
  (prog1 (wal-u64 (logical-cursor-bytes cursor) (logical-cursor-offset cursor))
    (incf (logical-cursor-offset cursor) 8)))

(defun logical-read-octets (cursor count)
  (logical-cursor-require cursor count)
  (prog1 (subseq (logical-cursor-bytes cursor)
                 (logical-cursor-offset cursor)
                 (+ (logical-cursor-offset cursor) count))
    (incf (logical-cursor-offset cursor) count)))

(defun logical-read-string-u32 (cursor)
  (sb-ext:octets-to-string (logical-read-octets cursor (logical-read-u32 cursor))
                           :external-format :utf-8))

(defun logical-u32-signed (value)
  (if (logbitp 31 value) (- value #x100000000) value))

(defun logical-u64-signed (value)
  (if (logbitp 63 value) (- value #x10000000000000000) value))

(defun logical-read-value (cursor type)
  (let ((tag (logical-read-u8 cursor)))
    (case tag
      (0 +null+)
      (1 (unless (eq type :int4)
           (error "logical value tag does not match its column type"))
       (logical-u32-signed (logical-read-u32 cursor)))
      (2 (unless (eq type :int8)
           (error "logical value tag does not match its column type"))
       (logical-u64-signed (logical-read-u64 cursor)))
      (3 (unless (eq type :varchar)
           (error "logical value tag does not match its column type"))
       (sb-ext:octets-to-string
        (logical-read-octets cursor (logical-read-u32 cursor))
        :external-format :utf-8))
      (otherwise (error "invalid logical value tag ~a" tag)))))

(defun logical-read-row (cursor columns)
  (coerce (loop for column in columns
                collect (logical-read-value cursor (getf column :type)))
          'vector))

(defun logical-decode-change (bytes)
  (let* ((cursor (make-logical-cursor :bytes bytes))
         (kind (logical-kind-from-code (logical-read-u8 cursor)))
         (database-oid (logical-read-u32 cursor))
         (relation-oid (logical-read-u32 cursor))
         (relation-name (logical-read-string-u32 cursor))
         (column-count (logical-read-u16 cursor))
         (columns
           (loop repeat column-count
                 collect (list :attnum (logical-read-u16 cursor)
                               :name (logical-read-string-u32 cursor)
                               :type (logical-type-from-code
                                      (logical-read-u8 cursor))
                               :typmod (logical-u32-signed
                                        (logical-read-u32 cursor))
                               :notnull (case (logical-read-u8 cursor)
                                          (0 nil)
                                          (1 t)
                                          (otherwise
                                           (error "invalid logical column flags"))))))
         (rows
           (loop repeat 2
                 collect (case (logical-read-u8 cursor)
                           (0 nil)
                           (1 (logical-read-row cursor columns))
                           (otherwise (error "invalid logical row-image flag")))))
         (change (make-logical-change :kind kind
                                      :database-oid database-oid
                                      :relation-oid relation-oid
                                      :relation-name relation-name
                                      :columns columns
                                      :old-values (first rows)
                                      :new-values (second rows))))
    (unless (= (logical-cursor-offset cursor) (length bytes))
      (error "logical row-change payload has trailing bytes"))
    (unless (case kind
              (:insert (and (null (logical-change-old-values change))
                            (logical-change-new-values change)))
              (:update (and (logical-change-old-values change)
                            (logical-change-new-values change)))
              (:delete (and (logical-change-old-values change)
                            (null (logical-change-new-values change)))))
      (error "logical row change has inconsistent old/new images"))
    change))

(defun logical-note-row-change (kind table &key old-values new-values)
  "Capture a successful executor row operation for its current transaction.

PostgreSQL's logical decode path queues heap changes in DecodeInsert,
DecodeUpdate, and DecodeDelete; this subset already has decoded values in the
executor, so capture the same operation before transaction commit."
  (when (and *backend* (backend-xid *backend*))
    (let* ((columns
             (loop for column across (pg-class-columns table)
                   collect (list :attnum (pg-column-attnum column)
                                 :name (copy-seq (pg-column-name column))
                                 :type (pg-column-type column)
                                 :typmod (pg-column-typmod column)
                                 :notnull (pg-column-notnull column))))
           (change (make-logical-change
                    :kind kind
                    :database-oid (database-oid (current-database))
                    :relation-oid (pg-class-oid table)
                    :relation-name (copy-seq (pg-class-name table))
                    :columns columns
                    :old-values (and old-values (copy-seq old-values))
                    :new-values (and new-values (copy-seq new-values))))
           (encoded (logical-encode-change change))
           (total (+ (backend-logical-change-bytes *backend*)
                     (length encoded))))
      (when (> total +max-logical-transaction-bytes+)
        (ereport "54000"
                 "logical change stream transaction exceeds the 64 MiB limit"))
      (setf (backend-logical-changes *backend*)
            (cons encoded (backend-logical-changes *backend*))
            (backend-logical-change-bytes *backend*) total)))
  (values))

(defun logical-encode-chunk (index count events)
  (let ((buffer (make-array 0 :element-type '(unsigned-byte 8)
                              :adjustable t :fill-pointer 0)))
    (logical-write-u32 buffer index)
    (logical-write-u32 buffer count)
    (logical-write-u32 buffer (length events))
    (dolist (event events)
      (logical-write-u32 buffer (length event))
      (logical-write-octets buffer event))
    buffer))

(defun logical-split-encoded-changes (events)
  (let ((payload-limit (- +wal-segment-size+ +wal-header-size+
                          +logical-change-chunk-header-size+))
        (chunks '())
        (chunk '())
        (chunk-bytes 0))
    (dolist (event events)
      (let ((entry-bytes (+ 4 (length event))))
        (when (> entry-bytes payload-limit)
          (error "one logical row change exceeds the maximum WAL record size"))
        (when (and chunk (> (+ chunk-bytes entry-bytes) payload-limit))
          (push (nreverse chunk) chunks)
          (setf chunk nil chunk-bytes 0))
        (push event chunk)
        (incf chunk-bytes entry-bytes)))
    (when chunk (push (nreverse chunk) chunks))
    (nreverse chunks)))

(defun wal-append-logical-changes (xid encoded-changes)
  "Append ordered, bounded chunks that remain invisible until XID commits."
  (when encoded-changes
    (unless (and (integerp xid) (plusp xid))
      (error "logical row changes require a transaction ID"))
    (let* ((events encoded-changes)
           (event-bytes (reduce #'+ events :key #'length :initial-value 0)))
      (when (> event-bytes +max-logical-transaction-bytes+)
        (error "logical transaction exceeds its configured change limit"))
      (let* ((chunks (logical-split-encoded-changes events))
             (chunk-count (length chunks)))
        (loop for chunk in chunks
              for index from 0
              do (wal-append-record +wal-record-logical-changes+ xid
                                    (logical-encode-chunk index chunk-count chunk))))))
  (values))

(defun wal-decode-logical-change-chunk (payload)
  (let* ((cursor (make-logical-cursor :bytes payload))
         (index (logical-read-u32 cursor))
         (count (logical-read-u32 cursor))
         (event-count (logical-read-u32 cursor))
         (change-bytes 0)
         (changes '()))
    (unless (and (plusp count) (< index count) (plusp event-count))
      (error "invalid logical WAL chunk header"))
    (loop repeat event-count
          for length = (logical-read-u32 cursor)
          for event = (logical-read-octets cursor length)
          do (incf change-bytes length)
             (push (logical-decode-change event) changes))
    (unless (= (logical-cursor-offset cursor) (length payload))
      (error "logical WAL chunk has trailing bytes"))
    (values index count (nreverse changes) change-bytes)))

(defun wal-recovery-reset-logical-change-batch ()
  (setf *wal-recovery-logical-xid* nil
        *wal-recovery-logical-chunk-count* nil
        *wal-recovery-logical-next-chunk* 0
        *wal-recovery-logical-bytes* 0)
  (values))

(defun wal-recovery-accept-logical-change-chunk (record)
  (unless *wal-recovery-group-active*
    (error "logical row changes appear outside a WAL batch"))
  (multiple-value-bind (index count changes event-bytes)
      (wal-decode-logical-change-chunk (wal-record-payload record))
    (declare (ignore changes))
    (unless (plusp (wal-record-xid record))
      (error "logical row-change record has no transaction ID"))
    (unless *wal-recovery-logical-chunk-count*
      (unless (zerop index)
        (error "logical WAL chunks do not start at index zero"))
      (setf *wal-recovery-logical-xid* (wal-record-xid record)
            *wal-recovery-logical-chunk-count* count))
    (unless (and (= (wal-record-xid record) *wal-recovery-logical-xid*)
                 (= count *wal-recovery-logical-chunk-count*)
                 (= index *wal-recovery-logical-next-chunk*))
      (error "logical WAL chunks are out of order or change transaction"))
    (incf *wal-recovery-logical-next-chunk*)
    (incf *wal-recovery-logical-bytes* event-bytes)
    (when (> *wal-recovery-logical-bytes* +max-logical-transaction-bytes+)
      (error "logical WAL transaction exceeds the 64 MiB limit")))
  (values))

(defun wal-recovery-finish-logical-change-batch (xid)
  (when *wal-recovery-logical-chunk-count*
    (unless (and (= xid *wal-recovery-logical-xid*)
                 (= *wal-recovery-logical-next-chunk*
                    *wal-recovery-logical-chunk-count*))
      (error "logical WAL chunks do not match their commit record")))
  (wal-recovery-reset-logical-change-batch))

(defun wal-logical-reader-reset-batch (state batch-open-p)
  (setf (logical-reader-state-batch-open-p state) batch-open-p
        (logical-reader-state-xid state) nil
        (logical-reader-state-chunk-count state) nil
        (logical-reader-state-next-chunk state) 0
        (logical-reader-state-change-bytes state) 0
        (logical-reader-state-changes state) '())
  (values))

(defun wal-logical-reader-start-batch (state)
  ;; Recovery treats an unterminated group as a crash tail and starts staging
  ;; anew at the next batch marker. Match that rule so later commits remain
  ;; readable after an interrupted earlier commit.
  (wal-logical-reader-reset-batch state t))

(defun wal-logical-reader-add-chunk (state record)
  (unless (logical-reader-state-batch-open-p state)
    (error "logical WAL changes appear outside a batch"))
  (multiple-value-bind (index count changes event-bytes)
      (wal-decode-logical-change-chunk (wal-record-payload record))
    (unless (plusp (wal-record-xid record))
      (error "logical row-change record has no transaction ID"))
    (unless (logical-reader-state-chunk-count state)
      (unless (zerop index)
        (error "logical WAL chunks do not start at index zero"))
      (setf (logical-reader-state-xid state) (wal-record-xid record)
            (logical-reader-state-chunk-count state) count))
    (unless (and (= (wal-record-xid record) (logical-reader-state-xid state))
                 (= count (logical-reader-state-chunk-count state))
                 (= index (logical-reader-state-next-chunk state)))
      (error "logical WAL chunks are out of order or change transaction"))
    (incf (logical-reader-state-next-chunk state))
    (incf (logical-reader-state-change-bytes state) event-bytes)
    (when (> (logical-reader-state-change-bytes state)
             +max-logical-transaction-bytes+)
      (error "logical WAL transaction exceeds the 64 MiB limit"))
    (setf (logical-reader-state-changes state)
          (nconc (logical-reader-state-changes state) changes)))
  (values))

(defun wal-logical-reader-finish-batch (state record)
  (unless (logical-reader-state-batch-open-p state)
    (error "logical WAL terminal appears outside a batch"))
  (if (= (wal-record-type record) +wal-record-commit+)
      (progn
        (when (logical-reader-state-chunk-count state)
          (unless (and (= (wal-record-xid record)
                          (logical-reader-state-xid state))
                       (= (logical-reader-state-next-chunk state)
                          (logical-reader-state-chunk-count state)))
            (error "logical WAL chunks do not match their commit record")))
        (let ((changes
                (remove-if-not
                 (lambda (change)
                   (= (logical-change-database-oid change)
                      (logical-reader-state-database-oid state)))
                 (logical-reader-state-changes state))))
          (when changes
            (push (make-logical-transaction :xid (wal-record-xid record)
                                            :commit-lsn (wal-record-end record)
                                            :changes changes)
                  (logical-reader-state-transactions state)))))
      (when (logical-reader-state-chunk-count state)
        (error "logical row changes are attached to a non-commit WAL batch")))
  (wal-logical-reader-reset-batch state nil))

(defun wal-logical-reader-has-database-changes-p (state)
  (some (lambda (change)
          (= (logical-change-database-oid change)
             (logical-reader-state-database-oid state)))
        (logical-reader-state-changes state)))

(defun wal-logical-reader-validate-limits
    (max-transactions max-bytes max-scan-bytes)
  (unless (and (integerp max-transactions) (plusp max-transactions))
    (ereport "22023" "logical read transaction limit must be positive"))
  (unless (and (integerp max-bytes) (plusp max-bytes))
    (ereport "22023" "logical read byte limit must be positive"))
  (unless (and (integerp max-scan-bytes) (plusp max-scan-bytes))
    (ereport "22023" "logical WAL scan byte limit must be positive"))
  (values))

(defun wal-logical-reader-return (transactions progress-lsn)
  (when (and *wal-logical-slot-owner*
             (integerp progress-lsn))
    (setf *wal-logical-slot-read-progress*
          (max (or *wal-logical-slot-read-progress* progress-lsn)
               progress-lsn)))
  (values transactions progress-lsn))

(defun call-with-logical-replication-slot (name function)
  "Hold an exclusive runtime claim across the reader's apply/ack callback.

PostgreSQL's ReplicationSlotAcquire() keeps a slot from being dropped or read
by a second walsender while it is active. This callback is the equivalent
session boundary until the wire-protocol replication command is added."
  (let ((owner (gensym "LOGICAL-SLOT-"))
        (slot nil))
    (sb-thread:with-mutex (*wal-lock*)
      (setf slot (gethash name *wal-replication-slots*))
      (unless slot
        (ereport "42704" "replication slot \"~a\" does not exist" name))
      (unless (wal-replication-slot-database-oid slot)
        (ereport "55000" "replication slot has no database binding"))
      (when (eq (wal-replication-slot-wal-status slot) :lost)
        (ereport "55000" "replication slot \"~a\" has lost its WAL" name))
      (unless (wal-replication-slot-confirmed-flush-lsn slot)
        (ereport "55000" "replication slot initial copy is not acknowledged"))
      (when (wal-replication-slot-active-owner slot)
        (ereport "55006" "replication slot \"~a\" is active" name))
      (when (and *backend*
                 (/= (database-oid (current-database))
                     (wal-replication-slot-database-oid slot)))
        (ereport "55000" "replication slot belongs to another database"))
      (setf (wal-replication-slot-active-owner slot) owner))
    (unwind-protect
         (let ((*wal-logical-slot-owner* owner)
               (*wal-logical-slot-read-progress*
                 (wal-replication-slot-confirmed-flush-lsn slot)))
           (funcall function slot))
      (wal-release-replication-slot-owner name owner))))

(defun wal-logical-slot-read-transactions
    (name &key upto-lsn
            (max-transactions +max-logical-read-transactions+)
            (max-bytes +max-logical-read-bytes+)
            (max-scan-bytes +max-logical-read-scan-bytes+))
  "Read a bounded committed batch and its safe progress LSN for an owned slot.

MAX-BYTES limits encoded row-change bytes per read; a first transaction may
exceed it, up to the per-transaction limit, so callers can always make progress.
MAX-SCAN-BYTES bounds WAL scanning through the next terminal batch boundary.
The second return value is the last safe progress LSN: the last terminal fully
scanned, or the slot's starting position when no scan was needed."
  (wal-logical-reader-validate-limits max-transactions max-bytes max-scan-bytes)
  (sb-thread:with-mutex (*wal-lock*)
    (wal-check-healthy)
    (let* ((slot (gethash name *wal-replication-slots*))
           (owner (and slot (wal-replication-slot-active-owner slot)))
           (start-lsn (and slot
                           (or (wal-replication-slot-confirmed-flush-lsn slot)
                               (wal-replication-slot-consistent-lsn slot))))
           (end-lsn (or upto-lsn *wal-durable-lsn*)))
      (unless slot
        (ereport "42704" "replication slot \"~a\" does not exist" name))
      (unless (and *wal-logical-slot-owner*
                   (eq owner *wal-logical-slot-owner*))
        (ereport "55000" "logical replication slot is not owned by this reader"))
      (when (eq (wal-replication-slot-wal-status slot) :lost)
        (ereport "55000" "replication slot \"~a\" has lost its WAL" name))
      (unless (wal-replication-slot-confirmed-flush-lsn slot)
        (ereport "55000" "replication slot initial copy is not acknowledged"))
      (unless (and (integerp (wal-replication-slot-database-oid slot)) start-lsn)
        (ereport "55000" "logical replication slot is not initialized"))
      (unless (and (integerp end-lsn) (<= start-lsn end-lsn *wal-durable-lsn*))
        (ereport "22023" "logical replication read end is outside durable WAL"))
      (when (= start-lsn end-lsn)
        (return-from wal-logical-slot-read-transactions
          (wal-logical-reader-return '() start-lsn)))
      (let ((state (make-logical-reader-state
                    :database-oid (wal-replication-slot-database-oid slot)))
            (transaction-count 0)
            (change-bytes 0)
            (progress-lsn start-lsn))
        ;; PostgreSQL's XLogReadRecord() consumer reads only a stable WAL
        ;; horizon. Here *wal-lock* also excludes commit append and checkpoint
        ;; recycling while the bounded scan is in progress.
        (wal-scan
         (lambda (record)
           (let ((record-type (wal-record-type record)))
             (case record-type
               (#.+wal-record-batch-start+
                (wal-logical-reader-start-batch state))
               (#.+wal-record-logical-changes+
                (wal-logical-reader-add-chunk state record))
               (otherwise
                (when (wal-terminal-record-type-p record-type)
                  (let* ((has-changes
                           (and (= record-type +wal-record-commit+)
                                (wal-logical-reader-has-database-changes-p
                                 state)))
                         (batch-bytes
                           (logical-reader-state-change-bytes state)))
                    (when (and has-changes
                               (plusp transaction-count)
                               (or (>= transaction-count max-transactions)
                                   (> (+ change-bytes batch-bytes) max-bytes)))
                      (return-from wal-logical-slot-read-transactions
                        (wal-logical-reader-return
                         (nreverse
                          (logical-reader-state-transactions state))
                         progress-lsn)))
                    (wal-logical-reader-finish-batch state record)
                    (setf progress-lsn (wal-record-end record))
                    (when has-changes
                      (incf transaction-count)
                      (incf change-bytes batch-bytes))
                    (when (or (>= transaction-count max-transactions)
                              (>= change-bytes max-bytes)
                              (>= (- progress-lsn start-lsn) max-scan-bytes))
                      (return-from wal-logical-slot-read-transactions
                        (wal-logical-reader-return
                         (nreverse
                          (logical-reader-state-transactions state))
                         progress-lsn)))))))))
         start-lsn :end-lsn end-lsn)
        ;; An open batch at the durable scan horizon is an incomplete commit
        ;; tail. Its changes stay invisible, as in DecodeCommit/reorderbuffer.
        (wal-logical-reader-return
         (nreverse (logical-reader-state-transactions state)) progress-lsn)))))

(defun wal-logical-slot-acknowledge (name acknowledged-lsn)
  "Acknowledge scanned WAL after all preceding row changes are durable."
  (unless *wal-logical-slot-owner*
    (ereport "55000" "logical replication slot is not owned by this reader"))
  (unless (and (integerp acknowledged-lsn)
               (integerp *wal-logical-slot-read-progress*)
               (<= acknowledged-lsn *wal-logical-slot-read-progress*))
    (ereport "22023" "logical acknowledgment is past the reader's progress LSN"))
  (wal-acknowledge-replication-slot
   name acknowledged-lsn *wal-logical-slot-owner*))
