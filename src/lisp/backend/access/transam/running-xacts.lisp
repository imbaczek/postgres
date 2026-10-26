;;;; Native source transaction visibility metadata.
(in-package #:pg)

;;; These helpers describe source transaction visibility for a future WAL and
;;; replay integration.  They do not themselves capture, write, or replay WAL.
;;; The corresponding PostgreSQL C code is LogStandbySnapshot in standby.c
;;; and RecordKnownAssignedTransactionIds in procarray.c.
;;;
;;; A producer must serialize snapshot capture through WAL insertion in WAL
;;; order.  The planned lock order is WAL then XID under the commit WAL lock.
;;; NEXT is the actual next XID, not the end of a reserved allocation range.
;;; Do not filter a restored running set through CLOG: on-disk CLOG can be
;;; ahead of the generation that supplies the authoritative replay position.
;;;
;;; Bound the exact set so a long interval between anchors cannot consume
;;; unbounded memory.  An incomplete snapshot carries no running XIDs.

(defconstant +running-xacts-limit+ 65536)

(defun running-xacts-normal-xid-p (xid)
  (and (typep xid '(integer 3 18446744073709551615))
       (>= (ldb (byte 32 0) xid) 3)))

(defun running-xacts-next-p (next)
  (and (typep next '(integer 3 18446744073709551616))
       (= next (canonical-next-full-xid next))))

(defun running-xacts-check (next running complete)
  (unless (and (running-xacts-next-p next)
               (member complete '(nil t))
               (listp running)
               (let ((count (list-length running)))
                 (and count (<= count +running-xacts-limit+)))
               (or complete (null running))
               (loop for previous = nil then xid
                     for xid in running
                     always (and (running-xacts-normal-xid-p xid)
                                 (< xid next)
                                 (or (null previous) (< previous xid)))))
    (error "invalid source running transaction snapshot"))
  t)

;;; PGRUNX01 is a private payload, not PostgreSQL's standby record format.
;;; Its 24-byte header contains the eight-byte magic, a u32 flags word, a
;;; u64 next XID, and a u32 count, followed by count u64 XIDs.  Integers use
;;; the WAL helpers' byte order.  Flag bit 0 marks a complete set; bit 1
;;; represents the one-past-maximum next XID with a zero next field.  Other
;;; bits are invalid.  XIDs must be sorted, unique, normal, and below NEXT.

(defun wal-encode-running-xacts (next running complete)
  "Encode native visibility metadata with little-endian integer fields."
  (running-xacts-check next running complete)
  (let* ((exhausted (= next +max-xid-one-past+))
         (bytes (make-array (+ 24 (* 8 (length running)))
                            :element-type '(unsigned-byte 8) :initial-element 0)))
    (replace bytes #(80 71 82 85 78 88 48 49))
    (wal-set-u32 bytes 8 (logior (if complete 1 0) (if exhausted 2 0)))
    (wal-set-u64 bytes 12 (if exhausted 0 next))
    (wal-set-u32 bytes 20 (length running))
    (loop for xid in running for offset from 24 by 8
          do (wal-set-u64 bytes offset xid))
    bytes))

(defun wal-decode-running-xacts (bytes)
  (unless (and (typep bytes '(vector (unsigned-byte 8)))
               (>= (length bytes) 24)
               (equalp (subseq bytes 0 8) #(80 71 82 85 78 88 48 49)))
    (error "invalid running transaction payload header"))
  (let* ((flags (wal-u32 bytes 8))
         (raw-next (wal-u64 bytes 12))
         (exhausted (logbitp 1 flags))
         (complete (logbitp 0 flags))
         (count (wal-u32 bytes 20)))
    (unless (and (zerop (logandc2 flags 3))
                 (eq exhausted (zerop raw-next))
                 (<= count +running-xacts-limit+)
                 (= (length bytes) (+ 24 (* 8 count))))
      (error "invalid running transaction payload flags or length"))
    (let ((next (if exhausted +max-xid-one-past+ raw-next))
          (running (loop for offset from 24 below (length bytes) by 8
                         collect (wal-u64 bytes offset))))
      (running-xacts-check next running complete)
      (list :next-xid next :running running :complete complete))))

;;; An anchor replaces the known state with an exact or incomplete snapshot
;;; at LSN.  ANCHOR identifies that snapshot; THROUGH is the position through
;;; which later progress has been applied.  Copy the list so the caller retains
;;; ownership of the snapshot.

(defun running-xacts-anchor (snapshot lsn)
  (unless (typep lsn '(unsigned-byte 64))
    (error "invalid running transaction anchor position"))
  (let ((next (getf snapshot :next-xid))
        (running (getf snapshot :running))
        (complete (getf snapshot :complete)))
    (running-xacts-check next running complete)
    (list :version 1 :anchor lsn :through lsn :next-xid next
          :running (copy-list running) :complete complete)))

(defun running-xacts-check-state (state)
  (unless (and (listp state) (eql (list-length state) 12)
               (loop for key in '(:version :anchor :through :next-xid :running :complete)
                     always (= 1 (loop for candidate in state by #'cddr
                                       count (eq key candidate))))
               (eql (getf state :version) 1)
               (typep (getf state :anchor) '(unsigned-byte 64))
               (typep (getf state :through) '(unsigned-byte 64))
               (<= (getf state :anchor) (getf state :through)))
    (error "invalid source running transaction state"))
  (running-xacts-check (getf state :next-xid) (getf state :running)
                       (getf state :complete)))

;;; Return a new state without changing STATE.  Advance monotonically through
;;; WAL positions and actual next-XID values, adding newly assigned XIDs to an
;;; exact set.  If the bound is exceeded, discard the set and mark it
;;; incomplete until a later anchor replaces it.
;;;
;;; Remove only an ordered COMMIT XID.  Native ABORT currently has no WAL
;;; record, so aborted XIDs can remain in the set; a later exact anchor clears
;;; that stale membership.

(defun running-xacts-advance (state next through &key committed-xid)
  "Preserve complete coverage as a conservative superset, not exact live membership.
NIL state stays unknown until an anchor. Inferred XIDs may already have aborted."
  (unless (and (running-xacts-next-p next)
               (typep through '(unsigned-byte 64))
               (or (null committed-xid)
                   (and (running-xacts-normal-xid-p committed-xid)
                        (< committed-xid next))))
    (error "invalid source transaction progress"))
  (unless state (return-from running-xacts-advance nil))
  (running-xacts-check-state state)
  (unless (and (>= next (getf state :next-xid))
               (>= through (getf state :through)))
    (error "source transaction progress moved backwards"))
  (let* ((result (copy-list state))
         (running (remove committed-xid (getf state :running)))
         (count (length running))
         (added nil)
         (complete (getf state :complete)))
    (when complete
      (loop for xid = (getf state :next-xid)
              then (canonical-next-full-xid (1+ xid))
            while (< xid next)
            unless (eql xid committed-xid)
              do (incf count)
                 (when (> count +running-xacts-limit+)
                   (setf complete nil)
                   (return))
                 (push xid added)))
    (setf (getf result :through) through
          (getf result :next-xid) next
          (getf result :complete) complete
          (getf result :running) (when complete (append running (nreverse added))))
    result))

;;; Source WAL emission
;;;
;;; The pure helpers above do not capture or replay WAL themselves.  The
;;; source commit path now emits this metadata, using the WAL-then-XID lock
;;; order described above.  The record decoder validates the metadata, but
;;; replay does not yet publish its membership.  Standby snapshot and
;;; generation integration remain future work.
;;;
;;; Native record type 14 was introduced in the same unpublished native WAL
;;; version 7.  Older executables reject the new type, so mixed checkouts are
;;; not compatible.  This private format is not PostgreSQL C disk WAL.

(defun wal-running-xacts-record-info (record)
  "Validate a native record and return its decoded snapshot."
  (unless (and (= (wal-record-type record) +wal-record-running-xacts+)
               (>= (wal-record-version record) 7)
               (zerop (wal-record-xid record))
               (typep (wal-record-start record) '(unsigned-byte 64))
               (typep (wal-record-end record) '(unsigned-byte 64))
               (= (wal-record-end record)
                  (+ (wal-record-start record) +wal-header-size+
                     (length (wal-record-payload record))))
               (> (wal-record-end record) (wal-record-start record))
               (= (floor (wal-record-start record) +wal-segment-size+)
                  (floor (1- (wal-record-end record)) +wal-segment-size+)))
    (error "invalid native running transaction record"))
  (wal-decode-running-xacts (wal-record-payload record)))

;;; The caller holds the WAL lock.  As in LogStandbySnapshot, keep the XID
;;; lock from capturing the actual next XID and active set through appending
;;; the batch marker and metadata.  Release it before the normal row batch;
;;; the normal commit flush makes the records durable together.
;;;
;;; Only a registered committer can establish coverage.  A standalone caller
;;; without a registered XID emits no anchor.  If the active set exceeds the
;;; bound, emit an explicit incomplete snapshot instead of a partial set.

(defun wal-append-running-xacts-under-lock (committer)
  "Append a running-XID snapshot for a registered COMMITTER under the WAL lock."
  (sb-thread:with-mutex (*xid-lock*)
    ;; Standalone WAL callers do not necessarily register transactions.
    ;; Only the actual allocator registry can provide coverage.
    (when (member committer *active-xids* :test #'eql)
      (let* ((complete (<= (length *active-xids*) +running-xacts-limit+))
             (payload (wal-encode-running-xacts
                       *next-xid*
                       (when complete (sort (copy-list *active-xids*) #'<))
                       complete)))
        (wal-append-batch-start)
        (wal-append-record +wal-record-running-xacts+ 0 payload)))))
