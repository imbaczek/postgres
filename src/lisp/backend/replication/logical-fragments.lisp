;;;; Bounded fragmentation of encoded logical WAL events.
(in-package #:pg)

(defstruct logical-fragment event start end)

(defun logical-plan-event-fragments (events fragment-limit transaction-limit)
  "Plan slices of ordered EVENTS without copying their octets.
Reject oversized transactions before any WAL record is published."
  (unless (and (integerp fragment-limit) (plusp fragment-limit)
               (integerp transaction-limit) (plusp transaction-limit))
    (error "invalid logical fragmentation limits"))
  (let ((total 0) (fragments '()))
    (dolist (event events)
      (let ((size (length event)))
        (unless (plusp size) (error "empty logical WAL event"))
        (incf total size)
        (when (> total transaction-limit)
          (error "logical transaction exceeds its WAL change limit"))
        (loop for start from 0 below size by fragment-limit
              for end = (min size (+ start fragment-limit))
              do (push (make-logical-fragment :event event :start start :end end)
                       fragments))))
    (nreverse fragments)))

(defconstant +logical-fragment-tag+ #xffffffff)

(defun logical-encode-fragment (index count fragment)
  "Encode one planned slice with its batch position and event bounds."
  (let* ((event (logical-fragment-event fragment))
         (start (logical-fragment-start fragment))
         (end (logical-fragment-end fragment))
         (buffer (make-array 0 :element-type '(unsigned-byte 8)
                               :adjustable t :fill-pointer 0)))
    (unless (and (integerp index) (integerp count) (<= 0 index) (< index count)
                 (typep event '(vector (unsigned-byte 8)))
                 (integerp start) (integerp end) (<= 0 start) (< start end)
                 (<= end (length event)))
      (error "invalid logical WAL fragment"))
    (logical-write-u32 buffer +logical-fragment-tag+)
    (logical-write-u32 buffer index)
    (logical-write-u32 buffer count)
    (logical-write-u32 buffer (length event))
    (logical-write-u32 buffer start)
    (logical-write-u32 buffer (- end start))
    (loop for position from start below end
          do (vector-push-extend (aref event position) buffer))
    buffer))

(defun logical-fragment-payload-p (payload)
  "Recognize the reserved fragment tag, which cannot be a valid legacy index."
  (and (>= (length payload) 4)
       (= (logical-read-u32 (make-logical-cursor :bytes payload)) +logical-fragment-tag+)))

(defun logical-decode-fragment (payload)
  "Decode a fragment header and bytes without assembling or trusting its total."
  (let* ((cursor (make-logical-cursor :bytes payload))
         (tag (logical-read-u32 cursor))
         (index (logical-read-u32 cursor))
         (count (logical-read-u32 cursor))
         (total (logical-read-u32 cursor))
         (offset (logical-read-u32 cursor))
         (size (logical-read-u32 cursor)))
    (unless (and (= tag +logical-fragment-tag+) (plusp count) (< index count)
                 (plusp total) (plusp size) (<= (+ offset size) total)
                 (= (+ +logical-fragment-header-size+ size) (length payload)))
      (error "invalid logical WAL fragment payload"))
    (values index count total offset (logical-read-octets cursor size))))

(defun logical-preflight-event-fragments (events)
  "Check the whole transaction before its first WAL batch record is written."
  (logical-plan-event-fragments events
                                (- +wal-segment-size+ +wal-header-size+
                                   +logical-fragment-header-size+)
                                +max-logical-wal-transaction-bytes+)
  (values))

(defstruct (logical-fragment-assembler
            (:constructor make-logical-fragment-assembler (limit)))
  limit (bytes 0) buffer (received 0))

(defun logical-assemble-event-fragment (state total offset bytes)
  "Append one validated fragment; return the completed event or NIL.
LIMIT bounds the total declared bytes across the batch before allocation."
  (let ((size (length bytes))
        (buffer (logical-fragment-assembler-buffer state)))
    (unless (and (typep bytes '(vector (unsigned-byte 8)))
                 (integerp total) (plusp total)
                 (integerp offset) (not (minusp offset))
                 (plusp size) (<= (+ offset size) total))
      (error "invalid logical WAL fragment bounds"))
    (cond
      (buffer
       (unless (and (= total (length buffer))
                    (= offset (logical-fragment-assembler-received state)))
         (error "logical WAL fragments are out of order")))
      (t
       (unless (zerop offset)
         (error "logical WAL event does not start at offset zero"))
       (let ((next (+ total (logical-fragment-assembler-bytes state))))
         (when (> next (logical-fragment-assembler-limit state))
           (error "logical WAL transaction exceeds its change limit"))
         (setf buffer (make-array total :element-type '(unsigned-byte 8)))
         (setf (logical-fragment-assembler-bytes state) next
               (logical-fragment-assembler-buffer state) buffer))))
    (replace buffer bytes :start1 offset)
    (setf (logical-fragment-assembler-received state) (+ offset size))
    (when (= (+ offset size) total)
      (setf (logical-fragment-assembler-buffer state) nil
            (logical-fragment-assembler-received state) 0)
      buffer)))

(defun logical-finish-fragment-assembly (state)
  "Require an event boundary before accepting a transaction terminal."
  (when (logical-fragment-assembler-buffer state)
    (error "logical WAL transaction ends within an event"))
  (values))

(defstruct (logical-fragment-batch
            (:constructor make-logical-fragment-batch (limit)))
  limit xid count (next 0) assembler)

(defun logical-fragment-batch-accept (batch xid payload)
  "Validate one ordered fragment and decode a completed event if present."
  (multiple-value-bind (index count total offset bytes) (logical-decode-fragment payload)
    (unless (and (integerp xid) (plusp xid))
      (error "logical row-change record has no transaction ID"))
    (if (logical-fragment-batch-count batch)
        (unless (and (= xid (logical-fragment-batch-xid batch))
                     (= count (logical-fragment-batch-count batch))
                     (= index (logical-fragment-batch-next batch)))
          (error "logical WAL fragments are out of order or change transaction"))
        (unless (zerop index)
          (error "logical WAL fragments do not start at index zero")))
    (let* ((assembler (or (logical-fragment-batch-assembler batch)
                          (make-logical-fragment-assembler (logical-fragment-batch-limit batch))))
           (encoded (logical-assemble-event-fragment assembler total offset bytes))
           (change (and encoded (logical-decode-change encoded))))
      (when (and change (not (member (logical-change-kind change) '(:insert :update :delete :truncate))))
        (error "catalog event appears in a transactional row-change record"))
      (setf (logical-fragment-batch-assembler batch) assembler
            (logical-fragment-batch-xid batch) xid
            (logical-fragment-batch-count batch) count
            (logical-fragment-batch-next batch) (1+ index))
      change)))

(defun logical-fragment-batch-finish (batch xid)
  "Validate that XID commits the complete fragment sequence at an event boundary."
  (unless (and (logical-fragment-batch-count batch)
               (= xid (logical-fragment-batch-xid batch))
               (= (logical-fragment-batch-next batch) (logical-fragment-batch-count batch)))
    (error "logical WAL fragments do not match their commit record"))
  (logical-finish-fragment-assembly (logical-fragment-batch-assembler batch))
  (values))
