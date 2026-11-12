;; Read a proven WAL interval from local files or a selected retained root. The carrier
;; comes from the selected history; the owner comes from each record's position.
;; Traversal supplies local record progress, not slot acknowledgement authority.

(in-package #:pg)

(declaim (ftype function logical-wal-archive-check-cursor logical-wal-archive-copy-at
                logical-wal-archive-compare-live logical-wal-archive-prepare
                call-with-optional-logical-wal-archive))

(defstruct (logical-wal-cursor (:constructor %make-logical-wal-cursor))
  "A scoped, thread-bound traversal of a frozen durable WAL interval.
The guard is checked under the captured WAL mutex before each copy and advance."
  (history nil :read-only t) (namespace nil :read-only t)
  (end 0 :read-only t) (guard nil :read-only t) (archive nil :read-only t)
  (thread sb-thread:*current-thread* :read-only t)
  (position 0) (batch-open nil))

(defun logical-wal-segment-carrier (segment history)
  "Choose the timeline carrying the last byte of SEGMENT in the selected history."
  (unless (typep segment '(integer 0 1099511627775))
    (error "invalid logical WAL segment"))
  (timeline-at-point (1- (* (1+ segment) +wal-segment-size+)) history))

(defun logical-wal-cursor-check-scope (cursor)
  "Require the owning thread and live source scope under the WAL mutex."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (and (eq sb-thread:*current-thread* (logical-wal-cursor-thread cursor))
               (funcall (logical-wal-cursor-guard cursor)))
    (error "logical WAL cursor is outside its source scope"))
  (when (logical-wal-cursor-archive cursor)
    (logical-wal-archive-check-cursor (logical-wal-cursor-archive cursor)
                                      (logical-wal-cursor-history cursor)
                                      (logical-wal-cursor-namespace cursor))))

(defun call-with-logical-wal-cursor (history namespace start end owner role guard function &key archive)
  "Call FUNCTION with a cursor over START through the frozen END.
The caller must prove the source and start boundary and supply a guard that
checks that proof under the WAL mutex.  A geometrically valid empty interval
alone does not verify record bytes or a record boundary.  The cursor is valid
only in this call, on this thread, with the captured mutex.  An optional
archive must also pass its own context check."
  (timeline-check-lineage history)
  (unless (and (functionp guard)
               (typep start '(unsigned-byte 64)) (typep end '(unsigned-byte 64))
               (<= start end) (member namespace '(:legacy :timeline))
               (or (eq namespace :timeline)
                   (every (lambda (entry) (= 1 (timeline-history-entry-timeline entry))) history)))
    (error "invalid logical WAL cursor scope"))
  (unless (ecase role
            (:prefix-end (timeline-prefix-boundary-p owner start history))
            (:record-start (= owner (timeline-at-point start history))))
    (error "logical WAL start owner disagrees with its position role"))
  (let* ((active t)
         (mutex *wal-lock*)
         (cursor (%make-logical-wal-cursor
                  :history (mapcar #'copy-timeline-history-entry history)
                  :namespace namespace :position start :end end :archive archive
                  :guard (lambda () (and active (eq mutex *wal-lock*) (funcall guard))))))
    (unwind-protect
         (progn
           (sb-thread:with-mutex (*wal-lock*) (logical-wal-cursor-check-scope cursor))
           (funcall function cursor))
      (sb-sys:without-interrupts (setf active nil)))))

(defun logical-wal-check-record-length (bytes header-offset position size end)
  "Check the shared record length bounds for a live or archived header."
  (let ((offset (mod position +wal-segment-size+)))
    (unless (and (<= (+ header-offset +wal-header-size+) (length bytes))
                 (<= (+ offset +wal-header-size+) size)
                 (<= (+ position +wal-header-size+) end))
      (error "incomplete logical WAL header"))
    (let ((length (wal-u32 bytes (+ header-offset 8))))
      (unless (and (<= +wal-header-size+ length +wal-segment-size+)
                   (<= (+ offset length) size +wal-segment-size+)
                   (<= (+ position length) end))
        (error "incomplete or out-of-range logical WAL record"))
      length)))

(defun logical-wal-cursor-copy-at (cursor position carrier)
  "Copy from the exact local carrier, or consult the archive on open ENOENT.
Return bytes, size and ready; ready true with NIL bytes means EOF, while a
false ready value requests preparation.  Other open failures remain errors."
  (let ((stream nil) (size nil) (missing nil)
                     (archive (logical-wal-cursor-archive cursor))
                     (segment (floor position +wal-segment-size+)))
    (unwind-protect
         (progn
           (sb-sys:without-interrupts
             (handler-case
                 (multiple-value-setq (stream size)
                   (open-bounded-regular-file
                    (wal-segment-path segment carrier (logical-wal-cursor-namespace cursor))
                    +wal-segment-size+))
               (sb-posix:syscall-error (condition)
                 (if (and archive (= sb-posix:enoent (sb-posix:syscall-errno condition)))
                     (setf missing t)
                     (error condition)))))
           (when missing
             (return-from logical-wal-cursor-copy-at
               (logical-wal-archive-copy-at archive position (logical-wal-cursor-end cursor) carrier)))
           (let ((matched-cache (and archive (logical-wal-archive-compare-live
                                              archive position carrier size nil)))
                 (offset (mod position +wal-segment-size+)))
             (when (> offset size) (error "logical WAL start exceeds carrier length"))
             (let ((bytes (unless (= offset size)
                            (logical-wal-copy-record stream size position
                                                     (logical-wal-cursor-end cursor)))))
               (when (and matched-cache bytes)
                 (logical-wal-archive-compare-live archive position carrier size bytes))
               (values bytes size t))))
      (when stream (close stream)))))

(defun logical-wal-cursor-read-next (cursor)
  "Prepare the archive segment outside WAL exclusion and retry the local open.
Keep position, gap and original position private across retries.  At most two
segments may be prepared; each retry checks the scope and original position.
Return bytes, position, carrier and original position."
  (let ((original nil) (continuation nil) (prepared nil))
    (dotimes (attempt 3)
      (declare (ignorable attempt))
      (multiple-value-bind (bytes position carrier missing)
          (sb-thread:with-mutex (*wal-lock*)
            (unless original (setf original (logical-wal-cursor-position cursor)))
            (unless (= original (logical-wal-cursor-position cursor))
              (error "logical WAL cursor changed during archive preparation"))
            (logical-wal-cursor-copy-next cursor continuation))
        (unless missing
          (return-from logical-wal-cursor-read-next (values bytes position carrier original)))
        (let ((segment (floor position +wal-segment-size+)))
          (when (or (member segment prepared) (= 2 (length prepared)))
            (error "logical WAL archive preparation did not stabilize"))
          (push segment prepared)
          (setf continuation missing)
          (logical-wal-archive-prepare (logical-wal-cursor-archive cursor) segment carrier))))
    (error "logical WAL archive preparation exceeded its segment bound")))

(defun logical-wal-copy-record (stream size position end)
  "Return a fresh copy of a complete live record using the shared length check."
  (let* ((offset (mod position +wal-segment-size+))
         (header (make-array +wal-header-size+ :element-type '(unsigned-byte 8))))
    (unless (and (<= (+ offset +wal-header-size+) size)
                 (<= (+ position +wal-header-size+) end)
                 (file-position stream offset)
                 (= (read-sequence header stream) +wal-header-size+))
      (error "incomplete logical WAL header"))
    (let* ((length (logical-wal-check-record-length header 0 position size end))
           (bytes (make-array length :element-type '(unsigned-byte 8))))
      (replace bytes header)
      (unless (= (read-sequence bytes stream :start +wal-header-size+) length)
        (error "short logical WAL record read"))
      bytes)))

(defun logical-wal-cursor-copy-next (cursor &optional continuation)
  "Copy the next record under WAL exclusion using the last-byte carrier.
Never fall back to an older filename.  A miss returns a private continuation;
a ready EOF may instead move the private position over a checked rotation gap."
  (logical-wal-cursor-check-scope cursor)
  (let* ((original (logical-wal-cursor-position cursor))
         (position (if continuation (first continuation) original))
         (end (logical-wal-cursor-end cursor))
         (history (logical-wal-cursor-history cursor))
         (gap (second continuation)))
    (when (and continuation (/= original (third continuation)))
      (error "logical WAL cursor changed during archive preparation"))
    (when (= position end)
      (when (logical-wal-cursor-batch-open cursor)
        (error "logical WAL horizon ends inside a batch"))
      (return-from logical-wal-cursor-copy-next nil))
    (when (> position end) (error "logical WAL cursor passed its horizon"))
    (dotimes (attempt 2)
      (declare (ignorable attempt))
      (let* ((segment (floor position +wal-segment-size+))
             (offset (mod position +wal-segment-size+))
             (carrier (logical-wal-segment-carrier segment history)))
        (multiple-value-bind (bytes size ready)
            (logical-wal-cursor-copy-at cursor position carrier)
          (unless ready
            (return-from logical-wal-cursor-copy-next
              (values nil position carrier (list position gap original))))
          (if (= offset size)
              (let ((next (* (1+ segment) +wal-segment-size+)))
                (when (or gap (zerop offset) (>= next end)
                          (some (lambda (entry)
                                  (< position (timeline-history-entry-begin entry) next)) history))
                  (error "invalid logical WAL rotation gap"))
                (setf gap position position next))
              (progn
                (when (and gap (<= (length bytes) (- position gap)))
                  (error "logical WAL record fits in the skipped segment capacity"))
                (return-from logical-wal-cursor-copy-next
                  (values bytes position carrier)))))))
    (error "logical WAL rotation destination is empty")))

(defun logical-wal-eor-position (fork)
  "Return the expected child end-of-recovery record position for FORK."
  (if (<= (+ (mod fork +wal-segment-size+) +wal-end-of-recovery-record-size+)
          +wal-segment-size+)
      fork (* (1+ (floor fork +wal-segment-size+)) +wal-segment-size+)))

(defun logical-wal-cursor-check-record (cursor record)
  "Check record ownership, fork and batch rules against the selected history.
End-of-recovery is metadata, not an ordinary logical record."
  (let* ((history (logical-wal-cursor-history cursor))
         (start (wal-record-start record))
         (owner (timeline-at-point start history))
         (entry (find owner history :key #'timeline-history-entry-timeline))
         (parent (second (member entry history)))
         (type (wal-record-type record))
         (eor (= type +wal-record-end-of-recovery+)))
    (unless (and (<= (wal-record-end record) (logical-wal-cursor-end cursor))
                 (or (null (timeline-history-entry-end entry))
                     (<= (wal-record-end record) (timeline-history-entry-end entry))))
      (error "logical WAL record crosses its fork or horizon"))
    (when (and parent (= start (logical-wal-eor-position (timeline-history-entry-begin entry)))
               (not eor))
      (error "logical WAL child does not begin with end-of-recovery"))
    (if eor
        (let ((info (wal-end-of-recovery-record-info record)))
          (unless (and parent (not (logical-wal-cursor-batch-open cursor))
                       (= owner (getf info :timeline))
                       (= (timeline-history-entry-timeline parent) (getf info :prev-timeline))
                       (= start (logical-wal-eor-position (timeline-history-entry-begin entry))))
            (error "logical WAL end-of-recovery disagrees with selected history")))
        (progn
          (wal-require-ordinary-record-type type)
          (when (and (= type +wal-record-batch-start+) (logical-wal-cursor-batch-open cursor))
            (error "nested logical WAL batch"))))
    (values owner (if eor :end-of-recovery :ordinary))))

(defun logical-wal-cursor-next (cursor)
  "Decode a fresh record copy outside WAL exclusion, then recheck the full
scope and original position before advancing.  Return the record, bytes,
record owner, carrier and kind; local progress is not slot acknowledgement."
  (multiple-value-bind (bytes position carrier original)
      (logical-wal-cursor-read-next cursor)
    (unless bytes (return-from logical-wal-cursor-next nil))
    (let ((record (wal-decode-record bytes position)))
      (multiple-value-bind (owner kind) (logical-wal-cursor-check-record cursor record)
        (sb-thread:with-mutex (*wal-lock*)
          (logical-wal-cursor-check-scope cursor)
          (unless (= original (logical-wal-cursor-position cursor))
            (error "logical WAL cursor changed during record decode"))
          (setf (logical-wal-cursor-position cursor) (wal-record-end record))
          (cond ((= (wal-record-type record) +wal-record-batch-start+)
                 (setf (logical-wal-cursor-batch-open cursor) t))
                ((wal-terminal-record-type-p (wal-record-type record))
                 (setf (logical-wal-cursor-batch-open cursor) nil))))
        (values record bytes owner carrier kind)))))

(declaim (ftype function physical-slot-startup-system-id))

(defun logical-slot-wal-start-proven-p (slot start owner role)
  "Accept a birth or consumed prefix, or a retained pending record start.
Standard feedback may be unaligned; only its retained restart floor is a
decoding anchor. A zero restart keeps the writer's record-start semantics."
  (let* ((pending (loop for entry being the hash-values
                          of (wal-replication-slot-pending-xids slot)
                            thereis (= start (car entry))))
         (restart (and (eql start (wal-replication-slot-restart-lsn slot))
                       (eql owner (wal-replication-slot-restart-timeline slot))))
         (provenance (wal-replication-slot-logical-provenance slot)))
    (ecase role
      (:prefix-end
       (or (and restart (plusp start) (not pending))
           (and (not (equal (wal-replication-slot-plugin slot) "pgoutput"))
                (eql start (wal-replication-slot-confirmed-flush-lsn slot))
                (eql owner (wal-replication-slot-confirmed-timeline slot)))
           (and (eql start (wal-replication-slot-consistent-lsn slot))
                provenance (eql owner (logical-slot-provenance-birth-timeline provenance)))))
      (:record-start
       (and restart (or pending (zerop start)))))))

(defun logical-slot-wal-check-control (scan system-id)
  "Check persisted control against the captured primary and durable horizon."
  (let* ((fields (multiple-value-list (wal-read-control-fields)))
         (metadata (seventh fields)))
    (unless (and (integerp (first fields))
                 (eql (fourth fields) (wal-retained-slot-scan-timeline scan))
                 (eq (sixth fields) (wal-retained-slot-scan-namespace scan))
                 (<= (wal-retained-slot-scan-end scan) (second fields) *wal-durable-lsn*)
                 (or (null metadata) (eql system-id (getf metadata :system-id))))
      (error "logical WAL control differs from its captured primary source"))))

(defun call-with-logical-slot-wal-cursor (name from-lsn upto-lsn owner role function)
  "Traverse a retained slot interval using captured primary source evidence.
Hold the pin and slot identity fixed. Recheck control coordinates and exact
history bytes, root, profile and birth, and require a proven start. The end
is frozen; later WAL growth and CLOG reset are allowed.  The slot pin encloses
the optional retained-root lease.  The archive captures the same source check,
so the cursor need not run it twice; both scopes still enforce their own
thread, mutex and lifetime checks.  This grants no import authority."
  (call-with-wal-retained-slot-scan
   name from-lsn upto-lsn
   (lambda (scan)
     (let ((catalog nil) (system-id nil) (provenance nil)
                         (history nil) (history-bytes nil) (root nil) (birth nil) (profile nil))
       (labels ((check-source ()
                  (wal-retained-slot-scan-validate-under-lock scan)
                  (unless (and *wal-ready* (not (recovery-in-progress-p))
                               (eq catalog *catalog*)
                               (eq profile (wal-slot-profile (wal-retained-slot-scan-slot scan)))
                               (equal root (truename *pgdata*))
                               (eql system-id (catalog-system-identifier))
                               (eql system-id (physical-slot-startup-system-id))
                               (or (null *wal-control-metadata*)
                                   (eql system-id (getf *wal-control-metadata* :system-id)))
                               (eql birth (wal-replication-slot-consistent-lsn
                                           (wal-retained-slot-scan-slot scan)))
                               (eq provenance
                                   (wal-replication-slot-logical-provenance
                                    (wal-retained-slot-scan-slot scan)))
                               (<= (wal-retained-slot-scan-end scan) *wal-durable-lsn*)
                               (equalp history-bytes
                                       (unless (= (wal-retained-slot-scan-timeline scan) 1)
                                         (timeline-history-bytes
                                          (wal-retained-slot-scan-timeline scan)))))
                    (error "logical WAL source changed during cursor scope"))
                  (logical-slot-wal-check-control scan system-id)
                  t))
         (sb-thread:with-mutex (*wal-lock*)
           (wal-retained-slot-scan-validate-under-lock scan)
           (setf catalog *catalog* system-id (physical-slot-startup-system-id)
                 provenance (wal-replication-slot-logical-provenance
                             (wal-retained-slot-scan-slot scan))
                 root (truename *pgdata*)
                 birth (wal-replication-slot-consistent-lsn (wal-retained-slot-scan-slot scan))
                 profile (wal-slot-profile (wal-retained-slot-scan-slot scan))
                 history-bytes (unless (= (wal-retained-slot-scan-timeline scan) 1)
                                 (timeline-history-bytes (wal-retained-slot-scan-timeline scan)))
                 history (timeline-check-lineage
                          (timeline-parse-history (wal-retained-slot-scan-timeline scan)
                                                  (if history-bytes (bytes->string history-bytes) ""))))
           (unless (and catalog provenance
                        (eql system-id (logical-slot-provenance-system-id provenance)))
             (error "logical WAL cursor requires known primary slot identity"))
           (unless (logical-slot-wal-start-proven-p
                    (wal-retained-slot-scan-slot scan)
                    (wal-retained-slot-scan-start scan) owner role)
             (error "logical WAL start lacks a captured slot boundary"))
           (logical-slot-check-primary-history (wal-retained-slot-scan-slot scan) history)
           (check-source))
         (call-with-optional-logical-wal-archive
          system-id history (wal-retained-slot-scan-namespace scan) #'check-source
          (lambda (archive)
            (call-with-logical-wal-cursor
             history (wal-retained-slot-scan-namespace scan)
             (wal-retained-slot-scan-start scan) (wal-retained-slot-scan-end scan)
             owner role (if archive (constantly t) #'check-source) function :archive archive))))))))
