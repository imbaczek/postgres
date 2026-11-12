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
  (batch-policy :strict :read-only t)
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

(defun call-with-logical-wal-cursor (history namespace start end owner role guard function &key archive (batch-policy :strict))
  "Call FUNCTION with a cursor over START through the frozen END.
The caller must prove the source and start boundary and supply a guard that
checks that proof under the WAL mutex.  A geometrically valid empty interval
alone does not verify record bytes or a record boundary.  The cursor is valid
only in this call, on this thread, with the captured mutex.  An optional
archive must also pass its own context check. Strict batch policy rejects
nested starts and an open batch at the horizon. The logical-reader policy
lets the consumer discard abandoned batches and hide an incomplete tail;
both policies reject end-of-recovery inside an open batch."
  (timeline-check-lineage history)
  (unless (and (member batch-policy '(:strict :logical-reader))
               (functionp guard)
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
                  :batch-policy batch-policy
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
      (when (and (logical-wal-cursor-batch-open cursor)
                 (eq :strict (logical-wal-cursor-batch-policy cursor)))
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
End-of-recovery is metadata, not an ordinary logical record.  Strict policy
rejects nested batch starts; the logical reader applies its own batch rules."
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
          (when (and (= type +wal-record-batch-start+) (logical-wal-cursor-batch-open cursor)
                     (eq :strict (logical-wal-cursor-batch-policy cursor)))
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

(declaim (ftype function logical-slot-wal-check-control-coordinates logical-slot-wal-start-proven-p))

(defstruct (logical-wal-source (:constructor %make-logical-wal-source))
  "Captured source facts for rechecking a slot reader under the WAL mutex.
These facts retain history metadata, not pins, leases or WAL payload buffers."
  (slot nil :read-only t) (name nil :read-only t) (owner nil :read-only t)
  (database nil :read-only t) (plugin nil :read-only t)
  (catalog nil :read-only t) (system-id nil :read-only t)
  (profile nil :read-only t) (provenance nil :read-only t) (birth nil :read-only t)
  (root nil :read-only t) (history nil :read-only t) (history-bytes nil :read-only t)
  (timeline nil :read-only t) (namespace nil :read-only t)
  (thread nil :read-only t) (mutex nil :read-only t))

(defstruct (logical-wal-continuation (:constructor %make-logical-wal-continuation))
  "Evidence for a returned, finished prefix within one slot owner scope.
Reuse requires a fresh source check and new scan and archive scopes."
  (source nil :read-only t) (position nil :read-only t) (owner nil :read-only t))

(defun logical-wal-current-root-identity ()
  "Identify the data root by its resolved path, device and inode."
  (let* ((path (truename *pgdata*)) (info (sb-posix:stat path)))
    (list path (sb-posix:stat-dev info) (sb-posix:stat-ino info))))

(defun logical-wal-capture-source (scan)
  "Capture primary slot and source facts under WAL exclusion."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let* ((slot (wal-retained-slot-scan-slot scan))
         (timeline (wal-retained-slot-scan-timeline scan))
         (bytes (unless (= timeline 1) (timeline-history-bytes timeline)))
         (history (timeline-check-lineage
                   (timeline-parse-history timeline (if bytes (bytes->string bytes) ""))))
         (provenance (wal-replication-slot-logical-provenance slot))
         (system-id (physical-slot-startup-system-id)))
    (unless (and *catalog* provenance
                 (eql system-id (logical-slot-provenance-system-id provenance)))
      (error "logical WAL cursor requires known primary slot identity"))
    (logical-slot-check-primary-history slot history)
    (%make-logical-wal-source
     :slot slot :name (copy-seq (wal-retained-slot-scan-name scan))
     :owner (wal-retained-slot-scan-owner scan)
     :database (wal-retained-slot-scan-database scan)
     :plugin (copy-seq (wal-retained-slot-scan-plugin scan))
     :catalog *catalog* :system-id system-id :profile (wal-slot-profile slot)
     :provenance provenance :birth (wal-replication-slot-consistent-lsn slot)
     :root (logical-wal-current-root-identity) :history history :history-bytes bytes
     :timeline timeline :namespace (wal-retained-slot-scan-namespace scan)
     :thread sb-thread:*current-thread* :mutex *wal-lock*)))

(defun logical-wal-check-source (source end)
  "Recheck captured facts and durable control under WAL exclusion.
WAL and control may advance beyond END. CLOG rotation does not replace the
selected WAL source."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let* ((slot (logical-wal-source-slot source))
         (system-id (logical-wal-source-system-id source))
         (timeline (logical-wal-source-timeline source)))
    (wal-check-healthy)
    (unless (and (eq *wal-lock* (logical-wal-source-mutex source))
                 (eq sb-thread:*current-thread* (logical-wal-source-thread source))
                 *wal-ready* (not (recovery-in-progress-p))
                 (eq slot (wal-find-usable-replication-slot-under-lock (logical-wal-source-name source)))
                 (eq (logical-wal-source-owner source) *wal-logical-slot-owner*)
                 (eq (logical-wal-source-owner source) (wal-replication-slot-active-owner slot))
                 (eq :reserved (wal-replication-slot-wal-status slot))
                 (eql (logical-wal-source-database source) (wal-replication-slot-database-oid slot))
                 (equal (logical-wal-source-plugin source) (wal-replication-slot-plugin slot))
                 (eq (logical-wal-source-catalog source) *catalog*)
                 (eq (logical-wal-source-profile source) (wal-slot-profile slot))
                 (eq (logical-wal-source-provenance source) (wal-replication-slot-logical-provenance slot))
                 (eql (logical-wal-source-birth source) (wal-replication-slot-consistent-lsn slot))
                 (equal (logical-wal-source-root source) (logical-wal-current-root-identity))
                 (eql timeline *wal-insert-timeline*)
                 (eq (logical-wal-source-namespace source) *wal-segment-namespace*)
                 (eql system-id (catalog-system-identifier))
                 (eql system-id (physical-slot-startup-system-id))
                 (or (null *wal-control-metadata*)
                     (eql system-id (getf *wal-control-metadata* :system-id)))
                 (<= end *wal-durable-lsn*)
                 (equalp (logical-wal-source-history-bytes source)
                         (unless (= timeline 1) (timeline-history-bytes timeline))))
      (error "logical WAL source changed during cursor scope"))
    (logical-slot-wal-check-control-coordinates
     timeline (logical-wal-source-namespace source) end system-id)
    t))

(defun logical-wal-reusable-continuation (slot position end)
  "Reuse a matching owner-scoped boundary only while its source still matches.
A stale certificate returns NIL so the reader can replay from restart."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((certificate *wal-logical-slot-read-continuation*))
    (when (and certificate
               (eq slot (logical-wal-source-slot (logical-wal-continuation-source certificate)))
               (eql position (logical-wal-continuation-position certificate)))
      (handler-case
          (when (logical-wal-check-source (logical-wal-continuation-source certificate) end)
            certificate)
        (error () nil)))))

(defun logical-wal-certify-boundary (source position)
  "Make boundary evidence after the caller has rechecked the finished prefix."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (plusp position) (error "logical WAL continuation requires a completed prefix"))
  (%make-logical-wal-continuation
   :source source :position position
   :owner (timeline-at-point (1- position) (logical-wal-source-history source))))

(defun logical-slot-wal-scan-start-position (scan)
  "Choose the owner and role for the scan's captured start."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-retained-slot-scan-validate-under-lock scan)
  (let ((certificate (wal-retained-slot-scan-continuation scan))
        (slot (wal-retained-slot-scan-slot scan)))
    (if certificate
        (progn
          (logical-wal-check-source (logical-wal-continuation-source certificate)
                                    (wal-retained-slot-scan-end scan))
          (values (logical-wal-continuation-owner certificate) :prefix-end))
        (let ((owner (wal-replication-slot-restart-timeline slot)))
          (values owner
                  (if (logical-slot-wal-start-proven-p
                       slot (wal-retained-slot-scan-start scan) owner :record-start)
                      :record-start :prefix-end))))))

(defun logical-slot-wal-start-proven-p (slot start owner role)
  "Check a start against the slot's persisted anchors and position role.
Standard feedback can be unaligned, so its exact confirmed position alone is
not a decoding anchor.  A positive restart can prove a prefix end unless a
pending transaction names it as a record start.  Zero restart retains its
record-start owner, not an empty-prefix owner.  The birth prefix has its own
proof from the consistent position and birth timeline."
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

(defun logical-slot-wal-check-control-coordinates (timeline namespace end system-id)
  "Check persisted control against the captured primary and durable horizon."
  (let* ((fields (multiple-value-list (wal-read-control-fields)))
         (metadata (seventh fields)))
    (unless (and (integerp (first fields))
                 (eql (fourth fields) timeline) (eq (sixth fields) namespace)
                 (<= end (second fields) *wal-durable-lsn*)
                 (or (null metadata) (eql system-id (getf metadata :system-id))))
      (error "logical WAL control differs from its captured primary source"))))

(defun call-with-logical-slot-wal-cursor-from-scan (scan owner role function &key (batch-policy :strict))
  "Traverse a pinned scan with a fresh archive and cursor scope.
Pass the cursor and rechecked source facts to FUNCTION; neither a saved
continuation nor its facts retain a pin, reader lease or cached WAL payload."
  (let ((source nil) (certificate (wal-retained-slot-scan-continuation scan)))
    (labels ((check-source ()
               (wal-retained-slot-scan-validate-under-lock scan)
               (logical-wal-check-source source (wal-retained-slot-scan-end scan))))
      (sb-thread:with-mutex (*wal-lock*)
        (wal-retained-slot-scan-validate-under-lock scan)
        (setf source (if certificate (logical-wal-continuation-source certificate)
                         (logical-wal-capture-source scan)))
        (unless (and (eq (logical-wal-source-slot source) (wal-retained-slot-scan-slot scan))
                     (eq (logical-wal-source-owner source) (wal-retained-slot-scan-owner scan))
                     (eql (logical-wal-source-timeline source) (wal-retained-slot-scan-timeline scan))
                     (eq (logical-wal-source-namespace source) (wal-retained-slot-scan-namespace scan)))
          (error "logical WAL continuation source differs from its scan"))
        (unless (if certificate
                    (and (eql (wal-retained-slot-scan-start scan)
                              (logical-wal-continuation-position certificate))
                         (eql owner (logical-wal-continuation-owner certificate))
                         (eq role :prefix-end))
                    (logical-slot-wal-start-proven-p
                     (wal-retained-slot-scan-slot scan) (wal-retained-slot-scan-start scan) owner role))
          (error "logical WAL start lacks a captured slot boundary"))
        (check-source))
      (call-with-optional-logical-wal-archive
       (logical-wal-source-system-id source) (logical-wal-source-history source)
       (logical-wal-source-namespace source) #'check-source
       (lambda (archive)
         (call-with-logical-wal-cursor
          (logical-wal-source-history source) (logical-wal-source-namespace source)
          (wal-retained-slot-scan-start scan) (wal-retained-slot-scan-end scan)
          owner role (if archive (constantly t) #'check-source)
          (lambda (cursor) (funcall function cursor source))
          :archive archive :batch-policy batch-policy))))))

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
     (call-with-logical-slot-wal-cursor-from-scan
      scan owner role (lambda (cursor source) (declare (ignore source)) (funcall function cursor))))))
