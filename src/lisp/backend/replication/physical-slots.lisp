(in-package #:pg)

;;; Durable physical slot state and primary startup validation.
;;; Slot ownership is process-local; the image records retention identity and
;;; restart state, not the active owner.

(defun physical-slot-state-valid-p (slot)
  "Check physical kind and durable field shape, including the paired nullable
restart LSN and timeline. A null restart or lost slot does not retain WAL."
  (let ((restart (wal-replication-slot-restart-lsn slot))
        (timeline (wal-replication-slot-restart-timeline slot)))
    (and (eq :physical (wal-replication-slot-kind slot))
         (wal-valid-replication-slot-name-p (wal-replication-slot-name slot))
         (typep (wal-replication-slot-system-id slot) '(unsigned-byte 64))
         (null (wal-replication-slot-database-oid slot))
         (null (wal-replication-slot-plugin slot))
         (null (wal-replication-slot-consistent-lsn slot))
         (null (wal-replication-slot-confirmed-flush-lsn slot))
         (zerop (hash-table-count (wal-replication-slot-pending-xids slot)))
         (or (and (null restart) (null timeline))
             (and (typep restart '(integer 1 18446744073709551615))
                  (typep timeline '(integer 1 4294967295))))
         (member (wal-replication-slot-wal-status slot) '(:reserved :lost))
         (member (wal-replication-slot-temporary-p slot) '(nil t))
         (typep (wal-replication-slot-inactive-since slot) '(signed-byte 64)))))

(defun physical-slot-check-position (slot system-id history frontier)
  "Check slot identity and lineage against FRONTIER; return a reserved restart.
A null or lost restart needs no WAL coverage."
  (unless (and (physical-slot-state-valid-p slot)
               (eql system-id (wal-replication-slot-system-id slot))
               (typep frontier '(unsigned-byte 64)))
    (error "physical slot belongs to another cluster or has invalid durable state"))
  (timeline-check-lineage history)
  (let ((restart (wal-replication-slot-restart-lsn slot)))
    (when (and restart (eq :reserved (wal-replication-slot-wal-status slot)))
      (unless (and (<= restart frontier)
                   (timeline-prefix-boundary-p
                    (wal-replication-slot-restart-timeline slot) restart history))
        (error "physical slot restart is outside durable source history"))
      restart)))

(defun physical-slot-check-retained-range (slot system-id history frontier read-segment)
  "Check exact system identity and restart position against source history.
A reserved restart may be inside a segment; require coverage through FRONTIER
and validate native record placement across segment gaps. Null or lost slots
need no WAL read. READ-SEGMENT supplies the selected lineage."
  (let ((restart (physical-slot-check-position slot system-id history frontier)))
    (when restart
      (when (< restart frontier)
        (let ((previous-end nil)
              (previous-bytes nil)
              (last (floor (1- frontier) +wal-segment-size+)))
          (loop for segment from (floor restart +wal-segment-size+) to last
                for origin = (* segment +wal-segment-size+)
                for bytes = (funcall read-segment segment (max restart origin))
                do (unless (and (typep bytes '(vector (unsigned-byte 8)))
                                (<= 1 (length bytes) +wal-segment-size+)
                                (or (< segment last)
                                    (<= (- frontier origin) (length bytes))))
                     (error "physical slot ~a lacks required WAL in segment ~a"
                            (wal-replication-slot-name slot) segment))
                   (when (and previous-end (< previous-end origin))
                     (wal-lineage-scan-segment
                      (constantly nil) previous-bytes (1- segment)
                      (length previous-bytes) 0 previous-end history
                      (timeline-history-entry-timeline (first history)) nil)
                     (wal-lineage-check-gap
                      previous-end (wal-lineage-read-record bytes segment 0 (length bytes)) history))
                   (setf previous-end (+ origin (length bytes))
                         previous-bytes bytes))))))
  slot)

(declaim (ftype function physical-source-lineage-segment))

(defun physical-slot-check-primary-wal (slot system-id)
  "Under WAL exclusion, check retained coverage against durable primary history.
Eligible ancestor segments may satisfy a public physical slot."
  (sb-thread:with-mutex (*wal-lock*)
    (when (recovery-in-progress-p)
      (error "physical slot primary validation cannot run during recovery"))
    (let* ((timeline *wal-insert-timeline*)
           (history (timeline-check-lineage
                     (timeline-parse-history
                      timeline (if (= timeline 1) "" (bytes->string (timeline-history-bytes timeline)))))))
      (physical-slot-check-retained-range
       slot system-id history *wal-durable-lsn*
       (lambda (segment cursor)
         (physical-source-lineage-segment system-id timeline history segment cursor t))))))

;;; Standby startup validates the slot batch against retained and received WAL.
;;; Startup isolation or exclusive quiescence keeps the mutable spool stable.

(defun physical-slot-check-receive-chain (store index replay)
  "Scan retained extents and spool once from base through floor to durable flush.
Check native gaps, the FNV floor and flush digests, and a replay record boundary.
The caller protects the root and excludes changes to the receive spool."
  (let* ((history (physical-store-retained-history store))
         (cursor (physical-store-base-lsn store))
         (digest #xCBF29CE484222325)
         (floor (physical-store-floor-lsn store))
         (floor-seen (= cursor floor))
         (replay-seen (= cursor replay)))
    (when (and floor-seen (/= digest (physical-store-floor-digest store)))
      (error "physical slot receive floor has an invalid base digest"))
    (physical-follow-map-active-prefix
     store index
     (lambda (record raw)
       (unless (= cursor (wal-record-start record))
         (wal-lineage-check-gap cursor record history))
       (setf cursor (wal-record-end record)
             digest (physical-store-update-digest digest raw))
       (when (= cursor replay) (setf replay-seen t))
       (when (= cursor floor)
         (unless (= digest (physical-store-floor-digest store))
           (error "physical slot archive and receive floor digests differ"))
         (setf floor-seen t))
       (when (and (> cursor floor) (not floor-seen))
         (error "physical slot archive skipped the receive floor")))
     (physical-store-base-lsn store))
    (unless (and floor-seen replay-seen
                 (= cursor (physical-store-flushed-lsn store))
                 (= digest (physical-store-flushed-digest store)))
      (error "physical slot WAL does not reach its replay and durable receive boundaries"))
    t))

(defun physical-slot-check-standby-control (state system-id replay)
  "Match recovery phase, system identity and base provenance to durable control;
require replay to cover both the base and durable applied position."
  (let ((control (physical-recovery-read-control *pgdata*)))
    (unless (and (member (getf state :phase) '(1 2))
                 (eql (getf state :phase) (getf control :phase))
                 (eql system-id (getf state :system-id))
                 (every (lambda (key) (eql (getf state key) (getf control key)))
                        '(:system-id :timeline :manifest-digest :checkpoint))
                 (typep replay '(unsigned-byte 64))
                 (<= (getf state :end) replay)
                 (<= (getf control :applied) replay))
      (error "physical slot standby identity or replay differs from durable recovery"))
    control))

(defun physical-slot-check-standby-overlap (state restarts)
  "Verify the manifest-bound base overlap and reject restarts before its segment."
  (multiple-value-bind (bytes entry)
      (physical-recovery-read-base-overlap state *pgdata*)
    (declare (ignore entry))
    (let ((origin (- (getf state :end) (length bytes))))
      (unless (every (lambda (restart) (>= restart origin)) restarts)
        (error "physical slot restart precedes the immutable standby overlap")))))

(defun physical-slot-check-standby-receive (store state replay)
  "Check receive identity and replay against a checked candidate, then recheck
its control tuple. The caller holds a root lease and excludes spool mutation;
the candidate and tuple recheck alone do not protect the spool."
  (unless (and (eql (physical-store-system-id store) (getf state :system-id))
               (eql (physical-store-timeline store) (getf state :timeline))
               (eql (physical-store-base-lsn store) (getf state :end))
               (equal (physical-store-retained-identity store)
                      (physical-recovery-retained-identity state))
               (<= replay (physical-store-flushed-lsn store)))
    (error "physical slot receive store differs from its immutable recovery base"))
  (let* ((tuple (physical-store-control-tuple store))
         (candidate (physical-follow-copy-checked-store store))
         (index (physical-follow-controller-index candidate)))
    (physical-recovery-check-store-point candidate replay (physical-recovery-replay-timeline state))
    (physical-slot-check-receive-chain candidate index replay)
    (unless (equal tuple (physical-store-control-tuple store))
      (error "physical slot receive state changed during validation"))
    (physical-store-read-retained-control store tuple))
  t)

(defun physical-slot-check-standby-slots (slots system-id)
  "Validate a batch against durable recovery identity and applied and replay pairs.
For reserved restarts, check base overlap and scan the protected receive chain
once, regardless of slot count. Null and lost slots require no WAL reads.

Call only during owned startup or equivalent exclusive quiescence: the caller
must exclude receive, replay, compaction and other spool changes for the entire
proof. The root lease protects retained objects, not the mutable spool. There
are only startup calls in production."
  (unless (recovery-in-progress-p)
    (error "physical slot standby validation requires verified recovery"))
  (let* ((state (copy-tree *physical-recovery-state*))
         (replay *last-replay-lsn*)
         (control (physical-slot-check-standby-control state system-id replay))
         (store *physical-receive-store*)
         (history (if store (physical-store-retained-history store)
                      (timeline-check-lineage
                       (timeline-parse-history
                        (getf state :timeline)
                        (if (= 1 (getf state :timeline)) ""
                            (bytes->string (timeline-history-bytes (getf state :timeline))))))))
         (restarts (remove nil (mapcar
                                (lambda (slot)
                                  (physical-slot-check-position slot system-id history replay))
                                slots))))
    (unless (eq (not (null store)) (= 2 (getf state :phase)))
      (error "physical slot recovery phase differs from receive storage"))
    (unless
        (if store
            (and (<= (physical-store-base-lsn store) (getf control :applied)
                     replay (physical-store-flushed-lsn store))
                 (eql (physical-recovery-replay-timeline state)
                      (physical-store-prefix-timeline store replay))
                 (eql (getf control :applied-timeline (getf control :timeline))
                      (physical-store-prefix-timeline store (getf control :applied))))
            (and (= replay (getf state :end))
                 (eql (physical-recovery-replay-timeline state) (getf state :timeline))
                 (eql (getf control :applied-timeline (getf control :timeline))
                      (getf state :timeline))))
      (error "physical slot standby replay pair differs from its durable receive history"))
    (when restarts
      (physical-slot-check-standby-overlap state restarts)
      (if store
          (physical-follow-call-with-receive-root
           store (lambda () (physical-slot-check-standby-receive store state replay)))
          (unless (= replay (getf state :end))
            (error "physical slot recovery beyond base requires a durable receive store"))))
    (unless (and (equalp state *physical-recovery-state*)
                 (= replay *last-replay-lsn*)
                 (equalp control (physical-recovery-read-control *pgdata*)))
      (error "physical slot recovery witnesses changed during validation")))
  t)

(defun physical-slot-startup-system-id ()
  "Read the durable catalog identity for slot restoration. Primary slot loading
precedes catalog initialization, so startup must not invent an identity."
  (unless (probe-file (catalog-path))
    (error "physical slot startup requires the durable cluster catalog"))
  (let* ((*catalog-identity-needs-persist-p* nil)
         (catalog (load-catalog)))
    (when *catalog-identity-needs-persist-p*
      (error "physical slot startup cannot invent a missing cluster identity"))
    (catalog-system-identifier catalog)))

(defun physical-slot-restore (slot inactive-since)
  "Remove temporary slots at startup and set the persistent inactive timestamp.
Validate primary WAL here; standby slots are checked together after loading."
  (cond
    ((wal-replication-slot-temporary-p slot)
     (wal-delete-replication-slot-file (wal-replication-slot-name slot))
     nil)
    (t
     (unless (recovery-in-progress-p)
       (physical-slot-check-primary-wal slot (physical-slot-startup-system-id)))
     (setf (wal-replication-slot-inactive-since slot) inactive-since)
     slot)))

(defun physical-slot-image-p (bytes)
  (let ((magic (string->bytes "PGPHYSICALSLOT1 ")))
    (and (>= (length bytes) (length magic))
         (loop for index below (length magic) always (= (aref bytes index) (aref magic index))))))

(defun physical-slot-encode (slot)
  "Encode checked durable slot fields with a checksum. Active and temporary
owners and pending process state are not persisted."
  (unless (physical-slot-state-valid-p slot) (error "invalid physical replication slot state"))
  (let* ((prefix (format nil "PGPHYSICALSLOT1 ~a ~d ~a ~a ~a ~d ~d"
                         (wal-replication-slot-name slot) (wal-replication-slot-system-id slot)
                         (or (wal-replication-slot-restart-timeline slot) "-")
                         (or (wal-replication-slot-restart-lsn slot) "-")
                         (string-downcase (symbol-name (wal-replication-slot-wal-status slot)))
                         (if (wal-replication-slot-temporary-p slot) 1 0)
                         (wal-replication-slot-inactive-since slot)))
         (crc (wal-crc32 (string->bytes prefix))))
    (string->bytes (format nil "~a ~8,'0X~%" prefix crc))))

(defun physical-slot-number (text &key nullable signed)
  (when (and nullable (equal text "-")) (return-from physical-slot-number nil))
  (unless (and (stringp text) (<= 1 (length text) 20)
               (let ((start (if (and signed (char= (char text 0) #\-)) 1 0)))
                 (and (< start (length text))
                      (every (lambda (char) (find char "0123456789")) (subseq text start)))))
    (error "invalid physical replication slot integer"))
  (let ((value (parse-integer text)))
    (unless (typep value (if signed '(signed-byte 64) '(unsigned-byte 64)))
      (error "physical replication slot integer is out of range"))
    value))

(defun physical-slot-decode (bytes name)
  "Decode a bounded, checksummed image and check its name and physical fields.
The returned slot has fresh process-local ownership and transaction state."
  (unless (and (typep bytes '(vector (unsigned-byte 8))) (<= 1 (length bytes) 2048)
               (= 10 (aref bytes (1- (length bytes))))
               (every (lambda (byte) (or (= byte 10) (<= 32 byte 126))) bytes))
    (error "invalid physical replication slot image"))
  (let* ((text (bytes->string (subseq bytes 0 (1- (length bytes)))))
         (fields (wal-split-fields text))
         (prefix-end (position #\Space text :from-end t)))
    (unless (and (= 9 (length fields)) (equal (first fields) "PGPHYSICALSLOT1")
                 (equal name (second fields)) (wal-valid-replication-slot-name-p name)
                 (member (sixth fields) '("reserved" "lost") :test #'equal)
                 (member (seventh fields) '("0" "1") :test #'equal)
                 (= 8 (length (ninth fields)))
                 (every (lambda (char) (find char "0123456789ABCDEFabcdef")) (ninth fields))
                 (= (parse-integer (ninth fields) :radix 16)
                    (wal-crc32 (string->bytes (subseq text 0 prefix-end)))))
      (error "physical replication slot name, schema or checksum differs"))
    (let ((slot (make-wal-replication-slot
                 :name name :kind :physical :system-id (physical-slot-number (third fields))
                 :restart-timeline (physical-slot-number (fourth fields) :nullable t)
                 :restart-lsn (physical-slot-number (fifth fields) :nullable t)
                 :consistent-lsn nil :confirmed-flush-lsn nil :plugin nil
                 :wal-status (if (equal (sixth fields) "reserved") :reserved :lost)
                 :temporary-p (equal (seventh fields) "1")
                 :inactive-since (physical-slot-number (eighth fields) :signed t))))
      (unless (physical-slot-state-valid-p slot) (error "invalid decoded physical replication slot state"))
      slot)))
