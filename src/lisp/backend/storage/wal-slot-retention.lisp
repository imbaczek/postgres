;;;; WAL retention horizons and advisory replication slot status.
(in-package #:pg)

(defun wal-slot-retention-mode (policy)
  "Return POLICY's mode."
  (wal-slot-retention-policy-mode policy))

(defun wal-slot-retention-limit (policy)
  "Return POLICY's byte limit, or NIL if the cap is disabled."
  (wal-slot-retention-policy-limit policy))

(defun wal-slot-reserved-p (slot)
  "Return the healthy internal restart floor, if any.
This is distinct from the advisory public reserved, extended or unreserved status.
Reject uncertainty before retention limits can bypass the old pin."
  (when (wal-replication-slot-state-uncertain-p slot)
    (error "cannot recycle WAL while a replication slot state is uncertain"))
  (and (eq :reserved (wal-replication-slot-wal-status slot))
       (wal-replication-slot-restart-lsn slot)))

(defun wal-keep-retention-segment (current keep-mb)
  "Return the wal_keep_size floor at CURRENT, rounding down to whole segments.
Native WAL starts at segment zero, so clamp there rather than C's segment one."
  (max 0 (- (floor current +wal-segment-size+)
            (floor (* keep-mb 1024 1024) +wal-segment-size+))))

(defun wal-independent-retention-segment (redo-segment current keep-mb)
  "Combine redo, wal_keep_size, active XID and snapshot floors independent of slots.
The caller holds WAL; take the XID lock to copy active positions. CURRENT is the
checkpoint endpoint used for both the initial calculation and later rechecks."
  (let ((positions
          (append
           (sb-thread:with-mutex (*xid-lock*)
             (loop for lsn being the hash-values of *wal-xid-first-lsn* collect lsn))
           (loop for lsn being the hash-values of *wal-snapshot-retention-pins* collect lsn))))
    (min redo-segment
         (if (plusp keep-mb) (wal-keep-retention-segment current keep-mb) redo-segment)
         (if positions (floor (reduce #'min positions) +wal-segment-size+) redo-segment))))

(defun wal-slot-removal-segment (redo-segment independent-segment current policy)
  "Combine independent retention with reserved slot floors under the WAL lock.
The standard policy rounds its limit down to whole WAL segments."
  (let* ((positions (loop for slot being the hash-values of *wal-replication-slots*
                          when (wal-slot-reserved-p slot)
                            collect (wal-replication-slot-restart-lsn slot)))
         (limit (wal-slot-retention-limit policy))
         (slot-segment (when positions (floor (reduce #'min positions) +wal-segment-size+))))
    (when (and slot-segment limit (eq :segments (wal-slot-retention-mode policy)))
      (setf slot-segment
            (max slot-segment (- (floor current +wal-segment-size+)
                                 (floor limit +wal-segment-size+)))))
    (min redo-segment independent-segment (or slot-segment redo-segment))))

(declaim (ftype function physical-slot-recovery-write-lsn))

(defun wal-slot-safe-wal-bytes (slot policy keep-mb)
  "Report remaining safe bytes from the write endpoint, which can be negative.
For the standard segment policy use the greater of slot cap and wal_keep_size,
plus one segment. The native byte policy retains its own exact-byte calculation.
The caller holds WAL and supplies POLICY and KEEP-MB from one configuration."
  (let* ((restart (wal-replication-slot-restart-lsn slot))
         (limit (wal-slot-retention-limit policy))
         (mode (wal-slot-retention-mode policy))
         (write (if (recovery-in-progress-p) (physical-slot-recovery-write-lsn) *wal-end-lsn*)))
    (if (or (null limit) (eq :lost (wal-replication-slot-wal-status slot))
            (and (eq mode :bytes) (null restart)))
        +null+
        (- (ecase mode
             (:segments (* (+ (floor (or restart 0) +wal-segment-size+)
                              (max (floor limit +wal-segment-size+)
                                   (floor (* keep-mb 1024 1024) +wal-segment-size+)) 1)
                           +wal-segment-size+))
             (:bytes (+ restart limit 1)))
           write))))

(defun wal-slot-byte-limit-exceeded-p (slot current limit)
  "Test the native byte cap at CURRENT; the caller chooses write or durable WAL."
  (and (integerp limit)
       (> (- current (wal-replication-slot-restart-lsn slot)) limit)))

(defun wal-slot-within-normal-window-p (restart write policy)
  "Test RESTART against the normal max_wal_size window in POLICY."
  (let* ((maximum (wal-slot-retention-policy-max-wal-bytes policy))
         (oldest (max 0 (- (floor write +wal-segment-size+)
                           (1+ (floor maximum +wal-segment-size+))))))
    (>= (floor restart +wal-segment-size+) oldest)))

(defun wal-slot-catalog-status (slot policy keep-mb)
  "Compute advisory public WAL status at the write endpoint; do not invalidate.
The standard horizon includes wal_keep_size but excludes XID and snapshot floors,
as in C's metadata calculation. Lost slots remain lost. The caller holds WAL
and supplies POLICY and KEEP-MB from the same captured configuration."
  (when (eq :lost (wal-replication-slot-wal-status slot))
    (return-from wal-slot-catalog-status "lost"))
  (let ((restart (wal-replication-slot-restart-lsn slot)))
    (unless restart (return-from wal-slot-catalog-status +null+))
    (let* ((write (if (recovery-in-progress-p) (physical-slot-recovery-write-lsn) *wal-end-lsn*))
           (current (floor write +wal-segment-size+))
           (unreserved
             (ecase (wal-slot-retention-mode policy)
               (:bytes (wal-slot-byte-limit-exceeded-p slot write (wal-slot-retention-policy-limit policy)))
               (:segments (< (floor restart +wal-segment-size+)
                             (wal-slot-removal-segment current (wal-keep-retention-segment write keep-mb)
                                                       write policy))))))
      (cond (unreserved "unreserved")
            ((wal-slot-within-normal-window-p restart write policy) "reserved")
            (t "extended")))))
