;;;; Native replication slot persistence, acknowledgment, and startup restore.

(in-package #:pg)

(defun wal-valid-replication-slot-name-p (name)
  (slot-state-valid-name-p name))

(declaim (ftype function physical-slot-encode physical-slot-decode physical-slot-image-p))

(declaim (ftype function logical-slot-state-valid-p logical-slot-state-required-p
                logical-slot-state-encode logical-slot-state-image-p logical-slot-state-decode))

(defun wal-encode-replication-slot (slot)
  "Encode using the slot's explicit kind; reject unknown kinds.
Logical slots retain PGSLOT4 or PGSLOT5 when representable; provenance or a
wal_level_insufficient cause requires PGSLOT6, or PGSLOT7 with a child owner."
  (when (eq :physical (wal-replication-slot-kind slot))
    (return-from wal-encode-replication-slot (physical-slot-encode slot)))
  (unless (wal-logical-slot-p slot)
    (error "unknown WAL replication slot kind ~s" (wal-replication-slot-kind slot)))
  (unless (logical-slot-state-valid-p slot)
    (error "invalid logical slot state"))
  (when (logical-slot-state-required-p slot)
    (return-from wal-encode-replication-slot (logical-slot-state-encode slot)))
  (let* ((name (wal-replication-slot-name slot))
         (database-oid (wal-replication-slot-database-oid slot))
         (restart-lsn (wal-replication-slot-restart-lsn slot))
         (consistent-lsn (wal-replication-slot-consistent-lsn slot))
         (confirmed-flush-lsn (wal-replication-slot-confirmed-flush-lsn slot))
         (wal-status (wal-replication-slot-wal-status slot))
         (plugin (wal-replication-slot-plugin slot))
         (profile (wal-slot-profile slot)))
    (let* ((prefix (format nil "~a ~a ~a ~a ~a ~a ~a ~a~a"
                           (if (eq profile :text) "PGSLOT4" "PGSLOT5")
                           name (or database-oid "-") restart-lsn consistent-lsn
                           (or confirmed-flush-lsn "-")
                           (string-downcase (symbol-name wal-status)) plugin
                           (if (eq profile :text) "" " binary-bytea")))
           (crc (wal-crc32 (string->bytes prefix))))
      (string->bytes (format nil "~a ~8,'0X~%" prefix crc)))))

(defun wal-finish-replication-slot-creation-under-lock (slot receipt completed)
  ;; Remove only our unpublished candidate. An uncertain or published failure
  ;; keeps its WAL pin and is fenced until startup reads fresh durable state.
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (when (and (not completed)
             (eq slot (gethash (wal-replication-slot-name slot) *wal-replication-slots*)))
    (if (eq (car receipt) :unpublished)
        (remhash (wal-replication-slot-name slot) *wal-replication-slots*)
        (setf (wal-replication-slot-creation-failed-p slot) t
              (wal-replication-slot-active-owner slot) nil
              (wal-replication-slot-active-owner-backend slot) nil)))
  (values))

(defun wal-create-replication-slot-file (slot &optional publication-receipt)
  ;; CREATE publishes a staged directory without replacing an existing name.
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (return-from wal-create-replication-slot-file
      (sb-thread:with-mutex (*wal-lock*)
        (wal-create-replication-slot-file slot publication-receipt))))
  (physical-slot-check-name-under-lock (wal-replication-slot-name slot))
  (wal-require-usable-replication-slot slot)
  (when (wal-replication-slot-temporary-lifetime slot)
    (physical-slot-lifetime-call-under-lock
     (wal-replication-slot-temporary-lifetime slot) :creating slot))
  ;; A protected group fences creation even if the candidate's type changed.
  (logical-slot-continuation-check-slot
   slot (if (wal-replication-slot-temporary-lifetime slot) :physical-create t))
  (slot-state-create (wal-replication-slot-directory)
                     (wal-replication-slot-name slot)
                     (wal-encode-replication-slot slot)
                     :publication-receipt publication-receipt)
  (values))

(defun wal-save-replication-slot (slot)
  ;; UPDATE requires an existing directory and cannot recreate a dropped slot.
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (return-from wal-save-replication-slot
      (sb-thread:with-mutex (*wal-lock*)
        (wal-save-replication-slot slot))))
  (physical-slot-check-name-under-lock (wal-replication-slot-name slot))
  (wal-require-usable-replication-slot slot)
  (when (wal-physical-slot-p slot)
    (physical-slot-check-temporary-candidate-under-lock slot))
  ;; Check the live group before the file mutation.
  (logical-slot-continuation-check-slot slot t)
  (slot-state-update (wal-replication-slot-directory)
                     (wal-replication-slot-name slot)
                     (wal-encode-replication-slot slot))
  (values))

(defun wal-delete-replication-slot-file (name &optional expected-slot)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (physical-slot-check-name-under-lock name)
  (let ((slot (gethash name *wal-replication-slots*)))
    (unless (and slot (or (null expected-slot) (eq slot expected-slot)))
      (error "replication slot deletion requires its exact registered object"))
    (wal-require-known-replication-slot slot)
    (when (wal-physical-slot-p slot)
      (physical-slot-temporary-drop-under-lock slot :drop-check))
    ;; Check the live group before removing its registered member.
    (logical-slot-continuation-check-slot
     slot (if (wal-replication-slot-temporary-lifetime slot) :physical-drop t))
    ;; Save the numeric directory identities before removal. On failure the
    ;; claim and WAL pin remain, and only this generation may retry the drop.
    (unless (wal-replication-slot-drop-claim slot)
      (setf (wal-replication-slot-drop-claim slot)
            (slot-state-claim-drop (wal-replication-slot-directory) name)))
    (when (wal-physical-slot-p slot)
      (physical-slot-temporary-drop-under-lock slot :begin-drop))
    (slot-state-drop (wal-replication-slot-directory) name
                     (wal-replication-slot-drop-claim slot))
    (when (wal-physical-slot-p slot)
      (physical-slot-temporary-drop-under-lock slot :finish-drop)))
  (values))

(defun wal-split-fields (line)
  (loop with start = 0
        for end = (position #\Space line :start start)
        collect (subseq line start end)
        while end
        do (setf start (1+ end))))

(defun wal-decode-replication-slot-image (bytes expected-name)
  "Decode a slot image without applying the local WAL frontier.
Preserve full 64-bit LSNs. This grants no import or runtime authority; callers
must separately validate system, history, lifetime, replay and storage context."
  (unless (and (typep bytes '(vector (unsigned-byte 8))) (<= 1 (length bytes) 2048))
    (error "invalid replication slot state bytes"))
  (when (physical-slot-image-p bytes)
    (return-from wal-decode-replication-slot-image
      (physical-slot-decode bytes expected-name)))
  (when (logical-slot-state-image-p bytes)
    (return-from wal-decode-replication-slot-image (logical-slot-state-decode bytes expected-name)))
  (with-input-from-string (stream (bytes->string bytes))
    (let* ((line (read-line stream nil nil))
           (fields (and line (wal-split-fields line)))
           (version (first fields))
           (v4-p (equal version "PGSLOT4"))
           (v5-p (equal version "PGSLOT5"))
           (expected-count (cond (v4-p 9) (v5-p 10))))
      (unless (and expected-count (= (length fields) expected-count)
                   (every (lambda (field) (plusp (length field))) fields)
                   (null (read-line stream nil nil)))
        (error "replication slot file ~a is malformed" expected-name))
      (let* ((name (second fields))
             (database-text (third fields))
             (database-oid (when (not (string= database-text "-"))
                             (ignore-errors (parse-integer database-text))))
             (database-valid-p (or (string= database-text "-")
                                   (and database-oid (<= 1 database-oid #xffffffff))))
             (restart-lsn (ignore-errors (parse-integer (fourth fields))))
             (consistent-lsn (ignore-errors (parse-integer (fifth fields))))
             (confirmed-text (sixth fields))
             (confirmed-flush-lsn (when (not (string= confirmed-text "-"))
                                    (ignore-errors (parse-integer confirmed-text))))
             (status-text (seventh fields))
             (wal-status (cond ((string= status-text "reserved") :reserved)
                               ((string= status-text "lost") :lost)))
             (plugin (eighth fields))
             (profile (if v5-p (nth 8 fields) "text"))
             (prefix (format nil "~{~a~^ ~}" (butlast fields)))
             (crc (ignore-errors (parse-integer (car (last fields)) :radix 16))))
        (unless (and (wal-valid-replication-slot-name-p name)
                     (string= name expected-name)
                     restart-lsn consistent-lsn crc wal-status database-valid-p
                     (or (string= confirmed-text "-") confirmed-flush-lsn)
                     (wal-valid-slot-plugin-state-p plugin database-oid confirmed-flush-lsn)
                     (or (and (not v5-p) (string= profile "text"))
                         (and v5-p database-oid (string= profile "binary-bytea") (string= plugin "pg_lisp")))
                     (<= 0 restart-lsn #xffffffffffffffff)
                     (<= 0 consistent-lsn #xffffffffffffffff)
                     (or (null confirmed-flush-lsn)
                         (and (<= consistent-lsn confirmed-flush-lsn #xffffffffffffffff)
                              (<= restart-lsn confirmed-flush-lsn)))
                     (or confirmed-flush-lsn (<= restart-lsn consistent-lsn))
                     (= crc (wal-crc32 (string->bytes prefix))))
          (error "replication slot file ~a is corrupt" expected-name))
        (let ((slot (make-wal-replication-slot
                     :name name :database-oid database-oid :restart-lsn restart-lsn
                     :consistent-lsn consistent-lsn :confirmed-flush-lsn confirmed-flush-lsn
                     :wal-status wal-status :plugin plugin)))
          (when v5-p (wal-slot-set-initial-profile slot :binary-bytea))
          slot)))))

(defun wal-decode-replication-slot (bytes expected-name)
  "Decode a slot for local reads and startup, checking logical LSNs against durable WAL."
  (let ((slot (wal-decode-replication-slot-image bytes expected-name)))
    (when (wal-logical-slot-p slot)
      (unless (wal-logical-slot-covered-p slot *wal-durable-lsn*)
        (error "replication slot file ~a is corrupt or outside durable WAL" expected-name)))
    slot))

(defun wal-read-replication-slot (path &key stabilize)
  ;; Canonical state is read through the held slot directory descriptor.
  (if (equal (file-namestring path) "state")
      (let* ((directory (uiop:pathname-directory-pathname path))
             (name (car (last (pathname-directory directory))))
             (root (uiop:pathname-parent-directory-pathname directory)))
        (slot-state-read root name #'wal-decode-replication-slot :stabilize stabilize))
      (wal-decode-replication-slot
       (read-bounded-regular-file path 2048 :stabilize stabilize)
       (pathname-name path))))

(defun wal-validate-replication-slot-wal (slot segments)
  (let ((restart-lsn (wal-replication-slot-restart-lsn slot)))
    ;; A slot at the current durable end does not require a segment yet. Any
    ;; earlier restart point requires every segment through the durable tail.
    (when (and (eq (wal-replication-slot-wal-status slot) :reserved)
               (< restart-lsn *wal-durable-lsn*))
      (let ((first (floor restart-lsn +wal-segment-size+))
            (last (floor (1- *wal-durable-lsn*) +wal-segment-size+)))
        (loop for segment from first to last
              unless (member segment segments :test #'=)
                do (error "WAL required by replication slot ~a is missing segment ~a"
                          (wal-replication-slot-name slot) segment)))))
  slot)

(declaim (ftype function logical-wal-reusable-continuation))

(defun wal-slot-terminal-ack-p (slot target-lsn)
  "Check a terminal acknowledgment without applying metadata batches.
An exact completed prefix with rechecked source and owner can establish the
target without a rescan; a numeric frontier alone cannot. Otherwise scan the
current local timeline for the terminal. Callers check ownership, range and
durability before saving the slot."
  (when (and (wal-replication-slot-logical-provenance slot)
             (logical-wal-reusable-continuation slot target-lsn target-lsn))
    (return-from wal-slot-terminal-ack-p t))
  (let ((*wal-recovery-reservation-batch-clean-p* nil)
        (batch-open nil)
        (found nil)
        (scan-from-lsn (or (wal-replication-slot-confirmed-flush-lsn slot)
                           (wal-replication-slot-consistent-lsn slot)
                           (wal-replication-slot-restart-lsn slot))))
    (wal-scan
     (lambda (record)
       (wal-note-recovery-batch-record record)
       (when (wal-empty-metadata-terminal-type-p (wal-record-type record))
         (wal-validate-empty-metadata-terminal
          record (and batch-open *wal-recovery-reservation-batch-clean-p*)))
       (cond
         ((= (wal-record-type record) +wal-record-batch-start+)
          (setf batch-open t))
         (t
          (when (and batch-open
                     (wal-terminal-record-type-p (wal-record-type record)))
            (when (= (wal-record-end record) target-lsn)
              (setf found t))
            (setf batch-open nil *wal-recovery-reservation-batch-clean-p* nil))))
       (when (or found (>= (wal-record-end record) target-lsn))
         :stop))
     ;; LogicalConfirmReceivedLocation() in src/backend/replication/logical/logical.c
     ;; advances confirmed_flush monotonically. Validate forward from that point,
     ;; even when an older in-flight XID keeps restart_lsn behind it.
     scan-from-lsn)
    found))

(defun wal-slot-rebuild-progress (slot)
  "Rebuild pending transaction WAL from retained records after restart."
  (unless (wal-logical-slot-p slot) (return-from wal-slot-rebuild-progress slot))
  (let ((first-lsns (make-hash-table :test #'eql))
        (committed (make-hash-table :test #'eql))
        (batch-start-lsn nil)
        (pending (make-hash-table :test #'eql)))
    (unless (or (eq (wal-replication-slot-wal-status slot) :lost)
                (and (wal-replication-slot-confirmed-flush-lsn slot)
                     (>= (wal-replication-slot-confirmed-flush-lsn slot)
                         *wal-durable-lsn*)))
      (wal-scan
       (lambda (record)
         (cond
           ((= (wal-record-type record) +wal-record-batch-start+)
            (setf batch-start-lsn (wal-record-start record)))
           ((= (wal-record-type record) +wal-record-pages+)
            (dolist (image (wal-decode-page-images (wal-record-payload record)
                                                   (wal-record-end record)))
              (dolist (xid (wal-page-image-xids image))
                (unless (gethash xid first-lsns)
                  (setf (gethash xid first-lsns) batch-start-lsn)))))
           ((= (wal-record-type record) +wal-record-commit+)
            (let* ((xid (wal-record-xid record))
                   (first-lsn (gethash xid first-lsns)))
              (when (and (plusp xid) first-lsn)
                (setf (gethash xid committed)
                      (cons first-lsn (wal-record-end record))))))))
       (wal-replication-slot-restart-lsn slot))
      (maphash
       (lambda (xid entry)
         (let ((confirmed (wal-replication-slot-confirmed-flush-lsn slot)))
           (when (or (null confirmed) (> (cdr entry) confirmed))
             (setf (gethash xid pending) entry))))
       committed)
      (setf (wal-replication-slot-pending-xids slot) pending))
    slot))

(defun wal-slot-pending-after-ack (slot acknowledged-lsn)
  (let ((pending (make-hash-table :test #'eql)))
    (maphash (lambda (xid entry)
               (let ((commit-lsn (cdr entry)))
                 (unless (or (and commit-lsn (<= commit-lsn acknowledged-lsn))
                             ;; CLOG stores the aborted state as the two-bit
                             ;; value 2 (see access/transam/clog.lisp).
                             (= (clog-get xid) 2))
                   (setf (gethash xid pending) (cons (car entry) commit-lsn)))))
             (wal-replication-slot-pending-xids slot))
    pending))

(defun wal-slot-restart-for-ack (acknowledged-lsn pending)
  (let ((restart-lsn acknowledged-lsn))
    (maphash (lambda (xid entry)
               (declare (ignore xid))
               (setf restart-lsn (min restart-lsn (car entry))))
             pending)
    restart-lsn))

(declaim (ftype function logical-slot-save-update-under-lock))

(declaim (ftype function logical-slot-acknowledgment-positions-under-lock))

(defun wal-check-slot-ack-publication-under-lock (slot)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (when (and slot (wal-replication-slot-child-owner slot))
    (ereport "55000" "imported slot acknowledgment requires a prepared continuation publication"))
  t)

(defun wal-replication-slot-ack-candidate-under-lock
    (slot acknowledged-lsn &optional (restart-floor acknowledged-lsn))
  ;; Derive a fresh candidate from current pending transactions without
  ;; changing or saving the slot.  The ordinary persistence path still
  ;; checks its publication fence before calling this helper.
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-require-logical-slot slot)
  (let* ((pending (wal-slot-pending-after-ack slot acknowledged-lsn))
         (candidate (wal-copy-replication-slot-with-profile slot)))
    (setf (wal-replication-slot-confirmed-flush-lsn candidate) acknowledged-lsn
          (wal-replication-slot-pending-xids candidate) pending)
    (if (wal-replication-slot-logical-provenance slot)
        (multiple-value-bind (restart-lsn restart-timeline confirmed-timeline)
            (logical-slot-acknowledgment-positions-under-lock
             slot acknowledged-lsn restart-floor pending)
          (setf (wal-replication-slot-restart-lsn candidate) restart-lsn
                (wal-replication-slot-restart-timeline candidate) restart-timeline
                (wal-replication-slot-confirmed-timeline candidate) confirmed-timeline))
        (setf (wal-replication-slot-restart-lsn candidate)
              (wal-slot-restart-for-ack restart-floor pending)))
    candidate))

(declaim (ftype function logical-slot-apply-catalog-evidence))

(defun wal-persist-replication-slot-ack-under-lock (slot acknowledged-lsn
                                                    &optional (restart-floor acknowledged-lsn) catalog-evidence)
  "Persist an already-validated acknowledgment while *WAL-LOCK* is held.
Known provenance selects timeline owners from primary history; slots with
unknown provenance retain the LSN-only path. The guarded update saves before publishing progress
to the live slot. Startup and scan admission also check history, since no-op
acknowledgments and resume paths can return before this helper."
  (wal-check-slot-ack-publication-under-lock slot)
  (let ((candidate (wal-replication-slot-ack-candidate-under-lock slot acknowledged-lsn restart-floor)))
    (logical-slot-apply-catalog-evidence candidate acknowledged-lsn catalog-evidence)
    (logical-slot-save-update-under-lock slot candidate)))

(declaim (ftype function call-with-logical-slot-terminal-check))

(defun wal-check-replication-slot-ack-under-lock (slot name acknowledged-lsn expected-owner)
  "Check acknowledgment permission and bounds; return the confirmed LSN."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-healthy)
  (unless slot (ereport "42704" "replication slot \"~a\" does not exist" name))
  (wal-require-logical-slot slot)
  (when (and expected-owner
             (not (eq (wal-replication-slot-active-owner slot) expected-owner)))
    (ereport "55006" "replication slot \"~a\" is not owned by this reader" name))
  (when (and (wal-replication-slot-active-owner slot)
             (not (eq (wal-replication-slot-active-owner slot) *wal-logical-slot-owner*)))
    (ereport "55006" "replication slot \"~a\" is active" name))
  (when (eq (wal-replication-slot-wal-status slot) :lost)
    (ereport "55000" "replication slot \"~a\" has lost its WAL" name))
  (let ((confirmed (wal-replication-slot-confirmed-flush-lsn slot)))
    (unless confirmed
      (ereport "55000" "initial replication slot acknowledgment requires its owner"))
    (when (< acknowledged-lsn confirmed)
      (ereport "22023" "replication slot acknowledgment cannot move backwards"))
    (when (> acknowledged-lsn *wal-durable-lsn*)
      (ereport "22023" "replication slot acknowledgment is past durable WAL"))
    confirmed))

(defun wal-acknowledge-replication-slot
    (name acknowledged-lsn &optional expected-owner)
  "Durably advance a slot after a consumer completes a terminal WAL batch.
An unchanged acknowledgment returns without changing its stored owners."
  (unless (and (integerp acknowledged-lsn)
               (<= 0 acknowledged-lsn #xFFFFFFFFFFFFFFFF))
    (ereport "22023" "invalid replication slot acknowledgment LSN"))
  (call-with-logical-slot-terminal-check
   name acknowledged-lsn
   (lambda (slot)
     (let ((confirmed (wal-check-replication-slot-ack-under-lock
                       slot name acknowledged-lsn expected-owner)))
       (values (= confirmed acknowledged-lsn) slot)))
   (lambda (slot found)
     (let ((confirmed (wal-check-replication-slot-ack-under-lock
                       slot name acknowledged-lsn expected-owner)))
       (cond ((= confirmed acknowledged-lsn) slot)
             (found (wal-persist-replication-slot-ack-under-lock slot acknowledged-lsn))
             (t (ereport "22023"
                         "replication slot acknowledgment must name a terminal WAL batch LSN")))))))

(defun wal-finalize-replication-slot-copy (name boundary-lsn expected-owner)
  "Confirm an initial snapshot boundary for its exact active slot owner."
  (unless (and (integerp boundary-lsn)
               (<= 0 boundary-lsn #xFFFFFFFFFFFFFFFF))
    (ereport "22023" "invalid initial replication slot boundary LSN"))
  (sb-thread:with-mutex (*wal-lock*)
    (wal-check-healthy)
    (let ((slot (wal-find-usable-replication-slot-under-lock name)))
      (unless slot
        (ereport "42704" "replication slot \"~a\" does not exist" name))
      (wal-require-logical-slot slot)
      (unless (and expected-owner
                   (eq (wal-replication-slot-active-owner slot) expected-owner))
        (ereport "55006" "initial replication slot finalization requires its active owner"))
      (when (eq (wal-replication-slot-wal-status slot) :lost)
        (ereport "55000" "replication slot \"~a\" has lost its WAL" name))
      (when (wal-replication-slot-confirmed-flush-lsn slot)
        (ereport "55000" "replication slot initial copy is already finalized"))
      (unless (= boundary-lsn (wal-replication-slot-consistent-lsn slot))
        (ereport "22023" "initial replication acknowledgment must name the consistent LSN"))
      (when (> boundary-lsn *wal-durable-lsn*)
        (ereport "22023" "initial replication acknowledgment is past durable WAL"))
      ;; A snapshot boundary is captured under the writer gate and need not be
      ;; rediscovered by scanning a terminal record. Match SnapBuild's one-time
      ;; consistent-point confirmation while retaining exact owner validation.
      (wal-persist-replication-slot-ack-under-lock slot boundary-lsn))))

(defun wal-format-lsn (lsn)
  (format nil "~X/~8,'0X" (ash lsn -32) (logand lsn #xFFFFFFFF)))

(defun wal-replication-slot-rows ()
  "Return the private eight-column logical-only snapshot for existing consumers."
  (sb-thread:with-mutex (*wal-lock*)
    (loop for slot being the hash-values of *wal-replication-slots*
          when (wal-logical-slot-p slot)
            collect
            (vector (wal-replication-slot-name slot)
                    (if (wal-replication-slot-active-owner slot) t nil)
                    (wal-format-lsn (wal-replication-slot-restart-lsn slot))
                    (if (wal-replication-slot-confirmed-flush-lsn slot)
                        (wal-format-lsn
                         (wal-replication-slot-confirmed-flush-lsn slot))
                        +null+)
                    (if (eq (wal-replication-slot-wal-status slot) :lost)
                        0
                        (max 0 (- *wal-durable-lsn*
                                  (wal-replication-slot-restart-lsn slot))))
                    (string-downcase
                     (symbol-name (wal-replication-slot-wal-status slot)))
                    (wal-replication-slot-plugin slot)
                    (or (wal-replication-slot-database-oid slot) +null+)))))

(declaim (ftype function physical-slot-restore physical-slot-check-standby-slots physical-slot-startup-system-id))

(declaim (ftype function logical-slot-check-startup-identities))

(declaim (ftype function logical-slot-check-startup-history))

(declaim (ftype function logical-slot-load-startup-inventory))

(defun wal-load-replication-slots (&key gate-owner ownership)
  "Validate startup slots before publishing their registry.
Known logical slots require the real postmaster and gate-owned startup source."
  (logical-slot-load-startup-inventory gate-owner ownership))
