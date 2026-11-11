(in-package #:pg)

;;; Logical slot provenance and durable PGSLOT6 image encoding.
;;; These helpers describe source identity and positions without admitting a slot.

(defstruct (logical-slot-provenance
            (:constructor %make-logical-slot-provenance
                (system-id generation birth-timeline on-start-lsn on-start-timeline)))
  "Immutable slot identity and optional logical ON interval evidence.
Provenance validation checks shape. The image codec also checks framing, CRC
and canonical bytes. Startup checks known system IDs against the durable
catalog and available control metadata. Creation stamping, acknowledgment
guards, cause publication, SQL projection and import remain unwired; callers
must separately prove history and lifetime."
  (system-id nil :read-only t)
  (generation nil :read-only t)
  (birth-timeline nil :read-only t)
  (on-start-lsn nil :read-only t)
  (on-start-timeline nil :read-only t))

(defun logical-slot-position-valid-p (lsn timeline)
  "Check an unsigned 64-bit LSN and a nonzero 32-bit timeline."
  (and (typep lsn '(unsigned-byte 64))
       (typep timeline '(integer 1 4294967295))))

(defun logical-slot-provenance-valid-p (provenance birth-lsn)
  "Check numeric bounds and the optional ON pair against BIRTH-LSN.
A present provenance object requires numeric system ID and generation, including
zero. Only its ON pair is optional; NIL for the whole object means unknown.
History and source identity require separate verification."
  (and (logical-slot-provenance-p provenance)
       (logical-slot-position-valid-p
        birth-lsn (logical-slot-provenance-birth-timeline provenance))
       (typep (logical-slot-provenance-system-id provenance)
              '(unsigned-byte 64))
       (typep (logical-slot-provenance-generation provenance) '(unsigned-byte 128))
       (wal-logical-on-interval-valid-p
        t (logical-slot-provenance-on-start-lsn provenance)
        (logical-slot-provenance-on-start-timeline provenance)
        birth-lsn (logical-slot-provenance-birth-timeline provenance))))

(defun make-logical-slot-provenance
    (system-id generation birth-lsn birth-timeline &optional on-start-lsn on-start-timeline)
  "Construct immutable provenance after checking its bounds against BIRTH-LSN."
  (let ((provenance (%make-logical-slot-provenance
                     system-id generation birth-timeline on-start-lsn on-start-timeline)))
    (unless (logical-slot-provenance-valid-p provenance birth-lsn)
      (error "invalid logical slot provenance"))
    provenance))

(defun logical-slot-check-startup-identities (slots)
  "Check known logical slot system IDs before publishing the startup registry.
When any logical slot has known provenance, read the durable catalog identity
once and compare it with each known ID and available control metadata. Zero is
valid; slots with unknown provenance are unchanged. This does not initialize or
repair the catalog, or prove history and lifetime."
  (let ((known (remove-if-not
                (lambda (slot)
                  (and (wal-logical-slot-p slot)
                       (wal-replication-slot-logical-provenance slot)))
                slots)))
    (when known
      (let ((system-id (physical-slot-startup-system-id)))
        (unless (and (typep system-id '(unsigned-byte 64))
                     (or (null *wal-control-metadata*)
                         (eql system-id (getf *wal-control-metadata* :system-id))))
          (error "logical slot startup catalog and control identities differ"))
        (dolist (slot known)
          (unless (= system-id
                     (logical-slot-provenance-system-id
                      (wal-replication-slot-logical-provenance slot)))
            (error "logical slot ~a belongs to another cluster"
                   (wal-replication-slot-name slot)))))))
  (values))

(defun logical-slot-check-update-under-lock (slot candidate)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-require-logical-slot slot)
  (wal-require-logical-slot candidate)
  (unless (and (not (eq slot candidate))
               (eq slot (gethash (wal-replication-slot-name slot) *wal-replication-slots*))
               (logical-slot-state-valid-p slot)
               (logical-slot-state-valid-p candidate)
               (equal (wal-replication-slot-name slot) (wal-replication-slot-name candidate))
               (eql (wal-replication-slot-database-oid slot) (wal-replication-slot-database-oid candidate))
               (eql (wal-replication-slot-consistent-lsn slot) (wal-replication-slot-consistent-lsn candidate))
               (equal (wal-replication-slot-plugin slot) (wal-replication-slot-plugin candidate))
               (eq (wal-slot-profile slot) (wal-slot-profile candidate))
               (eq (wal-replication-slot-logical-provenance slot)
                   (wal-replication-slot-logical-provenance candidate))
               (hash-table-p (wal-replication-slot-pending-xids candidate)))
    (error "logical slot update has stale or changed identity"))
  (let ((cause (logical-slot-effective-invalidation-cause slot))
        (next (logical-slot-effective-invalidation-cause candidate))
        (confirmed (wal-replication-slot-confirmed-flush-lsn slot))
        (next-confirmed (wal-replication-slot-confirmed-flush-lsn candidate)))
    (unless (and (or (eq cause :none) (eq cause next))
                 (or (null confirmed)
                     (and next-confirmed (<= confirmed next-confirmed)))
                 (or (eq cause :none)
                     (and (eql confirmed next-confirmed)
                          (eql (wal-replication-slot-restart-lsn slot)
                               (wal-replication-slot-restart-lsn candidate))
                          (eql (wal-replication-slot-restart-timeline slot)
                               (wal-replication-slot-restart-timeline candidate))
                          (eql (wal-replication-slot-confirmed-timeline slot)
                               (wal-replication-slot-confirmed-timeline candidate)))))
      (error "logical slot update changes terminal cause or moves acknowledged progress backwards")))
  (values))

(defun logical-slot-save-update-under-lock (slot candidate)
  (logical-slot-check-update-under-lock slot candidate)
  (sb-sys:without-interrupts
    (let ((completed nil))
      (unwind-protect
           (progn
             (wal-save-replication-slot candidate)
             (setf (wal-replication-slot-confirmed-flush-lsn slot)
                   (wal-replication-slot-confirmed-flush-lsn candidate)
                   (wal-replication-slot-restart-lsn slot)
                   (wal-replication-slot-restart-lsn candidate)
                   (wal-replication-slot-confirmed-timeline slot)
                   (wal-replication-slot-confirmed-timeline candidate)
                   (wal-replication-slot-restart-timeline slot)
                   (wal-replication-slot-restart-timeline candidate)
                   (wal-replication-slot-pending-xids slot)
                   (wal-replication-slot-pending-xids candidate)
                   (wal-replication-slot-invalidation-cause slot)
                   (wal-replication-slot-invalidation-cause candidate)
                   (wal-replication-slot-wal-status slot)
                   (wal-replication-slot-wal-status candidate)
                   completed t))
        (unless completed
          (setf (wal-replication-slot-state-uncertain-p slot) t)))))
  slot)

(defun logical-slot-generation-token (generation)
  "Encode an unsigned 128-bit generation as 32 lowercase hex digits."
  (unless (typep generation '(unsigned-byte 128))
    (error "invalid logical slot generation"))
  (string-downcase (format nil "~32,'0X" generation)))

(defun logical-slot-parse-generation (token)
  "Parse a canonical 32-digit lowercase hexadecimal generation."
  (unless (and (stringp token) (= 32 (length token))
               (every (lambda (char) (find char "0123456789abcdef")) token))
    (error "noncanonical logical slot generation"))
  (parse-integer token :radix 16))

(defun logical-slot-select-restart-position (floor-lsn floor-timeline pending-positions)
  "Return the selected LSN, timeline and position role.
FLOOR-LSN is a prefix end; PENDING-POSITIONS are (LSN . timeline) record starts
with caller-verified owners. At an
equal LSN, a pending position wins, as at a fork. Reject conflicting selected
pending owners. This selection does not verify history."
  (unless (and (logical-slot-position-valid-p floor-lsn floor-timeline)
               (ignore-errors (list-length pending-positions)))
    (error "invalid logical slot restart candidates"))
  (let ((minimum floor-lsn) (owner floor-timeline) (pending-p nil))
    (dolist (position pending-positions)
      (unless (and (consp position)
                   (logical-slot-position-valid-p (car position) (cdr position)))
        (error "invalid pending logical slot restart position"))
      (setf minimum (min minimum (car position))))
    (dolist (position pending-positions)
      (when (= minimum (car position))
        (when (and pending-p (/= owner (cdr position)))
          (error "conflicting logical slot restart owners"))
        (setf owner (cdr position) pending-p t)))
    (values minimum owner (if pending-p :record-start :prefix-end))))

(defun logical-slot-effective-invalidation-cause (slot)
  "Project the legacy NIL cause from WAL status without inventing provenance.
Reserved maps to :NONE and lost to :WAL-REMOVED."
  (or (wal-replication-slot-invalidation-cause slot)
      (case (wal-replication-slot-wal-status slot)
        (:reserved :none) (:lost :wal-removed) (otherwise :invalid))))

(defun logical-slot-cause-token (cause)
  "Return the canonical token for a supported invalidation cause."
  (case cause
    (:none "none")
    (:wal-removed "wal_removed")
    (:wal-level-insufficient "wal_level_insufficient")
    (otherwise (error "invalid logical slot invalidation cause"))))

(defun logical-slot-parse-cause (token)
  "Parse a canonical invalidation cause token."
  (cond ((string= token "none") :none)
        ((string= token "wal_removed") :wal-removed)
        ((string= token "wal_level_insufficient") :wal-level-insufficient)
        (t (error "invalid logical slot invalidation cause"))))

(defun logical-slot-state-valid-p (slot)
  "Check logical slot field shape and ordering, not identity or history.
The WAL status mirrors the cause. Provenance requires an explicit cause, but
the cause can exist without provenance. At an equal boundary LSN, restart
may belong to a child timeline while confirmed belongs to its parent."
  (and
   (wal-replication-slot-p slot)
   (wal-logical-slot-p slot)
   (let* ((restart (wal-replication-slot-restart-lsn slot))
          (birth (wal-replication-slot-consistent-lsn slot))
          (confirmed (wal-replication-slot-confirmed-flush-lsn slot))
          (provenance (wal-replication-slot-logical-provenance slot))
          (restart-timeline (wal-replication-slot-restart-timeline slot))
          (confirmed-timeline (wal-replication-slot-confirmed-timeline slot))
          (cause (logical-slot-effective-invalidation-cause slot))
          (profile (wal-slot-profile slot)))
     (and (wal-valid-replication-slot-name-p (wal-replication-slot-name slot))
          (wal-valid-slot-plugin-state-p (wal-replication-slot-plugin slot)
                                         (wal-replication-slot-database-oid slot) confirmed)
          (or (null (wal-replication-slot-database-oid slot))
              (typep (wal-replication-slot-database-oid slot) '(integer 1 4294967295)))
          (typep restart '(unsigned-byte 64))
          (typep birth '(unsigned-byte 64))
          (if confirmed
              (and (typep confirmed '(unsigned-byte 64))
                   (<= birth confirmed) (<= restart confirmed))
              (<= restart birth))
          (member profile '(:text :binary-bytea))
          (or (eq profile :text)
              (and (wal-replication-slot-database-oid slot)
                   (string= (wal-replication-slot-plugin slot) "pg_lisp")))
          (member cause '(:none :wal-removed :wal-level-insufficient))
          (eq (wal-replication-slot-wal-status slot)
              (if (eq cause :none) :reserved :lost))
          (if provenance
              (and (wal-replication-slot-invalidation-cause slot)
                   (logical-slot-provenance-valid-p provenance birth)
                   (logical-slot-position-valid-p restart restart-timeline)
                   (if confirmed
                       (and (logical-slot-position-valid-p confirmed confirmed-timeline)
                            (<= (logical-slot-provenance-birth-timeline provenance) confirmed-timeline))
                       (null confirmed-timeline))
                   (or (= restart (or confirmed birth))
                       (<= restart-timeline
                           (or confirmed-timeline (logical-slot-provenance-birth-timeline provenance)))))
              (and (null restart-timeline) (null confirmed-timeline)))))))

(defun logical-slot-state-required-p (slot)
  "Use PGSLOT6 for provenance or a cause the older images cannot represent."
  (or (wal-replication-slot-logical-provenance slot)
      (eq :wal-level-insufficient (logical-slot-effective-invalidation-cause slot))))

(defun logical-slot-state-image-p (bytes)
  "Recognize the PGSLOT6 prefix; full validation belongs to the decoder."
  (and (typep bytes '(vector (unsigned-byte 8))) (>= (length bytes) 8)
       (loop for char across "PGSLOT6 " for i from 0
             always (= (char-code char) (aref bytes i)))))

(defun logical-slot-integer-token (value)
  "Encode an integer in decimal, or a dash for NIL."
  (if value (format nil "~D" value) "-"))

(defun logical-slot-state-provenance-tokens (slot)
  "Encode the seven provenance fields, or seven dashes when absent.
Known identity may have an unknown ON pair; confirmed timeline is absent when
confirmed LSN is absent."
  (let ((provenance (wal-replication-slot-logical-provenance slot)))
    (if provenance
        (list (logical-slot-integer-token (logical-slot-provenance-system-id provenance))
              (logical-slot-generation-token (logical-slot-provenance-generation provenance))
              (logical-slot-integer-token (logical-slot-provenance-birth-timeline provenance))
              (logical-slot-integer-token (logical-slot-provenance-on-start-lsn provenance))
              (logical-slot-integer-token (logical-slot-provenance-on-start-timeline provenance))
              (logical-slot-integer-token (wal-replication-slot-restart-timeline slot))
              (logical-slot-integer-token (wal-replication-slot-confirmed-timeline slot)))
        '("-" "-" "-" "-" "-" "-" "-"))))

(defun logical-slot-state-encode (slot)
  "Encode a canonical PGSLOT6 image with an explicit invalidation cause.
Its 17 tokens are magic, name, database, restart, birth, confirmed, plugin,
profile, system ID, generation, birth timeline, ON LSN, ON timeline, restart
timeline, confirmed timeline, cause and CRC. Consistent LSN is the birth LSN."
  (unless (and (logical-slot-state-valid-p slot)
               (wal-replication-slot-invalidation-cause slot))
    (error "invalid durable logical slot state"))
  (let* ((fields (append
                  (list "PGSLOT6" (wal-replication-slot-name slot)
                        (logical-slot-integer-token (wal-replication-slot-database-oid slot))
                        (logical-slot-integer-token (wal-replication-slot-restart-lsn slot))
                        (logical-slot-integer-token (wal-replication-slot-consistent-lsn slot))
                        (logical-slot-integer-token (wal-replication-slot-confirmed-flush-lsn slot))
                        (wal-replication-slot-plugin slot)
                        (if (eq :text (wal-slot-profile slot)) "text" "binary-bytea"))
                  (logical-slot-state-provenance-tokens slot)
                  (list (logical-slot-cause-token (logical-slot-effective-invalidation-cause slot)))))
         (prefix (format nil "~{~A~^ ~}" fields)))
    (string->bytes (format nil "~A ~8,'0X~%" prefix (wal-crc32 (string->bytes prefix))))))

(defun logical-slot-state-parse-integer (token &optional optional)
  "Parse decimal input, mapping an optional dash to NIL.
The caller checks range and canonical bytes."
  (if (and optional (string= token "-")) nil
      (progn
        (unless (and (plusp (length token))
                     (every (lambda (char) (find char "0123456789")) token))
          (error "invalid logical slot integer"))
        (parse-integer token :radix 10))))

(defun logical-slot-state-read-fields (bytes expected-name)
  "Read a bounded one-line PGSLOT6 frame, checking name and CRC.
The complete decoder checks canonical tokens and line termination by re-encoding."
  (unless (and (typep bytes '(vector (unsigned-byte 8))) (<= 1 (length bytes) 2048))
    (error "invalid logical slot image bytes"))
  (with-input-from-string (stream (bytes->string bytes))
    (let* ((line (read-line stream nil nil))
           (fields (and line (wal-split-fields line))))
      (unless (and (= 17 (length fields))
                   (every (lambda (field) (plusp (length field))) fields)
                   (string= (first fields) "PGSLOT6")
                   (stringp expected-name) (string= (second fields) expected-name)
                   (null (read-line stream nil nil)))
        (error "invalid logical slot image frame"))
      (let ((crc (nth 16 fields)) (prefix (format nil "~{~A~^ ~}" (butlast fields))))
        (unless (and (= 8 (length crc))
                     (every (lambda (char) (find char "0123456789ABCDEF")) crc)
                     (= (parse-integer crc :radix 16) (wal-crc32 (string->bytes prefix))))
          (error "invalid logical slot image checksum")))
      fields)))

(defun logical-slot-state-read-provenance (fields birth)
  "Read provenance, restart timeline and confirmed timeline from FIELDS.
Require all seven fields to be absent together when provenance is unknown."
  (if (string= (nth 8 fields) "-")
      (progn
        (unless (loop for i from 8 to 14 always (string= (nth i fields) "-"))
          (error "partial logical slot provenance"))
        (values nil nil nil))
      (values
       (make-logical-slot-provenance
        (logical-slot-state-parse-integer (nth 8 fields))
        (logical-slot-parse-generation (nth 9 fields))
        birth (logical-slot-state-parse-integer (nth 10 fields))
        (logical-slot-state-parse-integer (nth 11 fields) t)
        (logical-slot-state-parse-integer (nth 12 fields) t))
       (logical-slot-state-parse-integer (nth 13 fields))
       (logical-slot-state-parse-integer (nth 14 fields) t))))

(defun logical-slot-state-decode (bytes expected-name)
  "Decode a canonical PGSLOT6 image without granting runtime authority.
The detached image retains full unsigned 64-bit positions, including positions
ahead of local WAL. Admission requires separate identity and history checks."
  (let* ((fields (logical-slot-state-read-fields bytes expected-name))
         (birth (logical-slot-state-parse-integer (nth 4 fields)))
         (cause (logical-slot-parse-cause (nth 15 fields)))
         (profile (cond ((string= (nth 7 fields) "text") :text)
                        ((string= (nth 7 fields) "binary-bytea") :binary-bytea)
                        (t (error "invalid logical slot profile")))))
    (multiple-value-bind (provenance restart-timeline confirmed-timeline)
        (logical-slot-state-read-provenance fields birth)
      (let ((slot (make-wal-replication-slot
                   :name (second fields)
                   :database-oid (logical-slot-state-parse-integer (nth 2 fields) t)
                   :restart-lsn (logical-slot-state-parse-integer (nth 3 fields))
                   :consistent-lsn birth
                   :confirmed-flush-lsn (logical-slot-state-parse-integer (nth 5 fields) t)
                   :plugin (nth 6 fields) :logical-provenance provenance
                   :restart-timeline restart-timeline :confirmed-timeline confirmed-timeline
                   :invalidation-cause cause :wal-status (if (eq cause :none) :reserved :lost))))
        (wal-slot-set-initial-profile slot profile)
        (unless (equalp bytes (logical-slot-state-encode slot))
          (error "noncanonical logical slot image"))
        slot))))
