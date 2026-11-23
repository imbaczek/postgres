;;;; Versioned native checkpoint facts; the native WAL format is not C WAL.
(in-package #:pg)

(defconstant +wal-checkpoint-snapshot-size+ 88)

(defconstant +wal-checkpoint-resource-snapshot-size+ 136
  "PGCKPT02 payload size with a complete resource tuple and coordinates.")

(defconstant +wal-checkpoint-commit-ts-size+ 8
  "Bytes for the oldest and newest commit timestamp XIDs as two u32 values.")

(defun wal-checkpoint-commit-ts-fields-valid-p (facts)
  "Accept absent fields or one oldest and newest pair with both values zero.
Absent fields preserve unknown history.  Duplicate, partial, NIL and nonzero
fields are invalid.  Callers establish policy before supplying facts."
  (loop for key in facts by #'cddr
        count (eq key :oldest-commit-ts-xid) into oldest-count
        count (eq key :newest-commit-ts-xid) into newest-count
        finally (return
                  (or (zerop (+ oldest-count newest-count))
                      (and (= oldest-count newest-count 1)
                           (eql 0 (getf facts :oldest-commit-ts-xid))
                           (eql 0 (getf facts :newest-commit-ts-xid)))))))

(defun wal-copy-checkpoint-commit-ts-fields (facts)
  "Validate and copy the optional commit timestamp pair from FACTS."
  (unless (wal-checkpoint-commit-ts-fields-valid-p facts)
    (error "invalid checkpoint commit timestamp facts"))
  (when (getf facts :oldest-commit-ts-xid)
    (list :oldest-commit-ts-xid 0 :newest-commit-ts-xid 0)))

(defun wal-checkpoint-boolean-fact-valid-p (facts key)
  "Accept a well-formed property list with KEY absent or present once as NIL or T."
  (let ((length (and (listp facts) (list-length facts))))
    (and length (evenp length)
         (loop for candidate in facts by #'cddr
               count (eq candidate key) into count
               finally (return (or (zerop count)
                                   (and (= count 1)
                                        (member (getf facts key) '(nil t)))))))))

(defun wal-checkpoint-full-page-writes-fields-valid-p (facts)
  "Accept an absent fact or one Boolean full-page-writes fact.
Absence means unknown; present NIL means off and present T means on."
  (wal-checkpoint-boolean-fact-valid-p facts :full-page-writes))

(defun wal-copy-checkpoint-full-page-writes-fields (facts)
  "Validate and copy the optional fact, retaining a known NIL value."
  (unless (wal-checkpoint-full-page-writes-fields-valid-p facts)
    (error "invalid checkpoint full page writes fact"))
  (when (get-properties facts '(:full-page-writes))
    (list :full-page-writes (getf facts :full-page-writes))))

(defconstant +wal-checkpoint-logical-interval-size+ 16)

(defun wal-logical-on-interval-valid-p (enabled lsn timeline end owner)
  "Check the optional ON pair's shape and bounds, not its history or identity."
  (or (and (null lsn) (null timeline))
      (and (eq enabled t)
           (typep lsn '(integer 33 18446744073709551615))
           (typep timeline '(integer 1 4294967295))
           (<= lsn end) (<= timeline owner))))

(defun wal-checkpoint-logical-decoding-fields-valid-p (facts)
  (and (wal-checkpoint-boolean-fact-valid-p facts :logical-decoding-enabled)
       (let ((positions (loop for key in facts by #'cddr
                              count (eq key :logical-on-start-lsn)))
             (timelines (loop for key in facts by #'cddr
                              count (eq key :logical-on-start-timeline))))
         (or (and (zerop positions) (zerop timelines))
             (and (= positions timelines 1)
                  (getf facts :logical-on-start-lsn)
                  (getf facts :logical-on-start-timeline)
                  (wal-logical-on-interval-valid-p
                   (getf facts :logical-decoding-enabled)
                   (getf facts :logical-on-start-lsn)
                   (getf facts :logical-on-start-timeline)
                   #xffffffffffffffff #xffffffff))))))

(defun wal-copy-checkpoint-logical-decoding-fields (facts)
  (unless (wal-checkpoint-logical-decoding-fields-valid-p facts)
    (error "invalid checkpoint logical decoding fact"))
  (when (get-properties facts '(:logical-decoding-enabled))
    (append (list :logical-decoding-enabled (getf facts :logical-decoding-enabled))
            (when (getf facts :logical-on-start-lsn)
              (list :logical-on-start-lsn (getf facts :logical-on-start-lsn)
                    :logical-on-start-timeline (getf facts :logical-on-start-timeline))))))

(defun wal-checkpoint-snapshot-length-p (length)
  (member length '(88 96 104 112 136 144 152 160) :test #'=))

(declaim (ftype function wal-control-resource-fields-valid-p wal-control-resource-parameters
                wal-copy-control-resource-fields))

(defun wal-checkpoint-record-size (&optional facts)
  "Plan placement from the same frozen optional facts used by the encoder."
  (unless (and (wal-control-resource-fields-valid-p facts)
               (wal-checkpoint-commit-ts-fields-valid-p facts)
               (wal-checkpoint-full-page-writes-fields-valid-p facts)
               (wal-checkpoint-logical-decoding-fields-valid-p facts))
    (error "invalid checkpoint facts"))
  (+ +wal-header-size+
     (if (wal-control-resource-parameters facts)
         +wal-checkpoint-resource-snapshot-size+
         +wal-checkpoint-snapshot-size+)
     (if (getf facts :oldest-commit-ts-xid) +wal-checkpoint-commit-ts-size+ 0)
     (if (getf facts :logical-on-start-lsn) +wal-checkpoint-logical-interval-size+ 0)))

(defun wal-checkpoint-record-length-p (length)
  (or (= length +wal-header-size+)
      (wal-checkpoint-snapshot-length-p (- length +wal-header-size+))))

(defun wal-checkpoint-snapshot-valid-p (snapshot)
  "Check checkpoint coordinates and optional resource and status facts.
Full-page-writes and logical-decoding facts may
be absent, NIL or T: absence is unknown, NIL is off and T is on.
The zero commit timestamp pair represents disabled tracking; absence preserves
unknown history. Ancestry requires separate history validation."
  (let ((version (getf snapshot :version))
        (start (getf snapshot :start)) (end (getf snapshot :end))
        (redo (getf snapshot :redo)) (next (getf snapshot :next-xid))
        (reserved (getf snapshot :xid-reserved-through))
        (oid (getf snapshot :next-oid)) (active (getf snapshot :oldest-active-xid)))
    (and (eql version 2)
         (wal-control-resource-fields-valid-p snapshot)
         (wal-checkpoint-commit-ts-fields-valid-p snapshot)
         (wal-checkpoint-full-page-writes-fields-valid-p snapshot)
         (wal-checkpoint-logical-decoding-fields-valid-p snapshot)
         (every (lambda (value) (typep value '(unsigned-byte 64)))
                (list start end redo reserved))
         (typep next '(integer 3 18446744073709551616))
         (typep active '(integer 0 18446744073709551616))
         (= end (+ start (wal-checkpoint-record-size snapshot)))
         (= (floor start +wal-segment-size+) (floor (1- end) +wal-segment-size+))
         (= redo start) (<= next (1+ reserved)) (<= 2 reserved)
         (or (zerop active) (<= 3 active next))
         (or (null oid) (typep oid '(unsigned-byte 64)))
         (typep (getf snapshot :time) '(signed-byte 64))
         (typep (getf snapshot :timeline) '(integer 1 4294967295))
         (typep (getf snapshot :prev-timeline) '(integer 1 4294967295))
         (or (null (wal-control-resource-parameters snapshot))
             (and (<= (getf snapshot :resource-lsn) start)
                  (<= (getf snapshot :resource-timeline) (getf snapshot :timeline))))
         (wal-logical-on-interval-valid-p
          (getf snapshot :logical-decoding-enabled)
          (getf snapshot :logical-on-start-lsn) (getf snapshot :logical-on-start-timeline)
          start (getf snapshot :timeline))
         (<= (getf snapshot :prev-timeline) (getf snapshot :timeline)))))

(defun wal-encode-checkpoint-snapshot (snapshot)
  "Encode a validated checkpoint snapshot.
PGCKPT02 uses flags for one-past FullXIDs. Bit 3 marks full-page-writes
presence and bit 4 its value; bits 5 and 6 do the same for logical decoding.
These status bits add no payload bytes.
Resource facts add 48 bytes. Bit 7 adds the final 16-byte ON interval.
Flag bit 2 adds the eight-byte oldest and newest u32 commit timestamp pair
after the base payload and any resource extension. Currently, both values
must be zero. Absence preserves unknown history."
  (unless (wal-checkpoint-snapshot-valid-p snapshot) (error "invalid native checkpoint snapshot"))
  (let* ((bytes (make-array (- (wal-checkpoint-record-size snapshot) +wal-header-size+)
                            :element-type '(unsigned-byte 8) :initial-element 0))
         (next-exhausted (= (getf snapshot :next-xid) 18446744073709551616))
         (active-exhausted (= (getf snapshot :oldest-active-xid) 18446744073709551616)))
    (replace bytes #(80 71 67 75 80 84 48 50))
    (loop for key in '(:start :end :redo :next-xid :xid-reserved-through :next-oid)
          for offset from 8 by 8 for value = (or (getf snapshot key) 0)
          do (wal-set-u64 bytes offset (if (= value 18446744073709551616) 0 value)))
    (wal-set-i64 bytes 56 (getf snapshot :time))
    (wal-set-u32 bytes 64 (getf snapshot :timeline))
    (wal-set-u32 bytes 68 (getf snapshot :prev-timeline))
    (wal-set-u64 bytes 72 (if active-exhausted 0 (getf snapshot :oldest-active-xid)))
    (when (getf snapshot :next-oid) (wal-set-u32 bytes 80 1))
    (wal-set-u32 bytes 84 (logior (if next-exhausted 1 0) (if active-exhausted 2 0)
                                  (if (getf snapshot :oldest-commit-ts-xid) 4 0)
                                  (if (get-properties snapshot '(:full-page-writes)) 8 0)
                                  (if (getf snapshot :full-page-writes) 16 0)
                                  (if (get-properties snapshot '(:logical-decoding-enabled)) 32 0)
                                  (if (getf snapshot :logical-decoding-enabled) 64 0)
                                  (if (getf snapshot :logical-on-start-lsn) 128 0)))
    (when (getf snapshot :resource-parameters)
      (replace bytes (wal-encode-resource-fact (wal-copy-control-resource-fields snapshot)) :start1 88))
    (when (getf snapshot :oldest-commit-ts-xid)
      (let ((offset (- (length bytes) +wal-checkpoint-commit-ts-size+
                       (if (getf snapshot :logical-on-start-lsn) +wal-checkpoint-logical-interval-size+ 0))))
        (wal-set-u32 bytes offset (getf snapshot :oldest-commit-ts-xid))
        (wal-set-u32 bytes (+ offset 4) (getf snapshot :newest-commit-ts-xid))))
    (when (getf snapshot :logical-on-start-lsn)
      (let ((offset (- (length bytes) +wal-checkpoint-logical-interval-size+)))
        (wal-set-u64 bytes offset (getf snapshot :logical-on-start-lsn))
        (wal-set-u32 bytes (+ offset 8) (getf snapshot :logical-on-start-timeline))))
    bytes))

(defun wal-decode-checkpoint-snapshot (bytes)
  "Decode a checkpoint payload covered by WAL or control CRC.
PGCKPT02 flag bit 2 marks an eight-byte commit timestamp pair after the base
and any 48-byte resource extension. Bits 3 and 4 mark full-page-writes
presence and value; bits 5 and 6 mark logical decoding presence and value.
These four bits do not change payload length, and a value without presence is
invalid. Bit 7 adds a final 16-byte ON interval after the commit timestamp
pair, if present. Absent status is unknown;
present NIL is off and present T is on."
  (unless (wal-checkpoint-snapshot-length-p (length bytes))
    (error "invalid checkpoint snapshot length"))
  (let* ((flags (wal-u32 bytes 84))
         (commit-ts-p (logbitp 2 flags))
         (interval-p (logbitp 7 flags))
         (resource-end (- (length bytes) (if commit-ts-p +wal-checkpoint-commit-ts-size+ 0)
                          (if interval-p +wal-checkpoint-logical-interval-size+ 0))))
    (unless (and (equalp (subseq bytes 0 8) #(80 71 67 75 80 84 48 50))
                 (<= flags 255)
                 (or (not (logbitp 4 flags)) (logbitp 3 flags))
                 (or (not (logbitp 6 flags)) (logbitp 5 flags))
                 (or (not interval-p)
                     (and (logbitp 5 flags) (logbitp 6 flags)
                          (zerop (wal-u32 bytes (- (length bytes) 4)))))
                 (member resource-end
                         (list +wal-checkpoint-snapshot-size+ +wal-checkpoint-resource-snapshot-size+))
                 (member (wal-u32 bytes 80) '(0 1))
                 (or (= 1 (wal-u32 bytes 80)) (zerop (wal-u64 bytes 48)))
                 (or (not (logbitp 0 flags)) (zerop (wal-u64 bytes 32)))
                 (or (not (logbitp 1 flags)) (zerop (wal-u64 bytes 72))))
      (error "corrupt or unsupported native checkpoint snapshot"))
    (let ((snapshot
            (list :version 2 :start (wal-u64 bytes 8) :end (wal-u64 bytes 16)
                  :redo (wal-u64 bytes 24)
                  :next-xid (if (logbitp 0 flags) 18446744073709551616 (wal-u64 bytes 32))
                  :xid-reserved-through (wal-u64 bytes 40)
                  :next-oid (when (= 1 (wal-u32 bytes 80)) (wal-u64 bytes 48))
                  :time (wal-i64 bytes 56) :timeline (wal-u32 bytes 64)
                  :prev-timeline (wal-u32 bytes 68)
                  :oldest-active-xid (if (logbitp 1 flags) 18446744073709551616 (wal-u64 bytes 72)))))
      (when (= resource-end +wal-checkpoint-resource-snapshot-size+)
        (setf snapshot
              (append snapshot
                      (wal-decode-resource-fact
                       (subseq bytes +wal-checkpoint-snapshot-size+
                               +wal-checkpoint-resource-snapshot-size+)))))
      (when commit-ts-p
        (setf snapshot
              (append snapshot
                      (list :oldest-commit-ts-xid (wal-u32 bytes resource-end)
                            :newest-commit-ts-xid (wal-u32 bytes (+ resource-end 4))))))
      (when (logbitp 3 flags)
        (setf snapshot (append snapshot (list :full-page-writes (logbitp 4 flags)))))
      (when (logbitp 5 flags)
        (setf snapshot (append snapshot (list :logical-decoding-enabled (logbitp 6 flags)))))
      (when interval-p
        (let ((offset (- (length bytes) +wal-checkpoint-logical-interval-size+)))
          (setf snapshot (append snapshot
                                 (list :logical-on-start-lsn (wal-u64 bytes offset)
                                       :logical-on-start-timeline (wal-u32 bytes (+ offset 8)))))))
      (unless (wal-checkpoint-snapshot-valid-p snapshot)
        (error "invalid native checkpoint snapshot"))
      snapshot)))

(defun wal-checkpoint-record-snapshot (record)
  "Decode the WAL8 checkpoint snapshot and bind it to the record coordinates."
  (unless (and (= (wal-record-type record) +wal-record-checkpoint+)
               (= (wal-record-version record) +wal-version+)
               (zerop (wal-record-xid record)))
    (error "invalid native checkpoint record"))
  (let ((snapshot (wal-decode-checkpoint-snapshot (wal-record-payload record))))
    (unless (and (= (wal-record-start record) (getf snapshot :start))
                 (= (wal-record-end record) (getf snapshot :end)))
      (error "checkpoint snapshot differs from its WAL record"))
    snapshot))

(defun wal-read-checkpoint-record (start end)
  "Read the exact supported record length and validate the decoded snapshot."
  (unless (and (typep start '(unsigned-byte 64)) (typep end '(unsigned-byte 64))
               (wal-checkpoint-snapshot-length-p (- end start +wal-header-size+))
               (= (floor start +wal-segment-size+) (floor (1- end) +wal-segment-size+)))
    (error "invalid checkpoint record coordinates"))
  (let ((bytes (make-array (- end start) :element-type '(unsigned-byte 8)))
        (path (wal-read-segment-path (floor start +wal-segment-size+))))
    (flet ((read-exact (stream)
             (unless (and (file-position stream (mod start +wal-segment-size+))
                          (= (read-sequence bytes stream) (length bytes)))
               (error "truncated checkpoint record"))))
      (if *physical-image-context*
          (call-with-physical-image-input path #'read-exact :element-type '(unsigned-byte 8))
          (let ((stream (open-bounded-regular-file path +wal-segment-size+)))
            (unwind-protect (read-exact stream) (close stream)))))
    (let ((record (wal-decode-record bytes start)))
      (wal-checkpoint-record-snapshot record)
      record)))

(defun wal-checkpoint-record-start (&optional (end *wal-checkpoint-lsn*))
  "Return zero for END zero. Use the recorded start when the snapshot matches END.
Otherwise subtract the legacy checkpoint header size from END."
  (cond ((zerop end) 0)
        ((and *wal-checkpoint-snapshot* (= end (getf *wal-checkpoint-snapshot* :end)))
         (getf *wal-checkpoint-snapshot* :start))
        (t (- end +wal-header-size+))))

(defun wal-checkpoint-snapshot-hex (snapshot)
  "Encode a supported 88, 96, 104, 112, 136, 144, 152 or 160 byte snapshot."
  (with-output-to-string (stream)
    (loop for byte across (wal-encode-checkpoint-snapshot snapshot)
          do (format stream "~2,'0X" byte))))

(defun wal-checkpoint-snapshot-from-hex (text)
  "Decode and validate a supported checkpoint snapshot from control metadata."
  (unless (and (evenp (length text))
               (wal-checkpoint-snapshot-length-p (/ (length text) 2))
               (every (lambda (char) (digit-char-p char 16)) text))
    (error "invalid checkpoint control snapshot encoding"))
  (let ((bytes (make-array (/ (length text) 2) :element-type '(unsigned-byte 8))))
    (loop for index below (length bytes)
          do (setf (aref bytes index)
                   (parse-integer text :start (* index 2) :end (+ (* index 2) 2) :radix 16)))
    (wal-decode-checkpoint-snapshot bytes)))

(defparameter *wal-retained-reference-keys*
  '(:root-id :base-timeline :base :manifest-digest :end :end-digest))

(defun wal-retained-reference-valid-p (reference)
  "Check the six named fields of a retained archive reference."
  (and (listp reference) (= 12 (length reference))
       (let ((keys (loop for (key value) on reference by #'cddr collect key)))
         (and (= 6 (length (remove-duplicates keys)))
              (null (set-difference keys *wal-retained-reference-keys*))))
       (every (lambda (key) (typep (getf reference key) '(unsigned-byte 64)))
              '(:root-id :base :manifest-digest :end :end-digest))
       (typep (getf reference :base-timeline) '(integer 1 4294967295))
       (<= (getf reference :base) (getf reference :end))))

(defun wal-control-resource-parameters (metadata)
  "Reconstruct a checked read-only tuple from the canonical five-integer list.
An absent key means unknown; a present NIL is malformed."
  (let ((keys (loop for (key value) on metadata by #'cddr collect key)))
    (when (member :resource-parameters keys)
      (resource-parameters-from-values (getf metadata :resource-parameters)))))

(defun wal-control-resource-fields-valid-p (metadata)
  "Require either no resource fields or a valid tuple and both coordinates."
  (let* ((keys (loop for (key value) on metadata by #'cddr collect key))
         (present (intersection keys '(:resource-parameters :resource-lsn :resource-timeline))))
    (or (null present)
        (and (= 3 (length present))
             (handler-case-noninterrupt (wal-control-resource-parameters metadata) (condition nil))
             (typep (getf metadata :resource-lsn) '(integer 1 18446744073709551615))
             (typep (getf metadata :resource-timeline) '(integer 1 4294967295))))))

(defun wal-control-resource-position-valid-p (metadata durable timeline)
  "Check numeric bounds against durable LSN and insertion timeline only.
These bounds do not prove ancestry or application at a recovery seed."
  (or (null (getf metadata :resource-parameters))
      (and (<= (getf metadata :resource-lsn) durable)
           (<= (getf metadata :resource-timeline) timeline))))

(defun wal-copy-control-resource-fields (metadata)
  "Copy the tuple list so refreshed metadata cannot alias its input."
  (when (getf metadata :resource-parameters)
    (list :resource-parameters (copy-list (getf metadata :resource-parameters))
          :resource-lsn (getf metadata :resource-lsn)
          :resource-timeline (getf metadata :resource-timeline))))

(defun wal-decode-control-metadata (fields)
  "Decode lifecycle metadata and optional retained and resource extensions.
The resource extension uses tag R, encoded tuple, LSN and timeline. Store the
five integers as a list so EQUAL compares metadata without case-folding strings."
  (when (member (length fields) '(11 15 17 21))
    (let ((metadata
            (make-wal-control-metadata
             (handler-case-noninterrupt (parse-integer (eighth fields)) (condition nil))
             (handler-case-noninterrupt (parse-integer (ninth fields)) (condition nil))
             (handler-case-noninterrupt (parse-integer (tenth fields)) (condition nil)))))
      (when (member (length fields) '(17 21))
        (setf metadata
              (append metadata
                      (list :retained-reference
                            (loop for key in *wal-retained-reference-keys*
                                  for field in (subseq fields 10 16)
                                  append (list key (handler-case-noninterrupt (parse-integer field) (condition nil))))))))
      (when (member (length fields) '(15 21))
        (let ((offset (if (= 21 (length fields)) 16 10)))
          (unless (equal "R" (nth offset fields))
            (error "invalid resource control extension tag"))
          (setf metadata
                (append metadata
                        (list :resource-parameters (resource-parameters-values (wal-resource-parameters-from-hex (nth (1+ offset) fields)))
                              :resource-lsn (handler-case-noninterrupt (parse-integer (nth (+ offset 2) fields)) (condition nil))
                              :resource-timeline (handler-case-noninterrupt (parse-integer (nth (+ offset 3) fields)) (condition nil)))))))
      (unless (wal-control-metadata-valid-p metadata)
        (error "invalid primary control metadata"))
      metadata)))

(defun wal-control-metadata-valid-p (metadata)
  (and (listp metadata) (member (length metadata) '(6 8 10 12 14 16 18 20))
       (let ((keys (loop for (key value) on metadata by #'cddr collect key)))
         (and (= (/ (length metadata) 2) (length (remove-duplicates keys)))
              (null (set-difference '(:system-id :db-state :state-time) keys))
              (null (set-difference keys '(:system-id :db-state :state-time :retained-reference
                                           :resource-parameters :resource-lsn :resource-timeline
                                           :page-checksum-version :logical-slot-evidence-digest
                                           :checkpoint-clog-bytes)))
              (or (not (member :checkpoint-clog-bytes keys))
                  (typep (getf metadata :checkpoint-clog-bytes) '(integer 0 1073741824)))
              (or (not (member :page-checksum-version keys))
                  (eql 1 (getf metadata :page-checksum-version)))
              (or (not (member :logical-slot-evidence-digest keys))
                  (and (member :retained-reference keys)
                       (typep (getf metadata :logical-slot-evidence-digest) '(unsigned-byte 64))))
              (or (not (member :retained-reference keys))
                  (wal-retained-reference-valid-p (getf metadata :retained-reference)))))
       (typep (getf metadata :system-id) '(unsigned-byte 64))
       (typep (getf metadata :db-state) '(integer 0 6))
       (typep (getf metadata :state-time) '(signed-byte 64))
       (wal-control-resource-fields-valid-p metadata)))

(defun make-wal-control-metadata (system-id state &optional (time (- (get-universal-time) 2208988800)))
  "Build checked primary lifecycle metadata, using Unix seconds for TIME."
  (let ((metadata (list :system-id system-id :db-state state :state-time time)))
    (unless (wal-control-metadata-valid-p metadata) (error "invalid primary control metadata"))
    metadata))

(defun wal-refresh-control-metadata (metadata state &optional (time (- (get-universal-time) 2208988800)))
  "Change lifecycle state without losing existing durable metadata."
  (unless (wal-control-metadata-valid-p metadata)
    (error "invalid primary control metadata"))
  (append (make-wal-control-metadata (getf metadata :system-id) state time)
          (when (getf metadata :retained-reference)
            (list :retained-reference (copy-list (getf metadata :retained-reference))))
          (wal-copy-control-resource-fields metadata)
          (when (getf metadata :logical-slot-evidence-digest)
            (list :logical-slot-evidence-digest (getf metadata :logical-slot-evidence-digest)))
          (when (getf metadata :checkpoint-clog-bytes)
            (list :checkpoint-clog-bytes (getf metadata :checkpoint-clog-bytes)))
          (when (getf metadata :page-checksum-version)
            (list :page-checksum-version (getf metadata :page-checksum-version)))))

(defun wal-control-base-metadata (metadata)
  "Remove local-only control facts when deriving a backup or adoption image.
Return the copied metadata and whether any field was removed."
  (let* ((copy (copy-list metadata))
         (removed-reference (remf copy :retained-reference))
         (removed-logical-evidence (remf copy :logical-slot-evidence-digest))
         (removed-checksum (remf copy :page-checksum-version))
         (removed-clog-extent (remf copy :checkpoint-clog-bytes)))
    (values copy (or removed-reference removed-logical-evidence removed-checksum removed-clog-extent))))

(defun wal-encode-control-image (checkpoint durable snapshot
                                 &key (timeline 1) (namespace :timeline) metadata
                                   (checkpoint-timeline (if snapshot (getf snapshot :timeline) 1)))
  "Encode a CRC-bound control image with independent insertion/checkpoint TLIs.

PGWAL6 has eight fields without metadata and eleven with lifecycle metadata.
A retained reference adds six fields; resource metadata adds R, tuple hex, LSN
and timeline, giving 15 fields alone or 21 with a reference. Reference values
follow the fixed key order. Optional C1 adds one field for native checksum mode.
An optional X token records the locally fsynced CLOG byte extent. The CRC covers
all fields before the CRC itself."
  (unless (and (typep checkpoint '(unsigned-byte 64))
               (typep durable '(unsigned-byte 64)) (<= checkpoint durable)
               (typep timeline '(integer 1 4294967295))
               (typep checkpoint-timeline '(integer 1 4294967295))
               (<= checkpoint-timeline timeline)
               (member namespace '(:legacy :timeline))
               (or (null metadata)
                   (and (wal-control-metadata-valid-p metadata)
                        (wal-control-resource-position-valid-p metadata durable timeline)))
               (or (null snapshot)
                   (and (wal-checkpoint-snapshot-valid-p snapshot)
                        (= checkpoint (getf snapshot :end))
                        (= checkpoint-timeline (getf snapshot :timeline)))))
    (error "invalid WAL control checkpoint copy"))
  (let* ((prefix (format nil "PGWAL6 ~d ~d ~d ~d ~a ~a" checkpoint durable timeline
                         checkpoint-timeline (ecase namespace (:legacy "legacy") (:timeline "timeline"))
                         (if snapshot (wal-checkpoint-snapshot-hex snapshot) "-")))
         (prefix (if metadata
                     (format nil "~a ~d ~d ~d" prefix (getf metadata :system-id)
                             (getf metadata :db-state) (getf metadata :state-time))
                     prefix))
         (reference (getf metadata :retained-reference))
         (prefix (if reference
                     (format nil "~a~{ ~d~}" prefix
                             (mapcar (lambda (key) (getf reference key))
                                     *wal-retained-reference-keys*))
                     prefix))
         (parameters (wal-control-resource-parameters metadata))
         (prefix (if parameters
                     (format nil "~a R ~a ~d ~d" prefix (wal-resource-parameters-hex parameters)
                             (getf metadata :resource-lsn) (getf metadata :resource-timeline))
                     prefix))
         (prefix (if (getf metadata :logical-slot-evidence-digest)
                     (format nil "~a L ~d" prefix (getf metadata :logical-slot-evidence-digest))
                     prefix))
         (prefix (if (getf metadata :checkpoint-clog-bytes)
                     (format nil "~a X~d" prefix (getf metadata :checkpoint-clog-bytes))
                     prefix))
         (prefix (if (getf metadata :page-checksum-version)
                     (format nil "~a C1" prefix)
                     prefix))
         (crc (wal-crc32 (string->bytes prefix))))
    (string->bytes (format nil "~a ~8,'0X~%" prefix crc))))

(defun wal-decode-control-line (line)
  "Decode a CRC-bound control image, including the optional native C1 mode.
The control CRC does not verify relation pages. Resource facts may be absent."
  (let* ((fields (and line (wal-split-fields line)))
         (extent-tail (if (equal "C1" (first (last fields 2))) 3 2))
         (extent-token (first (last fields extent-tail)))
         (extent-p (and extent-token (plusp (length extent-token))
                        (char= #\X (char extent-token 0))))
         (shape-fields (if extent-p (append (butlast fields extent-tail) (rest (last fields extent-tail))) fields))
         (checksum-p (member (length shape-fields) '(12 16 18 20 22 24)))
         (base-fields (if checksum-p (append (butlast shape-fields 2) (last shape-fields)) shape-fields))
         (logical-evidence-p (member (length base-fields) '(19 23)))
         (metadata-fields (if logical-evidence-p
                              (append (butlast base-fields 3) (last base-fields))
                              base-fields))
         (checkpoint (handler-case-noninterrupt (parse-integer (second fields)) (condition nil)))
         (durable (handler-case-noninterrupt (parse-integer (third fields)) (condition nil)))
         (timeline (handler-case-noninterrupt (parse-integer (fourth fields)) (condition nil)))
         (checkpoint-timeline (handler-case-noninterrupt (parse-integer (fifth fields)) (condition nil)))
         (namespace (cond ((equal (sixth fields) "legacy") :legacy)
                          ((equal (sixth fields) "timeline") :timeline)))
         (crc (handler-case-noninterrupt (parse-integer (car (last fields)) :radix 16) (condition nil))))
    (unless (and (equal "PGWAL6" (first fields))
                 (member (length metadata-fields) '(8 11 15 17 21))
                 (or (not checksum-p) (equal "C1" (first (last shape-fields 2))))
                 (or (not logical-evidence-p) (equal "L" (first (last base-fields 3))))
                 (typep checkpoint '(unsigned-byte 64))
                 (typep durable '(unsigned-byte 64)) (<= checkpoint durable)
                 (typep timeline '(integer 1 4294967295))
                 (typep checkpoint-timeline '(integer 1 4294967295))
                 (<= checkpoint-timeline timeline) namespace
                 crc (= crc (wal-crc32 (string->bytes (format nil "~{~a~^ ~}" (butlast fields))))))
      (error "corrupt WAL control image"))
    (let* ((payload (seventh fields))
           (snapshot (unless (equal payload "-")
                       (wal-checkpoint-snapshot-from-hex payload))))
      (when (and snapshot (or (/= checkpoint (getf snapshot :end))
                              (/= checkpoint-timeline (getf snapshot :timeline))))
        (error "checkpoint control copy names another record or timeline"))
      (let ((metadata (wal-decode-control-metadata metadata-fields)))
        (when logical-evidence-p
          (setf metadata
                (append metadata
                        (list :logical-slot-evidence-digest
                              (handler-case-noninterrupt (parse-integer (first (last base-fields 2)))
                                                         (condition nil))))))
        (when extent-p
          (unless metadata (error "transaction status extent requires primary control metadata"))
          (setf metadata (append metadata
                                 (list :checkpoint-clog-bytes (parse-integer extent-token :start 1)))))
        (when checksum-p
          (setf metadata (append metadata (list :page-checksum-version 1))))
        (unless (or (null metadata) (wal-control-metadata-valid-p metadata))
          (error "invalid primary control metadata"))
        (unless (wal-control-resource-position-valid-p metadata durable timeline)
          (error "resource facts exceed durable control coordinates"))
        (values checkpoint durable snapshot timeline checkpoint-timeline namespace metadata)))))

(declaim (ftype function physical-promotion-check-child-control))

(defun wal-require-supported-timelines (insertion checkpoint)
  "Accept children only through verified child checkpoint control before mutation.

A retained primary reference also requires validation against child history and
its archive before mutation."

  (unless (and (eql insertion 1) (eql checkpoint 1)
               (null (getf (nth-value 6 (wal-read-control-fields)) :retained-reference)))
    (handler-case (physical-promotion-check-child-control insertion checkpoint)
      (error (condition)
        (error "native timeline-keyed WAL is not yet supported without a verified child checkpoint: ~a"
               condition)))))

(declaim (special *xid-lock* *next-xid* *xid-batch-end* *active-xids* *catalog*))

(declaim (notinline catalog-next-oid))

(defun wal-capture-checkpoint-snapshot (start &optional resource-facts)
  "Called with writer and WAL locks, sample counters under XID lock, as C does.
Copy the caller's frozen resource, commit timestamp, full-page-writes and
logical-decoding facts into the snapshot. Omitted facts remain unknown; a
present NIL Boolean is off and T is on. The caller establishes policy and
plans the record size before choosing START."
  (sb-thread:with-mutex (*xid-lock*)
    (let* ((next *next-xid*)
           (active (reduce #'min *active-xids* :initial-value next))
           (snapshot (append (list :version 2 :start start :end (+ start (wal-checkpoint-record-size resource-facts))
                                   ;; Native checkpoints synchronously install every preceding page.
                                   :redo start :next-xid next :xid-reserved-through (1- *xid-batch-end*)
                                   :next-oid (when *catalog* (catalog-next-oid *catalog*))
                                   :time (- (get-universal-time) 2208988800)
                                   :timeline *wal-insert-timeline* :prev-timeline *wal-prev-timeline* :oldest-active-xid active)
                             (wal-copy-control-resource-fields resource-facts)
                             (wal-copy-checkpoint-commit-ts-fields resource-facts)
                             (wal-copy-checkpoint-full-page-writes-fields resource-facts)
                             (wal-copy-checkpoint-logical-decoding-fields resource-facts))))
      (unless (wal-checkpoint-snapshot-valid-p snapshot)
        (error "source allocation state cannot be represented in checkpoint"))
      snapshot)))
