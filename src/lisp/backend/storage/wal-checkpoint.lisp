;;;; Versioned native checkpoint facts; the native WAL format is not C WAL.
(in-package #:pg)

(defconstant +wal-checkpoint-snapshot-size+ 88)

(defun wal-checkpoint-record-size ()
  (+ +wal-header-size+ +wal-checkpoint-snapshot-size+))

(defun wal-checkpoint-snapshot-valid-p (snapshot)
  (let ((version (getf snapshot :version))
        (start (getf snapshot :start)) (end (getf snapshot :end))
        (redo (getf snapshot :redo)) (next (getf snapshot :next-xid))
        (reserved (getf snapshot :xid-reserved-through))
        (oid (getf snapshot :next-oid)) (active (getf snapshot :oldest-active-xid)))
    (and (member version '(1 2))
         (every (lambda (value) (typep value '(unsigned-byte 64)))
                (list start end redo reserved))
         (typep next (if (= version 1) '(integer 3 18446744073709551615)
                         '(integer 3 18446744073709551616)))
         (typep active (if (= version 1) '(unsigned-byte 64)
                           '(integer 0 18446744073709551616)))
         (= end (+ start (wal-checkpoint-record-size)))
         (= (floor start +wal-segment-size+) (floor (1- end) +wal-segment-size+))
         (= redo start) (<= next (1+ reserved)) (<= 2 reserved)
         (or (zerop active) (<= 3 active next))
         (or (null oid) (typep oid '(unsigned-byte 64)))
         (typep (getf snapshot :time) '(signed-byte 64))
         (typep (getf snapshot :timeline) '(integer 1 4294967295))
         (typep (getf snapshot :prev-timeline) '(integer 1 4294967295))
         (<= (getf snapshot :prev-timeline) (getf snapshot :timeline)))))

(defun wal-encode-checkpoint-snapshot (snapshot)
  "V2 uses explicit flags for one-past FullXIDs, without truncating the sentinel."
  (unless (wal-checkpoint-snapshot-valid-p snapshot) (error "invalid native checkpoint snapshot"))
  (let* ((bytes (make-array +wal-checkpoint-snapshot-size+
                            :element-type '(unsigned-byte 8) :initial-element 0))
         (next-exhausted (= (getf snapshot :next-xid) 18446744073709551616))
         (active-exhausted (= (getf snapshot :oldest-active-xid) 18446744073709551616)))
    (replace bytes (if (= 1 (getf snapshot :version))
                       #(80 71 67 75 80 84 48 49) #(80 71 67 75 80 84 48 50)))
    (loop for key in '(:start :end :redo :next-xid :xid-reserved-through :next-oid)
          for offset from 8 by 8 for value = (or (getf snapshot key) 0)
          do (wal-set-u64 bytes offset (if (= value 18446744073709551616) 0 value)))
    (wal-set-i64 bytes 56 (getf snapshot :time))
    (wal-set-u32 bytes 64 (getf snapshot :timeline))
    (wal-set-u32 bytes 68 (getf snapshot :prev-timeline))
    (wal-set-u64 bytes 72 (if active-exhausted 0 (getf snapshot :oldest-active-xid)))
    (when (getf snapshot :next-oid) (wal-set-u32 bytes 80 1))
    (wal-set-u32 bytes 84 (logior (if next-exhausted 1 0) (if active-exhausted 2 0)))
    bytes))

(defun wal-decode-checkpoint-snapshot (bytes)
  "Containing WAL/control supplies CRC; reject ambiguous flags and hidden values."
  (unless (= (length bytes) +wal-checkpoint-snapshot-size+)
    (error "invalid checkpoint snapshot length"))
  (let* ((version (cond ((equalp (subseq bytes 0 8) #(80 71 67 75 80 84 48 49)) 1)
                        ((equalp (subseq bytes 0 8) #(80 71 67 75 80 84 48 50)) 2)))
         (flags (wal-u32 bytes 84)))
    (unless (and version (if (= version 1) (zerop flags) (<= 0 flags 3))
                 (member (wal-u32 bytes 80) '(0 1))
                 (or (= 1 (wal-u32 bytes 80)) (zerop (wal-u64 bytes 48)))
                 (or (not (logbitp 0 flags)) (zerop (wal-u64 bytes 32)))
                 (or (not (logbitp 1 flags)) (zerop (wal-u64 bytes 72))))
      (error "corrupt or unsupported native checkpoint snapshot"))
    (let ((snapshot
            (list :version version :start (wal-u64 bytes 8) :end (wal-u64 bytes 16)
                  :redo (wal-u64 bytes 24)
                  :next-xid (if (logbitp 0 flags) 18446744073709551616 (wal-u64 bytes 32))
                  :xid-reserved-through (wal-u64 bytes 40)
                  :next-oid (when (= 1 (wal-u32 bytes 80)) (wal-u64 bytes 48))
                  :time (wal-i64 bytes 56) :timeline (wal-u32 bytes 64)
                  :prev-timeline (wal-u32 bytes 68)
                  :oldest-active-xid (if (logbitp 1 flags) 18446744073709551616 (wal-u64 bytes 72)))))
      (unless (wal-checkpoint-snapshot-valid-p snapshot) (error "invalid native checkpoint snapshot"))
      snapshot)))

(defun wal-checkpoint-record-snapshot (record)
  "Validate a checkpoint terminal; legacy empty payloads have unknown metadata."
  (unless (and (= (wal-record-type record) +wal-record-checkpoint+)
               (zerop (wal-record-xid record)))
    (error "invalid native checkpoint record"))
  (if (zerop (length (wal-record-payload record)))
      (progn
        (unless (and (member (wal-record-version record) '(2 3 4 5))
                     (= (wal-record-end record) (+ (wal-record-start record) +wal-header-size+)))
          (error "invalid legacy checkpoint record boundary"))
        nil)
      (let ((snapshot (wal-decode-checkpoint-snapshot (wal-record-payload record))))
        (unless (and (>= (wal-record-version record) (if (= 2 (getf snapshot :version)) 7 6))
                     (= (wal-record-start record) (getf snapshot :start))
                     (= (wal-record-end record) (getf snapshot :end)))
          (error "checkpoint snapshot differs from its WAL record"))
        snapshot)))

(defun wal-read-checkpoint-record (start end)
  (unless (and (typep start '(unsigned-byte 64)) (typep end '(unsigned-byte 64))
               (member (- end start) (list +wal-header-size+ (wal-checkpoint-record-size)))
               (= (floor start +wal-segment-size+) (floor (1- end) +wal-segment-size+)))
    (error "invalid checkpoint record coordinates"))
  (let ((bytes (make-array (- end start) :element-type '(unsigned-byte 8))))
    (with-open-file (stream (wal-segment-path (floor start +wal-segment-size+))
                            :element-type '(unsigned-byte 8))
      (file-position stream (mod start +wal-segment-size+))
      (unless (= (read-sequence bytes stream) (length bytes))
        (error "truncated checkpoint record")))
    (let ((record (wal-decode-record bytes start)))
      (wal-checkpoint-record-snapshot record)
      record)))

(defun wal-checkpoint-record-start (&optional (end *wal-checkpoint-lsn*))
  (cond ((zerop end) 0)
        ((and *wal-checkpoint-snapshot* (= end (getf *wal-checkpoint-snapshot* :end)))
         (getf *wal-checkpoint-snapshot* :start))
        (t (- end +wal-header-size+))))

(defun wal-checkpoint-snapshot-hex (snapshot)
  (with-output-to-string (stream)
    (loop for byte across (wal-encode-checkpoint-snapshot snapshot)
          do (format stream "~2,'0X" byte))))

(defun wal-checkpoint-snapshot-from-hex (text)
  (unless (and (= (length text) (* 2 +wal-checkpoint-snapshot-size+))
               (every (lambda (char) (digit-char-p char 16)) text))
    (error "invalid checkpoint control snapshot encoding"))
  (let ((bytes (make-array +wal-checkpoint-snapshot-size+ :element-type '(unsigned-byte 8))))
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
             (ignore-errors (wal-control-resource-parameters metadata))
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
             (ignore-errors (parse-integer (eighth fields)))
             (ignore-errors (parse-integer (ninth fields)))
             (ignore-errors (parse-integer (tenth fields))))))
      (when (member (length fields) '(17 21))
        (setf metadata
              (append metadata
                      (list :retained-reference
                            (loop for key in *wal-retained-reference-keys*
                                  for field in (subseq fields 10 16)
                                  append (list key (ignore-errors (parse-integer field))))))))
      (when (member (length fields) '(15 21))
        (let ((offset (if (= 21 (length fields)) 16 10)))
          (unless (equal "R" (nth offset fields))
            (error "invalid resource control extension tag"))
          (setf metadata
                (append metadata
                        (list :resource-parameters (resource-parameters-values (wal-resource-parameters-from-hex (nth (1+ offset) fields)))
                              :resource-lsn (ignore-errors (parse-integer (nth (+ offset 2) fields)))
                              :resource-timeline (ignore-errors (parse-integer (nth (+ offset 3) fields))))))))
      (unless (wal-control-metadata-valid-p metadata)
        (error "invalid primary control metadata"))
      metadata)))

(defun wal-control-metadata-valid-p (metadata)
  (and (listp metadata) (member (length metadata) '(6 8 12 14))
       (let ((keys (loop for (key value) on metadata by #'cddr collect key)))
         (and (= (/ (length metadata) 2) (length (remove-duplicates keys)))
              (null (set-difference '(:system-id :db-state :state-time) keys))
              (null (set-difference keys '(:system-id :db-state :state-time :retained-reference
                                           :resource-parameters :resource-lsn :resource-timeline)))
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
          (wal-copy-control-resource-fields metadata)))

(defun wal-encode-control-image (checkpoint durable snapshot
                                 &key (timeline 1) (namespace :legacy) metadata
                                   (checkpoint-timeline (if snapshot (getf snapshot :timeline) 1)))
  "Encode a CRC-bound control image with independent insertion/checkpoint TLIs.

PGWAL6 has eight fields without metadata and eleven with lifecycle metadata.
A retained reference adds six fields; resource metadata adds R, tuple hex, LSN
and timeline, giving 15 fields alone or 21 with a reference. Reference values
follow the fixed key order. The CRC covers every supplied field."
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
         (crc (wal-crc32 (string->bytes prefix))))
    (string->bytes (format nil "~a ~8,'0X~%" prefix crc))))

(defun wal-decode-control-line (line)
  "Decode a CRC-bound control image, retaining unknown legacy resource facts."
  (let* ((fields (and line (wal-split-fields line)))
         (version (first fields))
         (v6 (equal version "PGWAL6"))
         (identities (member version '("PGWAL5" "PGWAL6") :test #'equal))
         (v4 (equal version "PGWAL4"))
         (checkpoint (ignore-errors (parse-integer (second fields))))
         (durable (ignore-errors (parse-integer (third fields))))
         (timeline (if identities (ignore-errors (parse-integer (fourth fields))) 1))
         (checkpoint-timeline (if identities (ignore-errors (parse-integer (fifth fields))) 1))
         (namespace (if v6 (cond ((equal (sixth fields) "legacy") :legacy)
                                 ((equal (sixth fields) "timeline") :timeline))
                        :legacy))
         (crc (ignore-errors (parse-integer (car (last fields)) :radix 16))))
    (unless (and (cond (v6 (member (length fields) '(8 11 15 17 21)))
                       (identities (= (length fields) 7))
                       (v4 (= (length fields) 5))
                       (t (and (= (length fields) 4)
                               (member version '("PGWAL2" "PGWAL3") :test #'equal))))
                 (typep checkpoint '(unsigned-byte 64))
                 (typep durable '(unsigned-byte 64)) (<= checkpoint durable)
                 (typep timeline '(integer 1 4294967295))
                 (typep checkpoint-timeline '(integer 1 4294967295))
                 (<= checkpoint-timeline timeline) namespace
                 crc (= crc (wal-crc32 (string->bytes (format nil "~{~a~^ ~}" (butlast fields))))))
      (error "corrupt WAL control image"))
    (let* ((payload (if v6 (seventh fields) (sixth fields)))
           (snapshot
             (cond ((and identities (not (equal payload "-")))
                    (wal-checkpoint-snapshot-from-hex payload))
                   (v4 (wal-checkpoint-snapshot-from-hex (fourth fields))))))
      (when (and snapshot (or (/= checkpoint (getf snapshot :end))
                              (/= checkpoint-timeline (getf snapshot :timeline))))
        (error "checkpoint control copy names another record or timeline"))
      (let ((metadata (when v6 (wal-decode-control-metadata fields))))
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

(defun wal-capture-checkpoint-snapshot (start)
  "Called with writer and WAL locks; sample counters under XID lock, as C does."
  (sb-thread:with-mutex (*xid-lock*)
    (let* ((next *next-xid*)
           (active (reduce #'min *active-xids* :initial-value next))
           (snapshot (list :version 2 :start start :end (+ start (wal-checkpoint-record-size))
                           ;; Native checkpoints synchronously install every preceding page.
                           :redo start :next-xid next :xid-reserved-through (1- *xid-batch-end*)
                           :next-oid (when *catalog* (catalog-next-oid *catalog*))
                           :time (- (get-universal-time) 2208988800)
                           :timeline *wal-insert-timeline* :prev-timeline *wal-prev-timeline* :oldest-active-xid active)))
      (unless (wal-checkpoint-snapshot-valid-p snapshot)
        (error "source allocation state cannot be represented in checkpoint"))
      snapshot)))
