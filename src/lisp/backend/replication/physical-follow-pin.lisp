(in-package #:pg)

(defstruct (physical-follow-pin (:include physical-source-pin))
  "A source pin retaining WAL for a proposed follow anchor.

The included source-pin fields retain their existing layout. READY grants
eligibility for a future follow path; bootstrap START does not accept this pin."
  anchor (stage :retaining))

(defconstant +physical-follow-pin-maximum-bytes+ (* 16 1024 1024))
(defparameter *physical-follow-pin-magic* "PGPHYSFOLLOW1")
(defparameter *physical-follow-anchor-keys*
  '(:version :system-id :base-timeline :base :manifest-digest :source-timeline
    :receive-timeline :receive-lsn :receive-digest :overlap-start :overlap-digest
    :history-text :eor-octets))
(defparameter *physical-follow-pin-keys*
  '(:version :mode :name :system-id :timeline :manifest-digest :restart :consistent
    :confirmed :digest :lost :stage :anchor))

(defun physical-follow-pin-image-limit ()
  "Return the maximum encoded follow-pin size."
  +physical-follow-pin-maximum-bytes+)

(defun physical-follow-check-plist (value keys)
  "Require exactly one occurrence of each expected field."
  (unless (and (listp value) (= (length value) (* 2 (length keys)))
               (every (lambda (key)
                        (= 1 (loop for name in value by #'cddr count (eq key name))))
                      keys))
    (error "invalid physical follow pin fields")))

(defun physical-follow-request-copy (request)
  "Check and copy the shape and bounds of receiver coordinates.
This does not prove their contents or authenticate the source; it needs no
source history or WAL."
  (let ((keys (remove-if (lambda (key) (member key '(:history-text :eor-octets)))
                         *physical-follow-anchor-keys*)))
    (physical-follow-check-plist request keys)
    (unless (and (eql 1 (getf request :version))
                 (every (lambda (key) (typep (getf request key) '(unsigned-byte 64)))
                        '(:system-id :base :manifest-digest :receive-lsn :receive-digest
                          :overlap-start :overlap-digest))
                 (every (lambda (key) (typep (getf request key) '(integer 1 4294967295)))
                        '(:base-timeline :source-timeline :receive-timeline)))
      (wal-invalid-input "invalid receiver follow coordinates"))
    (loop for key in keys append (list key (getf request key)))))

(defun physical-follow-bootstrap-request-copy (request)
  (let ((keys '(:version :system-id :source-timeline :base :manifest-digest)))
    (physical-follow-check-plist request keys)
    (unless (and (eql 1 (getf request :version))
                 (typep (getf request :source-timeline) '(integer 1 4294967295))
                 (every (lambda (key) (typep (getf request key) '(unsigned-byte 64)))
                        '(:system-id :base :manifest-digest)))
      (error "invalid native bootstrap retirement identity"))
    (loop for key in keys append (list key (getf request key)))))

(defun physical-follow-anchor-check (request)
  "Validate anchor fields, ancestry and the proposed end-of-recovery record.

Return the parsed history, fork and record. This checks the request, not its
agreement with retained source WAL. Its base and manifest describe the receiver,
which may differ from the source's retained archive base and manifest."
  (physical-follow-check-plist request *physical-follow-anchor-keys*)
  (let ((source (getf request :source-timeline))
        (parent (getf request :receive-timeline))
        (base-timeline (getf request :base-timeline))
        (base (getf request :base)) (cursor (getf request :receive-lsn))
        (start (getf request :overlap-start)) (octets (getf request :eor-octets)))
    (unless (and (eql 1 (getf request :version))
                 (typep source '(integer 2 4294967295))
                 (typep parent '(integer 1 4294967295))
                 (typep base-timeline '(integer 1 4294967295))
                 (typep base '(unsigned-byte 64))
                 (typep cursor '(integer 1 18446744073709551615))
                 (typep start '(unsigned-byte 64)) (<= base cursor)
                 (= start (* (floor (1- cursor) +wal-segment-size+) +wal-segment-size+))
                 (every (lambda (key) (typep (getf request key) '(unsigned-byte 64)))
                        '(:system-id :manifest-digest :receive-digest :overlap-digest))
                 (stringp (getf request :history-text))
                 (listp octets) (= (length octets) +wal-end-of-recovery-record-size+)
                 (every (lambda (byte) (typep byte '(unsigned-byte 8))) octets))
      (error "invalid physical follow anchor request"))
    (let* ((history (timeline-check-lineage
                     (timeline-parse-history source (getf request :history-text))))
           (position (position parent history :key #'timeline-history-entry-timeline))
           (entry (and position (nth position history)))
           (next (and position (plusp position) (nth (1- position) history)))
           (fork (and entry (timeline-history-entry-end entry))))
      (unless (and next fork (<= cursor fork)
                   (timeline-prefix-boundary-p parent cursor history)
                   (timeline-prefix-boundary-p base-timeline base history))
        (error "physical follow anchor is outside source ancestry"))
      (let* ((raw (coerce octets '(vector (unsigned-byte 8))))
             (record (wal-decode-record raw (wal-record-start-after fork (length raw)))))
        (unless (= (timeline-history-entry-timeline next)
                   (timeline-check-end-of-recovery record parent history))
          (error "physical follow anchor has foreign end-of-recovery WAL"))
        (values history fork record)))))

(defun physical-follow-anchor-copy (request)
  "Validate REQUEST and copy its mutable anchor values for pin ownership."
  (physical-follow-anchor-check request)
  (loop for key in *physical-follow-anchor-keys*
        for value = (getf request key)
        append (list key (typecase value
                           (string (copy-seq value))
                           (cons (copy-list value))
                           (t value)))))

(defun physical-follow-anchor-digest (anchor)
  "Return the canonical immutable anchor identity checksum.
Apply FNV-1a 64 with seed #xCBF29CE484222325 to the UTF-8 bytes of
physical-bootstrap-manifest-text of physical-follow-anchor-copy of ANCHOR.
The full history and EOR are included; mutable stage, progress and lost state
are not. This checksum identifies an anchor; it is not authentication."
  (physical-store-update-digest
   #xCBF29CE484222325
   (string->bytes (physical-bootstrap-manifest-text (physical-follow-anchor-copy anchor)))))

(defun physical-follow-pin-ready-p (pin)
  "Return whether PIN reached the ready stage; retention may since have been lost."
  (and (physical-follow-pin-p pin) (eq :ready (physical-follow-pin-stage pin))))

(defun physical-follow-pin-check-fields (pin)
  "Check that PIN's stage and progress agree with its anchor."
  (unless (physical-follow-pin-p pin) (error "not a physical follow pin"))
  (let* ((request (physical-follow-pin-anchor pin))
         (cursor (getf request :receive-lsn))
         (confirmed (physical-source-pin-confirmed-lsn pin))
         (digest (physical-source-pin-prefix-digest pin)))
    (physical-follow-anchor-check request)
    (unless (and (wal-valid-replication-slot-name-p (physical-source-pin-name pin))
                 (eql (physical-source-pin-system-id pin) (getf request :system-id))
                 (eql (physical-source-pin-timeline pin) (getf request :source-timeline))
                 (eql (physical-source-pin-manifest-digest pin) (getf request :manifest-digest))
                 (eql (physical-source-pin-consistent-lsn pin) cursor)
                 (typep digest '(unsigned-byte 64))
                 (member (physical-source-pin-lost-p pin) '(nil t))
                 (case (physical-follow-pin-stage pin)
                   (:retaining
                    (and (null confirmed)
                         (eql (physical-source-pin-restart-lsn pin) (getf request :overlap-start))
                         (= digest (getf request :receive-digest))))
                   (:ready
                    (and (typep confirmed '(unsigned-byte 64)) (<= cursor confirmed)
                         (eql (physical-source-pin-restart-lsn pin) confirmed)
                         (or (> confirmed cursor) (= digest (getf request :receive-digest)))))))
      (error "physical follow pin phase or progress contradicts its anchor")))
  pin)

(defun physical-follow-pin-payload (pin)
  "Build a validated payload with a copy of the anchor."
  (physical-follow-pin-check-fields pin)
  (list :version 1 :mode :follow-anchor :name (physical-source-pin-name pin)
        :system-id (physical-source-pin-system-id pin) :timeline (physical-source-pin-timeline pin)
        :manifest-digest (physical-source-pin-manifest-digest pin)
        :restart (physical-source-pin-restart-lsn pin) :consistent (physical-source-pin-consistent-lsn pin)
        :confirmed (physical-source-pin-confirmed-lsn pin) :digest (physical-source-pin-prefix-digest pin)
        :lost (physical-source-pin-lost-p pin) :stage (physical-follow-pin-stage pin)
        :anchor (physical-follow-anchor-copy (physical-follow-pin-anchor pin))))

(defun physical-follow-pin-image-p (bytes)
  "Recognize a follow-pin envelope before decoding it."
  (let ((magic (string->bytes *physical-follow-pin-magic*)))
    (and (> (length bytes) (length magic)) (= 32 (aref bytes (length magic)))
         (equalp magic (subseq bytes 0 (length magic))))))

(defun physical-follow-pin-encode (pin)
  "Encode PIN with a checksum and enforce the reopen size bound."
  (let* ((payload (physical-follow-pin-payload pin))
         (body (string->bytes (physical-bootstrap-manifest-text payload)))
         (header (string->bytes (format nil "~a ~d ~8,'0X~%"
                                        *physical-follow-pin-magic* (length body) (wal-crc32 body)))))
    (unless (<= (+ (length header) (length body)) +physical-follow-pin-maximum-bytes+)
      (error "physical follow pin exceeds its reopen bound"))
    (concatenate '(vector (unsigned-byte 8)) header body)))

(defun physical-follow-pin-read-payload (bytes)
  "Check the bounded envelope and checksum before reading its payload."
  (unless (and (typep bytes '(vector (unsigned-byte 8)))
               (<= (length bytes) +physical-follow-pin-maximum-bytes+)
               (physical-follow-pin-image-p bytes))
    (error "invalid physical follow pin envelope"))
  (let ((newline (position 10 bytes :end (min (length bytes) 80))))
    (unless newline (error "physical follow pin header is too long"))
    (let* ((fields (wal-split-fields (bytes->string (subseq bytes 0 newline))))
           (size (ignore-errors (parse-integer (second fields))))
           (crc (ignore-errors (parse-integer (third fields) :radix 16)))
           (body (subseq bytes (1+ newline))))
      (unless (and (= 3 (length fields)) (eql size (length body))
                   (typep crc '(unsigned-byte 32)) (= crc (wal-crc32 body)))
        (error "physical follow pin length or checksum mismatch"))
      (recovery-parse-manifest-text (bytes->string body)))))

(defun physical-follow-pin-decode (bytes)
  "Decode a follow pin and validate its anchor and progress."
  (let ((payload (physical-follow-pin-read-payload bytes)))
    (physical-follow-check-plist payload *physical-follow-pin-keys*)
    (unless (and (eql 1 (getf payload :version)) (eq :follow-anchor (getf payload :mode)))
      (error "unsupported physical follow pin format"))
    (physical-follow-pin-check-fields
     (make-physical-follow-pin
      :name (getf payload :name) :system-id (getf payload :system-id)
      :timeline (getf payload :timeline) :manifest-digest (getf payload :manifest-digest)
      :restart-lsn (getf payload :restart) :consistent-lsn (getf payload :consistent)
      :confirmed-lsn (getf payload :confirmed) :prefix-digest (getf payload :digest)
      :lost-p (getf payload :lost) :stage (getf payload :stage)
      :anchor (physical-follow-anchor-copy (getf payload :anchor))))))

(defun physical-follow-pin-history (pin)
  "Parse and validate the source history retained in PIN."
  (timeline-check-lineage
   (timeline-parse-history (physical-source-pin-timeline pin)
                           (getf (physical-follow-pin-anchor pin) :history-text))))

(defun physical-follow-pin-check-source (pin)
  "Check PIN against the current source identity, timeline and durable WAL."
  (unless (and (= (physical-source-pin-system-id pin) (catalog-system-identifier))
               (= (physical-source-pin-timeline pin) *wal-insert-timeline*)
               (eq *wal-segment-namespace* :timeline)
               (<= (or (physical-source-pin-confirmed-lsn pin)
                       (physical-source-pin-consistent-lsn pin))
                   *wal-durable-lsn*))
    (wal-invalid-input "physical follow anchor belongs to another source or future WAL"))
  (unless (equal (getf (physical-follow-pin-anchor pin) :history-text)
                 (bytes->string (timeline-history-bytes *wal-insert-timeline*)))
    (wal-invalid-input "physical follow anchor history differs from its source")))

(defun physical-follow-pin-check-retained-wal (pin)
  "Require retained ancestry or current-source WAL through the durable endpoint.
For a ready pin, scan its confirmed suffix; startup performs this before listeners."
  (when (physical-follow-pin-ready-p pin)
    (return-from physical-follow-pin-check-retained-wal
      (physical-follow-source-check-ready-range pin)))
  (let ((floor (physical-source-pin-restart-lsn pin)))
    (when (< floor *wal-durable-lsn*)
      (loop for segment from (floor floor +wal-segment-size+)
              to (floor (1- *wal-durable-lsn*) +wal-segment-size+)
            do (physical-follow-source-segment
                pin segment (max floor (physical-source-pin-consistent-lsn pin)
                                 (* segment +wal-segment-size+)))))))

(defun physical-follow-pin-check-wal (pin)
  "Validate PIN and, unless lost, its retained source file inventory."
  (physical-follow-pin-check-fields pin)
  (physical-follow-pin-check-source pin)
  (unless (physical-source-pin-lost-p pin)
    (physical-follow-pin-check-retained-wal pin))
  pin)
