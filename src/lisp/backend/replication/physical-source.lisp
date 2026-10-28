;;;; Durable physical bootstrap WAL pins; no leader or writer-authority coordination.
(in-package #:pg)

(defstruct physical-source-pin
  "A retained source position and its timeline and optional backup manifest digest.
The digest binds a backup to its stream request; the prefix digest separately
checks received WAL progress. These digests detect corruption or divergence."
  name system-id restart-lsn consistent-lsn
  (timeline 1) manifest-digest confirmed-lsn (prefix-digest #xCBF29CE484222325) (lost-p nil))

(defvar *physical-source-pins* (make-hash-table :test #'equal))

(defvar *physical-source-senders* (make-hash-table :test #'equal))

(defvar *physical-source-test-hook* nil)

(defun physical-source-pin-test-point (point pin)
  (when *physical-source-test-hook* (funcall *physical-source-test-hook* point pin)))

(declaim (ftype function physical-source-pin-path physical-source-pin-encode))

(defun physical-source-pin-save (pin)
  (physical-source-pin-test-point :before-save pin)
  (wal-atomic-write-bytes (physical-source-pin-path (physical-source-pin-name pin))
                          (physical-source-pin-encode pin))
  (physical-source-pin-test-point :after-save pin))

(defun physical-source-pin-directory ()
  (merge-pathnames "pg_physical_pins/" *pgdata*))

(defun physical-source-pin-path (name)
  (merge-pathnames (format nil "~a.pin" name) (physical-source-pin-directory)))

(declaim (ftype function physical-follow-pin-p physical-follow-pin-encode physical-follow-pin-decode
                physical-follow-pin-image-p physical-follow-pin-image-limit
                physical-follow-pin-check-wal physical-follow-pin-ready-p))

(defun physical-source-pin-check-wal (pin)
  "Validate a bootstrap checkpoint or a durable confirmed receive floor."
  (declare (notinline physical-follow-pin-p))
  (when (physical-follow-pin-p pin)
    (return-from physical-source-pin-check-wal (physical-follow-pin-check-wal pin)))
  (let* ((restart (physical-source-pin-restart-lsn pin))
         (consistent (physical-source-pin-consistent-lsn pin))
         (confirmed (physical-source-pin-confirmed-lsn pin))
         (segments (wal-discover-segments)))
    (unless (and (typep (physical-source-pin-timeline pin) '(integer 1 4294967295))
                 (= (physical-source-pin-timeline pin) *wal-insert-timeline*)
                 (<= 0 consistent *wal-durable-lsn*)
                 (if confirmed (= restart confirmed)
                     (if (zerop consistent) (zerop restart)
                         (and (typep restart '(unsigned-byte 64))
                              (< restart consistent)
                              (member (- consistent restart)
                                      (list +wal-header-size+ (wal-checkpoint-record-size)))
                              (= (floor restart +wal-segment-size+)
                                 (floor (1- consistent) +wal-segment-size+)))))
                 (or (null confirmed) (<= consistent confirmed *wal-durable-lsn*))
                 (typep (physical-source-pin-prefix-digest pin) '(unsigned-byte 64))
                 (or (and confirmed (> confirmed consistent))
                     (= (physical-source-pin-prefix-digest pin) #xCBF29CE484222325)))
      (error "physical source pin has an invalid checkpoint or confirmed boundary"))
    ;; Durable invalidation permits missing old segments, but never revalidates
    ;; the pin merely because some WAL still happens to remain.
    (when (physical-source-pin-lost-p pin) (return-from physical-source-pin-check-wal))
    (when (< restart *wal-durable-lsn*)
      (let ((remaining (member (floor restart +wal-segment-size+) segments)))
        (loop for segment from (floor restart +wal-segment-size+)
                to (floor (1- *wal-durable-lsn*) +wal-segment-size+)
              unless (eql segment (pop remaining))
                do (error "WAL required by physical source pin ~a is missing segment ~a"
                          (physical-source-pin-name pin) segment))))
    (when (and (null confirmed) (plusp consistent))
      (let* ((length (- consistent restart))
             (bytes (make-array length :element-type '(unsigned-byte 8))))
        (with-open-file (stream (wal-segment-path (floor restart +wal-segment-size+))
                                :element-type '(unsigned-byte 8))
          (file-position stream (mod restart +wal-segment-size+))
          (unless (= (read-sequence bytes stream) length)
            (error "physical source pin checkpoint is truncated")))
        (let ((record (wal-decode-record bytes restart)))
          (let ((snapshot (wal-checkpoint-record-snapshot record)))
            (when (and (= consistent *wal-checkpoint-lsn*)
                       (not (equal snapshot *wal-checkpoint-snapshot*)))
              (error "physical checkpoint WAL differs from its control copy")))
          (unless (and (= (wal-record-type record) +wal-record-checkpoint+)
                       (= (wal-record-end record) consistent)
                       (zerop (wal-record-xid record)))
            (error "physical source pin does not name a checkpoint record")))))))

(defun physical-source-pin-encode (pin)
  (declare (notinline physical-follow-pin-p))
  (when (physical-follow-pin-p pin)
    (return-from physical-source-pin-encode (physical-follow-pin-encode pin)))
  (let* ((prefix (format nil "PGPHYSPIN2 ~a ~d ~d ~d ~d ~d ~d ~a"
                         (physical-source-pin-name pin) (physical-source-pin-system-id pin)
                         (physical-source-pin-timeline pin)
                         (physical-source-pin-restart-lsn pin)
                         (physical-source-pin-consistent-lsn pin)
                         (or (physical-source-pin-confirmed-lsn pin) -1)
                         (physical-source-pin-prefix-digest pin)
                         (if (physical-source-pin-lost-p pin) "lost" "reserved")))
         (prefix (if (physical-source-pin-manifest-digest pin)
                     (format nil "~a ~d" prefix (physical-source-pin-manifest-digest pin)) prefix))
         (crc (wal-crc32 (string->bytes prefix))))
    (string->bytes (format nil "~a ~8,'0X~%" prefix crc))))

(defun physical-source-bootstrap-pin-read (path bytes)
  (unless (<= (length bytes) 1024)
    (error "corrupt or foreign physical source pin ~a" path))
  (with-input-from-string (stream (bytes->string bytes))
    (let* ((line (read-line stream nil nil))
           (fields (and line (wal-split-fields line)))
           (legacy (equal (first fields) "PGPHYSPIN1"))
           (extended (and (not legacy) (= (length fields) 11)))
           (manifest-digest (when extended (ignore-errors (parse-integer (tenth fields)))))
           (name (second fields))
           (system-id (ignore-errors (parse-integer (third fields))))
           (timeline (ignore-errors (parse-integer (fourth fields))))
           (restart (ignore-errors (parse-integer (fifth fields))))
           (consistent (ignore-errors (parse-integer (sixth fields))))
           (confirmed (if legacy -1 (ignore-errors (parse-integer (seventh fields)))))
           (digest (if legacy #xCBF29CE484222325
                       (ignore-errors (parse-integer (eighth fields)))))
           (status (if legacy "reserved" (ninth fields)))
           (crc (ignore-errors (parse-integer (car (last fields)) :radix 16))))
      (unless (and (if legacy (= (length fields) 7)
                       (and (member (length fields) '(10 11)) (equal (first fields) "PGPHYSPIN2")))
                   (wal-valid-replication-slot-name-p name)
                   (equal name (pathname-name path))
                   (typep timeline '(integer 1 4294967295))
                   (or (not extended) (typep manifest-digest '(unsigned-byte 64)))
                   (or (not legacy) (= timeline 1))
                   (= timeline *wal-insert-timeline*)
                   (typep system-id '(unsigned-byte 64)) (= system-id (catalog-system-identifier))
                   (typep restart '(unsigned-byte 64)) (typep consistent '(unsigned-byte 64))
                   (or (eql confirmed -1) (typep confirmed '(unsigned-byte 64)))
                   (typep digest '(unsigned-byte 64)) (member status '("reserved" "lost") :test #'equal)
                   crc (= crc (wal-crc32 (string->bytes
                                          (format nil "~{~a~^ ~}" (butlast fields)))))
                   (null (read-line stream nil nil)))
        (error "corrupt or foreign physical source pin ~a" path))
      (let ((pin (make-physical-source-pin :name name :system-id system-id :timeline timeline
                                           :manifest-digest manifest-digest
                                           :restart-lsn restart :consistent-lsn consistent
                                           :confirmed-lsn (unless (= confirmed -1) confirmed)
                                           :prefix-digest digest :lost-p (equal status "lost"))))
        (physical-source-pin-check-wal pin)
        pin))))

(defun physical-source-pin-read (path)
  (let ((bytes (read-bounded-regular-file path (physical-follow-pin-image-limit))))
    (if (physical-follow-pin-image-p bytes)
        (let ((pin (physical-follow-pin-decode bytes)))
          (unless (equal (physical-source-pin-name pin) (pathname-name path))
            (error "physical follow pin pathname differs from its name"))
          (physical-source-pin-check-wal pin)
          pin)
        (physical-source-bootstrap-pin-read path bytes))))

(defun physical-source-pin-stream-ready-p (pin)
  (declare (notinline physical-follow-pin-p))
  (and (not (physical-source-pin-lost-p pin))
       (or (not (physical-follow-pin-p pin)) (physical-follow-pin-ready-p pin))))

(defun physical-source-pin-create (name)
  "Retain the bootstrap checkpoint until a verified receiver durably confirms WAL."
  (prevent-recovery-primary-operation "physical source pin creation")
  (unless (wal-valid-replication-slot-name-p name)
    (ereport "42602" "invalid physical source pin name ~s" name))
  (with-write-lock
    (with-wal-critical
      (when (gethash name *physical-source-pins*)
        (wal-invalid-input "physical source pin ~a already exists" name))
      (when (probe-file (physical-source-pin-path name))
        (error "unregistered physical source pin file ~a" name))
      (let* ((end *wal-checkpoint-lsn*)
             (pin (make-physical-source-pin
                   :name name :system-id (catalog-system-identifier) :timeline *wal-insert-timeline*
                   :consistent-lsn end
                   :restart-lsn (wal-checkpoint-record-start end))))
        (physical-source-pin-check-wal pin)
        (wal-ensure-directory-durable (physical-source-pin-directory))
        (physical-source-pin-save pin)
        (setf (gethash pin *wal-snapshot-retention-pins*)
              (physical-source-pin-restart-lsn pin)
              (gethash name *physical-source-pins*) pin)
        pin))))

(defun physical-source-pin-bind-manifest (pin digest)
  "Durably bind an unused source pin to the completed backup manifest digest.
Require the current timeline and checkpoint, and no confirmed progress or sender.
Publish the in-memory binding only after saving the candidate pin."
  (declare (notinline physical-follow-pin-p))
  (with-write-lock
    (with-wal-critical
      (unless (and (not (physical-follow-pin-p pin))
                   (typep digest '(unsigned-byte 64))
                   (eq pin (gethash (physical-source-pin-name pin) *physical-source-pins*))
                   (= (physical-source-pin-timeline pin) *wal-insert-timeline*)
                   (= (physical-source-pin-consistent-lsn pin) *wal-checkpoint-lsn*)
                   (not (physical-source-pin-lost-p pin))
                   (null (physical-source-pin-confirmed-lsn pin))
                   (null (physical-source-pin-manifest-digest pin))
                   (null (gethash (physical-source-pin-name pin) *physical-source-senders*)))
        (wal-invalid-input "physical source pin cannot bind this backup manifest"))
      (let ((candidate (copy-physical-source-pin pin)))
        (setf (physical-source-pin-manifest-digest candidate) digest)
        (physical-source-pin-save candidate)
        (setf (physical-source-pin-manifest-digest pin) digest))))
  pin)

(defun physical-source-pin-load ()
  "Restore source retention before startup can admit checkpoint/recycle work."
  (with-write-lock
    (with-wal-critical
      (let ((candidates (make-hash-table :test #'equal)))
        (dolist (path (directory (merge-pathnames "*.pin" (physical-source-pin-directory))))
          (let* ((pin (physical-source-pin-read path))
                 (name (physical-source-pin-name pin)))
            (when (gethash name candidates)
              (error "duplicate physical source pin ~a" name))
            (setf (gethash name candidates) pin)))
        ;; Validate the complete registry before replacing any existing pins.
        (maphash (lambda (name pin)
                   (declare (ignore name))
                   (remhash pin *wal-snapshot-retention-pins*))
                 *physical-source-pins*)
        (maphash (lambda (name pin)
                   (declare (ignore name))
                   (unless (physical-source-pin-lost-p pin)
                     (setf (gethash pin *wal-snapshot-retention-pins*)
                           (physical-source-pin-restart-lsn pin))))
                 candidates)
        (setf *physical-source-pins* candidates))))
  (values))

(defun physical-source-confirm-receipt (name owner lsn digest)
  "Publish only a verified complete-record flush, durably before releasing WAL."
  (with-wal-critical
    (let* ((pin (gethash name *physical-source-pins*))
           (old (and pin (physical-source-pin-confirmed-lsn pin))))
      (unless (and pin (physical-source-pin-stream-ready-p pin)
                   (not (physical-source-pin-lost-p pin))
                   (= (physical-source-pin-timeline pin) *wal-insert-timeline*)
                   (eq owner (gethash name *physical-source-senders*))
                   (typep digest '(unsigned-byte 64))
                   (<= (or old (physical-source-pin-consistent-lsn pin)) lsn *wal-durable-lsn*))
        (wal-invalid-input "physical receipt no longer belongs to a valid retained sender"))
      (when (eql old lsn)
        (unless (= digest (physical-source-pin-prefix-digest pin))
          (wal-invalid-input "physical receipt digest changed at the same boundary"))
        (return-from physical-source-confirm-receipt pin))
      (let ((candidate (copy-physical-source-pin pin)))
        (setf (physical-source-pin-confirmed-lsn candidate) lsn
              (physical-source-pin-restart-lsn candidate) lsn
              (physical-source-pin-prefix-digest candidate) digest)
        (physical-source-pin-save candidate)
        ;; Keep the object used by the recycler stable across acknowledgment.
        (setf (physical-source-pin-confirmed-lsn pin) lsn
              (physical-source-pin-restart-lsn pin) lsn
              (physical-source-pin-prefix-digest pin) digest
              (gethash pin *wal-snapshot-retention-pins*) lsn)
        (physical-source-pin-test-point :after-publish pin)
        pin))))

(defun physical-source-invalidate-lagging-pins-under-wal-lock ()
  "Persist lost retention before checkpoint may remove its required WAL."
  (when (integerp *max-slot-wal-keep-bytes*)
    (maphash
     (lambda (name pin)
       (declare (ignore name))
       (when (and (not (physical-source-pin-lost-p pin))
                  (> (- *wal-durable-lsn* (physical-source-pin-restart-lsn pin))
                     *max-slot-wal-keep-bytes*))
         (let ((candidate (copy-physical-source-pin pin)))
           (setf (physical-source-pin-lost-p candidate) t)
           (physical-source-pin-save candidate)
           (setf (physical-source-pin-lost-p pin) t)
           (remhash pin *wal-snapshot-retention-pins*))))
     *physical-source-pins*))
  (values))

(defun physical-source-pin-unlink (pin)
  "Unlink PIN and sync its directory before removing in-memory retention.
The caller owns the write and WAL critical section. An I/O failure fences WAL;
restart must resolve an ambiguous deletion before recycling."
  (let ((name (physical-source-pin-name pin)))
    (let ((path (physical-source-pin-path name)))
      (physical-source-pin-test-point :before-release-unlink pin)
      (when (probe-file path) (delete-file path))
      (physical-source-pin-test-point :after-release-unlink pin)
      (fsync-directory (physical-source-pin-directory))
      (physical-source-pin-test-point :after-release-directory-fsync pin))
    (remhash pin *wal-snapshot-retention-pins*)
    (remhash name *physical-source-pins*)))

(defun physical-source-pin-release (name)
  "Durably remove a source pin before allowing checkpoint WAL recycling.
This local wrapper requires an existing pin without an active sender. Physical
pins use their own namespace, separate from logical replication slots."
  (prevent-recovery-primary-operation "physical source pin release")
  (unless (wal-valid-replication-slot-name-p name)
    (ereport "42602" "invalid physical source pin name ~s" name))
  (with-write-lock
    (with-wal-critical
      (let ((pin (gethash name *physical-source-pins*)))
        (unless pin (wal-invalid-input "physical source pin ~a does not exist" name))
        (when (gethash name *physical-source-senders*)
          (wal-invalid-input "physical source pin ~a has an active sender" name))
        (physical-source-pin-unlink pin))))
  (values))
