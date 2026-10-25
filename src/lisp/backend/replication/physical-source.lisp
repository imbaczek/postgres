;;;; Durable physical bootstrap WAL pins; no leader or writer-authority coordination.
(in-package #:pg)

(defstruct physical-source-pin name system-id restart-lsn consistent-lsn
  confirmed-lsn (prefix-digest #xCBF29CE484222325) (lost-p nil))

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

(defun physical-source-pin-check-wal (pin)
  "Validate a bootstrap checkpoint or a durable confirmed receive floor."
  (let* ((restart (physical-source-pin-restart-lsn pin))
         (consistent (physical-source-pin-consistent-lsn pin))
         (confirmed (physical-source-pin-confirmed-lsn pin))
         (segments (wal-discover-segments)))
    (unless (and (<= 0 consistent *wal-durable-lsn*)
                 (= restart (if confirmed confirmed
                                (if (zerop consistent) 0 (- consistent +wal-header-size+))))
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
      (let ((bytes (make-array +wal-header-size+ :element-type '(unsigned-byte 8))))
        (with-open-file (stream (wal-segment-path (floor restart +wal-segment-size+))
                                :element-type '(unsigned-byte 8))
          (file-position stream (mod restart +wal-segment-size+))
          (unless (= (read-sequence bytes stream) +wal-header-size+)
            (error "physical source pin checkpoint is truncated")))
        (let ((record (wal-decode-record bytes restart)))
          (unless (and (= (wal-record-type record) +wal-record-checkpoint+)
                       (= (wal-record-end record) consistent)
                       (zerop (wal-record-xid record)))
            (error "physical source pin does not name a checkpoint record")))))))

(defun physical-source-pin-encode (pin)
  (let* ((prefix (format nil "PGPHYSPIN2 ~a ~d 1 ~d ~d ~d ~d ~a"
                         (physical-source-pin-name pin) (physical-source-pin-system-id pin)
                         (physical-source-pin-restart-lsn pin)
                         (physical-source-pin-consistent-lsn pin)
                         (or (physical-source-pin-confirmed-lsn pin) -1)
                         (physical-source-pin-prefix-digest pin)
                         (if (physical-source-pin-lost-p pin) "lost" "reserved")))
         (crc (wal-crc32 (string->bytes prefix))))
    (string->bytes (format nil "~a ~8,'0X~%" prefix crc))))

(defun physical-source-pin-read (path)
  (with-open-file (stream path)
    (unless (<= (file-length stream) 1024)
      (error "corrupt or foreign physical source pin ~a" path))
    (let* ((line (read-line stream nil nil))
           (fields (and line (wal-split-fields line)))
           (legacy (equal (first fields) "PGPHYSPIN1"))
           (name (second fields))
           (system-id (ignore-errors (parse-integer (third fields))))
           (restart (ignore-errors (parse-integer (fifth fields))))
           (consistent (ignore-errors (parse-integer (sixth fields))))
           (confirmed (if legacy -1 (ignore-errors (parse-integer (seventh fields)))))
           (digest (if legacy #xCBF29CE484222325
                       (ignore-errors (parse-integer (eighth fields)))))
           (status (if legacy "reserved" (ninth fields)))
           (crc (ignore-errors (parse-integer (car (last fields)) :radix 16))))
      (unless (and (if legacy (= (length fields) 7)
                       (and (= (length fields) 10) (equal (first fields) "PGPHYSPIN2")))
                   (wal-valid-replication-slot-name-p name)
                   (equal name (pathname-name path)) (equal (fourth fields) "1")
                   (typep system-id '(unsigned-byte 64)) (= system-id (catalog-system-identifier))
                   (typep restart '(unsigned-byte 64)) (typep consistent '(unsigned-byte 64))
                   (or (eql confirmed -1) (typep confirmed '(unsigned-byte 64)))
                   (typep digest '(unsigned-byte 64)) (member status '("reserved" "lost") :test #'equal)
                   crc (= crc (wal-crc32 (string->bytes
                                          (format nil "~{~a~^ ~}" (butlast fields)))))
                   (null (read-line stream nil nil)))
        (error "corrupt or foreign physical source pin ~a" path))
      (let ((pin (make-physical-source-pin :name name :system-id system-id
                                           :restart-lsn restart :consistent-lsn consistent
                                           :confirmed-lsn (unless (= confirmed -1) confirmed)
                                           :prefix-digest digest :lost-p (equal status "lost"))))
        (physical-source-pin-check-wal pin)
        pin))))

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
                   :name name :system-id (catalog-system-identifier)
                   :consistent-lsn end
                   :restart-lsn (if (zerop end) 0 (- end +wal-header-size+)))))
        (physical-source-pin-check-wal pin)
        (wal-ensure-directory-durable (physical-source-pin-directory))
        (physical-source-pin-save pin)
        (setf (gethash pin *wal-snapshot-retention-pins*)
              (physical-source-pin-restart-lsn pin)
              (gethash name *physical-source-pins*) pin)
        pin))))

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
      (unless (and pin (not (physical-source-pin-lost-p pin))
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

(defun physical-source-pin-release (name)
  "Durably remove a source pin before allowing checkpoint WAL recycling."
  (prevent-recovery-primary-operation "physical source pin release")
  (unless (wal-valid-replication-slot-name-p name)
    (ereport "42602" "invalid physical source pin name ~s" name))
  (with-write-lock
    (with-wal-critical
      (let ((pin (gethash name *physical-source-pins*)))
        (unless pin (wal-invalid-input "physical source pin ~a does not exist" name))
        (when (gethash name *physical-source-senders*)
          (wal-invalid-input "physical source pin ~a has an active sender" name))
        (let ((path (physical-source-pin-path name)))
          (when (probe-file path) (delete-file path))
          (fsync-directory (physical-source-pin-directory)))
        (remhash pin *wal-snapshot-retention-pins*)
        (remhash name *physical-source-pins*))))
  (values))
