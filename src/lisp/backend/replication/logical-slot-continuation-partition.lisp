(in-package #:pg)

(defun logical-slot-continuation-check-ordinary-logical
    (slot system-id history frontier child-provenances)
  (unless (and (logical-slot-state-valid-p slot)
               (null (wal-replication-slot-child-owner slot))
               (<= (wal-replication-slot-restart-lsn slot) frontier)
               (<= (wal-replication-slot-consistent-lsn slot) frontier)
               (or (null (wal-replication-slot-confirmed-flush-lsn slot))
                   (<= (wal-replication-slot-confirmed-flush-lsn slot) frontier)))
    (error "ordinary logical slot has invalid state, owner or durable positions"))
  (let ((provenance (wal-replication-slot-logical-provenance slot)))
    (if provenance
        (progn
          (unless (and (= system-id (logical-slot-provenance-system-id provenance))
                       (not (some
                             (lambda (child)
                               (and (= system-id (logical-slot-provenance-system-id child))
                                    (= (logical-slot-provenance-generation provenance)
                                       (logical-slot-provenance-generation child))))
                             child-provenances)))
            (error "ordinary logical slot has foreign or certified child identity"))
          (when (eq :reserved (wal-replication-slot-wal-status slot))
            (logical-slot-check-primary-history slot history)))
        (when (and (eq :reserved (wal-replication-slot-wal-status slot))
                   (< (wal-replication-slot-restart-lsn slot)
                      (timeline-history-entry-begin (first history))))
          (error "legacy ordinary slot requires a local child restart"))))
  slot)

;; Ordinary slots admitted alongside the continuation must be persistent
;; physical slots. Validate their state and any reserved restart against the
;; current history.
(defun logical-slot-continuation-check-ordinary-slot
    (slot system-id history frontier &optional child-provenances)
  (unless (and (wal-replication-slot-p slot)
               (not (wal-replication-slot-temporary-p slot)))
    (error "ordinary continuation inventory requires a persistent slot"))
  (if (wal-physical-slot-p slot)
      (physical-slot-check-position slot system-id history frontier)
      (logical-slot-continuation-check-ordinary-logical
       slot system-id history frontier child-provenances))
  slot)

(defun logical-slot-continuation-check-ordinary-registry
    (registry members system-id history frontier)
  (unless (and (hash-table-p registry) (eq (hash-table-test registry) 'equal))
    (error "continuation registry requires an EQUAL table"))
  (let ((child-provenances
          (mapcar (lambda (entry)
                    (wal-replication-slot-logical-provenance (cdr entry)))
                  members)))
    (maphash
     (lambda (name slot)
       (unless (and (stringp name) (wal-replication-slot-p slot)
                    (string= name (wal-replication-slot-name slot)))
         (error "ordinary continuation slot changed its registry binding"))
       (unless (assoc name members :test #'string=)
         (logical-slot-continuation-check-ordinary-slot
          slot system-id history frontier child-provenances)))
     registry))
  t)

(defun logical-slot-continuation-check-ordinary-candidate
    (registry members value system-id history frontier)
  (unless (and (wal-replication-slot-p value)
               (not (assoc (wal-replication-slot-name value) members :test #'string=)))
    (error "ordinary slot is outside its live registry"))
  (let ((live (gethash (wal-replication-slot-name value) registry)))
    (unless (and (wal-replication-slot-p live)
                 (eq (wal-physical-slot-p live) (wal-physical-slot-p value)))
      (error "ordinary slot changed its live kind or registry binding"))
    (when (and (wal-logical-slot-p value) (not (eq value live)))
      (logical-slot-check-update-under-lock live value))
    (logical-slot-continuation-check-ordinary-slot value system-id history frontier)))

;; Split the full saved inventory into certified child rows and ordinary
;; physical rows, preserving their order.  Check names, child state and physical
;; positions before returning either list.
(defun logical-slot-continuation-partition-rows (rows certificate host)
  (logical-slot-copy-descriptor-list-count rows +logical-slot-continuation-obligation-limit+)
  (logical-slot-copy-descriptor-list-count
   (getf certificate :slots) +logical-slot-continuation-obligation-limit+)
  (unless (eql (first host) (getf certificate :system-id))
    (error "continuation inventory belongs to another cluster"))
  (let ((certified (make-hash-table :test #'equal))
        (names (make-hash-table :test #'equal))
        (history (logical-slot-continuation-history (fifth host) (fourth host)))
        (children nil) (ordinary nil) (child-provenances nil))
    (dolist (entry (getf certificate :slots))
      (let ((name (first entry)))
        (when (gethash name certified)
          (error "continuation certificate repeats a slot name"))
        (setf (gethash name certified) entry)
        (when (fourth entry)
          (push (wal-replication-slot-logical-provenance
                 (logical-slot-state-decode (string->bytes (fourth entry)) name))
                child-provenances))))
    (dolist (row rows)
      (logical-slot-continuation-list row 2)
      (let* ((name (first row)) (bytes (second row))
                                (entry (gethash name certified)))
        (unless (wal-valid-replication-slot-name-p name)
          (error "continuation inventory has an invalid slot name"))
        (logical-slot-copy-descriptor-blob-size bytes 1 +slot-state-maximum-bytes+)
        (when (gethash name names)
          (error "continuation inventory repeats a slot name"))
        (setf (gethash name names) t)
        (if (and entry (fourth entry))
            (progn
              (logical-slot-imported-check-state
               (logical-slot-state-decode bytes name)
               (logical-slot-state-decode (string->bytes (fourth entry)) name)
               history (sixth host))
              (push row children))
            (let ((slot (wal-decode-replication-slot-image bytes name)))
              (logical-slot-continuation-check-ordinary-slot
               slot (first host) history (sixth host) child-provenances)
              (push row ordinary)))))
    (values (nreverse children) (nreverse ordinary))))

(defun logical-slot-continuation-selected-child-p (slot snapshot)
  (and (wal-logical-slot-p slot)
       (wal-replication-slot-child-owner slot)
       (assoc (wal-replication-slot-name slot) (getf snapshot :slots) :test #'string=)))
