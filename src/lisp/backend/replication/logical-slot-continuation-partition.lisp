(in-package #:pg)

;; Known ordinary slots must belong to this system and have a generation
;; distinct from certified children.  Legacy slots lack that identity: for a
;; reserved slot, require a restart on the local child without claiming origin.
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

;; Check persistent ordinary slots beside the certified children. Physical
;; and logical slots keep their own state and position checks.
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

(defun logical-slot-continuation-check-ordinary-registry-with-validator
    (registry members system-id history frontier validator)
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
         (funcall validator slot system-id history frontier child-provenances)))
     registry))
  t)

;; Runtime may also contain a temporary physical slot with a checked lifetime.
;; The permitted capability identifies the one creation or drop in progress;
;; other temporary slots must already be live.
(defun logical-slot-continuation-check-runtime-ordinary-slot
    (slot system-id history frontier &optional child-provenances permitted operation)
  (unless (or (null permitted) (physical-slot-lifetime-p permitted))
    (error "temporary physical operation requires a typed lifetime"))
  (if (or (wal-replication-slot-temporary-p slot)
          (wal-replication-slot-temporary-lifetime slot)
          (gethash slot *physical-slot-lifetimes*))
      (progn
        (unless (and (wal-physical-slot-p slot)
                     (physical-slot-lifetime-p (wal-replication-slot-temporary-lifetime slot)))
          (error "runtime ordinary temporary slot requires a physical lifetime"))
        (let* ((lifetime (wal-replication-slot-temporary-lifetime slot))
               (mode (if (eq lifetime permitted)
                         (ecase operation
                           (:physical-create :creating)
                           (:physical-drop :drop-check))
                         :live)))
          (physical-slot-lifetime-call-under-lock lifetime mode slot))
        (physical-slot-check-position slot system-id history frontier))
      (logical-slot-continuation-check-ordinary-slot
       slot system-id history frontier child-provenances))
  slot)

(defun logical-slot-continuation-check-runtime-ordinary-registry
    (registry members system-id history frontier &optional permitted operation)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (physical-slot-check-names-under-lock)
  (logical-slot-continuation-check-ordinary-registry-with-validator
   registry members system-id history frontier
   (lambda (slot sid chain end provenances)
     (logical-slot-continuation-check-runtime-ordinary-slot
      slot sid chain end provenances permitted operation))))

(defun logical-slot-continuation-check-ordinary-registry
    (registry members system-id history frontier)
  (logical-slot-continuation-check-ordinary-registry-with-validator
   registry members system-id history frontier
   #'logical-slot-continuation-check-ordinary-slot))

(defun logical-slot-continuation-check-ordinary-candidate
    (registry members value system-id history frontier)
  (unless (and (wal-replication-slot-p value)
               (not (assoc (wal-replication-slot-name value) members :test #'string=)))
    (error "ordinary slot is outside its live registry"))
  ;; A copied logical candidate must retain its immutable identity and match
  ;; the kind of the live entry bound to this name.
  (let ((live (gethash (wal-replication-slot-name value) registry)))
    (unless (and (wal-replication-slot-p live)
                 (eq (wal-physical-slot-p live) (wal-physical-slot-p value)))
      (error "ordinary slot changed its live kind or registry binding"))
    (when (and (wal-logical-slot-p value) (not (eq value live)))
      (logical-slot-check-update-under-lock live value))
    (logical-slot-continuation-check-ordinary-slot value system-id history frontier)))

(defun logical-slot-continuation-check-runtime-ordinary-candidate
    (registry members value system-id history frontier)
  (unless (and (wal-replication-slot-p value)
               (not (assoc (wal-replication-slot-name value) members :test #'string=)))
    (error "ordinary slot is outside its live registry"))
  (let ((live (gethash (wal-replication-slot-name value) registry)))
    (unless (and (wal-replication-slot-p live)
                 (eq (wal-physical-slot-p live) (wal-physical-slot-p value)))
      (error "ordinary slot changed its live kind or registry binding"))
    (if (or (wal-replication-slot-temporary-p live)
            (wal-replication-slot-temporary-p value))
        (progn
          (unless (and (wal-physical-slot-p live)
                       (wal-replication-slot-temporary-p live)
                       (wal-replication-slot-temporary-p value))
            (error "ordinary candidate changed temporary physical identity"))
          (physical-slot-lifetime-check-under-lock live)
          (physical-slot-check-temporary-candidate-under-lock value)
          (physical-slot-check-position value system-id history frontier))
        (logical-slot-continuation-check-ordinary-candidate
         registry members value system-id history frontier)))
  value)

(defun logical-slot-continuation-partition-rows-with-validator
    (rows certificate host validator frontier)
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
              (funcall validator slot (first host) history frontier child-provenances)
              (push row ordinary)))))
    (values (nreverse children) (nreverse ordinary))))

(defun logical-slot-continuation-check-runtime-ordinary-image
    (slot system-id history frontier child-provenances)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((live (gethash (wal-replication-slot-name slot) *wal-replication-slots*)))
    (unless (and (wal-replication-slot-p live)
                 (eq (wal-physical-slot-p slot) (wal-physical-slot-p live)))
      (error "runtime ordinary image has no matching live slot"))
    (if (wal-replication-slot-temporary-p slot)
        (progn
          (unless (and (wal-physical-slot-p slot)
                       (wal-replication-slot-temporary-p live))
            (error "runtime temporary image requires a physical live slot"))
          (physical-slot-lifetime-check-under-lock live)
          (physical-slot-check-position slot system-id history frontier))
        (logical-slot-continuation-check-ordinary-slot
         slot system-id history frontier child-provenances))
    (unless (equalp (logical-slot-startup-candidate-image slot)
                    (logical-slot-startup-candidate-image live))
      (error "runtime ordinary disk image differs from its live durable state")))
  slot)

(declaim (ftype function logical-slot-continuation-native-preflight-registry))

(defun logical-slot-continuation-partition-runtime-rows-under-lock (rows certificate host)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (physical-slot-check-names-under-lock)
  (unless (= (length rows) (hash-table-count *wal-replication-slots*))
    (error "runtime continuation inventory differs from the full live registry"))
  (logical-slot-continuation-partition-rows-with-validator
   rows certificate host #'logical-slot-continuation-check-runtime-ordinary-image
   *wal-durable-lsn*))

;; Split the full saved inventory into certified child rows and ordinary
;; rows, preserving their order. Check child certificates separately from
;; ordinary slot state; callers still retain the complete raw inventory.
(defun logical-slot-continuation-partition-rows (rows certificate host)
  (logical-slot-continuation-partition-rows-with-validator
   rows certificate host #'logical-slot-continuation-check-ordinary-slot (sixth host)))

(defun logical-slot-continuation-selected-child-p (slot snapshot)
  (and (wal-logical-slot-p slot)
       (wal-replication-slot-child-owner slot)
       (assoc (wal-replication-slot-name slot) (getf snapshot :slots) :test #'string=)))
