(in-package #:pg)

(defun physical-standby-backup-checkpoint ()
  "Prove that the frozen standby is at an exact source checkpoint.
The applied next XID and catalog next OID must match the checkpoint, and its
oldest active XID must equal next XID.  The completed source reservation must
also reach this replay position and match the checkpoint's reservation bound."
  (check-physical-recovery-work)
  (unless (and (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
               (not *live-operation-drain-open*) (zerop *live-operation-depth*)
               (eql 2 (getf *physical-recovery-state* :phase))
               *catalog* *physical-receive-store*
               (not *swank-listener-enabled-p*)
               (not (physical-store-failed-p *physical-receive-store*))
               (not (physical-store-closed-p *physical-receive-store*)))
    (error "standby export requires healthy streaming recovery under quiescence"))
  (let* ((snapshot (getf *physical-recovery-state* :source-checkpoint))
         (reservation (getf *physical-recovery-state* :source-reservation))
         (system-id (catalog-system-identifier)))
    (unless (and snapshot (wal-checkpoint-snapshot-valid-p snapshot)
                 (= system-id (getf *physical-recovery-state* :system-id))
                 (equal (catalog-cluster-identity *catalog*)
                        (getf *physical-recovery-state* :cluster-identity))
                 (= system-id (physical-store-system-id *physical-receive-store*))
                 (= (physical-store-timeline *physical-receive-store*)
                    (getf *physical-recovery-state* :timeline))
                 (= (physical-store-base-lsn *physical-receive-store*)
                    (getf *physical-recovery-state* :end))
                 (= (getf snapshot :timeline) (physical-recovery-replay-timeline))
                 (= (getf snapshot :end) *last-replay-lsn*)
                 (= (getf snapshot :next-xid) *next-xid*)
                 (getf snapshot :next-oid)
                 (= (getf snapshot :next-oid) (catalog-next-oid *catalog*))
                 (= (getf snapshot :oldest-active-xid) *next-xid*)
                 (physical-source-reservation-valid-p reservation)
                 (getf reservation :complete)
                 (= (getf reservation :through) *last-replay-lsn*)
                 (= (getf reservation :bound) (getf snapshot :xid-reserved-through)))
      (error "standby export requires an exact source checkpoint with no active XIDs"))
    (physical-standby-backup-logical-facts snapshot)
    (copy-tree snapshot)))

(defun physical-standby-backup-check-prefix (prefix snapshot)
  (let* ((start (getf snapshot :start)) (end (getf snapshot :end))
                                        (size (1+ (mod (1- end) +wal-segment-size+))))
    (unless (and (typep prefix '(vector (unsigned-byte 8))) (= (length prefix) size))
      (error "standby checkpoint lacks its complete segment prefix"))
    (let* ((record (wal-decode-record
                    (subseq prefix (mod start +wal-segment-size+)) start))
           (decoded (wal-checkpoint-record-snapshot record)))
      (unless (and (>= (wal-record-version record) 7)
                   (= (wal-record-type record) +wal-record-checkpoint+)
                   (zerop (wal-record-xid record)) (= (wal-record-end record) end)
                   (equal decoded snapshot))
        (error "standby checkpoint WAL differs from its applied snapshot"))
      (values (copy-seq prefix) decoded))))

(defun physical-standby-backup-prefix (store snapshot)
  "Return the complete checkpoint segment prefix through SNAPSHOT's end.
For a checkpoint later than the immutable base, validate the full retained
inventory and its digest chain from the base LSN through the current receive
floor before reading the extent. The caller holds the store mutex; a reader
lease protects retained images. Compare the decoded checkpoint with the applied
snapshot in both cases."
  (when (= (getf snapshot :end) (getf *physical-recovery-state* :end))
    (return-from physical-standby-backup-prefix
      (physical-standby-backup-check-prefix
       (physical-recovery-read-base-overlap *physical-recovery-state* *pgdata*) snapshot)))
  (let ((root (physical-store-retained-root store))
        (directory (physical-store-retained-directory store))
        (identity (physical-store-retained-identity store))
        (history (physical-store-retained-history store)))
    (unless root (error "standby checkpoint has no selected retained WAL root"))
    (call-with-physical-retained-root-reader
     (lambda ()
       (let* ((found nil)
              (index (physical-retained-inventory-load
                      directory root identity history
                      :on-record (lambda (record raw)
                                   (declare (ignore raw))
                                   (when (= (wal-record-start record) (getf snapshot :start))
                                     (setf found t)))))
              (entry (find-if
                      (lambda (item)
                        (let ((metadata (third item)))
                          (<= (getf metadata :start) (getf snapshot :start)
                              (getf snapshot :end) (getf metadata :end))))
                      (getf index :entries))))
         (physical-store-check-retained-coverage store (getf index :entries))
         (unless (and found entry) (error "source checkpoint is absent from retained WAL"))
         (multiple-value-bind (metadata bytes)
             (physical-retained-extent-decode
              (physical-retained-object-read directory :extent (first entry)
                                             +physical-retained-image-limit+))
           (unless (equal metadata (third entry))
             (error "standby checkpoint extent changed after validation"))
           (physical-standby-backup-check-prefix
            (subseq bytes 0 (1+ (mod (1- (getf snapshot :end)) +wal-segment-size+))) snapshot))))
     (physical-store-retained-owner store) directory identity root)))

(defun physical-standby-backup-write-bytes (relative target bytes)
  (let ((path (merge-pathnames relative target)))
    (wal-ensure-directory-durable (uiop:pathname-directory-pathname path))
    (wal-atomic-write-bytes path bytes)
    (physical-bootstrap-test-point :after-file target)
    (list relative (length bytes) (physical-store-update-digest #xCBF29CE484222325 bytes))))

(defun physical-standby-backup-logical-facts (snapshot)
  (let* ((end (getf snapshot :end)) (timeline (getf snapshot :timeline))
                                    (current (physical-logical-copy-fact
                                              (getf *physical-recovery-state* :source-logical-fact)))
                                    (history (when (or current (get-properties snapshot '(:logical-decoding-enabled)))
                                               (physical-recovery-resource-history
                                                *physical-receive-store* *physical-recovery-state*))))
    (when current (physical-logical-check-fact current history end timeline))
    (setf current (physical-recovery-scan-checkpoint-logical snapshot current end timeline))
    (let* ((anchor
             (when (and current
                        (or (not (get-properties snapshot '(:logical-decoding-enabled)))
                            (and (getf current :logical-on-start-lsn)
                                 (null (getf snapshot :logical-on-start-lsn)))))
               (physical-logical-encode-base-anchor current)))
           (manifest (append (list :end end :timeline timeline)
                             (when anchor (list :logical-base-anchor anchor)))))
      (values (physical-logical-base-fact manifest snapshot history) anchor))))

(defun physical-standby-backup-copy (target label snapshot prefix policy)
  "Copy frozen checkpoint data and derive the target controls from SNAPSHOT.
Mark TARGET first and publish its manifest last, after checking the copied
catalog; a failed copy remains guarded.  LABEL is the manifest export label
and creates no upstream WAL retention.  Resumed follow requires an independently
retained upstream slot or equivalent retention."
  (multiple-value-bind (logical anchor) (physical-standby-backup-logical-facts snapshot)
    (let* ((end (getf snapshot :end)) (timeline (getf snapshot :timeline))
                                      (system-id (catalog-system-identifier))
                                      (expected-catalog (catalog-image-bytes))
                                      (metadata (append (make-wal-control-metadata system-id +db-in-archive-recovery+)
                                                        (wal-copy-control-resource-fields snapshot)))
                                      (wal-name (concatenate 'string "pg_wal/"
                                                             (wal-segment-filename (floor (getf snapshot :start) +wal-segment-size+)
                                                                                   timeline :timeline))))
      (physical-bootstrap-mark-target target)
      (wal-atomic-write-bytes
       (physical-recovery-control-path target)
       (apply #'physical-recovery-control-image
              0 system-id timeline 0 end end (getf snapshot :next-xid)
              :resource-fact (wal-copy-control-resource-fields snapshot)
              (when logical (list :logical-fact logical))))
      (wal-ensure-directory-durable (merge-pathnames "base/" target))
      (authentication-write-backup-policy policy target)
      (let* ((*wal-durable-lsn* end) (*wal-insert-timeline* timeline)
                                     (*wal-segment-namespace* :timeline)
                                     (files
                                       (mapcar
                                        (lambda (name)
                                          (cond
                                            ((equal name wal-name) (physical-standby-backup-write-bytes name target prefix))
                                            ((equal name "global/pg_control")
                                             (physical-standby-backup-write-bytes
                                              name target (string->bytes (format nil "~d~%" (1+ (getf snapshot :xid-reserved-through))))))
                                            ((equal name "global/pg_wal_control")
                                             (physical-standby-backup-write-bytes
                                              name target (wal-encode-control-image end end snapshot :timeline timeline
                                                                                                     :checkpoint-timeline timeline :namespace :timeline
                                                                                                     :metadata metadata)))
                                            (t (physical-bootstrap-copy-file name target))))
                                        (physical-bootstrap-inventory (getf snapshot :start))))
                                     (manifest (list :version 3 :system-id system-id
                                                     :cluster-identity (catalog-cluster-identity *catalog*) :timeline timeline
                                                     :checkpoint end :checkpoint-start (getf snapshot :start) :end end
                                                     :next-xid (getf snapshot :next-xid) :source-pin label
                                                     :files files :auth-policy-required 1)))
        (when anchor (setf manifest (append manifest (list :logical-base-anchor anchor))))
        (recovery-check-manifest-schema manifest)
        (let ((catalog (recovery-check-image-metadata manifest target)))
          (unless (and (= (getf snapshot :next-oid) (catalog-next-oid catalog))
                       (equalp expected-catalog
                               (recovery-read-bytes (merge-pathnames "global/pg_catalog.dat" target)
                                                    :maximum (length expected-catalog))))
            (error "standby copied catalog differs from its frozen checkpoint state")))
        (physical-bootstrap-test-point :before-manifest target)
        (wal-atomic-write-bytes (merge-pathnames "physical-backup.manifest" target)
                                (physical-bootstrap-manifest-image manifest))
        (fsync-directory target)
        (physical-bootstrap-test-point :after-manifest target)
        manifest))))

(defun physical-standby-base-backup (directory label)
  "Export a local, quiescent standby at its exact applied source checkpoint.
The reload lock and owned quiescence gate exclude reload, replay and mutations
while the checkpoint is proved and the data copied.  This is not an online
backup or a cascading wire clone.  Unlike PostgreSQL's online standby backup,
full_page_writes may be off because source data files stay unchanged during copy."
  (when (plusp *live-operation-depth*)
    (error "standby export must start outside a live operation"))
  (unless (wal-valid-replication-slot-name-p label) (error "invalid standby export label"))
  (sb-thread:with-mutex (*reload-run-lock*)
    (with-live-operation-quiescence
      (let* ((snapshot (physical-standby-backup-checkpoint))
             (target (physical-bootstrap-target directory))
             (policy (authentication-capture-policy)))
        (physical-recovery-restartpoint-core)
        (multiple-value-bind (prefix checked)
            (with-physical-store (*physical-receive-store*)
              (physical-standby-backup-prefix *physical-receive-store* snapshot))
          (physical-standby-backup-copy target label checked prefix policy))))))
