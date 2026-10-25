;;;; Native standby redo; the base checkpoint remains the permanent restart floor.
(in-package #:pg)

(defvar *physical-replay-test-hook* nil)

(defun physical-recovery-spool-directory ()
  (merge-pathnames "pg_physical_receive/" *pgdata*))

(defun physical-recovery-check-base-forks (manifest removed)
  "Require every original fork unless complete durable WAL proves its deletion.
Native same-identity shrinking is not implemented. The mutable disk catalog
can be ahead after a crash and never grants a missing-file exemption."
  (dolist (entry (getf manifest :files))
    (let ((name (first entry)))
      (when (and (uiop:string-prefix-p "base/" name)
                 (not (gethash (parse-integer name :start 5 :junk-allowed t) removed)))
        (let ((path (merge-pathnames name *pgdata*)))
          (unless (probe-file path)
            (error "still-live base relation fork is missing: ~a" name))
          (with-open-file (stream path :element-type '(unsigned-byte 8))
            (unless (>= (file-length stream) (second entry))
              (error "still-live base relation fork is truncated: ~a" name))))))))

(defun physical-recovery-check-streaming-base (manifest control)
  "Validate unchanged provenance; changed relation/CLOG/catalog bytes require redo."
  (unless (and (<= (getf manifest :end) (getf control :applied))
               (<= (load-next-xid) (getf control :next-xid))
               (probe-file (clog-path))
               (probe-file (catalog-path))
               (not (probe-file (xid-wrap-marker-path))))
    (error "streaming recovery lost its required base metadata"))
  ;; These files are never changed by standby redo. Their checksums still bind
  ;; the original base, while mutable files are validated/repaired by redo.
  (let ((immutable (copy-list manifest)))
    (setf (getf immutable :files)
          (remove-if-not
           (lambda (entry)
             (or (member (first entry) '("PG_VERSION" "global/pg_control" "global/pg_wal_control")
                         :test #'equal)
                 (uiop:string-prefix-p "pg_wal/" (first entry))))
           (getf manifest :files)))
    (recovery-check-manifest-files immutable *pgdata*))
  (let ((*catalog-identity-needs-persist-p* nil)
        (catalog (load-catalog)))
    (unless (equal (catalog-cluster-identity catalog) (getf manifest :cluster-identity))
      (error "streaming catalog has a foreign identity")))
  (values))

(defun physical-recovery-check-live-files ()
  "After redo, every currently referenced main fork must exist; never synthesize it."
  (dolist (name (physical-bootstrap-inventory))
    (unless (probe-file (merge-pathnames name *pgdata*))
      (error "replayed catalog references missing durable file ~a" name))))

(defun physical-recovery-reset-replay-history ()
  (setf *physical-replay-seen-relfilenodes* (make-hash-table :test #'eql))
  (dolist (entry (getf *physical-recovery-state* :files))
    (when (uiop:string-prefix-p "base/" (first entry))
      (setf (gethash (parse-integer (first entry) :start 5 :junk-allowed t)
                     *physical-replay-seen-relfilenodes*) t))))

(defun physical-recovery-reader-conflict-p ()
  "Call only after operation drain. Do not wait for idle transactions in the gate."
  (or (not (active-snapshots-empty-p))
      (some (lambda (backend)
              (member (backend-state backend) '(:in-progress :aborted)))
            (list-backends))))

(defun physical-recovery-save-progress (applied next-xid)
  (let ((state *physical-recovery-state*))
    (wal-atomic-write-bytes
     (physical-recovery-control-path)
     (physical-recovery-control-image
      2 (getf state :system-id) (getf state :timeline)
      (getf state :manifest-digest) (getf state :checkpoint)
      (max applied (or (getf state :required-applied) applied))
      (max next-xid (or (getf state :observed-next-xid) next-xid))))))

(defun physical-recovery-open-receive-store (&key create)
  (let ((directory (physical-recovery-spool-directory)))
    (when create
      (wal-ensure-directory-durable directory))
    (physical-store-open directory (getf *physical-recovery-state* :system-id)
                         (getf *physical-recovery-state* :timeline)
                         :create create :start-lsn (getf *physical-recovery-state* :end))))

(defun physical-recovery-enable-streaming ()
  "Enter restartable native receive/replay, retaining all WAL from the base.
Direct control operation; does not promote, follow a network source or coordinate."
  (check-physical-recovery-worker-start)
  (unless (recovery-in-progress-p) (error "streaming requires a verified recovery base"))
  (when (plusp *live-operation-depth*) (error "streaming setup requires its own admission gate"))
  (sb-thread:with-mutex (*reload-run-lock*)
    (with-live-operation-quiescence
      (check-physical-recovery-worker-start)
      (when *swank-listener-enabled-p* (error "streaming setup requires Swank to be off"))
      (when (physical-recovery-reader-conflict-p) (return-from physical-recovery-enable-streaming :deferred))
      (unless *physical-receive-store*
        ;; A process may have stopped after spool creation but before phase 2.
        ;; Reuse only an empty, valid spool with this exact base and identity.
        (let* ((existing (probe-file (merge-pathnames "receive.control"
                                                      (physical-recovery-spool-directory))))
               (store (physical-recovery-open-receive-store :create (not existing)))
               (success nil))
          (unwind-protect
               (progn
                 (unless (= (physical-store-base-lsn store) (physical-store-flushed-lsn store)
                            (getf *physical-recovery-state* :end))
                   (error "unattached receive spool is not an empty base continuation"))
                 ;; Persist the role transition before reopening CLOG for redo.
                 (physical-recovery-save-progress *last-replay-lsn* *next-xid*)
                 (physical-recovery-reset-replay-history)
                 (setf *physical-receive-store* store success t
                       (getf *physical-recovery-state* :phase) 2))
            (unless success (physical-store-close store)))))
      (physical-restartpoint-start-worker)
      (getf *physical-recovery-state* :phase))))

(defun physical-recovery-receive (start-lsn bytes)
  (check-physical-recovery-work)
  (unless *physical-receive-store* (error "no native physical receiver spool"))
  (physical-store-input *physical-receive-store* start-lsn bytes))

(defun physical-recovery-flush ()
  (check-physical-recovery-work)
  (unless *physical-receive-store* (error "no native physical receiver spool"))
  (let ((flush (physical-store-flush *physical-receive-store*)))
    (setf (getf *physical-recovery-state* :receive-lsn) flush)
    flush))

(defun physical-replay-observed-next-xid (record next-xid)
  "Maintain read visibility, not a promotion-safe source allocation reservation."
  (flet ((observe (xid)
           (when (>= xid +first-normal-xid+)
             (setf next-xid (max next-xid (advance-full-xid xid))))))
    (observe (wal-record-xid record))
    (cond
      ((= (wal-record-type record) +wal-record-pages+)
       (unless (>= (wal-record-version record) +wal-version-v4+)
         (error "streaming redo requires FullXID contributor metadata"))
       (dolist (image (wal-decode-page-images (wal-record-payload record)
                                              (wal-record-end record)
                                              (wal-record-version record)))
         (dolist (xid (wal-page-image-xids image)) (observe xid))))
      ((= (wal-record-type record) +wal-record-clog-reset+)
       (setf next-xid (max next-xid (wal-read-u64-payload (wal-record-payload record))))))
    next-xid))

(defun physical-replay-create-heap-forks (catalog)
  "Native catalog terminals redo a heap's first appearance in catalog history.
Never recreate a previously established live heap or synthesize index contents.
Full restart replay resets history to the base inventory before reconstructing
all post-base storage, including empty tables, from retained catalog/page WAL."
  (unless *physical-replay-seen-relfilenodes* (error "missing physical replay relation history"))
  (maphash
   (lambda (name database)
     (declare (ignore name))
     (maphash
      (lambda (name table)
        (declare (ignore name))
        (let* ((relfilenode (pg-class-relfilenode table))
               (seen (gethash relfilenode *physical-replay-seen-relfilenodes*)))
          (unless (probe-file (relation-file-path relfilenode))
            (cond ((not seen) (smgr-create relfilenode :redo t))
                  ((not *physical-replay-restarting-p*)
                   (error "previously replayed heap storage is missing: ~d" relfilenode))))
          (setf (gethash relfilenode *physical-replay-seen-relfilenodes*) t)))
      (database-tables database)))
   (catalog-databases catalog)))

(defun physical-replay-publish-terminal (record next-xid)
  "Durably finish pages/CLOG/catalog before publishing an applied terminal."
  (when *physical-replay-test-hook*
    (funcall *physical-replay-test-hook* :after-redo record))
  (let* ((*catalog-identity-needs-persist-p* nil)
         (catalog (load-catalog)))
    (unless (and (not *catalog-identity-needs-persist-p*)
                 (equal (catalog-cluster-identity catalog)
                        (getf *physical-recovery-state* :cluster-identity)))
      (error "replayed catalog lost the source cluster identity"))
    (when (member (wal-record-type record)
                  (list +wal-record-catalog+ +wal-record-catalog-commit+))
      (physical-replay-create-heap-forks catalog)
      (let ((*catalog* catalog))
        (dolist (node (wal-relation-filennodes))
          (setf (gethash node *physical-replay-seen-relfilenodes*) t))))
    (unless *physical-replay-restarting-p*
      (let ((*catalog* catalog)) (physical-recovery-check-live-files)))
    (smgr-fsync-all)
    (clog-fsync)
    (fsync-directory (merge-pathnames "base/" *pgdata*))
    (fsync-directory (merge-pathnames "global/" *pgdata*))
    (physical-recovery-save-progress (wal-record-end record) next-xid)
    (setf *catalog* catalog
          *next-xid* next-xid
          *xid-batch-end* next-xid
          *last-replay-lsn* (wal-record-end record))
    (catalog-refresh-oldest-relfrozenxid)
    (wal-clear-page-cache))
  (when *physical-replay-test-hook*
    (funcall *physical-replay-test-hook* :after-progress record)))

(defun physical-replay-from (start-lsn)
  "Called only before listeners or under operation quiescence, with no old readers."
  (with-physical-replay-staging ()
    (let ((next-xid *next-xid*))
      (physical-store-map-durable-records
       *physical-receive-store*
       (lambda (record raw)
         (declare (ignore raw))
         (when *physical-replay-test-hook*
           (funcall *physical-replay-test-hook* :before-record record))
         (setf next-xid (physical-replay-observed-next-xid record next-xid))
         (wal-replay-record record)
         (unless *wal-recovery-group-active*
           (physical-replay-publish-terminal record next-xid)))
       :start-lsn start-lsn)))
  *last-replay-lsn*)

(defun physical-replay-pause-command (name)
  "Control replay under admission; native redo cannot overlap an admitted caller."
  (with-live-operation
    (check-recovery-available)
    (when (member name '("pg_wal_replay_pause" "pg_wal_replay_resume") :test #'string=)
      (check-physical-recovery-work))
    (unless (recovery-in-progress-p)
      (ereport "55000" "recovery is not in progress"))
    (sb-thread:with-mutex (*physical-replay-pause-lock*)
      ;; Unlike C's independently running startup process, native redo runs
      ;; exclusively behind this admission gate. A request is already at a
      ;; complete applied boundary, so it can acknowledge PAUSED immediately.
      (cond
        ((string= name "pg_wal_replay_pause")
         (setf *physical-replay-paused-p* t)
         "")
        ((string= name "pg_wal_replay_resume")
         (setf *physical-replay-paused-p* nil)
         "")
        ((string= name "pg_is_wal_replay_paused") *physical-replay-paused-p*)
        ((string= name "pg_get_wal_replay_pause_state")
         (if *physical-replay-paused-p* "paused" "not paused"))
        (t (error "unknown replay control function: ~a" name))))))

(defun physical-recovery-replay ()
  "Apply durable terminal batches between transactions; return :DEFERRED on conflict."
  (check-physical-recovery-work)
  (unless *physical-receive-store* (error "no native physical receiver spool"))
  (when (plusp *live-operation-depth*) (error "replay requires its own admission gate"))
  (sb-thread:with-mutex (*reload-run-lock*)
    (with-live-operation-quiescence
      (check-physical-recovery-work)
      (when *swank-listener-enabled-p* (error "native replay requires Swank to be off"))
      (sb-thread:with-mutex (*physical-replay-pause-lock*)
        (when *physical-replay-paused-p*
          (return-from physical-recovery-replay :paused)))
      (when (physical-recovery-reader-conflict-p) (return-from physical-recovery-replay :deferred))
      (let ((finished nil))
        (unwind-protect
             (progn
               (sb-thread:with-mutex (*clog-lock*)
                 (when *clog-stream* (close *clog-stream*))
                 (setf *clog-stream* (open (clog-path) :direction :io
                                                       :element-type '(unsigned-byte 8)
                                                       :if-exists :overwrite :if-does-not-exist :error)))
               (physical-replay-from *last-replay-lsn*)
               (setf finished t))
          ;; Nonlocal exits and asynchronous interrupts also fail closed.
          (unless finished (setf *physical-recovery-failed-p* t))))
      *last-replay-lsn*)))
