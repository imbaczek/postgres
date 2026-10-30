;;;; Native standby redo; the base checkpoint remains the permanent restart floor.
(in-package #:pg)

(defvar *physical-replay-test-hook* nil)

(defun physical-resource-fact-next (prior record timeline history)
  "Derive the fact at a terminal without changing replay state.
A parameter change replaces it; a known checkpoint must agree with a known prior fact.
An unknown checkpoint preserves the last reported fact.
Check the fact's origin against the selected history."
  (when prior
    (unless (and (wal-resource-fact-valid-p prior)
                 (<= (getf prior :resource-lsn) (wal-record-start record)))
      (error "resource seed belongs to a later replay position")))
  (let* ((type (wal-record-type record))
         (candidate
           (cond
             ((= type +wal-record-parameter-change+)
              (list :resource-parameters (resource-parameters-values (wal-parameter-change-record-info record))
                    :resource-lsn (wal-record-end record) :resource-timeline timeline))
             ((= type +wal-record-checkpoint+)
              (let ((fact (wal-copy-control-resource-fields (wal-checkpoint-record-snapshot record))))
                (when (and fact prior (not (equal fact prior)))
                  (error "source checkpoint resources differ from preceding WAL"))
                (or fact prior)))
             (t prior))))
    (when candidate
      (unless (<= (getf candidate :resource-timeline) timeline)
        (error "resource origin is newer than the replay timeline"))
      (timeline-check-resource-origin candidate (if (functionp history) (funcall history) history)))))

(defun physical-recovery-resource-history (store state)
  (let ((selection (and store (physical-store-selection store))))
    (if selection (getf selection :history)
        (timeline-read-history (getf state :timeline)))))

(defun physical-recovery-scan-resource-fact (store start end seed state)
  "Reconstruct a fact through complete terminals in a durable prefix without redo.
The seed belongs to START; selected end-of-recovery records advance the timeline."
  (unless (<= (physical-store-floor-lsn store) start end (physical-store-flushed-lsn store))
    (error "resource proof prefix is unavailable; take a fresh base backup"))
  (let ((fact (wal-copy-control-resource-fields seed))
        (timeline (physical-store-prefix-timeline store start))
        (terminal start)
        (*physical-recovery-state* state)
        (*physical-history-reset-forks* nil)
        (removed (make-hash-table :test #'eql)))
    (when fact
      (unless (<= (getf fact :resource-lsn) start)
        (error "resource seed is newer than its replay position"))
      (timeline-check-resource-origin fact (physical-recovery-resource-history store state)))
    (if (= start end)
        fact
        (with-physical-replay-staging ()
          (block scan
            (physical-recovery-map-permitted-records
             store
             (lambda (record raw)
               (when (> (wal-record-end record) end)
                 (error "resource proof boundary is not a complete terminal"))
               (if (= (wal-record-type record) +wal-record-end-of-recovery+)
                   (setf timeline (physical-replay-check-selected-eor store record raw terminal timeline))
                   (physical-history-accept-record record (getf state :cluster-identity) removed))
               (unless *wal-recovery-group-active*
                 (unless (= (wal-record-type record) +wal-record-end-of-recovery+)
                   (setf fact (physical-resource-fact-next
                               fact record timeline
                               (lambda () (physical-recovery-resource-history store state)))))
                 (setf terminal (wal-record-end record))
                 (when (= terminal end)
                   (return-from scan (wal-copy-control-resource-fields fact)))))
             :start-lsn start :policy (getf state :timeline-policy))
            (error "resource proof did not reach its required boundary; take a fresh base backup"))))))

(defun physical-recovery-spool-directory ()
  (merge-pathnames "pg_physical_receive/" *pgdata*))

(defun physical-recovery-check-base-forks (manifest removed &optional reset-forks)
  "Require live base forks, allowing shrink only with an exact durable reset proof.
A reset proof permits a shorter existing file for that relation and fork; it
does not excuse a missing file or a different fork."
  (dolist (entry (getf manifest :files))
    (let ((name (first entry)))
      (when (uiop:string-prefix-p "base/" name)
        (let* ((node (parse-integer name :start 5 :junk-allowed t))
               (fork (if (string= (or (pathname-type name) "") "vm") :visibility :main)))
          (unless (gethash node removed)
            (let ((path (merge-pathnames name *pgdata*)))
              (unless (probe-file path)
                (error "still-live base relation fork is missing: ~a" name))
              (with-open-file (stream path :element-type '(unsigned-byte 8))
                (unless (or (>= (file-length stream) (second entry))
                            (and reset-forks (gethash (list node fork) reset-forks)))
                  (error "still-live base relation fork is truncated: ~a" name))))))))))

(defun physical-recovery-check-streaming-base (manifest control)
  "Validate unchanged provenance; changed relation/CLOG/catalog bytes require redo."
  (unless (and (<= (getf manifest :end) (getf control :applied))
               (<= (getf manifest :next-xid) (getf control :next-xid))
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
  (dolist (name (physical-bootstrap-inventory (physical-manifest-checkpoint-start *physical-recovery-state*)))
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

(defun physical-recovery-save-progress (applied next-xid
                                        &key (replay-timeline (getf *physical-recovery-state* :replay-timeline
                                                                    (getf *physical-recovery-state* :timeline)))
                                          (resource-fact (getf *physical-recovery-state* :source-resource-fact)))
  "Publish the replay position and its source resource fact together."
  (let ((candidate (copy-list *physical-recovery-state*)))
    (setf (getf candidate :replay-timeline) replay-timeline)
    (when (or resource-fact (getf candidate :source-resource-fact))
      (setf (getf candidate :source-resource-fact) (wal-copy-control-resource-fields resource-fact)))
    (setf *physical-recovery-state* (physical-recovery-write-control candidate 2 applied next-xid))))

(defun physical-recovery-retained-directory ()
  "Return the recovery archive outside the receive spool."
  (merge-pathnames "pg_physical_retained/" *pgdata*))

(defun physical-recovery-retained-identity (&optional (state *physical-recovery-state*))
  "Build the receive inventory identity from verified recovery base state."
  (list :system-id (getf state :system-id) :base-timeline (getf state :timeline)
        :base (getf state :end) :manifest-digest (getf state :manifest-digest)))

(defun physical-recovery-open-receive-store (&key create)
  (let ((directory (physical-recovery-spool-directory)))
    (when create
      (wal-ensure-directory-durable directory))
    (physical-store-open directory (getf *physical-recovery-state* :system-id)
                         (getf *physical-recovery-state* :timeline)
                         :create create :start-lsn (getf *physical-recovery-state* :end)
                         :retained-identity (physical-recovery-retained-identity)
                         :retained-directory (physical-recovery-retained-directory))))

(defun physical-recovery-open-clog-redo ()
  ;; Open once when streaming starts. Keep the old stream if opening fails.
  (sb-thread:with-mutex (*clog-lock*)
    (let ((replacement nil) (installed nil) (previous *clog-stream*))
      (unwind-protect
           (sb-sys:without-interrupts
             (setf replacement (open (clog-path) :direction :io
                                                 :element-type '(unsigned-byte 8)
                                                 :if-exists :overwrite :if-does-not-exist :error))
             (setf *clog-stream* replacement installed t)
             (when previous (close previous)))
        (when (and replacement (not installed)) (close replacement)))))
  (values))

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
                 (physical-recovery-open-clog-redo)
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
      ((= (wal-record-type record) +wal-record-running-xacts+)
       (setf next-xid (max next-xid (getf (wal-running-xacts-record-info record) :next-xid))))
      ((= (wal-record-type record) +wal-record-checkpoint+)
       (let ((snapshot (wal-checkpoint-record-snapshot record)))
         (when snapshot
           (setf next-xid (max next-xid (getf snapshot :next-xid))))))
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

(defun physical-source-reservation-valid-p (state)
  (and (listp state) (= (length state) 8)
       (equal (loop for (key value) on state by #'cddr collect key)
              '(:version :through :bound :complete))
       (eql 1 (getf state :version))
       (typep (getf state :through) '(unsigned-byte 64))
       (wal-xid-reservation-valid-bound-p (getf state :bound))
       (member (getf state :complete) '(nil t))))

(defun physical-recovery-initial-reservation (manifest)
  "Only after immutable base verification; old source formats do not prove coverage."
  (let* ((end (getf manifest :end))
         (record (wal-read-checkpoint-record (physical-manifest-checkpoint-start manifest) end))
         (bound (1- (canonical-next-full-xid (load-next-xid))))
         (complete (>= (wal-record-version record) 7)))
    (when complete
      (let ((snapshot (wal-checkpoint-record-snapshot record)))
        (unless (and (= bound (getf snapshot :xid-reserved-through))
                     (= (getf manifest :next-xid) (getf snapshot :next-xid)))
          (error "base XID counters differ from checkpoint"))))
    (list :version 1 :through end :bound bound :complete complete)))

(defun physical-source-reservation-next (prior record observed-next)
  "Track source capacity separately from reader visibility; no promotion authority.
EOR keeps the proven bound but clears completeness. A child checkpoint must
restore proof before promotion; observed next XID must stay within that bound."
  (when (and prior (not (physical-source-reservation-valid-p prior)))
    (error "invalid source allocation frontier"))
  (when (and prior (> (getf prior :through) (wal-record-start record)))
    (error "source allocation history moved backwards"))
  (let ((bound (if prior (getf prior :bound) 2))
        (complete (and prior (getf prior :complete)))
        (type (wal-record-type record))
        (version (wal-record-version record)))
    (when (< version 7) (setf complete nil))
    (cond
      ((= type +wal-record-end-of-recovery+) (setf complete nil))
      ((= type +wal-record-xid-reservation+)
       (let ((new-bound (wal-xid-reservation-record-bound record)))
         (when (< new-bound bound) (error "source XID reservation regressed"))
         (setf bound new-bound complete t)))
      ((= type +wal-record-checkpoint+)
       (let ((snapshot (wal-checkpoint-record-snapshot record)))
         (when snapshot
           (let ((new-bound (1- (canonical-next-full-xid
                                 (1+ (getf snapshot :xid-reserved-through))))))
             (when (and (>= version 7) (< new-bound bound))
               (error "checkpoint XID reservation regressed"))
             (setf bound (max bound new-bound))))
         (when (>= version 7) (setf complete t)))))
    (when (and complete (> observed-next (1+ bound)))
      (error "observed XID exceeds the source's durable allocation reservation"))
    (list :version 1 :through (wal-record-end record) :bound bound :complete complete)))

;;; Compute prospective membership for each record in WAL order.  An anchor
;;; replaces prior coverage; a reset or missing prior state leaves it unknown.
;;; The set is conservative: aborted transactions can remain because native
;;; ABORT has no WAL record.  Between anchors, only an ordered commit removes
;;; its XID.
;;; This result is not published until its batch reaches an applied terminal.

(defun physical-source-xids-next (prior record observed-next)
  "Track prospective source membership independently of existing reader snapshots.
EOR clears this proof. A later running-transaction anchor can establish it again."
  (when prior
    (running-xacts-check-state prior)
    (when (> (getf prior :through) (wal-record-start record))
      (error "source transaction history moved backwards")))
  (let ((type (wal-record-type record)) (end (wal-record-end record)))
    (cond
      ((= type +wal-record-running-xacts+)
       (let ((snapshot (wal-running-xacts-record-info record)))
         (unless (and (= observed-next (getf snapshot :next-xid))
                      (or (null prior) (<= (getf prior :next-xid) observed-next)))
           (error "source running snapshot differs from observed actual next XID"))
         (running-xacts-anchor snapshot end)))
      ((member type (list +wal-record-clog-reset+ +wal-record-end-of-recovery+)) nil)
      ((null prior) nil)
      (t
       (let ((next (running-xacts-advance
                    prior observed-next end
                    :committed-xid (when (= type +wal-record-commit+) (wal-record-xid record)))))
         (when (< (wal-record-version record) 7)
           (setf (getf next :complete) nil (getf next :running) nil))
         next)))))

(defun physical-replay-create-heap-forks (catalog)
  "Redo first appearances of parent and TOAST heap main forks at catalog terminals.
The TOAST heap is a separate relation, as is its index (see PostgreSQL's
catalog/toasting.c).  This catalog-driven replay creates heap forks only; it
must not synthesize an index or its contents.  PostgreSQL's catalog/storage.c
logs main-fork creation separately; here catalog history supplies the heap's
first-appearance evidence, including for an empty heap with no page WAL.

During streaming replay a missing previously seen heap is an error, not a new
empty fork.  Full restart resets seen history to the base inventory and replays
retained catalog/page WAL from the base; missing seen forks are tolerated during
that reconstruction, and the final live-file check must still reject any missing
referenced fork."
  (unless *physical-replay-seen-relfilenodes* (error "missing physical replay relation history"))
  (labels ((replay-heap (relfilenode)
             (let ((seen (gethash relfilenode *physical-replay-seen-relfilenodes*)))
               (unless (probe-file (relation-file-path relfilenode))
                 (cond ((not seen) (smgr-create relfilenode :redo t))
                       ((not *physical-replay-restarting-p*)
                        (error "previously replayed heap storage is missing: ~d" relfilenode))))
               (setf (gethash relfilenode *physical-replay-seen-relfilenodes*) t))))
    (maphash
     (lambda (name database)
       (declare (ignore name))
       (maphash
        (lambda (name table)
          (declare (ignore name))
          (replay-heap (pg-class-relfilenode table))
          (let ((toast (pg-class-toast table)))
            (when toast (replay-heap (pg-toast-relfilenode toast)))))
        (database-tables database)))
     (catalog-databases catalog))))

;;; Publish the terminal's source membership together with actual next XID and
;;; replay LSN under the XID lock, after data and control progress are durable.
;;; Copy membership so the prospective replay state remains independently owned.

(defun physical-replay-publish-progress (record next-xid reservation source-xids &key replay-timeline
                                                                                   (resource-fact (getf *physical-recovery-state* :source-resource-fact)))
  "Make redo durable before publishing the applied terminal and visibility state.
Source allocation capacity stays separate from the observed next XID.
Updating source membership does not change existing snapshots."
  ;; Make redo durable before recording the applied terminal and visibility.
  (smgr-fsync-all)
  (clog-fsync)
  (fsync-directory (merge-pathnames "base/" *pgdata*))
  (fsync-directory (merge-pathnames "global/" *pgdata*))
  (apply #'physical-recovery-save-progress (wal-record-end record) next-xid
         (append (when replay-timeline (list :replay-timeline replay-timeline))
                 (when (or resource-fact (getf *physical-recovery-state* :source-resource-fact))
                   (list :resource-fact resource-fact))))
  (sb-thread:with-mutex (*xid-lock*)
    (setf (getf *physical-recovery-state* :source-xids) (copy-tree source-xids)
          (getf *physical-recovery-state* :source-reservation) (copy-list reservation)
          *next-xid* next-xid *xid-batch-end* next-xid
          *last-replay-lsn* (wal-record-end record))
    (when (= (wal-record-type record) +wal-record-checkpoint+)
      (setf (getf *physical-recovery-state* :source-checkpoint)
            (wal-checkpoint-record-snapshot record)))))

(defun physical-replay-publish-terminal (record next-xid reservation source-xids
                                         &key (resource-fact (getf *physical-recovery-state* :source-resource-fact)))
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
        (dolist (node (catalog-physical-relfilenodes))
          (setf (gethash node *physical-replay-seen-relfilenodes*) t))))
    (unless *physical-replay-restarting-p*
      (let ((*catalog* catalog)) (physical-recovery-check-live-files)))
    (if (or resource-fact (getf *physical-recovery-state* :source-resource-fact))
        (physical-replay-publish-progress record next-xid reservation source-xids :resource-fact resource-fact)
        (physical-replay-publish-progress record next-xid reservation source-xids))
    (setf *catalog* catalog)
    (catalog-refresh-oldest-relfrozenxid)
    (wal-clear-page-cache))
  (when *physical-replay-test-hook*
    (funcall *physical-replay-test-hook* :after-progress record)))

(declaim (ftype function physical-replay-selected-eor physical-replay-check-child-initialization))

(defun physical-replay-from (start-lsn)
  "Called only before listeners or under operation quiescence, with no old readers."
  (physical-recovery-control-frontier *physical-recovery-state* start-lsn)
  (with-physical-replay-staging ()
    (let ((next-xid *next-xid*)
          (resource-fact (wal-copy-control-resource-fields (getf *physical-recovery-state* :source-resource-fact)))
          (resource-history (lambda ()
                              (physical-recovery-resource-history
                               *physical-receive-store* *physical-recovery-state*)))
          (source-xids (copy-tree (getf *physical-recovery-state* :source-xids)))
          (reservation (copy-list (getf *physical-recovery-state* :source-reservation))))
      (physical-recovery-note-policy-exhaustion
       (physical-recovery-map-permitted-records
        *physical-receive-store*
        (lambda (record raw)
          (when *physical-replay-test-hook*
            (funcall *physical-replay-test-hook* :before-record record))
          (if (= (wal-record-type record) +wal-record-end-of-recovery+)
              (multiple-value-setq (reservation source-xids)
                (physical-replay-selected-eor record raw next-xid reservation source-xids))
              (progn
                (physical-replay-check-child-initialization record next-xid)
                (setf next-xid (physical-replay-observed-next-xid record next-xid)
                      reservation (physical-source-reservation-next reservation record next-xid)
                      source-xids (physical-source-xids-next source-xids record next-xid))
                (let ((candidate (when (wal-terminal-record-type-p (wal-record-type record))
                                   (physical-resource-fact-next
                                    resource-fact record (physical-recovery-replay-timeline) resource-history))))
                  (wal-replay-record record)
                  (unless *wal-recovery-group-active*
                    (setf resource-fact candidate)
                    (if (or resource-fact (getf *physical-recovery-state* :source-resource-fact))
                        (physical-replay-publish-terminal
                         record next-xid reservation source-xids :resource-fact resource-fact)
                        (physical-replay-publish-terminal record next-xid reservation source-xids)))))))
        :start-lsn start-lsn) *physical-receive-store*)))
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
      (when (and *physical-promotion-stopping-p*
                 (member name '("pg_wal_replay_pause" "pg_wal_replay_resume") :test #'string=))
        (ereport "55000" "standby promotion is ongoing"
                 :hint (format nil "~a() cannot be executed after promotion is triggered." name)))
      ;; Native redo runs exclusively behind this admission gate. A request is
      ;; already at a complete applied boundary and can acknowledge PAUSED here.
      (cond
        ((string= name "pg_wal_replay_pause") (setf *physical-replay-paused-p* t) "")
        ((string= name "pg_wal_replay_resume") (setf *physical-replay-paused-p* nil) "")
        ((string= name "pg_is_wal_replay_paused") *physical-replay-paused-p*)
        ((string= name "pg_get_wal_replay_pause_state")
         (if *physical-replay-paused-p* "paused" "not paused"))
        (t (error "unknown replay control function: ~a" name))))))

(defun physical-recovery-replay-core ()
  "Replay complete durable batches while owning a closed admission gate.
Require no old reader transactions or Swank listener. The public replay command
checks pause first; promotion clears pause before calling this core. CLOG is
already writable; fence any error or nonlocal exit once replay starts."
  (unless (and (zerop *live-operation-depth*)
               (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)
               (not *live-operation-drain-open*) (not *swank-listener-enabled-p*)
               (not (physical-recovery-reader-conflict-p)))
    (error "native replay requires owned quiescence without readers"))
  (check-physical-recovery-work)
  (unless *physical-receive-store* (error "no native physical receiver spool"))
  (let ((finished nil))
    (unwind-protect
         (progn
           (unless (and *clog-stream* (output-stream-p *clog-stream*))
             (error "recovery CLOG is not open for replay"))
           (prog1 (physical-replay-from *last-replay-lsn*) (setf finished t)))
      (unless finished (setf *physical-recovery-failed-p* t)))))

(declaim (ftype function physical-replay-with-readers))

(defun physical-recovery-replay ()
  "Apply durable batches under closed admission, deferring an unsafe reader prefix."
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
      (if (physical-recovery-reader-conflict-p)
          (physical-replay-with-readers)
          (physical-recovery-replay-core)))))
