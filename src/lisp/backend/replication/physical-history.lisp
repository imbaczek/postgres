;;;; Read-only native catalog history validation before standby redo.
(in-package #:pg)

(defvar *physical-history-reset-forks* nil)

(defmacro with-physical-replay-staging (() &body body)
  `(let ((*wal-recovery-page-spool-enabled* t))
     (with-wal-replay-staging () ,@body)))

(defun physical-history-check-plist (value required)
  "Reject ambiguous keys before using serialized catalog entries as drop proof."
  (unless (and (listp value) (evenp (length value)))
    (error "invalid catalog history property list"))
  (let ((seen (make-hash-table :test #'eq)))
    (loop for (key item) on value by #'cddr
          do (unless (and (keywordp key) (not (gethash key seen)))
               (error "duplicate or invalid catalog history key"))
             (setf (gethash key seen) t))
    (unless (every (lambda (key) (gethash key seen)) required)
      (error "catalog history lacks required metadata")))
  value)

(defun physical-history-read-catalog (bytes)
  "Read one catalog form without evaluation or circular reader-label objects.
Native readable array syntax remains accepted, including SBCL string arrays."
  (with-standard-io-syntax
    (let ((*read-eval* nil) (*readtable* (copy-readtable nil)))
      (flet ((reject-label (stream character argument)
               (declare (ignore stream character argument))
               (error "reader labels are not allowed in catalog history")))
        (set-dispatch-macro-character #\# #\= #'reject-label)
        (set-dispatch-macro-character #\# #\# #'reject-label))
      (with-input-from-string (stream (bytes->string bytes))
        (let* ((end (gensym))
               (image (read stream nil end)))
          (unless (eq (read stream nil end) end)
            (error "catalog history has trailing forms"))
          image)))))

(defun physical-history-catalog-form-equal-p (left right)
  "Compare finite decoded catalog forms, treating strings as case-sensitive text.
Other arrays keep their shape and element type; scalars keep their types."
  ;; Compare the whole tree without rebuilding a catalog: unknown entries,
  ;; duplicates and their order remain part of the comparison.
  (cond
    ((stringp left) (and (stringp right) (string= left right)))
    ((consp left)
     (and (consp right)
          (physical-history-catalog-form-equal-p (car left) (car right))
          (physical-history-catalog-form-equal-p (cdr left) (cdr right))))
    ((arrayp left)
     (and (arrayp right) (not (stringp right))
          (equal (array-dimensions left) (array-dimensions right))
          (equal (array-element-type left) (array-element-type right))
          (eql (array-has-fill-pointer-p left) (array-has-fill-pointer-p right))
          (or (not (array-has-fill-pointer-p left))
              (= (fill-pointer left) (fill-pointer right)))
          (loop for i below (array-total-size left)
                always (physical-history-catalog-form-equal-p
                        (row-major-aref left i) (row-major-aref right i)))))
    ((or (symbolp left) (numberp left) (characterp left)) (eql left right))
    (t nil)))

(defun physical-history-catalog-images-equal-p (left right)
  "Compare complete catalog forms while allowing reader whitespace and comments.
This is an in-memory consistency check; raw images still bind WAL and R."
  ;; Read the original forms directly so reader whitespace does not turn an
  ;; otherwise identical catalog into a mismatch.
  (physical-history-catalog-form-equal-p
   (physical-history-read-catalog left) (physical-history-read-catalog right)))

(defun physical-history-catalog-relfilenodes (bytes identity)
  "Read exactly one source catalog and retain every serialized fork reference.
Do not round-trip through catalog hash tables: duplicate names must not hide
a still-referenced relfilenode when checking a terminal's delete list.
A table's optional TOAST heap and index are references too; retaining both
prevents a terminal from certifying either file as dropped while referenced.
Invalid or ambiguous catalog metadata fails closed instead of proving a drop."
  (let ((image (physical-history-read-catalog bytes))
        (live (make-hash-table :test #'eql)))
    (labels ((retain-node (node)
               (unless (typep node '(integer 1 4294967295))
                 (error "catalog history has an invalid relfilenode"))
               (setf (gethash node live) t)))
      (physical-history-check-plist image '(:cluster-identity :next-oid :databases))
      (unless (and (equal identity (getf image :cluster-identity))
                   (typep (getf image :next-oid) '(unsigned-byte 32)))
        (error "catalog history has a foreign or invalid identity"))
      (dolist (database (getf image :databases))
        (physical-history-check-plist database '(:name :tables :indexes))
        (dolist (relation (append (getf database :tables) (getf database :indexes)))
          (physical-history-check-plist relation '(:name :relfilenode))
          (retain-node (getf relation :relfilenode)))
        (dolist (table (getf database :tables))
          (let ((toast (getf table :toast)))
            (when toast
              (physical-history-check-plist toast '(:relfilenode :index-relfilenode))
              (retain-node (getf toast :relfilenode))
              (retain-node (getf toast :index-relfilenode)))))))
    live))

(defun physical-history-accept-catalog (payload identity removed)
  (multiple-value-bind (deletes bytes) (wal-decode-catalog-payload payload :borrow-image-p t)
    (let ((live (physical-history-catalog-relfilenodes bytes identity)))
      (dolist (node deletes)
        (when (or (zerop node) (gethash node live))
          (error "catalog history deletes a referenced or invalid relfilenode")))
      ;; A later reference revokes earlier deletion proof, even though the
      ;; native allocator normally never reuses a published relfilenode.
      (maphash (lambda (node present) (declare (ignore present)) (remhash node removed)) live)
      (dolist (node deletes) (setf (gethash node removed) t)))))

(defun physical-history-accept-terminal (record identity removed)
  (let ((type (wal-record-type record))
        (decoded (wal-recovery-decode-terminal record)))
    (cond
      ((= type +wal-record-commit+)
       (when *physical-history-reset-forks*
         (dolist (reset *wal-recovery-relation-resets*)
           (setf (gethash (copy-list (cdr reset)) *physical-history-reset-forks*) t))))
      ((or (= type +wal-record-catalog+) (= type +wal-record-catalog-commit+))
       (physical-history-accept-catalog decoded identity removed))))
  (wal-recovery-reset-batch nil))

(defun physical-history-validate-pages (record)
  (wal-decode-page-images (wal-record-payload record) (wal-record-end record)))

(defun physical-history-accept-record (record identity removed)
  "Validate native batch ordering and payloads without applying any storage."
  (wal-require-fpw-physical-replay-supported record)
  (wal-require-logical-status-physical-replay-supported record)
  (unless (wal-recovery-stage-batch-record record #'physical-history-validate-pages)
    (if (wal-terminal-record-type-p (wal-record-type record))
        (physical-history-accept-terminal record identity removed)
        (error "unsupported record in catalog history"))))

(declaim (ftype function physical-replay-check-selected-eor
                physical-replay-check-child-initialization physical-recovery-check-store-point))

(defun physical-recovery-resource-history (store state)
  (let ((selection (and store (physical-store-selection store))))
    (if selection (getf selection :history)
        (timeline-read-history (getf state :timeline)))))

(defun physical-recovery-scan-stage (store start state checkpoint next &key (stage-proven-p t)
                                                                         (timeline (physical-store-prefix-timeline store start)))
  "Build private replay state at START, clearing the checkpoint for an unproven stage.
CHECKPOINT and NEXT must describe that prefix, never a later checkpoint or
observed XID. A NIL checkpoint with a known NEXT can describe a child awaiting
its initialization checkpoint. Callers that already resolved the store's prefix
timeline at START may pass it to avoid repeating the history lookup."
  (let ((private (copy-tree state)))
    (unless stage-proven-p
      (setf (getf private :source-checkpoint) nil
            (getf private :replay-timeline) timeline)
      (return-from physical-recovery-scan-stage private))
    (unless (or (null next) (typep next '(integer 3 18446744073709551616)))
      (error "suffix scan has an invalid observed XID seed"))
    (if checkpoint
        (progn
          (unless (and (wal-checkpoint-snapshot-valid-p checkpoint)
                       (<= (getf checkpoint :end) start)
                       next (<= (getf checkpoint :next-xid) next))
            (error "suffix scan checkpoint and XID seeds differ from its prefix"))
          (physical-policy-check-record-owner
           (getf checkpoint :start) (getf checkpoint :end) (getf checkpoint :timeline)
           (physical-recovery-resource-history store state)))
        (unless (or next (and (= timeline (getf state :timeline))
                              (= start (getf state :end))))
          (error "suffix scan lacks a checkpoint stage at its starting position")))
    (setf (getf private :source-checkpoint) (copy-tree checkpoint)
          (getf private :replay-timeline) timeline)
    private))

(defun physical-recovery-scan-policy-seeds (store state checkpoint start timeline fpw logical)
  "Validate private policy seeds and refine them from the starting checkpoint."
  (physical-replay-check-policy-seeds
   fpw logical (when (or fpw logical) (physical-recovery-resource-history store state)) start timeline)
  (values (physical-recovery-scan-checkpoint-fpw checkpoint fpw start timeline)
          (physical-recovery-scan-checkpoint-logical checkpoint logical start timeline)))

(defun physical-replay-resolve-boundary-history (provider record resource fpw logical resource-p)
  "Acquire selected history for a boundary only when a fact needs it."
  (when (pg.physical-replay-facts:physical-replay-needs-boundary-history-p record resource fpw logical resource-p)
    (funcall provider)))

(defun physical-history-observe-record (record observed fpw-fact logical-fact identity removed)
  "Check child initialization, track source XIDs, then validate the batch record."
  (let ((next (physical-replay-check-and-observe-xid record observed fpw-fact logical-fact)))
    (physical-history-accept-record record identity removed)
    next))

(defun physical-history-cross-eor (store record raw terminal timeline &optional (starting-stage-and-xid-proven-p t))
  "Advance a selected timeline boundary in the private scan state.
A scan that cannot establish its starting stage and XID passes NIL."
  (unless starting-stage-and-xid-proven-p
    (error "WAL proof cannot cross EOR without proven starting stage and XID"))
  (setf (getf *physical-recovery-state* :replay-timeline)
        (physical-replay-check-selected-eor store record raw terminal timeline)))

(defun physical-recovery-proven-drops (store identity
                                       &key (start-lsn (physical-store-floor-lsn store))
                                         required-lsn (policy (getf *physical-recovery-state* :timeline-policy))
                                         (checkpoint-seed nil checkpoint-seed-p)
                                         (next-xid-seed nil next-xid-seed-p) fpw-seed logical-seed)
  "Certify drops and fork resets from complete terminals in validated durable WAL.
An unfinished batch grants no exemption. REQUIRED-LSN must be a complete terminal.
Checkpoint and observed XID seeds must be supplied together and prove the stage
at START-LSN. FPW and logical seeds are copied and folded privately while
validating the suffix. A stateless
parent scan can start with unknown seeds, but cannot cross EOR without a proven
stage and XID. Return the deletion set, last terminal, and committed relation/fork
resets and the first complete CLOG reset terminal, if any. Large catalog batches
may use scratch files. The scan applies no database changes."
  (let* ((state (or *physical-recovery-state*
                    (list :timeline (physical-store-timeline store) :end (physical-store-base-lsn store))))
         (timeline (physical-recovery-check-store-point store start-lsn)))
    (multiple-value-bind (checkpoint next stage-proven-p)
        (physical-recovery-scan-seeds state start-lsn checkpoint-seed checkpoint-seed-p
                                      next-xid-seed next-xid-seed-p)
      (let* ((*physical-receive-store* store)
             (*physical-recovery-state*
               (physical-recovery-scan-stage store start-lsn state checkpoint next
                                             :stage-proven-p stage-proven-p :timeline timeline))
             (observed (or next 3))
             (fpw-fact (physical-fpw-copy-fact fpw-seed))
             (logical-fact (physical-logical-copy-fact logical-seed)))
        (multiple-value-setq (fpw-fact logical-fact)
          (physical-recovery-scan-policy-seeds store state checkpoint start-lsn timeline fpw-fact logical-fact))
        (when (and (< start-lsn (physical-store-flushed-lsn store))
                   (not stage-proven-p) (/= timeline (getf state :timeline)))
          (error "drop proof lacks its child checkpoint and observed XID seeds"))
        (with-physical-replay-staging ()
          (let ((*physical-history-reset-forks* (make-hash-table :test #'equal))
                (removed (make-hash-table :test #'eql))
                (terminal start-lsn)
                (clog-reset nil)
                (required-found (eql required-lsn start-lsn)))
            (physical-recovery-map-permitted-records
             store
             (lambda (record raw)
               (if (= (wal-record-type record) +wal-record-end-of-recovery+)
                   (setf timeline (physical-history-cross-eor
                                   store record raw terminal timeline (and stage-proven-p next)))
                   (setf observed (physical-history-observe-record
                                   record observed fpw-fact logical-fact identity removed)))
               (unless *wal-recovery-group-active*
                 (multiple-value-setq (fpw-fact logical-fact)
                   (physical-replay-policy-facts-next
                    fpw-fact logical-fact record timeline
                    (physical-replay-resolve-boundary-history
                     (lambda () (physical-recovery-resource-history store state)) record nil fpw-fact logical-fact nil)))
                 (when (= (wal-record-type record) +wal-record-checkpoint+)
                   (setf (getf *physical-recovery-state* :source-checkpoint)
                         (wal-checkpoint-record-snapshot record)))
                 (setf terminal (wal-record-end record))
                 (when (and (not clog-reset) (= (wal-record-type record) +wal-record-clog-reset+))
                   (setf clog-reset terminal))
                 (when (eql required-lsn terminal) (setf required-found t))))
             :start-lsn start-lsn :policy policy)
            (when (and required-lsn (not required-found))
              (error "durable WAL lost a previously applied terminal"))
            (values removed terminal *physical-history-reset-forks* clog-reset)))))))
