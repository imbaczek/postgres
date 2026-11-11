;;;; Keep base provenance, minimum recovery and actual replay on separate timelines.
(in-package #:pg)

;;;; Replay selected child WAL while keeping the immutable backup identity.
;;;; Durable receive selection proves lineage; recovery control binds applied
;;;; progress to that lineage. Following a child never grants writer authority.

(defun physical-recovery-replay-timeline (&optional (state *physical-recovery-state*))
  "Return the applied replay timeline, defaulting to the immutable base timeline."
  (getf state :replay-timeline (getf state :timeline)))

(defun physical-recovery-check-history-reference (store target digest)
  "Accept an absent reference or find its selection in STORE's history chain.
A partial reference or a referenced selection missing from the chain is an error."
  (when (or target digest)
    (unless (and target digest)
      (error "incomplete recovery history reference"))
    (or (loop for selection = (physical-store-selection store) then (getf selection :previous)
              while selection
              when (and (eql target (getf selection :target)) (eql digest (getf selection :digest)))
                return selection)
        (error "recovery history reference is absent from the receive chain"))))

(defun physical-recovery-check-store-point (store point timeline)
  "Check that POINT belongs to TIMELINE and retained durable receive history.
Below the spool floor, the saved history proves timeline ownership; retained
bytes also require an exact record boundary."
  (unless (and (typep point '(unsigned-byte 64))
               (<= (physical-store-base-lsn store) point (physical-store-flushed-lsn store))
               (eql timeline (physical-store-prefix-timeline store point)))
    (error "recovery position differs from its receive history"))
  (when (>= point (physical-store-floor-lsn store))
    (physical-store-prefix-digest store point))
  timeline)

(defun physical-recovery-attach-selected-store (state store)
  "Attach receive-history metadata after checking the immutable base identity.
Any saved reference must name a selection in the store's ancestor chain.
Adopt the latest selection without moving the applied replay timeline."
  (unless (and (eql (getf state :system-id) (physical-store-system-id store))
               (eql (getf state :timeline) (physical-store-timeline store))
               (eql (getf state :end) (physical-store-base-lsn store)))
    (error "receive history differs from immutable recovery base"))
  (physical-recovery-check-history-reference store (getf state :history-target) (getf state :history-digest))
  (physical-recovery-check-store-point
   store (getf state :required-applied)
   (getf state :required-timeline (getf state :timeline)))
  (when (getf state :required-resource-fact)
    (timeline-check-resource-origin (getf state :required-resource-fact)
                                    (physical-recovery-resource-history store state)))
  (let ((candidate (copy-list state)) (selection (physical-store-selection store)))
    (when selection
      (setf (getf candidate :history-target) (getf selection :target)
            (getf candidate :history-digest) (getf selection :digest)))
    candidate))

(defun physical-replay-check-eor-stage (store previous timeline)
  "Check native lineage and allocation proof before a selected EOR.
A present checkpoint must be valid, match TIMELINE and end by PREVIOUS.
Without one, TIMELINE must be the immutable store base timeline, including
when runtime recovery state is absent.
C PostgreSQL can accept queries after lightweight EOR before its asynchronous
checkpoint. The native producer durably writes EOR, optional FPW change and
checkpoint before starting the writer."
  (let ((checkpoint (getf *physical-recovery-state* :source-checkpoint)))
    (unless (and (typep previous '(unsigned-byte 64))
                 (typep timeline '(integer 1 4294967295))
                 (if checkpoint
                     (and (wal-checkpoint-snapshot-valid-p checkpoint)
                          (= timeline (getf checkpoint :timeline))
                          (<= (getf checkpoint :end) previous))
                     (= timeline (physical-store-timeline store))))
      (error "end-of-recovery requires a completed checkpoint on its parent timeline")))
  t)

(defun physical-replay-check-selected-eor (store record raw previous timeline)
  "Check EOR against the selected receive history and current replay timeline.
Reject an active batch first, then check the parent checkpoint stage before
history and minimum recovery position. Return the selected child timeline
without changing recovery state or server role."
  (when *wal-recovery-group-active*
    (error "end-of-recovery cannot finish an active WAL batch"))
  (physical-replay-check-eor-stage store previous timeline)
  (let ((selection (physical-store-selection store)))
    (unless selection (error "end-of-recovery has no selected receive history"))
    (physical-store-history-check-record store record raw previous)
    (timeline-check-end-of-recovery
     record timeline (getf selection :history)
     :min-recovery-point (or (getf *physical-recovery-state* :required-applied) 0)
     :min-recovery-timeline (if (getf *physical-recovery-state* :required-applied)
                                (getf *physical-recovery-state* :required-timeline
                                      (getf *physical-recovery-state* :timeline))
                                0))))

(defun physical-recovery-check-required-pair (store)
  "Verify that replay reached the saved minimum recovery LSN on its timeline.
Equal positions on different timelines do not satisfy the required pair."
  (let* ((state *physical-recovery-state*) (required (getf state :required-applied))
                                           (required-timeline (getf state :required-timeline (getf state :timeline))))
    (physical-recovery-check-store-point store required required-timeline)
    (physical-recovery-check-store-point store *last-replay-lsn* (physical-recovery-replay-timeline state))
    (unless (<= required *last-replay-lsn*)
      (error "streaming redo did not reach its required replay pair"))))

(defun physical-recovery-identify-position ()
  "Return a safe LSN and its replay timeline for IDENTIFY_SYSTEM.
Receipt may advance the position only when its durable owner is the replay timeline."
  (let* ((replay (or *last-replay-lsn* 0)) (timeline (physical-recovery-replay-timeline))
                                           (store *physical-receive-store*))
    (if store
        (sb-thread:with-mutex ((physical-store-mutex store))
          (let ((flush (physical-store-flushed-lsn store)))
            (values (if (= timeline (physical-store-prefix-timeline store flush)) (max replay flush) replay)
                    timeline)))
        (values (max replay (or (getf *physical-recovery-state* :receive-lsn) 0)) timeline))))

(defun physical-replay-check-child-policy (record fpw-fact timeline)
  "Check the optional child FPW change and its checkpoint policy.
FPW-FACT describes the prefix before RECORD. A change must be the first child
FPW change before the initialization checkpoint and occur in a clean batch;
the child checkpoint must carry the same policy. Return true without changing
caller state."
  (let ((marker-seen (and fpw-fact
                          (eq :change (getf fpw-fact :witness-kind))
                          (eql timeline (getf fpw-fact :witness-timeline)))))
    (cond
      ((= (wal-record-type record) +wal-record-full-page-writes-change+)
       (unless (and *wal-recovery-group-active* *wal-recovery-reservation-batch-clean-p*
                    (not marker-seen))
         (error "child initialization requires one clean FPW change batch"))
       (wal-full-page-writes-change-record-info record))
      ((and marker-seen (= (wal-record-type record) +wal-record-checkpoint+))
       (let ((checkpoint (wal-checkpoint-record-snapshot record)))
         (unless (and checkpoint (get-properties checkpoint '(:full-page-writes))
                      (eql (getf fpw-fact :enabled) (getf checkpoint :full-page-writes)))
           (error "child initialization checkpoint loses its FPW change"))))))
  t)

(defun physical-replay-check-child-logical-policy (record logical-fact timeline)
  (let* ((type (wal-record-type record))
         (marker-seen (and logical-fact (eq :change (getf logical-fact :witness-kind))
                           (eql timeline (getf logical-fact :witness-timeline)))))
    (cond
      ((= type +wal-record-logical-decoding-status-change+)
       (unless (and *wal-recovery-group-active* *wal-recovery-reservation-batch-clean-p*
                    (not marker-seen))
         (error "child initialization requires one clean logical change batch"))
       (wal-logical-decoding-status-record-info record))
      ((and marker-seen (= type +wal-record-full-page-writes-change+))
       (error "child initialization places FPW after logical status"))
      ((and marker-seen (= type +wal-record-checkpoint+))
       (let ((checkpoint (wal-checkpoint-record-snapshot record)))
         (unless (and checkpoint (get-properties checkpoint '(:logical-decoding-enabled))
                      (eql (getf logical-fact :enabled) (getf checkpoint :logical-decoding-enabled)))
           (error "child initialization checkpoint loses its logical change"))))))
  t)

(defun physical-replay-check-child-initialization (record next-xid &optional (fpw-fact (getf *physical-recovery-state* :source-fpw-fact))
                                                                     (logical-fact (getf *physical-recovery-state* :source-logical-fact)))
  "Check native checkpoint timeline and selected child initialization during replay.
Only a validated selected EOR can switch the replay timeline. After EOR,
require the child initialization checkpoint before later child work. One validated
FPW change may precede it in a clean batch, and the checkpoint must preserve
that policy. FPW-FACT describes the prefix before RECORD and defaults to the
published source fact. Validation leaves caller state unchanged."
  (wal-require-fpw-physical-replay-supported record)
  (wal-require-logical-status-physical-replay-supported record)
  (let* ((state *physical-recovery-state*) (timeline (physical-recovery-replay-timeline state))
                                           (snapshot (getf state :source-checkpoint)) (type (wal-record-type record)))
    (when (= type +wal-record-checkpoint+)
      (let ((checkpoint (wal-checkpoint-record-snapshot record)))
        (when (and checkpoint (/= timeline (getf checkpoint :timeline)))
          (error "native checkpoint differs from its replay timeline"))))
    (when (/= timeline (getf snapshot :timeline (getf state :timeline)))
      (physical-replay-check-child-policy record fpw-fact timeline)
      (physical-replay-check-child-logical-policy record logical-fact timeline)
      (cond
        ((= type +wal-record-batch-start+)
         (when *wal-recovery-group-active* (error "child initialization has repeated batch framing")))
        ((or (= type +wal-record-full-page-writes-change+)
             (= type +wal-record-logical-decoding-status-change+)) nil)
        ((= type +wal-record-checkpoint+)
         (let* ((child (wal-checkpoint-record-snapshot record))
                (selection (loop for item = (physical-store-selection *physical-receive-store*)
                                   then (getf item :previous) while item
                                 when (= timeline (getf item :target)) return item)))
           (unless (and *wal-recovery-group-active* *wal-recovery-reservation-batch-clean-p*
                        child selection (= timeline (getf child :timeline))
                        (= (getf selection :parent) (getf child :prev-timeline))
                        (<= next-xid (getf child :next-xid)))
             (error "child initialization checkpoint differs from its replay timeline or allocation floor"))))
        (t (error "child end-of-recovery requires its initialization checkpoint"))))))

(defun physical-recovery-publish-selected-history (store)
  "Publish the selected history chain after checking immutable base ancestry.
Existing timeline files must match; a conflicting file is an error."
  (let ((selection (physical-store-selection store)))
    (when selection
      (unless (physical-store-history-extends-p
               (timeline-read-history (physical-store-timeline store)) (getf selection :history))
        (error "selected receive history changes immutable base ancestry"))
      (dolist (node (reverse (loop for item = selection then (getf item :previous) while item collect item)))
        (let* ((path (timeline-history-path (getf node :target) *pgdata*))
               (bytes (string->bytes (getf node :history-text))))
          (if (wal-namespace-file-stat path t)
              (unless (equalp bytes (recovery-read-bytes path))
                (error "selected history differs from its published timeline file"))
              (timeline-publish-history-bytes path bytes))))
      (fsync-directory (wal-directory)))))

(defun physical-promotion-parent-namespace (state)
  "Return the WAL namespace for promotion's actual applied parent timeline.
A replayed child uses timeline-qualified WAL even when the base used legacy names."
  (if (> (physical-recovery-replay-timeline state) 1) :timeline
      (getf state :promotion-source-namespace)))

(defun physical-replay-selected-eor (record raw next-xid reservation source-xids
                                     &key (fpw-fact (getf *physical-recovery-state* :source-fpw-fact))
                                       (logical-fact (getf *physical-recovery-state* :source-logical-fact)))
  "Apply a verified selected EOR and durably publish replay progress.
The applied timeline and source FPW and logical facts advance with durable
recovery control. The fourth return value is the new logical fact. This does
not end recovery or grant writer authority."
  (let* ((child (physical-replay-check-selected-eor
                 *physical-receive-store* record raw *last-replay-lsn*
                 (physical-recovery-replay-timeline)))
         (history (lambda () (physical-recovery-resource-history
                              *physical-receive-store* *physical-recovery-state*)))
         (candidate (physical-replay-fpw-fact-next
                     fpw-fact record child
                     history))
         (logical-candidate (physical-replay-logical-fact-next logical-fact record child history)))
    (physical-recovery-publish-selected-history *physical-receive-store*)
    (setf reservation (physical-source-reservation-next reservation record next-xid)
          source-xids (physical-source-xids-next source-xids record next-xid))
    (when *physical-replay-test-hook* (funcall *physical-replay-test-hook* :before-eor-progress record))
    (apply #'physical-replay-publish-progress record next-xid reservation source-xids
           :replay-timeline child (append (when candidate (list :fpw-fact candidate))
                                          (when logical-candidate (list :logical-fact logical-candidate))))
    (when *physical-replay-test-hook* (funcall *physical-replay-test-hook* :after-eor-progress record))
    (values reservation source-xids candidate logical-candidate)))
