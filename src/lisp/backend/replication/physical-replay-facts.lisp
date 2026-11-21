;;;; Derive and validate physical replay facts before proof or redo effects.
(in-package #:pg)

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

(defun physical-replay-fpw-fact-next (prior record timeline history)
  "Advance the source FPW fact at a complete terminal. NIL means unknown;
known OFF is a non-NIL fact. The fact describes source history, not local policy."
  (when (or prior
            (= (wal-record-type record) +wal-record-full-page-writes-change+)
            (and (= (wal-record-type record) +wal-record-checkpoint+)
                 (get-properties (wal-checkpoint-record-snapshot record) '(:full-page-writes))))
    (physical-fpw-fact-next prior record timeline
                            (if (functionp history) (funcall history) history))))

(defun physical-replay-logical-fact-next (prior record timeline history)
  "Advance private logical evidence at a complete terminal, leaving unknown NIL
until a status record or checkpoint establishes it."
  (when (or prior
            (= (wal-record-type record) +wal-record-logical-decoding-status-change+)
            (and (= (wal-record-type record) +wal-record-checkpoint+)
                 (get-properties (wal-checkpoint-record-snapshot record) '(:logical-decoding-enabled))))
    (physical-logical-fact-next prior record timeline
                                (if (functionp history) (funcall history) history))))

(defun physical-replay-policy-facts-next (fpw-fact logical-fact record timeline history)
  "Advance both source policies at a caller-validated complete terminal.
HISTORY may resolve lazily so unknown facts do not require a history read."
  (values (physical-replay-fpw-fact-next fpw-fact record timeline history)
          (physical-replay-logical-fact-next logical-fact record timeline history)))

(defun physical-replay-terminal-facts-next (resource-fact fpw-fact logical-fact record timeline history)
  "Validate the candidate facts before redo can modify standby storage."
  (let ((resource (physical-resource-fact-next resource-fact record timeline history)))
    (multiple-value-bind (fpw logical)
        (physical-replay-policy-facts-next fpw-fact logical-fact record timeline history)
      (values resource fpw logical))))

(defun physical-recovery-resource-history (store state)
  (let ((selection (and store (physical-store-selection store))))
    (if selection (getf selection :history)
        (timeline-read-history (getf state :timeline)))))

(defun physical-recovery-scan-stage (store start state checkpoint next)
  "Build private replay state for the proven stage at START.
CHECKPOINT and NEXT must describe that prefix, never a later checkpoint or
observed XID. A NIL checkpoint with a known NEXT can describe a child awaiting
its initialization checkpoint."
  (let ((timeline (physical-store-prefix-timeline store start))
        (private (copy-tree state)))
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
       (dolist (image (wal-decode-page-images (wal-record-payload record)
                                              (wal-record-end record)))
         (dolist (xid (wal-page-image-xids image)) (observe xid))))
      ((= (wal-record-type record) +wal-record-clog-reset+)
       (setf next-xid (max next-xid (wal-read-u64-payload (wal-record-payload record))))))
    next-xid))

(defun physical-recovery-scan-seeds (state start checkpoint checkpoint-p next next-p)
  "Select checkpoint and observed XID seeds for exactly START.
Explicit seeds must be supplied together and are the caller's proof of that
prefix. Otherwise use state only where its position establishes the seeds.
Return the seeds and whether the starting stage is proven."
  (unless (eql checkpoint-p next-p)
    (error "suffix scan requires checkpoint and observed XID seeds together"))
  (cond
    (checkpoint-p (values checkpoint next t))
    ((eql start (getf (getf state :source-checkpoint) :end))
     (let ((snapshot (getf state :source-checkpoint)))
       (values snapshot (getf snapshot :next-xid) t)))
    ((and (eql start (getf state :end))
          (or (null (getf state :source-checkpoint))
              (<= (getf (getf state :source-checkpoint) :end) start))
          (typep (getf state :next-xid) '(integer 3 18446744073709551616)))
     (values (getf state :source-checkpoint) (getf state :next-xid) t))
    (t (values nil nil nil))))

(defun physical-recovery-scan-checkpoint-fpw (checkpoint fact start timeline)
  "Use known FPW policy from a checkpoint ending at START.
If FACT is present, require matching witness and policy; otherwise derive it.
Return FACT unchanged when the checkpoint does not prove policy at START."
  (if (and checkpoint (= start (getf checkpoint :end))
           (get-properties checkpoint '(:full-page-writes)))
      (let ((expected (physical-fpw-fact-from-state
                       (wal-fpw-state-from-checkpoint checkpoint (make-symbol "SCAN-FPW"))
                       start timeline)))
        (when fact
          (unless (physical-policy-witness-equal-p fact expected)
            (error "suffix scan FPW seed differs from its starting checkpoint")))
        (or fact expected))
      fact))

(defun physical-recovery-scan-checkpoint-logical (checkpoint fact start timeline)
  "Use a checkpoint at START to supply or refine logical status evidence.
When FACT is present, require the same witness before merging interval knowledge.
The caller must establish the checkpoint and its place in the selected history."
  (if (and checkpoint (= start (getf checkpoint :end))
           (get-properties checkpoint '(:logical-decoding-enabled)))
      (let ((expected (physical-logical-fact-from-state
                       (wal-logical-status-from-checkpoint checkpoint) :checkpoint
                       (getf checkpoint :start) (getf checkpoint :timeline)
                       start timeline nil nil)))
        (if fact (physical-logical-merge-checkpoint-interval fact expected) expected))
      fact))

(defun physical-recovery-scan-policy-seeds (store state checkpoint start timeline fpw logical)
  "Validate private policy seeds and refine them from the starting checkpoint."
  (when fpw
    (physical-fpw-check-fact fpw (physical-recovery-resource-history store state) start timeline))
  (when logical
    (physical-logical-check-fact logical (physical-recovery-resource-history store state) start timeline))
  (values (physical-recovery-scan-checkpoint-fpw checkpoint fpw start timeline)
          (physical-recovery-scan-checkpoint-logical checkpoint logical start timeline)))
