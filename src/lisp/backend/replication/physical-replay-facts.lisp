;;;; Derive and validate physical replay facts before proof or redo effects.
(in-package #:pg.physical-replay-facts)

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
      (timeline-check-resource-origin candidate history))))

(defun physical-replay-fpw-needs-history-p (prior record)
  (or prior (= (wal-record-type record) +wal-record-full-page-writes-change+)
      (and (= (wal-record-type record) +wal-record-checkpoint+)
           (get-properties (wal-checkpoint-record-snapshot record) '(:full-page-writes)))))

(defun physical-replay-fpw-fact-next (prior record timeline history)
  "Advance the source FPW fact at a complete terminal. NIL means unknown;
known OFF is a non-NIL fact. The fact describes source history, not local policy."
  (when (physical-replay-fpw-needs-history-p prior record)
    (physical-fpw-fact-next prior record timeline
                            history)))

(defun physical-replay-logical-needs-history-p (prior record)
  (or prior (= (wal-record-type record) +wal-record-logical-decoding-status-change+)
      (and (= (wal-record-type record) +wal-record-checkpoint+)
           (get-properties (wal-checkpoint-record-snapshot record) '(:logical-decoding-enabled)))))

(defun physical-replay-logical-fact-next (prior record timeline history)
  "Advance private logical evidence at a complete terminal, leaving unknown NIL
until a status record or checkpoint establishes it."
  (when (physical-replay-logical-needs-history-p prior record)
    (physical-logical-fact-next prior record timeline
                                history)))

(defun physical-replay-policy-facts-next (fpw-fact logical-fact record timeline history)
  "Advance both source policies using the caller's resolved timeline history."
  (values (physical-replay-fpw-fact-next fpw-fact record timeline history)
          (physical-replay-logical-fact-next logical-fact record timeline history)))

(defun physical-replay-terminal-facts-next (resource-fact fpw-fact logical-fact record timeline history)
  "Validate candidate facts against the caller's resolved history."
  (let ((resource (physical-resource-fact-next resource-fact record timeline history)))
    (multiple-value-bind (fpw logical)
        (physical-replay-policy-facts-next fpw-fact logical-fact record timeline history)
      (values resource fpw logical))))

(defun physical-replay-needs-boundary-history-p (record resource fpw logical resource-p)
  "Report whether any candidate fact needs history validation."
  (or (physical-replay-fpw-needs-history-p fpw record)
      (physical-replay-logical-needs-history-p logical record)
      (and resource-p
           (or resource
               (= (wal-record-type record) +wal-record-parameter-change+)
               (and (= (wal-record-type record) +wal-record-checkpoint+)
                    (getf (wal-checkpoint-record-snapshot record) :resource-parameters))))))

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

(defun physical-source-reservation-valid-p (state)
  (and (listp state) (= (length state) 8)
       (equal (loop for (key value) on state by #'cddr collect key)
              '(:version :through :bound :complete))
       (eql 1 (getf state :version))
       (typep (getf state :through) '(unsigned-byte 64))
       (wal-xid-reservation-valid-bound-p (getf state :bound))
       (member (getf state :complete) '(nil t))))

(defun physical-recovery-checkpoint-reservation (manifest record next-xid)
  "Derive the base reservation from the checkpoint and verified manifest."
  (let ((end (getf manifest :end))
        (bound (1- (canonical-next-full-xid next-xid)))
        (snapshot (wal-checkpoint-record-snapshot record)))
    (unless (and (= (wal-record-start record) (physical-manifest-checkpoint-start manifest))
                 (= (wal-record-end record) end))
      (error "base reservation record differs from checkpoint coordinates"))
    (unless (and (= bound (getf snapshot :xid-reserved-through))
                 (= (getf manifest :next-xid) (getf snapshot :next-xid)))
      (error "base XID counters differ from checkpoint"))
    (list :version 1 :through end :bound bound :complete t)))

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
        (type (wal-record-type record)))
    (cond
      ((= type +wal-record-end-of-recovery+) (setf complete nil))
      ((= type +wal-record-xid-reservation+)
       (let ((new-bound (wal-xid-reservation-record-bound record)))
         (when (< new-bound bound) (error "source XID reservation regressed"))
         (setf bound new-bound complete t)))
      ((= type +wal-record-checkpoint+)
       (let* ((snapshot (wal-checkpoint-record-snapshot record))
              (new-bound (1- (canonical-next-full-xid (1+ (getf snapshot :xid-reserved-through))))))
         (when (< new-bound bound) (error "checkpoint XID reservation regressed"))
         (setf bound new-bound complete t))))
    (when (and complete (> observed-next (1+ bound)))
      (error "observed XID exceeds the source's durable allocation reservation"))
    (list :version 1 :through (wal-record-end record) :bound bound :complete complete)))

;;; Compute prospective membership for each record in WAL order.  A complete
;;; reanchor preserves prior exclusions; reset or missing state leaves coverage
;;; unknown until a later anchor.
;;; The set is conservative: aborted transactions can remain because native
;;; ABORT has no WAL record.  Between anchors, only an ordered commit removes
;;; its XID.
;;; This result is not published until its batch reaches an applied terminal.

(defun physical-source-xids-next (prior record observed-next)
  "Track prospective source membership independently of existing reader snapshots.
EOR clears this proof. A later running-transaction anchor can establish it again.
Complete prior coverage preserves known exclusions when a new anchor arrives.
Do not consult replay CLOG: it may be ahead of this record's applied position."
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
         (running-xacts-reanchor prior snapshot end)))
      ((member type (list +wal-record-clog-reset+ +wal-record-end-of-recovery+)) nil)
      ((null prior) nil)
      (t
       (running-xacts-advance
        prior observed-next end
        :committed-xid (when (wal-transaction-terminal-type-p type) (wal-record-xid record)))))))

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

(defun physical-replay-check-policy-seeds (fpw logical history start timeline)
  "Check both supplied facts at one prefix against resolved history."
  (when fpw (physical-fpw-check-fact fpw history start timeline))
  (when logical (physical-logical-check-fact logical history start timeline)))
