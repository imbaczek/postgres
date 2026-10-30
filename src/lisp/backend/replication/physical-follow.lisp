;;;; Local receiver/replayer workers. No election, promotion or routing policy.
(in-package #:pg)

(define-condition physical-follow-terminal-error (simple-error) ())
(defstruct (physical-follow-runtime (:print-function physical-libpq-print-private-object))
  "CONFIG is the runtime's private saved configuration; TRANSITION requests a
controller handoff. Lifecycle and client ownership use the follow lock.
STOPPED-P stops both workers; RECEIVER-STOPPED-P stops receipt alone."
  host port pin config transition limit next-timeline
  (stopped-p nil) (receiver-stopped-p nil) (connected-p nil) last-error client receiver-thread replay-thread)

(define-condition physical-follow-receiver-stopped (physical-client-stopped) ()
  (:documentation "The runtime stopped receipt before client registration completed."))

(defun physical-follow-receiver-stopped-p (runtime)
  "Whether either the receiver-only or the all-workers stop was requested."
  (or (physical-follow-runtime-stopped-p runtime)
      (physical-follow-runtime-receiver-stopped-p runtime)))

(defvar *physical-follow-runtime* nil)
(defvar *physical-follow-lock* (sb-thread:make-mutex :name "native physical follow lifecycle"))

(defvar *physical-follow-command-lock* (sb-thread:make-mutex :name "native physical follow commands"))

(defvar *physical-follow-statistics* nil
  "Session-tagged receiver telemetry, protected by *PHYSICAL-FOLLOW-LOCK*.")

(defvar *physical-follow-last-end-lsn* 0
  "C's latestWalEnd survives receiver exit/reconnect within a server process.")

(defun physical-follow-connection-description (runtime)
  "Describe the successful native plaintext connection, without libpq-only defaults."
  (let ((client (physical-follow-runtime-client runtime)))
    (when (typep client 'physical-libpq-channel)
      (return-from physical-follow-connection-description
        (physical-libpq-public-connection-info (physical-libpq-channel-info client)))))
  (let ((host (with-output-to-string (stream)
                (write-char (code-char 39) stream)
                (loop for character across (physical-follow-runtime-host runtime)
                      do (when (member (char-code character) '(39 92))
                           (write-char (code-char 92) stream))
                         (write-char character stream))
                (write-char (code-char 39) stream))))
    (format nil "host=~a port=~d user=postgres dbname=postgres application_name=pg_lisp_physical_receiver replication=database sslmode=disable"
            host (physical-follow-runtime-port runtime))))

(defun physical-follow-monitor-start (runtime cursor)
  "Publish C's connecting state with stale sender details cleared."
  (let ((now (postgres-current-timestamp)))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (setf *physical-follow-statistics*
            (list :runtime runtime :pid (sb-thread:thread-os-tid sb-thread:*current-thread*)
                  :public-slot (physical-follow-config-public-slot (physical-follow-runtime-config runtime))
                  :receive-start-lsn cursor :receive-start-tli (physical-follow-runtime-receive-timeline runtime)
                  :last-send-time now :last-receipt-time now
                  :latest-end-lsn *physical-follow-last-end-lsn*
                  :latest-end-time now)
            (physical-follow-runtime-connected-p runtime) nil))))

(defun physical-follow-monitor-connected (runtime)
  "Publish sender details only after native streaming is established."
  (sb-thread:with-mutex (*physical-follow-lock*)
    (when (eq runtime (getf *physical-follow-statistics* :runtime))
      (let* ((client (physical-follow-runtime-client runtime))
             (info (when (typep client 'physical-libpq-channel)
                     (physical-libpq-channel-info client))))
        (setf (getf *physical-follow-statistics* :host)
              (if info (physical-libpq-display-octets (physical-libpq-connection-info-host info))
                  (physical-follow-runtime-host runtime))
              (getf *physical-follow-statistics* :port)
              (if info (physical-libpq-connection-info-port info) (physical-follow-runtime-port runtime))
              (getf *physical-follow-statistics* :conninfo) (physical-follow-connection-description runtime)
              (physical-follow-runtime-connected-p runtime) t
              (physical-follow-runtime-last-error runtime) nil)))))

(defun physical-follow-monitor-message (runtime end send-time)
  "Match ProcessWalSndrMessage without using source end as local progress."
  (let ((now (postgres-current-timestamp)))
    (sb-thread:with-mutex (*physical-follow-lock*)
      (when (eq runtime (getf *physical-follow-statistics* :runtime))
        (when (< (getf *physical-follow-statistics* :latest-end-lsn) end)
          (setf (getf *physical-follow-statistics* :latest-end-time) send-time))
        (setf *physical-follow-last-end-lsn* end
              (getf *physical-follow-statistics* :latest-end-lsn) end
              (getf *physical-follow-statistics* :last-send-time) send-time
              (getf *physical-follow-statistics* :last-receipt-time) now)))))

(defun physical-follow-monitor-stop (runtime)
  (sb-thread:with-mutex (*physical-follow-lock*)
    (setf (physical-follow-runtime-connected-p runtime) nil)
    (when (eq runtime (getf *physical-follow-statistics* :runtime))
      (setf *physical-follow-statistics* nil))))

(defun physical-follow-statistics ()
  "Copy receiver metadata and real spool write/flush; never infer a live receiver from a spool."
  (let ((store *physical-receive-store*))
    (when store
      ;; Do not wait for a store mutex while holding the lifecycle lock.
      (sb-thread:with-mutex ((physical-store-mutex store))
        (sb-thread:with-mutex (*physical-follow-lock*)
          (let* ((runtime *physical-follow-runtime*)
                 (statistics *physical-follow-statistics*))
            (when (and runtime (eq runtime (getf statistics :runtime)))
              (append (copy-list statistics)
                      (list :status (cond ((physical-follow-runtime-stopped-p runtime) "stopping")
                                          ((physical-follow-runtime-connected-p runtime) "streaming")
                                          (t "connecting"))
                            :written-lsn (physical-store-written-lsn store)
                            :flushed-lsn (physical-store-flushed-lsn store)
                            :received-tli (physical-store-prefix-timeline store (physical-store-flushed-lsn store)))))))))))

(defparameter *physical-receive-flush-bytes* (* 1024 1024)
  "Batch native flushes while busy; a keepalive flushes all completed records.")

(defun physical-follow-reject (message)
  (error 'physical-follow-terminal-error :format-control "~a" :format-arguments (list message)))

(defun physical-follow-config-path ()
  (merge-pathnames "physical-follow.control" *pgdata*))

(defconstant +physical-follow-config-maximum-bytes+
  (+ (* 4 +physical-store-history-maximum-bytes+) 65536
     (* 2 (+ (* 2 1048576) 32768))
     (* +physical-follow-cleanup-capacity+ 32768))
  "Maximum config envelope size before parsing, including bounded histories.")

(defparameter *physical-follow-binding-keys*
  '(:version :phase :anchor :child-history :before :after :applied :required :recovery)
  "Exact keys of the optional version 1 binding.")

(defun physical-follow-policy-selection (policy local-history &optional source-history)
  "Choose a follow action from resolved policy and checked history intervals.
Return :DISCOVER when source history is needed. Equal source and receive
timelines use same-timeline proof. A source toward the target uses descendant
proof. At the target, a later source uses retained-timeline proof.
A locally cached child past the target permits
replay only. A different local branch returns :REPLAY-ONLY with an unavailable
reason; an incompatible source returns :UNAVAILABLE.  Only a source whose
history is an ancestor of the target requests automatic source retry."
  (timeline-check-lineage local-history)
  (let* ((target (getf policy :target))
         (target-history (getf policy :history))
         (parent (timeline-history-entry-timeline (first local-history))))
    (when target
      (timeline-check-lineage target-history)
      (unless (= target (timeline-history-entry-timeline (first target-history)))
        (error "follow policy target differs from its resolved history"))
      (unless (physical-store-history-extends-p local-history target-history)
        (return-from physical-follow-policy-selection
          (list :action :replay-only
                :reason (if (physical-store-history-extends-p target-history local-history)
                            :timeline-target-reached :timeline-history-unavailable)))))
    (unless source-history
      (return-from physical-follow-policy-selection (list :action :discover)))
    (timeline-check-lineage source-history)
    (when (and target (not (physical-store-history-extends-p target-history source-history)))
      (return-from physical-follow-policy-selection
        (list :action :unavailable :reason :timeline-history-unavailable
              :retry-source (physical-store-history-extends-p source-history target-history))))
    (unless (physical-store-history-extends-p local-history source-history)
      (error "source follow history rewrites local ancestry"))
    (let* ((source (timeline-history-entry-timeline (first source-history)))
           (position (position parent source-history :key #'timeline-history-entry-timeline))
           (entry (and position (nth position source-history)))
           (next (when (and position (plusp position))
                   (timeline-history-entry-timeline (nth (1- position) source-history))))
           (kind (cond ((= source parent) :same-timeline)
                       ((and target (= target parent)) :retained-timeline)
                       (t :descendant))))
      (unless entry (error "follow target does not contain receive ancestry"))
      (list :action :attach :proof-kind kind :source source :parent parent
            :history source-history :fork (timeline-history-entry-end entry)
            :child (when (eq kind :descendant) next)))))

(defun physical-follow-binding-pair-p (pair)
  "Recognize a bounded (LSN timeline) pair."
  (and (listp pair) (= 2 (length pair))
       (typep (first pair) '(unsigned-byte 64))
       (typep (second pair) '(integer 1 4294967295))))

(defun physical-follow-binding-reference-p (timeline digest)
  "Recognize an absent or complete selected-history reference."
  (or (and (null timeline) (null digest))
      (and (typep timeline '(integer 2 4294967295))
           (typep digest '(unsigned-byte 64)))))

(defun physical-follow-binding-control-check (tuple)
  "Check the representation and internal bounds of a receive-control tuple.
Use PHYSICAL-STORE-CONTROL-TUPLE's field order; no durable file is read here."
  (unless (and (listp tuple) (= 11 (length tuple))
               (every (lambda (index) (typep (nth index tuple) '(unsigned-byte 64)))
                      '(0 2 3 4 5 6 9))
               (typep (second tuple) '(integer 1 4294967295))
               (<= (third tuple) (sixth tuple) (fourth tuple))
               (or (/= (third tuple) (sixth tuple)) (= (seventh tuple) #xCBF29CE484222325))
               (physical-follow-binding-reference-p (eighth tuple) (ninth tuple))
               (or (null (nth 10 tuple)) (typep (nth 10 tuple) '(unsigned-byte 64))))
    (error "invalid follow receive-control witness"))
  tuple)

(defun physical-follow-binding-recovery-check (witness before applied)
  "Check the named phase-2 recovery witness against the saved tuple and applied pair.
Identity, checkpoint, observed XID, old history reference and restartpoint token
still require comparison with durable recovery state."
  (physical-follow-check-plist
   witness '(:phase :cluster-identity :checkpoint :observed-next-xid
             :history-target :history-digest :restartpoint))
  (let ((identity (getf witness :cluster-identity))
        (checkpoint (getf witness :checkpoint))
        (observed (getf witness :observed-next-xid))
        (point (getf witness :restartpoint)))
    (unless (and (eql 2 (getf witness :phase))
                 (stringp identity) (<= 1 (length identity) 4096)
                 (typep checkpoint '(unsigned-byte 64)) (<= checkpoint (third before))
                 (typep observed '(integer 3 18446744073709551616))
                 (physical-follow-binding-reference-p (getf witness :history-target)
                                                      (getf witness :history-digest))
                 (if point
                     (and (listp point) (= 2 (length point))
                          (every (lambda (value) (typep value '(unsigned-byte 64))) point)
                          (<= (sixth before) (first point) (first applied)))
                     (= (sixth before) (third before))))
      (error "invalid follow recovery witness"))))

(defun physical-follow-binding-selection-digest (anchor child history before digest)
  "Derive the candidate selection digest from the anchor and immediate child history."
  (physical-store-update-digest
   #xCBF29CE484222325
   (physical-store-history-image
    (list :version 1 :system-id (getf anchor :system-id)
          :base-timeline (getf anchor :base-timeline) :base (getf anchor :base)
          :target child :history-text history :fork-digest digest
          :previous-digest (ninth before) :eor-octets (getf anchor :eor-octets)))))

(defun physical-follow-descendant-binding-transition-check (anchor text before after)
  "Check the catch-up or rewind plan and its immediate child history.
The source history can extend beyond that child. Preserve the base, floor,
manifest and root fields, and validate the proposed selection digest."
  (multiple-value-bind (history fork eor) (physical-follow-anchor-check anchor)
    (let* ((child (timeline-check-end-of-recovery eor (getf anchor :receive-timeline) history))
           (child-history (timeline-check-lineage (timeline-parse-history child text))))
      (unless (and (physical-store-history-extends-p child-history history)
                   (= (or (eighth before) (second before)) (getf anchor :receive-timeline))
                   (= (min (fourth before) fork) (getf anchor :receive-lsn))
                   (every (lambda (index) (eql (nth index before) (nth index after))) '(0 1 2 5 6 9 10)))
        (error "follow binding changes immutable provenance or source ancestry"))
      (if (equal before after)
          (unless (and (<= (fourth before) fork)
                       (= (fifth before) (getf anchor :receive-digest)))
            (error "follow binding retained an unapplied divergent tail"))
          (unless (and (<= fork (fourth before)) (= (fourth after) fork)
                       (= (fifth after) (getf anchor :receive-digest))
                       (eql (eighth after) child)
                       (eql (ninth after)
                            (physical-follow-binding-selection-digest anchor child text before (fifth after))))
            (error "follow binding has a foreign post-transition selection")))
      child-history)))

(defun physical-follow-binding-transition-check (anchor text before after)
  "Check the transition implied by the anchor kind.
Same-timeline and retained bindings keep all eleven control fields and have no
child history.  Retained receipt is bounded by the anchor's fork."
  (ecase (physical-follow-anchor-kind anchor)
    (:descendant (physical-follow-descendant-binding-transition-check anchor text before after))
    ((:same-timeline :retained-timeline)
     (let ((history (physical-follow-anchor-check anchor)))
       (unless (and (null text) (equal before after)
                    (= (or (eighth before) (second before)) (getf anchor :receive-timeline))
                    (= (fourth before) (getf anchor :receive-lsn))
                    (= (fifth before) (getf anchor :receive-digest)))
         (error "same-timeline follow binding changes receive state or its proven cursor"))
       history))))

(defun physical-follow-binding-check (binding)
  "Validate the shape and internal coherence of a version 1 follow binding.
The anchor, optional child history, control tuples, applied and required pairs
and recovery witness describe a plan, not durable proof. Pending bindings need
reconciliation; active bindings require durable-state checks before admission."
  (physical-follow-check-plist binding *physical-follow-binding-keys*)
  (let* ((anchor (getf binding :anchor)) (text (getf binding :child-history))
                                         (before (physical-follow-binding-control-check (getf binding :before)))
                                         (after (physical-follow-binding-control-check (getf binding :after)))
                                         (applied (getf binding :applied)) (required (getf binding :required))
                                         (witness (getf binding :recovery)))
    (unless (and (eql 1 (getf binding :version)) (member (getf binding :phase) '(:pending :active))
                 (if (member (physical-follow-anchor-kind anchor) '(:same-timeline :retained-timeline))
                     (null text)
                     (and (stringp text)
                          (<= (length (string->bytes text)) +physical-store-history-maximum-bytes+)))
                 (stringp (getf anchor :history-text))
                 (<= (length (string->bytes (getf anchor :history-text))) +physical-store-history-maximum-bytes+)
                 (physical-follow-binding-pair-p applied) (physical-follow-binding-pair-p required)
                 (= (first before) (getf anchor :system-id))
                 (= (second before) (getf anchor :base-timeline))
                 (= (third before) (getf anchor :base))
                 (= (nth 9 before) (getf anchor :manifest-digest))
                 (<= (third before) (first required) (first applied) (getf anchor :receive-lsn))
                 (<= (sixth before) (first applied))
                 (<= (sixth before) (getf anchor :receive-lsn)))
      (error "invalid native physical follow binding"))
    (let ((history (physical-follow-binding-transition-check anchor text before after)))
      (dolist (pair (list applied required))
        (unless (and (timeline-prefix-boundary-p (second pair) (first pair) history)
                     (= (second pair) (if (= (first pair) (getf anchor :base))
                                          (getf anchor :base-timeline)
                                          (timeline-at-point (1- (first pair)) history))))
          (error "follow recovery frontier has a foreign timeline"))))
    (physical-follow-binding-recovery-check witness before applied)
    (when (getf witness :history-target)
      (unless (<= (getf witness :history-target) (getf anchor :receive-timeline))
        (error "follow recovery history witness is ahead of the old receive selection"))))
  binding)

(defun physical-follow-replay-seed (store base applied restartpoint)
  "Check RESTARTPOINT against the durable pointer and choose a replay seed.
A version 3 or later generation supplies the seed; an older generation falls
back to BASE and requires the full prefix. Check its timeline, history and
prefix digest against STORE, and require the seed not to pass APPLIED. The
caller supplies a verified BASE and protects PGDATA and the retained root."
  (multiple-value-bind (metadata catalog prefix digest) (physical-restartpoint-read base)
    (declare (ignore catalog prefix))
    (unless (equal restartpoint
                   (when metadata (list (getf metadata :lsn) digest)))
      (error "follow restartpoint witness differs from its durable pointer"))
    (when (and metadata (< (getf metadata :version) 3))
      (setf metadata nil))
    (let* ((start (if metadata (getf metadata :lsn) (getf base :end)))
           (timeline (if metadata (getf metadata :replay-timeline (getf metadata :timeline))
                         (getf base :timeline)))
           (next (getf (or metadata base) :next-xid))
           (snapshot (copy-tree (getf (or metadata base) :source-checkpoint)))
           (reservation (copy-tree (getf (or metadata base) :source-reservation))))
      (unless (and (<= (physical-store-floor-lsn store) start
                       (first applied))
                   (typep next '(integer 3 18446744073709551616))
                   (or metadata (= start (physical-store-base-lsn store))))
        (error "follow replay lacks a usable base or restartpoint"))
      (physical-recovery-check-store-point store start timeline)
      (when metadata
        (physical-recovery-check-history-reference
         store (getf metadata :history-target) (getf metadata :history-digest))
        (unless (= (getf metadata :prefix-digest) (physical-store-prefix-digest store start))
          (error "follow restartpoint differs from its receive prefix")))
      (when snapshot
        (unless (and (wal-checkpoint-snapshot-valid-p snapshot)
                     (<= (getf snapshot :end) start)
                     (or metadata (= timeline (getf snapshot :timeline))))
          (error "follow replay seed has an invalid source checkpoint")))
      (when reservation
        (unless (and (physical-source-reservation-valid-p reservation)
                     (= start (getf reservation :through))
                     (or (not (getf reservation :complete))
                         (<= next (1+ (getf reservation :bound)))))
          (error "follow replay seed has an invalid allocation frontier")))
      (values start timeline next snapshot reservation
              (when metadata (copy-tree (getf metadata :source-xids)))))))

(defun physical-follow-scan-replay-from-seed
    (candidate verified-base applied required observed seed map-records allow-open-tail &optional witness)
  "Scan a read-only candidate from SEED without redo or publication.
APPLIED and REQUIRED are semantic recovery frontiers, observed at clean
recovery group boundaries rather than arbitrary record ends. A REQUIRED point
before SEED relies on the seed's visibility. WITNESS, when supplied, is a
historical (LSN timeline next-XID) triple checked at a clean boundary. The
caller owns the candidate and keeps its input stable during the scan.  Return
its disposition with the semantic result; this scan does not update replay's
exhaustion cache.  A policy fence must leave a clean batch boundary."
  (destructuring-bind (start timeline next snapshot reservation source-xids) seed
    (let* ((*physical-recovery-state* (copy-tree verified-base))
           (*physical-receive-store* candidate)
           (terminal start) (disposition nil) (applied-seen nil) (required-seen nil) (required-next nil)
           (witness-seen (null witness))
           (removed (make-hash-table :test #'eql)))
      (setf (getf *physical-recovery-state* :replay-timeline) timeline
            (getf *physical-recovery-state* :source-checkpoint) snapshot
            (getf *physical-recovery-state* :required-applied) (first required)
            (getf *physical-recovery-state* :required-timeline) (second required))
      (physical-recovery-check-store-point candidate (first required) (second required))
      (when (< (first required) start)
        (unless (>= next observed)
          (error "follow restartpoint loses required XID visibility"))
        (setf required-seen t required-next observed))
      (labels ((note-boundary ()
                 (when (and witness (= terminal (first witness)))
                   (unless (equal witness (list terminal timeline next))
                     (error "follow replay differs from its restartpoint visibility"))
                   (setf witness-seen t))
                 (when (equal applied (list terminal timeline))
                   (setf applied-seen t))
                 (when (equal required (list terminal timeline))
                   (unless (= next observed)
                     (error "follow replay differs from required XID visibility"))
                   (setf required-seen t required-next next))))
        (note-boundary)
        (with-physical-replay-staging ()
          (setf disposition
                (funcall
                 map-records
                 (lambda (record raw)
                   (if (= (wal-record-type record) +wal-record-end-of-recovery+)
                       (setf timeline (physical-replay-check-selected-eor
                                       candidate record raw terminal timeline)
                             (getf *physical-recovery-state* :replay-timeline) timeline
                             reservation (physical-source-reservation-next reservation record next)
                             source-xids (physical-source-xids-next source-xids record next))
                       (progn
                         (physical-replay-check-child-initialization record next)
                         (setf next (physical-replay-observed-next-xid record next)
                               reservation (physical-source-reservation-next reservation record next)
                               source-xids (physical-source-xids-next source-xids record next))
                         (physical-history-accept-record
                          record (getf verified-base :cluster-identity) removed)
                         (when (= (wal-record-type record) +wal-record-checkpoint+)
                           (setf (getf *physical-recovery-state* :source-checkpoint)
                                 (wal-checkpoint-record-snapshot record)))))
                   (unless *wal-recovery-group-active*
                     (setf terminal (wal-record-end record))
                     (note-boundary)))
                 start))
          (unless (and applied-seen required-seen witness-seen
                       (if (member disposition '(:timeline-target-reached :timeline-history-unavailable))
                           (not *wal-recovery-group-active*)
                           (or (and allow-open-tail *wal-recovery-group-active*)
                               (and (not *wal-recovery-group-active*)
                                    (= terminal (physical-store-flushed-lsn candidate))))))
            (error "follow replay does not reach clean recovery boundaries"))))
      (list :disposition disposition :start start :terminal terminal :timeline timeline :next-xid next
            :required-next-xid required-next :reservation reservation :source-xids source-xids))))

(defun physical-follow-scan-replay (candidate verified-base applied required observed restartpoint
                                    &key allow-open-tail)
  "Validate the durable seed and scan its retained suffix without redo."
  (physical-follow-scan-replay-from-seed
   candidate verified-base applied required observed
   (multiple-value-list (physical-follow-replay-seed candidate verified-base applied restartpoint))
   (lambda (function start)
     (physical-store-map-durable-records candidate function :start-lsn start))
   allow-open-tail))

(defun physical-follow-check-source-history (store history)
  (unless (physical-store-history-extends-p (physical-store-retained-history store) history)
    (error "source follow history rewrites local ancestry"))
  t)

(defun physical-follow-validate-candidate-replay (candidate verified-base binding)
  "Validate a private pending candidate without redo or publication.
Candidate proof remains strict for pending or new bindings. Same-timeline
receipt may have an open tail; complete records are still checked."
  (physical-follow-binding-check binding)
  (physical-follow-check-source-history candidate (nth-value 0 (physical-follow-anchor-check (getf binding :anchor))))
  (unless (and (equal (physical-store-control-tuple candidate) (getf binding :after))
               (eql (getf verified-base :system-id) (physical-store-system-id candidate))
               (eql (getf verified-base :timeline) (physical-store-timeline candidate))
               (eql (getf verified-base :end) (physical-store-base-lsn candidate))
               (eql (getf verified-base :manifest-digest) (nth 9 (getf binding :after)))
               (equal (getf verified-base :cluster-identity)
                      (getf (getf binding :recovery) :cluster-identity)))
    (error "follow replay candidate differs from its verified base or binding"))
  (physical-follow-scan-replay
   candidate verified-base (getf binding :applied) (getf binding :required)
   (getf (getf binding :recovery) :observed-next-xid)
   (getf (getf binding :recovery) :restartpoint)
   :allow-open-tail
   (or (eq :same-timeline (physical-follow-anchor-kind (getf binding :anchor)))
       (and (equal (getf binding :before) (getf binding :after))
            (< (physical-store-flushed-lsn candidate)
               (nth-value 1 (physical-follow-anchor-check (getf binding :anchor))))))))

(defun physical-follow-pending-recovery-position (base binding control)
  "Match durable recovery provenance, required pair and XID to the witness.
Return whether the history reference is the saved old or proposed post state;
reject any other state."
  (let* ((witness (getf binding :recovery))
         (required (getf binding :required))
         (before (getf binding :before))
         (after (getf binding :after))
         (old (list (getf witness :history-target) (getf witness :history-digest)))
         (new (if (equal before after) old (list (eighth after) (ninth after))))
         (current (list (getf control :history-target) (getf control :history-digest))))
    (unless (and (eql 2 (getf control :phase)) (eql 3 (getf control :control-version))
                 (= (getf control :system-id) (getf base :system-id) (first before))
                 (= (getf control :timeline) (getf base :timeline) (second before))
                 (= (getf control :manifest-digest) (getf base :manifest-digest) (nth 9 before))
                 (= (getf control :checkpoint) (getf base :checkpoint) (getf witness :checkpoint))
                 (equal (getf base :cluster-identity) (getf witness :cluster-identity))
                 (equal required (list (getf control :applied)
                                       (getf control :applied-timeline (getf control :timeline))))
                 (eql (getf control :next-xid) (getf witness :observed-next-xid)))
      (error "pending follow differs from durable recovery provenance or progress"))
    (cond ((equal current old) :before)
          ((equal current new) :after)
          (t (error "pending follow has an unrelated recovery history reference")))))

(defun physical-follow-copy-replay-candidate (store)
  "Copy STORE for read-only validation without owning its streams or lock.
The copy shares STORE's mutex and may be latched failed independently. The
caller must keep STORE quiescent and protect its retained root while it is read."
  (let ((candidate (copy-physical-store store)))
    (setf (physical-store-stream candidate) nil
          (physical-store-lock-stream candidate) nil)
    candidate))

(defun physical-follow-check-active-control (store base binding control)
  "Check immutable recovery identity and monotone durable recovery progress."
  (let* ((after (getf binding :after)) (witness (getf binding :recovery))
                                       (required (list (getf control :applied)
                                                       (getf control :applied-timeline (getf control :timeline)))))
    (unless (and (eql 2 (getf control :phase)) (eql 3 (getf control :control-version))
                 (= (getf control :system-id) (getf base :system-id) (first after))
                 (= (getf control :timeline) (getf base :timeline) (second after))
                 (= (getf control :manifest-digest) (getf base :manifest-digest) (nth 9 after))
                 (= (getf control :checkpoint) (getf base :checkpoint) (getf witness :checkpoint))
                 (equal (getf base :cluster-identity) (getf witness :cluster-identity))
                 (equal (list (getf control :history-target) (getf control :history-digest))
                        (list (eighth after) (ninth after)))
                 (<= (first (getf binding :required)) (first required))
                 (<= (getf witness :observed-next-xid) (getf control :next-xid)))
      (error "active follow differs from durable recovery provenance or progress"))
    (dolist (pair (list required (getf binding :required) (getf binding :applied)))
      (physical-recovery-check-store-point store (first pair) (second pair)))
    required))

(defun physical-follow-active-prefix-digest (store cursor index)
  "Derive a saved boundary digest from the spool or checked retained root.
This preserves proof of the saved anchor and floor after spool compaction."
  (when (<= (physical-store-floor-lsn store) cursor)
    (return-from physical-follow-active-prefix-digest (physical-store-prefix-digest store cursor)))
  (unless index (error "active follow lacks retained proof of an older boundary"))
  (when (= cursor (physical-store-base-lsn store))
    (return-from physical-follow-active-prefix-digest #xCBF29CE484222325))
  (dolist (entry (getf index :entries))
    (let ((metadata (third entry)))
      (when (<= (getf metadata :start) cursor (getf metadata :end))
        (multiple-value-bind (actual bytes)
            (physical-retained-extent-decode
             (physical-retained-object-read (physical-store-retained-directory store)
                                            :extent (first entry) +physical-retained-image-limit+))
          (let ((digest (physical-retained-extent-scan
                         actual bytes (physical-store-retained-history store) cursor)))
            (when digest (return-from physical-follow-active-prefix-digest digest)))))))
  (error "active follow retained root omits a saved boundary"))

(defun physical-follow-check-active-receive (store base binding)
  "Check the exact saved receive selection and any later retained prefix.
A different child selection needs a new binding. Same-timeline receipt may
advance beyond the saved cursor. Check the anchor and floor digests even when
compaction has moved them into retained extents."
  (let* ((saved (getf binding :after)) (current (physical-store-control-tuple store))
                                       (anchor (getf binding :anchor)) (cursor (getf anchor :receive-lsn)))
    (unless (and (every (lambda (index) (eql (nth index saved) (nth index current)))
                        '(0 1 2 7 8 9))
                 (= (getf base :end) (third current))
                 (<= (fourth saved) (fourth current))
                 (<= (sixth saved) (sixth current))
                 (or (/= (sixth saved) (sixth current))
                     (= (seventh saved) (seventh current))))
      (error "active follow receive identity, selection or floor regressed"))
    (physical-follow-check-source-history store (nth-value 0 (physical-follow-anchor-check anchor)))
    (when (and (member (physical-follow-anchor-kind anchor) '(:descendant :retained-timeline))
               (equal (getf binding :before) saved))
      (unless (<= (fourth current)
                  (nth-value 1 (physical-follow-anchor-check anchor)))
        (error "active parent receipt crossed its unselected child fork")))
    (when (physical-store-selection store)
      (unless (equalp (physical-store-selection store)
                      (physical-store-load-history-selection store (eighth current) (ninth current)))
        (error "active follow in-memory history differs from its durable selection")))
    (physical-store-validate-prefix store)
    (when (or (> (sixth current) (third current)) (nth 10 saved))
      (unless (physical-store-retained-root store)
        (error "active follow lost its retained root")))
    (let ((index
            (when (physical-store-retained-root store)
              (physical-store-check-retained-identity store)
              (let ((index (physical-retained-inventory-load
                            (physical-store-retained-directory store) (physical-store-retained-root store)
                            (physical-store-retained-identity store) (physical-store-retained-history store))))
                (physical-store-check-retained-coverage store (getf index :entries))
                index))))
      (unless (and (= (getf anchor :receive-digest)
                      (physical-follow-active-prefix-digest store cursor index))
                   (= (seventh saved)
                      (physical-follow-active-prefix-digest store (sixth saved) index)))
        (error "active follow lost its source-verified prefix or saved floor"))
      (values current index))))

(defun physical-follow-check-current-restartpoint (store base old)
  (multiple-value-bind (metadata catalog prefix digest) (physical-restartpoint-read base)
    (declare (ignore catalog prefix))
    (let ((token (when metadata (list (getf metadata :lsn) digest))))
      (when old
        (unless (and token (<= (first old) (first token))
                     (or (/= (first old) (first token)) (equal old token)))
          (error "active follow restartpoint regressed or changed at its saved boundary")))
      (when (> (physical-store-floor-lsn store) (getf base :end))
        (unless (and metadata (>= (getf metadata :version) 3)
                     (<= (physical-store-floor-lsn store) (getf metadata :lsn)))
          (error "active follow compaction lacks a usable durable restartpoint")))
      (values metadata token))))

(defun physical-follow-map-active-prefix (store index function start &optional policy)
  "Map retained WAL from the base to the current floor, then the spool.
The caller supplies the checked current inventory, validates the whole spool
before this mapper runs, and keeps the root protected.
The archived prefix can be scanned in full because its floor is at or before
the policy-checked usable restartpoint seed; legacy fallback has floor at base.
The store validates its whole spool before callbacks stop at a policy fence."
  (unless (= start (physical-store-base-lsn store))
    (error "active historical scan must start at the verified base"))
  (when (> (physical-store-floor-lsn store) start)
    (unless index (error "active historical replay lacks retained WAL"))
    (dolist (entry (getf index :entries))
      (multiple-value-bind (metadata bytes)
          (physical-retained-extent-decode
           (physical-retained-object-read (physical-store-retained-directory store)
                                          :extent (first entry) +physical-retained-image-limit+))
        (physical-retained-extent-scan metadata bytes (physical-store-retained-history store)
                                       nil function))))
  (physical-recovery-map-permitted-records store function :start-lsn (physical-store-floor-lsn store) :policy policy))

(defun physical-follow-scan-current-state (candidate base applied required control index minimum &optional policy)
  "Check applied, required and seed positions, then scan their semantic history.
A fixed policy bounds spool callbacks, while full durable validation still runs.
Return the scan disposition; it is not a numeric reconciliation success value."
  (physical-recovery-check-store-point candidate (first applied) (second applied))
  (multiple-value-bind (metadata token)
      (physical-follow-check-current-restartpoint candidate base minimum)
    (let* ((applied (max (first applied) (first required)
                         (if (and metadata (>= (getf metadata :version) 3))
                             (getf metadata :lsn) (getf base :end))))
           (pair (list applied (physical-store-prefix-timeline candidate applied)))
           (seed (multiple-value-list (physical-follow-replay-seed candidate base pair token))))
      (when (getf policy :target)
        (dolist (point (list pair required (subseq seed 0 2)))
          (physical-recovery-policy-check-pair point (getf policy :history))))
      (if (< (first required) (first seed))
          (physical-follow-scan-replay-from-seed
           candidate base pair required (getf control :next-xid)
           (list (getf base :end) (getf base :timeline) (getf base :next-xid)
                 (copy-tree (getf base :source-checkpoint))
                 (copy-tree (getf base :source-reservation)) nil)
           (lambda (function start) (physical-follow-map-active-prefix candidate index function start policy))
           t (subseq seed 0 3))
          (physical-follow-scan-replay-from-seed
           candidate base pair required (getf control :next-xid) seed
           (lambda (function start)
             (physical-recovery-map-permitted-records candidate function :start-lsn start :policy policy))
           t)))))

(defun physical-follow-call-with-receive-root (store function)
  "Call FUNCTION with a reader lease on STORE's retained root when present.
The caller must keep the selected store and root stable while acquiring it."
  (if (physical-store-retained-root store)
      (call-with-physical-retained-root-reader
       function (physical-store-retained-owner store) (physical-store-retained-directory store)
       (physical-store-retained-identity store) (physical-store-retained-root store))
      (funcall function)))

(defun physical-follow-copy-checked-store (store)
  "Check live, complete input and retained control under the store mutex.
Return a private read-only candidate; the caller keeps input and root stable."
  (sb-thread:with-mutex ((physical-store-mutex store))
    (physical-store-check-live store)
    (physical-store-check-complete-input store)
    (physical-store-read-retained-control store (physical-store-control-tuple store))
    (physical-follow-copy-replay-candidate store)))

(defun physical-follow-check-active-state (store verified-base config &key (policy (getf *physical-recovery-state* :timeline-policy)))
  "Validate an active binding without publication, redo or source contact.
The caller must exclude concurrent recovery, reload and receive changes. This
function copies a read-only candidate, holds a root reader lease when needed,
and rechecks durable witnesses and the receive tuple after the scan. Startup
and receiver launch call this before admitting a bound configuration.  The
result includes the scan disposition and does not alter replay exhaustion."
  (physical-follow-check-config config)
  (unless (and (member (length config) '(5 6)) (eq :active (getf (fifth config) :phase))
               (equal config (physical-follow-read-config)))
    (error "active follow config is absent, pending or changed"))
  (labels ((validate ()
             (let* ((binding (fifth config))
                    (control (physical-recovery-read-control *pgdata*))
                    (candidate
                      (physical-follow-copy-checked-store store)))
               (multiple-value-bind (tuple index)
                   (physical-follow-check-active-receive candidate verified-base binding)
                 (let ((required (physical-follow-check-active-control candidate verified-base binding control)))
                   (physical-follow-check-history-files candidate binding)
                   (let ((result (physical-follow-scan-current-state
                                  candidate verified-base (getf binding :applied) required control index
                                  (getf (getf binding :recovery) :restartpoint) policy)))
                     (unless (and (equal control (physical-recovery-read-control *pgdata*))
                                  (equal config (physical-follow-read-config)))
                       (error "active follow durable witnesses changed during validation"))
                     (sb-thread:with-mutex ((physical-store-mutex store))
                       (unless (equal tuple (physical-store-control-tuple store))
                         (error "active follow receive state changed during validation"))
                       (physical-store-read-retained-control store tuple))
                     result))))))
    (physical-follow-call-with-receive-root store #'validate)))

(defun physical-follow-check-pending-state (store verified-base config)
  "Validate a saved pending binding against durable recovery and receive state.
Accept only the saved old or post receive tuple; post recovery with old receive
is invalid. Check the actual restartpoint and scan a disposable candidate
outside the store mutex, then recheck control and config for changes.
The caller must exclude recovery and reload and protect the selected root."
  (physical-follow-check-config config)
  (unless (and (member (length config) '(5 6)) (eq :pending (getf (fifth config) :phase))
               (equal config (physical-follow-read-config)))
    (error "pending follow config is absent, active or changed"))
  (let* ((binding (fifth config)) (before (getf binding :before)) (after (getf binding :after))
                                  (control (physical-recovery-read-control *pgdata*))
                                  (recovery-position (physical-follow-pending-recovery-position verified-base binding control))
                                  (position nil)
                                  (candidate
                                    (sb-thread:with-mutex ((physical-store-mutex store))
                                      (physical-store-check-live store)
                                      (physical-store-check-complete-input store)
                                      (let ((current (physical-store-control-tuple store)))
                                        (physical-store-read-retained-control store current)
                                        (cond
                                          ((equal current after)
                                           (setf position :after)
                                           (physical-follow-copy-replay-candidate store))
                                          ((equal current before)
                                           (when (eq recovery-position :after)
                                             (error "pending follow recovery history precedes its receive publication"))
                                           (setf position :before)
                                           (multiple-value-bind (history fork eor)
                                               (physical-follow-anchor-check (getf binding :anchor))
                                             (declare (ignore history fork eor))
                                             (physical-store-follow-candidate
                                              store before (getf binding :applied) (getf binding :required)
                                              (eighth after) (getf binding :child-history) (coerce (getf (getf binding :anchor) :eor-octets) '(vector (unsigned-byte 8))))))
                                          (t (error "pending follow receive control differs from both saved states")))))))
    (physical-follow-validate-candidate-replay candidate verified-base binding)
    (unless (and (equal control (physical-recovery-read-control *pgdata*))
                 (equal config (physical-follow-read-config)))
      (error "pending follow durable witnesses changed during validation"))
    (values candidate position control)))

(defvar *physical-follow-test-hook* nil
  "Optional test hook for reconciliation and config publication cuts.")

(defun physical-follow-test-point (point)
  "Invoke the optional test hook at a reconciliation or config publication cut."
  (when *physical-follow-test-hook* (funcall *physical-follow-test-hook* point)))

(defun physical-follow-check-history-files (store binding)
  "Preflight published history files before mutation."
  (labels ((check-file (target text)
             (let ((path (timeline-history-path target *pgdata*)))
               (when (wal-namespace-file-stat path t)
                 (unless (equalp (string->bytes text) (recovery-read-bytes path))
                   (error "pending follow conflicts with a published history file"))))))
    (let ((selection (physical-store-selection store)))
      (when selection
        (unless (physical-store-history-extends-p
                 (timeline-read-history (physical-store-timeline store)) (getf selection :history))
          (error "pending follow changes immutable base ancestry"))
        (loop for item = selection then (getf item :previous) while item
              do (check-file (getf item :target) (getf item :history-text)))))
    (let ((anchor (getf binding :anchor)))
      (multiple-value-bind (history fork eor) (physical-follow-anchor-check anchor)
        (declare (ignore fork))
        (case (physical-follow-anchor-kind anchor)
          (:same-timeline
           (when (> (getf anchor :source-timeline) 1)
             (check-file (getf anchor :source-timeline) (getf anchor :history-text))))
          (:descendant
           (check-file (timeline-check-end-of-recovery eor (getf anchor :receive-timeline) history)
                       (getf binding :child-history)))
          (:retained-timeline nil))))))

(defun physical-follow-publish-pending-recovery (base binding)
  "Publish the required recovery pair and witnessed XID with the selected history.
Read control back and verify the resulting history and recovery provenance."
  (let* ((state (copy-tree base)) (required (getf binding :required))
                                  (witness (getf binding :recovery)) (after (getf binding :after))
                                  (rewind (not (equal (getf binding :before) after)))
                                  (target (if rewind (eighth after) (getf witness :history-target)))
                                  (digest (if rewind (ninth after) (getf witness :history-digest))))
    (setf (getf state :phase) 2
          (getf state :required-applied) (first required)
          (getf state :required-timeline) (second required)
          (getf state :replay-timeline) (second required)
          (getf state :observed-next-xid) (getf witness :observed-next-xid)
          (getf state :history-target) target (getf state :history-digest) digest)
    (physical-recovery-write-control state 2 (first required) (getf witness :observed-next-xid))
    (let ((control (physical-recovery-read-control *pgdata*)))
      (physical-follow-pending-recovery-position base binding control)
      (unless (and (eql target (getf control :history-target))
                   (eql digest (getf control :history-digest)))
        (error "pending follow recovery history readback differs"))
      control)))

(defun physical-follow-reconcile-pending-store (store verified-base config)
  "Reconcile a pending binding under caller-owned recovery and reload exclusion.
Protect a retained root with a reader lease. Preflight the candidate and history
before mutation, then publish receive state, history files, recovery control and
active config in order, with readbacks.
A failure after mutation begins latches STORE failed; reopen to resolve durable
state. The active config still needs durable-state validation before admission."
  (labels ((reconcile ()
             (let ((started nil) (finished nil))
               (unwind-protect
                    (multiple-value-bind (candidate position)
                        (physical-follow-check-pending-state store verified-base config)
                      (physical-follow-check-history-files candidate (fifth config))
                      (physical-follow-test-point :after-pending-preflight)
                      (setf started t)
                      (let* ((binding (fifth config)) (after (getf binding :after))
                                                      (anchor (getf binding :anchor)))
                        (if (eq position :before)
                            (physical-store-rewind-select-history
                             store (getf binding :before) (getf binding :applied) (getf binding :required)
                             (eighth after) (getf binding :child-history)
                             (coerce (getf anchor :eor-octets) '(vector (unsigned-byte 8))))
                            (with-physical-store (store)
                              (physical-store-check-retained-root store)
                              (physical-store-discard-tail store)))
                        (physical-follow-test-point :after-pending-receive)
                        (physical-recovery-publish-selected-history store)
                        (physical-follow-test-point :after-pending-history)
                        (physical-follow-publish-pending-recovery verified-base binding)
                        (physical-follow-test-point :after-pending-recovery)
                        (sb-thread:with-mutex ((physical-store-mutex store))
                          (physical-store-read-retained-control store after))
                        (unless (equal config (physical-follow-read-config))
                          (error "pending follow config changed before activation"))
                        (let ((active (copy-tree config)))
                          (setf (getf (fifth active) :phase) :active)
                          (physical-follow-save-config active)
                          (physical-follow-test-point :after-pending-active)
                          (unless (equal active (physical-follow-read-config))
                            (error "pending follow active config readback differs"))
                          (setf finished t)
                          active)))
                 (when (and started (not finished))
                   (sb-thread:with-mutex ((physical-store-mutex store))
                     (setf (physical-store-failed-p store) t)))))))
    (physical-follow-call-with-receive-root store #'reconcile)))

(defun physical-follow-reconcile-pending-offline ()
  "Reconcile a saved pending binding privately before storage or workers start.
Require owned PGDATA and exclude reload and live operations. Open and close a
temporary store without contacting the source, running redo or installing runtime
state. An active result still requires startup validation before workers launch."
  (unless (and *postmaster-data-lock* (postmaster-pid-owned-p)
               (zerop *live-operation-depth*) (not *wal-ready*)
               (not *physical-receive-store*) (not *physical-follow-runtime*)
               (not *physical-restartpoint-worker*))
    (error "offline follow reconciliation requires owned PGDATA before storage and workers"))
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (sb-thread:with-mutex (*reload-run-lock*)
      (with-live-operation-quiescence
        (let ((config (physical-follow-read-config)))
          (unless (and (member (length config) '(5 6)) (eq :pending (getf (fifth config) :phase)))
            (error "offline follow reconciliation requires a saved pending binding"))
          (let* ((base (physical-recovery-verify-base-state))
                 (store (physical-store-open
                         (physical-recovery-spool-directory) (getf base :system-id) (getf base :timeline)
                         :start-lsn (getf base :end) :discard-tail nil :create-lock nil
                         :retained-identity (physical-recovery-retained-identity base)
                         :retained-directory (physical-recovery-retained-directory))))
            (unwind-protect
                 (physical-follow-reconcile-pending-store store base config)
              (physical-store-close store))))))))

(defun physical-follow-check-config (config)
  (unless (and (listp config) (member (length config) '(4 5 6)) (eql 1 (first config)))
    (error "invalid native physical follow configuration"))
  (unless (physical-follow-no-source-p config)
    (physical-follow-endpoint-check (second config) (third config) (fourth config))
    (when (>= (length config) 5)
      (if (fifth config) (physical-follow-binding-check (fifth config))
          (unless (= 6 (length config)) (error "invalid empty native follow binding")))))
  (when (= 6 (length config))
    (let* ((state (physical-follow-controller-copy (sixth config)))
           (preparing (getf state :preparing)))
      (when (and (getf state :mode)
                 (not (and (physical-follow-unattached-p config)
                           (not (getf state :enabled))
                           (getf state :unowned-bootstrap))))
        (error "invalid unattached standby controller state"))
      (when (and (physical-follow-no-source-p config)
                 (or (getf state :source) (getf state :public-slot)))
        (error "a controller without a source cannot carry accepted receipt settings"))
      (when (getf state :unowned-bootstrap)
        (let ((pin (getf (getf state :unowned-bootstrap) :pin)))
          (when (or (equal pin (fourth config))
                    (equal pin (getf preparing :pin))
                    (find pin (getf state :cleanup) :key (lambda (entry) (getf entry :pin))
                                                    :test #'equal))
            (error "an unowned bootstrap pin cannot be reused or assigned a cleanup route"))))
      (when (and preparing (fifth config)
                 (not (eq :active (getf (fifth config) :phase))))
        (error "preparation must preserve an active current binding"))
      (when (and preparing
                 (or (equal (getf preparing :pin) (fourth config))
                     (find (getf preparing :pin) (getf state :cleanup)
                           :key (lambda (entry) (getf entry :pin)) :test #'equal)))
        (error "preparation must use a fresh pin name"))
      (when (and (not (physical-follow-cleanup-only-p config))
                 (find (fourth config) (getf state :cleanup)
                       :key (lambda (entry) (getf entry :pin)) :test #'equal))
        (error "cleanup cannot target the current source pin"))
      (when (and (physical-follow-cleanup-only-p config)
                 (or (getf state :enabled) (getf state :preparing)))
        (error "cleanup-only control cannot enable receipt or preparation"))))
  config)

(defun physical-follow-config-image (config)
  "Encode a checked config in the existing bounded PGPHYSICALFOLLOW1 envelope."
  (physical-follow-check-config config)
  (let* ((text (physical-bootstrap-manifest-text config))
         (crc (wal-crc32 (string->bytes text)))
         (bytes (string->bytes (format nil "PGPHYSICALFOLLOW1 ~8,'0X~%~a~%" crc text))))
    (unless (<= (length bytes) +physical-follow-config-maximum-bytes+)
      (error "native physical follow configuration exceeds its reopen bound"))
    bytes))

(defun physical-follow-save-config (config)
  "Atomically write a checked config image.
Optional test hooks mark fsync, rename and directory-sync cuts."
  (let ((path (physical-follow-config-path)) (bytes (physical-follow-config-image config)))
    (with-atomic-output
        (stream path (make-pathname :type "tmp" :defaults path) *pgdata*
                :element-type '(unsigned-byte 8) :mode #o600
                :fsync-function
                (lambda (fd)
                  (physical-follow-test-point :before-follow-config-fsync)
                  (sb-posix:fsync fd)
                  (physical-follow-test-point :after-follow-config-fsync))
                :rename-function
                (lambda (from to)
                  (physical-follow-test-point :before-follow-config-rename)
                  (sb-posix:rename from to)
                  (physical-follow-test-point :after-follow-config-rename))
                :fsync-directory-function
                (lambda (directory)
                  (fsync-directory directory)
                  (physical-follow-test-point :after-follow-config-directory-fsync)))
      (write-sequence bytes stream))))

(defun physical-follow-decode-config (bytes)
  "Check the bounded envelope and checksum, then validate its literal payload.
Validation checks coherence, not durable state or source proof."
  (unless (<= (length bytes) +physical-follow-config-maximum-bytes+)
    (error "native physical follow configuration exceeds its reopen bound"))
  (let* ((text (bytes->string bytes)) (newline (position #\Newline text))
                                      (prefix "PGPHYSICALFOLLOW1 "))
    (unless (and newline (= newline (+ (length prefix) 8)) (uiop:string-prefix-p prefix text))
      (error "corrupt native physical follow envelope"))
    (let* ((body (subseq text (1+ newline)))
           (crc (parse-integer text :start (length prefix) :end newline :radix 16)))
      (unless (and (plusp (length body)) (char= #\Newline (char body (1- (length body))))
                   (= crc (wal-crc32 (string->bytes (subseq body 0 (1- (length body)))))))
        (error "native physical follow checksum mismatch"))
      (physical-follow-check-config (recovery-parse-manifest-text body)))))

(defun physical-follow-read-config ()
  "Read the whole bounded config file before decoding it."
  (physical-follow-decode-config
   (recovery-read-bytes (physical-follow-config-path) :maximum +physical-follow-config-maximum-bytes+)))

(defun physical-follow-require-bootstrap-config (config)
  "Require the legacy four-field bootstrap configuration for this helper."
  (physical-follow-check-config config)
  (unless (= 4 (length config))
    (error "native follow binding requires reconciliation and bound receiver support"))
  config)

(defun physical-follow-check-startup-config ()
  "Check a saved configuration using the legacy bootstrap-only guard."
  (when (probe-file (physical-follow-config-path))
    (physical-follow-require-bootstrap-config (physical-follow-read-config))))

(defun physical-follow-feedback (client)
  "Snapshot independent write/flush/apply, then perform socket I/O outside admission."
  (when *physical-stream-test-hook* (funcall *physical-stream-test-hook* :before-feedback))
  (let ((payload (with-live-operation
                   (let ((store *physical-receive-store*) (result (make-payload)))
                     (check-recovery-available)
                     (sb-thread:with-mutex ((physical-store-mutex store))
                       (physical-store-check-live store)
                       (payload-push-byte result (char-code #\r))
                       (pg-client-push-u64 result (physical-store-written-lsn store))
                       (pg-client-push-u64 result (physical-store-flushed-lsn store))
                       (pg-client-push-u64 result *last-replay-lsn*)
                       (pg-client-push-u64 result (postgres-current-timestamp))
                       (payload-push-byte result 0))
                     result))))
    (sb-ext:with-timeout 10
      (physical-client-feedback client payload))))

(defun physical-follow-process-frame (payload &optional runtime)
  (unless (and payload (plusp (length payload))) (physical-follow-reject "empty native physical frame"))
  (case (code-char (aref payload 0))
    (#\w
     (unless (<= 26 (length payload) (+ 25 65536))
       (physical-follow-reject "invalid native XLogData size"))
     (let ((start (pg-client-u64 payload 1)) (end (pg-client-u64 payload 9))
                                             (bytes (subseq payload 25)))
       (unless (<= (+ start (length bytes)) end #xffffffffffffffff)
         (physical-follow-reject "native XLogData exceeds durable source end"))
       (when runtime
         (when (and (physical-follow-runtime-limit runtime)
                    (> (+ start (length bytes)) (physical-follow-runtime-limit runtime)))
           (physical-follow-reject "native receipt crossed its unselected child fork"))
         (physical-follow-monitor-message runtime end (pg-client-i64 payload 17)))
       (physical-recovery-receive start bytes)
       ;; Match walreceiver's write-then-flush cycle rather than forcing a
       ;; manifest fsync for every tiny physical record. Feedback still exposes
       ;; written versus durable flush independently.
       (when (>= (- (physical-store-written-lsn *physical-receive-store*)
                    (physical-store-flushed-lsn *physical-receive-store*))
                 *physical-receive-flush-bytes*)
         (physical-recovery-flush))))
    (#\k
     (unless (and (= (length payload) 18) (member (aref payload 17) '(0 1)))
       (physical-follow-reject "invalid native keepalive"))
     (when runtime
       (physical-follow-monitor-message runtime (pg-client-u64 payload 1) (pg-client-i64 payload 9)))
     ;; Caught-up traffic must become durable even below the batching threshold.
     ;; The source WAL-end hint is never substituted for the local write cursor.
     (physical-recovery-flush))
    (otherwise (physical-follow-reject "unsupported native physical message"))))

(define-condition physical-follow-source-changed (simple-error)
  ((timeline :initarg :timeline :reader physical-follow-changed-timeline)))

(define-condition physical-follow-route-changed (simple-error) ())

(defun physical-follow-runtime-binding (runtime)
  (fifth (physical-follow-runtime-config runtime)))

(defun physical-follow-runtime-receive-timeline (runtime)
  "Return the selected receive timeline, which can differ from applied replay."
  (let ((binding (physical-follow-runtime-binding runtime)))
    (if binding
        (let ((after (getf binding :after))) (or (eighth after) (second after)))
        (getf *physical-recovery-state* :timeline))))

(defun physical-follow-runtime-boundary (runtime)
  (let ((binding (physical-follow-runtime-binding runtime)))
    (when binding
      (multiple-value-bind (history fork eor) (physical-follow-anchor-check (getf binding :anchor))
        (declare (ignore fork eor))
        (let* ((position (position (physical-follow-runtime-receive-timeline runtime) history
                                   :key #'timeline-history-entry-timeline))
               (entry (when position (nth position history))))
          (unless entry (physical-follow-reject "receive timeline is absent from its source binding"))
          (values (timeline-history-entry-end entry)
                  (when (and position (plusp position))
                    (timeline-history-entry-timeline (nth (1- position) history)))))))))

(defun physical-follow-client-identity (client)
  (let ((row (sb-ext:with-timeout 10 (physical-client-identity-row client))))
    (values (pg-client-native-u64 (aref row 0))
            (pg-client-native-u64 (aref row 1) t)
            (pg-client-native-lsn (aref row 2)))))

(defun physical-follow-start-query (runtime cursor)
  "Build a native START with the private proof pin and optional public slot.
The public slot is a separate persistent source slot, not the proof pin."
  (with-live-operation
    (format nil
            "START_REPLICATION~a PHYSICAL ~a (native_format '~a', proof_pin '~a', system_id '~d', timeline '~d', base_lsn '~a', prefix_digest '~d'~a~a)"
            (let ((slot (physical-follow-config-public-slot (physical-follow-runtime-config runtime))))
              (if slot (format nil " SLOT \"~a\"" slot) ""))
            (wal-format-lsn cursor) +physical-native-format+
            (physical-follow-runtime-pin runtime) (getf *physical-recovery-state* :system-id)
            (physical-follow-runtime-receive-timeline runtime)
            (wal-format-lsn (getf *physical-recovery-state* :end))
            (physical-store-flushed-digest *physical-receive-store*)
            (if (getf *physical-recovery-state* :manifest-digest)
                (format nil ", manifest_digest '~d'" (getf *physical-recovery-state* :manifest-digest))
                "")
            (if (physical-follow-runtime-binding runtime) ", mode 'follow'" ""))))

(defun physical-follow-check-identity (rows)
  (unless (and (= (length rows) 1) (= (length (first rows)) 4)
               (equal (aref (first rows) 0)
                      (format nil "~d" (getf *physical-recovery-state* :system-id)))
               (equal (aref (first rows) 1)
                      (format nil "~d" (getf *physical-recovery-state* :timeline))))
    (physical-follow-reject "source identity or timeline differs from the native recovery base")))

(defun physical-follow-flush-on-exit (runtime)
  "Flush complete writes before receiver exit; record fatal cleanup errors without masking an unwind."
  (with-live-operation
    (when *physical-recovery-failed-p*
      (return-from physical-follow-flush-on-exit :failed))
    (handler-case
        (let ((store *physical-receive-store*))
          (unless store (error "receiver exit lost its native receive spool"))
          (multiple-value-bind (failed written flushed)
              (sb-thread:with-mutex ((physical-store-mutex store))
                (values (or (physical-store-failed-p store) (physical-store-closed-p store))
                        (physical-store-written-lsn store) (physical-store-flushed-lsn store)))
            (cond (failed :failed)
                  ((= written flushed) flushed)
                  (t (physical-recovery-flush)))))
      (error (condition)
        ;; C's WAL fsync failure is PANIC. Keep this native recovery image
        ;; closed until restart, even if a stop request is pending.
        (setf *physical-recovery-failed-p* t)
        (physical-follow-note-error runtime condition t)
        :failed))))

(defun physical-follow-check-exit ()
  "A joined receiver must not turn poisoned receive storage into a successful stop."
  (with-live-operation
    (check-recovery-available)
    (unless *physical-receive-store* (error "receiver exit lost its native receive spool"))
    (sb-thread:with-mutex ((physical-store-mutex *physical-receive-store*))
      (physical-store-check-live *physical-receive-store*))))

(defun physical-follow-register-runtime-client (runtime client)
  (sb-thread:with-mutex (*physical-follow-lock*)
    (unless (eq runtime *physical-follow-runtime*)
      (setf (physical-follow-runtime-stopped-p runtime) t)
      (error "native physical receiver no longer owns its runtime"))
    (when (physical-follow-receiver-stopped-p runtime)
      (error 'physical-follow-receiver-stopped))
    (setf (physical-follow-runtime-client runtime) client))
  client)

(defun physical-follow-open-runtime-client (runtime publish)
  "Reconnect using the original settings, then compare the selected route.
A changed route forces fresh preparation before queries or WAL. Route equality
is not authentication or proof of source identity."
  (let ((source (physical-follow-config-source (physical-follow-runtime-config runtime))))
    (if source
        (let* ((profile (make-physical-libpq-profile (getf source :conninfo)
                                                     :fallback (getf source :fallback)))
               (client (physical-libpq-connect (physical-libpq-helper-path) profile :on-spawn publish))
               (owner (physical-libpq-frozen-profile-from-datum (getf source :owner))))
          (unless (equal (physical-libpq-connection-route (physical-libpq-channel-info client))
                         (physical-libpq-frozen-route owner))
            (error 'physical-follow-route-changed
                   :format-control "native physical source route changed"))
          client)
        (let ((client (sb-ext:with-timeout 10
                        (pg-client-connect (physical-follow-runtime-host runtime)
                                           (physical-follow-runtime-port runtime) "postgres"
                                           :application-name "pg_lisp_physical_receiver"
                                           :replication-p t))))
          (funcall publish client)
          (setf (sb-bsd-sockets:sockopt-tcp-nodelay (pg-client-socket client)) t)
          client))))

(defun physical-follow-call-with-client (runtime cursor function)
  (let ((client nil))
    (unwind-protect
         (progn
           (physical-follow-monitor-start runtime cursor)
           (setf client
                 (physical-follow-open-runtime-client
                  runtime (lambda (opened)
                            (setf client opened)
                            (physical-follow-register-runtime-client runtime opened))))
           (physical-follow-register-runtime-client runtime client)
           (funcall function client))
      (unwind-protect
           (physical-follow-flush-on-exit runtime)
        (physical-follow-monitor-stop runtime)
        (sb-thread:with-mutex (*physical-follow-lock*)
          (when (eq client (physical-follow-runtime-client runtime))
            (setf (physical-follow-runtime-client runtime) nil)))
        (physical-client-close client)))))

(defun physical-follow-finish-bound-stream (client runtime)
  "Flush and verify the parent fork before accepting the source's timeline switch."
  (let ((limit (physical-follow-runtime-limit runtime))
        (next (physical-follow-runtime-next-timeline runtime)))
    (unless (and limit next)
      (physical-follow-reject "unexpected completion of an open source timeline"))
    (with-live-operation
      (physical-recovery-flush)
      (let ((store *physical-receive-store*))
        (sb-thread:with-mutex ((physical-store-mutex store))
          (physical-store-check-live store)
          (physical-store-check-complete-input store)
          (unless (= limit (physical-store-flushed-lsn store))
            (physical-follow-reject "parent stream ended before its durable fork")))))
    (physical-follow-feedback client)
    (sb-ext:with-timeout 10 (physical-client-finish client next limit))))

(defun physical-follow-copy-stream (client runtime)
  (physical-follow-monitor-connected runtime)
  (loop until (physical-follow-receiver-stopped-p runtime)
        do (multiple-value-bind (kind payload)
               (physical-client-receive client)
             (case kind
               (:data (with-live-operation (physical-follow-process-frame payload runtime)))
               (:done
                (unless (physical-follow-runtime-binding runtime)
                  (physical-follow-reject "unexpected native CopyDone"))
                (return-from physical-follow-copy-stream
                  (physical-follow-finish-bound-stream client runtime)))
               (:idle nil)
               (otherwise (physical-follow-reject "unexpected native replication frame"))))
           (physical-follow-feedback client)))

(defun physical-follow-bound-completion (runtime result)
  "Accept retained completion only at the bound next timeline and fork.
Receipt ends there; replay remains free to reach its own policy fence."
  (when (and (eq :retained-timeline
                 (physical-follow-anchor-kind (getf (physical-follow-runtime-binding runtime) :anchor)))
             result)
    (let ((fork (physical-follow-runtime-limit runtime))
          (next (physical-follow-runtime-next-timeline runtime)))
      (unless (equal result (list :switch next fork))
        (physical-follow-reject "retained completion differs from its bound fork"))
      (return-from physical-follow-bound-completion
        (list :timeline-end :timeline (physical-follow-runtime-receive-timeline runtime)
                            :fork fork :next next))))
  result)

(defun physical-follow-bound-stream (client runtime cursor)
  (multiple-value-bind (system-id timeline) (physical-follow-client-identity client)
    (unless (= system-id (getf *physical-recovery-state* :system-id))
      (physical-follow-reject "native source system identity changed"))
    (let ((expected (getf (getf (physical-follow-runtime-binding runtime) :anchor) :source-timeline)))
      (unless (= expected timeline)
        (if (> timeline expected)
            (error 'physical-follow-source-changed :timeline timeline
                                                   :format-control "native source advanced to timeline ~d" :format-arguments (list timeline))
            (physical-follow-reject "native source timeline regressed")))))
  (multiple-value-bind (limit next) (physical-follow-runtime-boundary runtime)
    (setf (physical-follow-runtime-limit runtime) limit
          (physical-follow-runtime-next-timeline runtime) next)
    (let* ((query (physical-follow-start-query runtime cursor))
           (result (sb-ext:with-timeout 10 (physical-client-start client query))))
      (physical-follow-bound-completion runtime
                                        (if (eq :copy-both result)
                                            (physical-follow-copy-stream client runtime)
                                            (progn
                                              (unless (and limit (= cursor limit) (equal result (list :switch next limit)))
                                                (physical-follow-reject "immediate source completion differs from its bound fork"))
                                              result))))))

(defun physical-follow-session (runtime cursor)
  (physical-follow-call-with-client
   runtime cursor
   (lambda (client)
     (if (physical-follow-runtime-binding runtime)
         (physical-follow-bound-stream client runtime cursor)
         (let ((query (physical-follow-start-query runtime cursor)))
           (sb-ext:with-timeout 10
             (physical-follow-check-identity (list (physical-client-identity-row client)))
             (unless (eq :copy-both (physical-client-start client query))
               (physical-follow-reject "unbound native stream returned a timeline switch")))
           (physical-follow-copy-stream client runtime))))))

(defun physical-follow-terminal-p (condition)
  (or (typep condition 'physical-follow-terminal-error)
      *physical-recovery-failed-p*
      (and *physical-receive-store* (physical-store-failed-p *physical-receive-store*))
      (and (typep condition 'pg-client-error)
           (not (physical-client-retryable-condition-p condition)))))

(defun physical-follow-note-error (runtime condition &optional terminal)
  (setf (physical-follow-runtime-last-error runtime) (princ-to-string condition))
  (when terminal (setf (physical-follow-runtime-stopped-p runtime) t))
  (admin-log "native physical receiver ~a: ~a" (if terminal "stopped" "retrying")
             (physical-follow-runtime-last-error runtime)))

(defun physical-follow-receiver-loop (runtime)
  (loop until (or (physical-follow-receiver-stopped-p runtime) (shutdown-in-progress-p))
        do (handler-case
               (let ((cursor (with-live-operation
                               (check-recovery-available)
                               (physical-store-reset-unflushed *physical-receive-store*))))
                 (let ((result (physical-follow-session runtime cursor)))
                   (when result
                     (sb-thread:with-mutex (*physical-follow-lock*)
                       (when (and (eq runtime *physical-follow-runtime*)
                                  (not (physical-follow-receiver-stopped-p runtime)))
                         (setf (physical-follow-runtime-transition runtime) result)))
                     (return-from physical-follow-receiver-loop :switch))))
             (physical-follow-source-changed (condition)
               (sb-thread:with-mutex (*physical-follow-lock*)
                 (when (and (eq runtime *physical-follow-runtime*)
                            (not (physical-follow-receiver-stopped-p runtime)))
                   (setf (physical-follow-runtime-transition runtime)
                         (list :refresh (physical-follow-changed-timeline condition)))))
               (return-from physical-follow-receiver-loop :refresh))
             (physical-follow-route-changed ()
               (sb-thread:with-mutex (*physical-follow-lock*)
                 (when (and (eq runtime *physical-follow-runtime*)
                            (not (physical-follow-receiver-stopped-p runtime)))
                   (setf (physical-follow-runtime-transition runtime) '(:refresh-route))))
               (return-from physical-follow-receiver-loop :refresh))
             (physical-client-stopped ()
               (return-from physical-follow-receiver-loop :stopped))
             ((or error sb-ext:timeout) (condition)
               (let ((terminal (physical-follow-terminal-p condition)))
                 (when (or (not (physical-follow-receiver-stopped-p runtime))
                           (and terminal (not *physical-recovery-failed-p*)))
                   (physical-follow-note-error runtime condition terminal)))))
           (unless (physical-follow-receiver-stopped-p runtime) (sleep 0.2))))

(defun physical-follow-replay-loop (runtime)
  (loop until (or (physical-follow-runtime-stopped-p runtime) (shutdown-in-progress-p))
        do (handler-case
               (when (and (not (sb-thread:with-mutex (*physical-replay-pause-lock*)
                                 *physical-replay-paused-p*))
                          (physical-recovery-replay-pending-p))
                 (physical-recovery-replay))
             (error (condition)
               (physical-follow-note-error runtime condition t)))
           (sleep 0.05)))

(defun physical-follow-check-launch (config)
  "Admit a bootstrap or reconciled active binding; validate bound state under
reload exclusion and quiescence before workers can read it."
  (physical-follow-check-config config)
  (when (or (physical-follow-cleanup-only-p config)
            (getf (physical-follow-controller-state config) :preparing)
            (not (physical-follow-controller-enabled-p config)))
    (error "native follow configuration does not admit a receiver"))
  (when (fifth config)
    (unless (and (eq :active (getf (fifth config) :phase))
                 (recovery-in-progress-p) *physical-receive-store*
                 (zerop *live-operation-depth*))
      (error "bound follow launch requires active streaming recovery outside admission"))
    (sb-thread:with-mutex (*reload-run-lock*)
      (with-live-operation-quiescence
        (physical-follow-check-active-state
         *physical-receive-store* (physical-recovery-verify-base-state) config))))
  config)

(defun physical-follow-launch-workers (config receive-p &optional transition)
  "Publish replay and, when RECEIVE-P, receipt workers together.
The caller holds the command mutex outside admission and has validated CONFIG."
  (unless (and (zerop *live-operation-depth*)
               (sb-thread:holding-mutex-p *physical-follow-command-lock*))
    (error "follow worker launch requires command exclusion outside admission"))
  (sb-thread:with-mutex (*physical-follow-lock*)
    (when *physical-follow-runtime* (error "native physical follow already has a runtime; stop it first"))
    (let ((runtime (make-physical-follow-runtime :host (second config) :port (third config)
                                                 :pin (fourth config) :config (copy-tree config)
                                                 :receiver-stopped-p (not receive-p) :transition transition))
          (ready nil) (success nil))
      (unwind-protect
           (flet ((run (function)
                    (loop until ready do (sleep 0.01))
                    (unless (physical-follow-runtime-stopped-p runtime)
                      (funcall function runtime))))
             (when receive-p
               (setf (physical-follow-runtime-receiver-thread runtime)
                     (sb-thread:make-thread (lambda () (run #'physical-follow-receiver-loop))
                                            :name "native physical WAL receiver")))
             (setf (physical-follow-runtime-replay-thread runtime)
                   (sb-thread:make-thread (lambda () (run #'physical-follow-replay-loop))
                                          :name "native physical WAL replay")
                   *physical-follow-runtime* runtime success t ready t)
             :started)
        (unless success
          ;; No worker can touch storage before every requested worker is published.
          (setf (physical-follow-runtime-stopped-p runtime) t ready t))))))

(defun physical-follow-launch-decision (config)
  "Choose receipt, refresh or replay only from the admitted local state.
A completed retained fork launches replay without contacting the source."
  (with-live-operation
    (let* ((policy (getf *physical-recovery-state* :timeline-policy))
           (store *physical-receive-store*)
           (binding (fifth config)))
      (sb-thread:with-mutex ((physical-store-mutex store))
        (let* ((history (physical-store-retained-history store))
               (local (physical-follow-policy-selection policy history)))
          (unless (eq :discover (getf local :action))
            (return-from physical-follow-launch-decision local))
          (when (and (getf policy :target) (null binding))
            (return-from physical-follow-launch-decision (list :action :refresh)))
          (when binding
            (let* ((source-history (physical-follow-anchor-check (getf binding :anchor)))
                   (route (physical-follow-policy-selection policy history source-history))
                   (timeline (physical-store-selected-timeline store))
                   (entry (find timeline source-history :key #'timeline-history-entry-timeline))
                   (fork (and entry (timeline-history-entry-end entry))))
              (unless (eq :attach (getf route :action))
                (return-from physical-follow-launch-decision (list :action :refresh)))
              (when (and (eql timeline (getf policy :target)) fork
                         (= fork (physical-store-flushed-lsn store)))
                (return-from physical-follow-launch-decision
                  (list :action :replay-only :reason :timeline-target-reached)))))
          (list :action :receive))))))

(defun physical-follow-launch (config)
  "Launch an admitted bootstrap or active bound config with the command mutex held."
  (check-physical-recovery-worker-start)
  (physical-follow-check-launch config)
  (let ((decision (physical-follow-launch-decision config)))
    (if (eq :receive (getf decision :action))
        (physical-follow-launch-workers config t)
        (physical-follow-launch-workers
         config nil (if (eq :refresh (getf decision :action))
                        '(:policy-refresh)
                        (list :waiting (getf decision :reason)))))))

(defun physical-follow-start (host port pin)
  "Enable an accepted endpoint without replacing its binding or cleanup queue."
  (when (plusp *live-operation-depth*)
    (error "follow start requires its own operation admission"))
  (physical-follow-endpoint-check host port pin)
  (physical-follow-call-with-controller-blocked
   (lambda ()
     (sb-thread:with-mutex (*physical-follow-command-lock*)
       (check-physical-recovery-worker-start)
       (let* ((saved (when (probe-file (physical-follow-config-path))
                       (physical-follow-read-config)))
              (config (list 1 host port pin)))
         (when *physical-follow-runtime*
           (error "stop the existing native physical follow runtime first"))
         (when (and saved (> (length saved) 4))
           (unless (equal (subseq saved 0 4) config)
             (error "use follow retarget to replace an accepted endpoint"))
           (let ((state (physical-follow-controller-state saved)))
             (when (or (getf state :preparing)
                       (eq :pending (getf (fifth saved) :phase)))
               (error "follow start requires a reconciled binding"))
             (setf (getf state :enabled) t
                   config (physical-follow-with-controller saved state))))
         (when (eq :deferred (physical-recovery-enable-streaming))
           (return-from physical-follow-start :deferred))
         (physical-follow-check-launch config)
         (with-live-operation (physical-follow-controller-save-config config))
         (physical-follow-launch config)))))
  (physical-follow-controller-start-worker)
  :started)

(defun physical-follow-quiesce-receiver ()
  "Stop and join receipt, preserving replay, pause state, config and runtime.
The lifecycle owner must first block the controller and cancel its operation
outside command, reload and admission locks, then hold the command mutex here.
A controller operation already owns its transition when quiescing its runtime.
Signal and join outside admission; a failed join or exit flush signals an error."
  (when (plusp *live-operation-depth*)
    (error "receiver quiescence must join outside admission"))
  (unless (sb-thread:holding-mutex-p *physical-follow-command-lock*)
    (error "receiver quiescence requires follow command exclusion"))
  (let ((runtime *physical-follow-runtime*))
    (when runtime
      (let ((client (sb-thread:with-mutex (*physical-follow-lock*)
                      (setf (physical-follow-runtime-receiver-stopped-p runtime) t
                            (physical-follow-runtime-transition runtime) nil)
                      (physical-follow-runtime-client runtime))))
        (when client (ignore-errors (physical-client-stop client))))
      (let ((thread (physical-follow-runtime-receiver-thread runtime)))
        (when thread
          (sb-thread:join-thread thread :timeout 15 :default :timeout)
          (when (sb-thread:thread-alive-p thread)
            (error "native physical receiver has not stopped"))))
      (physical-follow-flush-on-exit runtime)
      (physical-follow-check-exit)
      (physical-follow-monitor-stop runtime)))
  :stopped)

(defun physical-follow-quiesce-runtime ()
  "Stop and join both workers and preserve durable follow configuration.
The caller owns the command mutex and joins outside admission. A lifecycle
owner first blocks and cancels the controller; a controller operation already
owns its transition."
  (when (plusp *live-operation-depth*) (error "follow quiescence must join outside admission"))
  (let ((runtime *physical-follow-runtime*))
    (when runtime
      (sb-thread:with-mutex (*physical-follow-lock*)
        (setf (physical-follow-runtime-stopped-p runtime) t))
      (physical-follow-quiesce-receiver)
      (let ((thread (physical-follow-runtime-replay-thread runtime)))
        (when thread
          (sb-thread:join-thread thread :timeout 15 :default :timeout)
          (when (sb-thread:thread-alive-p thread)
            (error "native physical replay worker has not stopped"))))
      (sb-thread:with-mutex (*physical-follow-lock*)
        (setf *physical-follow-runtime* nil))))
  :stopped)

(defun physical-follow-stop ()
  "Stop receipt and replay while preserving accepted bindings and cleanup work.
An unattached standby rejects this operation; use replay pause instead."
  (when (plusp *live-operation-depth*)
    (error "follow stop must join outside operation admission"))
  (with-live-operation
    (check-recovery-available)
    (when *physical-shutdown-state* (check-physical-recovery-work)))
  (physical-follow-call-with-controller-blocked
   (lambda ()
     (sb-thread:with-mutex (*physical-follow-command-lock*)
       (with-live-operation
         (check-recovery-available)
         (when *physical-shutdown-state* (check-physical-recovery-work)))
       (when (and (probe-file (physical-follow-config-path))
                  (physical-follow-unattached-p (physical-follow-read-config)))
         (error "unattached standby replay cannot be disabled by follow stop"))
       (physical-follow-quiesce-runtime)
       (sb-thread:with-mutex (*reload-run-lock*)
         (with-live-operation-quiescence
           (check-recovery-available)
           (when (probe-file (physical-follow-config-path))
             (let* ((config (physical-follow-read-config))
                    (state (physical-follow-controller-state config)))
               (when (or (getf state :preparing)
                         (eq (getf (fifth config) :phase) :pending))
                 (error "follow stop cannot discard an unresolved transition"))
               (when (fifth config) (physical-follow-controller-check-current config))
               (if (= (length config) 4)
                   (progn (delete-file (physical-follow-config-path))
                          (fsync-directory *pgdata*))
                   (progn
                     (setf (getf state :enabled) nil)
                     (physical-follow-controller-save-config
                      (physical-follow-with-controller config state))))))))
       :stopped))))

(defun physical-recovery-select-history (target history-text eor-bytes)
  "Select receive history under follow-command, reload and quiescence exclusion.
Follow must already be stopped. The caller must independently verify remote
system identity, exact prefix and retention before invoking this private control.
A failure after selection work begins fences recovery; this does not admit a network source."
  (when (plusp *live-operation-depth*) (error "history selection requires its own operation admission"))
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (check-physical-recovery-worker-start)
    (when *physical-follow-runtime* (error "stop native follow before selecting receive history"))
    (sb-thread:with-mutex (*reload-run-lock*)
      (with-live-operation-quiescence
        (check-physical-recovery-worker-start)
        (unless (and *physical-receive-store* (eql 2 (getf *physical-recovery-state* :phase)))
          (error "history selection requires streaming recovery"))
        (let ((history (timeline-check-lineage (timeline-parse-history target history-text)))
              (finished nil))
          (unless (physical-store-history-extends-p
                   (timeline-read-history (getf *physical-recovery-state* :timeline)) history)
            (error "receive history changes immutable base ancestry"))
          (unwind-protect
               (progn
                 (physical-store-select-history *physical-receive-store* target history-text eor-bytes)
                 (physical-recovery-publish-selected-history *physical-receive-store*)
                 (let ((candidate (physical-recovery-attach-selected-store
                                   *physical-recovery-state* *physical-receive-store*)))
                   (setf *physical-recovery-state*
                         (physical-recovery-write-control candidate 2 *last-replay-lsn* *next-xid*)))
                 (setf finished t)
                 target)
            (unless finished (setf *physical-recovery-failed-p* t))))))))

(defun physical-follow-autostart ()
  "Launch admitted startup workers, using replay only for managed or unattached state."
  (sb-thread:with-mutex (*physical-follow-command-lock*)
    (check-physical-recovery-worker-start)
    (when (probe-file (physical-follow-config-path))
      (let ((config (physical-follow-read-config)))
        (when (or (getf *physical-recovery-state* :standard-follow-managed-p)
                  (physical-follow-unattached-p config)
                  (physical-follow-controller-enabled-p config))
          (unless (and (recovery-in-progress-p) *physical-receive-store*)
            (error "native follow configuration requires streaming recovery"))
          (unless *physical-follow-runtime*
            (if (or (getf *physical-recovery-state* :standard-follow-managed-p)
                    (physical-follow-unattached-p config))
                (physical-follow-launch-replay-only config)
                (physical-follow-launch config))))))))
