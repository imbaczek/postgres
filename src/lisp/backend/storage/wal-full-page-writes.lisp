;; Primary effective full page writes policy is derived from checkpoint and WAL.
;; Configuration and physical replay are not yet wired to this module.

(in-package #:pg)

(defconstant +wal-full-page-writes-change-size+ 16)

(defstruct (wal-fpw-state
            (:constructor %make-wal-fpw-state
                (incarnation enabled witness-kind start end timeline coverage-start coverage-timeline last-disable-start last-disable-timeline)))
  "Immutable policy evidence owned by an incarnation token.
Primary state uses the local WAL storage incarnation; a private fold uses a fresh
owner token, never serialized. START, END and TIMELINE identify the latest
checkpoint or change witness. COVERAGE-START and COVERAGE-TIMELINE give the
coverage floor. The optional last disabling change records its START and timeline,
not its end. A timeline number alone does not prove ancestry."
  (incarnation nil :read-only t)
  (enabled nil :read-only t)
  (witness-kind nil :read-only t)
  (start 0 :read-only t)
  (end 0 :read-only t)
  (timeline 1 :read-only t)
  (coverage-start 0 :read-only t)
  (coverage-timeline 1 :read-only t)
  (last-disable-start nil :read-only t)
  (last-disable-timeline nil :read-only t))

(defun wal-encode-full-page-writes-change (enabled)
  "Encode the 16-byte type-17 WAL payload for the boolean ENABLED."
  (unless (member enabled '(nil t)) (error "invalid full page writes policy"))
  (let ((bytes (make-array +wal-full-page-writes-change-size+
                           :element-type '(unsigned-byte 8) :initial-element 0)))
    (replace bytes #(80 71 70 80 87 48 48 49))
    (setf (aref bytes 8) (if enabled 1 0))
    bytes))

(defun wal-decode-full-page-writes-change (bytes)
  "Validate a type-17 WAL payload and return its boolean policy."
  (unless (and (= (length bytes) +wal-full-page-writes-change-size+)
               (equalp (subseq bytes 0 8) #(80 71 70 80 87 48 48 49))
               (member (aref bytes 8) '(0 1))
               (loop for index from 9 below 16 always (zerop (aref bytes index))))
    (error "invalid native full page writes change"))
  (= 1 (aref bytes 8)))

(defun wal-full-page-writes-change-record-info (record)
  "Validate a type-17 WAL terminal and return its boolean policy."
  (unless (and (= (wal-record-type record) +wal-record-full-page-writes-change+)
               (>= (wal-record-version record) 8) (zerop (wal-record-xid record))
               (typep (wal-record-start record) '(unsigned-byte 64))
               (typep (wal-record-end record) '(unsigned-byte 64))
               (= (wal-record-end record)
                  (+ (wal-record-start record) +wal-header-size+
                     +wal-full-page-writes-change-size+))
               (= (floor (wal-record-start record) +wal-segment-size+)
                  (floor (1- (wal-record-end record)) +wal-segment-size+)))
    (error "invalid native full page writes terminal"))
  (wal-decode-full-page-writes-change (wal-record-payload record)))

(defun wal-fpw-state-valid-p (state)
  "Check the shape and ordering of evidence; this does not prove ancestry."
  (and (wal-fpw-state-p state)
       (symbolp (wal-fpw-state-incarnation state)) (wal-fpw-state-incarnation state)
       (member (wal-fpw-state-enabled state) '(nil t))
       (member (wal-fpw-state-witness-kind state) '(:checkpoint :change))
       (every (lambda (value) (typep value '(unsigned-byte 64)))
              (list (wal-fpw-state-start state) (wal-fpw-state-end state)
                    (wal-fpw-state-coverage-start state)))
       (<= (wal-fpw-state-coverage-start state) (wal-fpw-state-start state))
       (< (wal-fpw-state-start state) (wal-fpw-state-end state))
       (typep (wal-fpw-state-timeline state) '(integer 1 4294967295))
       (typep (wal-fpw-state-coverage-timeline state) '(integer 1 4294967295))
       (<= (wal-fpw-state-coverage-timeline state) (wal-fpw-state-timeline state))
       (= (floor (wal-fpw-state-start state) +wal-segment-size+)
          (floor (1- (wal-fpw-state-end state)) +wal-segment-size+))
       (if (eq :change (wal-fpw-state-witness-kind state))
           (= (wal-fpw-state-end state)
              (+ (wal-fpw-state-start state) +wal-header-size+ +wal-full-page-writes-change-size+))
           (wal-checkpoint-snapshot-length-p
            (- (wal-fpw-state-end state) (wal-fpw-state-start state) +wal-header-size+)))
       (let ((disabled (wal-fpw-state-last-disable-start state))
             (disabled-timeline (wal-fpw-state-last-disable-timeline state)))
         (and (or (and (null disabled) (null disabled-timeline))
                  (and (typep disabled '(unsigned-byte 64))
                       (<= (wal-fpw-state-coverage-start state) disabled)
                       (<= disabled (wal-fpw-state-start state))
                       (typep disabled-timeline '(integer 1 4294967295))
                       (<= (wal-fpw-state-coverage-timeline state) disabled-timeline
                           (wal-fpw-state-timeline state))))
              (or (not (eq :change (wal-fpw-state-witness-kind state)))
                  (wal-fpw-state-enabled state)
                  (and (eql disabled (wal-fpw-state-start state))
                       (eql disabled-timeline (wal-fpw-state-timeline state))))
              (or (null disabled)
                  (and (eq :change (wal-fpw-state-witness-kind state))
                       (null (wal-fpw-state-enabled state)))
                  (< disabled (wal-fpw-state-start state)))
              (or (null disabled)
                  (and (<= (+ disabled +wal-header-size+ +wal-full-page-writes-change-size+)
                           (if (and (eq :change (wal-fpw-state-witness-kind state))
                                    (null (wal-fpw-state-enabled state)))
                               (wal-fpw-state-end state) (wal-fpw-state-start state)))
                       (= (floor disabled +wal-segment-size+)
                          (floor (+ disabled +wal-header-size+ +wal-full-page-writes-change-size+ -1)
                                 +wal-segment-size+))))))))

(defun wal-fpw-state-from-checkpoint (snapshot incarnation)
  "Seed evidence from a checkpoint with a policy field, or return NIL if absent.
A present NIL field is known OFF. The checkpoint starts the coverage floor."
  (unless (and incarnation (symbolp incarnation)
               (or (null snapshot) (wal-checkpoint-snapshot-valid-p snapshot)))
    (error "invalid full page writes checkpoint seed"))
  (when (get-properties snapshot '(:full-page-writes))
    (%make-wal-fpw-state incarnation (getf snapshot :full-page-writes) :checkpoint
                         (getf snapshot :start) (getf snapshot :end)
                         (getf snapshot :timeline) (getf snapshot :start) (getf snapshot :timeline) nil nil)))

(defun wal-fpw-state-next (state record timeline incarnation)
  "Derive evidence without changing shared state.
A checkpoint supplies policy and must agree with any preceding known policy;
a change establishes a new one. Records without a known policy field preserve
STATE. Replay callers verify complete batches and selected history. Timeline
ordering alone does not prove ancestry."
  (unless (and incarnation (symbolp incarnation)
               (typep timeline '(integer 1 4294967295))
               (wal-record-p record)
               (typep (wal-record-start record) '(unsigned-byte 64))
               (typep (wal-record-end record) '(unsigned-byte 64))
               (> (wal-record-end record) (wal-record-start record))
               (or (null state)
                   (and (wal-fpw-state-valid-p state)
                        (eq incarnation (wal-fpw-state-incarnation state))
                        (<= (wal-fpw-state-timeline state) timeline))))
    (error "invalid full page writes replay state"))
  (let* ((type (wal-record-type record))
         (snapshot (when (= type +wal-record-checkpoint+)
                     (wal-checkpoint-record-snapshot record)))
         (known (or (= type +wal-record-full-page-writes-change+)
                    (get-properties snapshot '(:full-page-writes))))
         (enabled (if snapshot (getf snapshot :full-page-writes)
                      (when (= type +wal-record-full-page-writes-change+)
                        (wal-full-page-writes-change-record-info record))))
         (kind (if (= type +wal-record-checkpoint+) :checkpoint :change))
         (start (wal-record-start record)) (end (wal-record-end record)))
    (when (and snapshot (/= timeline (getf snapshot :timeline)))
      (error "checkpoint policy belongs to a different replay timeline"))
    (when (and state known (= start (wal-fpw-state-start state))
               (= end (wal-fpw-state-end state)))
      (unless (and (eq kind (wal-fpw-state-witness-kind state))
                   (eql enabled (wal-fpw-state-enabled state))
                   (= timeline (wal-fpw-state-timeline state)))
        (error "full page writes witness differs at the same position"))
      (return-from wal-fpw-state-next state))
    (when (and state (> (wal-fpw-state-end state) start))
      (error "full page writes seed belongs to a later replay position"))
    (unless known (return-from wal-fpw-state-next state))
    (when (and snapshot state (not (eql enabled (wal-fpw-state-enabled state))))
      (error "checkpoint full page writes policy differs from preceding WAL"))
    (%make-wal-fpw-state
     incarnation enabled kind start end timeline
     (if state (wal-fpw-state-coverage-start state) start)
     (if state (wal-fpw-state-coverage-timeline state) timeline)
     (if (and (eq kind :change) (null enabled)) start
         (when state (wal-fpw-state-last-disable-start state)))
     (if (and (eq kind :change) (null enabled)) timeline
         (when state (wal-fpw-state-last-disable-timeline state))))))

(defvar *wal-fpw-test-hook* nil)

(defun wal-fpw-test-point (point state)
  (when *wal-fpw-test-hook* (funcall *wal-fpw-test-hook* point state)))

(defun wal-checked-fpw-state-under-lock ()
  "Return durable evidence or NIL when unknown, requiring the WAL lock.
Do not call while an enabling candidate is installed before its flush."
  (unless (sb-thread:holding-mutex-p *wal-lock*) (error "full page writes requires WAL lock"))
  (wal-check-healthy)
  (unless *wal-ready* (wal-invalid-input "full page writes requires initialized WAL"))
  (when (eq :unrecovered *wal-fpw-state*)
    (wal-invalid-input "full page writes recovery has not completed"))
  (when *wal-fpw-state*
    (unless (and (wal-fpw-state-valid-p *wal-fpw-state*)
                 (eq *wal-storage-incarnation* (wal-fpw-state-incarnation *wal-fpw-state*))
                 (= *wal-insert-timeline* (wal-fpw-state-timeline *wal-fpw-state*))
                 (<= (wal-fpw-state-end *wal-fpw-state*) *wal-durable-lsn* *wal-end-lsn*))
      (error "full page writes state differs from WAL storage")))
  *wal-fpw-state*)

(defun wal-current-full-page-writes-fields ()
  "Return checkpoint fields under the WAL lock, preserving known OFF versus absent."
  (let ((state (wal-checked-fpw-state-under-lock)))
    (when state (list :full-page-writes (wal-fpw-state-enabled state)))))

(defun wal-fpw-prepare-recovery (snapshot marker-p)
  "Mark known policy evidence unavailable until recovery completes, under WAL lock."
  (unless (sb-thread:holding-mutex-p *wal-lock*) (error "FPW recovery requires WAL lock"))
  (setf *wal-fpw-state*
        (when (or marker-p (get-properties snapshot '(:full-page-writes))) :unrecovered)))

(defun wal-fpw-install-recovered-state (state incarnation)
  "Install recovered evidence under WAL lock after the full scan and sync succeed.
The caller must exclude storage replacement throughout recovery."
  (unless (sb-thread:holding-mutex-p *wal-lock*) (error "FPW recovery requires WAL lock"))
  (unless (eq incarnation *wal-storage-incarnation*)
    (wal-invalid-input "full page writes recovery belongs to replaced storage"))
  (unless (or (null state)
              (and (wal-fpw-state-valid-p state)
                   (eq incarnation (wal-fpw-state-incarnation state))
                   (= *wal-insert-timeline* (wal-fpw-state-timeline state))
                   (<= (wal-fpw-state-end state) *wal-durable-lsn* *wal-end-lsn*)))
    (error "invalid recovered full page writes state"))
  (setf *wal-fpw-state* state))

(defun wal-fpw-note-published-checkpoint (snapshot)
  "Advance evidence after checkpoint control publication, under the WAL lock."
  (let ((state (wal-checked-fpw-state-under-lock)))
    (when (or state (get-properties snapshot '(:full-page-writes)))
      (unless (get-properties snapshot '(:full-page-writes))
        (error "checkpoint omitted established full page writes policy"))
      (setf *wal-fpw-state*
            (wal-fpw-state-next
             state (make-wal-record :version +wal-version+ :type +wal-record-checkpoint+ :xid 0
                                    :start (getf snapshot :start) :end (getf snapshot :end)
                                    :payload (wal-encode-checkpoint-snapshot snapshot))
             *wal-insert-timeline* *wal-storage-incarnation*)))))

(defun wal-report-full-page-writes (enabled &optional (incarnation *wal-storage-incarnation*))
  "Publish a primary policy change and return its durable witness end.
An unchanged known policy returns its current witness end without I/O. A change
settles preceding page and CLOG work before append. Errors and nonlocal exits
after publication starts fail-stop. ON is installed before the change terminal;
OFF only after the terminal is durable. The transient ON candidate exists while
the WAL lock is held. This helper does not install a configuration producer or
alter native page images."
  (unless (member enabled '(nil t)) (error "invalid full page writes policy"))
  (prevent-recovery-primary-operation "full page writes policy publication")
  (with-wal-publication-fence (begin-publication)
    (unless (eq incarnation *wal-storage-incarnation*)
      (wal-invalid-input "full page writes request belongs to replaced storage"))
    (let ((prior (wal-checked-fpw-state-under-lock)))
      (if (and prior (eql enabled (wal-fpw-state-enabled prior)))
          (wal-fpw-state-end prior)
          (progn
            (wal-settle-page-publications-under-lock)
            (wal-require-no-pending-storage)
            (let* ((payload (wal-encode-full-page-writes-change enabled))
                   (batch-start (wal-next-record-start +wal-header-size+))
                   (start (wal-record-start-after (+ batch-start +wal-header-size+)
                                                  (+ +wal-header-size+ (length payload))))
                   (end (+ start +wal-header-size+ (length payload)))
                   (record (make-wal-record :version +wal-version+ :type +wal-record-full-page-writes-change+
                                            :xid 0 :start start :end end :payload payload))
                   (candidate (wal-fpw-state-next prior record *wal-insert-timeline* incarnation)))
              (begin-publication)
              (unless (= batch-start (wal-append-batch-start))
                (error "FPW batch placement differs from its plan"))
              (when enabled (setf *wal-fpw-state* candidate))
              (wal-fpw-test-point :before-record candidate)
              (unless (= end (wal-append-record +wal-record-full-page-writes-change+ 0 payload))
                (error "FPW record placement differs from its plan"))
              (wal-fpw-test-point :after-record candidate)
              (wal-flush)
              (unless (<= end *wal-durable-lsn*) (error "FPW terminal is not durable"))
              (wal-fpw-test-point :after-flush candidate)
              (setf *wal-fpw-state* candidate)
              (wal-fpw-test-point :after-state candidate)
              end))))))

(defun wal-require-fpw-physical-replay-supported (record)
  "Validate a type-17 FPW change record before replay credits its state.
Batch framing and durable source-fact publication are checked by callers.
This validates source policy; it does not change local writer policy or incarnation."
  (when (= (wal-record-type record) +wal-record-full-page-writes-change+)
    (wal-full-page-writes-change-record-info record))
  (values))
