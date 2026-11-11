;;; Source logical status evidence and its recovery control encoding.
;;; NIL means unknown; a fact with ENABLED NIL is known OFF. The wrapper retains
;;; coverage and transition provenance outside the private WAL status state.
;;; This is codec and fold groundwork; it does not publish runtime authority.

(in-package #:pg)

(defconstant +physical-logical-fact-size+ 88)

(defconstant +physical-promotion-logical-intent-size+ 18)

(defun physical-logical-promotion-intent-valid-p (enabled level)
  (and (member enabled '(nil t)) (member level '(:replica :logical))
       (or (eq level :replica) enabled)))

(defun physical-logical-encode-promotion-intent
    (enabled &optional (level (if enabled :logical :replica)))
  "Encode logical decoding ON or OFF in the 18-byte PGLOGPI1 promotion intent."
  (unless (physical-logical-promotion-intent-valid-p enabled level)
    (error "invalid promotion logical decoding policy"))
  (let ((bytes (make-array +physical-promotion-logical-intent-size+
                           :element-type '(unsigned-byte 8) :initial-element 0)))
    (replace bytes #(80 71 76 79 71 80 73 49))
    (setf (aref bytes 8) (if enabled 1 0)
          (aref bytes 9) (ecase level (:replica 1) (:logical 2)))
    bytes))

(defun physical-logical-decode-promotion-intent (bytes)
  "Decode PGLOGPI1 after checking its length, magic, value and reserved bytes."
  (unless (and (typep bytes '(vector (unsigned-byte 8)))
               (= (length bytes) +physical-promotion-logical-intent-size+)
               (equalp (subseq bytes 0 8) #(80 71 76 79 71 80 73 49))
               (member (aref bytes 8) '(0 1)) (member (aref bytes 9) '(1 2))
               (every #'zerop (subseq bytes 10)))
    (error "invalid promotion logical decoding intent"))
  (let ((enabled (= 1 (aref bytes 8)))
        (level (case (aref bytes 9) (1 :replica) (2 :logical))))
    (unless (physical-logical-promotion-intent-valid-p enabled level)
      (error "inconsistent promotion logical decoding intent"))
    (values enabled level)))

(defun physical-promotion-intents-core-size (size)
  "Separate intent lengths from SIZE, keeping the final four CRC bytes.
SIZE excludes any source logical fact. Its length modulo four identifies no
intent, FPW, logical or both. Return the core size including CRC, followed by
FPW and logical presence flags. The caller checks the layout, intent bytes and
CRC."
  (let* ((residue (mod size 4))
         (fpw (logbitp 0 residue))
         (logical (logbitp 1 residue))
         (core (- size (if fpw +physical-promotion-fpw-intent-size+ 0)
                  (if logical +physical-promotion-logical-intent-size+ 0))))
    (unless (>= core 60) (error "truncated promotion intent control"))
    (values core fpw logical)))

(defconstant +physical-recovery-control-maximum-bytes+
  (+ 196 +physical-fpw-fact-size+ +physical-promotion-fpw-intent-size+ +physical-logical-fact-size+ +physical-promotion-logical-intent-size+))

(defun physical-logical-control-core-size (bytes)
  "Separate an optional 88-byte logical suffix, retaining the outer CRC size.
Bit 30 signals presence; the bounded image is at most 391 bytes."
  (unless (and (typep bytes '(vector (unsigned-byte 8)))
               (<= 60 (length bytes) +physical-recovery-control-maximum-bytes+))
    (error "invalid physical recovery control length"))
  (let* ((present (logbitp 30 (wal-u32 bytes 8)))
         (size (- (length bytes) (if present +physical-logical-fact-size+ 0))))
    (unless (>= size 60) (error "truncated logical recovery control fact"))
    (values size present)))

(defun physical-logical-state-fields (fact)
  (%make-wal-logical-status
   (getf fact :enabled) (getf fact :witness-start) (getf fact :witness-end)
   (getf fact :witness-timeline) (getf fact :last-enable-lsn)
   (getf fact :last-enable-timeline)))

(defun physical-logical-boundary-valid-p (lsn timeline end owner)
  (or (and (null lsn) (null timeline))
      (and (and (typep lsn '(unsigned-byte 64)) (>= lsn (1+ +wal-header-size+)))
           (typep timeline '(integer 1 4294967295))
           (<= lsn end) (<= timeline owner))))

(defun physical-logical-fact-valid-p (fact)
  "Check fact shape and ordering, not history geometry or prefix identity."
  (and (eql 26 (ignore-errors (list-length fact)))
       (equal (loop for (key value) on fact by #'cddr collect key)
              '(:enabled :witness-kind :witness-start :witness-end :witness-timeline
                :coverage-start :coverage-timeline :through :through-timeline
                :last-enable-lsn :last-enable-timeline :last-disable-lsn :last-disable-timeline))
       (member (getf fact :witness-kind) '(:checkpoint :change))
       (wal-logical-status-valid-p (physical-logical-state-fields fact))
       (typep (getf fact :coverage-start) '(unsigned-byte 64))
       (typep (getf fact :coverage-timeline) '(integer 1 4294967295))
       (typep (getf fact :through) '(unsigned-byte 64))
       (typep (getf fact :through-timeline) '(integer 1 4294967295))
       (<= (getf fact :coverage-start) (getf fact :witness-start))
       (<= (getf fact :witness-end) (getf fact :through))
       (<= (getf fact :coverage-timeline) (getf fact :witness-timeline)
           (getf fact :through-timeline))
       (every (lambda (keys)
                (physical-logical-boundary-valid-p
                 (getf fact (first keys)) (getf fact (second keys))
                 (getf fact :witness-end) (getf fact :witness-timeline)))
              '((:last-enable-lsn :last-enable-timeline) (:last-disable-lsn :last-disable-timeline)))
       (let ((enabled (getf fact :enabled))
             (on (getf fact :last-enable-lsn)) (off (getf fact :last-disable-lsn)))
         (and (if enabled (or (null off) (and on (> on off)))
                  (or (null on) (and off (> off on))))
              (or (eq :checkpoint (getf fact :witness-kind))
                  (and (= (- (getf fact :witness-end) (getf fact :witness-start))
                          (1+ +wal-header-size+))
                       (eql (getf fact :witness-end) (if enabled on off))
                       (eql (getf fact :witness-timeline)
                            (getf fact (if enabled :last-enable-timeline :last-disable-timeline)))))))))

(defun physical-logical-copy-fact (fact)
  "Return a validated independent copy, preserving NIL as unknown."
  (when fact
    (unless (physical-logical-fact-valid-p fact) (error "invalid source logical fact"))
    (copy-list fact)))

(defun physical-logical-state-from-fact (fact)
  "Reconstruct private fold state; coverage and OFF provenance stay in FACT."
  (when fact
    (unless (physical-logical-fact-valid-p fact) (error "invalid source logical fact"))
    (physical-logical-state-fields fact)))

(defun physical-logical-fact-from-state
    (state kind coverage-start coverage-timeline through through-timeline disable disable-timeline)
  "Wrap private STATE with coverage and transition provenance in a new fact."
  (when state
    (let ((fact (list :enabled (wal-logical-status-enabled state)
                      :witness-kind kind :witness-start (wal-logical-status-start state)
                      :witness-end (wal-logical-status-end state)
                      :witness-timeline (wal-logical-status-timeline state)
                      :coverage-start coverage-start :coverage-timeline coverage-timeline
                      :through through :through-timeline through-timeline
                      :last-enable-lsn (wal-logical-status-last-enable-lsn state)
                      :last-enable-timeline (wal-logical-status-last-enable-timeline state)
                      :last-disable-lsn disable :last-disable-timeline disable-timeline)))
      (unless (physical-logical-fact-valid-p fact) (error "invalid source logical export"))
      fact)))

(defun physical-logical-fact-from-checkpoint (snapshot)
  "Start coverage at a checkpoint witness, if it reports logical status.
An ON checkpoint does not establish the last enabling record or earlier continuity."
  (let ((state (wal-logical-status-from-checkpoint snapshot)))
    (when state
      (physical-logical-fact-from-state
       state :checkpoint (wal-logical-status-start state) (wal-logical-status-timeline state)
       (wal-logical-status-end state) (wal-logical-status-timeline state) nil nil))))

(defun physical-logical-check-fact (fact history through through-timeline)
  "Check the fact's boundary and witnesses against selected history; return a copy.
The caller must separately prove source identity and the verified WAL prefix."
  (when fact
    (unless (and (physical-logical-fact-valid-p fact)
                 (eql through (getf fact :through))
                 (eql through-timeline (getf fact :through-timeline)))
      (error "source logical fact differs from its replay boundary"))
    (physical-policy-check-record-owner
     (getf fact :witness-start) (getf fact :witness-end) (getf fact :witness-timeline) history)
    (unless (= (getf fact :coverage-timeline) (timeline-at-point (getf fact :coverage-start) history))
      (error "logical coverage belongs to a different timeline"))
    (dolist (keys '((:last-enable-lsn :last-enable-timeline)
                    (:last-disable-lsn :last-disable-timeline)))
      (when (getf fact (first keys))
        (physical-policy-check-record-owner
         (- (getf fact (first keys)) (1+ +wal-header-size+))
         (getf fact (first keys)) (getf fact (second keys)) history)))
    (unless (timeline-prefix-boundary-p through-timeline through history)
      (error "logical coverage ends outside selected history"))
    (copy-list fact)))

(defun physical-logical-fact-next (prior record timeline history)
  "Fold one complete terminal into a new source fact on selected history.
The caller must supply every complete terminal in verified order. LSN gaps are
allowed but do not prove that no status record was omitted. Coverage starts at
the first observation, not at a proven earlier ON. Each ON refreshes the last
enable end; retain the last OFF boundary across later records."
  (when prior
    (physical-logical-check-fact prior history (getf prior :through) (getf prior :through-timeline)))
  (physical-policy-check-terminal record timeline history (getf prior :through))
  (let* ((state (wal-logical-status-next (physical-logical-state-from-fact prior) record timeline))
         (changed (or (null prior)
                      (/= (wal-logical-status-end state) (getf prior :witness-end))))
         (kind (if changed
                   (if (= (wal-record-type record) +wal-record-checkpoint+) :checkpoint :change)
                   (getf prior :witness-kind)))
         (disabled (and (= (wal-record-type record) +wal-record-logical-decoding-status-change+)
                        (not (wal-logical-status-enabled state))))
         (fact (physical-logical-fact-from-state
                state kind
                (if prior (getf prior :coverage-start) (and state (wal-logical-status-start state)))
                (if prior (getf prior :coverage-timeline) timeline)
                (wal-record-end record) timeline
                (if disabled (wal-record-end record) (getf prior :last-disable-lsn))
                (if disabled timeline (getf prior :last-disable-timeline)))))
    (physical-logical-check-fact fact history (wal-record-end record) timeline)))

(defun physical-logical-encode-fact (fact)
  "Encode known source evidence as the 88-byte PGLOGFC1 suffix."
  (unless (physical-logical-fact-valid-p fact) (error "invalid source logical fact"))
  (let ((bytes (make-array +physical-logical-fact-size+ :element-type '(unsigned-byte 8)
                                                        :initial-element 0)))
    (replace bytes #(80 71 76 79 71 70 67 49))
    (wal-set-u32 bytes 8 (logior (if (getf fact :enabled) 1 0)
                                 (if (eq :checkpoint (getf fact :witness-kind)) 2 0)
                                 (if (getf fact :last-disable-lsn) 4 0)
                                 (if (getf fact :last-enable-lsn) 8 0)))
    (loop for key in '(:witness-start :witness-end :coverage-start :through)
          for offset in '(16 24 40 48) do (wal-set-u64 bytes offset (getf fact key)))
    (loop for key in '(:witness-timeline :coverage-timeline :through-timeline)
          for offset in '(32 36 56) do (wal-set-u32 bytes offset (getf fact key)))
    (dolist (item '((:last-disable-lsn :last-disable-timeline 64 60)
                    (:last-enable-lsn :last-enable-timeline 72 80)))
      (when (getf fact (first item))
        (wal-set-u64 bytes (third item) (getf fact (first item)))
        (wal-set-u32 bytes (fourth item) (getf fact (second item)))))
    bytes))

(defun physical-logical-decode-fact (bytes)
  "Decode and shape-check the 88-byte suffix, including reserved fields."
  (unless (and (typep bytes '(vector (unsigned-byte 8)))
               (= (length bytes) +physical-logical-fact-size+)
               (equalp (subseq bytes 0 8) #(80 71 76 79 71 70 67 49)))
    (error "invalid source logical encoding"))
  (let* ((flags (wal-u32 bytes 8)) (on (logbitp 3 flags)) (off (logbitp 2 flags)))
    (unless (and (<= flags 15) (zerop (wal-u32 bytes 12)) (zerop (wal-u32 bytes 84))
                 (or on (and (zerop (wal-u64 bytes 72)) (zerop (wal-u32 bytes 80))))
                 (or off (and (zerop (wal-u64 bytes 64)) (zerop (wal-u32 bytes 60)))))
      (error "invalid source logical encoding flags"))
    (let ((fact (list :enabled (logbitp 0 flags)
                      :witness-kind (if (logbitp 1 flags) :checkpoint :change)
                      :witness-start (wal-u64 bytes 16) :witness-end (wal-u64 bytes 24)
                      :witness-timeline (wal-u32 bytes 32)
                      :coverage-start (wal-u64 bytes 40) :coverage-timeline (wal-u32 bytes 36)
                      :through (wal-u64 bytes 48) :through-timeline (wal-u32 bytes 56)
                      :last-enable-lsn (when on (wal-u64 bytes 72))
                      :last-enable-timeline (when on (wal-u32 bytes 80))
                      :last-disable-lsn (when off (wal-u64 bytes 64))
                      :last-disable-timeline (when off (wal-u32 bytes 60)))))
      (unless (physical-logical-fact-valid-p fact) (error "invalid decoded source logical fact"))
      fact)))

(defun physical-logical-encode-base-anchor (fact)
  "Encode the 88-byte source fact as 176 uppercase hexadecimal characters."
  (let* ((bytes (physical-logical-encode-fact fact))
         (digits "0123456789ABCDEF")
         (text (make-string (* 2 (length bytes)) :element-type 'base-char)))
    (loop for octet across bytes for position from 0 by 2
          do (setf (char text position) (char digits (ldb (byte 4 4) octet))
                   (char text (1+ position)) (char digits (ldb (byte 4 0) octet))))
    text))

(defun physical-logical-decode-base-anchor (text)
  "Decode a canonical uppercase base anchor and validate its source fact."
  (unless (and (stringp text) (= (length text) (* 2 +physical-logical-fact-size+))
               (every (lambda (digit) (find digit "0123456789ABCDEF" :test #'char=)) text))
    (error "invalid immutable logical base anchor"))
  (let ((bytes (make-array +physical-logical-fact-size+ :element-type '(unsigned-byte 8))))
    (loop for index below (length bytes) for position from 0 by 2
          do (setf (aref bytes index)
                   (logior (ash (digit-char-p (char text position) 16) 4)
                           (digit-char-p (char text (1+ position)) 16))))
    (physical-logical-decode-fact bytes)))

(defun physical-logical-base-fact (manifest checkpoint history)
  "Derive the immutable base's logical fact on HISTORY.
A checkpoint with known logical status starts fresh coverage; any anchor must
agree with its witness. If the checkpoint omits status, an anchor preserves its
earlier coverage and witness, which must end by checkpoint start.
Without either witness, return NIL."
  (let* ((end (getf manifest :end)) (timeline (getf manifest :timeline))
                                    (checkpoint-fact (physical-logical-fact-from-checkpoint checkpoint))
                                    (anchor (when (get-properties manifest '(:logical-base-anchor))
                                              (physical-logical-decode-base-anchor (getf manifest :logical-base-anchor)))))
    (when anchor (physical-logical-check-fact anchor history end timeline))
    (cond
      (checkpoint-fact
       (physical-logical-check-fact checkpoint-fact history end timeline)
       (when (and anchor (not (physical-policy-witness-equal-p anchor checkpoint-fact)))
         (error "logical base anchor contradicts its known checkpoint"))
       checkpoint-fact)
      (anchor
       (unless (and checkpoint (wal-checkpoint-snapshot-valid-p checkpoint)
                    (= end (getf checkpoint :end)) (= timeline (getf checkpoint :timeline))
                    (<= (getf anchor :witness-end) (getf checkpoint :start)))
         (error "unknown base checkpoint cannot supply a logical status witness"))
       anchor)
      (t nil))))

(defun physical-logical-fact-at-p (fact through timeline)
  "Check fact shape and the exact frontier and timeline."
  (and (physical-logical-fact-valid-p fact)
       (eql through (getf fact :through)) (eql timeline (getf fact :through-timeline))))

(defun physical-logical-restrict-fact (fact minimum)
  "Copy FACT at MINIMUM's coverage floor, dropping older transition boundaries."
  (let ((copy (copy-list fact)) (floor (getf minimum :coverage-start)))
    (setf (getf copy :coverage-start) floor
          (getf copy :coverage-timeline) (getf minimum :coverage-timeline))
    (dolist (keys '((:last-enable-lsn :last-enable-timeline)
                    (:last-disable-lsn :last-disable-timeline)))
      (when (and (getf copy (first keys)) (<= (getf copy (first keys)) floor))
        (setf (getf copy (first keys)) nil (getf copy (second keys)) nil)))
    copy))

(defun physical-logical-check-extension (fact minimum required)
  "Reject new witnesses or transitions invented inside the required prefix."
  (unless (or (physical-policy-witness-equal-p fact minimum)
              (>= (getf fact :witness-start) required))
    (error "logical witness was inserted inside a required prefix"))
  (dolist (keys '((:last-enable-lsn :last-enable-timeline)
                  (:last-disable-lsn :last-disable-timeline)))
    (let ((current (getf fact (first keys))) (saved (getf minimum (first keys))))
      (unless (if saved
                  (and current
                       (or (and (= current saved)
                                (= (getf fact (second keys)) (getf minimum (second keys))))
                           (>= (- current (1+ +wal-header-size+)) required)))
                  (or (null current) (<= current (getf minimum :coverage-start))
                      (>= (- current (1+ +wal-header-size+)) required)))
        (error "logical transition history differs inside a required prefix"))))
  (values))

(defun physical-logical-required-fact (source minimum applied timeline required required-timeline)
  "Reconcile source evidence with a required frontier and return a copy.
These numeric checks require separate selected-history, source identity and
verified-prefix proofs from the caller."
  (unless (and (typep applied '(unsigned-byte 64)) (typep timeline '(integer 1 4294967295))
               (or (null source) (physical-logical-fact-at-p source applied timeline))
               (or (null required)
                   (and (typep required '(unsigned-byte 64))
                        (typep required-timeline '(integer 1 4294967295))))
               (or (null minimum)
                   (and required (physical-logical-fact-at-p minimum required required-timeline))))
    (error "source logical fact is outside its recovery frontier"))
  (when (and required (= applied required) (/= timeline required-timeline))
    (error "logical replay timeline differs at its required boundary"))
  (when (and required (< applied required))
    (return-from physical-logical-required-fact (physical-logical-copy-fact minimum)))
  (when minimum
    (unless (and source
                 (or (< (getf source :coverage-start) (getf minimum :coverage-start))
                     (and (= (getf source :coverage-start) (getf minimum :coverage-start))
                          (= (getf source :coverage-timeline) (getf minimum :coverage-timeline)))))
      (error "required logical coverage was lost"))
    (if (= applied required)
        (unless (equal minimum (physical-logical-restrict-fact source minimum))
          (error "reconstructed logical evidence differs at its required boundary"))
        (physical-logical-check-extension source minimum required)))
  (physical-logical-copy-fact source))
