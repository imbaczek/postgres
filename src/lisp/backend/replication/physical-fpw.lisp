;; Source FPW facts describe policy evidence and an exact covered frontier.
;; Shape checks, selected-history geometry and authenticated prefix identity are
;; separate requirements; these helpers do not implement physical replay.

;; Callers verify source identity and the WAL prefix before using a fact for a
;; policy decision.

(in-package #:pg)

(defconstant +physical-fpw-fact-size+ 72
  "Size in bytes of the optional PGFPWFC1 control suffix.")

(defconstant +physical-promotion-fpw-intent-size+ 17)

(defun physical-fpw-encode-promotion-intent (enabled)
  "Encode the desired promotion policy independently of the source FPW fact."
  (unless (member enabled '(nil t)) (error "invalid promotion full page writes policy"))
  (let ((bytes (make-array +physical-promotion-fpw-intent-size+
                           :element-type '(unsigned-byte 8) :initial-element 0)))
    (replace bytes #(80 71 70 80 87 80 73 49))
    (setf (aref bytes 8) (if enabled 1 0))
    bytes))

(defun physical-fpw-decode-promotion-intent (bytes)
  "Decode the 17-byte PGFPWPI1 intent, checking its value and reserved bytes."
  (unless (and (= (length bytes) +physical-promotion-fpw-intent-size+)
               (equalp (subseq bytes 0 8) #(80 71 70 80 87 80 73 49))
               (member (aref bytes 8) '(0 1))
               (every #'zerop (subseq bytes 9)))
    (error "invalid promotion full page writes intent"))
  (= 1 (aref bytes 8)))

(defun physical-fpw-control-core-size (bytes &optional (end (length bytes)))
  "Return the old layout size and whether a 17-byte intent follows it.
END excludes later suffixes but includes the final CRC's four bytes.
The caller must still check the layout, intent and CRC."
  (case (mod end 4)
    (0 (values end nil))
    (1 (values (- end +physical-promotion-fpw-intent-size+) t))
    (otherwise (error "invalid physical recovery control length"))))

(defun physical-fpw-state-fields (fact incarnation)
  "Construct fold evidence with the supplied private owner token."
  (%make-wal-fpw-state
   incarnation (getf fact :enabled) (getf fact :witness-kind)
   (getf fact :witness-start) (getf fact :witness-end) (getf fact :witness-timeline)
   (getf fact :coverage-start) (getf fact :coverage-timeline)
   (getf fact :last-disable-start) (getf fact :last-disable-timeline)))

(defun physical-fpw-fact-valid-p (fact)
  "Check fact shape and ordering, not selected-history geometry or prefix identity."
  (and (eql 22 (ignore-errors (list-length fact)))
       (equal (loop for (key value) on fact by #'cddr collect key)
              '(:enabled :witness-kind :witness-start :witness-end :witness-timeline
                :coverage-start :coverage-timeline :through :through-timeline
                :last-disable-start :last-disable-timeline))
       (wal-fpw-state-valid-p (physical-fpw-state-fields fact :fpw-shape-check))
       (typep (getf fact :through) '(unsigned-byte 64))
       (typep (getf fact :through-timeline) '(integer 1 4294967295))
       (<= (getf fact :witness-end) (getf fact :through))
       (<= (getf fact :witness-timeline) (getf fact :through-timeline))))

(defun physical-fpw-copy-fact (fact)
  "Validate and copy FACT, preserving NIL as unknown."
  (when fact
    (unless (physical-fpw-fact-valid-p fact) (error "invalid source FPW fact"))
    (copy-list fact)))

(defun physical-fpw-state-from-fact (fact incarnation)
  "Recreate fold evidence using INCARNATION as a private owner token.
The token is not stored in the fact."
  (unless (and incarnation (symbolp incarnation)) (error "invalid FPW fold owner"))
  (when fact
    (unless (physical-fpw-fact-valid-p fact) (error "invalid source FPW fact"))
    (physical-fpw-state-fields fact incarnation)))

(defun physical-fpw-fact-from-state (state through through-timeline)
  "Export evidence at the exact THROUGH and THROUGH-TIMELINE boundary.
NIL state is unknown; a present fact with ENABLED NIL is known OFF. The caller
must prove coverage through the supplied pair; export checks shape only."
  (unless (and (typep through '(unsigned-byte 64))
               (typep through-timeline '(integer 1 4294967295))
               (or (null state) (wal-fpw-state-valid-p state)))
    (error "invalid FPW state export"))
  (when state
    (let ((fact (list :enabled (wal-fpw-state-enabled state)
                      :witness-kind (wal-fpw-state-witness-kind state)
                      :witness-start (wal-fpw-state-start state)
                      :witness-end (wal-fpw-state-end state)
                      :witness-timeline (wal-fpw-state-timeline state)
                      :coverage-start (wal-fpw-state-coverage-start state)
                      :coverage-timeline (wal-fpw-state-coverage-timeline state)
                      :through through :through-timeline through-timeline
                      :last-disable-start (wal-fpw-state-last-disable-start state)
                      :last-disable-timeline (wal-fpw-state-last-disable-timeline state))))
      (unless (physical-fpw-fact-valid-p fact) (error "invalid FPW state export frontier"))
      fact)))

(defun physical-policy-check-record-owner (start end timeline history)
  "Check that a record belongs to one selected timeline and WAL segment."
  (unless (and (typep start '(unsigned-byte 64)) (typep end '(unsigned-byte 64))
               (< start end) (= timeline (timeline-at-point start history))
               (= timeline (timeline-at-point (1- end) history))
               (= (floor start +wal-segment-size+) (floor (1- end) +wal-segment-size+)))
    (error "policy evidence crosses its selected timeline or segment"))
  (values))

(defun physical-policy-check-terminal (record timeline history prior-through)
  "Preflight a complete terminal's geometry, ancestry and ordering.
The caller must supply every terminal in verified order; an allowed LSN gap
does not prove the absence of an omitted policy transition."
  (unless (and (wal-record-p record)
               (or (wal-terminal-record-type-p (wal-record-type record))
                   (= (wal-record-type record) +wal-record-end-of-recovery+)))
    (error "policy coverage requires a complete terminal"))
  (physical-policy-check-record-owner (wal-record-start record) (wal-record-end record) timeline history)
  (when (and prior-through (> prior-through (wal-record-start record)))
    (error "policy coverage moved backwards"))
  (when (= (wal-record-type record) +wal-record-end-of-recovery+)
    (let* ((info (wal-end-of-recovery-record-info record))
           (parent (second (member timeline history :key #'timeline-history-entry-timeline))))
      (unless (and (= timeline (getf info :timeline)) parent
                   (= (getf info :prev-timeline) (timeline-history-entry-timeline parent)))
        (error "policy coverage uses a different recovery ancestry"))))
  (values))

(defun physical-fpw-check-fact (fact history through through-timeline)
  "Check FACT at its exact boundary against selected-history geometry.
Source identity and the verified WAL prefix must be established separately."
  (when fact
    (unless (and (physical-fpw-fact-valid-p fact)
                 (eql through (getf fact :through))
                 (eql through-timeline (getf fact :through-timeline)))
      (error "source FPW fact differs from its replay boundary"))
    (physical-policy-check-record-owner
     (getf fact :witness-start) (getf fact :witness-end) (getf fact :witness-timeline) history)
    (unless (= (getf fact :coverage-timeline) (timeline-at-point (getf fact :coverage-start) history))
      (error "FPW coverage belongs to a different timeline"))
    (when (getf fact :last-disable-start)
      (physical-policy-check-record-owner
       (getf fact :last-disable-start)
       (+ (getf fact :last-disable-start) +wal-header-size+ +wal-full-page-writes-change-size+)
       (getf fact :last-disable-timeline) history))
    (unless (timeline-prefix-boundary-p through-timeline through history)
      (error "FPW coverage ends outside selected history"))
    (copy-list fact)))

(defun physical-fpw-fact-next (prior record timeline history)
  "Derive a source fact from PRIOR and a complete terminal.
The caller verifies complete batch framing, source identity and the WAL prefix.
The fold uses a fresh private owner token which is not stored in the fact."
  (when prior
    (physical-fpw-check-fact prior history (getf prior :through) (getf prior :through-timeline)))
  (physical-policy-check-terminal record timeline history (getf prior :through))
  (let* ((incarnation (make-symbol "SOURCE-FPW-FOLD"))
         (state (physical-fpw-state-from-fact prior incarnation))
         (next (wal-fpw-state-next state record timeline incarnation))
         (fact (physical-fpw-fact-from-state next (wal-record-end record) timeline)))
    (physical-fpw-check-fact fact history (wal-record-end record) timeline)))

(defun physical-fpw-encode-fact (fact)
  "Encode a known fact as the 72-byte PGFPWFC1 suffix.
The suffix has no CRC of its own; the enclosing control image supplies one."
  (unless (physical-fpw-fact-valid-p fact) (error "invalid source FPW fact"))
  (let ((bytes (make-array +physical-fpw-fact-size+ :element-type '(unsigned-byte 8) :initial-element 0)))
    (replace bytes #(80 71 70 80 87 70 67 49))
    (wal-set-u32 bytes 8 (logior (if (getf fact :enabled) 1 0)
                                 (if (eq :checkpoint (getf fact :witness-kind)) 2 0)
                                 (if (getf fact :last-disable-start) 4 0)))
    (loop for key in '(:witness-start :witness-end :coverage-start :through)
          for offset in '(16 24 40 48)
          do (wal-set-u64 bytes offset (getf fact key)))
    (loop for key in '(:witness-timeline :coverage-timeline :through-timeline)
          for offset in '(32 36 56)
          do (wal-set-u32 bytes offset (getf fact key)))
    (when (getf fact :last-disable-start)
      (wal-set-u32 bytes 60 (getf fact :last-disable-timeline))
      (wal-set-u64 bytes 64 (getf fact :last-disable-start)))
    bytes))

(defun physical-fpw-decode-fact (bytes)
  "Decode and shape-check a 72-byte PGFPWFC1 suffix."
  (unless (and (typep bytes '(vector (unsigned-byte 8)))
               (= (length bytes) +physical-fpw-fact-size+)
               (equalp (subseq bytes 0 8) #(80 71 70 80 87 70 67 49)))
    (error "invalid source FPW encoding"))
  (let* ((flags (wal-u32 bytes 8))
         (disabled (logbitp 2 flags)))
    (unless (and (<= flags 7) (zerop (wal-u32 bytes 12))
                 (or disabled (and (zerop (wal-u32 bytes 60)) (zerop (wal-u64 bytes 64)))))
      (error "invalid source FPW encoding flags"))
    (let ((fact (list :enabled (logbitp 0 flags)
                      :witness-kind (if (logbitp 1 flags) :checkpoint :change)
                      :witness-start (wal-u64 bytes 16) :witness-end (wal-u64 bytes 24)
                      :witness-timeline (wal-u32 bytes 32)
                      :coverage-start (wal-u64 bytes 40) :coverage-timeline (wal-u32 bytes 36)
                      :through (wal-u64 bytes 48) :through-timeline (wal-u32 bytes 56)
                      :last-disable-start (when disabled (wal-u64 bytes 64))
                      :last-disable-timeline (when disabled (wal-u32 bytes 60)))))
      (unless (physical-fpw-fact-valid-p fact) (error "invalid decoded source FPW fact"))
      fact)))

(defun physical-fpw-fact-at-p (fact through timeline)
  "Check fact shape and its exact THROUGH and timeline pair."
  (and (physical-fpw-fact-valid-p fact)
       (eql through (getf fact :through))
       (eql timeline (getf fact :through-timeline))))

(defun physical-fpw-covers-required-p (fact minimum)
  "Check that FACT covers at least MINIMUM's starting floor.
Both facts must already belong to the same selected history."
  (or (< (getf fact :coverage-start) (getf minimum :coverage-start))
      (and (= (getf fact :coverage-start) (getf minimum :coverage-start))
           (= (getf fact :coverage-timeline) (getf minimum :coverage-timeline)))))

(defun physical-fpw-witness-equal-p (left right)
  "Compare policy and checkpoint or change witness, excluding the frontier."
  (every (lambda (key) (eql (getf left key) (getf right key)))
         '(:enabled :witness-kind :witness-start :witness-end :witness-timeline)))

(defun physical-fpw-restrict-fact (fact minimum)
  "Copy with MINIMUM coverage floor; forget a saved disable only if it precedes that floor."
  (let ((restricted (copy-list fact)) (floor (getf minimum :coverage-start)))
    (setf (getf restricted :coverage-start) floor
          (getf restricted :coverage-timeline) (getf minimum :coverage-timeline))
    (when (and (getf restricted :last-disable-start)
               (< (getf restricted :last-disable-start) floor))
      (setf (getf restricted :last-disable-start) nil
            (getf restricted :last-disable-timeline) nil))
    restricted))

(defun physical-fpw-check-extension (fact minimum required)
  "Reject invented witnesses or disables inside the required prefix."
  (unless (or (physical-fpw-witness-equal-p fact minimum)
              (>= (getf fact :witness-start) required))
    (error "FPW witness was inserted inside a required prefix"))
  (let ((disabled (getf fact :last-disable-start))
        (saved (getf minimum :last-disable-start)))
    (unless (if saved
                (and disabled
                     (or (and (= saved disabled)
                              (= (getf fact :last-disable-timeline)
                                 (getf minimum :last-disable-timeline)))
                         (>= disabled required)))
                (or (null disabled) (< disabled (getf minimum :coverage-start))
                    (>= disabled required)))
      (error "FPW disable history differs inside a required prefix")))
  (values))

(defun physical-fpw-required-fact (source minimum applied timeline required required-timeline)
  "Preserve MINIMUM below REQUIRED and reconcile SOURCE at or beyond it.
At the boundary compare exactly after restricting stronger earlier coverage.
Beyond it, reject invented witnesses or disables inside the promised prefix.
Source identity, history and WAL prefix verification are separate from these
consistency comparisons."
  (unless (and (typep applied '(unsigned-byte 64))
               (typep timeline '(integer 1 4294967295))
               (or (null source) (physical-fpw-fact-at-p source applied timeline))
               (or (null required)
                   (and (typep required '(unsigned-byte 64))
                        (typep required-timeline '(integer 1 4294967295))))
               (or (null minimum)
                   (and required (physical-fpw-fact-at-p minimum required required-timeline))))
    (error "source FPW fact is outside its recovery frontier"))
  (when (and required (= applied required) (/= timeline required-timeline))
    (error "FPW replay timeline differs at its required boundary"))
  (when (and required (< applied required))
    (return-from physical-fpw-required-fact (physical-fpw-copy-fact minimum)))
  (when minimum
    (unless (and source (physical-fpw-covers-required-p source minimum))
      (error "required FPW coverage was lost"))
    (if (= applied required)
        (unless (equal minimum (physical-fpw-restrict-fact source minimum))
          (error "reconstructed FPW evidence differs at its required boundary"))
        (physical-fpw-check-extension source minimum required)))
  (physical-fpw-copy-fact source))
