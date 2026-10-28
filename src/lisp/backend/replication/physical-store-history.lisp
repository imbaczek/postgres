;;;; Local receive-history selection; source admission and replay remain separate.
(in-package #:pg)

(defconstant +physical-store-history-chain-limit+ 256)

(defconstant +physical-store-history-maximum-bytes+ (* 16 1024 1024))

(defun physical-store-selected-timeline (store)
  "Return the selected receive timeline, or the original base timeline."
  (or (getf (physical-store-selection store) :target) (physical-store-timeline store)))

(defun physical-store-history-path (store target digest)
  (physical-store-path store (format nil "~8,'0X-~16,'0X.history" target digest)))

(defun physical-store-history-image (payload)
  "Encode a bounded receive-history payload with a CRC envelope.
The CRC detects accidental corruption; it does not authenticate the payload."
  (let* ((text (string->bytes (physical-bootstrap-manifest-text payload)))
         (size (+ 16 (length text))))
    (unless (<= size +physical-store-history-maximum-bytes+)
      (error "receive history envelope exceeds its reopen bound"))
    (let ((bytes (make-array size :element-type '(unsigned-byte 8) :initial-element 0)))
      (replace bytes #(80 71 80 72 83 72 48 49))
      (wal-set-u32 bytes 8 (length text))
      (replace bytes text :start1 12)
      (wal-set-u32 bytes (- size 4) (wal-crc32 bytes))
      bytes)))

(defun physical-store-history-payload (bytes)
  "Check the envelope length and CRC before reading its bounded payload."
  (unless (and (<= 16 (length bytes) +physical-store-history-maximum-bytes+)
               (equalp (subseq bytes 0 8) #(80 71 80 72 83 72 48 49))
               (= (wal-u32 bytes 8) (- (length bytes) 16)))
    (error "invalid receive history envelope"))
  (let ((copy (copy-seq bytes)) (offset (- (length bytes) 4)))
    (wal-set-u32 copy offset 0)
    (unless (= (wal-u32 bytes offset) (wal-crc32 copy))
      (error "receive history checksum mismatch")))
  (recovery-parse-manifest-text (bytes->string (subseq bytes 12 (- (length bytes) 4)))))

(defun physical-store-history-extends-p (previous history)
  "Preserve PREVIOUS as the exact oldest ancestry of HISTORY.
Keep every fixed interval and allow only the old open tip to close. Genuinely
new descendants may have zero-width intervals, but cannot replace an older entry."
  (if (null previous)
      t
      (let ((suffix (member (timeline-history-entry-timeline (first previous)) history
                            :key #'timeline-history-entry-timeline)))
        (and (= (length previous) (length suffix))
             (every (lambda (old new)
                      (and (= (timeline-history-entry-timeline old)
                              (timeline-history-entry-timeline new))
                           (= (timeline-history-entry-begin old) (timeline-history-entry-begin new))
                           (or (null (timeline-history-entry-end old))
                               (eql (timeline-history-entry-end old) (timeline-history-entry-end new)))))
                    previous suffix)))))

(defun physical-store-check-history-selection (store payload digest previous)
  "Validate base identity, lineage and the expected end-of-recovery bytes.
Check the fork digest's representation and preserve previous history intervals.
Comparing that digest with stored WAL is a separate prefix-validation step.
Publication and loading enforce the same chain bound."
  (when (>= (loop for item = previous then (getf item :previous) while item count item)
            +physical-store-history-chain-limit+)
    (error "receive history chain exceeds its reopen bound"))
  (let ((keys '(:version :system-id :base-timeline :base :target :history-text
                :fork-digest :previous-digest :eor-octets)))
    (unless (and (listp payload) (= (length payload) (* 2 (length keys)))
                 (every (lambda (key) (= 1 (loop for name in payload by #'cddr count (eq name key)))) keys)
                 (eql 1 (getf payload :version))
                 (eql (physical-store-system-id store) (getf payload :system-id))
                 (eql (physical-store-timeline store) (getf payload :base-timeline))
                 (eql (physical-store-base-lsn store) (getf payload :base))
                 (typep (getf payload :target) '(integer 2 4294967295))
                 (stringp (getf payload :history-text))
                 (typep (getf payload :fork-digest) '(unsigned-byte 64))
                 (eql (getf payload :previous-digest) (getf previous :digest))
                 (listp (getf payload :eor-octets))
                 (= (length (getf payload :eor-octets)) +wal-end-of-recovery-record-size+)
                 (every (lambda (byte) (typep byte '(unsigned-byte 8))) (getf payload :eor-octets)))
      (error "foreign or malformed receive history selection")))
  (let* ((target (getf payload :target))
         (history (timeline-check-lineage (timeline-parse-history target (getf payload :history-text))))
         (parent-entry (second history))
         (parent (and parent-entry (timeline-history-entry-timeline parent-entry)))
         (fork (and parent-entry (timeline-history-entry-end parent-entry)))
         (raw (coerce (getf payload :eor-octets) '(vector (unsigned-byte 8)))))
    (unless (and parent (= parent (or (getf previous :target) (physical-store-timeline store)))
                 (<= (physical-store-base-lsn store) fork)
                 (timeline-prefix-boundary-p (physical-store-timeline store)
                                             (physical-store-base-lsn store) history)
                 (or (null previous)
                     (and (<= (+ (wal-u64 (getf previous :eor-bytes) 12)
                                 (length (getf previous :eor-bytes))) fork)
                          (physical-store-history-extends-p (getf previous :history) history))))
      (error "receive history does not extend selected ancestry"))
    (let* ((start (if (<= (+ (mod fork +wal-segment-size+) (length raw)) +wal-segment-size+)
                      fork (* (1+ (floor fork +wal-segment-size+)) +wal-segment-size+)))
           (record (wal-decode-record raw start)))
      (unless (= target (timeline-check-end-of-recovery record parent history))
        (error "receive history has a foreign end-of-recovery record")))
    (append payload (list :digest digest :history history :parent parent :fork fork
                          :eor-bytes raw :previous previous))))

(defun physical-store-load-history-selection (store target digest &optional (depth 0))
  "Load and validate the control-referenced history chain within its depth bound.
The FNV reference detects accidental corruption, not malicious changes."
  (unless (and (typep target '(integer 2 4294967295))
               (typep digest '(unsigned-byte 64)) (< depth +physical-store-history-chain-limit+))
    (error "invalid or excessive receive history chain"))
  (let* ((bytes (recovery-read-bytes (physical-store-history-path store target digest)))
         (payload (physical-store-history-payload bytes))
         (previous-digest (getf payload :previous-digest))
         (history (timeline-check-lineage (timeline-parse-history target (getf payload :history-text))))
         (parent (and (second history) (timeline-history-entry-timeline (second history)))))
    (unless (and (eql target (getf payload :target))
                 (= digest (physical-store-update-digest #xCBF29CE484222325 bytes)))
      (error "receive history differs from its durable reference"))
    (physical-store-check-history-selection
     store payload digest
     (when previous-digest
       (physical-store-load-history-selection store parent previous-digest (1+ depth))))))

(declaim (ftype function physical-store-control-tuple physical-store-read-retained-control
                physical-store-check-retained-coverage physical-store-retained-directory
                physical-store-retained-history physical-store-check-retained-root
                physical-retained-inventory-load physical-store-check-retained-identity))

(defun physical-store-history-candidate (store target history-text eor-bytes fork-digest)
  "Build and validate a history selection and its immutable encoded blob.
Bind it to STORE's base, prior selection and the locally supplied fork digest."
  (let* ((payload (list :version 1 :system-id (physical-store-system-id store)
                        :base-timeline (physical-store-timeline store) :base (physical-store-base-lsn store)
                        :target target :history-text history-text :fork-digest fork-digest
                        :previous-digest (getf (physical-store-selection store) :digest)
                        :eor-octets (coerce eor-bytes 'list)))
         (bytes (physical-store-history-image payload))
         (digest (physical-store-update-digest #xCBF29CE484222325 bytes)))
    (values (physical-store-check-history-selection
             store payload digest (physical-store-selection store)) bytes)))

(defun physical-store-publish-history (store selection bytes)
  "Durably publish the immutable selection blob, or verify an existing copy.
The blob alone does not select history; control publication is the commit point."
  (let ((path (physical-store-history-path store (getf selection :target) (getf selection :digest))))
    (physical-store-test-point :before-history-publication store)
    (if (probe-file path)
        (progn
          (unless (equalp bytes (recovery-read-bytes path)) (error "receive history path changed"))
          (fsync-directory (physical-store-directory store)))
        (timeline-publish-history-bytes path bytes))
    (physical-store-test-point :after-history-publication store)))

(defun physical-store-check-complete-input (store)
  "Require all input flushed and the decoder at a complete record boundary."
  (unless (and (= (physical-store-written-lsn store) (physical-store-flushed-lsn store))
               (zerop (physical-wal-decoder-used (physical-store-decoder store))))
    (error "receive history selection requires no unflushed input")))

(defun physical-store-select-history (store target history-text eor-bytes)
  "Select a descendant receive history at the exact flushed fork.

Require no unflushed input and a durable prior selected end-of-recovery record.
The control reference is the commit point; a history blob alone is not selected.
Before calling, stop and reconcile the old source and independently verify remote
cluster identity, exact prefix, base history and retention. This checks local
stored proof only. Errors latch the store; after an ambiguous control rename,
reopen from disk to determine the durable selection."
  (with-physical-store (store)
    (physical-store-validate-prefix store)
    (physical-store-check-complete-input store)
    (multiple-value-bind (selection bytes)
        (physical-store-history-candidate store target history-text eor-bytes
                                          (physical-store-flushed-digest store))
      (unless (= (getf selection :fork) (physical-store-flushed-lsn store))
        (error "receive history requires the complete parent prefix through its fork"))
      (physical-store-publish-history store selection bytes)
      (physical-store-save-control store (physical-store-flushed-lsn store)
                                   :digest (physical-store-flushed-digest store) :selection selection)
      (setf (physical-store-selection store) selection)
      (physical-store-test-point :after-history-selection store)
      target)))

(defun physical-store-prefix-timeline (store boundary)
  "Return the timeline owning the byte before a verified prefix boundary.
The original base boundary keeps its base timeline."
  (if (or (null (physical-store-selection store)) (= boundary (physical-store-base-lsn store)))
      (physical-store-timeline store)
      (timeline-at-point (1- boundary) (getf (physical-store-selection store) :history))))

(defun physical-store-history-check-record (store record raw previous)
  "Check record placement in the selected history and require the expected EOR bytes.
Return the timeline used for its receive segment."
  (let ((selection (physical-store-selection store)))
    (unless selection
      (wal-require-ordinary-record-type (wal-record-type record))
      (return-from physical-store-history-check-record (physical-store-timeline store)))
    (let* ((history (getf selection :history))
           (owner (timeline-at-point (wal-record-start record) history))
           (entry (find owner history :key #'timeline-history-entry-timeline))
           (prior (physical-store-prefix-timeline store previous)))
      (unless (or (null (timeline-history-entry-end entry))
                  (<= (wal-record-end record) (timeline-history-entry-end entry)))
        (error "received WAL record crosses a selected fork"))
      (if (= owner prior)
          (wal-require-ordinary-record-type (wal-record-type record))
          (let ((transition (loop for item = selection then (getf item :previous) while item
                                  when (= owner (getf item :target)) return item)))
            (unless (and transition (= previous (getf transition :fork))
                         (= prior (getf transition :parent))
                         (equalp raw (getf transition :eor-bytes)))
              (error "received WAL lacks its selected end-of-recovery transition"))))
      owner)))

(defun physical-store-history-segment-entries (store)
  "List receive files as (timeline segment-number pathname) entries."
  (loop for path in (directory (physical-store-path store "*.wal"))
        for name = (pathname-name path)
        collect
        (cond
          ((and (= (length name) 16) (every (lambda (char) (digit-char-p char 16)) name))
           (list (physical-store-timeline store) (parse-integer name :radix 16) path))
          ((and (= (length name) 25) (char= (char name 8) #\-)
                (every (lambda (char) (digit-char-p char 16)) (concatenate 'string (subseq name 0 8) (subseq name 9))))
           (let ((timeline (parse-integer name :radix 16 :end 8)))
             (unless (> timeline (physical-store-timeline store)) (error "invalid receive child filename"))
             (list timeline (parse-integer name :radix 16 :start 9) path)))
          (t (error "invalid physical receive segment name ~a" path)))))

(defun physical-store-history-read-chunk (store decoder cursor flush callback)
  "Decode buffered bytes from CURSOR's timeline within one segment and history interval.
Return the next scan cursor, leaving rotation-gap validation to the decoder."
  (let* ((history (getf (physical-store-selection store) :history))
         (timeline (timeline-at-point cursor history))
         (entry (find timeline history :key #'timeline-history-entry-timeline))
         (segment (floor cursor +wal-segment-size+))
         (segment-start (* segment +wal-segment-size+))
         (path (physical-store-segment-path store segment timeline))
         (offset (- cursor segment-start)))
    (if (probe-file path)
        (with-open-file (stream path :element-type '(unsigned-byte 8))
          (let ((limit (min (+ segment-start (file-length stream)) flush
                            (or (timeline-history-entry-end entry) flush))))
            (unless (<= 0 (file-length stream) +wal-segment-size+)
              (error "oversized receive history segment"))
            (when (< cursor limit)
              (file-position stream offset)
              (let ((buffer (make-array (min 65536 (- limit cursor)) :element-type '(unsigned-byte 8))))
                (loop while (< cursor limit)
                      for count = (min (length buffer) (- limit cursor))
                      do (unless (= count (read-sequence buffer stream :end count))
                           (error "short receive history segment"))
                         (physical-wal-decode-input decoder cursor (subseq buffer 0 count) :on-record callback)
                         (incf cursor count))))
            (if (and (< cursor flush)
                     (not (eql cursor (timeline-history-entry-end entry)))
                     (< cursor (+ segment-start +wal-segment-size+)))
                (+ segment-start +wal-segment-size+)
                cursor)))
        (+ segment-start +wal-segment-size+))))

(defun physical-store-validate-history-prefix (store &key on-record digest-cursor)
  "Validate retained selected-history records and cumulative digests through flush.
Compare saved fork digests when those boundaries are retained. Immutable selection
metadata keeps EOR provenance after a restartpoint removes old records. Hash each
raw record once per scan; return a requested digest only at a valid boundary."
  (let* ((floor (physical-store-floor-lsn store))
         (flush (physical-store-flushed-lsn store))
         (decoder (make-physical-wal-decoder :start-lsn floor))
         (cursor floor) (previous floor)
         (digest (physical-store-floor-digest store))
         (cursor-digest (and (eql digest-cursor floor) digest)))
    (labels ((check-fork (boundary)
               (loop for item = (physical-store-selection store) then (getf item :previous) while item
                     when (= boundary (getf item :fork))
                       do (unless (= digest (getf item :fork-digest))
                            (error "receive parent prefix differs from its history selection"))))
             (record (record raw)
               (physical-store-history-check-record store record raw previous)
               (setf previous (wal-record-end record)
                     digest (physical-store-update-digest digest raw))
               (check-fork previous)
               (when (eql digest-cursor previous) (setf cursor-digest digest))
               (when on-record (funcall on-record record raw))))
      (check-fork floor)
      (loop while (< cursor flush)
            do (setf cursor (physical-store-history-read-chunk store decoder cursor flush #'record))
               (when (> cursor flush) (error "missing acknowledged receive history bytes"))))
    (unless (and (= previous flush (physical-wal-decoder-next-lsn decoder))
                 (zerop (physical-wal-decoder-used decoder))
                 (= digest (physical-store-flushed-digest store)))
      (error "receive history differs from its acknowledged prefix"))
    (values nil cursor-digest)))

(defun physical-store-history-discard-tail (store)
  "Discard bytes outside the validated selected prefix, preserving each timeline's range."
  (let* ((history (getf (physical-store-selection store) :history))
         (floor (physical-store-floor-lsn store)) (flush (physical-store-flushed-lsn store)))
    (dolist (file (physical-store-history-segment-entries store))
      (destructuring-bind (timeline segment path) file
        (let* ((entry (find timeline history :key #'timeline-history-entry-timeline))
               (start (* segment +wal-segment-size+))
               (lower (and entry (max floor start (timeline-history-entry-begin entry))))
               (upper (and entry (min flush (+ start +wal-segment-size+)
                                      (or (timeline-history-entry-end entry) flush)))))
          (if (or (null entry) (>= lower upper))
              (delete-file path)
              (with-open-file (stream path :direction :io :if-exists :overwrite
                                           :element-type '(unsigned-byte 8))
                (when (> (file-length stream) (- upper start))
                  (sb-posix:ftruncate (sb-sys:fd-stream-fd stream) (- upper start))
                  (fsync-stream stream)))))))
    (fsync-directory (physical-store-directory store))))

(defun physical-store-check-follow-frontier (store pair history fork &key applied)
  "Check an (LSN timeline) frontier against both histories through FORK.
An applied frontier must be at or above the retained floor. At or above the
floor, prove the local prefix and its timeline; a required frontier below the
floor gets an ancestry check only and must come from verified durable recovery."
  (unless (and (listp pair) (= (length pair) 2)
               (typep (first pair) '(unsigned-byte 64))
               (typep (second pair) '(integer 1 4294967295))
               (<= (first pair) fork)
               (or (not applied) (<= (physical-store-floor-lsn store) (first pair)))
               (timeline-prefix-boundary-p (second pair) (first pair) history)
               (timeline-prefix-boundary-p
                (second pair) (first pair) (physical-store-retained-history store)))
    (error "follow frontier is outside the retained common ancestry"))
  (when (>= (first pair) (physical-store-floor-lsn store))
    (physical-store-prefix-digest-under-lock store (first pair))
    (unless (= (second pair) (physical-store-prefix-timeline store (first pair)))
      (error "follow frontier timeline differs from its durable prefix"))))

(defun physical-store-follow-candidate (store expected applied required target history-text eor-bytes)
  "Validate a proposed rewind against the full EXPECTED control tuple.
Validate the entire old acknowledged prefix, including the discarded tail, and
derive the fork digest locally. Require floor <= fork <= flush and check APPLIED
and REQUIRED as (LSN timeline) pairs. The caller supplies verified durable
recovery frontiers. The copied candidate owns no streams or lock; it validates
the same retained root under the candidate history without reconciling ownership."
  (unless (equal expected (physical-store-control-tuple store))
    (error "receive store changed since follow preparation"))
  (physical-store-read-retained-control store expected)
  (physical-store-check-complete-input store)
  (let* ((history (timeline-check-lineage (timeline-parse-history target history-text)))
         (fork (and (second history) (timeline-history-entry-end (second history)))))
    (unless (and fork (<= (physical-store-floor-lsn store) fork
                          (physical-store-flushed-lsn store)))
      (error "follow fork is outside the retained durable prefix"))
    (unless (physical-store-history-extends-p (physical-store-retained-history store) history)
      (error "follow history rewrites existing base ancestry"))
    (let ((digest (physical-store-prefix-digest-under-lock store fork)))
      (physical-store-check-follow-frontier store applied history fork :applied t)
      (physical-store-check-follow-frontier store required history fork)
      (multiple-value-bind (selection bytes)
          (physical-store-history-candidate store target history-text eor-bytes digest)
        (let ((candidate (copy-physical-store store)))
          (setf (physical-store-flushed-lsn candidate) fork
                (physical-store-written-lsn candidate) fork
                (physical-store-flushed-digest candidate) digest
                (physical-store-written-digest candidate) digest
                (physical-store-selection candidate) selection
                (physical-store-stream candidate) nil
                (physical-store-lock-stream candidate) nil)
          (physical-store-validate-prefix candidate)
          (when (physical-store-retained-root candidate)
            (physical-store-check-retained-identity candidate)
            (let ((index (physical-retained-inventory-load
                          (physical-store-retained-directory candidate)
                          (physical-store-retained-root candidate)
                          (physical-store-retained-identity candidate) history)))
              (physical-store-check-retained-coverage candidate (getf index :entries))))
          (values candidate bytes))))))

(defun physical-store-rewind-select-history
    (store expected applied required target history-text eor-bytes)
  "Rewind a private receive store to a fork and select its descendant history.
The caller must establish source proof, recovery and reload quiescence, and a
durable pending binding, and validate restartpoint, clean-batch, observed-XID
and required floors before production use. Publish one control with lower flush,
local digest and selection, preserving base, floor, manifest and retained root.
Read it back fully before installing memory and decoder state, reconciling root
ownership or discarding the tail. An ambiguous failure latches the store; reopen
to determine the durable state. This operation is not pg_rewind."
  (with-physical-store (store)
    (multiple-value-bind (candidate bytes)
        (physical-store-follow-candidate store expected applied required target history-text eor-bytes)
      (let ((fork (physical-store-flushed-lsn candidate))
            (digest (physical-store-flushed-digest candidate))
            (selection (physical-store-selection candidate)))
        (when (physical-store-stream store)
          (close (physical-store-stream store) :abort t))
        (setf (physical-store-stream store) nil
              (physical-store-segment store) nil
              (physical-store-segment-timeline store) nil)
        (physical-store-publish-history store selection bytes)
        (physical-store-save-control store fork :digest digest :selection selection)
        (physical-store-read-retained-control store (physical-store-control-tuple candidate))
        (physical-store-test-point :after-follow-control-readback store)
        (setf (physical-store-flushed-lsn store) fork
              (physical-store-written-lsn store) fork
              (physical-store-flushed-digest store) digest
              (physical-store-written-digest store) digest
              (physical-store-selection store) selection
              (physical-store-decoder store) (make-physical-wal-decoder :start-lsn fork))
        (physical-store-check-retained-root store)
        (physical-store-test-point :before-follow-tail-cleanup store)
        (physical-store-discard-tail store)
        (physical-store-test-point :after-follow-tail-cleanup store)
        target))))
