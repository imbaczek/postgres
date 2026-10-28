;;;; Explicitly native WAL transport using PostgreSQL replication envelopes.
(in-package #:pg)

(defparameter +physical-native-format+ (format nil "pg_lisp_wal_~d" +wal-version+))

(defvar *physical-stream-test-hook* nil)

(defvar *physical-follow-source-read-cache* nil)

(define-condition physical-stream-copy-done (condition) ()
  (:documentation "An exact frontend CopyDone ended a physical stream normally."))

(defun physical-stream-option (command name)
  (let ((value (cdr (assoc name (logical-replication-command-options command) :test #'equal))))
    (when value (replication-command-value value))))

(defun physical-stream-u64 (text)
  (unless (and (stringp text) (<= 1 (length text) 20)
               (every (lambda (char) (find char "0123456789")) text))
    (ereport "22023" "native physical stream requires an unsigned decimal integer"))
  (let ((value (parse-integer text)))
    (unless (typep value '(unsigned-byte 64)) (ereport "22023" "physical integer out of range"))
    value))

(defun physical-stream-mode (command)
  "Select explicit follow mode or the default bootstrap mode; reject others."
  (let ((mode (or (physical-stream-option command "mode") "bootstrap")))
    (unless (member mode '("bootstrap" "follow") :test #'equal)
      (ereport "0A000" "unsupported native physical stream mode"))
    mode))

(defun physical-follow-stream-boundary (command pin)
  "Return the requested timeline's fork limit and next timeline from PIN.
Allow the segment containing the timeline's beginning, as C does, but reject
starts before that segment or beyond its fork. The pin names the current writer;
the requested timeline may be an ancestor."
  (let* ((history (physical-follow-pin-history pin))
         (timeline (physical-stream-u64 (physical-stream-option command "timeline")))
         (position (position timeline history :key #'timeline-history-entry-timeline))
         (entry (when position (nth position history)))
         (limit (when entry (timeline-history-entry-end entry))))
    (unless (and entry
                 (<= (* (floor (timeline-history-entry-begin entry) +wal-segment-size+)
                        +wal-segment-size+)
                     (logical-replication-command-requested-lsn command))
                 (or (null limit) (<= (logical-replication-command-requested-lsn command) limit)))
      (ereport "55000" "requested physical timeline or start is outside source ancestry"))
    (values limit (when (and position (plusp position))
                    (timeline-history-entry-timeline (nth (1- position) history))))))

(defun parse-native-physical-start (words)
  "Parse the explicitly negotiated native physical START and validate its options.
Follow mode is explicit; a failed bootstrap request never falls back to it."
  (unless (and (>= (length words) 5) (string-equal (second words) "SLOT")
               (string-equal (fourth words) "PHYSICAL"))
    (ereport "0A000" "native physical replication requires an explicit format negotiation"))
  (let* ((name (replication-command-identifier (third words)))
         (options (parse-replication-generic-options (nthcdr 5 words)))
         (keys '("native_format" "system_id" "timeline" "base_lsn" "prefix_digest")))
    (when (assoc "manifest_digest" options :test #'equal) (push "manifest_digest" keys))
    (when (assoc "mode" options :test #'equal) (push "mode" keys))
    (unless (wal-valid-replication-slot-name-p name) (ereport "42602" "invalid source pin name"))
    (unless (and (= (length keys) (length options))
                 (every (lambda (key) (= 1 (count key options :key #'car :test #'equal))) keys))
      (ereport "0A000" "native physical stream requires native_format, system_id, timeline, base_lsn, prefix_digest and optional manifest_digest and mode"))
    (let ((command (make-logical-replication-command
                    :type :physical-start :slot-name name :options options
                    :requested-lsn (parse-replication-lsn (fifth words)))))
      (unless (equal +physical-native-format+ (physical-stream-option command "native_format"))
        (ereport "0A000" "unsupported native physical WAL format"))
      (physical-stream-u64 (physical-stream-option command "system_id"))
      (physical-stream-u64 (physical-stream-option command "prefix_digest"))
      (when (physical-stream-option command "manifest_digest")
        (physical-stream-u64 (physical-stream-option command "manifest_digest")))
      (unless (typep (physical-stream-u64 (physical-stream-option command "timeline"))
                     '(integer 1 4294967295))
        (ereport "22023" "invalid native physical timeline"))
      (parse-replication-lsn (physical-stream-option command "base_lsn"))
      (physical-stream-mode command)
      command)))

(defparameter *physical-follow-wire-fields*
  '(("system_id" :system-id :u64) ("base_timeline" :base-timeline :timeline)
    ("base_lsn" :base :lsn) ("manifest_digest" :manifest-digest :u64)
    ("source_timeline" :source-timeline :timeline)
    ("receive_timeline" :receive-timeline :timeline) ("receive_lsn" :receive-lsn :lsn)
    ("prefix_digest" :receive-digest :u64) ("overlap_start" :overlap-start :lsn)
    ("overlap_digest" :overlap-digest :u64))
  "Ordered receiver option names, anchor keys and wire value kinds for preparation.
The same order defines the receiver fields in the result row.")

(defun physical-follow-prepare-request (command)
  "Decode receiver coordinates and check their wire types.
Timelines must fit a nonzero unsigned 32-bit value; LSNs and other fields use
their respective wire parsers. Source preparation checks ancestry and proof."
  (cons :version
        (cons 1
              (loop for (name key kind) in *physical-follow-wire-fields*
                    for text = (physical-stream-option command name)
                    for value = (if (eq kind :lsn) (parse-replication-lsn text)
                                    (physical-stream-u64 text))
                    do (when (and (eq kind :timeline)
                                  (not (typep value '(integer 1 4294967295))))
                         (ereport "22023" "invalid native physical timeline"))
                    append (list key value)))))

(defun parse-native-physical-prepare (words)
  "Parse PREPARE_FOLLOW and require the native format and each receiver field once.
Reject unknown, duplicate or missing options before preparing a pin."
  (unless (and (>= (length words) 4) (string-equal (second words) "SLOT"))
    (ereport "42601" "expected PREPARE_FOLLOW SLOT name (native options)"))
  (let* ((name (replication-command-identifier (third words)))
         (options (parse-replication-generic-options (nthcdr 3 words)))
         (keys (cons "native_format" (mapcar #'first *physical-follow-wire-fields*))))
    (unless (wal-valid-replication-slot-name-p name)
      (ereport "42602" "invalid physical follow pin name"))
    (unless (and (= (length keys) (length options))
                 (every (lambda (key) (= 1 (count key options :key #'car :test #'equal))) keys))
      (ereport "0A000" "PREPARE_FOLLOW requires exactly the negotiated receiver coordinates"))
    (let ((command (make-logical-replication-command
                    :type :physical-prepare :slot-name name :options options)))
      (unless (equal +physical-native-format+ (physical-stream-option command "native_format"))
        (ereport "0A000" "unsupported native physical WAL format"))
      (physical-follow-prepare-request command)
      command)))

(defun parse-native-physical-release (words)
  "Parse RELEASE_FOLLOW with exactly the native format and anchor digest."
  (unless (and (>= (length words) 4) (string-equal (second words) "SLOT"))
    (ereport "42601" "expected RELEASE_FOLLOW SLOT name (native options)"))
  (let* ((name (replication-command-identifier (third words)))
         (options (parse-replication-generic-options (nthcdr 3 words))))
    (unless (wal-valid-replication-slot-name-p name)
      (ereport "42602" "invalid physical follow pin name"))
    (unless (and (= 2 (length options))
                 (every (lambda (key) (= 1 (count key options :key #'car :test #'equal)))
                        '("native_format" "anchor_digest")))
      (ereport "0A000" "RELEASE_FOLLOW requires exactly the negotiated anchor identity"))
    (let ((command (make-logical-replication-command
                    :type :physical-release :slot-name name :options options)))
      (unless (equal +physical-native-format+ (physical-stream-option command "native_format"))
        (ereport "0A000" "unsupported native physical WAL format"))
      (physical-stream-u64 (physical-stream-option command "anchor_digest"))
      command)))

(defun release-native-physical-follow (stream command)
  "Release the checked follow pin and return command completion without a row.
Map validation failures to 55000, preserve PostgreSQL errors including busy,
and report unexpected failures as XX000."
  (handler-case
      (progn
        (with-live-operation
          (physical-source-follow-release
           (logical-replication-command-slot-name command)
           (physical-stream-u64 (physical-stream-option command "anchor_digest"))))
        (with-live-operation
          (send-command-complete stream "RELEASE_FOLLOW")
          (send-ready-for-query stream (transaction-status-char))))
    (wal-input-validation-error (condition) (ereport "55000" "~a" condition))
    (pg-error (condition) (error condition))
    (error (condition) (ereport "XX000" "physical follow release failed: ~a" condition))))

(defun physical-follow-prepare-row (command)
  "Prepare and recheck a live ready pin, then copy its source-owned result.
WAL input validation errors report 55000; preserve PostgreSQL errors and report
other preparation failures as XX000."
  (handler-case
      (let ((pin (physical-source-follow-prepare
                  (logical-replication-command-slot-name command)
                  (physical-follow-prepare-request command))))
        (with-live-operation
          (sb-thread:with-mutex (*wal-lock*)
            (physical-follow-pin-check-live pin)
            (unless (physical-follow-pin-ready-p pin)
              (wal-invalid-input "physical follow pin is not ready"))
            (let ((anchor (physical-follow-anchor-copy (physical-follow-pin-anchor pin))))
              (multiple-value-bind (history fork eor) (physical-follow-anchor-check anchor)
                (declare (ignore eor))
                (let* ((position (position (getf anchor :receive-timeline) history
                                           :key #'timeline-history-entry-timeline))
                       (next (timeline-history-entry-timeline (nth (1- position) history))))
                  (append
                   (list (copy-seq (physical-source-pin-name pin)))
                   (loop for (name key kind) in *physical-follow-wire-fields*
                         for value = (getf anchor key)
                         collect (if (eq kind :lsn) (wal-format-lsn value)
                                     (format nil "~d" value)))
                   (list (copy-seq (getf anchor :history-text)) (format nil "~d" next)
                         (wal-format-lsn fork)
                         (format nil "~(~{~2,'0X~}~)" (getf anchor :eor-octets))))))))))
    (wal-input-validation-error (condition) (ereport "55000" "~a" condition))
    (pg-error (condition) (error condition))
    (error (condition) (ereport "XX000" "physical follow preparation failed: ~a" condition))))

(defun prepare-native-physical-follow (stream command)
  "Return the prepared anchor as one typed row after durable READY.
Timelines use INT8; identifiers, unsigned values, LSNs, history and hexadecimal
end-of-recovery bytes use text."
  (let* ((row (physical-follow-prepare-row command))
         (fields (append '(("slot_name" nil :text)) *physical-follow-wire-fields*
                         '(("history" nil :text) ("next_tli" nil :timeline)
                           ("next_tli_startpos" nil :lsn) ("eor_hex" nil :text))))
         (columns (loop for (name key kind) in fields
                        collect (make-result-column
                                 :name name :type-oid (if (eq kind :timeline) +int8oid+ +textoid+)
                                 :typlen (if (eq kind :timeline) 8 -1)))))
    (with-live-operation
      (send-row-description stream columns)
      (send-data-row-text stream row)
      (send-command-complete stream "PREPARE_FOLLOW")
      (send-ready-for-query stream (transaction-status-char)))))

(defun physical-source-acquire-sender (command)
  "Validate the requested mode and claim its pin under WAL exclusion.
A follow pin must be ready and bound to the current writer timeline; START may
request an eligible ancestor. In follow mode base_lsn is the receiver's immutable
base and bounds apply feedback, not the anchor cursor."
  (prevent-recovery-primary-operation "native physical walsender")
  (sb-thread:with-mutex (*wal-lock*)
    (let* ((name (logical-replication-command-slot-name command))
           (pin (gethash name *physical-source-pins*))
           (follow (equal "follow" (physical-stream-mode command)))
           (base (parse-replication-lsn (physical-stream-option command "base_lsn")))
           (cursor (logical-replication-command-requested-lsn command)))
      (when (and pin (physical-source-pin-lost-p pin))
        (ereport "55000" "native physical source retention is lost; reseed is required"))
      (unless (and pin (eq follow (physical-follow-pin-p pin))
                   (not (physical-source-pin-lost-p pin))
                   (or (not follow) (physical-follow-pin-ready-p pin))
                   (= (physical-source-pin-timeline pin) *wal-insert-timeline*)
                   (or follow
                       (= *wal-insert-timeline*
                          (physical-stream-u64 (physical-stream-option command "timeline"))))
                   (= (physical-source-pin-system-id pin)
                      (physical-stream-u64 (physical-stream-option command "system_id"))
                      (catalog-system-identifier))
                   (let ((expected (physical-source-pin-manifest-digest pin))
                         (supplied (physical-stream-option command "manifest_digest")))
                     (if expected
                         (and supplied (= expected (physical-stream-u64 supplied)))
                         (and (= *wal-insert-timeline* 1) (null supplied))))
                   (= base (if follow (getf (physical-follow-pin-anchor pin) :base)
                               (physical-source-pin-consistent-lsn pin)))
                   (<= (or (physical-source-pin-confirmed-lsn pin)
                           (physical-source-pin-consistent-lsn pin))
                       cursor *wal-durable-lsn*))
        (ereport "55000" "native source mode, identity, retained base or resume boundary mismatch"))
      (when (gethash name *physical-source-senders*)
        (ereport "55006" "source pin already has a sender"))
      (if follow (physical-follow-pin-check-live pin) (physical-source-pin-check-wal pin))
      (multiple-value-bind (limit next)
          (if follow (physical-follow-stream-boundary command pin) (values))
        (let ((owner (gensym "PHYSICAL-SENDER-")))
          (setf (gethash name *physical-source-senders*) owner)
          (values owner limit next))))))

(defun physical-source-release-sender (name owner)
  (sb-thread:with-mutex (*wal-lock*)
    (when (eq owner (gethash name *physical-source-senders*))
      (remhash name *physical-source-senders*))))

(defun physical-source-sender-floor (name owner)
  (sb-thread:with-mutex (*wal-lock*)
    (let ((pin (gethash name *physical-source-pins*)))
      (unless (and pin (physical-source-pin-stream-ready-p pin)
                   (not (physical-source-pin-lost-p pin))
                   (= (physical-source-pin-timeline pin) *wal-insert-timeline*)
                   (eq owner (gethash name *physical-source-senders*)))
        (ereport "55000" "native physical source retention is lost; reseed is required"))
      (values (or (physical-source-pin-confirmed-lsn pin) (physical-source-pin-consistent-lsn pin))
              (physical-source-pin-prefix-digest pin)))))

(defun physical-source-read-next (name owner cursor)
  "Read one immutable validated record, never truncating live source WAL.
A follow pin dispatches to the historical source reader after ownership checks.
Explicit follow START admission acquires a ready follow pin."
  (sb-thread:with-mutex (*wal-lock*)
    (wal-check-healthy)
    (unless (and (eq owner (gethash name *physical-source-senders*))
                 (let ((pin (gethash name *physical-source-pins*)))
                   (and pin (not (physical-source-pin-lost-p pin))
                        (= (physical-source-pin-timeline pin) *wal-insert-timeline*))))
      (ereport "55000" "physical sender lost its local source pin ownership"))
    (let ((pin (gethash name *physical-source-pins*)))
      (when (physical-follow-pin-p pin)
        (return-from physical-source-read-next (physical-follow-source-read-next pin cursor))))
    (let* ((*wal-insert-timeline* (physical-source-pin-timeline (gethash name *physical-source-pins*)))
           (durable *wal-durable-lsn*)
           (segment (floor cursor +wal-segment-size+))
           (offset (mod cursor +wal-segment-size+))
           (start cursor))
      (when (= cursor durable) (return-from physical-source-read-next (values nil nil durable)))
      (unless (< cursor durable) (ereport "55000" "physical source cursor is beyond durable WAL"))
      (with-open-file (input (wal-segment-path segment) :element-type '(unsigned-byte 8))
        (when (= offset (file-length input))
          (unless (plusp offset) (error "empty physical source segment"))
          (incf segment)
          (setf offset 0 start (* segment +wal-segment-size+))))
      (with-open-file (input (wal-segment-path segment) :element-type '(unsigned-byte 8))
        (file-position input offset)
        (let ((header (make-array +wal-header-size+ :element-type '(unsigned-byte 8))))
          (unless (= +wal-header-size+ (read-sequence header input))
            (error "truncated retained physical source header"))
          (let* ((length (wal-u32 header 8))
                 (end (+ start length)))
            (unless (and (<= +wal-header-size+ length +wal-segment-size+)
                         (<= (+ offset length) +wal-segment-size+)
                         (<= end durable)
                         (or (= start cursor)
                             (> (+ (mod cursor +wal-segment-size+) length) +wal-segment-size+)))
              (error "invalid retained physical record length or segment gap"))
            (let ((raw (make-array length :element-type '(unsigned-byte 8))))
              (replace raw header)
              (unless (= length (read-sequence raw input :start +wal-header-size+))
                (error "truncated retained physical source record"))
              (let ((record (wal-decode-record raw start)))
                (wal-require-ordinary-record-type (wal-record-type record))
                (values record raw durable)))))))))

(defun physical-source-prove-range (name owner cursor requested digest &optional progress)
  "Hash only a newly acknowledged suffix, rejecting an interior record cursor."
  (let ((last-heartbeat (get-internal-real-time))
        (*physical-follow-source-read-cache* (or *physical-follow-source-read-cache* (list nil))))
    (loop while (< cursor requested)
          do (multiple-value-bind (record raw durable)
                 (with-live-operation (physical-source-read-next name owner cursor))
               (unless (and record (<= (wal-record-end record) requested))
                 (ereport "22023" "physical receive acknowledgment is not a retained record end"))
               (loop for offset from 0 below (length raw) by 65536
                     do (setf digest
                              (physical-store-update-digest
                               digest (subseq raw offset (min (length raw) (+ offset 65536)))))
                        (when (and progress
                                   (> (- (get-internal-real-time) last-heartbeat)
                                      (* 0.2 internal-time-units-per-second)))
                          (funcall progress durable)
                          (setf last-heartbeat (get-internal-real-time))))
               (setf cursor (wal-record-end record)))))
  digest)

(defun physical-source-confirm-status (command owner status)
  "Advance retention from validated flush, independently of write and apply."
  (let ((name (logical-replication-command-slot-name command))
        (received (getf status :flush)))
    (multiple-value-bind (floor digest)
        (with-live-operation (physical-source-sender-floor name owner))
      (when (> received floor)
        (setf digest (physical-source-prove-range name owner floor received digest)))
      (with-live-operation
        (physical-source-confirm-receipt name owner received digest))))
  status)

(defun physical-source-validate-resume (command owner &optional progress)
  "Prove a resume suffix from the durable confirmed floor, never the old base."
  (let ((name (logical-replication-command-slot-name command))
        (requested (logical-replication-command-requested-lsn command)))
    (multiple-value-bind (floor digest)
        (with-live-operation (physical-source-sender-floor name owner))
      (unless (<= floor requested)
        (ereport "55000" "physical resume precedes retained WAL; reseed is required"))
      (when progress (funcall progress requested))
      (setf digest (physical-source-prove-range name owner floor requested digest progress))
      (unless (= digest (physical-stream-u64 (physical-stream-option command "prefix_digest")))
        (ereport "55000" "native physical resume prefix differs from retained source history"))
      (values requested digest))))

(defun physical-stream-validate-status (status previous sent base)
  (unless (and status
               (<= base (getf status :apply) (getf status :flush) (getf status :write) sent)
               (every (lambda (key) (<= (getf previous key) (getf status key)))
                      '(:write :flush :apply)))
    (ereport-fatal "08P01" "invalid or regressing native physical feedback"))
  status)

(defun physical-stream-feedback (stream)
  (sb-ext:with-timeout 10 (replication-standby-status stream t)))

(defun physical-stream-read-status (stream previous sent base)
  "Read outside admission; validate monotonic progress under short admission."
  (multiple-value-bind (reply done) (physical-stream-feedback stream)
    (when done (signal 'physical-stream-copy-done))
    (when reply
      (with-live-operation
        (physical-stream-validate-status reply previous sent base)))))

(defun physical-stream-send-data (stream start durable bytes)
  (sb-ext:with-timeout 10 (send-logical-xlog-data stream start durable bytes)))

(defun physical-stream-send-keepalive (stream durable)
  (sb-ext:with-timeout 10 (send-logical-keepalive stream durable)))

(defun physical-stream-complete (stream command owner next limit)
  "Release the sender before the completion result and ReadyForQuery.
Captured-backend cleanup also permits release when admission has closed."
  (with-live-operation-captured-backend-cleanup
    (physical-source-release-sender (logical-replication-command-slot-name command) owner))
  (if next
      (physical-stream-switch-result stream next limit)
      (progn
        (send-command-complete stream "START_STREAMING")
        (send-ready-for-query stream (transaction-status-char)))))

(defun physical-stream-switch-result (stream next limit)
  "Return C's next-timeline result: an INT8 timeline and text fork position."
  (send-row-description
   stream (list (make-result-column :name "next_tli" :type-oid +int8oid+ :typlen 8)
                (make-result-column :name "next_tli_startpos" :type-oid +textoid+ :typlen -1)))
  (send-data-row-text stream (list (format nil "~d" next) (wal-format-lsn limit)))
  (send-command-complete stream "START_STREAMING")
  (send-ready-for-query stream (transaction-status-char)))

(defun physical-stream-finish-historic (stream command owner limit previous)
  "Finish the server's CopyBoth side and await frontend CopyDone.
Validate and confirm any status messages received before that completion."
  (write-message stream #\c (make-payload))
  (loop
    (multiple-value-bind (status done)
        (sb-ext:with-timeout 10 (replication-standby-status stream t))
      (when done (return t))
      (unless status (return nil))
      (setf previous
            (with-live-operation
              (physical-stream-validate-status
               status previous limit (parse-replication-lsn (physical-stream-option command "base_lsn")))))
      (physical-source-confirm-status command owner status))))

(defun physical-stream-send-loop (stream command owner &optional limit)
  "Send records with one dynamically scoped historical source prefix cache.
The cache belongs to this sender loop; reuse requires the same source selection.
A historic stream stops at its fork before sending child end-of-recovery WAL."
  (let* ((*physical-follow-source-read-cache* (list nil))
         (name (logical-replication-command-slot-name command))
         (cursor (logical-replication-command-requested-lsn command))
         (base (parse-replication-lsn (physical-stream-option command "base_lsn")))
         (status (list :write cursor :flush cursor :apply base)))
    (loop
      (when (and limit (= cursor limit))
        (return (values :switch status)))
      (multiple-value-bind (record raw durable)
          (with-live-operation (physical-source-read-next name owner cursor))
        (when limit
          (setf durable (min durable limit))
          (when (and record (> (wal-record-end record) limit))
            (ereport "55000" "physical record crosses the requested timeline fork")))
        (if record
            (loop for offset from 0 below (length raw) by 65536
                  for end = (min (length raw) (+ offset 65536))
                  do (physical-stream-send-data stream (+ (wal-record-start record) offset)
                                                durable (subseq raw offset end))
                     (setf status (physical-stream-read-status
                                   stream status (+ (wal-record-start record) end) base))
                     (unless status (return-from physical-stream-send-loop))
                     (physical-source-confirm-status command owner status)
                     (when *physical-stream-test-hook*
                       (funcall *physical-stream-test-hook* :after-fragment stream (< end (length raw)))))
            (progn
              (physical-stream-send-keepalive stream durable)
              (setf status (physical-stream-read-status stream status cursor base))
              (unless status (return))
              (physical-source-confirm-status command owner status)
              (sleep 0.05)))
        (when record (setf cursor (wal-record-end record)))))))

(defun start-native-physical-replication (stream command)
  "Serve a negotiated native physical stream with bounded timeline completion.
Prove the resume prefix before WalData, outside the writer critical section.
If the request starts at a historic fork, complete without CopyBoth; otherwise
finish CopyBoth before the switch result. Early exact CopyDone ends normally
without confirming an unproved receipt. Always release the sender on exit."
  (multiple-value-bind (owner limit next)
      (with-live-operation (physical-source-acquire-sender command))
    (let ((name (logical-replication-command-slot-name command))
          (cursor (logical-replication-command-requested-lsn command))
          (base (parse-replication-lsn (physical-stream-option command "base_lsn")))
          (copy-mode nil))
      (unwind-protect
           (handler-case
               (if (and limit (= cursor limit))
                   (progn
                     (physical-source-validate-resume command owner)
                     (with-live-operation
                       (physical-source-confirm-receipt
                        name owner cursor (physical-stream-u64 (physical-stream-option command "prefix_digest"))))
                     (physical-stream-complete stream command owner next limit))
                   (progn
                     ;; CopyBoth permits heartbeats during proof, but no WAL bytes.
                     (sb-ext:with-timeout 10 (send-copy-both-response stream))
                     (setf copy-mode t)
                     (let ((status (list :write cursor :flush cursor :apply base)))
                       (physical-source-validate-resume
                        command owner
                        (lambda (durable)
                          (physical-stream-send-keepalive stream (if limit (min durable limit) durable))
                          (setf status (physical-stream-read-status stream status cursor base))
                          (unless status (error 'end-of-file :stream stream)))))
                     (with-live-operation
                       (physical-source-confirm-receipt
                        name owner cursor (physical-stream-u64 (physical-stream-option command "prefix_digest"))))
                     (multiple-value-bind (result status)
                         (physical-stream-send-loop stream command owner limit)
                       (when (and (eq result :switch)
                                  (physical-stream-finish-historic stream command owner limit status))
                         (physical-stream-complete stream command owner next limit)))))
             (physical-stream-copy-done ()
               (write-message stream #\c (make-payload))
               (physical-stream-complete stream command owner next limit))
             (sb-ext:timeout () (ignore-errors (close stream :abort t)))
             (stream-error () (ignore-errors (close stream :abort t)))
             (sb-bsd-sockets:socket-error () (ignore-errors (close stream :abort t)))
             (error (condition)
               (unless copy-mode (error condition))
               (ignore-errors
                (send-error-response stream
                                     (if (typep condition 'pg-error) condition
                                         (make-condition 'pg-error :sqlstate "XX000"
                                                                   :message (princ-to-string condition)))))
               (ignore-errors (close stream :abort t))))
        (with-live-operation-captured-backend-cleanup
          (physical-source-release-sender name owner))))))
