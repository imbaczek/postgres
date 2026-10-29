(in-package #:pg)

;;; Checked native physical follow exchanges. The caller owns transport and
;;; lifecycle; these helpers do not admit a receiver.

(define-condition physical-client-stopped (error) ()
  (:documentation "The transport was explicitly stopped by its owner or controller."))

(defconstant +physical-client-proof-frame-maximum+
  (+ +physical-store-history-maximum-bytes+ 65536)
  "Per-command frame bound for a 16 MiB history and result framing.")

(defun pg-client-native-reject (message)
  "Signal a protocol violation; the caller must close the client."
  (error 'pg-client-error :sqlstate "08P01" :message message))

(defun physical-client-retryable-condition-p (condition)
  "Classify transport failures for both clients; protocol violation 08P01 is not retryable."
  (or (typep condition '(or stream-error sb-bsd-sockets:socket-error sb-ext:timeout))
      (and (typep condition 'pg-client-error)
           (member (pg-client-error-sqlstate condition)
                   '("08001" "08006" "57P01" "57P03" "53300" "55006") :test #'equal))))

(defun pg-client-native-frame (client maximum)
  "Read one complete frame within this command's bound.
Malformed or interrupted framing leaves cleanup and client closure to the caller."
  (multiple-value-bind (type length-bytes payload)
      (read-message-frame (pg-client-stream client) maximum)
    (when (and length-bytes (not (<= 4 (bytes-to-u32 length-bytes 0) maximum)))
      (pg-client-native-reject "native response frame exceeds its command bound"))
    (unless (and type length-bytes payload)
      (error "native source connection ended during a message"))
    (values (code-char type) payload)))

(defun pg-client-native-cstring (payload position)
  "Decode one terminated text field or signal a protocol violation."
  (handler-case (pg-client-cstring payload position)
    (error () (pg-client-native-reject "native response has an invalid text field"))))

(defun pg-client-native-check-description (payload schema)
  "Require the exact ordered column names, types and text format in SCHEMA."
  (unless (and (>= (length payload) 2) (= (bytes-to-u16 payload 0) (length schema)))
    (pg-client-native-reject "native result has an unexpected column count"))
  (let ((position 2))
    (dolist (expected schema)
      (multiple-value-bind (name next) (pg-client-native-cstring payload position)
        (unless (and (<= (+ next 18) (length payload)) (equal name (first expected))
                     (= (bytes-to-u32 payload next) 0) (= (bytes-to-u16 payload (+ next 4)) 0)
                     (= (bytes-to-u32 payload (+ next 6)) (second expected))
                     (= (bytes-to-u16 payload (+ next 10)) (if (= (second expected) +int8oid+) 8 65535))
                     (= (bytes-to-u32 payload (+ next 12)) #xFFFFFFFF)
                     (= (bytes-to-u16 payload (+ next 16)) 0))
          (pg-client-native-reject "native result column differs from its expected text format"))
        (setf position (+ next 18))))
    (unless (= position (length payload))
      (pg-client-native-reject "native result description has trailing bytes")))
  (mapcar (lambda (expected) (make-pg-client-column :name (first expected) :type-oid (second expected))) schema))

(defun pg-client-native-error-condition (payload)
  "Validate an ErrorResponse and construct its condition with its SQLSTATE."
  (let ((position 0) (terminated nil))
    (loop while (< position (length payload))
          for code = (aref payload position)
          do (incf position)
             (when (zerop code)
               (unless (= position (length payload))
                 (pg-client-native-reject "native error response has trailing bytes"))
               (setf terminated t)
               (return))
             (multiple-value-bind (text next) (pg-client-native-cstring payload position)
               (declare (ignore text))
               (setf position next)))
    (unless terminated (pg-client-native-reject "native error response lacks its terminator")))
  (handler-case (pg-client-error-from-payload payload)
    (error () (pg-client-native-reject "native source returned a malformed error response"))))

(defun pg-client-native-query-error (client payload maximum)
  "Drain a valid query error through ReadyForQuery, then signal its SQLSTATE.
A malformed response or transport failure requires the caller to close the client."
  (let ((condition (pg-client-native-error-condition payload)))
    (loop
      (multiple-value-bind (type bytes) (pg-client-native-frame client maximum)
        (case type
          (#\Z
           (unless (and (= 1 (length bytes)) (find (aref bytes 0) '(73 84 69)))
             (pg-client-native-reject "native error ended with invalid query status"))
           (error condition))
          ((#\N #\S) nil)
          (otherwise (pg-client-native-reject "native error lacks query completion")))))))

(defun pg-client-native-source-error (payload)
  "Signal a streaming source error immediately; the caller must close the client."
  (error (pg-client-native-error-condition payload)))

(defun pg-client-native-read-result (client schema tag &key (maximum 4096) first-frame command-only)
  "Read one checked native result in protocol order.
A row result requires T, D, C and Z; COMMAND-ONLY requires C and Z with no row.
Allow notices and parameter status between frames. A normal error drains through
Z before signalling; protocol or transport failure requires the caller to close."
  (let ((state (if command-only :complete :description)) (columns nil) (row nil))
    (loop
      (multiple-value-bind (type payload)
          (if first-frame (values-list (prog1 first-frame (setf first-frame nil)))
              (pg-client-native-frame client maximum))
        (case type
          (#\T
           (unless (eq state :description) (pg-client-native-reject "duplicate native result description"))
           (setf columns (pg-client-native-check-description payload schema) state :row))
          (#\D
           (unless (eq state :row) (pg-client-native-reject "native result has a missing or duplicate row"))
           (setf row (handler-case (pg-client-read-data-row payload columns)
                       (error () (pg-client-native-reject "native result has a malformed row")))
                 state :complete)
           (unless (every #'stringp row) (pg-client-native-reject "native result contains a null field")))
          (#\C
           (unless (eq state :complete) (pg-client-native-reject "native result completed before its row"))
           (multiple-value-bind (value end) (pg-client-native-cstring payload 0)
             (unless (and (equal value tag) (= end (length payload)))
               (pg-client-native-reject "native result has an unexpected completion tag")))
           (setf state :ready))
          (#\Z
           (unless (and (eq state :ready) (equalp payload #(73)))
             (pg-client-native-reject "native result ended without idle query completion"))
           (return row))
          ((#\N #\S) nil)
          (#\E (pg-client-native-query-error client payload maximum))
          (otherwise (pg-client-native-reject "unexpected native command response")))))))

(defun pg-client-native-u64 (text &optional timeline)
  "Parse a bounded unsigned result, optionally requiring a valid timeline."
  (let ((value (handler-case (physical-stream-u64 text)
                 (error () (pg-client-native-reject "native result has an invalid unsigned integer")))))
    (when (and timeline (not (<= 1 value #xFFFFFFFF)))
      (pg-client-native-reject "native result timeline is out of range"))
    value))

(defun pg-client-native-switch (client &optional first-frame)
  "Read a typed next-child and fork result at START or after streaming."
  (let* ((row (pg-client-native-read-result client
                                            (list (list "next_tli" +int8oid+)
                                                  (list "next_tli_startpos" +textoid+))
                                            "START_STREAMING" :first-frame first-frame))
         (next (pg-client-native-u64 (aref row 0) t))
         (fork (pg-client-native-lsn (aref row 1))))
    (unless (> next 1) (pg-client-native-reject "native switch does not name a child timeline"))
    (list :switch next fork)))

(defun pg-client-native-finish (client next fork)
  "Finish a historic stream after the backend CopyDone has been consumed.
The caller must first flush local receipt and send truthful feedback. Send
frontend CopyDone and require the returned child and fork to match the preparation."
  (write-message (pg-client-stream client) #\c (make-payload))
  (let ((result (pg-client-native-switch client)))
    (unless (equal result (list :switch next fork))
      (pg-client-native-reject "native switch differs from the prepared child and fork"))
    result))

(defun pg-client-native-start (client query)
  "Start native streaming; return :COPY-BOTH or (:SWITCH child fork).
The caller owns the client, timeouts and cancellation, and closes on protocol
or transport failure. Do not perform network I/O under recovery quiescence."
  (pg-client-send-query client query)
  (loop
    (multiple-value-bind (type payload) (pg-client-native-frame client 4096)
      (case type
        (#\W
         (unless (equalp payload #(0 0 0))
           (pg-client-native-reject "native stream has an unsupported CopyBoth format"))
         (return :copy-both))
        (#\T (return (pg-client-native-switch client (list type payload))))
        ((#\N #\S) nil)
        (#\E (pg-client-native-query-error client payload 4096))
        (otherwise (pg-client-native-reject "unexpected native stream startup response"))))))

(defun pg-client-native-lsn (text)
  "Parse a bounded native result LSN or signal a protocol violation."
  (unless (and (stringp text) (<= 3 (length text) 17)
               (every (lambda (char) (find char "0123456789abcdefABCDEF/")) text))
    (pg-client-native-reject "native result has an invalid WAL position"))
  (handler-case (parse-replication-lsn text)
    (error () (pg-client-native-reject "native result has an invalid WAL position"))))

;; Both transports use these exchanges; changing transport does not relax the
;; caller's source, timeline, WAL, and durability checks.
(defgeneric physical-client-query (client query schema tag &key maximum command-only))

(defmethod physical-client-query ((client pg-client) query schema tag
                                  &key (maximum 4096) command-only)
  (pg-client-send-query client query)
  (pg-client-native-read-result client schema tag :maximum maximum :command-only command-only))

(defgeneric physical-client-start (client query))

(defmethod physical-client-start ((client pg-client) query)
  (pg-client-native-start client query))

(defgeneric physical-client-finish (client next fork))

(defmethod physical-client-finish ((client pg-client) next fork)
  (pg-client-native-finish client next fork))

(defgeneric physical-client-receive (client))

(defmethod physical-client-receive ((client pg-client))
  (multiple-value-bind (kind payload)
      (sb-ext:with-timeout 10 (pg-client-native-frame client (+ 4 25 65536)))
    (case kind
      (#\d (values :data payload))
      (#\c
       (unless (zerop (length payload))
         (pg-client-native-reject "native CopyDone has trailing bytes"))
       (values :done nil))
      (#\E (pg-client-native-source-error payload))
      (otherwise (pg-client-native-reject "unexpected native replication response")))))

(defgeneric physical-client-feedback (client payload))

(defmethod physical-client-feedback ((client pg-client) payload)
  (write-message (pg-client-stream client) #\d payload))

(defgeneric physical-client-stop (client))

(defmethod physical-client-stop ((client pg-client))
  (sb-bsd-sockets:socket-shutdown (pg-client-socket client) :direction :io))

(defgeneric physical-client-close (client))

(defmethod physical-client-close ((client pg-client))
  (pg-client-close client))

(defmethod physical-client-close ((client null))
  (values))

(defun physical-client-identity-row (client)
  (physical-client-query client "IDENTIFY_SYSTEM"
                         '(("systemid" 25) ("timeline" 20) ("xlogpos" 25) ("dbname" 25))
                         "IDENTIFY_SYSTEM"))

(defun pg-client-native-history (client timeline)
  "Fetch and validate the requested child history and return its UTF-8 bytes.
Preserve valid UTF-8 bytes and check filename, bound and lineage; the caller
must compare the history with the source anchor."
  (unless (typep timeline '(integer 2 4294967295))
    (error "native history request requires a child timeline"))
  (let* ((row (physical-client-query
               client (format nil "TIMELINE_HISTORY ~d" timeline)
               (list (list "filename" +textoid+) (list "content" +textoid+))
               "TIMELINE_HISTORY" :maximum +physical-client-proof-frame-maximum+))
         (bytes (string->bytes (aref row 1))))
    (unless (and (equal (aref row 0) (format nil "~8,'0X.history" timeline))
                 (<= 1 (length bytes) +physical-store-history-maximum-bytes+))
      (pg-client-native-reject "native history filename or size differs from the requested timeline"))
    (handler-case (timeline-check-lineage (timeline-parse-history timeline (aref row 1)))
      (error () (pg-client-native-reject "native source returned invalid timeline history")))
    bytes))

(defun pg-client-native-request-query (verb slot coordinates)
  "Build PREPARE_FOLLOW or exact RELEASE_FOLLOW from the same receiver request.
The copied fields are shape-checked, not proof of source identity or history."
  (unless (member verb '("PREPARE_FOLLOW" "RELEASE_FOLLOW") :test #'equal)
    (error "invalid native follow request command"))
  (unless (wal-valid-replication-slot-name-p slot) (error "invalid native follow pin name"))
  (let ((request (physical-follow-request-copy coordinates)))
    (with-output-to-string (out)
      (format out "~a SLOT ~a (native_format '~a'" verb slot +physical-native-format+)
      (loop for (name key kind) in *physical-follow-wire-fields*
            for value = (getf request key)
            do (format out ", ~a '~a'" name (if (eq kind :lsn) (wal-format-lsn value)
                                                (format nil "~d" value))))
      (write-char #\) out))))

(defun pg-client-native-prepare-query (slot coordinates)
  "Build a native PREPARE_FOLLOW query from checked receiver coordinates."
  (pg-client-native-request-query "PREPARE_FOLLOW" slot coordinates))

(defun pg-client-native-eor (text)
  "Decode the fixed-size hexadecimal end-of-recovery record."
  (unless (and (= (length text) (* 2 +wal-end-of-recovery-record-size+))
               (every (lambda (char) (find char "0123456789abcdefABCDEF")) text))
    (pg-client-native-reject "native source returned invalid end-of-recovery hex"))
  (loop for position from 0 below (length text) by 2
        collect (parse-integer text :start position :end (+ position 2) :radix 16)))

(defun pg-client-native-prepare (client slot coordinates)
  "Prepare and return a checked source anchor for the supplied coordinates.
Check the typed row, echoes, history, EOR, next timeline and fork. The caller
must independently verify source identity and local durable witnesses, and
compare the actual immediate-child history with the source ancestry. This helper
does not install history, redo WAL, update recovery state or admit a receiver.
The caller owns network deadlines, cancellation and client cleanup; it must not
perform network I/O under recovery quiescence."
  (let* ((schema (append (list (list "slot_name" +textoid+))
                         (loop for (name key kind) in *physical-follow-wire-fields*
                               collect (list name (if (eq kind :timeline) +int8oid+ +textoid+)))
                         (list (list "history" +textoid+) (list "next_tli" +int8oid+)
                               (list "next_tli_startpos" +textoid+) (list "eor_hex" +textoid+))))
         (row (physical-client-query client (pg-client-native-prepare-query slot coordinates)
                                     schema "PREPARE_FOLLOW"
                                     :maximum +physical-client-proof-frame-maximum+))
         (anchor (append (copy-tree coordinates)
                         (list :history-text (aref row 11) :eor-octets (pg-client-native-eor (aref row 14))))))
    (unless (<= 1 (length (string->bytes (aref row 11))) +physical-store-history-maximum-bytes+)
      (pg-client-native-reject "native preparation history exceeds its file bound"))
    (unless (equal slot (aref row 0)) (pg-client-native-reject "native source prepared a different pin"))
    (loop for (name key kind) in *physical-follow-wire-fields* for index from 1
          for value = (if (eq kind :lsn) (pg-client-native-lsn (aref row index))
                          (pg-client-native-u64 (aref row index) (eq kind :timeline)))
          unless (= value (getf coordinates key))
            do (pg-client-native-reject "native source changed a prepared receive coordinate"))
    (handler-case
        (multiple-value-bind (history fork eor) (physical-follow-anchor-check anchor)
          (unless (and (= fork (pg-client-native-lsn (aref row 13)))
                       (= (pg-client-native-u64 (aref row 12) t)
                          (timeline-check-end-of-recovery eor (getf anchor :receive-timeline) history)))
            (pg-client-native-reject "native preparation next timeline or fork differs from its anchor")))
      (pg-client-error (condition) (error condition))
      (error () (pg-client-native-reject "native preparation returned an invalid source anchor")))
    anchor))

(defun pg-client-native-release (client slot expected-digest)
  "Request checked release of SLOT using its canonical anchor digest.
The caller decides when retirement is safe, after replacement is durable or by
explicit operator action; automated handoff and retirement are not integrated."
  (unless (and (wal-valid-replication-slot-name-p slot)
               (typep expected-digest '(unsigned-byte 64)))
    (error "invalid native follow retirement identity"))
  (physical-client-query
   client (format nil "RELEASE_FOLLOW SLOT ~a (native_format '~a', anchor_digest '~d')"
                  slot +physical-native-format+ expected-digest)
   nil "RELEASE_FOLLOW" :command-only t)
  t)

(defun pg-client-native-abort (client slot coordinates)
  "Cancel an attempt by its saved request, even if PREPARE returned no anchor.
The caller must retain cleanup intent if completion is uncertain."
  (physical-client-query client (pg-client-native-request-query "RELEASE_FOLLOW" slot coordinates)
                         nil "RELEASE_FOLLOW" :command-only t)
  t)

(declaim (ftype function physical-follow-bootstrap-request-copy))

(defun pg-client-native-release-bootstrap-query (slot coordinates)
  (unless (wal-valid-replication-slot-name-p slot) (error "invalid native bootstrap pin name"))
  (let ((request (physical-follow-bootstrap-request-copy coordinates)))
    (format nil
            "RELEASE_BOOTSTRAP SLOT ~a (native_format '~a', system_id '~d', source_timeline '~d', base_lsn '~a', manifest_digest '~d')"
            slot +physical-native-format+ (getf request :system-id) (getf request :source-timeline)
            (wal-format-lsn (getf request :base)) (getf request :manifest-digest))))

(defun pg-client-native-release-bootstrap (client slot coordinates)
  "Request checked bootstrap retirement and wait for command completion."
  (physical-client-query client (pg-client-native-release-bootstrap-query slot coordinates)
                         nil "RELEASE_BOOTSTRAP" :command-only t)
  t)
