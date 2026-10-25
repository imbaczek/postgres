;;;; Explicitly native WAL transport using PostgreSQL replication envelopes.
(in-package #:pg)

(defparameter +physical-native-format+ "pg_lisp_wal_5")

(defvar *physical-stream-test-hook* nil)

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

(defun parse-native-physical-start (words)
  (unless (and (>= (length words) 5) (string-equal (second words) "SLOT")
               (string-equal (fourth words) "PHYSICAL"))
    (ereport "0A000" "native physical replication requires an explicit format negotiation"))
  (let* ((name (replication-command-identifier (third words)))
         (options (parse-replication-generic-options (nthcdr 5 words)))
         (keys '("native_format" "system_id" "timeline" "base_lsn" "prefix_digest")))
    (unless (wal-valid-replication-slot-name-p name) (ereport "42602" "invalid source pin name"))
    (unless (and (= 5 (length options))
                 (every (lambda (key) (= 1 (count key options :key #'car :test #'equal))) keys))
      (ereport "0A000" "native physical stream requires exactly native_format, system_id, timeline, base_lsn, prefix_digest"))
    (let ((command (make-logical-replication-command
                    :type :physical-start :slot-name name :options options
                    :requested-lsn (parse-replication-lsn (fifth words)))))
      (unless (equal +physical-native-format+ (physical-stream-option command "native_format"))
        (ereport "0A000" "unsupported native physical WAL format"))
      (physical-stream-u64 (physical-stream-option command "system_id"))
      (physical-stream-u64 (physical-stream-option command "prefix_digest"))
      (unless (= 1 (physical-stream-u64 (physical-stream-option command "timeline")))
        (ereport "0A000" "native physical history transitions are not implemented"))
      (parse-replication-lsn (physical-stream-option command "base_lsn"))
      command)))

(defun physical-source-acquire-sender (command)
  (prevent-recovery-primary-operation "native physical walsender")
  (sb-thread:with-mutex (*wal-lock*)
    (let* ((name (logical-replication-command-slot-name command))
           (pin (gethash name *physical-source-pins*))
           (base (parse-replication-lsn (physical-stream-option command "base_lsn")))
           (cursor (logical-replication-command-requested-lsn command)))
      (when (and pin (physical-source-pin-lost-p pin))
        (ereport "55000" "native physical source retention is lost; reseed is required"))
      (unless (and pin (not (physical-source-pin-lost-p pin))
                   (= (physical-source-pin-system-id pin)
                      (physical-stream-u64 (physical-stream-option command "system_id"))
                      (catalog-system-identifier))
                   (= base (physical-source-pin-consistent-lsn pin))
                   (<= (or (physical-source-pin-confirmed-lsn pin) base) cursor *wal-durable-lsn*))
        (ereport "55000" "native source identity, retained base or resume boundary mismatch"))
      (when (gethash name *physical-source-senders*) (ereport "55006" "source pin already has a sender"))
      (physical-source-pin-check-wal pin)
      (let ((owner (gensym "PHYSICAL-SENDER-")))
        (setf (gethash name *physical-source-senders*) owner)
        owner))))

(defun physical-source-release-sender (name owner)
  (sb-thread:with-mutex (*wal-lock*)
    (when (eq owner (gethash name *physical-source-senders*))
      (remhash name *physical-source-senders*))))

(defun physical-source-sender-floor (name owner)
  (sb-thread:with-mutex (*wal-lock*)
    (let ((pin (gethash name *physical-source-pins*)))
      (unless (and pin (not (physical-source-pin-lost-p pin))
                   (eq owner (gethash name *physical-source-senders*)))
        (ereport "55000" "native physical source retention is lost; reseed is required"))
      (values (or (physical-source-pin-confirmed-lsn pin) (physical-source-pin-consistent-lsn pin))
              (physical-source-pin-prefix-digest pin)))))

(defun physical-source-read-next (name owner cursor)
  "Read one immutable validated record, never truncating live source WAL."
  (sb-thread:with-mutex (*wal-lock*)
    (wal-check-healthy)
    (unless (and (eq owner (gethash name *physical-source-senders*))
                 (let ((pin (gethash name *physical-source-pins*)))
                   (and pin (not (physical-source-pin-lost-p pin)))))
      (ereport "55000" "physical sender lost its local source pin ownership"))
    (let* ((durable *wal-durable-lsn*)
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
              (values (wal-decode-record raw start) raw durable))))))))

(defun physical-source-prove-range (name owner cursor requested digest &optional progress)
  "Hash only a newly acknowledged suffix, rejecting an interior record cursor."
  (let ((last-heartbeat (get-internal-real-time)))
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
  (sb-ext:with-timeout 10 (replication-standby-status stream)))

(defun physical-stream-read-status (stream previous sent base)
  "Read outside admission; validate monotonic progress under short admission."
  (let ((reply (physical-stream-feedback stream)))
    (when reply
      (with-live-operation
        (physical-stream-validate-status reply previous sent base)))))

(defun physical-stream-send-data (stream start durable bytes)
  (sb-ext:with-timeout 10 (send-logical-xlog-data stream start durable bytes)))

(defun physical-stream-send-keepalive (stream durable)
  (sb-ext:with-timeout 10 (send-logical-keepalive stream durable)))

(defun physical-stream-send-loop (stream command owner)
  (let* ((name (logical-replication-command-slot-name command))
         (cursor (logical-replication-command-requested-lsn command))
         (base (parse-replication-lsn (physical-stream-option command "base_lsn")))
         (status (list :write cursor :flush cursor :apply base)))
    (loop
      (multiple-value-bind (record raw durable)
          (with-live-operation (physical-source-read-next name owner cursor))
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
  (let ((owner (with-live-operation (physical-source-acquire-sender command)))
        (name (logical-replication-command-slot-name command))
        (cursor (logical-replication-command-requested-lsn command))
        (base (parse-replication-lsn (physical-stream-option command "base_lsn")))
        (copy-mode nil))
    (unwind-protect
         (handler-case
             (progn
               ;; No WAL bytes are sent until the exact prefix proof succeeds.
               (sb-ext:with-timeout 10 (send-copy-both-response stream))
               (setf copy-mode t)
               (let ((status (list :write cursor :flush cursor :apply base)))
                 (physical-source-validate-resume
                  command owner
                  (lambda (durable)
                    (physical-stream-send-keepalive stream durable)
                    (setf status (physical-stream-read-status stream status cursor base))
                    (unless status (error 'end-of-file :stream stream)))))
               ;; Proof feedback is staged above. Only the completed, matched
               ;; prefix may release source retention.
               (with-live-operation
                 (physical-source-confirm-receipt
                  name owner cursor (physical-stream-u64 (physical-stream-option command "prefix_digest"))))
               (physical-stream-send-loop stream command owner))
           ;; Retry transport failures without converting them to terminal XX000.
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
        (physical-source-release-sender name owner)))))
