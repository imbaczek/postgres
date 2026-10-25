;;;; standard.lisp -- native nonstreamed pgoutput sender.
(in-package #:pg)

(defconstant +standard-stream-boundary-limit+ 1024)

(define-condition standard-replication-timeout (error) ())

(defstruct standard-stream
  stream socket slot-name slot-object publication-names
  emit-floor scan-cursor safe-frontier reported-flush restart-floor
  (pending-boundaries nil) pending-tail (pending-count 0)
  (input (make-array 39 :element-type '(unsigned-byte 8)))
  (input-count 0) input-length stop-reason reply-p raw-partial-p
  (last-send-time (get-internal-real-time))
  (last-reply-time (get-internal-real-time)))

(defun standard-note-frontier (state boundary)
  "Track only fully sent/filtered batch boundaries, bounded independently of WAL."
  (let ((confirmed (wal-replication-slot-confirmed-flush-lsn
                    (standard-stream-slot-object state))))
    (setf (standard-stream-safe-frontier state)
          (max boundary (standard-stream-safe-frontier state)))
    (if (<= boundary confirmed)
        (setf (standard-stream-restart-floor state)
              (max boundary (standard-stream-restart-floor state)))
        (let ((entry (list boundary)))
          (when (>= (standard-stream-pending-count state) +standard-stream-boundary-limit+)
            (error "standard replication boundary queue is full"))
          (if (standard-stream-pending-tail state)
              (setf (cdr (standard-stream-pending-tail state)) entry)
              (setf (standard-stream-pending-boundaries state) entry))
          (setf (standard-stream-pending-tail state) entry)
          (incf (standard-stream-pending-count state))))))

(defun standard-acknowledge-flush (state)
  "Persist exact flush with its aligned floor before retiring boundary entries."
  (let* ((slot (standard-stream-slot-object state))
         (flush (min (standard-stream-reported-flush state)
                     (standard-stream-safe-frontier state)))
         (safe (standard-stream-safe-frontier state))
         (remaining (standard-stream-pending-boundaries state))
         (floor (standard-stream-restart-floor state))
         (count (standard-stream-pending-count state)))
    (when (and (> flush (wal-replication-slot-confirmed-flush-lsn slot)) (<= flush safe))
      (loop while (and remaining (<= (car remaining) flush))
            do (setf floor (pop remaining))
               (decf count))
      (logical-test-failpoint :before-slot-acknowledgment (standard-stream-slot-name state) flush)
      (wal-acknowledge-standard-replication-slot
       (standard-stream-slot-name state) flush floor safe)
      (setf (standard-stream-pending-boundaries state) remaining
            (standard-stream-pending-count state) count
            (standard-stream-restart-floor state) floor)
      (unless remaining (setf (standard-stream-pending-tail state) nil)))))

(defun standard-accept-feedback (state payload)
  "Use flush alone; native apply/write may differ and resume claims may lead scans."
  (unless (and (= (length payload) 34) (= (aref payload 0) (char-code #\r))
               (member (aref payload 33) '(0 1)))
    (ereport-fatal "08P01" "invalid standard standby status"))
  (let ((flush (replication-read-u64 payload 9)))
    (when (> flush (max (standard-stream-safe-frontier state)
                        (standard-stream-emit-floor state)))
      (ereport "22023" "standby flush is past sent or requested replication progress"))
    (setf (standard-stream-reported-flush state)
          (max flush (standard-stream-reported-flush state))
          (standard-stream-last-reply-time state) (get-internal-real-time))
    (when (= (aref payload 33) 1) (setf (standard-stream-reply-p state) t))
    (standard-acknowledge-flush state)))

(defun replication-protocol-version (word)
  (let* ((value (or (replication-command-value word) ""))
         (text (string-left-trim (mapcar #'code-char '(9 10 11 12 13 32)) value))
         (digits (if (and (plusp (length text)) (char= (char text 0) #\+))
                     (subseq text 1) text)))
    (unless (and (plusp (length digits))
                 (every (lambda (character) (find character "0123456789")) digits))
      (ereport "22023" "invalid proto_version"))
    (let ((version (parse-integer digits)))
      (when (> version #xFFFFFFFF) (ereport "22023" "proto_version is out of range"))
      (unless (<= 1 version 4)
        (ereport "0A000" "unsupported pgoutput protocol version"))
      version)))

(defun parse-start-replication-options (words)
  "Parse C's plugin options; versions 1-4 share the supported nonstreamed layout."
  (let* ((options (when words (parse-replication-generic-options words)))
         (seen nil) (version 2))
    (dolist (option options)
      (let ((name (car option)) (word (cdr option)))
        (when (member name seen :test #'equal)
          (ereport "42601" "conflicting or redundant options"))
        (push name seen)
        (unless (or (null word) (char= (char word 0) #\'))
          (ereport "42601" "plugin option values must be quoted strings"))
        (cond
          ((equal name "proto_version") (setf version (replication-protocol-version word)))
          ((equal name "publication_names")
           (unless word (ereport "22023" "publication_names requires a value")))
          ((member name '("binary" "two_phase" "messages" "streaming") :test #'equal)
           (when (replication-option-boolean name word)
             (ereport "0A000" "~a is not supported by this pgoutput sender" name)))
          ((equal name "origin")
           (unless (string-equal (or (replication-command-value word) "") "any")
             (ereport "0A000" "only origin 'any' is supported")))
          (t (ereport "42601" "unrecognized pgoutput option ~a" name)))))
    (if (assoc "publication_names" options :test #'equal)
        (unless (assoc "proto_version" options :test #'equal)
          (ereport "22023" "proto_version option is required"))
        (unless (and (= version 2)
                     (every (lambda (option) (equal (car option) "proto_version")) options))
          (ereport "0A000" "private replication requires protocol version 2")))
    (values version options)))

(defun standard-publication-names (command)
  (let* ((options (logical-replication-command-options command))
         (word (cdr (assoc "publication_names" options :test #'equal))))
    (unless (and word (assoc "proto_version" options :test #'equal))
      (ereport "22023" "pgoutput requires proto_version and publication_names"))
    (let ((names
            (handler-case
                (search-path-identifiers (replication-command-value word))
              (pg-error () (ereport "42602" "invalid publication_names syntax")))))
      (unless names (ereport "22023" "publication_names must not be empty"))
      (mapcar #'sql-name-input names))))

(defun standard-selected-actions (names change)
  "Select only the immutable publication membership captured with this operation."
  (let ((history (logical-change-publication-selection change))
        (selection (make-hash-table :test #'eql)))
    (when (eq history :unknown)
      (ereport "0A000" "logical WAL lacks historic publication selection"))
    (logical-validate-publication-selection history)
    (dolist (relation history)
      (dolist (publication (getf relation :publications))
        (when (member (getf publication :name) names :test #'string=)
          (when (getf publication :unsupported)
            (ereport "0A000" "historic publication row filters are not supported by pgoutput"))
          (setf (gethash (getf relation :relation-oid) selection)
                (list (logical-change-kind change))))))
    selection))

(defun standard-truncate-oids (data selection)
  (let ((names (getf data :tables)) (oids (getf data :relation-oids)))
    (unless (and (listp names) (listp oids) (= (length names) (length oids))
                 (every #'stringp names)
                 (every (lambda (oid) (and (integerp oid) (< 0 oid #x100000000))) oids))
      (ereport "0A000" "TRUNCATE WAL lacks immutable relation identities"))
    (remove-if-not (lambda (oid) (member :truncate (gethash oid selection))) oids)))

(defun standard-truncate-relations (change selection)
  (let* ((data (logical-read-data-string (logical-change-event-payload change)))
         (oids (standard-truncate-oids data selection))
         (relations (getf data :relations)))
    (mapcar
     (lambda (oid)
       (let ((description (find oid relations :key (lambda (entry) (getf entry :oid)))))
         (unless (and (logical-plist-has-exact-keys-p
                       description '(:oid :name :columns :key-attnums))
                      (stringp (getf description :name))
                      (listp (getf description :columns))
                      (listp (getf description :key-attnums)))
           (ereport "0A000" "TRUNCATE WAL lacks a supported relation descriptor"))
         (make-logical-change :kind :truncate :relation-oid oid
                              :relation-name (getf description :name)
                              :columns (getf description :columns)
                              :key-attnums (getf description :key-attnums))))
     oids)))

(defun standard-transaction-changes (transaction names)
  "Use per-change historic publication state; never reinterpret a retained row."
  (loop for change in (logical-transaction-changes transaction)
        for kind = (logical-change-kind change)
        append
        (case kind
          ((:ddl :sequence-state) nil)
          (:truncate
           (let ((relations (standard-truncate-relations
                             change (standard-selected-actions names change))))
             (when relations (list (cons :truncate relations)))))
          ((:insert :update :delete)
           (when (member kind (gethash (logical-change-relation-oid change)
                                       (standard-selected-actions names change)))
             (list change)))
          (otherwise (ereport "0A000" "unsupported logical row operation")))))

(defun standard-transaction-visible-p (transaction floor)
  (let ((start (logical-transaction-commit-record-lsn transaction)))
    (unless start (error "logical transaction has no commit record position"))
    (>= start floor)))

(defun pgoutput-standard-begin (transaction)
  (unless (logical-transaction-commit-timestamp transaction)
    (ereport "0A000" "legacy WAL lacks a source commit timestamp"))
  (let ((payload (make-payload)))
    (payload-push-byte payload (char-code #\B))
    (replication-push-u64 payload (logical-transaction-commit-record-lsn transaction))
    (replication-push-i64 payload (logical-transaction-commit-timestamp transaction))
    (payload-push-u32 payload (logical-transaction-xid transaction))
    payload))

(defun pgoutput-standard-commit (transaction)
  (let ((payload (make-payload)))
    (payload-push-byte payload (char-code #\C))
    (payload-push-byte payload 0)
    (replication-push-u64 payload (logical-transaction-commit-record-lsn transaction))
    (replication-push-u64 payload (logical-transaction-commit-lsn transaction))
    (replication-push-i64 payload (logical-transaction-commit-timestamp transaction))
    payload))

(defun standard-stream-check (state)
  (check-for-interrupts)
  (sb-thread:with-mutex (*wal-lock*)
    (wal-check-healthy)
    (let ((slot (gethash (standard-stream-slot-name state) *wal-replication-slots*)))
      (unless (and (eq slot (standard-stream-slot-object state))
                   (eq (wal-replication-slot-active-owner slot) *wal-logical-slot-owner*))
        (ereport "55006" "standard replication slot is no longer owned"))
      (when (eq (wal-replication-slot-wal-status slot) :lost)
        (ereport "55000" "standard replication slot has lost its WAL"))))
  (when (> (- (get-internal-real-time) (standard-stream-last-reply-time state))
           (* 60 internal-time-units-per-second))
    (error 'standard-replication-timeout)))

(defun standard-input-ready-p (state)
  ;; Retain fd-stream read-ahead from the command parser. With one byte reader,
  ;; readiness guarantees the next read never waits for the rest of a frame.
  (or (listen (standard-stream-stream state))
      (sb-sys:wait-until-fd-usable
       (sb-bsd-sockets:socket-file-descriptor (standard-stream-socket state))
       :input 0 nil)))

(defun standard-input-header-length (bytes)
  (let ((type (code-char (aref bytes 0)))
        (length (bytes-to-u32 bytes 1)))
    (unless (case type
              (#\d (= length 38))
              ((#\c #\X) (= length 4)))
      (ereport-fatal "08P01" "invalid frontend frame in standard CopyBoth mode"))
    (1+ length)))

(defun standard-read-feedback-frame (state)
  "Consume at most one frame, retaining partial input and following command bytes."
  (when (standard-stream-stop-reason state)
    (return-from standard-read-feedback-frame nil))
  (let ((bytes (standard-stream-input state)))
    (loop while (standard-input-ready-p state)
          for byte = (read-byte (standard-stream-stream state) nil nil)
          do (unless byte
               (when (plusp (standard-stream-input-count state))
                 (ereport-fatal "08P01" "truncated standard replication feedback"))
               (setf (standard-stream-stop-reason state) :disconnect)
               (return nil))
             (setf (aref bytes (standard-stream-input-count state)) byte)
             (incf (standard-stream-input-count state))
             (when (= (standard-stream-input-count state) 5)
               (setf (standard-stream-input-length state)
                     (standard-input-header-length bytes)))
             (when (eql (standard-stream-input-count state)
                        (standard-stream-input-length state))
               (case (code-char (aref bytes 0))
                 (#\d (standard-accept-feedback state (subseq bytes 5 39)))
                 (#\c (setf (standard-stream-stop-reason state) :done))
                 (#\X (setf (standard-stream-stop-reason state) :disconnect)))
               (setf (standard-stream-input-count state) 0
                     (standard-stream-input-length state) nil)
               (return t)))))

(defun standard-poll-feedback (state)
  (with-live-operation
    (standard-stream-check state)
    (loop repeat 16
          while (and (not (standard-stream-stop-reason state))
                     (standard-read-feedback-frame state)))))

(defun standard-write-frame (state type payload &key finish-copy)
  "Write one bounded frame while consuming replies; never interleave frame bytes."
  (let* ((header (make-array 5 :element-type '(unsigned-byte 8)))
         (parts (list header payload))
         (written 0))
    (setf (aref header 0) (char-code type))
    (replace header (u32-to-bytes (+ 4 (length payload))) :start1 1)
    (unwind-protect
         (block sending
           (dolist (part parts)
             (loop with offset = 0
                   while (< offset (length part))
                   do (standard-poll-feedback state)
                      (when (and (standard-stream-stop-reason state)
                                 (zerop written) (not finish-copy))
                        (return-from sending nil))
                      (let ((chunk (subseq part offset
                                           (min (length part) (+ offset 65536)))))
                        ;; Retain this chunk across EAGAIN/EINTR retries.
                        (loop
                          (let ((count
                                  (sb-bsd-sockets:socket-send
                                   (standard-stream-socket state) chunk (length chunk)
                                   :dontwait t :nosignal t)))
                            (when (and count (plusp count))
                              (incf offset count)
                              (incf written count)
                              (setf (standard-stream-raw-partial-p state) t)
                              (return)))
                          (standard-poll-feedback state)
                          (when (and (standard-stream-stop-reason state)
                                     (zerop written) (not finish-copy))
                            (return-from sending nil))
                          (sb-sys:wait-until-fd-usable
                           (sb-bsd-sockets:socket-file-descriptor
                            (standard-stream-socket state))
                           :output 0.01 nil)))))
           (setf (standard-stream-raw-partial-p state) nil
                 (standard-stream-last-send-time state) (get-internal-real-time))
           t)
      ;; A condition interrupting an incomplete frame must close the connection:
      ;; no buffered ErrorResponse or CopyDone may be appended to its partial bytes.
      (when (standard-stream-raw-partial-p state)
        (ignore-errors (close (standard-stream-stream state) :abort t))))))

(defun standard-xlog-data (state payload &optional (end 0))
  (let ((data (make-payload)))
    (payload-push-byte data (char-code #\w))
    (replication-push-u64 data end)
    (replication-push-u64 data end)
    (replication-push-i64 data (postgres-current-timestamp))
    (payload-push data payload)
    (standard-write-frame state #\d data)))

(defun standard-keepalive (state request-reply)
  (let ((payload (make-payload)))
    (payload-push-byte payload (char-code #\k))
    (replication-push-u64 payload (standard-stream-safe-frontier state))
    (replication-push-i64 payload (postgres-current-timestamp))
    (payload-push-byte payload (if request-reply 1 0))
    (standard-write-frame state #\d payload)))

(defun standard-maybe-keepalive (state)
  (when (and (not (standard-stream-stop-reason state))
             (or (standard-stream-reply-p state)
                 (>= (- (get-internal-real-time)
                        (standard-stream-last-send-time state))
                     internal-time-units-per-second)))
    (let ((requested (not (standard-stream-reply-p state))))
      (setf (standard-stream-reply-p state) nil)
      (standard-keepalive state requested))))

(defun standard-send-transaction (state transaction changes)
  "Nonfinal headers stay zero; only fully transmitted Commit advances the frontier."
  (when (and changes
             (standard-transaction-visible-p transaction (standard-stream-emit-floor state)))
    (unless (standard-xlog-data state (pgoutput-standard-begin transaction))
      (return-from standard-send-transaction nil))
    (dolist (change changes)
      (when (standard-stream-stop-reason state)
        (return-from standard-send-transaction nil))
      (if (and (consp change) (eq (car change) :truncate))
          (progn
            (dolist (relation (cdr change))
              (unless (standard-xlog-data state (pgoutput-build-relation relation))
                (return-from standard-send-transaction nil)))
            (unless (standard-xlog-data
                     state (pgoutput-build-truncate
                            (mapcar #'logical-change-relation-oid (cdr change))))
              (return-from standard-send-transaction nil)))
          (progn
            (unless (standard-xlog-data state (pgoutput-build-relation change))
              (return-from standard-send-transaction nil))
            (unless (standard-xlog-data state (pgoutput-build-row-change change))
              (return-from standard-send-transaction nil))))
      (standard-maybe-keepalive state))
    (logical-test-failpoint :before-standard-commit (standard-stream-slot-name state)
                            (logical-transaction-commit-lsn transaction) transaction)
    (unless (standard-xlog-data state (pgoutput-standard-commit transaction)
                                (logical-transaction-commit-lsn transaction))
      (return-from standard-send-transaction nil)))
  (not (standard-stream-stop-reason state)))

(defun standard-stream-loop (state)
  (block streaming
    (loop
      (standard-poll-feedback state)
      (when (standard-stream-stop-reason state) (return))
      (let ((advanced nil))
        (when (< (standard-stream-pending-count state) +standard-stream-boundary-limit+)
          (multiple-value-bind (transactions progress)
              (with-live-operation
                (wal-logical-slot-read-transactions
                 (standard-stream-slot-name state)
                 :from-lsn (standard-stream-scan-cursor state) :max-transactions 1))
            (dolist (transaction transactions)
              (when (standard-transaction-visible-p transaction (standard-stream-emit-floor state))
                (unless (standard-send-transaction
                         state transaction
                         (standard-transaction-changes transaction (standard-stream-publication-names state)))
                  (return-from streaming nil))))
            (when (> progress (standard-stream-scan-cursor state))
              (setf advanced t (standard-stream-scan-cursor state) progress)
              (standard-note-frontier state progress)
              (with-live-operation (standard-acknowledge-flush state)))))
        (standard-maybe-keepalive state)
        (unless advanced
          (sb-sys:wait-until-fd-usable
           (sb-bsd-sockets:socket-file-descriptor (standard-stream-socket state)) :input 0.05 nil)))))
  (when (eq (standard-stream-stop-reason state) :done)
    (standard-write-frame state #\c #() :finish-copy t))
  (standard-stream-stop-reason state))

(defun start-standard-logical-replication (stream command)
  (let ((reason
          (handler-case
              (call-with-logical-replication-slot
               (logical-replication-command-slot-name command)
               (lambda (slot)
                 (unless (equal (wal-replication-slot-plugin slot) "pgoutput")
                   (ereport "0A000" "standard replication requires a pgoutput slot"))
                 (let* ((names (standard-publication-names command))
                        (confirmed (wal-replication-slot-confirmed-flush-lsn slot))
                        (state (make-standard-stream
                                :stream stream :socket (backend-socket *backend*)
                                :slot-name (wal-replication-slot-name slot) :slot-object slot
                                :publication-names names
                                :emit-floor
                                (max confirmed (logical-replication-command-requested-lsn command))
                                :scan-cursor (wal-replication-slot-restart-lsn slot)
                                :safe-frontier confirmed :reported-flush confirmed
                                :restart-floor (wal-replication-slot-restart-lsn slot))))
                   (logical-test-failpoint
                    :standard-stream-start (wal-replication-slot-name slot)
                    (logical-replication-command-protocol-version command) names)
                   (send-copy-both-response stream)
                   ;; Preserve input read-ahead while switching the fully drained
                   ;; output side of SBCL's dual-channel stream to raw sends.
                   (finish-output stream)
                   (standard-stream-loop state))))
            (standard-replication-timeout ()
              (ignore-errors (close stream :abort t))
              :disconnect))))
    ;; C releases its slot before both completion results and the following query.
    (case reason
      (:done
       (send-command-complete stream "COPY 0")
       (send-command-complete stream "START_REPLICATION")
       (send-ready-for-query stream #\I))
      (:disconnect (ignore-errors (close stream :abort t))))))
