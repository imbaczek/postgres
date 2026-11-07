(in-package #:pg)

;;; Native backup lease commands for admitted physical replication senders.
;;; All use the six lease options. HANDOFF and CLEANUP_SELECTED add successor
;;; identity. Allocated follow commands add generation, name and receiver coordinates.
;;; Values are quoted. The token is never echoed in a result or error.

;; STATUS, ABORT, HANDOFF and CLEANUP_SELECTED return eight text columns. UNKNOWN
;; has no state fields. Outcome, base identity and successor identity may be null.
(defparameter *physical-backup-lease-result-schema*
  '(("lease_name" 25) ("phase" 25) ("outcome" 25 :nullable)
    ("checkpoint_start" 25 :nullable) ("base_lsn" 25 :nullable)
    ("manifest_digest" 25 :nullable) ("successor_name" 25 :nullable)
    ("successor_anchor_digest" 25 :nullable)))

(defun physical-backup-lease-command-allocation (command)
  (physical-backup-lease-allocation-copy
   (cdr (assoc "allocation" (logical-replication-command-options command) :test #'equal))))

;; Allocation commands add a generation, a name, and prefixed follow
;; coordinates.  The lease source timeline is the capture timeline; the
;; follow source timeline identifies the chosen writer and can differ.

(defun physical-backup-lease-wire-option-keys (kind)
  (append '("native_format" "lease_name" "lease_token" "cluster_identity" "system_id" "source_timeline")
          (case kind
            ((:physical-lease-handoff :physical-lease-cleanup-selected)
             '("successor_name" "successor_anchor_digest"))
            ((:physical-lease-prepare-follow :physical-lease-cancel-follow)
             (append '("allocation_generation" "allocation_name")
                     (loop for (name) in *physical-follow-wire-fields*
                           collect (concatenate 'string "follow_" name)))))))

(defun physical-backup-lease-wire-allocation (kind request options)
  (when (member kind '(:physical-lease-prepare-follow :physical-lease-cancel-follow))
    (flet ((value (key) (replication-command-value (cdr (assoc key options :test #'equal)))))
      (physical-backup-lease-follow-intent
       request (physical-stream-u64 (value "allocation_generation")) (value "allocation_name")
       (physical-follow-prepare-request
        (make-logical-replication-command :options options)
        "follow_")
       :live))))

;; Require each verb's options exactly once as quoted literals. Allocated follow
;; commands also accept an optional follow_proof_kind.
(defun parse-native-backup-lease-command (words)
  (handler-case
      (let* ((kind (cdr (assoc (string-upcase (first words)) *physical-backup-lease-wire-verbs*
                               :test #'equal)))
             (options (parse-replication-generic-options (rest words)))
             (keys (physical-backup-lease-wire-option-keys kind))
             (optional (when (member kind '(:physical-lease-prepare-follow :physical-lease-cancel-follow))
                         '("follow_proof_kind"))))
        (unless (and kind
                     (every (lambda (key) (= 1 (count key options :key #'car :test #'equal))) keys)
                     (every (lambda (option)
                              (let ((word (cdr option)) (key (car option)))
                                (and (member key (append keys optional) :test #'equal)
                                     (= 1 (count key options :key #'car :test #'equal))
                                     (stringp word) (plusp (length word))
                                     (char= #\' (char word 0)))))
                            options))
          (ereport "42601" "invalid native backup lease options"))
        (flet ((value (key) (replication-command-value (cdr (assoc key options :test #'equal)))))
          (unless (equal +physical-native-format+ (value "native_format"))
            (ereport "0A000" "unsupported native physical WAL format"))
          (let* ((request (physical-backup-lease-request-copy
                           (list :version 1 :name (value "lease_name") :token (value "lease_token")
                                 :cluster-identity (value "cluster_identity")
                                 :system-id (physical-stream-u64 (value "system_id"))
                                 :timeline (physical-stream-u64 (value "source_timeline")))))
                 (successor (when (member kind '(:physical-lease-handoff :physical-lease-cleanup-selected))
                              (physical-backup-lease-successor-copy
                               (list :name (value "successor_name") :anchor-digest
                                     (physical-stream-u64 (value "successor_anchor_digest")))
                               request)))
                 (allocation (physical-backup-lease-wire-allocation kind request options)))
            (make-logical-replication-command
             :type kind :slot-name (getf request :name)
             :options (append (list (cons "request" request))
                              (when successor (list (cons "successor" successor)))
                              (when allocation (list (cons "allocation" allocation))))))))
    (pg-error (condition) (ereport (pg-error-sqlstate condition) "invalid native backup lease request"))
    (error () (ereport "22023" "invalid native backup lease request"))))

(defun physical-backup-lease-command-request (command)
  (physical-backup-lease-request-copy
   (cdr (assoc "request" (logical-replication-command-options command) :test #'equal))))

(defun physical-backup-lease-result-row (request record)
  (let ((request (physical-backup-lease-request-copy request))
        (record (when record (physical-backup-lease-copy record))))
    (when (and record (not (equal request (getf record :request))))
      (error "backup lease result has a different request"))
    (let ((base (getf record :base)) (successor (getf record :successor)))
      (list (getf request :name)
            (if record (string-downcase (symbol-name (getf record :phase))) "unknown")
            (when (getf record :outcome) (string-downcase (symbol-name (getf record :outcome))))
            (when base (wal-format-lsn (getf base :checkpoint-start)))
            (when base (wal-format-lsn (getf base :base)))
            (when base (format nil "~d" (getf base :manifest-digest)))
            (getf successor :name)
            (when successor (format nil "~d" (getf successor :anchor-digest)))))))

(defun physical-backup-lease-wire-preflight (command)
  ;; Every lease command needs an admitted physical sender and a clear transaction.
  ;; Selected cleanup is also allowed during recovery after the source checks.
  (with-live-operation
    (unless (and *backend* (eq :physical (backend-replication-mode *backend*))
                 (wal-sender-for-backend))
      (ereport "55000" "backup leases require an admitted physical replication connection"))
    (when (backend-explicit *backend*)
      (ereport "25001" "backup lease commands cannot run inside a transaction block"))
    (when (eq :aborted (backend-state *backend*))
      (ereport "25P02" "current transaction is aborted"))
    (check-recovery-available)
    (case (logical-replication-command-type command)
      (:physical-lease-capture
       (prevent-recovery-primary-operation "leased native backup")
       (when *swank-listener-enabled-p*
         (ereport "55000" "leased native backup requires Swank to be off"))
       (with-wal-critical (wal-check-healthy)))
      ((:physical-lease-handoff :physical-lease-prepare-follow)
       (prevent-recovery-primary-operation "backup lease handoff")
       (with-wal-critical (wal-check-healthy)))
      ((:physical-lease-status :physical-lease-abort :physical-lease-cancel-follow
                               :physical-lease-cleanup-selected) nil)
      (otherwise (ereport "42601" "unsupported native backup lease command")))))

(defun send-native-backup-lease-row (stream row schema tag)
  (let ((columns (loop for (name oid) in schema
                       collect (make-result-column :name name :type-oid oid
                                                   :typlen (if (= oid +int8oid+) 8 -1)))))
    (with-live-operation
      (send-row-description stream columns)
      (send-data-row-text stream row)
      (send-command-complete stream tag)
      (send-ready-for-query stream (transaction-status-char)))))

(defun send-native-backup-lease-result (stream request record tag)
  (send-native-backup-lease-row
   stream (physical-backup-lease-result-row request record) *physical-backup-lease-result-schema* tag))

(defparameter *physical-backup-lease-follow-cancel-schema*
  (append *physical-backup-lease-result-schema*
          '(("allocation_generation" 20) ("allocation_name" 25))))

(defun physical-backup-lease-follow-prepare-schema (coordinates)
  (append '(("lease_name" 25) ("allocation_generation" 20))
          (physical-follow-prepare-schema coordinates)))

;; Preparation's operation context has ended.  Under fresh admission, write
;; and WAL exclusion, recheck the durable parent and live allocation, the exact
;; registered pin, ready anchor and source health.  Copy the row before I/O;
;; cancellation before this snapshot rejects it, while later cancellation may
;; follow a successful reply.

(defun physical-backup-lease-follow-prepare-snapshot (row pin)
  (with-live-operation
    (with-write-lock
      (sb-thread:with-mutex (*wal-lock*)
        (call-with-physical-backup-lease-allocation-context
         :prepare row
         (lambda ()
           (physical-backup-lease-allocation-check-prepare
            (getf row :name) (getf row :coordinates) nil t)
           (unless (and (physical-follow-pin-p pin)
                        (equal (getf row :name) (physical-source-pin-name pin))
                        (eq pin (gethash (getf row :name) *physical-source-pins*)))
             (wal-invalid-input "backup lease allocation no longer owns its prepared pin"))
           (physical-follow-pin-check-live pin)
           (unless (and (physical-follow-pin-ready-p pin)
                        (physical-follow-request-matches-anchor-p
                         (getf row :coordinates) (physical-follow-pin-anchor pin)))
             (wal-invalid-input "backup lease allocation no longer has its ready anchor"))
           (append (list (copy-seq (getf (getf row :request) :name))
                         (format nil "~d" (getf row :generation)))
                   (physical-follow-prepare-anchor-row
                    (getf row :name) (physical-follow-anchor-copy (physical-follow-pin-anchor pin))))))))))

;; PREPARE prefixes the ordinary anchor row with lease name and generation.
;; CANCEL returns the checked parent and submitted allocation identity, not a
;; claim that a canceled child row exists.  A selected closing or closed handoff
;; is preserved, and an unknown child of a terminal parent is not created.

(defun execute-native-backup-lease-follow-command (stream command)
  (let* ((row (physical-backup-lease-command-allocation command))
         (request (getf row :request))
         (generation (getf row :generation))
         (name (getf row :name))
         (coordinates (getf row :coordinates))
         (prepare (eq :physical-lease-prepare-follow (logical-replication-command-type command))))
    (if prepare
        (let* ((pin (physical-backup-lease-follow-prepare request generation name coordinates))
               (result (physical-backup-lease-follow-prepare-snapshot row pin)))
          (send-native-backup-lease-row
           stream result (physical-backup-lease-follow-prepare-schema coordinates)
           "BACKUP_LEASE_PREPARE_FOLLOW"))
        (let ((parent (physical-backup-lease-follow-cancel request generation name coordinates)))
          (unless parent (wal-invalid-input "backup lease allocation has no parent outcome"))
          (send-native-backup-lease-row
           stream (append (physical-backup-lease-result-row request parent)
                          (list (format nil "~d" generation) name))
           *physical-backup-lease-follow-cancel-schema* "BACKUP_LEASE_CANCEL_FOLLOW")))))

;; A successful HANDOFF row reports the source's durable CLOSED outcome and
;; successor. A lost reply leaves the caller to recover the outcome with STATUS
;; or an exact retry on the original route.
(defun execute-native-backup-lease-command (stream command)
  (handler-case
      (progn
        (physical-backup-lease-wire-preflight command)
        (when (member (logical-replication-command-type command)
                      '(:physical-lease-prepare-follow :physical-lease-cancel-follow))
          (return-from execute-native-backup-lease-command
            (execute-native-backup-lease-follow-command stream command)))
        (let* ((request (physical-backup-lease-command-request command))
               (kind (logical-replication-command-type command))
               (record
                 (ecase kind
                   (:physical-lease-status (physical-backup-lease-status request))
                   (:physical-lease-abort (physical-backup-lease-abort request))
                   ;; Cleanup verifies the committed selection structurally and
                   ;; retires bootstrap ownership.  Handoff checks usable WAL.
                   (:physical-lease-cleanup-selected
                    (physical-backup-lease-cleanup-selected
                     request (cdr (assoc "successor" (logical-replication-command-options command)
                                         :test #'equal))))
                   (:physical-lease-handoff
                    (physical-backup-lease-handoff
                     request (cdr (assoc "successor" (logical-replication-command-options command)
                                         :test #'equal)))))))
          (send-native-backup-lease-result
           stream request record (car (rassoc kind *physical-backup-lease-wire-verbs*)))))
    (wal-input-validation-error () (ereport "55000" "backup lease request is not valid for this source"))
    (pg-error (condition) (error condition))
    (error () (ereport "XX000" "native backup lease command failed"))))

;; Start COPY only after a new claim is durably CAPTURING.  An existing claim
;; returns an error before COPY; it cannot retransmit an earlier stage.
(defun start-native-backup-lease-capture (stream command)
  (handler-case
      (progn
        (physical-backup-lease-wire-preflight command)
        (call-with-native-backup-copy-out
         stream "CAPTURE_BACKUP_NATIVE"
         (lambda (stage begin-copy)
           (multiple-value-bind (manifest claimed)
               (physical-backup-lease-capture stage (physical-backup-lease-command-request command)
                                              :on-start begin-copy)
             (unless claimed
               (ereport "55000" "backup lease already exists; use BACKUP_LEASE_STATUS or BACKUP_LEASE_ABORT"))
             manifest))))
    (wal-input-validation-error () (ereport "55000" "backup lease capture is not valid for this source"))
    (pg-error (condition) (error condition))
    (error () (ereport "XX000" "leased native backup failed"))))
