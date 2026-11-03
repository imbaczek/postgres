(in-package #:pg)


(defparameter *physical-backup-lease-wire-verbs*
  '(("CAPTURE_BACKUP_NATIVE" . :physical-lease-capture)
    ("BACKUP_LEASE_STATUS" . :physical-lease-status)
    ("BACKUP_LEASE_ABORT" . :physical-lease-abort)))

(defparameter *physical-backup-lease-result-schema*
  '(("lease_name" 25) ("phase" 25) ("outcome" 25 :nullable)
    ("checkpoint_start" 25 :nullable) ("base_lsn" 25 :nullable)
    ("manifest_digest" 25 :nullable) ("successor_name" 25 :nullable)
    ("successor_anchor_digest" 25 :nullable)))

(defun parse-native-backup-lease-command (words)
  (handler-case
      (let* ((kind (cdr (assoc (string-upcase (first words)) *physical-backup-lease-wire-verbs*
                               :test #'equal)))
             (options (parse-replication-generic-options (rest words)))
             (keys '("native_format" "lease_name" "lease_token" "cluster_identity" "system_id" "source_timeline")))
        (unless (and kind (= (length keys) (length options))
                     (every (lambda (key) (= 1 (count key options :key #'car :test #'equal))) keys)
                     (every (lambda (option)
                              (let ((word (cdr option)))
                                (and (stringp word) (plusp (length word)) (char= #\' (char word 0)))))
                            options))
          (ereport "42601" "invalid native backup lease options"))
        (flet ((value (key) (replication-command-value (cdr (assoc key options :test #'equal)))))
          (unless (equal +physical-native-format+ (value "native_format"))
            (ereport "0A000" "unsupported native physical WAL format"))
          (let ((request (physical-backup-lease-request-copy
                          (list :version 1 :name (value "lease_name") :token (value "lease_token")
                                :cluster-identity (value "cluster_identity")
                                :system-id (physical-stream-u64 (value "system_id"))
                                :timeline (physical-stream-u64 (value "source_timeline"))))))
            (make-logical-replication-command :type kind :slot-name (getf request :name)
                                              :options (list (cons "request" request))))))
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
      ((:physical-lease-status :physical-lease-abort) nil)
      (otherwise (ereport "42601" "unsupported native backup lease command")))))

(defun send-native-backup-lease-result (stream request record tag)
  (let ((row (physical-backup-lease-result-row request record))
        (columns (loop for (name oid) in *physical-backup-lease-result-schema*
                       collect (make-result-column :name name :type-oid oid :typlen -1))))
    (with-live-operation
      (send-row-description stream columns)
      (send-data-row-text stream row)
      (send-command-complete stream tag)
      (send-ready-for-query stream (transaction-status-char)))))

(defun execute-native-backup-lease-command (stream command)
  (handler-case
      (progn
        (physical-backup-lease-wire-preflight command)
        (let* ((request (physical-backup-lease-command-request command))
               (abort-p (eq :physical-lease-abort (logical-replication-command-type command)))
               (record (if abort-p (physical-backup-lease-abort request)
                           (physical-backup-lease-status request))))
          (send-native-backup-lease-result stream request record
                                           (if abort-p "BACKUP_LEASE_ABORT" "BACKUP_LEASE_STATUS"))))
    (wal-input-validation-error () (ereport "55000" "backup lease request is not valid for this source"))
    (pg-error (condition) (error condition))
    (error () (ereport "XX000" "native backup lease command failed"))))

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
