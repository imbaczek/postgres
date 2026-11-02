(in-package #:pg)

(declaim (ftype function physical-promotion-check-child-checkpoint))

(defvar *physical-origin-proof-test-hook* nil)

(defun physical-origin-proof-test-point (point value)
  (when *physical-origin-proof-test-hook* (funcall *physical-origin-proof-test-hook* point value)))

(defparameter *physical-origin-proof-request-fields*
  '(("system_id" :system-id :u64) ("base_timeline" :base-timeline :timeline)
    ("base_lsn" :base :lsn) ("manifest_digest" :manifest-digest :u64) ("end_lsn" :end :lsn)))

(defparameter *physical-origin-proof-result-fields*
  (append '(("slot_name" :slot-name :text) ("anchor_digest" :anchor-digest :u64))
          *physical-origin-proof-request-fields*
          '(("end_digest" :end-digest :u64) ("root_id" :root-id :u64)
            ("fork_lsn" :fork :lsn) ("source_tli" :source-timeline :timeline)
            ("history" :history-text :text))))

(defun physical-origin-proof-check-request (request)
  (physical-follow-check-plist request '(:system-id :base-timeline :base :manifest-digest :end))
  (unless (and (every (lambda (key) (typep (getf request key) '(unsigned-byte 64)))
                      '(:system-id :base :manifest-digest :end))
               (typep (getf request :base-timeline) '(integer 1 4294967295))
               (<= (getf request :base) (getf request :end)))
    (error "invalid retained origin proof request"))
  (copy-list request))

(defun physical-origin-proof-command-request (command)
  (physical-origin-proof-check-request
   (loop for (name key kind) in *physical-origin-proof-request-fields*
         for text = (physical-stream-option command name)
         append (list key (if (eq kind :lsn) (parse-replication-lsn text)
                              (physical-stream-u64 text))))))

(defun parse-native-physical-prove-retained (words)
  (unless (and (>= (length words) 4) (string-equal (second words) "SLOT"))
    (ereport "42601" "expected PROVE_RETAINED SLOT name (native options)"))
  (let* ((name (replication-command-identifier (third words)))
         (options (parse-replication-generic-options (nthcdr 3 words)))
         (keys (append '("native_format" "anchor_digest")
                       (mapcar #'first *physical-origin-proof-request-fields*)))
         (command (make-logical-replication-command
                   :type :physical-prove :slot-name name :options options)))
    (unless (wal-valid-replication-slot-name-p name)
      (ereport "42602" "invalid retained origin proof pin name"))
    (unless (and (= (length keys) (length options))
                 (every (lambda (key) (= 1 (count key options :key #'car :test #'equal))) keys))
      (ereport "0A000" "PROVE_RETAINED requires exactly its native proof coordinates"))
    (unless (equal +physical-native-format+ (physical-stream-option command "native_format"))
      (ereport "0A000" "unsupported native physical WAL format"))
    (physical-stream-u64 (physical-stream-option command "anchor_digest"))
    (handler-case (physical-origin-proof-command-request command)
      (pg-error (condition) (error condition))
      (error (condition) (ereport "22023" "~a" condition)))
    command))

(defun physical-origin-proof-query (name anchor-digest request)
  (unless (and (wal-valid-replication-slot-name-p name)
               (typep anchor-digest '(unsigned-byte 64)))
    (error "invalid retained origin proof pin or anchor digest"))
  (physical-origin-proof-check-request request)
  (format nil "PROVE_RETAINED SLOT ~a (native_format '~a', anchor_digest '~d'~{~a~})"
          name +physical-native-format+ anchor-digest
          (loop for (field key kind) in *physical-origin-proof-request-fields*
                collect (format nil ", ~a '~a'" field
                                (if (eq kind :lsn) (wal-format-lsn (getf request key))
                                    (format nil "~d" (getf request key)))))))

(defun physical-origin-proof-check-root (fields history request)
  (physical-origin-proof-check-request request)
  (let* ((metadata (seventh fields))
         (reference (getf metadata :retained-reference))
         (base (getf request :base))
         (end (getf request :end))
         (timeline (getf request :base-timeline)))
    (unless (and reference
                 (eql (getf request :system-id) (getf metadata :system-id))
                 (every (lambda (key) (eql (getf request key) (getf reference key)))
                        '(:base-timeline :base :manifest-digest))
                 (<= base end (getf reference :end))
                 (timeline-prefix-boundary-p timeline end history)
                 (or (= base end) (= timeline (timeline-at-point (1- end) history))))
      (wal-invalid-input "retained origin proof differs from selected root identity or ancestry"))
    reference))

(defun physical-origin-proof-hash-root (fields history request)
  (physical-origin-proof-check-root fields history request)
  (let* ((end (getf request :end))
         (cursor (getf request :base))
         (digest #xCBF29CE484222325))
    (physical-promotion-check-primary-retained
     (third fields) history (seventh fields)
     :on-record
     (lambda (record raw)
       (physical-origin-proof-test-point :before-record record)
       (check-for-interrupts)
       (when (<= (wal-record-end record) end)
         (when (< (wal-record-start record) cursor)
           (error "retained origin proof repeated source WAL"))
         (setf digest (physical-store-update-digest digest raw)
               cursor (wal-record-end record)))))
    (unless (= cursor end)
      (wal-invalid-input "retained origin proof endpoint is not a complete record boundary"))
    digest))

(defun physical-origin-proof-start-under-lock (command request)
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (error "retained origin proof claim requires WAL exclusion"))
  (let* ((name (logical-replication-command-slot-name command))
         (pin (gethash name *physical-source-pins*))
         (anchor (when (physical-follow-pin-p pin) (physical-follow-pin-anchor pin)))
         (end (getf request :end)))
    (unless (and anchor (physical-follow-pin-ready-p pin)
                 (= (physical-stream-u64 (physical-stream-option command "anchor_digest"))
                    (physical-follow-anchor-digest anchor))
                 (= end (getf anchor :base) (getf anchor :receive-lsn)
                    (or (physical-source-pin-confirmed-lsn pin) -1))
                 (= (getf request :base-timeline) (getf anchor :base-timeline)
                    (getf anchor :receive-timeline))
                 (= (getf request :system-id) (getf anchor :system-id))
                 (= #xCBF29CE484222325 (getf anchor :receive-digest)
                    (physical-source-pin-prefix-digest pin)))
      (wal-invalid-input "retained origin proof requires its exact initial READY base"))
    (make-logical-replication-command
     :type :physical-start :requested-lsn end
     :options (list (cons "proof_pin" name) (cons "mode" "follow")
                    (cons "system_id" (format nil "~d" (getf anchor :system-id)))
                    (cons "manifest_digest" (format nil "~d" (getf anchor :manifest-digest)))
                    (cons "base_lsn" (format nil "'~a'" (wal-format-lsn end)))
                    (cons "timeline" (format nil "~d" (getf anchor :receive-timeline)))))))

(defun call-with-physical-origin-proof-claim (command request function)
  (unless *backend* (wal-invalid-input "retained origin proof requires a backend"))
  (let* ((name (logical-replication-command-slot-name command))
         (owner (make-physical-sender :name name :backend *backend*)))
    (unwind-protect
         (progn
           (with-live-operation
             (let ((start (sb-thread:with-mutex (*wal-lock*)
                            (physical-origin-proof-start-under-lock command request))))
               (physical-source-acquire-sender start owner)))
           (physical-origin-proof-test-point :after-claim owner)
           (funcall function owner))
      (with-live-operation-captured-backend-cleanup
        (physical-source-release-sender name owner)))))

(defun physical-origin-proof-capture-under-lock (command request owner)
  (unless (sb-thread:holding-mutex-p *wal-lock*)
    (error "retained origin proof capture requires WAL exclusion"))
  (wal-check-healthy)
  (let* ((name (logical-replication-command-slot-name command))
         (pin (physical-source-check-sender-under-lock name owner))
         (fields (multiple-value-list (wal-read-control-fields)))
         (system (catalog-system-identifier)))
    (physical-origin-proof-start-under-lock command request)
    (physical-follow-pin-check-live pin)
    (unless (and (= system (getf (seventh fields) :system-id))
                 (= *wal-insert-timeline* (fourth fields) (fifth fields))
                 (eq *wal-segment-namespace* (sixth fields))
                 (= *wal-durable-lsn* (second fields)))
      (wal-invalid-input "retained origin proof source control differs from its writer"))
    (multiple-value-bind (snapshot history metadata)
        (physical-promotion-check-child-checkpoint (fourth fields) (fifth fields))
      (unless (and (equal snapshot (third fields)) (equal metadata (seventh fields)))
        (error "retained origin proof child control changed during capture"))
      (let ((reference (physical-origin-proof-check-root fields history request)))
        (list :fields fields :history history :pin pin
              :anchor-digest (physical-follow-anchor-digest (physical-follow-pin-anchor pin))
              :system-id system :timeline *wal-insert-timeline* :namespace *wal-segment-namespace*
              :incarnation *wal-storage-incarnation* :thread sb-thread:*current-thread*
              :reference (copy-tree reference)
              :history-text (bytes->string (timeline-history-bytes *wal-insert-timeline*))
              :root-owner (physical-promotion-retained-owner :primary)
              :identity (physical-promotion-retained-identity system reference))))))

(defun physical-origin-proof-recheck (command request owner captured)
  (with-live-operation
    (sb-thread:with-mutex (*wal-lock*)
      (let ((current (physical-origin-proof-capture-under-lock command request owner)))
        (unless (and (every (lambda (key) (equalp (getf current key) (getf captured key)))
                            '(:system-id :timeline :namespace :reference :history-text :identity :anchor-digest))
                     (every (lambda (key) (eq (getf current key) (getf captured key)))
                            '(:pin :incarnation :thread :root-owner))
                     (<= (first (getf captured :fields)) (first (getf current :fields)))
                     (<= (second (getf captured :fields)) (second (getf current :fields)))
                     (eql (getf (getf captured :reference) :root-id)
                          (physical-retained-root-current (getf captured :root-owner))))
          (wal-invalid-input "retained origin proof lost its selected source context"))
        current))))

(defun physical-origin-proof-run (command)
  (let ((request (physical-origin-proof-command-request command)))
    (call-with-physical-origin-proof-claim
     command request
     (lambda (owner)
       (with-live-operation
         (let* ((captured (with-live-operation
                            (sb-thread:with-mutex (*wal-lock*)
                              (physical-origin-proof-capture-under-lock command request owner))))
                (reference (getf captured :reference)))
           (call-with-physical-retained-root-reader
            (lambda ()
              (physical-origin-proof-recheck command request owner captured)
              (physical-origin-proof-test-point :after-lease captured)
              (check-for-interrupts)
              (let ((digest (physical-origin-proof-hash-root
                             (getf captured :fields) (getf captured :history) request)))
                (physical-origin-proof-test-point :before-final-check captured)
                (let ((current (physical-origin-proof-recheck command request owner captured)))
                  (append (list :slot-name (logical-replication-command-slot-name command)
                                :anchor-digest (getf current :anchor-digest))
                          request
                          (list :end-digest digest :root-id (getf reference :root-id)
                                :fork (getf reference :end) :source-timeline (getf current :timeline)
                                :history-text (getf current :history-text))))))
            (getf captured :root-owner) (physical-recovery-retained-directory)
            (getf captured :identity) (getf reference :root-id))))))))

(defun physical-origin-proof-result-schema ()
  (loop for (name key kind) in *physical-origin-proof-result-fields*
        collect (list name (if (eq kind :timeline) +int8oid+ +textoid+))))

(defun physical-origin-proof-result-row (proof)
  (loop for (name key kind) in *physical-origin-proof-result-fields*
        for value = (getf proof key)
        collect (case kind
                  (:text value)
                  (:lsn (wal-format-lsn value))
                  (otherwise (format nil "~d" value)))))

(defun physical-origin-proof-decode-result (row name anchor request)
  (let* ((proof
           (loop for (field key kind) in *physical-origin-proof-result-fields*
                 for index from 0 for text = (aref row index)
                 append (list key
                              (case kind
                                (:text text)
                                (:lsn (pg-client-native-lsn text))
                                (:timeline (pg-client-native-u64 text t))
                                (otherwise (pg-client-native-u64 text))))))
         (history
           (handler-case
               (timeline-parse-history (getf proof :source-timeline) (getf proof :history-text))
             (pg-error (condition) (error condition))
             (error () (pg-client-native-reject "retained origin proof returned invalid history")))))
    (unless (and (equal name (getf proof :slot-name))
                 (= (physical-follow-anchor-digest anchor) (getf proof :anchor-digest))
                 (every (lambda (key) (eql (getf proof key) (getf request key)))
                        '(:system-id :base-timeline :base :manifest-digest :end))
                 (= (getf anchor :source-timeline) (getf proof :source-timeline))
                 (equal (getf anchor :history-text) (getf proof :history-text))
                 (<= (getf request :end) (getf proof :fork))
                 (= (getf proof :fork) (timeline-history-entry-begin (first history)))
                 (timeline-prefix-boundary-p (getf request :base-timeline) (getf request :end) history))
      (pg-client-native-reject "retained origin proof differs from its prepared source or request"))
    proof))

(defun pg-client-native-prove-retained (client name anchor request)
  (let* ((anchor (physical-follow-anchor-copy anchor))
         (request (physical-origin-proof-check-request request))
         (end (getf request :end)))
    (unless (and (= (getf request :system-id) (getf anchor :system-id))
                 (= end (getf anchor :base) (getf anchor :receive-lsn))
                 (= (getf request :base-timeline) (getf anchor :base-timeline)
                    (getf anchor :receive-timeline))
                 (= #xCBF29CE484222325 (getf anchor :receive-digest)))
      (error "retained origin proof requires the prepared initial canonical base"))
    (let ((row (physical-client-query
                client (physical-origin-proof-query name (physical-follow-anchor-digest anchor) request)
                (physical-origin-proof-result-schema) "PROVE_RETAINED"
                :maximum +physical-client-proof-frame-maximum+)))
      (physical-origin-proof-decode-result row name anchor request))))

(defun prove-native-physical-retained (stream command)
  (handler-case
      (let* ((proof (physical-origin-proof-run command))
             (columns (loop for (name oid) in (physical-origin-proof-result-schema)
                            collect (make-result-column :name name :type-oid oid
                                                        :typlen (if (= oid +int8oid+) 8 -1))))
             (row (physical-origin-proof-result-row proof)))
        (with-live-operation
          (send-row-description stream columns)
          (send-data-row-text stream row)
          (send-command-complete stream "PROVE_RETAINED")
          (send-ready-for-query stream (transaction-status-char))))
    (wal-input-validation-error (condition) (ereport "55000" "~a" condition))
    (pg-error (condition) (error condition))
    (error (condition) (ereport "XX000" "retained origin proof failed: ~a" condition))))
