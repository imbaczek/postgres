;;;; Logical WAL sender shutdown.
;;
;; Capture the last flushed WAL position before the shutdown checkpoint.
;; Each ready stream keeps its exact slot claim and source epoch until cleanup.
;; The live-operation lock protects sender state. Validate storage under the WAL
;; lock between state checks, releasing each lock before acquiring the other.

(in-package #:pg)

(defstruct (logical-shutdown-owner
            (:constructor make-logical-shutdown-owner
                (slot name claim backend system-id timeline namespace incarnation database plugin)))
  ;; Capture the exact slot claim and source epoch while holding the WAL lock.
  ;; Later checks reject a replacement slot, claimant, timeline or storage instance.
  (slot nil :read-only t) (name nil :read-only t) (claim nil :read-only t)
  (backend nil :read-only t) (system-id nil :read-only t) (timeline nil :read-only t)
  (namespace nil :read-only t) (incarnation nil :read-only t)
  (database nil :read-only t) (plugin nil :read-only t))

(defun logical-shutdown-validate-owner-under-lock (owner &key allow-departed)
  "Validate OWNER under the WAL lock and return its slot and active status.
ALLOW-DEPARTED accepts a fully released claim for cleanup checks."
  (wal-check-healthy)
  (let ((slot (logical-shutdown-owner-slot owner)))
    (unless (and *wal-ready* (not (recovery-in-progress-p))
                 (= (logical-shutdown-owner-system-id owner) (catalog-system-identifier))
                 (= (logical-shutdown-owner-timeline owner) *wal-insert-timeline*)
                 (eq (logical-shutdown-owner-namespace owner) *wal-segment-namespace*)
                 (eq (logical-shutdown-owner-incarnation owner) *wal-storage-incarnation*)
                 (eq slot (gethash (logical-shutdown-owner-name owner) *wal-replication-slots*))
                 (wal-logical-slot-p slot)
                 (string= (logical-shutdown-owner-name owner) (wal-replication-slot-name slot))
                 (string= (logical-shutdown-owner-plugin owner) (wal-replication-slot-plugin slot))
                 (= (logical-shutdown-owner-database owner) (wal-replication-slot-database-oid slot))
                 (eq :reserved (wal-replication-slot-wal-status slot)))
      (error "logical shutdown sender lost its slot or storage identity"))
    (let ((active (and (eq (logical-shutdown-owner-claim owner) (wal-replication-slot-active-owner slot))
                       (eq (logical-shutdown-owner-backend owner) (wal-replication-slot-active-owner-backend slot)))))
      (unless (or active
                  (and allow-departed
                       (null (wal-replication-slot-active-owner slot))
                       (null (wal-replication-slot-active-owner-backend slot))))
        (error "logical shutdown sender lost its active slot claim"))
      (values slot active))))

(defun wal-sender-stream-logical-ready (slot)
  "Capture the slot claim under WAL, then publish it under the sender state lock."
  (when *wal-sender-stream-token*
    (with-live-operation
      (let ((owner
              (sb-thread:with-mutex (*wal-lock*)
                (unless (and *wal-logical-slot-owner*
                             (eq *wal-logical-slot-owner* (wal-replication-slot-active-owner slot))
                             (eq *backend* (wal-replication-slot-active-owner-backend slot)))
                  (error "logical sender does not own its slot"))
                (let ((owner (make-logical-shutdown-owner
                              slot (copy-seq (wal-replication-slot-name slot))
                              *wal-logical-slot-owner* *backend*
                              (catalog-system-identifier) *wal-insert-timeline*
                              *wal-segment-namespace* *wal-storage-incarnation*
                              (wal-replication-slot-database-oid slot)
                              (copy-seq (wal-replication-slot-plugin slot)))))
                  (logical-shutdown-validate-owner-under-lock owner)
                  owner))))
        (call-with-wal-sender-state-lock
         (lambda ()
           (when *wal-sender-shutdown-plan*
             (ereport-fatal "57P01" "WAL sender is stopping"))
           (let ((token *wal-sender-stream-token*))
             (unless (and (wal-sender-stream-current-p token)
                          (eq :logical (wal-sender-stream-token-kind token))
                          (eq :starting (wal-sender-stream-token-state token))
                          (sb-thread:with-mutex (*wal-sender-lock*)
                            (wal-sender-current-under-lock-p (wal-sender-stream-token-registration token))))
               (error "logical sender does not own its stream"))
             (setf (wal-sender-stream-token-owner token) owner
                   (wal-sender-stream-token-state token) :ready)))))))
  slot)

(defun wal-sender-stream-logical-leaving (slot claim)
  "Mark the frozen stream as cleaning up before its exact slot claim is released.
A departure without an earlier acknowledgement is an abandoned drain."
  (when *wal-sender-stream-token*
    (call-with-wal-sender-state-lock
     (lambda ()
       (let* ((token *wal-sender-stream-token*)
              (owner (and (wal-sender-stream-token-p token)
                          (wal-sender-stream-token-owner token)))
              (plan *wal-sender-shutdown-plan*))
         (when (and plan (wal-sender-stream-current-p token)
                    (eq :logical (wal-sender-stream-token-kind token))
                    (logical-shutdown-owner-p owner)
                    (eq slot (logical-shutdown-owner-slot owner))
                    (eq claim (logical-shutdown-owner-claim owner))
                    (member token (wal-sender-shutdown-plan-tokens plan) :test #'eq)
                    (sb-thread:with-mutex (*wal-sender-lock*)
                      (wal-sender-current-under-lock-p (wal-sender-stream-token-registration token))))
           (setf (wal-sender-stream-token-state token) :cleanup
                 (wal-sender-stream-token-stopped-p token) t)
           (unless (eq :acknowledged (wal-sender-stream-token-outcome token))
             (setf (wal-sender-stream-token-outcome token) :abandoned)))))))
  (values))

(defun wal-sender-shutdown-owned-quiescence-p (plan)
  "Test shutdown ownership while holding the live-operation state lock."
  (and plan (eq :stopping (wal-sender-shutdown-plan-stage plan))
       (not *live-operation-admission-open*)
       (not *live-operation-terminal-shutdown*)
       (eq *live-operation-quiescence-owner-thread* sb-thread:*current-thread*)))

(defun wal-sender-capture-logical-shutdown-target ()
  "Flush and capture the logical drain target before the shutdown checkpoint.
The caller owns nonterminal quiescence. Snapshot stream state under the state
lock, validate each source and claim under WAL, then recheck before publishing."
  (let (plan tokens rows target)
    (call-with-wal-sender-state-lock
     (lambda ()
       (setf plan *wal-sender-shutdown-plan*)
       (unless (wal-sender-shutdown-owned-quiescence-p plan)
         (error "logical shutdown target requires owned nonterminal quiescence"))
       (setf tokens (copy-list (wal-sender-shutdown-plan-tokens plan))
             rows (loop for token in tokens
                        when (eq :logical (wal-sender-stream-token-kind token))
                          collect (list token (wal-sender-stream-token-owner token) nil)))
       (dolist (row rows)
         (let ((token (first row)))
           (unless (and (logical-shutdown-owner-p (second row))
                        (null (wal-sender-stream-token-limit token))
                        (or (wal-sender-stream-token-stopped-p token)
                            (member (wal-sender-stream-token-state token) '(:cleanup :released))))
             (error "logical shutdown target requires an uncaptured stopped stream"))))))
    (when rows
      (sb-thread:with-mutex (*wal-lock*)
        (wal-flush)
        (dolist (row rows)
          (setf (third row)
                (nth-value 1 (logical-shutdown-validate-owner-under-lock
                              (second row) :allow-departed t))))
        (setf target *wal-durable-lsn*))
      (call-with-wal-sender-state-lock
       (lambda ()
         (unless (and (eq plan *wal-sender-shutdown-plan*)
                      (wal-sender-shutdown-owned-quiescence-p plan)
                      (equal tokens (wal-sender-shutdown-plan-tokens plan))
                      (typep target '(unsigned-byte 64))
                      (every (lambda (row)
                               (let ((token (first row)))
                                 (and (eq (second row) (wal-sender-stream-token-owner token))
                                      (eq :logical (wal-sender-stream-token-kind token))
                                      (null (wal-sender-stream-token-limit token))
                                      (or (wal-sender-stream-token-stopped-p token)
                                          (member (wal-sender-stream-token-state token) '(:cleanup :released)))
                                      (or (third row)
                                          (member (wal-sender-stream-token-state token) '(:cleanup :released))))))
                             rows))
           (error "logical shutdown stream changed while capturing its target"))
         (dolist (row rows)
           (setf (wal-sender-stream-token-limit (first row)) target))))))
  (values))

(defun wal-sender-publish-shutdown-target (system-id timeline namespace endpoint)
  "Publish the checkpoint endpoint and open the frozen streams for draining.
Logical streams use their earlier flushed target. Physical streams use the
checkpoint endpoint, bounded by any historic fork limit. Revalidate identities
across the state and WAL lock acquisitions before opening the drain lane."
  (let (plan tokens rows)
    (call-with-wal-sender-state-lock
     (lambda ()
       (setf plan *wal-sender-shutdown-plan*)
       (unless (wal-sender-shutdown-owned-quiescence-p plan)
         (error "shutdown checkpoint target requires owned nonterminal quiescence"))
       (unless (and (typep system-id '(unsigned-byte 64))
                    (typep timeline '(integer 1 4294967295))
                    (typep endpoint '(unsigned-byte 64)))
         (error "invalid shutdown checkpoint identity or endpoint"))
       (setf tokens (copy-list (wal-sender-shutdown-plan-tokens plan))
             rows (loop for token in tokens
                        collect (list token (wal-sender-stream-token-owner token)
                                      (wal-sender-stream-token-limit token)
                                      (wal-sender-stream-token-kind token) nil)))
       (dolist (row rows)
         (destructuring-bind (token owner limit kind active) row
           (declare (ignore active))
           (unless (and (or (wal-sender-stream-token-stopped-p token)
                            (eq :released (wal-sender-stream-token-state token)))
                        (ecase kind
                          (:physical
                           (and (= system-id (physical-sender-system-id owner))
                                (= timeline (physical-sender-timeline owner))
                                (eq namespace (physical-sender-namespace owner))))
                          (:logical
                           (and (logical-shutdown-owner-p owner)
                                (= system-id (logical-shutdown-owner-system-id owner))
                                (= timeline (logical-shutdown-owner-timeline owner))
                                (eq namespace (logical-shutdown-owner-namespace owner))
                                (typep limit '(unsigned-byte 64))
                                (<= limit endpoint)))))
             (error "shutdown endpoint differs from its stopped streams"))))))
    (when (find :logical rows :key #'fourth)
      (sb-thread:with-mutex (*wal-lock*)
        (dolist (row rows)
          (when (eq :logical (fourth row))
            (setf (fifth row)
                  (nth-value 1 (logical-shutdown-validate-owner-under-lock
                                (second row) :allow-departed t)))))))
    (call-with-wal-sender-state-lock
     (lambda ()
       (unless (and (eq plan *wal-sender-shutdown-plan*)
                    (wal-sender-shutdown-owned-quiescence-p plan)
                    (equal tokens (wal-sender-shutdown-plan-tokens plan))
                    (every (lambda (row)
                             (destructuring-bind (token owner limit kind active) row
                               (and (eq owner (wal-sender-stream-token-owner token))
                                    (eql limit (wal-sender-stream-token-limit token))
                                    (eq kind (wal-sender-stream-token-kind token))
                                    (or (wal-sender-stream-token-stopped-p token)
                                        (eq :released (wal-sender-stream-token-state token)))
                                    (or (eq :physical kind) active
                                        (member (wal-sender-stream-token-state token) '(:cleanup :released))))))
                           rows))
         (error "shutdown streams changed while publishing their targets"))
       (setf (wal-sender-shutdown-plan-endpoint plan) endpoint
             (wal-sender-shutdown-plan-system-id plan) system-id
             (wal-sender-shutdown-plan-timeline plan) timeline
             (wal-sender-shutdown-plan-namespace plan) namespace)
       (dolist (row rows)
         (destructuring-bind (token owner limit kind active) row
           (declare (ignore owner active))
           (setf (wal-sender-stream-token-target token)
                 (ecase kind
                   (:logical limit)
                   (:physical (min endpoint (or limit endpoint)))))))
       (setf (wal-sender-shutdown-plan-stage plan) :draining
             *live-operation-shutdown-drain-open* t)
       (sb-thread:condition-broadcast *live-operation-cv*))))
  (values))

(defun wal-sender-acknowledge-logical-shutdown (slot scanned &key partial)
  "Acknowledge a complete logical drain for the exact active claim.
The scan, sent position and confirmed flush must all reach the target, and no
output frame may be partial. Slot release is checked separately during cleanup."
  (when (and *wal-sender-stream-token* (not partial))
    (with-live-operation
      (multiple-value-bind (target owner sent)
          (call-with-wal-sender-state-lock
           (lambda ()
             (let ((token *wal-sender-stream-token*)
                   (plan *wal-sender-shutdown-plan*))
               (when (and plan (eq :draining (wal-sender-shutdown-plan-stage plan))
                          (wal-sender-stream-current-p token)
                          (eq :logical (wal-sender-stream-token-kind token))
                          (eq :stopping (wal-sender-stream-token-state token))
                          (wal-sender-stream-token-stopped-p token)
                          (member token (wal-sender-shutdown-plan-tokens plan) :test #'eq))
                 (let ((owner (wal-sender-stream-token-owner token)))
                   (unless (and (logical-shutdown-owner-p owner)
                                (eq slot (logical-shutdown-owner-slot owner)))
                     (error "logical shutdown acknowledgement has a different slot"))
                   (sb-thread:with-mutex (*wal-sender-lock*)
                     (unless (wal-sender-current-under-lock-p *wal-sender*)
                       (error "logical shutdown acknowledgement lost its sender registration"))
                     (values (wal-sender-stream-token-target token) owner
                             (wal-sender-status-sent-lsn *wal-sender*))))))))
        (when (and target sent
                   (sb-thread:with-mutex (*wal-lock*)
                     (logical-shutdown-validate-owner-under-lock owner)
                     (wal-sender-logical-shutdown-complete-p
                      target scanned sent (wal-replication-slot-confirmed-flush-lsn slot))))
          (wal-sender-note-shutdown-acknowledged)
          t)))))

(defun wal-sender-check-logical-shutdown-completion (slot)
  "Recheck an acknowledged stream after its slot claim has been released.
The source epoch must still match and confirmed flush must cover the target."
  (with-live-operation
    (let* ((token *wal-sender-stream-token*)
           (plan nil)
           (owner nil)
           (target nil))
      (call-with-wal-sender-state-lock
       (lambda ()
         (setf plan *wal-sender-shutdown-plan*)
         (unless (and plan (eq :draining (wal-sender-shutdown-plan-stage plan))
                      (wal-sender-stream-current-p token)
                      (eq :logical (wal-sender-stream-token-kind token))
                      (eq :cleanup (wal-sender-stream-token-state token))
                      (eq :acknowledged (wal-sender-stream-token-outcome token))
                      (wal-sender-stream-token-stopped-p token)
                      (member token (wal-sender-shutdown-plan-tokens plan) :test #'eq)
                      (sb-thread:with-mutex (*wal-sender-lock*)
                        (wal-sender-current-under-lock-p *wal-sender*)))
           (error "logical shutdown completion has no acknowledged cleanup stream"))
         (setf owner (wal-sender-stream-token-owner token)
               target (wal-sender-stream-token-target token))
         (unless (and (logical-shutdown-owner-p owner)
                      (eq slot (logical-shutdown-owner-slot owner))
                      (typep target '(unsigned-byte 64)))
           (error "logical shutdown completion has a different slot or target"))))
      (sb-thread:with-mutex (*wal-lock*)
        (multiple-value-bind (actual active)
            (logical-shutdown-validate-owner-under-lock owner :allow-departed t)
          (unless (and (eq slot actual) (not active)
                       (>= (wal-replication-slot-confirmed-flush-lsn slot) target))
            (error "logical shutdown completion retained an active or unconfirmed slot"))))
      (call-with-wal-sender-state-lock
       (lambda ()
         (unless (and (eq plan *wal-sender-shutdown-plan*)
                      (eq :draining (wal-sender-shutdown-plan-stage plan))
                      (wal-sender-stream-current-p token)
                      (eq owner (wal-sender-stream-token-owner token))
                      (eql target (wal-sender-stream-token-target token))
                      (eq :cleanup (wal-sender-stream-token-state token))
                      (eq :acknowledged (wal-sender-stream-token-outcome token))
                      (sb-thread:with-mutex (*wal-sender-lock*)
                        (wal-sender-current-under-lock-p *wal-sender*)))
           (error "logical shutdown completion changed while checking its released slot"))))
      t)))

(defun logical-stream-poll (state)
  "Observe stopping and check slot ownership while logical output is in flight."
  (wal-sender-shutdown-observe)
  (with-live-operation
    (standard-stream-check state :check-timeout nil)))

(defun logical-stream-read-status (stream state)
  "Read feedback with shutdown polling, retaining an incomplete input frame."
  (unless state
    (return-from logical-stream-read-status (replication-standby-status stream)))
  (let ((status nil))
    (loop
      (logical-stream-poll state)
      (when (with-live-operation
              (standard-read-feedback-frame
               state :allow-copy-done nil
               :accept-feedback
               (lambda (reader payload)
                 (declare (ignore reader))
                 (unless (= (aref payload 0) (char-code #\r))
                   (ereport-fatal "08P01" "expected a standby status update in CopyBoth mode"))
                 (setf status
                       (list :write (replication-read-u64 payload 1)
                             :flush (replication-read-u64 payload 9)
                             :apply (replication-read-u64 payload 17)
                             :reply-time (replication-read-i64 payload 25))))))
        (return status))
      (when (eq :disconnect (standard-stream-stop-reason state))
        (return nil))
      (sb-sys:wait-until-fd-usable
       (sb-bsd-sockets:socket-file-descriptor (standard-stream-socket state))
       :input 0.05 nil))))

(defun call-with-logical-stream-io (stream slot function)
  "Use interruptible framed socket output for the lifetime of a logical stream."
  (unless (and *backend* (backend-socket *backend*))
    (return-from call-with-logical-stream-io (funcall function nil)))
  (let* ((state (make-standard-stream
                 :stream stream :socket (backend-socket *backend*)
                 :slot-name (wal-replication-slot-name slot) :slot-object slot))
         (*logical-copy-frame-writer*
           (lambda (output payload &key payload-size producer)
             (unless (eq stream output)
               (error "logical stream writer received another connection"))
             (handler-case
                 (unless (if producer
                             (standard-write-streamed-frame
                              state #\d payload-size producer :poll #'logical-stream-poll)
                             (standard-write-frame state #\d payload :poll #'logical-stream-poll))
                   (error 'logical-stream-disconnect))
               (sb-bsd-sockets:socket-error () (error 'logical-stream-disconnect))
               (stream-error () (error 'logical-stream-disconnect))))))
    (finish-output stream)
    (funcall function state)))

(defun logical-stream-acknowledge-shutdown (state scanned)
  "Record shutdown as the stop reason once the logical drain is acknowledged."
  (when (and state
             (wal-sender-acknowledge-logical-shutdown
              (standard-stream-slot-object state) scanned
              :partial (standard-stream-raw-partial-p state)))
    (setf (standard-stream-stop-reason state) :shutdown)
    t))
