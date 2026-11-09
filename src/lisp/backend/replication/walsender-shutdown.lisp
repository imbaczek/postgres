;; Physical WAL sender shutdown coordination.
;;
;; Like WalSndInitStopping and ShutdownXLOG in the C server, freeze senders
;; before the shutdown checkpoint, then let them deliver the durable endpoint.
;; This native path does not yet implement C's logical sender drain.
;;
;; The live-operation mutex protects the plan and stream state.  The state-lock
;; helper masks interrupts across updates; the admission predicate runs under
;; that mutex and takes the sender registry lock afterward.  A stream token is
;; distinct from the status registration, which can be reused by later streams.
;; The frozen participant list stays intact through cleanup: its cleanup
;; capability can outlive the stream scope.  Even an empty plan blocks late
;; streams from joining the shutdown.
(in-package #:pg)

(defstruct wal-sender-stream-token
  ;; Bound to one backend, thread and registration.  The owner and fork limit
  ;; become fixed when a physical stream is ready.
  backend thread registration kind owner limit
  (state :starting) stopped-p target outcome)

(defstruct wal-sender-shutdown-plan
  ;; Frozen participants and the durable endpoint published after checkpoint.
  tokens (stage :stopping) endpoint system-id timeline namespace)

(defvar *wal-sender-stream-token* nil
  "Dynamically bound to the current stream, not the whole connection.")
(defvar *wal-sender-streams* (make-hash-table :test #'eq)
  "Map backends to current streams under the live-operation lock.")
(defvar *wal-sender-shutdown-plan* nil
  "Plan frozen once stopping starts, under the live-operation lock.")

(defun call-with-wal-sender-state-lock (function)
  ;; Keep multi-field plan and token updates atomic against interrupts.
  (call-with-live-operation-lock
   (lambda ()
     (sb-sys:without-interrupts (funcall function)))))

(defun wal-sender-stream-current-p (token)
  ;; Check the stream, backend, thread and reused status registration together.
  (and (wal-sender-stream-token-p token)
       (eq token (gethash (wal-sender-stream-token-backend token) *wal-sender-streams*))
       (eq *backend* (wal-sender-stream-token-backend token))
       (eq sb-thread:*current-thread* (wal-sender-stream-token-thread token))
       (eq *wal-sender* (wal-sender-stream-token-registration token))))

(defun wal-sender-stream-drain-admitted-p ()
  ;; Admit only the exact frozen stream and registration, never a parked caller.
  (let ((token *wal-sender-stream-token*) (plan *wal-sender-shutdown-plan*))
    (and (not *live-operation-parked-p*)
         plan (eq :draining (wal-sender-shutdown-plan-stage plan))
         (wal-sender-stream-current-p token)
         (member token (wal-sender-shutdown-plan-tokens plan) :test #'eq)
         (member (wal-sender-stream-token-state token) '(:stopping :complete :cleanup))
         (sb-thread:with-mutex (*wal-sender-lock*)
           (wal-sender-current-under-lock-p (wal-sender-stream-token-registration token))))))

(defun call-with-wal-sender-stream-scope (function kind)
  ;; Own the stream capability until unwind.  A disconnect without validated
  ;; completion is abandonment, not an acknowledgement.
  (unless (member kind '(:physical :logical :backup))
    (error "invalid WAL sender stream kind"))
  (when (plusp *live-operation-depth*)
    (error "WAL sender transfer requires an unadmitted caller"))
  (unless *wal-sender* (return-from call-with-wal-sender-stream-scope (funcall function)))
  (let* ((token (make-wal-sender-stream-token
                 :backend *backend* :thread sb-thread:*current-thread*
                 :registration *wal-sender* :kind kind))
         (*wal-sender-stream-token* token)
         (*live-operation-shutdown-drain-check* #'wal-sender-stream-drain-admitted-p))
    (sb-sys:without-interrupts
      (unwind-protect
           (progn
             (call-with-wal-sender-state-lock
              (lambda ()
                (when *wal-sender-shutdown-plan*
                  (ereport-fatal "57P01" "WAL sender is stopping"))
                (when (gethash *backend* *wal-sender-streams*)
                  (error "backend already owns a WAL sender stream"))
                (setf (gethash *backend* *wal-sender-streams*) token)))
             (sb-sys:with-local-interrupts (funcall function)))
        (call-with-wal-sender-state-lock
         (lambda ()
           (setf (wal-sender-stream-token-state token) :released)
           (unless (wal-sender-stream-token-outcome token)
             (setf (wal-sender-stream-token-outcome token) :abandoned))
           (when (eq token (gethash *backend* *wal-sender-streams*))
             (remhash *backend* *wal-sender-streams*))
           (sb-thread:condition-broadcast *live-operation-cv*)))))))

(defun wal-sender-logical-shutdown-complete-p (target scanned sent confirmed)
  (and target (>= scanned target) (>= sent target) (>= confirmed target)))

(defun wal-sender-stream-physical-ready (owner limit)
  ;; Publish the claimed physical owner and optional historic fork limit before
  ;; the shutdown plan can select this stream.
  (when *wal-sender-stream-token*
    (call-with-wal-sender-state-lock
     (lambda ()
       (let ((token *wal-sender-stream-token*))
         (when *wal-sender-shutdown-plan*
           (ereport-fatal "57P01" "WAL sender is stopping"))
         (unless (and (wal-sender-stream-current-p token)
                      (eq :physical (wal-sender-stream-token-kind token))
                      (eq :starting (wal-sender-stream-token-state token))
                      (eq *backend* (physical-sender-backend owner)))
           (error "physical sender does not own its stream"))
         (setf (wal-sender-stream-token-owner token) owner
               (wal-sender-stream-token-limit token) limit
               (wal-sender-stream-token-state token) :ready)))))
  owner)

(defun wal-sender-request-stopping ()
  ;; Freeze the ready physical participants once; later streams cannot join.
  (call-with-wal-sender-state-lock
   (lambda ()
     (or *wal-sender-shutdown-plan*
         (let ((tokens
                 (loop for token being the hash-values of *wal-sender-streams*
                       when (and (member (wal-sender-stream-token-kind token) '(:physical :logical))
                                 (eq :ready (wal-sender-stream-token-state token))
                                 (wal-sender-stream-token-owner token))
                         collect token)))
           (setf *wal-sender-shutdown-plan* (make-wal-sender-shutdown-plan :tokens tokens))
           (dolist (token tokens) (setf (wal-sender-stream-token-state token) :stopping))
           *wal-sender-shutdown-plan*)))))

(defun wal-sender-shutdown-token-for-backend (backend)
  (let ((plan *wal-sender-shutdown-plan*))
    (when plan
      (find backend (wal-sender-shutdown-plan-tokens plan)
            :key #'wal-sender-stream-token-backend :test #'eq))))

(defun wal-sender-shutdown-observe ()
  ;; The sender reaches its stop point before the final checkpoint begins.
  (when *wal-sender-stream-token*
    (call-with-wal-sender-state-lock
     (lambda ()
       (let ((token *wal-sender-stream-token*))
         (when (and (wal-sender-stream-current-p token)
                    (eq token (wal-sender-shutdown-token-for-backend *backend*)))
           (setf (wal-sender-stream-token-stopped-p token) t)
           (sb-thread:condition-broadcast *live-operation-cv*))))))
  (values))

(declaim (ftype (function (t t t t) (values)) wal-sender-publish-shutdown-target))

(defun wal-sender-shutdown-target ()
  ;; Only the current stream can obtain its frozen post-checkpoint target.
  (call-with-wal-sender-state-lock
   (lambda ()
     (when (and *wal-sender-shutdown-plan*
                (eq :draining (wal-sender-shutdown-plan-stage *wal-sender-shutdown-plan*))
                (wal-sender-stream-current-p *wal-sender-stream-token*))
       (wal-sender-stream-token-target *wal-sender-stream-token*)))))

(defun wal-sender-note-shutdown-acknowledged ()
  ;; The caller has checked sent and validated durable flush through the target.
  (call-with-wal-sender-state-lock
   (lambda ()
     (unless (and (wal-sender-stream-current-p *wal-sender-stream-token*)
                  *wal-sender-shutdown-plan*
                  (eq :draining (wal-sender-shutdown-plan-stage *wal-sender-shutdown-plan*))
                  (member *wal-sender-stream-token*
                          (wal-sender-shutdown-plan-tokens *wal-sender-shutdown-plan*) :test #'eq)
                  (wal-sender-stream-token-stopped-p *wal-sender-stream-token*)
                  (wal-sender-stream-token-target *wal-sender-stream-token*)
                  (eq :stopping (wal-sender-stream-token-state *wal-sender-stream-token*)))
       (error "shutdown acknowledgement has no draining stopped sender"))
     (setf (wal-sender-stream-token-state *wal-sender-stream-token*) :complete
           (wal-sender-stream-token-outcome *wal-sender-stream-token*) :acknowledged)))
  (values))

(defun wal-sender-shutdown-requested-p ()
  (call-with-wal-sender-state-lock (lambda () (not (null *wal-sender-shutdown-plan*)))))

(defun wal-sender-shutdown-streams-p ()
  ;; An empty frozen plan still rejects new streams, but needs no drain lane.
  (call-with-wal-sender-state-lock
   (lambda ()
     (and *wal-sender-shutdown-plan*
          (not (null (wal-sender-shutdown-plan-tokens *wal-sender-shutdown-plan*)))))))

(defun wal-sender-backend-stopping-p (backend)
  (call-with-wal-sender-state-lock
   (lambda () (and *wal-sender-shutdown-plan* backend (backend-replication-mode backend)))))

(defun wal-sender-shutdown-ordinary-clients-gone-p ()
  (sb-thread:with-mutex (*backend-registry-lock*)
    (and (= *n-connections* *n-backends* (hash-table-count *backend-registry*))
         (loop for backend being the hash-values of *backend-registry*
               always (eq :walsender (gethash backend *backend-admission-classes*))))))

(defun wal-sender-shutdown-clients-drained-p ()
  ;; Before checkpoint, require only stopped participants with no transaction.
  (call-with-wal-sender-state-lock
   (lambda ()
     (sb-thread:with-mutex (*backend-registry-lock*)
       (and (= *n-connections* *n-backends* (hash-table-count *backend-registry*))
            (loop for backend being the hash-values of *backend-registry*
                  for token = (wal-sender-shutdown-token-for-backend backend)
                  always (and token
                              (eq (backend-thread backend) (wal-sender-stream-token-thread token))
                              (or (wal-sender-stream-token-stopped-p token)
                                  (eq :released (wal-sender-stream-token-state token)))
                              (null (backend-xid backend))
                              (eq :idle (backend-state backend))
                              (not (backend-explicit backend)))))))))

(defun wal-sender-shutdown-terminate-nonparticipants ()
  ;; Logical, backup and idle senders are not selected for this physical drain.
  (let ((preserved
          (call-with-wal-sender-state-lock
           (lambda ()
             (mapcar #'wal-sender-stream-token-backend
                     (wal-sender-shutdown-plan-tokens *wal-sender-shutdown-plan*))))))
    (dolist (backend (list-backends))
      (unless (member backend preserved :test #'eq)
        (terminate-backend backend))))
  (values))

(defun wal-sender-shutdown-cleanup-check (backend)
  ;; The frozen sender's teardown may use this capability after stream scope
  ;; exits.  A parked operation cannot use it to bypass the admission fence.
  (call-with-wal-sender-state-lock
   (lambda ()
     (let ((plan *wal-sender-shutdown-plan*)
           (token (wal-sender-shutdown-token-for-backend backend)))
       (when (and token (eq sb-thread:*current-thread* (wal-sender-stream-token-thread token)))
         (lambda ()
           (and (eq plan *wal-sender-shutdown-plan*)
                (eq :draining (wal-sender-shutdown-plan-stage plan))
                (not *live-operation-parked-p*)
                (eq backend *backend*)
                (eq sb-thread:*current-thread* (wal-sender-stream-token-thread token)))))))))

(defun wal-sender-wait-shutdown-exit ()
  ;; Wait outside admission for all connections and their cleanup, then close
  ;; the special lane before the terminal fence.
  (when (plusp *live-operation-depth*) (error "sender shutdown wait must be unadmitted"))
  (loop until (sb-thread:with-mutex (*backend-registry-lock*)
                (and (zerop *n-connections*) (zerop *n-backends*)
                     (zerop (hash-table-count *backend-registry*))))
        do (sleep 0.01))
  (call-with-wal-sender-state-lock
   (lambda ()
     (setf *live-operation-shutdown-drain-open* nil)
     (when *wal-sender-shutdown-plan*
       (setf (wal-sender-shutdown-plan-stage *wal-sender-shutdown-plan*) :finished))
     (sb-thread:condition-broadcast *live-operation-cv*)))
  (values))

(defun wal-sender-check-shutdown-released ()
  ;; Clean primary state requires every stream, registration and slot claim gone.
  (call-with-wal-sender-state-lock
   (lambda ()
     (unless (zerop (hash-table-count *wal-sender-streams*))
       (error "primary shutdown retained a stream capability"))))
  (sb-thread:with-mutex (*wal-sender-lock*)
    (unless (zerop (hash-table-count *wal-sender-registry*))
      (error "primary shutdown retained a WAL sender registration")))
  (sb-thread:with-mutex (*wal-lock*)
    (unless (zerop (hash-table-count *physical-source-senders*))
      (error "primary shutdown retained a physical sender claim"))
    (loop for slot being the hash-values of *wal-replication-slots*
          when (wal-replication-slot-active-owner slot)
            do (error "primary shutdown retained a replication slot owner")))
  (values))
