;;;; postmaster.lisp -- server shutdown and cross-backend signaling.
;;;;
;;;; PostgreSQL splits this between postmaster.c (SIGTERM/SIGINT/SIGQUIT select
;;;; smart/fast/immediate shutdown) and signalfuncs.c (pg_cancel_backend,
;;;; pg_terminate_backend, pg_reload_conf).  This server is one process with a
;;;; thread per connection, so the "children" are the backends in
;;;; *BACKEND-REGISTRY*: SIGTERM/SIGINT/SIGQUIT to the process map to
;;;; smart/fast/immediate shutdown exactly as they do for the postmaster.
;;;;
;;;; Signal handlers only record the requested mode; a dedicated worker thread
;;;; (the analogue of the postmaster's main loop, which acts on signals only
;;;; once it returns to its event loop) does the real work.  SBCL runs an
;;;; ENABLE-INTERRUPT handler in whichever thread the OS delivered the signal
;;;; to -- possibly a backend that already holds *SHUTDOWN-LOCK* or
;;;; *WRITE-LOCK*, or even a backend about to be terminated -- so the handler
;;;; must not take locks, interrupt threads, or exit.

(in-package #:pg)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (require :sb-posix))

;;; ---- shutdown modes (postmaster.c: Shutdown) ----

(defconstant +no-shutdown+ 0)
(defconstant +smart-shutdown+ 1)
(defconstant +fast-shutdown+ 2)
(defconstant +immediate-shutdown+ 3)

(defvar *shutdown* +no-shutdown+)
(defvar *shutdown-lock* (sb-thread:make-mutex :name "pg shutdown lock"))
(defvar *shutdown-finished* nil)

(defvar *shutdown-finalizing-p* nil
  "The accept loop is stopping; the main thread must finalize storage before exit.")

;; Requested vs processed level.  Handlers only raise *PM-REQUESTED-MODE*, a
;; plain fixnum store with no lock; the worker observes it.  The mode is
;; monotonic (upward only), so there is nothing to reset and no lost-wakeup
;; window.
(defvar *pm-requested-mode* +no-shutdown+)
(defvar *pm-processed-mode* +no-shutdown+)
(defvar *postmaster-thread* nil)

(defun shutdown-mode ()
  "The current shutdown level, read under the lock."
  (sb-thread:with-mutex (*shutdown-lock*) *shutdown*))

(defun shutdown-in-progress-p ()
  (> (shutdown-mode) +no-shutdown+))

(defun shutdown-terminates-backends-p ()
  (>= (shutdown-mode) +fast-shutdown+))

(defun conns-allowed-p ()
  "CanAcceptConnections(): no new clients once shutdown has begun."
  (not (shutdown-in-progress-p)))

(defun admin-log (control-string &rest args)
  (format *error-output* "~&pg-lisp: ~a~%"
          (apply #'format nil control-string args))
  (finish-output *error-output*))

;;; ---- backend registry ----
;;;
;;; *N-CONNECTIONS* counts every accepted connection, including one still in
;;; the startup handshake; the postmaster must not exit while one is alive, or
;;; a client connecting during shutdown would lose the race and get a reset
;;; instead of FATAL 57P03.  Backends enter the registry before reading the
;;; startup packet so fast shutdown can interrupt a stalled handshake too.

(defvar *shutdown-abandoned-backends* nil
  "Backend objects whose storage cleanup was deferred during fast shutdown.
The registry lock protects this queue. MAIN retires it after all clients leave.")

(defvar *backend-registry* (make-hash-table :test #'eql))
(defvar *backend-registry-lock*
  (sb-thread:make-mutex :name "pg backend registry lock"))
(defvar *accept-lock* (sb-thread:make-mutex :name "pg accept lock"))
(defvar *accept-stop-requested* nil)
(defvar *n-backends* 0)
(defvar *n-connections* 0)

(defvar *max-connections* 100
  "Installed SQL session quota, selected at startup.")

(defvar *max-wal-senders* 10
  "Installed quota shared by physical and database WAL-sender sessions.")

(defvar *backend-admission-classes* (make-hash-table :test #'eq)
  "Backend-generation quota reservations, protected by the registry lock.
DEFVAR preserves reservations across function reloads.")

(declaim (special *live-operation-lock* *live-operation-cv*
                  *live-operation-backend-query-tokens*
                  *live-operation-admission-open*
                  *live-operation-cleanup-backends* *live-operation-depth*))

(defun list-backends ()
  (sb-thread:with-mutex (*backend-registry-lock*)
    (loop for backend being the hash-values of *backend-registry*
          collect backend)))

(defun lookup-backend (pid)
  (sb-thread:with-mutex (*backend-registry-lock*)
    (gethash pid *backend-registry*)))

(defun postmaster-admit-startup-backend (backend)
  "Reserve the SQL or WAL-sender quota for this registered backend generation.
Check exact PID ownership before accepting a repeated reservation; the class
cannot change. Exhaustion raises FATAL before authentication succeeds."
  (let* ((mode (backend-replication-mode backend))
         (kind (if mode :walsender :sql))
         (limit (if mode *max-wal-senders* *max-connections*)))
    (unless (member mode '(nil :physical :database))
      (error "invalid startup replication class"))
    (sb-thread:with-mutex (*backend-registry-lock*)
      (unless (eq backend (gethash (backend-pid backend) *backend-registry*))
        (error "startup backend is no longer registered"))
      (let ((old (gethash backend *backend-admission-classes*)))
        (when old
          (unless (eq old kind) (error "startup backend changed its admission class"))
          (return-from postmaster-admit-startup-backend backend)))
      (when (>= (loop for class being the hash-values of *backend-admission-classes*
                      count (eq class kind))
                limit)
        (if mode
            (ereport-fatal "53300"
                           "number of requested standby connections exceeds \"max_wal_senders\" (currently ~d)"
                           limit)
            (ereport-fatal "53300" "sorry, too many clients already")))
      (sb-sys:without-interrupts
        (setf (gethash backend *backend-admission-classes*) kind))))
  backend)

(defun register-backend (backend)
  "Register a unique PID and enroll late arrivals for closed-gate cleanup."
  ;; Serialize with the reload snapshot. A provisional backend registered after
  ;; admission closes may finish teardown during drain, but must not gain SQL
  ;; transaction-drain eligibility merely by arriving late.
  (sb-thread:with-mutex (*live-operation-lock*)
    (sb-thread:with-mutex (*backend-registry-lock*)
      (when (gethash (backend-pid backend) *backend-registry*)
        (error "backend PID is already registered"))
      (sb-sys:without-interrupts
        (setf (gethash (backend-pid backend) *backend-registry*) backend)
        (incf *n-backends*)
        (unless *live-operation-admission-open*
          (pushnew backend *live-operation-cleanup-backends* :test #'eq)))))
  ;; Close the race where fast shutdown snapshots the registry just before
  ;; this backend is added.  The caller is the backend thread itself.
  (when (and (eq backend *backend*)
             (>= (shutdown-mode) +fast-shutdown+))
    (ereport-fatal "57P01"
                   "terminating connection due to administrator command"))
  (values))

(defun postmaster-remember-abandoned-backend (backend)
  "Retain BACKEND for MAIN without waiting for storage admission during teardown."
  (sb-thread:with-mutex (*backend-registry-lock*)
    (pushnew backend *shutdown-abandoned-backends* :test #'eq)))

(defun postmaster-retire-abandoned-backends ()
  "Release deferred transactions and snapshots after the last client exits.
The caller must exclude new storage work. ABORT-TRANSACTION preserves a commit
already recorded in CLOG while releasing the backend's remaining registrations."
  (let ((backends (sb-thread:with-mutex (*backend-registry-lock*)
                    (unless (and (zerop *n-connections*) (zerop *n-backends*))
                      (error "shutdown still has live client connections"))
                    (copy-list *shutdown-abandoned-backends*))))
    (dolist (backend backends)
      (let ((*backend* backend)) (abort-transaction)))
    (unless (and (xid-wrap-users-empty-p) (active-snapshots-empty-p))
      (error "shutdown still has transaction or snapshot registrations"))
    (sb-thread:with-mutex (*backend-registry-lock*)
      (setf *shutdown-abandoned-backends* nil)))
  (values))

(defun unregister-backend (backend)
  "Remove BACKEND's reservation even if its PID now names another generation.
Registry removal is conditional on exact ownership; repeated cleanup is safe."
  ;; The query finalizer can itself be interrupted while it waits for reload
  ;; admission. Remove its token here as a final guarantee that teardown cannot
  ;; retain the backend. Acquire the live-operation lock before the registry
  ;; lock, matching cancel-target lookup, but release it before taking the latter.
  (sb-sys:without-interrupts
    (sb-thread:grab-mutex *live-operation-lock*)
    (unwind-protect
         (progn
           (setf (backend-executing backend) nil
                 (backend-cancel-pending backend) nil)
           (remhash backend *live-operation-backend-query-tokens*)
           (sb-thread:condition-broadcast *live-operation-cv*))
      (sb-thread:release-mutex *live-operation-lock*)))
  (sb-thread:with-mutex (*backend-registry-lock*)
    (sb-sys:without-interrupts
      (remhash backend *backend-admission-classes*)
      (when (eq (gethash (backend-pid backend) *backend-registry*) backend)
        (remhash (backend-pid backend) *backend-registry*)
        (decf *n-backends*))))
  (values))

(defun connection-started ()
  (sb-thread:with-mutex (*backend-registry-lock*)
    (sb-sys:without-interrupts
      (incf *n-connections*)))
  (values))

(defun connection-finished ()
  "A connection (registered or not) has gone.  Stop the postmaster if this
was the last one and a smart/fast shutdown is in progress."
  (let ((last nil))
    ;; Once teardown starts accounting, an asynchronous backend termination must
    ;; not interrupt the transition after the socket is unregistered but before
    ;; *N-CONNECTIONS* is decremented.
    (sb-sys:without-interrupts
      (sb-thread:with-mutex (*backend-registry-lock*)
        (decf *n-connections*)
        (let ((mode (sb-thread:with-mutex (*shutdown-lock*) *shutdown*)))
          (setf last (and (<= +smart-shutdown+ mode +fast-shutdown+)
                          (zerop *n-connections*))))))
    (when last
      (finish-shutdown)))
  (values))

;;; ---- pg_cancel_backend / pg_terminate_backend ----

(defun terminate-backend (backend)
  "Request termination of BACKEND, including while it is idle.
Publish the pending flag under the admission mutex and wake admission waiters.
The requester remains interrupt-masked through unlock and queuing the target
interrupt. The target consumes the flag at FATAL delivery; a queued helper does
nothing if a cooperative check has already delivered it."
  (when backend
    (let ((self-p (eq backend *backend*))
          (thread (backend-thread backend)))
      (when (or self-p (and thread (sb-thread:thread-alive-p thread)))
        (sb-sys:without-interrupts
          (sb-thread:with-mutex (*live-operation-lock*)
            (setf (backend-termination-pending backend) t)
            (sb-thread:condition-broadcast *live-operation-cv*))
          (if self-p
              (deliver-backend-termination backend)
              (handler-case
                  (sb-thread:interrupt-thread
                   thread (lambda () (deliver-backend-termination backend)))
                (error () nil)))))))
  (values))

(defun cancel-backend (backend)
  "Deliver the equivalent of SIGINT: cancel BACKEND's running statement.

As in PostgreSQL, a cancel request that arrives while the backend is reading a
command (idle) is ignored; the connection and its transaction state survive."
  (cond
    ((null backend) nil)
    ((eq backend *backend*)
     (ereport "57014" "canceling statement due to user request"))
    (t
     ;; Query start/end and reload admission use this lock too. Wake a backend
     ;; waiting on the reload gate so it can observe the cancel promptly.
     (sb-thread:with-mutex (*live-operation-lock*)
       (when (backend-executing backend)
         (setf (backend-cancel-pending backend) t)
         (sb-thread:condition-broadcast *live-operation-cv*)))))
  (values))

(defun terminate-all-backends ()
  (dolist (backend (list-backends))
    (terminate-backend backend)))

;;; ---- shutdown state machine ----

(defun finish-shutdown ()
  "Request main-thread finalization after every client connection has gone."
  ;; A backend can still hold live-operation admission here. Never join recovery
  ;; workers, close admission, or exit from this cleanup stack.
  (sb-thread:with-mutex (*accept-lock*)
    (when (and (<= +smart-shutdown+ (shutdown-mode) +fast-shutdown+)
               (sb-thread:with-mutex (*backend-registry-lock*)
                 (zerop *n-connections*)))
      (sb-thread:with-mutex (*shutdown-lock*)
        (when (and (not *shutdown-finished*) (not *shutdown-finalizing-p*)
                   (<= +smart-shutdown+ *shutdown* +fast-shutdown+))
          (setf *shutdown-finalizing-p* t
                *accept-stop-requested* t)))))
  (values))

(declaim (ftype function physical-promotion-start-worker))

(declaim (ftype function physical-follow-desired-start-worker))

(defun start-postmaster-recovery-workers ()
  "Start admitted recovery services and cleanup, tolerating concurrent shutdown."
  (handler-case
      (progn
        (physical-follow-autostart)
        (physical-follow-controller-start-worker)
        (physical-follow-desired-start-worker)
        (physical-restartpoint-start-worker)
        (physical-promotion-start-worker))
    (physical-recovery-worker-shutdown (condition)
      ;; Shutdown can be processed between either guard and either start.
      ;; A prepared recovery image without process shutdown is still an error.
      (unless (shutdown-in-progress-p)
        (error condition))))
  (values))

(declaim (ftype function prepare-primary-shutdown call-with-live-operation))

(declaim (ftype function managed-worker-request-shutdown managed-worker-shutdown))

(defun prepare-postmaster-shutdown ()
  "Stop managed workers and controller work before finalizing storage from MAIN.
Wait outside live admission so workers can finish; a worker timeout prevents
storage finalization. The signal worker remains available for immediate shutdown."
  (when (plusp *live-operation-depth*)
    (error "postmaster shutdown finalization requires an unadmitted main thread"))
  (unless *shutdown-finalizing-p*
    (error "postmaster shutdown finalization was not requested"))
  (unless (sb-thread:with-mutex (*shutdown-lock*) *shutdown-finished*)
    ;; Like C ShutdownXLOG, recovery shutdown publishes an applied restartpoint.
    ;; The signal worker remains free to process SIGQUIT while this waits.
    (managed-worker-shutdown)
    (physical-follow-controller-stop-worker)
    (if (recovery-in-progress-p)
        (progn
          (call-with-live-operation #'postmaster-retire-abandoned-backends)
          (physical-recovery-prepare-shutdown))
        (prepare-primary-shutdown))
    (sb-thread:with-mutex (*shutdown-lock*)
      (setf *shutdown-finished* t)))
  (values))

(defun maybe-finish-shutdown ()
  (when (and (shutdown-in-progress-p)
             (sb-thread:with-mutex (*backend-registry-lock*)
               (zerop *n-connections*)))
    (finish-shutdown)))

(defun immediate-shutdown ()
  "Exit without waiting for the backends, like PostgreSQL's immediate shutdown.

Non-fatal immediate shutdown still lets the postmaster exit cleanly: status 0,
the socket removed, and \"database system is shut down\" logged (in PostgreSQL
that log comes from the UnlinkLockFiles proc_exit callback).  Only the children
are killed abruptly."
  (let ((go nil))
    (sb-thread:with-mutex (*shutdown-lock*)
      (unless *shutdown-finished*
        (setf *shutdown-finished* t
              go t)))
    (when go
      ;; quickdie() tells each child why.  Idle backends can be told safely;
      ;; a busy one may not be able to read the message before the socket dies.
      (dolist (backend (list-backends))
        (unless (backend-executing backend)
          (ignore-errors
           (send-notice (backend-stream backend)
                        "terminating connection due to immediate shutdown command"
                        "WARNING"))))
      (admin-log "database system is shut down")))
  (sb-ext:exit :code 0))

(defun act-on-shutdown (mode)
  (when (or (= mode +smart-shutdown+) (= mode +fast-shutdown+))
    (managed-worker-request-shutdown))
  (case mode
    (#.+smart-shutdown+
     (admin-log "received smart shutdown request")
     ;; New connections are now rejected; existing clients may finish.
     (maybe-finish-shutdown))
    (#.+fast-shutdown+
     (admin-log "received fast shutdown request")
     (admin-log "aborting any active transactions")
     (terminate-all-backends)
     (maybe-finish-shutdown))
    (#.+immediate-shutdown+
     (admin-log "received immediate shutdown request")
     (immediate-shutdown))))

(defun process-shutdown-request (mode)
  (let ((raised nil))
    (sb-thread:with-mutex (*shutdown-lock*)
      (when (> mode *shutdown*)
        (setf *shutdown* mode
              raised t)))
    (when raised
      (act-on-shutdown mode))))

;;; ---- signal handlers and the postmaster worker ----

(defun request-shutdown (mode)
  ;; Called from a signal handler: no locks, no allocation, no blocking.
  ;; Multiple handlers can run in different threads; CAS keeps escalation
  ;; monotonic even when their reads and writes interleave.
  (loop
    for current = *pm-requested-mode*
    do (when (>= current mode)
         (return))
       (when (= (sb-ext:compare-and-swap *pm-requested-mode* current mode)
                current)
         (return)))
  (values))

(defun request-smart-shutdown (&rest signal-info)
  (declare (ignore signal-info))
  (request-shutdown +smart-shutdown+))

(defun request-fast-shutdown (&rest signal-info)
  "SIGINT, i.e. Ctrl-C in a terminal, is PostgreSQL's fast shutdown."
  (declare (ignore signal-info))
  (request-shutdown +fast-shutdown+))

(defun request-immediate-shutdown (&rest signal-info)
  (declare (ignore signal-info))
  (request-shutdown +immediate-shutdown+))

(defun postmaster-worker-loop ()
  (loop
    (let ((target *pm-requested-mode*))
      (when (> target *pm-processed-mode*)
        (setf *pm-processed-mode* target)
        (handler-case (process-shutdown-request target)
          (error (e)
            (admin-log "shutdown processing failed: ~a" e)))))
    (sleep 0.05)))

(defun enable-shutdown-handlers ()
  (sb-sys:enable-interrupt sb-unix:sigterm #'request-smart-shutdown)
  (sb-sys:enable-interrupt sb-unix:sigint #'request-fast-shutdown)
  (sb-sys:enable-interrupt sb-unix:sigquit #'request-immediate-shutdown)
  (unless (and *postmaster-thread*
               (sb-thread:thread-alive-p *postmaster-thread*))
    (setf *postmaster-thread*
          (sb-thread:make-thread #'postmaster-worker-loop
                                 :name "pg postmaster")))
  (values))

;;; ---- SQL entry points ----
;;;
;;; Argument coercion to the declared parameter types happens in ANALYZE, so
;;; these see only integers (PID int4, timeout int8).

(defun wait-until-termination (pid timeout-ms)
  "PostgreSQL's pg_wait_until_termination(): poll every 100 ms."
  (loop with remaining = timeout-ms
        do (cond
             ((null (lookup-backend pid))
              (return t))
             ((<= remaining 0)
              (warning-notice
               "backend with PID ~a did not terminate within ~a millisecond~:p"
               pid timeout-ms)
              (return nil))
             (t
              (check-for-interrupts)
              (sleep (min 0.1 (/ remaining 1000.0)))
              (decf remaining 100)))))

(defun admin-cancel-backend (pid)
  (let ((backend (lookup-backend pid)))
    (cond
      (backend
       (cancel-backend backend)
       t)
      (t
       (warning-notice "PID ~a is not a PostgreSQL backend process" pid)
       nil))))

(defun admin-terminate-backend (pid timeout)
  (when (minusp timeout)
    (ereport "22003" "\"timeout\" must not be negative"))
  (let ((backend (lookup-backend pid)))
    (cond
      ((null backend)
       (warning-notice "PID ~a is not a PostgreSQL backend process" pid)
       nil)
      (t
       (terminate-backend backend)
       (if (plusp timeout)
           (wait-until-termination pid timeout)
           t)))))

(defun admin-request-reload ()
  "Send SIGHUP to the owning process and return whether the signal was accepted.
On error, issue a warning and return NIL. Reload runs asynchronously after the
caller can leave operation admission."
  (handler-case
      (progn
        (sb-posix:kill (sb-posix:getpid) sb-unix:sighup)
        t)
    (error (condition)
      (warning-notice "failed to send signal to postmaster: ~a" condition)
      nil)))

(defun admin-reload-conf ()
  "Request the existing SIGHUP reload of code and startup settings.
True means the signal was accepted, not that reload completed or settings are valid."
  (admin-request-reload))

(defun admin-reload-code ()
  "Request the existing SIGHUP reload of code and startup settings.
True means the signal was accepted, not that reload completed or settings are valid."
  (admin-request-reload))
