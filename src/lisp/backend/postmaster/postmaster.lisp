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

(defun register-backend (backend)
  ;; Serialize with the reload snapshot. A provisional backend registered after
  ;; admission closes may finish teardown during drain, but must not gain SQL
  ;; transaction-drain eligibility merely by arriving late.
  (sb-thread:with-mutex (*live-operation-lock*)
    (unless *live-operation-admission-open*
      (pushnew backend *live-operation-cleanup-backends* :test #'eq))
    (sb-thread:with-mutex (*backend-registry-lock*)
      (sb-sys:without-interrupts
        (setf (gethash (backend-pid backend) *backend-registry*) backend)
        (incf *n-backends*))))
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
    (unless (and (active-xids-empty-p) (active-snapshots-empty-p))
      (error "shutdown still has transaction or snapshot registrations"))
    (sb-thread:with-mutex (*backend-registry-lock*)
      (setf *shutdown-abandoned-backends* nil)))
  (values))

(defun unregister-backend (backend)
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
  "Deliver the equivalent of SIGTERM: abort and terminate BACKEND.

Signalling ourselves raises the FATAL in this call stack; another backend is
interrupted, which unwinds it to the connection loop no matter what it was
doing (including a blocking read while idle)."
  (cond
    ((null backend) nil)
    ((eq backend *backend*)
     (ereport-fatal "57P01" "terminating connection due to administrator command"))
    (t
     (let ((thread (backend-thread backend)))
       (when (and thread (sb-thread:thread-alive-p thread))
         (handler-case
             (sb-thread:interrupt-thread
              thread
              (lambda ()
                (error (make-condition
                        'pg-error
                        :sqlstate "57P01"
                        :severity "FATAL"
                        :message "terminating connection due to administrator command"))))
           (error () nil))))))
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

(defun start-postmaster-recovery-workers ()
  "Start recovery services, tolerating only a concurrent process shutdown."
  (handler-case
      (progn
        (physical-follow-autostart)
        (physical-restartpoint-start-worker)
        (physical-promotion-start-worker))
    (physical-recovery-worker-shutdown (condition)
      ;; Shutdown can be processed between either guard and either start.
      ;; A prepared recovery image without process shutdown is still an error.
      (unless (shutdown-in-progress-p)
        (error condition))))
  (values))

(declaim (ftype function prepare-primary-shutdown call-with-live-operation))

(defun prepare-postmaster-shutdown ()
  "Finalize storage from MAIN after SERVE returns, outside live admission."
  (when (plusp *live-operation-depth*)
    (error "postmaster shutdown finalization requires an unadmitted main thread"))
  (unless *shutdown-finalizing-p*
    (error "postmaster shutdown finalization was not requested"))
  (unless (sb-thread:with-mutex (*shutdown-lock*) *shutdown-finished*)
    ;; Like C ShutdownXLOG, recovery shutdown publishes an applied restartpoint.
    ;; The signal worker remains free to process SIGQUIT while this waits.
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

(defun admin-reload-conf ()
  "Reload server configuration when supported; currently a successful no-op."
  ;; The Lisp server does not load PostgreSQL configuration files yet. Keep the
  ;; SQL function's success result, but do not use SIGHUP here: in this server
  ;; SIGHUP requests a code reload.
  t)

(defun admin-reload-code ()
  "pg_reload_code(): send SIGHUP to request an in-place Lisp code reload."
  (handler-case
      (progn
        (sb-posix:kill (sb-posix:getpid) sb-unix:sighup)
        t)
    (error (e)
      (warning-notice "failed to send signal to postmaster: ~a" e)
      nil)))
