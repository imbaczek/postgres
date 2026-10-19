;;;; postmaster.lisp -- server shutdown and cross-backend signaling.
;;;;
;;;; PostgreSQL splits this between postmaster.c (SIGTERM/SIGINT/SIGQUIT select
;;;; smart/fast/immediate shutdown) and signalfuncs.c (pg_cancel_backend,
;;;; pg_terminate_backend, pg_reload_conf).  This server is one process with a
;;;; thread per connection, so the "children" are the backends in
;;;; *BACKEND-REGISTRY*: SIGTERM/SIGINT/SIGQUIT to the process map to
;;;; smart/fast/immediate shutdown exactly as they do for the postmaster.

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

(defun shutdown-in-progress-p ()
  (> *shutdown* +no-shutdown+))

(defun conns-allowed-p ()
  "CanAcceptConnections(): no new clients once shutdown has begun."
  (not (shutdown-in-progress-p)))

(defun admin-log (control-string &rest args)
  (format *error-output* "~&pg-lisp: ~a~%"
          (apply #'format nil control-string args))
  (finish-output *error-output*))

;;; ---- backend registry ----

;; PID -> backend, for pg_cancel_backend() and pg_terminate_backend().
;; A backend is registered once its startup packet has been accepted, and
;; unregistered when it disconnects.
(defvar *backend-registry* (make-hash-table :test #'eql))
(defvar *backend-registry-lock*
  (sb-thread:make-mutex :name "pg backend registry lock"))
(defvar *n-backends* 0)

(defun list-backends ()
  (sb-thread:with-mutex (*backend-registry-lock*)
    (loop for backend being the hash-values of *backend-registry*
          collect backend)))

(defun backend-count ()
  (sb-thread:with-mutex (*backend-registry-lock*) *n-backends*))

(defun lookup-backend (pid)
  (sb-thread:with-mutex (*backend-registry-lock*)
    (gethash pid *backend-registry*)))

(defun register-backend (backend)
  (sb-thread:with-mutex (*backend-registry-lock*)
    (setf (gethash (backend-pid backend) *backend-registry*) backend)
    (incf *n-backends*))
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
    ((backend-executing backend)
     (setf (backend-cancel-pending backend) t)))
  (values))

(defun terminate-all-backends ()
  (dolist (backend (list-backends))
    (terminate-backend backend)))

;;; ---- shutdown state machine ----

(defun finish-shutdown ()
  "Exit once every client backend has gone (smart/fast shutdown)."
  (let ((go nil))
    (sb-thread:with-mutex (*shutdown-lock*)
      (unless *shutdown-finished*
        (setf *shutdown-finished* t
              go t)))
    (when go
      (admin-log "database system is shut down")
      (sb-ext:exit :code 0))))

(defun maybe-finish-shutdown ()
  (when (and (shutdown-in-progress-p) (zerop (backend-count)))
    (finish-shutdown)))

(defun unregister-backend (backend)
  (let ((last nil))
    (sb-thread:with-mutex (*backend-registry-lock*)
      (when (eq (gethash (backend-pid backend) *backend-registry*) backend)
        (remhash (backend-pid backend) *backend-registry*)
        (decf *n-backends*))
      ;; An immediate shutdown does not wait for backends; do not race it
      ;; into the clean-exit path with status 0.
      (setf last (and (<= +smart-shutdown+ *shutdown* +fast-shutdown+)
                      (zerop *n-backends*))))
    (when last
      (finish-shutdown)))
  (values))

(defun immediate-shutdown ()
  "Exit without cleanup, like PostgreSQL's immediate shutdown.

There is no WAL to recover, so the only cleanup skipped is removing the
Unix control socket; the next start deletes a stale node anyway."
  (sb-ext:exit :code 1 :abort t))

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

;;; ---- signal handlers ----

(defun request-smart-shutdown (&rest signal-info)
  (declare (ignore signal-info))
  (process-shutdown-request +smart-shutdown+))

(defun request-fast-shutdown (&rest signal-info)
  "SIGINT, i.e. Ctrl-C in a terminal, is PostgreSQL's fast shutdown."
  (declare (ignore signal-info))
  (process-shutdown-request +fast-shutdown+))

(defun request-immediate-shutdown (&rest signal-info)
  (declare (ignore signal-info))
  (process-shutdown-request +immediate-shutdown+))

(defun enable-shutdown-handlers ()
  (sb-sys:enable-interrupt sb-unix:sigterm #'request-smart-shutdown)
  (sb-sys:enable-interrupt sb-unix:sigint #'request-fast-shutdown)
  (sb-sys:enable-interrupt sb-unix:sigquit #'request-immediate-shutdown)
  (values))

;;; ---- SQL entry points ----

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
  (unless (integerp timeout)
    (ereport "22023" "invalid parameter value"))
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
  "pg_reload_conf(): send SIGHUP to ourselves, like signalling the postmaster."
  (handler-case
      (progn
        (sb-posix:kill (sb-posix:getpid) sb-unix:sighup)
        t)
    (error (e)
      (warning-notice "failed to send signal to postmaster: ~a" e)
      nil)))
