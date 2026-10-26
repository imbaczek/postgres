;;;; A promote file and SIGUSR1 request a local transition, as in C pg_ctl.
;;;; The signal handler only records a flag. A separate worker does the work so
;;;; the postmaster can still process shutdown, including immediate shutdown.
;;;;
;;;; Serialize against standby shutdown, then hold the follow command lock.
;;;; Stop new recovery workers and join existing workers before taking reload
;;;; exclusion or closing admission: their exit paths can need those gates.
;;;; If an idle transaction still owns a snapshot, reopen admission and retry.
;;;; Its client must remain able to commit or roll back.
;;;;
;;;; With readers gone, flush and replay all complete durable received batches.
;;;; Save a fresh restartpoint before binding intent to a fork and child timeline.
;;;; The existing child publisher then commits WAL/control and initializes the
;;;; primary before admission reopens. There is no authority or election service.
;;;; Before intent, process shutdown may take over without poisoning recovery.
;;;; After intent, an uncertain failure requires restart from durable control.
(in-package #:pg)

(defun physical-promotion-check-online ()
  "Require owned, healthy phase-2 streaming recovery before changing lifecycle state.
Call under admission or owned quiescence. Report shutdown separately so the
orchestrator can yield before intent without marking recovery failed."
  (check-recovery-available)
  (when (shutdown-in-progress-p) (error 'physical-promotion-shutdown))
  (unless (and *postmaster-data-lock* (postmaster-pid-owned-p)
               (eql 2 (getf *physical-recovery-state* :phase))
               *physical-receive-store* *wal-ready*
               (not *physical-shutdown-state*) (not (shutdown-in-progress-p))
               (not *swank-listener-enabled-p*))
    (error "online promotion requires an owned healthy streaming standby")))

(defun physical-promotion-try-online ()
  "Try to finish promotion after recovery workers have been joined.
Own reload exclusion and admission quiescence only for this attempt. Return
:DEFERRED if an idle transaction remains, so the caller can sleep with the gate
open. Otherwise replay the complete durable backlog, save its restartpoint,
publish intent and commit the child. Return its checkpoint end."
  (sb-thread:with-mutex (*reload-run-lock*)
    (with-live-operation-quiescence
      (physical-promotion-check-online)
      (when (physical-recovery-reader-conflict-p)
        (return-from physical-promotion-try-online :deferred))
      (physical-recovery-flush)
      (physical-recovery-replay-core)
      (physical-recovery-restartpoint-core)
      (physical-promotion-test-point :before-online-intent)
      (physical-promotion-publish-intent)
      (physical-promotion-test-point :after-online-intent)
      (physical-promotion-install-prefix)
      (prog1 (physical-promotion-complete-child)
        (setf *physical-promotion-stopping-p* nil)))))

(defun physical-recovery-promote ()
  "Promote the owning live standby without restarting its postmaster.
Run outside operation admission. Hold shutdown and follow-command exclusion,
latch worker starts, unpause replay and join workers before closing either gate.
Retry reader conflicts outside the gates; do not terminate reader sessions.
Return the child checkpoint end, or :SHUTDOWN if shutdown takes over before
intent. Other failures after work starts fence the process for restart."
  (when (plusp *live-operation-depth*)
    (error "online promotion must run outside operation admission"))
  (sb-thread:with-mutex (*physical-shutdown-lock*)
    (sb-thread:with-mutex (*physical-follow-command-lock*)
      (with-live-operation (physical-promotion-check-online))
      (let ((finished nil))
        (unwind-protect
             (handler-case
                 (progn
                   (with-live-operation
                     (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
                       (setf *physical-promotion-stopping-p* t))
                     (sb-thread:with-mutex (*physical-replay-pause-lock*)
                       (setf *physical-replay-paused-p* nil)))
                   (physical-follow-quiesce-runtime)
                   (physical-restartpoint-join-for-shutdown)
                   (physical-promotion-test-point :after-online-worker-join)
                   (loop for result = (physical-promotion-try-online)
                         unless (eq result :deferred)
                           do (setf finished t) (return result)
                         do (sleep 0.05)))
               (physical-promotion-shutdown (condition)
                 (unless (and (shutdown-in-progress-p)
                              (eql 2 (getf *physical-recovery-state* :phase))
                              (not *physical-recovery-failed-p*))
                   (error condition))
                 (setf finished t)
                 :shutdown))
          (unless finished (setf *physical-recovery-failed-p* t)))))))

(defun request-physical-promotion (&rest signal-info)
  "Record SIGUSR1 without locks or file I/O. The request worker checks the file."
  (declare (ignore signal-info))
  (setf *physical-promotion-requested-p* t))

(defun physical-promotion-consume-signal ()
  "Remove a regular promote signal file and sync its directory; return true if found.
This only consumes a request. It neither publishes durable intent nor grants
writer admission. Cold startup also uses it to discard stale requests."
  (let ((path (merge-pathnames "promote" *pgdata*)))
    (when (wal-namespace-file-stat path t)
      (sb-posix:unlink (namestring path))
      (fsync-directory *pgdata*)
      t)))

(defun physical-promotion-worker-step ()
  "Consume one signaled request and run promotion outside admission.
Log errors; the transition itself decides whether recovery must be fenced."
  (when *physical-promotion-requested-p*
    (setf *physical-promotion-requested-p* nil)
    (handler-case
        (when (physical-promotion-consume-signal)
          (admin-log "received promote request")
          (physical-recovery-promote))
      (error (condition)
        (admin-log "native promotion failed: ~a" condition)))))

(defun physical-promotion-worker-loop ()
  "Service requests while this process remains a healthy standby.
Keep this separate from the postmaster worker so shutdown stays responsive."
  (loop while (and (recovery-in-progress-p) (not (shutdown-in-progress-p))
                   (not *physical-recovery-failed-p*))
        do (physical-promotion-worker-step)
           (sleep 0.05)))

(defun physical-promotion-initialize-requests ()
  "Discard stale request state after PGDATA ownership, before recovery startup.
MAIN installs the signal handler before publishing its PID file. Requests queued
after this reset remain pending until the worker starts."
  (physical-promotion-consume-signal)
  (setf *physical-promotion-requested-p* nil))

(defun physical-promotion-start-worker ()
  "Start at most one request worker after recovery storage has been initialized.
Primary startup needs no worker. Reject worker starts during a recovery transition."
  (check-physical-recovery-worker-start)
  (when (and (recovery-in-progress-p)
             (not (and *physical-promotion-worker*
                       (sb-thread:thread-alive-p *physical-promotion-worker*))))
    (setf *physical-promotion-worker*
          (sb-thread:make-thread #'physical-promotion-worker-loop
                                 :name "native standby promotion"))))
