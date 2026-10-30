;;;; A promote file and SIGUSR1 request a local transition, as in C pg_ctl.
;;;; The signal handler only records a flag. A separate worker does the work so
;;;; the postmaster can still process shutdown, including immediate shutdown.
;;;;
;;;; Serialize against standby shutdown, then hold the follow command lock.
;;;; Stop new recovery workers and join existing workers before taking reload
;;;; exclusion or closing admission: their exit paths can need those gates.
;;;; Under closed admission, enroll retained readers and replay the safe durable
;;;; prefix. An unsafe complete batch defers the attempt until readers can end;
;;;; an unfinished tail does not advance the fork.
;;;;
;;;; Save a fresh restartpoint before binding intent to a fork and child timeline.
;;;; The child publisher commits WAL/control and adopts the live runtime before
;;;; admission reopens. Before intent, shutdown may take over healthy recovery.
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

(defun physical-promotion-replay-backlog ()
  "Flush receipt and replay the safe durable backlog with verified readers.

Return true when no complete terminal remains beyond the applied position.
An unsafe complete batch defers promotion; an unfinished tail does not."
  (unless *physical-promotion-preserve-readers-p*
    (error "promotion replay requires verified reader ownership"))
  (physical-recovery-flush)
  (if (physical-recovery-reader-conflict-p)
      (physical-replay-with-readers)
      (physical-recovery-replay-core))
  (multiple-value-bind (removed terminal)
      (physical-recovery-proven-drops
       *physical-receive-store* (getf *physical-recovery-state* :cluster-identity)
       :required-lsn *last-replay-lsn*
       :policy (getf *physical-recovery-state* :timeline-policy))
    (declare (ignore removed))
    (= terminal *last-replay-lsn*)))

(declaim (ftype (function () *) physical-promotion-awaiting-child-initialization-p))

(defun physical-promotion-try-online ()
  "Attempt promotion under reload exclusion and closed admission.
Recheck child initialization after worker joins and before enrolling readers.
An unsafe complete batch defers the attempt; otherwise publish the child."
  (sb-thread:with-mutex (*reload-run-lock*)
    (with-live-operation-quiescence
      (physical-promotion-check-online)
      (when (physical-promotion-awaiting-child-initialization-p)
        (return-from physical-promotion-try-online :awaiting-child-initialization))
      (call-with-physical-promotion-readers
       (lambda ()
         (if (not (physical-promotion-replay-backlog))
             :deferred
             (progn
               (physical-recovery-restartpoint-core)
               (physical-promotion-test-point :before-online-intent)
               (physical-promotion-publish-intent)
               (physical-promotion-test-point :after-online-intent)
               (physical-promotion-install-prefix)
               (prog1 (physical-promotion-complete-child)
                 (setf *physical-promotion-stopping-p* nil)))))))))

(defun physical-promotion-awaiting-child-initialization-p ()
  "Scan the durable suffix without applying it to check child initialization.
Validate allocation transitions too. Promotion waits for a complete child
checkpoint before stopping workers and rechecks after their final flush."
  (let* ((*physical-recovery-state* (copy-list *physical-recovery-state*))
         (store *physical-receive-store*)
         (timeline (physical-recovery-replay-timeline))
         (terminal *last-replay-lsn*)
         (next *next-xid*)
         (reservation (copy-list (getf *physical-recovery-state* :source-reservation)))
         (removed (make-hash-table :test #'eql)))
    (with-physical-replay-staging ()
      (physical-recovery-map-permitted-records
       store
       (lambda (record raw)
         (if (= (wal-record-type record) +wal-record-end-of-recovery+)
             (setf timeline (physical-replay-check-selected-eor store record raw terminal timeline)
                   (getf *physical-recovery-state* :replay-timeline) timeline)
             (progn
               (physical-replay-check-child-initialization record next)
               (setf next (physical-replay-observed-next-xid record next))
               (physical-history-accept-record
                record (getf *physical-recovery-state* :cluster-identity) removed)
               (when (= (wal-record-type record) +wal-record-checkpoint+)
                 (setf (getf *physical-recovery-state* :source-checkpoint)
                       (wal-checkpoint-record-snapshot record)))))
         (setf reservation (physical-source-reservation-next reservation record next))
         (unless *wal-recovery-group-active*
           (setf terminal (wal-record-end record))))
       :start-lsn *last-replay-lsn*))
    (/= timeline (getf (getf *physical-recovery-state* :source-checkpoint)
                       :timeline (getf *physical-recovery-state* :timeline)))))

(defun physical-promotion-resume-recovery-workers (config paused restartpoint)
  "Restore prior workers after a healthy pre-intent wait for child initialization.
Restore pause state before clearing stopping under restartpoint-start, then
pause lock. Acknowledged pause commands cannot be overwritten by this restore.
An unattached or disabled standby resumes replay without starting a receiver."
  (with-live-operation
    (physical-promotion-check-online)
    (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
      (sb-thread:with-mutex (*physical-replay-pause-lock*)
        (setf *physical-replay-paused-p* paused
              *physical-promotion-stopping-p* nil))))
  (when config
    (cond
      ((physical-follow-desired-claimed-p *physical-follow-desired*)
       (physical-follow-check-launch config)
       (physical-follow-launch-workers config nil))
      ((physical-follow-controller-enabled-p config) (physical-follow-launch config))
      (t (physical-follow-launch-replay-only config))))
  (when restartpoint (physical-restartpoint-start-worker))
  :awaiting-child-initialization)

(defun physical-recovery-promote ()
  "Promote the owning live standby without restarting its postmaster.

Run outside operation admission. Cancel and join any follow transition before
holding shutdown and follow-command exclusion,
stop worker starts, unpause replay and join workers before closing admission.
Use the fresh saved config, including a disabled route, if workers must resume.
Retry unsafe complete replay barriers outside the gates, allowing readers to
finish.  Return the child checkpoint end, or :SHUTDOWN if shutdown takes over
before intent.  Other failures after work starts fence the process for restart."
  (when (plusp *live-operation-depth*)
    (error "online promotion must run outside operation admission"))
  (physical-follow-call-with-controller-blocked
   (lambda ()
     (sb-thread:with-mutex (*physical-shutdown-lock*)
       (sb-thread:with-mutex (*physical-follow-command-lock*)
         (with-live-operation
           (physical-promotion-check-online)
           (when (physical-promotion-awaiting-child-initialization-p)
             (return-from physical-recovery-promote :awaiting-child-initialization)))
         (let* ((finished nil)
                (runtime *physical-follow-runtime*)
                (config (let ((saved (when (probe-file (physical-follow-config-path))
                                       (physical-follow-read-config))))
                          (when (and saved runtime (physical-follow-runtime-config runtime)
                                     (not (equal (physical-follow-effective-config saved)
                                                 (physical-follow-effective-config
                                                  (physical-follow-runtime-config runtime)))))
                            (error "saved follow route differs from the promotion runtime"))
                          (cond (saved saved)
                                (runtime
                                 (or (physical-follow-runtime-config runtime)
                                     (list 1 (physical-follow-runtime-host runtime)
                                           (physical-follow-runtime-port runtime)
                                           (physical-follow-runtime-pin runtime)))))))
                (paused nil)
                (restartpoint (and *physical-restartpoint-worker*
                                   (sb-thread:thread-alive-p *physical-restartpoint-worker*))))
           (unwind-protect
                (handler-case
                    (progn
                      (with-live-operation
                        (sb-thread:with-mutex (*physical-restartpoint-start-lock*)
                          (sb-thread:with-mutex (*physical-replay-pause-lock*)
                            (setf paused *physical-replay-paused-p*
                                  *physical-promotion-stopping-p* t
                                  *physical-replay-paused-p* nil))))
                      (physical-follow-quiesce-runtime)
                      (physical-restartpoint-join-for-shutdown)
                      (physical-promotion-test-point :after-online-worker-join)
                      (loop for result = (physical-promotion-try-online)
                            unless (eq result :deferred)
                              do (when (eq result :awaiting-child-initialization)
                                   (physical-promotion-resume-recovery-workers config paused restartpoint))
                                 (setf finished t) (return result)
                            do (sleep 0.05)))
                  (physical-promotion-shutdown (condition)
                    (unless (physical-promotion-can-yield-to-shutdown-p)
                      (error condition))
                    (setf finished t)
                    :shutdown))
             (unless finished (setf *physical-recovery-failed-p* t)))))))))

(defun request-physical-promotion (&rest signal-info)
  "Record SIGUSR1 without locks or file I/O. The request worker checks the file."
  (declare (ignore signal-info))
  (setf *physical-promotion-requested-p* t))

(defun physical-promotion-file-sqlstate (errno)
  "Map a promotion signal file error to a SQLSTATE."
  (cond
    ((member errno (list sb-posix:eperm sb-posix:eacces sb-posix:erofs)) "42501")
    ((= errno sb-posix:enoent) "58P01")
    ((= errno sb-posix:eexist) "58P02")
    ((member errno (list sb-posix:enotdir sb-posix:eisdir sb-posix:enotempty)) "42809")
    ((= errno sb-posix:enospc) "53100")
    ((= errno sb-posix:enomem) "53200")
    ((member errno (list sb-posix:enfile sb-posix:emfile)) "53000")
    ((= errno sb-posix:eio) "58030")
    ((= errno sb-posix:enametoolong) "58P03")
    (t "XX000")))

(defun physical-promotion-request-from-sql ()
  "Create and close the promotion signal file, then wake the worker.
Mask interrupts through the file and wake handoff. Once accepted, the request
is independent of the SQL transaction and survives cancellation or disconnect."
  (physical-promotion-check-online)
  (unless (and *physical-promotion-worker*
               (sb-thread:thread-alive-p *physical-promotion-worker*))
    (ereport "55000" "standby promotion worker is not running"))
  (let ((path (merge-pathnames "promote" *pgdata*))
        (fd nil))
    (handler-case
        (sb-sys:without-interrupts
          (unwind-protect
               (progn
                 (setf fd (sb-posix:open (namestring path)
                                         (logior sb-posix:o-wronly sb-posix:o-creat sb-posix:o-trunc
                                                 sb-posix:o-nofollow sb-posix:o-nonblock
                                                 +postmaster-o-cloexec+)
                                         #o600))
                 (unless (sb-posix:s-isreg (sb-posix:stat-mode (sb-posix:fstat fd)))
                   (ereport "42809" "promotion signal is not a regular file"))
                 (let ((owned fd))
                   (setf fd nil)
                   (sb-posix:close owned))
                 (physical-promotion-test-point :after-sql-request-close-before-wake)
                 (request-physical-promotion))
            (when fd
              (let ((owned fd))
                (setf fd nil)
                (sb-posix:close owned)))))
      (sb-posix:syscall-error (condition)
        (ereport (physical-promotion-file-sqlstate (sb-posix:syscall-errno condition))
                 "could not create or close file ~s: ~a" "promote" condition))))
  t)

(defun physical-promotion-wait-for-sql (deadline)
  "Wait until promotion, failure or DEADLINE using in-memory state only.
This parked callback must not enter admission or inspect storage.
Return false once the deadline expires, even if promotion finishes later."
  (loop
    (when (>= (get-internal-real-time) deadline) (return nil))
    (when *physical-recovery-failed-p* (return nil))
    (unless (recovery-in-progress-p) (return t))
    (check-for-interrupts)
    (sleep (min 0.1 (max 0.001
                         (/ (- deadline (get-internal-real-time))
                            (float internal-time-units-per-second 1d0)))))))

(defun pg-promote (wait wait-seconds)
  "Request standby promotion and optionally wait for completion.
NULL arguments return NULL. Check the recovery role before the wait limit.
A waiting call parks its operation token while preserving reader snapshots.
Check recovery health after restoring the token, and keep the callback's
deadline result. Timeout returns false with a warning without cancelling
the accepted request."
  (sql-strict-let ((wait-p wait) (seconds wait-seconds))
    (unless (recovery-in-progress-p)
      (ereport "55000" "recovery is not in progress"
               :hint "Recovery control functions can only be executed during recovery."))
    (unless (plusp seconds)
      (ereport "22003" "\"wait_seconds\" must not be negative or zero"))
    (physical-promotion-request-from-sql)
    (unless wait-p (return-from pg-promote t))
    (let* ((deadline (+ (get-internal-real-time)
                        (* seconds internal-time-units-per-second)))
           (promoted (call-with-live-operation-parked
                      (lambda () (physical-promotion-wait-for-sql deadline)))))
      (check-recovery-available)
      (if (and promoted (not (recovery-in-progress-p)))
          t
          (progn
            (warning-notice "server did not promote within ~d second~:p" seconds)
            nil)))))

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
  "Keep an accepted request pending while child initialization is incomplete.
If the wait is discovered after workers stop, restore them and republish the request.
A SQL timeout does not withdraw the accepted request."
  (when *physical-promotion-requested-p*
    (setf *physical-promotion-requested-p* nil)
    (handler-case
        (if (with-live-operation
              (physical-promotion-check-online)
              (physical-promotion-awaiting-child-initialization-p))
            (setf *physical-promotion-requested-p* t)
            (when (physical-promotion-consume-signal)
              (admin-log "received promote request")
              (when (eq :awaiting-child-initialization (physical-recovery-promote))
                (with-live-operation (physical-promotion-request-from-sql)))))
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
