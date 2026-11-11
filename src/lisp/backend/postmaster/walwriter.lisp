;;;; Primary WAL writer; independent of managed background-worker slots.
(in-package #:pg)

(declaim (special *reload-definition-lock* *reload-run-lock* *live-operation-depth*
                  *postmaster-data-lock* *server-output* *server-error*
                  *physical-follow-generation-lock* *live-operation-lock*))

(defstruct (wal-writer-epoch
            (:constructor make-wal-writer-epoch (incarnation timeline ownership)))
  "Immutable storage authority captured for a writer cycle.
A new epoch may be installed during a writer's lifetime."
  (incarnation nil :read-only t)
  (timeline nil :read-only t)
  (ownership nil :read-only t))

(defstruct (wal-writer-policy (:constructor make-wal-writer-policy (delay flush-after)))
  "Immutable cycle policy: delay in milliseconds and flush threshold in 8 KiB blocks."
  (delay 200 :type (integer 1 10000) :read-only t)
  (flush-after 128 :type (integer 0 2147483647) :read-only t))

(defstruct (primary-wal-writer
            (:constructor %make-primary-wal-writer (generation epoch output error-output)))
  "Writer lifetime and mutable state, separate from its current storage epoch.
When both locks are needed, take WAL before the writer state lock."
  (generation 0 :read-only t)
  epoch
  (output nil :read-only t)
  (error-output nil :read-only t)
  (ready (sb-thread:make-semaphore) :read-only t)
  (wake (sb-thread:make-semaphore) :read-only t)
  thread stop-p joined-p wake-pending-p
  (state :starting)
  failure
  (cycles 0)
  last-drained-end policy last-flush-time (idle-cycles 0) sleeping-p)

(defstruct (clog-reset-epoch-plan
            (:constructor %make-clog-reset-epoch-plan
                (service generation thread epoch old-incarnation new-incarnation
                 timeline ownership replacement &optional fpw-predecessor fpw-replacement
                                                  logical-predecessor logical-replacement
                                                  logical-info-predecessor logical-info-replacement)))
  "Preallocated reset identities, policy evidence and writer registration.
LOGICAL-PREDECESSOR and LOGICAL-REPLACEMENT carry logical status across the
same validated incarnation change.
FPW-PREDECESSOR captures the exact prior state; FPW-REPLACEMENT transfers its
evidence to the new incarnation. Descriptors are immutable; CONSUMED-P
prevents reuse."
  (service nil :read-only t)
  (generation 0 :read-only t)
  (thread nil :read-only t)
  (epoch nil :read-only t)
  (old-incarnation nil :read-only t)
  (new-incarnation nil :read-only t)
  (timeline nil :read-only t)
  (ownership nil :read-only t)
  (replacement nil :read-only t)
  (fpw-predecessor nil :read-only t)
  (fpw-replacement nil :read-only t)
  (logical-predecessor nil :read-only t)
  (logical-replacement nil :read-only t)
  (logical-info-predecessor nil :read-only t)
  (logical-info-replacement nil :read-only t)
  consumed-p)

(defvar *primary-wal-writer* nil)
(defvar *primary-wal-writer-generation* 0)

(defvar *primary-wal-writer-shutdown-p* nil)

(defvar *primary-wal-writer-lock* (sb-thread:make-mutex :name "primary WAL writer state"))

(defvar *primary-wal-writer-delay* nil
  "Test override in seconds. NIL uses the installed writer settings.")

(defvar *primary-wal-writer-join-timeout* 5)
(defvar *primary-wal-writer-test-hook* nil)

(defun primary-wal-writer-test-point (point writer)
  (when *primary-wal-writer-test-hook*
    (funcall *primary-wal-writer-test-hook* point writer)))

(defun primary-wal-writer-retired-under-state-lock-p (writer)
  (assert (sb-thread:holding-mutex-p *primary-wal-writer-lock*))
  (or (null writer)
      (and (primary-wal-writer-joined-p writer)
           (member (primary-wal-writer-state writer) '(:stopped :retired))
           (let ((thread (primary-wal-writer-thread writer)))
             (or (null thread) (not (sb-thread:thread-alive-p thread)))))))

(defun primary-wal-writer-storage-current-under-lock-p (epoch)
  "Test whether EPOCH still names this owned, ready primary store under WAL lock."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (and *wal-ready* (not (recovery-in-progress-p))
       (= +db-in-production+ (or (getf *wal-control-metadata* :db-state) -1))
       (eq *wal-storage-incarnation* (wal-writer-epoch-incarnation epoch))
       (eql *wal-insert-timeline* (wal-writer-epoch-timeline epoch))
       (eq *postmaster-data-lock* (wal-writer-epoch-ownership epoch))
       *postmaster-data-lock* (postmaster-pid-owned-p)))

(defun validate-primary-wal-writer-storage-under-lock (epoch)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (primary-wal-writer-storage-current-under-lock-p epoch)
    (wal-invalid-input "primary WAL writer storage authority is stale"))
  t)

(defun prepare-primary-wal-writer-clog-reset-under-lock ()
  "Preallocate the reset identity, FPW and logical policy replacements, and any
writer epoch before file effects.
Hold WAL then state lock. A writer already stopping cannot be prepared; a later
stop remains in effect. Reset WAL can retire dirty scratch, so the temp domain
need not yet be empty."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-require-no-pending-storage)
  (when (wal-fpw-state-p *wal-fpw-state*) (wal-checked-fpw-state-under-lock))
  (when (wal-logical-policy-p *wal-logical-policy*) (wal-checked-logical-policy-under-lock))
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (let* ((service *primary-wal-writer*)
           (active-p (not (primary-wal-writer-retired-under-state-lock-p service)))
           (epoch (when active-p (primary-wal-writer-epoch service)))
           (incarnation (gensym "WAL-STORE-"))
           (fpw-predecessor *wal-fpw-state*)
           (fpw-replacement (wal-fpw-rebind-incarnation
                             fpw-predecessor *wal-storage-incarnation* incarnation))
           (logical-predecessor *wal-logical-policy*)
           (logical-replacement (wal-logical-policy-rebind-incarnation
                                 logical-predecessor *wal-storage-incarnation* incarnation))
           (logical-info-predecessor
             (sb-thread:with-mutex (*xid-lock*)
               (wal-logical-info-current-under-xid-lock t)))
           (logical-info-replacement
             (wal-logical-info-rebind-incarnation
              logical-info-predecessor *wal-storage-incarnation* incarnation)))
      (when active-p
        (unless (and (= (primary-wal-writer-generation service) *primary-wal-writer-generation*)
                     (primary-wal-writer-thread service)
                     (sb-thread:thread-alive-p (primary-wal-writer-thread service))
                     (eq :running (primary-wal-writer-state service))
                     (not (primary-wal-writer-stop-p service))
                     (not (primary-wal-writer-failure service)))
          (wal-invalid-input "CLOG reset requires a healthy current primary WAL writer"))
        (wal-check-healthy)
        (validate-primary-wal-writer-storage-under-lock epoch))
      (%make-clog-reset-epoch-plan
       service *primary-wal-writer-generation*
       (when service (primary-wal-writer-thread service))
       epoch *wal-storage-incarnation* incarnation *wal-insert-timeline* *postmaster-data-lock*
       (when active-p
         (make-wal-writer-epoch incarnation *wal-insert-timeline* *postmaster-data-lock*))
       fpw-predecessor fpw-replacement logical-predecessor logical-replacement
       logical-info-predecessor logical-info-replacement))))

(defun validate-primary-wal-writer-clog-reset-plan-under-state-lock (plan)
  "Check an unused plan against its captured writer and storage descriptors.
Require the exact FPW and logical policy predecessors, with valid replacements
preserving their evidence under the new token. A later stop request remains in effect."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (assert (sb-thread:holding-mutex-p *primary-wal-writer-lock*))
  (unless (and (clog-reset-epoch-plan-p plan)
               (not (clog-reset-epoch-plan-consumed-p plan))
               (sb-thread:with-mutex (*xid-lock*)
                 (and (eq (wal-logical-info-current-under-xid-lock t)
                          (clog-reset-epoch-plan-logical-info-predecessor plan))
                      (wal-logical-info-rebinding-valid-p
                       *wal-logical-info* (clog-reset-epoch-plan-logical-info-replacement plan)
                       (clog-reset-epoch-plan-old-incarnation plan)
                       (clog-reset-epoch-plan-new-incarnation plan))))
               (eq *wal-logical-policy* (clog-reset-epoch-plan-logical-predecessor plan))
               (wal-logical-policy-rebinding-valid-p
                *wal-logical-policy* (clog-reset-epoch-plan-logical-replacement plan)
                (clog-reset-epoch-plan-old-incarnation plan)
                (clog-reset-epoch-plan-new-incarnation plan))
               (eq *wal-fpw-state* (clog-reset-epoch-plan-fpw-predecessor plan))
               (wal-fpw-rebinding-valid-p
                *wal-fpw-state* (clog-reset-epoch-plan-fpw-replacement plan)
                (clog-reset-epoch-plan-old-incarnation plan)
                (clog-reset-epoch-plan-new-incarnation plan))
               (clog-reset-epoch-plan-new-incarnation plan)
               (not (eq (clog-reset-epoch-plan-old-incarnation plan)
                        (clog-reset-epoch-plan-new-incarnation plan)))
               (let ((replacement (clog-reset-epoch-plan-replacement plan)))
                 (or (null replacement)
                     (and (clog-reset-epoch-plan-service plan)
                          (wal-writer-epoch-p (clog-reset-epoch-plan-epoch plan))
                          (wal-writer-epoch-p replacement)
                          (eq (wal-writer-epoch-incarnation replacement)
                              (clog-reset-epoch-plan-new-incarnation plan))
                          (eql (wal-writer-epoch-timeline replacement)
                               (clog-reset-epoch-plan-timeline plan))
                          (eq (wal-writer-epoch-ownership replacement)
                              (clog-reset-epoch-plan-ownership plan)))))
               (eq *wal-storage-incarnation* (clog-reset-epoch-plan-old-incarnation plan))
               (eql *wal-insert-timeline* (clog-reset-epoch-plan-timeline plan))
               (eq *postmaster-data-lock* (clog-reset-epoch-plan-ownership plan)))
    (wal-invalid-input "CLOG reset epoch plan is stale"))
  (let ((service (clog-reset-epoch-plan-service plan)))
    (unless (and (eq service *primary-wal-writer*)
                 (= *primary-wal-writer-generation* (clog-reset-epoch-plan-generation plan))
                 (or (null service)
                     (and (= (primary-wal-writer-generation service)
                             (clog-reset-epoch-plan-generation plan))
                          (eq (primary-wal-writer-thread service)
                              (clog-reset-epoch-plan-thread plan)))))
      (wal-invalid-input "CLOG reset writer registration changed"))
    (if (clog-reset-epoch-plan-replacement plan)
        (progn
          (unless (and (eq (clog-reset-epoch-plan-epoch plan)
                           (primary-wal-writer-epoch service))
                       (not (primary-wal-writer-failure service))
                       (or (eq :running (primary-wal-writer-state service))
                           (and (primary-wal-writer-stop-p service)
                                (member (primary-wal-writer-state service) '(:stopping :stopped)))))
            (wal-invalid-input "CLOG reset writer epoch changed"))
          (wal-check-healthy)
          (validate-primary-wal-writer-storage-under-lock
           (clog-reset-epoch-plan-epoch plan)))
        (unless (primary-wal-writer-retired-under-state-lock-p service)
          (wal-invalid-input "CLOG reset acquired an unexpected primary WAL writer"))))
  plan)

(defun validate-primary-wal-writer-clog-reset-plan-under-lock (plan)
  "Validate PLAN under WAL then writer state lock before reset file effects."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (validate-primary-wal-writer-clog-reset-plan-under-state-lock plan)))

(defun publish-primary-wal-writer-clog-reset-under-lock (plan)
  "Revalidate and publish storage identity, FPW and logical policy, plus any
prepared writer epoch.
After durability, hold WAL then state lock, consume PLAN and publish with
interrupts masked. Revalidation can fail after file effects; the enclosing reset
fail-stops rather than rolling those effects back."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (validate-primary-wal-writer-clog-reset-plan-under-state-lock plan)
    (sb-thread:with-mutex (*xid-lock*)
      (sb-sys:without-interrupts
        (setf (clog-reset-epoch-plan-consumed-p plan) t
              *wal-storage-incarnation* (clog-reset-epoch-plan-new-incarnation plan)
              *wal-fpw-state* (clog-reset-epoch-plan-fpw-replacement plan)
              *wal-logical-policy* (clog-reset-epoch-plan-logical-replacement plan)
              *wal-logical-info* (clog-reset-epoch-plan-logical-info-replacement plan))
        (when (clog-reset-epoch-plan-replacement plan)
          (setf (primary-wal-writer-epoch (clog-reset-epoch-plan-service plan))
                (clog-reset-epoch-plan-replacement plan))))))
  (values))

(defun require-primary-wal-writer-retired-under-lock ()
  "Require any registered writer to be stopped, joined and no longer live."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (unless (primary-wal-writer-retired-under-state-lock-p *primary-wal-writer*)
      (wal-invalid-input "storage adoption requires a stopped and joined primary WAL writer")))
  (values))

(defun require-primary-wal-writer-retired ()
  (if (sb-thread:holding-mutex-p *wal-lock*)
      (require-primary-wal-writer-retired-under-lock)
      (sb-thread:with-mutex (*wal-lock*)
        (require-primary-wal-writer-retired-under-lock))))

(defun primary-wal-writer-stopping-p (writer)
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (or (not (eq writer *primary-wal-writer*))
        (primary-wal-writer-stop-p writer))))

(defun wake-primary-wal-writer-under-state-lock (writer)
  "Coalesce wakeups into one pending signal under the state lock.
The wait path consumes signals and clears the pending flag under this lock."
  (assert (sb-thread:holding-mutex-p *primary-wal-writer-lock*))
  (unless (primary-wal-writer-wake-pending-p writer)
    (setf (primary-wal-writer-wake-pending-p writer) t)
    (sb-thread:signal-semaphore (primary-wal-writer-wake writer))))

(defun notify-primary-wal-writer-under-lock ()
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (when (and *primary-wal-writer*
               (not (primary-wal-writer-stop-p *primary-wal-writer*))
               (not (primary-wal-writer-failure *primary-wal-writer*)))
      (wake-primary-wal-writer-under-state-lock *primary-wal-writer*)))
  (values))

(declaim (ftype function startup-wal-writer-settings))

(defun primary-wal-writer-capture-policy ()
  "Capture a policy from the installed root or test delay override for one cycle and wait."
  (if *primary-wal-writer-delay*
      (make-wal-writer-policy (max 1 (round (* 1000 *primary-wal-writer-delay*))) 128)
      (multiple-value-bind (delay blocks) (startup-wal-writer-settings)
        (make-wal-writer-policy delay blocks))))

(defun notify-primary-wal-writer-configuration ()
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (let ((writer *primary-wal-writer*))
      (when (and writer (not (primary-wal-writer-stop-p writer))
                 (not (primary-wal-writer-failure writer)))
        (wake-primary-wal-writer-under-state-lock writer))))
  (values))

(defun wal-writer-flush-threshold-p (request flushed blocks)
  (or (zerop blocks)
      (>= (- (floor request 8192) (floor flushed 8192)) blocks)))

(defun notify-primary-wal-writer-async-under-lock (end)
  "Wake a sleeping writer or one whose async request reaches its flush threshold.
Wakeups are coalesced under the writer state lock; caller holds the WAL lock."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (let* ((writer *primary-wal-writer*)
           (policy (when writer (primary-wal-writer-policy writer))))
      (when (and writer (not (primary-wal-writer-stop-p writer))
                 (not (primary-wal-writer-failure writer))
                 (or (null policy) (primary-wal-writer-sleeping-p writer)
                     (wal-writer-flush-threshold-p
                      end *wal-durable-lsn* (wal-writer-policy-flush-after policy))))
        (wake-primary-wal-writer-under-state-lock writer))))
  (values))

(defun primary-wal-writer-available-under-lock-p ()
  "Check the running writer and its current storage authority under WAL then state lock."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (let ((writer *primary-wal-writer*))
      (and writer
           (not *primary-wal-writer-shutdown-p*)
           (= (primary-wal-writer-generation writer) *primary-wal-writer-generation*)
           (eq :running (primary-wal-writer-state writer))
           (not (primary-wal-writer-stop-p writer))
           (not (primary-wal-writer-failure writer))
           (primary-wal-writer-thread writer)
           (sb-thread:thread-alive-p (primary-wal-writer-thread writer))
           (primary-wal-writer-storage-current-under-lock-p (primary-wal-writer-epoch writer))))))

(defun settle-stopped-primary-wal-writer ()
  "After stop, settle pending work if WAL is healthy under definition then WAL lock.
Leave poisoned work intact; fail-stop on settlement failure after releasing both."
  (sb-sys:without-interrupts
    (let ((failure nil))
      (handler-case
          (call-with-reload-definitions
           (lambda ()
             (sb-thread:with-mutex (*wal-lock*)
               (unless *wal-poisoned*
                 (when (or *wal-pending-page-publication* *wal-pending-page-publication-tail*)
                   (wal-settle-page-publications-under-lock))
                 (when (and *wal-ready* (not (recovery-in-progress-p))
                            (> *wal-end-lsn* *wal-durable-lsn*))
                   (with-wal-poison-on-incomplete ("stopped WAL writer flush exited before completion")
                     (wal-flush-core)))))))
        (error (condition) (setf failure condition)))
      (when failure
        (wal-fail-stop failure)
        (error failure))))
  (values))

(defun validate-primary-wal-writer-under-lock (writer epoch)
  "Check writer and storage authority before a cycle has effects.
The stop check under the state lock decides whether this cycle may proceed;
a cycle accepted before a stop request may finish."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (unless (and (eq writer *primary-wal-writer*)
                 (= (primary-wal-writer-generation writer) *primary-wal-writer-generation*)
                 (eq (primary-wal-writer-thread writer) sb-thread:*current-thread*)
                 (member (primary-wal-writer-state writer) '(:running :stopping))
                 (eq epoch (primary-wal-writer-epoch writer))
                 (not (primary-wal-writer-failure writer)))
      (wal-invalid-input "primary WAL writer no longer owns this cycle"))
    (when (primary-wal-writer-stop-p writer)
      (return-from validate-primary-wal-writer-under-lock nil)))
  (validate-primary-wal-writer-storage-under-lock epoch))

(defun wal-writer-write-request (end flushed async)
  "Select the completed 8 KiB boundary beyond FLUSHED, otherwise the async end.
Return zero when neither advances beyond FLUSHED."
  (let ((completed (* 8192 (floor end 8192))))
    (cond ((> completed flushed) completed)
          ((> async flushed) async)
          (t 0))))

(defun wal-writer-flush-due-p (request flushed last now delay blocks)
  "Return true for pending work when no prior flush is timed, or the delay or block threshold is reached."
  (and (> request flushed)
       (or (null last) (>= (- now last) delay)
           (wal-writer-flush-threshold-p request flushed blocks))))

(defun primary-wal-writer-drain-under-lock (writer epoch)
  "Settle the FIFO and record the captured tail's exact terminal end.
With no FIFO owner, flush outstanding WAL without changing that metric.
This forced drain includes a partial tail at shutdown.
The caller must request fail-stop after releasing WAL."
  (unless (validate-primary-wal-writer-under-lock writer epoch)
    (return-from primary-wal-writer-drain-under-lock :stop))
  (wal-check-healthy)
  (let ((end (when *wal-pending-page-publication-tail*
               (wal-page-publication-terminal-end *wal-pending-page-publication-tail*))))
    (wal-settle-page-publications-under-lock)
    (when (and (null end) (> *wal-end-lsn* *wal-durable-lsn*))
      (with-wal-poison-on-incomplete ("primary WAL writer flush exited before completion")
        (wal-flush-core)))
    (sb-thread:with-mutex (*primary-wal-writer-lock*)
      (incf (primary-wal-writer-cycles writer))
      (when end (setf (primary-wal-writer-last-drained-end writer) end))))
  :continue)

(defun primary-wal-writer-step (writer)
  (when (primary-wal-writer-stopping-p writer) (return-from primary-wal-writer-step :stop))
  (sb-thread:with-mutex (*wal-lock*)
    (when (primary-wal-writer-stopping-p writer) (return-from primary-wal-writer-step :stop))
    (primary-wal-writer-drain-under-lock writer (primary-wal-writer-epoch writer))))

(defun wal-writer-now-ms ()
  (/ (* (get-internal-real-time) 1000) internal-time-units-per-second))

(defun primary-wal-writer-background-under-lock (writer epoch policy)
  "Run one cycle under the WAL lock after checking the writer's storage epoch.
Advertise possible hibernation early so async commits can wake the writer.
Sync WAL before draining page and CLOG publications through the durable frontier."
  (unless (validate-primary-wal-writer-under-lock writer epoch)
    (return-from primary-wal-writer-background-under-lock :stop))
  (wal-check-healthy)
  (wal-check-page-publication-queue-under-lock)
  (clog-check-queued-commits-under-wal-lock)
  (let* ((tail *wal-pending-page-publication-tail*)
         (request (wal-writer-write-request
                   *wal-end-lsn* *wal-durable-lsn*
                   (wal-async-flush-lsn)))
         (now (wal-writer-now-ms))
         (work-p (> request *wal-durable-lsn*))
         (flushed-p nil)
         (drained nil))
    (sb-thread:with-mutex (*primary-wal-writer-lock*)
      (setf (primary-wal-writer-policy writer) policy
            (primary-wal-writer-sleeping-p writer)
            (>= (primary-wal-writer-idle-cycles writer) 49)))
    (with-wal-poison-on-incomplete ("background WAL writer exited before completion")
      (when work-p
        (if (wal-writer-flush-due-p
             request *wal-durable-lsn* (primary-wal-writer-last-flush-time writer)
             now (wal-writer-policy-delay policy) (wal-writer-policy-flush-after policy))
            (progn (wal-flush-core) (setf flushed-p t))
            (when (> request (wal-explicit-written-lsn))
              (wal-write-core))))
      (when tail
        (loop for owner = *wal-pending-page-publication* then (wal-page-publication-next owner)
              while owner
              while (<= (wal-page-publication-terminal-end owner) *wal-durable-lsn*)
              do (setf drained owner)
              when (eq owner tail) return nil)
        (when drained
          (wal-drain-page-publications-through-under-lock drained :fault-on-first-write t)))
      (sb-thread:with-mutex (*primary-wal-writer-lock*)
        (incf (primary-wal-writer-cycles writer))
        (when flushed-p (setf (primary-wal-writer-last-flush-time writer) now))
        (when drained
          (setf (primary-wal-writer-last-drained-end writer)
                (wal-page-publication-terminal-end drained)))
        (setf (primary-wal-writer-idle-cycles writer)
              (if (or work-p drained) 0
                  (min 50 (1+ (primary-wal-writer-idle-cycles writer))))))))
  :continue)

(defun primary-wal-writer-background-step (writer policy)
  "Capture desired FPW policy before WAL locking, then validate the writer epoch.
Publish policy only for the validated epoch, process a deferred logical OFF,
and run the background cycle."
  (when (primary-wal-writer-stopping-p writer)
    (return-from primary-wal-writer-background-step :stop))
  (multiple-value-bind (enabled known) (startup-full-page-writes-policy)
    (with-wal-publication-fence (begin-publication)
      (let ((epoch (primary-wal-writer-epoch writer)))
        (unless (validate-primary-wal-writer-under-lock writer epoch)
          (return-from primary-wal-writer-background-step :stop))
        (when known
          (wal-report-full-page-writes-under-lock
           enabled (wal-writer-epoch-incarnation epoch) #'begin-publication))
        (wal-process-pending-logical-disable-under-lock #'begin-publication)
        (primary-wal-writer-background-under-lock writer epoch policy)))))

(defun request-primary-wal-writer-stop-under-state-lock (writer)
  (assert (sb-thread:holding-mutex-p *primary-wal-writer-lock*))
  (when writer
    (sb-sys:without-interrupts
      (setf (primary-wal-writer-stop-p writer) t)
      (when (member (primary-wal-writer-state writer) '(:starting :running))
        (setf (primary-wal-writer-state writer) :stopping))
      (wake-primary-wal-writer-under-state-lock writer)))
  writer)

(defun request-primary-wal-writer-shutdown ()
  "Close startup and capture the writer to stop in one state-lock section.
The gate stays closed even when there is no writer."
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (setf *primary-wal-writer-shutdown-p* t)
    (request-primary-wal-writer-stop-under-state-lock *primary-wal-writer*)))

(defun request-primary-wal-writer-stop (&optional (writer nil supplied-p))
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (request-primary-wal-writer-stop-under-state-lock
     (if supplied-p writer *primary-wal-writer*))))

(defun finish-primary-wal-writer (writer failure normal-p)
  "Retain the first failure, including an unexpected exit without a condition.
Only the installed writer poisons WAL. Request fail-stop after releasing locks."
  (let ((fence nil))
    (sb-sys:without-interrupts
      (sb-thread:with-mutex (*wal-lock*)
        (sb-thread:with-mutex (*primary-wal-writer-lock*)
          (let ((current-p (and (eq writer *primary-wal-writer*)
                                (= (primary-wal-writer-generation writer)
                                   *primary-wal-writer-generation*))))
            (cond
              ((and normal-p (or (not current-p) (primary-wal-writer-stop-p writer)))
               (setf (primary-wal-writer-state writer) (if current-p :stopped :retired)))
              (t
               (setf (primary-wal-writer-failure writer)
                     (or (primary-wal-writer-failure writer) failure
                         (make-condition 'simple-error
                                         :format-control "primary WAL writer exited unexpectedly"))
                     (primary-wal-writer-state writer) :failed)
               (when current-p
                 (setf *wal-poisoned* t fence (primary-wal-writer-failure writer))))))))
      (when fence
        (let ((*print-circle* t) (*print-level* 6) (*print-length* 20))
          (ignore-errors (admin-log "primary WAL writer failed: ~a" fence)))
        (handler-case (wal-fail-stop fence) (error () nil)))))
  (values))

(defun primary-wal-writer-wait (writer policy)
  "Wait using this cycle's policy; after 50 idle cycles use 25 times the delay."
  (let ((hibernate-p
          (sb-thread:with-mutex (*primary-wal-writer-lock*)
            (>= (primary-wal-writer-idle-cycles writer) 50))))
    (sb-thread:wait-on-semaphore
     (primary-wal-writer-wake writer)
     :timeout (* (/ (wal-writer-policy-delay policy) 1000.0) (if hibernate-p 25 1))))
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (loop while (sb-thread:try-semaphore (primary-wal-writer-wake writer)))
    (setf (primary-wal-writer-wake-pending-p writer) nil))
  (values))

(defun primary-wal-writer-main (writer)
  "Run the auxiliary writer outside backend and live-operation admission.
Capture one policy per cycle and use the same policy for its wait."
  (let ((*backend* nil) (*live-operation-depth* 0)
                        (*standard-output* (primary-wal-writer-output writer))
                        (*error-output* (primary-wal-writer-error-output writer))
                        (failure nil) (normal-p nil))
    (unwind-protect
         (handler-case
             (progn
               (sb-thread:wait-on-semaphore (primary-wal-writer-ready writer))
               (loop
                 (when (primary-wal-writer-stopping-p writer) (return))
                 (primary-wal-writer-test-point :before-cycle writer)
                 (let ((policy nil))
                   (when (eq :stop
                             (call-with-reload-definitions
                              (lambda ()
                                (setf policy (primary-wal-writer-capture-policy))
                                (primary-wal-writer-background-step writer policy))))
                     (return))
                   (primary-wal-writer-test-point :after-cycle writer)
                   (primary-wal-writer-wait writer policy)))
               (setf normal-p t))
           (error (condition) (setf failure condition)))
      (finish-primary-wal-writer writer failure normal-p))))

(defun make-primary-wal-writer-thread (writer)
  (sb-thread:make-thread (lambda () (primary-wal-writer-main writer))
                         :name "pg primary WAL writer"))

(defun start-primary-wal-writer-under-lock ()
  "Register one writer under WAL then state lock before releasing its ready gate.
The thread is independent of managed worker slots. Startup calls this for a
ready primary after installing source identity and before enabling reload."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-healthy)
  (unless (and *wal-ready* (not (recovery-in-progress-p))
               (= +db-in-production+ (or (getf *wal-control-metadata* :db-state) -1))
               *postmaster-data-lock* (postmaster-pid-owned-p)
               (not (shutdown-in-progress-p)))
    (wal-invalid-input "primary WAL writer requires an owned ready primary"))
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (when *primary-wal-writer-shutdown-p*
      (wal-invalid-input "primary WAL writer startup is closed for shutdown"))
    (unless (and (primary-wal-writer-retired-under-state-lock-p *primary-wal-writer*)
                 (or (null *primary-wal-writer*)
                     (null (primary-wal-writer-failure *primary-wal-writer*))))
      (wal-invalid-input "previous primary WAL writer has not retired cleanly"))
    (let ((writer (%make-primary-wal-writer
                   (1+ *primary-wal-writer-generation*)
                   (make-wal-writer-epoch *wal-storage-incarnation* *wal-insert-timeline*
                                          *postmaster-data-lock*)
                   (or *server-output* *standard-output*) (or *server-error* *error-output*))))
      (sb-sys:without-interrupts
        (setf (primary-wal-writer-thread writer) (make-primary-wal-writer-thread writer))
        (setf *primary-wal-writer* writer
              *primary-wal-writer-generation* (primary-wal-writer-generation writer)
              (primary-wal-writer-state writer) :running)
        (sb-thread:signal-semaphore (primary-wal-writer-ready writer)))
      writer)))

(defun start-primary-wal-writer ()
  (if (sb-thread:holding-mutex-p *wal-lock*)
      (start-primary-wal-writer-under-lock)
      (sb-thread:with-mutex (*wal-lock*) (start-primary-wal-writer-under-lock))))

(defun stop-primary-wal-writer (&key shutdown-p)
  "Stop and join outside live admission and the lifecycle locks checked below.
Ordinary stop permits restart; SHUTDOWN-P closes startup permanently. Publish
the joined state under WAL then state lock after the thread exits. Final shutdown
joins after reload workers stop and before storage finalization. After joining,
settle healthy FIFO debt under definition then WAL lock, even without a writer."
  (when (or (sb-thread:holding-mutex-p *wal-lock*)
            (sb-thread:holding-mutex-p *primary-wal-writer-lock*)
            (sb-thread:holding-mutex-p *reload-definition-lock*)
            (sb-thread:holding-mutex-p *reload-run-lock*)
            (sb-thread:holding-mutex-p *physical-follow-generation-lock*)
            (sb-thread:holding-mutex-p *live-operation-lock*)
            (plusp *live-operation-depth*))
    (error "primary WAL writer join requires an outside lifecycle caller"))
  (let ((writer (if shutdown-p
                    (request-primary-wal-writer-shutdown)
                    (request-primary-wal-writer-stop))))
    (when writer
      (when (eq sb-thread:*current-thread* (primary-wal-writer-thread writer))
        (error "primary WAL writer cannot join itself"))
      (let* ((timeout (list :timeout))
             (result (sb-thread:join-thread (primary-wal-writer-thread writer)
                                            :timeout *primary-wal-writer-join-timeout*
                                            :default timeout)))
        (when (eq timeout result) (error "primary WAL writer did not stop"))
        (sb-thread:with-mutex (*wal-lock*)
          (sb-thread:with-mutex (*primary-wal-writer-lock*)
            (setf (primary-wal-writer-joined-p writer) t))))))
  (settle-stopped-primary-wal-writer)
  (values))
