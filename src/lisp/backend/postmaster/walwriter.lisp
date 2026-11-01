;;;; Primary WAL writer; independent of managed background-worker slots.
(in-package #:pg)

(declaim (special *reload-definition-lock* *reload-run-lock* *live-operation-depth*
                  *postmaster-data-lock* *server-output* *server-error*))

(defstruct (wal-writer-epoch
            (:constructor make-wal-writer-epoch (incarnation timeline ownership)))
  "Immutable storage authority captured for a writer cycle.
A new epoch may be installed during a writer's lifetime."
  (incarnation nil :read-only t)
  (timeline nil :read-only t)
  (ownership nil :read-only t))

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
  last-drained-end)

(defvar *primary-wal-writer* nil)
(defvar *primary-wal-writer-generation* 0)
(defvar *primary-wal-writer-lock* (sb-thread:make-mutex :name "primary WAL writer state"))
(defvar *primary-wal-writer-delay* 0.2)
(defvar *primary-wal-writer-join-timeout* 5)
(defvar *primary-wal-writer-test-hook* nil)

(defun primary-wal-writer-test-point (point writer)
  (when *primary-wal-writer-test-hook*
    (funcall *primary-wal-writer-test-hook* point writer)))

(defun primary-wal-writer-stopping-p (writer)
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (or (not (eq writer *primary-wal-writer*))
        (primary-wal-writer-stop-p writer))))

(defun wake-primary-wal-writer-under-state-lock (writer)
  "Coalesce wakeups into one pending signal under the state lock.
A future loop must consume the signal and clear the pending flag coherently."
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
  (unless (and *wal-ready* (not (recovery-in-progress-p))
               (= +db-in-production+ (or (getf *wal-control-metadata* :db-state) -1))
               (eq *wal-storage-incarnation* (wal-writer-epoch-incarnation epoch))
               (eql *wal-insert-timeline* (wal-writer-epoch-timeline epoch))
               (eq *postmaster-data-lock* (wal-writer-epoch-ownership epoch))
               *postmaster-data-lock* (postmaster-pid-owned-p))
    (wal-invalid-input "primary WAL writer storage authority is stale"))
  t)

(defun primary-wal-writer-drain-under-lock (writer epoch)
  "Settle the FIFO and record the captured tail's exact terminal end.
The caller must fence failures after releasing WAL; this function does not."
  (unless (validate-primary-wal-writer-under-lock writer epoch)
    (return-from primary-wal-writer-drain-under-lock :stop))
  (wal-check-healthy)
  (let ((end (when *wal-pending-page-publication-tail*
               (wal-page-publication-terminal-end *wal-pending-page-publication-tail*))))
    (wal-settle-page-publications-under-lock)
    (sb-thread:with-mutex (*primary-wal-writer-lock*)
      (incf (primary-wal-writer-cycles writer))
      (when end (setf (primary-wal-writer-last-drained-end writer) end))))
  :continue)

(defun primary-wal-writer-step (writer)
  (when (primary-wal-writer-stopping-p writer) (return-from primary-wal-writer-step :stop))
  (sb-thread:with-mutex (*wal-lock*)
    (when (primary-wal-writer-stopping-p writer) (return-from primary-wal-writer-step :stop))
    (primary-wal-writer-drain-under-lock writer (primary-wal-writer-epoch writer))))

(defun request-primary-wal-writer-stop (&optional (writer *primary-wal-writer*))
  (when writer
    (sb-thread:with-mutex (*primary-wal-writer-lock*)
      (sb-sys:without-interrupts
        (setf (primary-wal-writer-stop-p writer) t)
        (when (member (primary-wal-writer-state writer) '(:starting :running))
          (setf (primary-wal-writer-state writer) :stopping))
        (wake-primary-wal-writer-under-state-lock writer))))
  writer)

