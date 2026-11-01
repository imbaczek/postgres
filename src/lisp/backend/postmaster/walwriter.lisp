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

(defstruct (clog-reset-epoch-plan
            (:constructor %make-clog-reset-epoch-plan
                (service generation thread epoch old-incarnation new-incarnation
                 timeline ownership replacement)))
  "Preallocated reset identities and writer registration for one publication.
The descriptors are immutable; CONSUMED-P prevents reuse."
  (service nil :read-only t)
  (generation 0 :read-only t)
  (thread nil :read-only t)
  (epoch nil :read-only t)
  (old-incarnation nil :read-only t)
  (new-incarnation nil :read-only t)
  (timeline nil :read-only t)
  (ownership nil :read-only t)
  (replacement nil :read-only t)
  consumed-p)

(defvar *primary-wal-writer* nil)
(defvar *primary-wal-writer-generation* 0)
(defvar *primary-wal-writer-lock* (sb-thread:make-mutex :name "primary WAL writer state"))
(defvar *primary-wal-writer-delay* 0.2)
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

(defun validate-primary-wal-writer-storage-under-lock (epoch)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (and *wal-ready* (not (recovery-in-progress-p))
               (= +db-in-production+ (or (getf *wal-control-metadata* :db-state) -1))
               (eq *wal-storage-incarnation* (wal-writer-epoch-incarnation epoch))
               (eql *wal-insert-timeline* (wal-writer-epoch-timeline epoch))
               (eq *postmaster-data-lock* (wal-writer-epoch-ownership epoch))
               *postmaster-data-lock* (postmaster-pid-owned-p))
    (wal-invalid-input "primary WAL writer storage authority is stale"))
  t)

(defun prepare-primary-wal-writer-clog-reset-under-lock ()
  "Prepare the reset identity and replacement epoch under WAL then state lock.
A writer already stopping cannot be prepared; a later stop remains in effect.
Do not require an empty temp domain here: reset WAL can retire dirty scratch."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-require-no-pending-storage)
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (let* ((service *primary-wal-writer*)
           (active-p (not (primary-wal-writer-retired-under-state-lock-p service)))
           (epoch (when active-p (primary-wal-writer-epoch service)))
           (incarnation (gensym "WAL-STORE-")))
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
         (make-wal-writer-epoch incarnation *wal-insert-timeline* *postmaster-data-lock*))))))

(defun validate-primary-wal-writer-clog-reset-plan-under-state-lock (plan)
  "Check one unused plan and its captured writer and storage descriptors.
An accepted plan permits a subsequent stop without clearing that request."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (assert (sb-thread:holding-mutex-p *primary-wal-writer-lock*))
  (unless (and (clog-reset-epoch-plan-p plan)
               (not (clog-reset-epoch-plan-consumed-p plan))
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
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (validate-primary-wal-writer-clog-reset-plan-under-state-lock plan)))

(defun publish-primary-wal-writer-clog-reset-under-lock (plan)
  "Revalidate and publish the paired identities under WAL then state lock.
Consume PLAN in the same critical section so it cannot be reused."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-thread:with-mutex (*primary-wal-writer-lock*)
    (validate-primary-wal-writer-clog-reset-plan-under-state-lock plan)
    (sb-sys:without-interrupts
      (setf (clog-reset-epoch-plan-consumed-p plan) t
            *wal-storage-incarnation* (clog-reset-epoch-plan-new-incarnation plan))
      (when (clog-reset-epoch-plan-replacement plan)
        (setf (primary-wal-writer-epoch (clog-reset-epoch-plan-service plan))
              (clog-reset-epoch-plan-replacement plan)))))
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
  (validate-primary-wal-writer-storage-under-lock epoch))

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

