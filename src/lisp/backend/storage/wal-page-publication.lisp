;;;; Preparation, handoff, and settlement of the primary WAL page FIFO.

(in-package #:pg)

(defun wal-prepare-page-publication-under-lock (owner xid resets split-p &key (kind :transaction))
  "Seal the spool and prepare empty replacement maps before the terminal record.
Record the current maps and WAL identity while the caller holds *WAL-LOCK*."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-healthy)
  (wal-check-page-publication-queue-under-lock)
  (unless (and *wal-ready* (wal-page-publication-p owner)
               (eq :stack (wal-page-publication-state owner))
               (not (wal-page-publication-prepared-p owner))
               (wal-page-publication-spool owner)
               (or (and (eq kind :transaction)
                        (typep xid '(integer 3 18446744073709551615))
                        (>= (logand xid #xffffffff) 3))
                   (and (eq kind :maintenance) (eql xid 0) (null resets)))
               (null (wal-page-publication-next owner))
               (or (null resets) (null *wal-pending-page-publication*)))
    (wal-invalid-input "invalid page publication preparation"))
  (call-with-wal-spool-owner
   (wal-page-publication-spool owner)
   (lambda () (logical-spool-seal (wal-page-publication-spool owner))))
  (let ((replacement-pages (make-hash-table :test #'equal))
        (replacement-contributors (make-hash-table :test #'equal)))
    (setf (wal-page-publication-pages owner) *wal-page-cache*
          (wal-page-publication-contributors owner) *wal-page-cache-xids*
          (wal-page-publication-resets owner) resets
          (wal-page-publication-split-p owner) split-p
          (wal-page-publication-xid owner) xid
          (wal-page-publication-kind owner) kind
          (wal-page-publication-incarnation owner) *wal-storage-incarnation*
          (wal-page-publication-timeline owner) *wal-insert-timeline*
          (wal-page-publication-replacement-pages owner) replacement-pages
          (wal-page-publication-replacement-contributors owner) replacement-contributors
          (wal-page-publication-prepared-p owner) t))
  (setf (wal-page-publication-pressure-ticket owner) (wal-actual-publication-pressure owner))
  owner)

(defun wal-publish-page-publication-under-lock (owner end)
  "Transfer both whole dirty maps to OWNER at the terminal record's END.
Install the preallocated empty maps by pointer, without changing references."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-sys:without-interrupts
    (wal-check-healthy)
    (wal-check-page-publication-queue-under-lock)
    (unless (and (wal-page-publication-p owner)
                 (eq :stack (wal-page-publication-state owner))
                 (wal-page-publication-prepared-p owner)
                 (eq *wal-page-cache* (wal-page-publication-pages owner))
                 (eq *wal-page-cache-xids* (wal-page-publication-contributors owner))
                 (eq *wal-storage-incarnation* (wal-page-publication-incarnation owner))
                 (eql *wal-insert-timeline* (wal-page-publication-timeline owner))
                 (typep end '(integer 1 18446744073709551615))
                 (<= end *wal-end-lsn*)
                 (null (wal-page-publication-next owner))
                 (or (null *wal-pending-page-publication-tail*)
                     (> end (wal-page-publication-terminal-end *wal-pending-page-publication-tail*)))
                 (hash-table-p (wal-page-publication-replacement-pages owner))
                 (zerop (hash-table-count (wal-page-publication-replacement-pages owner)))
                 (hash-table-p (wal-page-publication-replacement-contributors owner))
                 (zerop (hash-table-count (wal-page-publication-replacement-contributors owner))))
      (wal-invalid-input "page publication preparation changed before handoff"))
    (clog-check-publication-handoff-under-wal-lock owner end)
    (setf (wal-page-publication-terminal-end owner) end
          (wal-page-publication-state owner) :pending
          *wal-page-cache* (wal-page-publication-replacement-pages owner)
          *wal-page-cache-xids* (wal-page-publication-replacement-contributors owner)
          *wal-pending-btree-split* nil
          (wal-page-publication-replacement-pages owner) nil
          (wal-page-publication-replacement-contributors owner) nil)
    (if *wal-pending-page-publication-tail*
        (setf (wal-page-publication-next *wal-pending-page-publication-tail*) owner)
        (setf *wal-pending-page-publication* owner))
    (setf *wal-pending-page-publication-tail* owner))
  owner)

(defun wal-complete-publication-status-under-lock (owner)
  "Complete the durable FIFO head's CLOG obligation, if it has one.
This may be called again after materialization while page retirement is pending."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-page-publication-owner-under-lock owner)
  (unless (eq owner *wal-pending-page-publication*)
    (wal-invalid-input "CLOG completion must start at the queue head"))
  (clog-check-queued-commits-under-wal-lock)
  (let ((completion (wal-page-publication-completion owner)))
    (when completion
      (wal-slot-record-xid-commit-under-wal-lock
       (wal-page-publication-xid owner) (wal-page-publication-terminal-end owner))
      (clog-complete-reserved-commit-under-wal-lock completion owner)))
  (values))

(defun wal-drain-page-publication-under-lock (owner &key fault-on-first-write)
  "Complete CLOG and apply a durable owner, then retire its spool and references.
Only the FIFO head can retire, and it is removed last. Failure leaves debt for
fail-stop handling."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-sys:without-interrupts
    (wal-check-healthy)
    (wal-check-page-publication-owner-under-lock owner)
    (unless (eq owner *wal-pending-page-publication*)
      (wal-invalid-input "page publication drain must start at the queue head"))
    (unless (<= (wal-page-publication-terminal-end owner) *wal-durable-lsn*)
      (wal-invalid-input "pending page publication WAL is not durable"))
    (wal-complete-publication-status-under-lock owner)
    (when (eq :pending (wal-page-publication-state owner))
      (wal-apply-relation-resets (wal-page-publication-resets owner))
      (when (wal-page-publication-resets owner) (wal-test-pause :after-relation-resets))
      (wal-primary-apply-page-spool
       (wal-page-publication-spool owner)
       :split-p (wal-page-publication-split-p owner) :fault-on-first-write fault-on-first-write)
      (setf (wal-page-publication-state owner) :applied))
    (when (eq :applied (wal-page-publication-state owner))
      (wal-close-primary-page-spool (wal-page-publication-spool owner))
      (setf (wal-page-publication-state owner) :retiring))
    (maphash (lambda (key reference)
               (wal-dirty-release-reference reference)
               (remhash key (wal-page-publication-pages owner))
               (remhash key (wal-page-publication-contributors owner)))
             (wal-page-publication-pages owner))
    (wal-dirty-close-empty-store)
    (let ((next (wal-page-publication-next owner)))
      (setf (wal-page-publication-state owner) :released
            *wal-pending-page-publication* next
            (wal-page-publication-next owner) nil)
      (unless next (setf *wal-pending-page-publication-tail* nil))))
  (values))

(defun wal-drain-page-publication-prefix-under-lock (target &key include-target-p fault-on-first-write)
  "Validate and drain the durable FIFO prefix before TARGET, optionally including it."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-sys:without-interrupts
    (wal-check-healthy)
    (wal-check-page-publication-owner-under-lock target)
    (clog-check-queued-commits-under-wal-lock)
    (unless (<= (wal-page-publication-terminal-end target) *wal-durable-lsn*)
      (wal-invalid-input "captured page publication prefix is not durable"))
    (loop for head = *wal-pending-page-publication*
          until (and (eq head target) (not include-target-p))
          do (wal-drain-page-publication-under-lock head :fault-on-first-write fault-on-first-write)
          when (eq head target) return nil))
  (values))

(defun wal-drain-page-publications-through-under-lock (target &key fault-on-first-write)
  "Validate TARGET and its durable prefix before effects; retire through TARGET only."
  (wal-drain-page-publication-prefix-under-lock
   target :include-target-p t :fault-on-first-write fault-on-first-write))

(defmacro with-wal-poison-on-incomplete ((message) &body body)
  "Run BODY under the held WAL lock with interrupts masked.
On incomplete exit, poison WAL before unlock and preserve the first error.
Signal MESSAGE for a bare nonlocal exit. This macro does not request shutdown;
callers must do that after releasing locks."
  (let ((complete (gensym "COMPLETE-"))
        (failure (gensym "FAILURE-"))
        (condition (gensym "CONDITION-")))
    `(progn
       (assert (sb-thread:holding-mutex-p *wal-lock*))
       (let ((,complete nil) (,failure nil))
         (sb-sys:without-interrupts
           (unwind-protect
                (handler-bind ((error (lambda (,condition)
                                        (unless ,failure (setf ,failure ,condition)))))
                  (multiple-value-prog1 (progn ,@body)
                    (setf ,complete t)))
             (unless ,complete
               (setf *wal-poisoned* t)
               (unless ,failure (error ,message)))))))))

(defun wal-settle-page-publications-under-lock ()
  "Flush and drain the captured FIFO prefix under WAL exclusion.
Incomplete settlement poisons WAL. Preserve ordinary errors; convert a bare
nonlocal exit to an error so the caller can fence failure after unlocking."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-healthy)
  (wal-check-page-publication-queue-under-lock)
  (clog-check-queued-commits-under-wal-lock)
  (let ((target *wal-pending-page-publication-tail*))
    (when target
      (with-wal-poison-on-incomplete ("WAL predecessor drain exited before completion")
        (wal-flush-core)
        (wal-drain-page-publications-through-under-lock target :fault-on-first-write t)))
    target))
