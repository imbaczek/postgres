;;;; Preparation, handoff, and settlement of the primary WAL page FIFO.

(in-package #:pg.wal-publication)

(defstruct (wal-page-publication (:constructor %make-wal-page-publication))
  ;; Before FIFO handoff the stack normally closes SPOOL on exit. A transaction's
  ;; CLOG reservation retains the owner after its terminal attempt starts, so
  ;; cleanup leaves the spool even if handoff fails. The FIFO retains the sealed
  ;; spool, dirty maps, CLOG completion and WAL identity through head retirement. A transaction
  ;; completion points back to this owner; maintenance has XID zero and no
  ;; completion. NEXT links owners in terminal order.
  spool (state :stack) prepared-p pages contributors resets split-p xid
  next completion pressure-ticket (kind :transaction)
  incarnation timeline terminal-end replacement-pages replacement-contributors)

(defvar *wal-pending-page-publication* nil
  "Head of the page publication FIFO, protected with its tail by *WAL-LOCK*.")

(defvar *wal-pending-page-publication-tail* nil)

(defvar *wal-page-publication-appender* nil)

(defun wal-check-page-publication-queue-under-lock ()
  "Validate FIFO links, identity and increasing terminal ends under WAL lock."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (let ((last nil) (previous-end 0))
    (loop for owner = *wal-pending-page-publication* then (wal-page-publication-next owner)
          while owner
          do (unless (and (wal-page-publication-p owner)
                          (member (wal-page-publication-state owner) '(:pending :applied :retiring))
                          (or (null last) (eq :pending (wal-page-publication-state owner)))
                          (eq *wal-storage-incarnation* (wal-page-publication-incarnation owner))
                          (eql *wal-insert-timeline* (wal-page-publication-timeline owner))
                          (typep (wal-page-publication-terminal-end owner)
                                 '(integer 1 18446744073709551615))
                          (< previous-end (wal-page-publication-terminal-end owner))
                          (<= (wal-page-publication-terminal-end owner) *wal-end-lsn*))
               (wal-invalid-input "invalid pending page publication queue"))
             (setf last owner previous-end (wal-page-publication-terminal-end owner)))
    (unless (eq last *wal-pending-page-publication-tail*)
      (wal-invalid-input "pending page publication tail differs from its chain")))
  (values))

(defun wal-require-no-pending-page-publication ()
  "Reject page debt at a storage identity or lifecycle boundary.
Transaction and maintenance publishers enqueue and settle before return;
synchronous barriers settle predecessors first. Caller holds *WAL-LOCK* or has
lifecycle quiescence."
  (when (or *wal-pending-page-publication* *wal-pending-page-publication-tail*)
    (wal-invalid-input "pending page publication prevents another storage publisher"))
  (values))

(defun wal-check-page-publication-owner-under-lock (owner)
  (wal-check-page-publication-queue-under-lock)
  (unless (loop for entry = *wal-pending-page-publication* then (wal-page-publication-next entry)
                while entry thereis (eq owner entry))
    (wal-invalid-input "page publication is not installed in this WAL store"))
  owner)

(defun wal-pending-page-reference (key)
  "Return the newest queued page, presence flag and matching contributors under WAL lock."
  (when *wal-pending-page-publication*
    (wal-check-page-publication-queue-under-lock)
    (let ((reference nil) (present nil) (contributors nil))
      (loop for owner = *wal-pending-page-publication* then (wal-page-publication-next owner)
            while owner
            do (multiple-value-bind (candidate found)
                   (gethash key (wal-page-publication-pages owner))
                 (when found
                   (setf reference candidate present t
                         contributors (gethash key (wal-page-publication-contributors owner))))))
      (values reference present contributors))))

(defun wal-pending-page-contributors (key)
  (nth-value 2 (wal-pending-page-reference key)))

(defun wal-close-primary-page-spool (spool)
  "Close SPOOL and require its temporary-file cleanup to finish."
  (wal-close-owned-spool spool))

(defun call-with-wal-primary-page-spool (function)
  "Call FUNCTION with a stack-owned spool and its publication owner.
The spool is adopted before it is published to the owner. Cleanup closes an
untransferred spool unless a transaction's terminal attempt has begun. Its CLOG
reservation then retains the owner even if queue handoff fails."
  (call-with-wal-temp-cleanup-domain
   (lambda ()
     (when (or *wal-pending-page-publication* *wal-pending-page-publication-tail*)
       (wal-check-page-publication-queue-under-lock)
       (when (wal-page-publication-resets *wal-pending-page-publication-tail*)
         (wal-invalid-input "relation reset publication must finish before another publisher")))
     (let* ((owner (%make-wal-page-publication :incarnation *wal-storage-incarnation*
                                               :timeline *wal-insert-timeline*))
            (*wal-page-publication-appender* owner))
       (unwind-protect
            (progn
              (sb-sys:without-interrupts
                (setf (wal-page-publication-spool owner) (make-wal-owned-spool)))
              (funcall function (wal-page-publication-spool owner) owner))
         (when (and (eq :stack (wal-page-publication-state owner))
                    (or (null (wal-page-publication-completion owner))
                        (clog-release-unattempted-publication-under-wal-lock
                         (wal-page-publication-completion owner) owner)))
           (when (wal-page-publication-spool owner)
             (wal-close-primary-page-spool (wal-page-publication-spool owner)))
           (setf (wal-page-publication-state owner) :released)))))))

(defun wal-map-pending-publication-pages (function)
  "Visit the queued page maps in terminal order under WAL exclusion."
  (when *wal-pending-page-publication*
    (wal-check-page-publication-queue-under-lock)
    (loop for owner = *wal-pending-page-publication* then (wal-page-publication-next owner)
          while owner do (funcall function (wal-page-publication-pages owner))))
  (values))

(defun wal-check-publication-append ()
  "Check outstanding page debt before a WAL append."
  (when (or *wal-pending-page-publication* *wal-pending-page-publication-tail*)
    (if (and (sb-thread:holding-mutex-p *wal-lock*)
             (wal-page-publication-p *wal-page-publication-appender*)
             (eq :stack (wal-page-publication-state *wal-page-publication-appender*))
             (eq *wal-storage-incarnation*
                 (wal-page-publication-incarnation *wal-page-publication-appender*))
             (eql *wal-insert-timeline*
                  (wal-page-publication-timeline *wal-page-publication-appender*)))
        (wal-check-page-publication-queue-under-lock)
        (wal-require-no-pending-page-publication))))

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
                 (wal-dirty-cache-handoff-ready-p
                  (wal-page-publication-pages owner) (wal-page-publication-contributors owner)
                  (wal-page-publication-replacement-pages owner)
                  (wal-page-publication-replacement-contributors owner))
                 (eq *wal-storage-incarnation* (wal-page-publication-incarnation owner))
                 (eql *wal-insert-timeline* (wal-page-publication-timeline owner))
                 (typep end '(integer 1 18446744073709551615))
                 (<= end *wal-end-lsn*)
                 (null (wal-page-publication-next owner))
                 (or (null *wal-pending-page-publication-tail*)
                     (> end (wal-page-publication-terminal-end *wal-pending-page-publication-tail*))))
      (wal-invalid-input "page publication preparation changed before handoff"))
    (clog-check-publication-handoff-under-wal-lock owner end)
    (wal-install-dirty-cache-replacement-under-lock
     (wal-page-publication-replacement-pages owner)
     (wal-page-publication-replacement-contributors owner))
    (setf (wal-page-publication-terminal-end owner) end
          (wal-page-publication-state owner) :pending
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
