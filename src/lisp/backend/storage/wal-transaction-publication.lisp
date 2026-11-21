;;;; Primary WAL transaction and maintenance publication.

(in-package #:pg)

(declaim (ftype function wal-validate-logical-binary-capability-under-lock))

(defun wal-admit-transaction-under-lock (xid logical-changes)
  "Prepare and queue a transaction's pages and CLOG completion under WAL lock.
Reserve against the planned terminal end before attempting insertion. Publish
only after the actual end matches, then hand the owner to the FIFO. An
unattempted reservation can be cancelled; an ambiguous attempt retains its owner
for fail-stop handling. Return the owner and actual terminal end.
Caller masks interrupts through handoff and fences incomplete publication.
Pressure may settle predecessors before this batch starts. The third value
requires synchronous completion of this owner before any asynchronous acceptance.
This helper does not flush the new owner or provide an acceptance receipt."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-page-publication-queue-under-lock)
  (clog-check-queued-commits-under-wal-lock)
  (wal-validate-logical-binary-capability-under-lock xid)
  (when (and (fboundp 'transaction-has-relation-replacements-p)
             (funcall 'transaction-has-relation-replacements-p
                      (when (boundp '*backend*) (symbol-value '*backend*))))
    (wal-settle-page-publications-under-lock)
    (wal-require-no-pending-storage))
  (let* ((resets
           (when (fboundp 'publish-transaction-replacements-under-wal-lock)
             (funcall 'publish-transaction-replacements-under-wal-lock
                      (when (boundp '*backend*) (symbol-value '*backend*)))))
         (keys (wal-primary-page-keys))
         (split-p *wal-pending-btree-split*))
    (wal-primary-preflight-page-keys keys)
    (multiple-value-bind (projection forced-p) (wal-preflight-publication-pressure keys resets)
      (with-wal-primary-page-spool (spool publication)
        (wal-append-running-xacts-under-lock xid)
        (wal-primary-record-page-xids keys (wal-append-batch-start))
        (dolist (reset resets)
          (wal-append-record +wal-record-relation-reset+ xid
                             (wal-encode-relation-reset (first reset) (second reset))))
        (wal-primary-append-pages keys spool)
        (wal-primary-seal-page-spool spool)
        (wal-prepare-page-publication-under-lock publication xid resets split-p)
        (setf forced-p
              (wal-publication-pressure-requires-flush-p publication projection forced-p))
        (when logical-changes (wal-append-logical-changes xid logical-changes))
        (when *wal-output-stream* (finish-output *wal-output-stream*))
        (wal-test-pause :after-page-images)
        (let* ((payload (wal-encode-commit-timestamp (postgres-current-timestamp)))
               (size (+ +wal-header-size+ (length payload)))
               (planned-end (+ (wal-next-record-start size) size))
               (completion (clog-reserve-commit-under-wal-lock xid planned-end publication)))
          (assert (eq completion (wal-page-publication-completion publication)))
          (wal-test-pause :after-clog-reservation)
          (clog-begin-reserved-commit-under-wal-lock completion publication)
          (wal-test-pause :before-terminal-append)
          (let ((end (wal-append-record +wal-record-commit+ xid payload)))
            (clog-publish-reserved-commit-under-wal-lock completion publication end)
            (wal-test-pause :before-page-publication-transfer)
            (wal-publish-page-publication-under-lock publication end)
            (wal-test-pause :after-page-publication-transfer)
            (values publication end forced-p)))))))

(defun wal-commit-transaction
    (xid after-durable-function &key logical-changes terminal-durable-function
                                  (source-id nil source-id-supplied-p) accepted-function
                                  (local-durability :flush))
  "Publish XID's commit and return its end and immutable historical receipt.
LOCAL-DURABILITY defaults to :FLUSH. :ASYNC can accept a queued owner
without flushing when pressure, resets and writer availability permit it. The
FIFO retains the owner's pages, spool and CLOG completion independent of the
backend. Otherwise flush and apply through this owner before acceptance; reset
descriptors remain owned through AFTER-DURABLE-FUNCTION.

A receipt records whether durability was observed at acceptance; it is not
upgraded later. The async path calls neither durable callback. On the flush path
TERMINAL-DURABLE-FUNCTION observes the exact end after flush and before
AFTER-DURABLE-FUNCTION. Without WAL, the latter must write the commit status and the
receipt has no WAL end or proof. Incomplete publication is fail-stopped by the
publication fence.

SOURCE-ID must be an explicit immutable symbol or integer when WAL is ready;
NIL is valid. ACCEPTED-FUNCTION receives the returned receipt under the interrupt
mask and must only latch scalar ownership and receipt state, without allocation,
I/O or locks. With WAL it runs under the publication fence and WAL mutex."
  (wal-require-local-commit-durability local-durability)
  (when (and *wal-ready* (not source-id-supplied-p))
    (error "WAL commit requires an explicit source identity"))
  (unless (typep source-id '(or symbol integer))
    (error "WAL commit source identity must be immutable"))
  (unless *wal-ready*
    (sb-thread:with-mutex (*wal-lock*) (wal-require-no-pending-storage))
    (sb-sys:without-interrupts
      (when (fboundp 'publish-transaction-replacements-under-wal-lock)
        (with-wal-critical
          (funcall 'publish-transaction-replacements-under-wal-lock
                   (when (boundp '*backend*) (symbol-value '*backend*)) t)))
      (funcall after-durable-function nil)
      (let ((receipt (%make-wal-commit-receipt xid source-id nil nil)))
        (when accepted-function (funcall accepted-function receipt))
        (return-from wal-commit-transaction (values nil receipt)))))
  (let ((commit-lsn nil) (receipt nil))
    (sb-sys:without-interrupts
      (with-wal-publication-fence (begin-publication)
        (wal-check-page-publication-queue-under-lock)
        (clog-check-queued-commits-under-wal-lock)
        (wal-validate-logical-binary-capability-under-lock xid)
        (when (and (fboundp 'transaction-has-relation-replacements-p)
                   (funcall 'transaction-has-relation-replacements-p
                            (when (boundp '*backend*) (symbol-value '*backend*))))
          (wal-settle-page-publications-under-lock)
          (wal-require-no-pending-storage))
        (begin-publication)
        (multiple-value-bind (publication end forced-p)
            (wal-admit-transaction-under-lock xid logical-changes)
          (setf commit-lsn end)
          (if (and (eq :async local-durability)
                   (not forced-p)
                   (null (wal-page-publication-resets publication))
                   (primary-wal-writer-available-under-lock-p))
              (progn
                (setf receipt (%make-wal-commit-receipt xid source-id commit-lsn nil))
                (wal-slot-record-xid-commit-under-wal-lock xid commit-lsn)
                (clog-accept-publication-under-wal-lock publication)
                (wal-request-async-flush-under-lock commit-lsn))
              (progn
                (wal-flush)
                (when terminal-durable-function (funcall terminal-durable-function commit-lsn))
                (wal-drain-page-publication-prefix-under-lock publication :fault-on-first-write t)
                (wal-slot-record-xid-commit-under-wal-lock xid commit-lsn)
                (wal-test-pause :after-wal-flush)
                (wal-complete-publication-status-under-lock publication)
                (funcall after-durable-function commit-lsn)
                (wal-drain-page-publications-through-under-lock publication :fault-on-first-write t)
                (setf receipt (%make-wal-commit-receipt xid source-id commit-lsn t)))))
        (when accepted-function (funcall accepted-function receipt))))
    (values commit-lsn receipt)))

(defun wal-commit-maintenance ()
  "Durably publish staged storage maintenance without committing a user XID."
  (unless *wal-ready*
    (return-from wal-commit-maintenance (values)))
  (with-wal-publication-fence (begin-publication)
    (wal-check-page-publication-queue-under-lock)
    (clog-check-queued-commits-under-wal-lock)
    (let ((keys (wal-primary-page-keys)) (split-p *wal-pending-btree-split*))
      (wal-primary-preflight-page-keys keys)
      (with-wal-primary-page-spool (spool publication)
        (begin-publication)
        (wal-primary-record-page-xids keys (wal-append-batch-start))
        (wal-primary-append-pages keys spool)
        (wal-primary-seal-page-spool spool)
        (wal-prepare-page-publication-under-lock publication 0 nil split-p :kind :maintenance)
        (let ((end (wal-append-record +wal-record-maintenance+ 0
                                      (make-array 0 :element-type '(unsigned-byte 8)))))
          (wal-publish-page-publication-under-lock publication end))
        (wal-test-pause :after-page-publication-transfer)
        (wal-flush)
        (wal-test-pause :after-wal-flush)
        (wal-drain-page-publications-through-under-lock publication :fault-on-first-write t))))
  (values))

;; TODO: Stream catalog serialization into a temporary file and write bounded
;; WAL chunks from it. Recovery already spills incomplete batches and borrows
;; the catalog bytes from the validated contiguous terminal payload.
