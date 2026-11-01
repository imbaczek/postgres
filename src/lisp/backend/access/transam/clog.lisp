;;;; clog.lisp -- commit log (2 status bits per XID, 4 XIDs per byte).

(in-package #:pg)

(defconstant +clog-in-progress+ 0)
(defconstant +clog-committed+ 1)
(defconstant +clog-aborted+ 2)

(defvar *clog-lock* (sb-thread:make-mutex :name "pg clog lock"))
(defvar *clog-stream* nil)

(defun clog-path ()
  (merge-pathnames "pg_xact" *pgdata*))

(defun clog-init ()
  (sb-thread:with-mutex (*clog-lock*) (clog-init-under-lock))
  (values))

(defun xid32-of (xid)
  "Return the TransactionId stored in a tuple or pg_xact entry."
  (logand xid +xid-mask+))

(defun clog-byte-index (xid) (floor (xid32-of xid) 4))
(defun clog-bit-shift (xid) (* 2 (mod (xid32-of xid) 4)))

(defvar *clog-pending-commits* (make-hash-table :test #'eql)
  "Process-local map from 32-bit XID to completion, protected by CLOG lock.
Reserved, inserting and published entries read as IN_PROGRESS; committed entries
provide a committed overlay without changing raw bits. Readers take CLOG alone;
writers take WAL then CLOG. Production transaction owners hold their completions;
the FIFO retains queued owners. This map provides status lookup and retains debt
across reload.")

(defstruct (clog-commit-tag
            (:constructor %make-clog-commit-tag (xid incarnation timeline terminal-end)))
  ;; Full XID, local WAL store incarnation, timeline and exact terminal end
  ;; identify the owner of a pending commit; none may change after staging.
  (xid 0 :read-only t)
  (incarnation nil :read-only t)
  (timeline 0 :read-only t)
  (terminal-end 0 :read-only t))

(defstruct (clog-commit-completion
            (:constructor %make-clog-commit-completion (tag owner state)))
  ;; TAG fixes the WAL identity and planned terminal end. OWNER links back to
  ;; the publication holding this completion. The map retains both through an
  ;; ambiguous terminal attempt. STATE is protected by CLOG lock. Only an
  ;; unattempted reservation can be cancelled.
  (tag nil :read-only t)
  (owner nil :read-only t)
  (state :reserved))

(defun clog-check-planned-commit-under-wal-lock (xid end)
  "Validate a future terminal boundary while the caller holds WAL lock."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-healthy)
  (unless (and *wal-ready* *wal-storage-incarnation*
               (typep xid '(integer 3 18446744073709551615))
               (>= (xid32-of xid) +first-normal-xid+)
               (typep end '(integer 1 18446744073709551615))
               (> end *wal-end-lsn*))
    (wal-invalid-input "invalid planned CLOG commit boundary"))
  (values))

(defun clog-check-completion-under-lock (completion owner &key materialized-p uninstalled-reservation-p)
  "Check owner, WAL identity and map membership with WAL and CLOG locks held.
MATERIALIZED-P also permits an already retired completion.
UNINSTALLED-RESERVATION-P permits a reserved completion absent from the map
when installation failed."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (assert (sb-thread:holding-mutex-p *clog-lock*))
  (wal-check-healthy)
  (unless (and (clog-commit-completion-p completion)
               (eq owner (clog-commit-completion-owner completion)))
    (wal-invalid-input "foreign CLOG completion owner"))
  (let ((tag (clog-commit-completion-tag completion)))
    (unless (and (clog-commit-tag-p tag)
                 (eq *wal-storage-incarnation* (clog-commit-tag-incarnation tag))
                 (eql *wal-insert-timeline* (clog-commit-tag-timeline tag))
                 (or (and uninstalled-reservation-p
                          (eq :reserved (clog-commit-completion-state completion))
                          (not (nth-value 1
                                          (gethash (xid32-of (clog-commit-tag-xid tag))
                                                   *clog-pending-commits*))))
                     (and materialized-p
                          (eq :materialized (clog-commit-completion-state completion)))
                     (eq completion
                         (gethash (xid32-of (clog-commit-tag-xid tag))
                                  *clog-pending-commits*))))
      (wal-invalid-input "CLOG completion identity changed")))
  (values))

(defun clog-reserve-commit-under-wal-lock (xid end owner)
  "Reserve an IN_PROGRESS slot for OWNER and a planned terminal end.
Caller holds WAL lock, masks interrupts through ownership handoff, and must
cancel before beginning the insertion attempt or finish under a failure fence.
For a publication, link the completion to OWNER before installing it in the
map, so unwind can find even an uninstalled reservation. The map retains OWNER
if the terminal outcome is ambiguous."
  (clog-check-planned-commit-under-wal-lock xid end)
  (unless owner (wal-invalid-input "CLOG reservation needs an owner"))
  (sb-thread:with-mutex (*clog-lock*)
    (let ((key (xid32-of xid)))
      (when (gethash key *clog-pending-commits*)
        (wal-invalid-input "CLOG slot is already reserved"))
      (unless *clog-stream* (clog-init-under-lock))
      (unless (= +clog-in-progress+ (clog-read-status-under-lock xid))
        (wal-invalid-input "CLOG reservation would replace a terminal status"))
      (let* ((tag (%make-clog-commit-tag xid *wal-storage-incarnation* *wal-insert-timeline* end))
             (completion (%make-clog-commit-completion tag owner :reserved)))
        (when (wal-page-publication-p owner)
          (unless (null (wal-page-publication-completion owner))
            (wal-invalid-input "publication already owns a CLOG reservation"))
          (setf (wal-page-publication-completion owner) completion)
          (wal-test-pause :before-clog-reservation-install))
        (setf (gethash key *clog-pending-commits*) completion)
        completion))))

(defun clog-cancel-reserved-commit-under-wal-lock (completion owner)
  "Release OWNER's reservation before any terminal insertion attempt."
  (sb-thread:with-mutex (*clog-lock*)
    (clog-check-completion-under-lock completion owner)
    (unless (eq :reserved (clog-commit-completion-state completion))
      (wal-invalid-input "attempted terminal CLOG reservation cannot be cancelled"))
    (remhash (xid32-of (clog-commit-tag-xid (clog-commit-completion-tag completion)))
             *clog-pending-commits*)
    (setf (clog-commit-completion-state completion) :cancelled))
  (values))

(defun clog-begin-reserved-commit-under-wal-lock (completion owner)
  "Mark the terminal insertion attempt before append; cancellation is now unsafe.
The caller must retain the completion and fence an incomplete attempt."
  (sb-thread:with-mutex (*clog-lock*)
    (clog-check-completion-under-lock completion owner)
    (unless (eq :reserved (clog-commit-completion-state completion))
      (wal-invalid-input "CLOG terminal attempt has already begun"))
    (setf (clog-commit-completion-state completion) :inserting))
  (values))

(defun clog-publish-reserved-commit-under-wal-lock (completion owner end)
  "Publish after append using its actual END, which must match the reservation.
The status remains IN_PROGRESS until durable completion."
  (sb-thread:with-mutex (*clog-lock*)
    (clog-check-completion-under-lock completion owner)
    (let ((tag (clog-commit-completion-tag completion)))
      (clog-check-commit-tag-under-wal-lock tag)
      (unless (and (eq :inserting (clog-commit-completion-state completion))
                   (eql end (clog-commit-tag-terminal-end tag))
                   (eql end *wal-end-lsn*))
        (wal-invalid-input "CLOG terminal differs from its reservation"))
      (setf (clog-commit-completion-state completion) :published)))
  (values))

(defun clog-materialize-completion-under-lock (completion &key fsync)
  "Write and retire a committed completion with CLOG lock held.
Caller must first verify its WAL boundary is durable and fence write failure."
  (assert (sb-thread:holding-mutex-p *clog-lock*))
  (let ((tag (clog-commit-completion-tag completion)))
    (clog-write-status-under-lock (clog-commit-tag-xid tag) +clog-committed+ :fsync fsync)
    (unless fsync (finish-output *clog-stream*))
    (remhash (xid32-of (clog-commit-tag-xid tag)) *clog-pending-commits*)
    (setf (clog-commit-completion-state completion) :materialized))
  (values))

(defun clog-complete-reserved-commit-under-wal-lock (completion owner &key fsync)
  "Materialize OWNER's published completion only after its terminal WAL is durable.
Caller holds WAL lock and must fail-stop on an incomplete or ambiguous write.
An already materialized completion may be checked again."
  (sb-thread:with-mutex (*clog-lock*)
    (clog-check-completion-under-lock completion owner :materialized-p t)
    (let ((tag (clog-commit-completion-tag completion)))
      (clog-check-commit-tag-under-wal-lock tag)
      (unless (and (member (clog-commit-completion-state completion)
                           '(:published :accepted :committed :materialized))
                   (<= (clog-commit-tag-terminal-end tag) *wal-durable-lsn*))
        (wal-invalid-input "reserved CLOG commit is not durably published"))
      (unless (eq :materialized (clog-commit-completion-state completion))
        (setf (clog-commit-completion-state completion) :committed)
        (clog-materialize-completion-under-lock completion :fsync fsync))))
  (values))

(defun clog-check-publication-under-lock
    (owner &key (terminal-end (wal-page-publication-terminal-end owner)) publishing-p)
  "Check the publication's CLOG obligation under WAL and CLOG locks.
Transactions need a matching completion; maintenance has XID zero and none.
A queued completion may already be materialized and absent from the map."
  (assert (sb-thread:holding-mutex-p *clog-lock*))
  (let ((completion (wal-page-publication-completion owner)))
    (unless (case (wal-page-publication-kind owner)
              (:transaction (clog-commit-completion-p completion))
              (:maintenance (and (null completion)
                                 (eql 0 (wal-page-publication-xid owner))
                                 (null (wal-page-publication-resets owner)))))
      (wal-invalid-input "publication has an invalid CLOG obligation"))
    (when completion
      (clog-check-completion-under-lock completion owner :materialized-p (not publishing-p))
      (let ((tag (clog-commit-completion-tag completion)))
        (clog-check-commit-tag-under-wal-lock tag)
        (multiple-value-bind (installed present-p)
            (gethash (xid32-of (clog-commit-tag-xid tag)) *clog-pending-commits*)
          (unless (and (eql (clog-commit-tag-xid tag) (wal-page-publication-xid owner))
                       (eql (clog-commit-tag-terminal-end tag) terminal-end)
                       (if publishing-p
                           (and (eq :published (clog-commit-completion-state completion))
                                (eq installed completion))
                           (case (clog-commit-completion-state completion)
                             ((:published :accepted :committed) (eq installed completion))
                             (:materialized (not present-p)))))
            (wal-invalid-input "queued CLOG completion differs from its publication"))))))
  (values))

(defun clog-check-publication-handoff-under-wal-lock (owner end)
  "Check a published completion against the actual end before queue handoff."
  (sb-thread:with-mutex (*clog-lock*)
    (clog-check-publication-under-lock owner :terminal-end end :publishing-p t))
  (values))

(defun clog-check-queued-commits-under-wal-lock ()
  "Check both the FIFO's obligations and the pending map under WAL then CLOG.
Caller has already validated the FIFO links."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (sb-thread:with-mutex (*clog-lock*)
    (loop for owner = *wal-pending-page-publication* then (wal-page-publication-next owner)
          while owner do (clog-check-publication-under-lock owner))
    (maphash
     (lambda (key completion)
       (let ((owner (clog-commit-completion-owner completion)))
         (unless (and (wal-page-publication-p owner)
                      (eq completion (wal-page-publication-completion owner))
                      (eql key (xid32-of (clog-commit-tag-xid (clog-commit-completion-tag completion))))
                      (loop for queued = *wal-pending-page-publication*
                              then (wal-page-publication-next queued)
                            while queued thereis (eq owner queued)))
           (wal-invalid-input "pending CLOG completion has no queued owner"))))
     *clog-pending-commits*))
  (values))

(defun clog-accept-publication-under-wal-lock (owner)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-page-publication-queue-under-lock)
  (clog-check-queued-commits-under-wal-lock)
  (unless (and (wal-page-publication-p owner)
               (eq :pending (wal-page-publication-state owner))
               (loop for queued = *wal-pending-page-publication*
                       then (wal-page-publication-next queued)
                     while queued thereis (eq owner queued)))
    (wal-invalid-input "CLOG acceptance requires a linked pending publication"))
  (sb-thread:with-mutex (*clog-lock*)
    (clog-check-publication-under-lock owner)
    (let ((completion (wal-page-publication-completion owner)))
      (unless (and completion
                   (eq :published (clog-commit-completion-state completion)))
        (wal-invalid-input "CLOG acceptance requires an unaccepted transaction"))
      (setf (clog-commit-completion-state completion) :accepted)))
  (values))

(defun clog-release-unattempted-publication-under-wal-lock (completion owner)
  "Cancel an unattempted reservation, including one not yet installed.
Return NIL after an insertion attempt, leaving its owner for failure handling."
  (unless (and (wal-page-publication-p owner)
               (eq completion (wal-page-publication-completion owner)))
    (wal-invalid-input "publication lost its CLOG reservation"))
  (sb-thread:with-mutex (*clog-lock*)
    (clog-check-completion-under-lock completion owner :uninstalled-reservation-p t)
    (when (eq :reserved (clog-commit-completion-state completion))
      (remhash (xid32-of (clog-commit-tag-xid (clog-commit-completion-tag completion)))
               *clog-pending-commits*)
      (setf (clog-commit-completion-state completion) :cancelled)
      (return-from clog-release-unattempted-publication-under-wal-lock t)))
  nil)

(defun clog-require-no-pending-under-lock ()
  (assert (sb-thread:holding-mutex-p *clog-lock*))
  (unless (zerop (hash-table-count *clog-pending-commits*))
    (wal-invalid-input "pending CLOG commits prevent storage replacement"))
  (values))

(defun clog-require-no-pending-commits ()
  "Check pending debt under CLOG lock.
Callers must also provide their existing quiescence or WAL exclusion: this
snapshot alone cannot prevent a new stage."
  (sb-thread:with-mutex (*clog-lock*)
    (clog-require-no-pending-under-lock)))

(defun clog-init-under-lock ()
  ;; Reopening storage cannot discard pending commits.
  (assert (sb-thread:holding-mutex-p *clog-lock*))
  (clog-require-no-pending-under-lock)
  (let* ((path (clog-path))
         (existed (probe-file path)))
    (setf *clog-stream*
          (open path :direction :io :element-type '(unsigned-byte 8)
                     :if-exists :overwrite :if-does-not-exist :create))
    (unless existed (fsync-directory *pgdata*)))
  (values))

(defun clog-read-status-under-lock (xid)
  (assert (sb-thread:holding-mutex-p *clog-lock*))
  (if *clog-stream*
      (progn
        (file-position *clog-stream* (clog-byte-index xid))
        (ldb (byte 2 (clog-bit-shift xid)) (read-byte *clog-stream* nil 0)))
      +clog-in-progress+))

(defun clog-write-status-under-lock (xid status &key fsync)
  ;; Read the raw byte, not the logical overlay, so another pending slot in
  ;; this byte cannot leak into storage through this read-modify-write.
  (assert (sb-thread:holding-mutex-p *clog-lock*))
  (unless *clog-stream* (clog-init-under-lock))
  (let ((pos (clog-byte-index xid)) (shift (clog-bit-shift xid)))
    (file-position *clog-stream* pos)
    (let ((old (read-byte *clog-stream* nil 0)))
      (file-position *clog-stream* pos)
      (write-byte (dpb status (byte 2 shift) old) *clog-stream*)
      (when fsync (fsync-stream *clog-stream*))))
  (values))

(defun clog-check-commit-boundary-under-wal-lock (xid end)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-check-healthy)
  (unless (and *wal-ready* *wal-storage-incarnation*
               (typep xid '(integer 3 18446744073709551615))
               (>= (xid32-of xid) +first-normal-xid+)
               (typep end '(integer 1 18446744073709551615))
               (<= end *wal-end-lsn*))
    (wal-invalid-input "invalid pending CLOG commit boundary"))
  (values))

(defun clog-check-commit-tag-under-wal-lock (tag)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (clog-commit-tag-p tag) (wal-invalid-input "invalid pending CLOG tag"))
  (clog-check-commit-boundary-under-wal-lock
   (clog-commit-tag-xid tag) (clog-commit-tag-terminal-end tag))
  (unless (and (eq *wal-storage-incarnation* (clog-commit-tag-incarnation tag))
               (eql *wal-insert-timeline* (clog-commit-tag-timeline tag)))
    (wal-invalid-input "pending CLOG tag belongs to another WAL store"))
  (values))

(defun clog-stage-commit-under-wal-lock (xid end)
  "Install or return the matching tag for XID and its exact terminal end.
This legacy staging API exposes committed status immediately and remains for
compatibility and tests. Production publication uses reserved completions."
  (clog-check-commit-boundary-under-wal-lock xid end)
  (sb-thread:with-mutex (*clog-lock*)
    (let* ((key (xid32-of xid)) (old (gethash key *clog-pending-commits*)))
      (when old
        (let ((tag (clog-commit-completion-tag old)))
          (clog-check-commit-tag-under-wal-lock tag)
          (unless (and (eq :committed (clog-commit-completion-state old))
                       (null (clog-commit-completion-owner old))
                       (= xid (clog-commit-tag-xid tag))
                       (= end (clog-commit-tag-terminal-end tag)))
            (wal-invalid-input "pending CLOG slot already has another owner"))
          (return-from clog-stage-commit-under-wal-lock tag)))
      (unless *clog-stream* (clog-init-under-lock))
      (unless (= +clog-in-progress+ (clog-read-status-under-lock xid))
        (wal-invalid-input "pending CLOG commit would replace a terminal status"))
      (let ((tag (%make-clog-commit-tag xid *wal-storage-incarnation* *wal-insert-timeline* end)))
        (setf (gethash key *clog-pending-commits*)
              (%make-clog-commit-completion tag nil :committed))
        tag))))

(defun clog-materialize-commit-under-wal-lock (tag &key fsync)
  "Write TAG only after the authoritative WAL durable frontier reaches it."
  (clog-check-commit-tag-under-wal-lock tag)
  (unless (<= (clog-commit-tag-terminal-end tag) *wal-durable-lsn*)
    (wal-invalid-input "pending CLOG commit WAL is not durable"))
  (sb-thread:with-mutex (*clog-lock*)
    (let ((completion (gethash (xid32-of (clog-commit-tag-xid tag)) *clog-pending-commits*)))
      (unless (and completion
                   (null (clog-commit-completion-owner completion))
                   (eq tag (clog-commit-completion-tag completion))
                   (eq :committed (clog-commit-completion-state completion)))
        (wal-invalid-input "pending CLOG tag is not the installed committed owner"))
      (clog-materialize-completion-under-lock completion :fsync fsync)))
  (values))

(defun clog-commit-durable-under-wal-lock (xid end)
  "Stage and immediately write a commit after WAL flush, not asynchronous acceptance."
  (clog-materialize-commit-under-wal-lock
   (clog-stage-commit-under-wal-lock xid end)))

(defun clog-get (xid)
  "Return logical status from completion state or raw CLOG bits."
  (sb-thread:with-mutex (*clog-lock*)
    (let ((completion (gethash (xid32-of xid) *clog-pending-commits*)))
      (if completion
          (ecase (clog-commit-completion-state completion)
            ((:reserved :inserting :published) +clog-in-progress+)
            ((:accepted :committed) +clog-committed+))
          (clog-read-status-under-lock xid)))))

(defun clog-set (xid status &key fsync)
  "Write raw status, rejecting a slot owned by a pending commit."
  (sb-thread:with-mutex (*clog-lock*)
    (when (gethash (xid32-of xid) *clog-pending-commits*)
      (wal-invalid-input "direct CLOG write would replace a pending commit"))
    (clog-write-status-under-lock xid status :fsync fsync))
  (values))

(defun clog-fsync ()
  "Sync raw CLOG bytes; this does not materialize pending commits."
  (sb-thread:with-mutex (*clog-lock*)
    (when *clog-stream*
      (fsync-stream *clog-stream*))))

(defun clog-reset-file-under-wal-lock (fsync-function fsync-directory-function &optional epoch-plan)
  "Replace CLOG only after page, CLOG and temporary cleanup debt are absent.
Check under WAL lock before closing the file. After the replacement file and
its directory are durable and the file is reopened, publish the new store
identity and any prepared writer epoch from the validated plan."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (wal-require-no-pending-page-publication)
  (wal-require-no-temp-cleanup-under-lock)
  (unless epoch-plan
    (require-primary-wal-writer-retired-under-lock)
    (setf epoch-plan (prepare-primary-wal-writer-clog-reset-under-lock)))
  (validate-primary-wal-writer-clog-reset-plan-under-lock epoch-plan)
  (sb-thread:with-mutex (*clog-lock*)
    (clog-require-no-pending-under-lock)
    (when *clog-stream*
      (close *clog-stream*)
      (setf *clog-stream* nil))
    (let ((path (clog-path)))
      (with-open-file (stream path :direction :output :element-type '(unsigned-byte 8)
                                   :if-exists :supersede :if-does-not-exist :create)
        (funcall fsync-function stream))
      (funcall fsync-directory-function *pgdata*)
      (setf *clog-stream*
            (open path :direction :io :element-type '(unsigned-byte 8)
                       :if-exists :overwrite :if-does-not-exist :create))))
  (publish-primary-wal-writer-clog-reset-under-lock epoch-plan)
  (values))

(defun clog-reset-under-wal-lock
    (fsync-function fsync-directory-function wal-reset-p &optional begin-publication)
  "Settle primary page debt, then require no debt through reset and epoch change.
Caller holds WAL lock. Prepare the plan before logging the reset; that log applies
dirty pages and can retire their shared scratch. The file helper then requires
an empty temp domain before closing CLOG. Replay and cold reset require debt to
be absent already."
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (when (and wal-reset-p *wal-ready*) (wal-settle-page-publications-under-lock))
  (wal-require-no-pending-storage)
  (unless (and wal-reset-p *wal-ready*)
    (require-primary-wal-writer-retired-under-lock))
  (let ((plan (prepare-primary-wal-writer-clog-reset-under-lock)))
    (when begin-publication (funcall begin-publication))
    (when (and wal-reset-p *wal-ready*) (wal-log-clog-reset-under-lock))
    (clog-reset-file-under-wal-lock fsync-function fsync-directory-function plan)))

(defun clog-reset (&key (fsync-function #'fsync-stream)
                     (fsync-directory-function #'fsync-directory)
                     (wal-reset-p t))
  "Reset CLOG under the WAL publication fence.
On the primary, freeze every heap reference from the old XID cycle before
emptying the file. Replay with WAL-RESET-P NIL applies the already logged
freeze, page and reset transition. A caller already holding WAL must supply
its own critical publication fence."
  (if (sb-thread:holding-mutex-p *wal-lock*)
      (clog-reset-under-wal-lock fsync-function fsync-directory-function wal-reset-p)
      (with-wal-publication-fence (begin-publication)
        (clog-reset-under-wal-lock fsync-function fsync-directory-function
                                   wal-reset-p #'begin-publication)))
  (values))

(defun clog-recover (next-xid)
  "Leave commit replay to startup recovery after the CLOG file is open."
  (declare (ignore next-xid))
  ;; A zero CLOG status is IN_PROGRESS only while its full XID is present in
  ;; *ACTIVE-XIDS*. After restart that set is empty, so the same entry is
  ;; treated as ABORTED without scanning the entire transaction-ID history.
  (values))
