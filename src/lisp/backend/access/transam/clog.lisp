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

(defvar *clog-pending-commits* (make-hash-table :test #'eql))

(defstruct (clog-commit-tag
            (:constructor %make-clog-commit-tag (xid incarnation timeline terminal-end)))
  (xid 0 :read-only t)
  (incarnation nil :read-only t)
  (timeline 0 :read-only t)
  (terminal-end 0 :read-only t))

(defun clog-require-no-pending-under-lock ()
  (assert (sb-thread:holding-mutex-p *clog-lock*))
  (unless (zerop (hash-table-count *clog-pending-commits*))
    (wal-invalid-input "pending CLOG commits prevent storage replacement"))
  (values))

(defun clog-require-no-pending-commits ()
  (sb-thread:with-mutex (*clog-lock*)
    (clog-require-no-pending-under-lock)))

(defun clog-init-under-lock ()
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
  (clog-check-commit-boundary-under-wal-lock xid end)
  (sb-thread:with-mutex (*clog-lock*)
    (let* ((key (xid32-of xid)) (old (gethash key *clog-pending-commits*)))
      (when old
        (clog-check-commit-tag-under-wal-lock old)
        (unless (and (= xid (clog-commit-tag-xid old))
                     (= end (clog-commit-tag-terminal-end old)))
          (wal-invalid-input "pending CLOG slot already has another owner"))
        (return-from clog-stage-commit-under-wal-lock old))
      (unless *clog-stream* (clog-init-under-lock))
      (unless (= +clog-in-progress+ (clog-read-status-under-lock xid))
        (wal-invalid-input "pending CLOG commit would replace a terminal status"))
      (let ((tag (%make-clog-commit-tag xid *wal-storage-incarnation* *wal-insert-timeline* end)))
        (setf (gethash key *clog-pending-commits*) tag)
        tag))))

(defun clog-materialize-commit-under-wal-lock (tag &key fsync)
  (clog-check-commit-tag-under-wal-lock tag)
  (unless (<= (clog-commit-tag-terminal-end tag) *wal-durable-lsn*)
    (wal-invalid-input "pending CLOG commit WAL is not durable"))
  (sb-thread:with-mutex (*clog-lock*)
    (let ((key (xid32-of (clog-commit-tag-xid tag))))
      (unless (eq tag (gethash key *clog-pending-commits*))
        (wal-invalid-input "pending CLOG tag is not the installed owner"))
      (clog-write-status-under-lock (clog-commit-tag-xid tag) +clog-committed+ :fsync fsync)
      (unless fsync (finish-output *clog-stream*))
      (remhash key *clog-pending-commits*)))
  (values))

(defun clog-commit-durable-under-wal-lock (xid end)
  (clog-materialize-commit-under-wal-lock
   (clog-stage-commit-under-wal-lock xid end)))

(defun clog-get (xid)
  (sb-thread:with-mutex (*clog-lock*)
    (if (gethash (xid32-of xid) *clog-pending-commits*)
        +clog-committed+
        (clog-read-status-under-lock xid))))

(defun clog-set (xid status &key fsync)
  (sb-thread:with-mutex (*clog-lock*)
    (when (gethash (xid32-of xid) *clog-pending-commits*)
      (wal-invalid-input "direct CLOG write would replace a pending commit"))
    (clog-write-status-under-lock xid status :fsync fsync))
  (values))

(defun clog-fsync ()
  (sb-thread:with-mutex (*clog-lock*)
    (when *clog-stream*
      (fsync-stream *clog-stream*))))

(defun clog-reset-file-under-wal-lock (fsync-function fsync-directory-function)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
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
  (wal-replace-storage-incarnation-under-lock)
  (values))

(defun clog-reset-under-wal-lock (fsync-function fsync-directory-function wal-reset-p)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (clog-require-no-pending-commits)
  (when (and wal-reset-p *wal-ready*) (wal-log-clog-reset-under-lock))
  (clog-reset-file-under-wal-lock fsync-function fsync-directory-function))

(defun clog-reset (&key (fsync-function #'fsync-stream)
                     (fsync-directory-function #'fsync-directory)
                     (wal-reset-p t))
  (if (sb-thread:holding-mutex-p *wal-lock*)
      (clog-reset-under-wal-lock fsync-function fsync-directory-function wal-reset-p)
      (with-wal-publication-fence (begin-publication)
        (clog-require-no-pending-commits)
        (begin-publication)
        (clog-reset-under-wal-lock fsync-function fsync-directory-function wal-reset-p)))
  (values))

(defun clog-recover (next-xid)
  "Leave commit replay to startup recovery after the CLOG file is open."
  (declare (ignore next-xid))
  ;; A zero CLOG status is IN_PROGRESS only while its full XID is present in
  ;; *ACTIVE-XIDS*. After restart that set is empty, so the same entry is
  ;; treated as ABORTED without scanning the entire transaction-ID history.
  (values))
