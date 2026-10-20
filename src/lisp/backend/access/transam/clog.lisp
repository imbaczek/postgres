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
  (let* ((path (clog-path))
         (existed (probe-file path)))
    (setf *clog-stream*
          (open path
                :direction :io
                :element-type '(unsigned-byte 8)
                :if-exists :overwrite
                :if-does-not-exist :create))
    (unless existed
      ;; The CLOG file's directory entry must survive before a checkpoint can
      ;; make older commit records unnecessary.
      (fsync-directory *pgdata*)))
  (values))

(defun xid32-of (xid)
  "Return the TransactionId stored in a tuple or pg_xact entry."
  (logand xid +xid-mask+))

(defun clog-byte-index (xid) (floor (xid32-of xid) 4))
(defun clog-bit-shift (xid) (* 2 (mod (xid32-of xid) 4)))

(defun clog-get (xid)
  (sb-thread:with-mutex (*clog-lock*)
    (unless *clog-stream*
      (return-from clog-get +clog-in-progress+))
    (file-position *clog-stream* (clog-byte-index xid))
    (let ((byte (read-byte *clog-stream* nil 0)))
      (ldb (byte 2 (clog-bit-shift xid)) (or byte 0)))))

(defun clog-set (xid status &key fsync)
  (sb-thread:with-mutex (*clog-lock*)
    (unless *clog-stream* (clog-init))
    (let ((pos (clog-byte-index xid))
          (shift (clog-bit-shift xid)))
      (file-position *clog-stream* pos)
      (let* ((old (or (read-byte *clog-stream* nil 0) 0))
             (new (dpb status (byte 2 shift) old)))
        (file-position *clog-stream* pos)
        (write-byte new *clog-stream*)
        (when fsync
          (fsync-stream *clog-stream*)))))
  (values))

(defun clog-fsync ()
  (sb-thread:with-mutex (*clog-lock*)
    (when *clog-stream*
      (fsync-stream *clog-stream*))))

(defun clog-reset (&key (fsync-function #'fsync-stream)
                     (fsync-directory-function #'fsync-directory)
                     (wal-reset-p t))
  "Start a new 32-bit XID cycle after every heap tuple has been frozen."
  (when (and wal-reset-p *wal-ready*)
    ;; The reset record follows the freeze-page images in WAL. Recovery can
    ;; repeat the transition if the process dies before this file replacement.
    (wal-log-clog-reset))
  (sb-thread:with-mutex (*clog-lock*)
    (when *clog-stream*
      (close *clog-stream*)
      (setf *clog-stream* nil))
    (let ((path (clog-path)))
      (with-open-file (s path
                         :direction :output
                         :element-type '(unsigned-byte 8)
                         :if-exists :supersede
                         :if-does-not-exist :create)
        (funcall fsync-function s))
      (funcall fsync-directory-function *pgdata*)
      (setf *clog-stream*
            (open path
                  :direction :io
                  :element-type '(unsigned-byte 8)
                  :if-exists :overwrite
                  :if-does-not-exist :create))))
  (values))

(defun clog-recover (next-xid)
  "Leave commit replay to startup recovery after the CLOG file is open."
  (declare (ignore next-xid))
  ;; A zero CLOG status is IN_PROGRESS only while its full XID is present in
  ;; *ACTIVE-XIDS*. After restart that set is empty, so the same entry is
  ;; treated as ABORTED without scanning the entire transaction-ID history.
  (values))
