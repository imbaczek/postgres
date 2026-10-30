;;;; smgr.lisp -- relation file storage: one file per relfilenode.

(in-package #:pg)

(defvar *relation-streams* (make-hash-table :test #'equal))
(defvar *smgr-lock* (sb-thread:make-mutex :name "pg smgr lock"))

(defun relation-file-path (relfilenode &optional (fork :main))
  (merge-pathnames
   (format nil "base/~a~a" relfilenode
           (case fork
             (:main "")
             (:visibility ".vm")
             (otherwise (error "unknown relation fork ~a" fork))))
   *pgdata*))

(defun smgr-open-locked (relfilenode &optional (fork :main))
  (let ((key (list relfilenode fork)))
    (or (gethash key *relation-streams*)
        (setf (gethash key *relation-streams*)
              (open-relation-file (relation-file-path relfilenode fork))))))

(defun smgr-create (relfilenode &key redo)
  "Durably create an empty main fork before publishing its catalog identity.
REDO permits an existing file and never truncates it, matching smgrcreate redo."
  (let ((path (relation-file-path relfilenode)))
    (sb-thread:with-mutex (*smgr-lock*)
      (when (and (not redo) (probe-file path))
        (error "relation storage already exists: ~a" path))
      (fsync-stream (smgr-open-locked relfilenode)))
    (fsync-directory (merge-pathnames "base/" *pgdata*)))
  (values))

(defun smgr-nblocks (relfilenode &optional (fork :main))
  (sb-thread:with-mutex (*smgr-lock*)
    (let ((stream (gethash (list relfilenode fork) *relation-streams*))
          (path (relation-file-path relfilenode fork)))
      (cond (stream (relation-nblocks stream))
            ((and (eq fork :visibility) (not (probe-file path))) 0)
            (t (relation-nblocks (smgr-open-locked relfilenode fork)))))))

(defun smgr-read (relfilenode block &optional (fork :main))
  (sb-thread:with-mutex (*smgr-lock*)
    (let ((stream (gethash (list relfilenode fork) *relation-streams*)))
      (cond (stream (read-page stream block))
            ((and (eq fork :visibility)
                  (not (probe-file (relation-file-path relfilenode fork))))
             nil)
            (t (read-page (smgr-open-locked relfilenode fork) block))))))

(defun smgr-write (relfilenode block page &optional (fork :main))
  (sb-thread:with-mutex (*smgr-lock*)
    (write-page (smgr-open-locked relfilenode fork) block page))
  (values))

(defun smgr-extend (relfilenode page &optional (fork :main))
  "Append PAGE and return its block number."
  (sb-thread:with-mutex (*smgr-lock*)
    (let* ((stream (smgr-open-locked relfilenode fork))
           (block (relation-nblocks stream)))
      (write-page stream block page)
      block)))

(defun smgr-reset-fork (relfilenode fork)
  "Reset one fork without reading or rewriting its old pages.
The caller must make the reset WAL durable before calling this for WAL-backed
storage, and is responsible for the eventual fsync. Reopen the cached stream
under the smgr lock so buffered state cannot survive the truncation."
  (sb-thread:with-mutex (*smgr-lock*)
    (let* ((key (list relfilenode fork))
           (stream (smgr-open-locked relfilenode fork)))
      (finish-output stream)
      (sb-posix:ftruncate (sb-sys:fd-stream-fd stream) 0)
      ;; Reopen after truncation to discard both buffered input and output state.
      (close stream)
      (remhash key *relation-streams*)
      (smgr-open-locked relfilenode fork)))
  (when (and (eq fork :main) (fboundp 'heap-fsm-forget))
    (funcall 'heap-fsm-forget relfilenode))
  (values))

(defun smgr-fsync (relfilenode &optional (fork :main))
  (sb-thread:with-mutex (*smgr-lock*)
    (let ((stream (gethash (list relfilenode fork) *relation-streams*)))
      (when stream
        (fsync-stream stream)))))

(defun smgr-fsync-all ()
  (sb-thread:with-mutex (*smgr-lock*)
    (maphash (lambda (key stream)
               (declare (ignore key))
               (fsync-stream stream))
             *relation-streams*))
  (values))

(defun smgr-forget (relfilenode &key delete)
  "Close every cached fork for RELFILENODE, optionally deleting each file."
  (sb-thread:with-mutex (*smgr-lock*)
    (let ((keys '()))
      (maphash (lambda (key stream)
                 (declare (ignore stream))
                 (when (eql (first key) relfilenode)
                   (push key keys)))
               *relation-streams*)
      (dolist (key keys)
        (let ((stream (gethash key *relation-streams*)))
          (when stream (ignore-errors (close stream)))
          (remhash key *relation-streams*))))
    (when delete
      (dolist (path (list (relation-file-path relfilenode :main)
                          (relation-file-path relfilenode :visibility)))
        (when (probe-file path)
          (delete-file path))))
    (values))
  ;; Heap FSM state is volatile and owned by heapam, which loads after smgr.
  ;; Discard it after releasing *SMGR-LOCK* to preserve lock ordering.
  (when (fboundp 'heap-fsm-forget)
    (funcall (symbol-function 'heap-fsm-forget) relfilenode))
  (when (and delete (fboundp 'btree-forget))
    (funcall (symbol-function 'btree-forget) relfilenode))
  (values))

(defun smgr-close-all ()
  (sb-thread:with-mutex (*smgr-lock*)
    (maphash (lambda (k v)
               (declare (ignore k))
               (ignore-errors (close v)))
             *relation-streams*)
    (clrhash *relation-streams*)))
