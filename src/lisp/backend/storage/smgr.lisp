;;;; smgr.lisp -- relation file storage: one file per relfilenode.

(in-package #:pg)

(defvar *relation-streams* (make-hash-table :test #'eql))
(defvar *smgr-lock* (sb-thread:make-mutex :name "pg smgr lock"))

(defun relation-file-path (relfilenode)
  (merge-pathnames (format nil "base/~a" relfilenode) *pgdata*))

(defun smgr-open-locked (relfilenode)
  (or (gethash relfilenode *relation-streams*)
      (setf (gethash relfilenode *relation-streams*)
            (open-relation-file (relation-file-path relfilenode)))))

(defun smgr-nblocks (relfilenode)
  (sb-thread:with-mutex (*smgr-lock*)
    (relation-nblocks (smgr-open-locked relfilenode))))

(defun smgr-read (relfilenode block)
  (sb-thread:with-mutex (*smgr-lock*)
    (read-page (smgr-open-locked relfilenode) block)))

(defun smgr-write (relfilenode block page)
  (sb-thread:with-mutex (*smgr-lock*)
    (write-page (smgr-open-locked relfilenode) block page))
  (values))

(defun smgr-extend (relfilenode page)
  "Append PAGE and return its block number."
  (sb-thread:with-mutex (*smgr-lock*)
    (let* ((stream (smgr-open-locked relfilenode))
           (block (relation-nblocks stream)))
      (write-page stream block page)
      block)))

(defun smgr-fsync (relfilenode)
  (sb-thread:with-mutex (*smgr-lock*)
    (let ((stream (gethash relfilenode *relation-streams*)))
      (when stream
        (fsync-stream stream)))))

(defun smgr-forget (relfilenode &key delete)
  "Close the cached stream for RELFILENODE, optionally deleting the file."
  (sb-thread:with-mutex (*smgr-lock*)
    (let ((stream (gethash relfilenode *relation-streams*)))
      (when stream
        (ignore-errors (close stream))
        (remhash relfilenode *relation-streams*)))
    (when delete
      (ignore-errors (delete-file (relation-file-path relfilenode))))
    (values)))

(defun smgr-close-all ()
  (sb-thread:with-mutex (*smgr-lock*)
    (maphash (lambda (k v)
               (declare (ignore k))
               (ignore-errors (close v)))
             *relation-streams*)
    (clrhash *relation-streams*)))
