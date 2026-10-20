;;;; bufmgr.lisp -- page access layer.
;;;;
;;;; The MVP has no buffer pool: smgr serializes page I/O under its lock,
;;;; and a read copies the page out so that a scan never observes a torn
;;;; pd_lower/pd_upper while a writer is modifying the page.

(in-package #:pg)

(defun buf-read (relfilenode block &optional (fork :main))
  (let ((page (wal-buffer-read relfilenode block fork)))
    (when page
      (when (eq fork :main)
        (page-migrate-legacy-btree-header page))
      (let ((version (page-u16 page +pd-pagesize-version-off+))
            (expected (logior +blcksz+ +pg-page-layout-version+)))
        (unless (= version expected)
          (ereport "XX001"
                   "unsupported page layout version ~a (expected ~a)"
                   version expected)))
      (unless (wal-verify-page page relfilenode block fork)
        (ereport "XX001" "page checksum mismatch in relation ~a block ~a"
                 relfilenode block))
      page)))

(defun buf-write (relfilenode block page &optional (fork :main))
  (wal-buffer-write relfilenode block page fork))

(defun buf-extend (relfilenode page &optional (fork :main))
  (wal-buffer-extend relfilenode page fork))

(defun buf-nblocks (relfilenode &optional (fork :main))
  (wal-buffer-nblocks relfilenode fork))

(defun buf-fsync (relfilenode &optional (fork :main))
  (smgr-fsync relfilenode fork))
