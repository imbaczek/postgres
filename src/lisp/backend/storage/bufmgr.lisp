;;;; bufmgr.lisp -- page access layer.
;;;;
;;;; The MVP has no buffer pool: smgr serializes page I/O under its lock,
;;;; and a read copies the page out so that a scan never observes a torn
;;;; pd_lower/pd_upper while a writer is modifying the page.

(in-package #:pg)

(defun buf-read (relfilenode block)
  (let ((page (wal-buffer-read relfilenode block)))
    (when page
      (page-migrate-legacy-btree-header page)
      (let ((version (page-u16 page +pd-pagesize-version-off+))
            (expected (logior +blcksz+ +pg-page-layout-version+)))
        (unless (= version expected)
          (ereport "XX001"
                   "unsupported page layout version ~a (expected ~a)"
                   version expected)))
      (unless (wal-verify-page page relfilenode block)
        (ereport "XX001" "page checksum mismatch in relation ~a block ~a"
                 relfilenode block))
      page)))

(defun buf-write (relfilenode block page)
  (wal-buffer-write relfilenode block page))

(defun buf-extend (relfilenode page)
  (wal-buffer-extend relfilenode page))

(defun buf-nblocks (relfilenode)
  (wal-buffer-nblocks relfilenode))

(defun buf-fsync (relfilenode)
  (smgr-fsync relfilenode))
