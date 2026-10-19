;;;; bufmgr.lisp -- page access layer.
;;;;
;;;; The MVP has no buffer pool: smgr serializes page I/O under its lock,
;;;; and a read copies the page out so that a scan never observes a torn
;;;; pd_lower/pd_upper while a writer is modifying the page.

(in-package #:pg)

(defun buf-read (relfilenode block)
  (smgr-read relfilenode block))

(defun buf-write (relfilenode block page)
  (smgr-write relfilenode block page))

(defun buf-extend (relfilenode page)
  (smgr-extend relfilenode page))

(defun buf-nblocks (relfilenode)
  (smgr-nblocks relfilenode))

(defun buf-fsync (relfilenode)
  (smgr-fsync relfilenode))
