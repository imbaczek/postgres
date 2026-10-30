;;;; bufmgr.lisp -- page access layer.
;;;;
;;;; The MVP has no buffer pool: smgr serializes page I/O under its lock,
;;;; and a read copies the page out so that a scan never observes a torn
;;;; pd_lower/pd_upper while a writer is modifying the page.

(in-package #:pg)

(defvar *backend* nil)

(defstruct relation-replacement
  ;; PAGES owns private dirty references until abort or commit cleanup; the
  ;; original remains visible to other backends until publication.
  relfilenode fork (nblocks 0)
  (pages (make-hash-table :test #'eql)))

(defvar *transaction-relation-replacements*
  (make-hash-table :test #'eq :synchronized t))

(defun current-relation-replacement (relfilenode fork)
  (let ((replacements (and *backend*
                           (gethash *backend* *transaction-relation-replacements*))))
    (and replacements (gethash (list relfilenode fork) replacements))))

(defun call-with-replacement-page-lock (function)
  (if (eq (sb-thread:mutex-owner *wal-lock*) sb-thread:*current-thread*)
      (funcall function)
      (with-wal-page-state-lock (funcall function))))

(defun replacement-read-page (replacement block)
  (call-with-replacement-page-lock
   (lambda ()
     (let ((reference (gethash block (relation-replacement-pages replacement))))
       (when reference
         (copy-seq
          (wal-dirty-read-reference
           (wal-page-key (relation-replacement-relfilenode replacement)
                         block (relation-replacement-fork replacement))
           reference)))))))

(defun replacement-write-page (replacement block page)
  (call-with-replacement-page-lock
   (lambda ()
     (sb-sys:without-interrupts
       (let* ((key (wal-page-key (relation-replacement-relfilenode replacement)
                                 block (relation-replacement-fork replacement)))
              (pages (relation-replacement-pages replacement))
              (reference (wal-dirty-new-reference key (wal-copy-dirty-page page)))
              (old (gethash block pages)))
         (setf (gethash block pages) reference
               (relation-replacement-nblocks replacement)
               (max (1+ block) (relation-replacement-nblocks replacement)))
         (when old (wal-dirty-release-reference old))))))
  (values))

(defun discard-replacement-pages (replacement)
  (maphash (lambda (block reference)
             (declare (ignore block))
             (wal-dirty-release-reference reference))
           (relation-replacement-pages replacement))
  (clrhash (relation-replacement-pages replacement))
  (values))

(defun transaction-replace-relation (relfilenode)
  "Start an empty private relation while retaining the original pages for abort."
  (call-with-replacement-page-lock
   (lambda ()
     (let ((replacements
             (or (gethash *backend* *transaction-relation-replacements*)
                 (setf (gethash *backend* *transaction-relation-replacements*)
                       (make-hash-table :test #'equal)))))
       (dolist (fork '(:main :visibility))
         (let* ((key (list relfilenode fork))
                (previous (gethash key replacements))
                (replacement (make-relation-replacement :relfilenode relfilenode :fork fork)))
           (sb-sys:without-interrupts
             (when previous (discard-replacement-pages previous))
             (setf (gethash key replacements) replacement)))))))
  (when (fboundp 'heap-fsm-forget) (funcall 'heap-fsm-forget relfilenode))
  (values))

(defun publish-transaction-replacements-under-wal-lock (backend &optional direct-p)
  "Publish private pages and return owned fork-reset descriptors for commit.
The descriptors remain valid across the durable commit callback, which may
release the private replacements. Discarding cached pages costs work in the
staged cache, but does not scan the old physical relation."
  (let ((replacements (gethash backend *transaction-relation-replacements*))
        (resets nil))
    (when replacements
      (let ((nodes nil))
        (maphash (lambda (key replacement)
                   (declare (ignore key))
                   (pushnew (relation-replacement-relfilenode replacement) nodes :test #'eql))
                 replacements)
        (wal-discard-relation-pages-under-lock nodes))
      (maphash
       (lambda (key replacement)
         (declare (ignore key))
         (let ((node (relation-replacement-relfilenode replacement))
               (fork (relation-replacement-fork replacement)))
           (push (list node fork) resets)
           (when direct-p (smgr-reset-fork node fork))
           (maphash
            (lambda (block reference)
              (let ((page (wal-dirty-read-reference (wal-page-key node block fork) reference)))
                (if direct-p
                    (smgr-write node block page fork)
                    (let ((*wal-row-page-write-p* nil))
                      (wal-dirty-install-page (wal-page-key node block fork)
                                              (wal-copy-dirty-page page))))))
            (relation-replacement-pages replacement))
           (when direct-p (smgr-fsync node fork))))
       replacements))
    (nreverse resets)))

(defun discard-transaction-replacements (backend)
  "Release BACKEND's private references and close scratch when no references remain."
  (call-with-replacement-page-lock
   (lambda ()
     (let ((replacements (gethash backend *transaction-relation-replacements*)))
       (when replacements
         (maphash (lambda (key replacement)
                    (declare (ignore key))
                    (discard-replacement-pages replacement)
                    (when (fboundp 'heap-fsm-forget)
                      (funcall 'heap-fsm-forget
                               (relation-replacement-relfilenode replacement))))
                  replacements)
         (remhash backend *transaction-relation-replacements*)
         (wal-dirty-close-empty-store)))))
  (values))

(defun buf-read (relfilenode block &optional (fork :main))
  (let* ((replacement (current-relation-replacement relfilenode fork))
         (page (if replacement (replacement-read-page replacement block)
                   (wal-buffer-read relfilenode block fork))))
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
  (let ((replacement (current-relation-replacement relfilenode fork)))
    (if replacement (replacement-write-page replacement block page)
        (wal-buffer-write relfilenode block page fork))))

(defun buf-extend (relfilenode page &optional (fork :main))
  (let ((replacement (current-relation-replacement relfilenode fork)))
    (if replacement
        (let ((block (relation-replacement-nblocks replacement)))
          (replacement-write-page replacement block page)
          block)
        (wal-buffer-extend relfilenode page fork))))

(defun buf-nblocks (relfilenode &optional (fork :main))
  (let ((replacement (current-relation-replacement relfilenode fork)))
    (if replacement (relation-replacement-nblocks replacement)
        (wal-buffer-nblocks relfilenode fork))))

(defun buf-fsync (relfilenode &optional (fork :main))
  (smgr-fsync relfilenode fork))
