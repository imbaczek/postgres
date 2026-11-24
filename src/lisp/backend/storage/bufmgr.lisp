;;;; bufmgr.lisp -- page access layer.
;;;;
;;;; Readers can borrow immutable resident WAL or clean storage pages.
;;;; Callers that edit pages obtain their own vectors through BUF-READ.

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

(defun replacement-write-page (replacement block page &optional owned-p)
  (call-with-replacement-page-lock
   (lambda ()
     (sb-sys:without-interrupts
       (let* ((key (wal-page-key (relation-replacement-relfilenode replacement)
                                 block (relation-replacement-fork replacement)))
              (pages (relation-replacement-pages replacement))
              (reference (wal-dirty-new-reference key (wal-prepare-dirty-page page owned-p)))
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

(defun transaction-has-relation-replacements-p (backend)
  "Check BACKEND's private reset work while the caller holds WAL lock."
  (let ((replacements (gethash backend *transaction-relation-replacements*)))
    (and replacements (plusp (hash-table-count replacements)))))

(defun publish-transaction-replacements-under-wal-lock (backend &optional direct-p)
  "Publish private pages and return owned fork-reset descriptors for commit.
The descriptors remain valid across the durable commit callback, which may
release the private replacements. Discarding cached pages costs work in the
staged cache, but does not scan the old physical relation."
  (declare (special *wal-control-metadata*))
  (let ((replacements (gethash backend *transaction-relation-replacements*))
        (resets nil))
    (when replacements
      (when (and direct-p (getf *wal-control-metadata* :page-checksum-version))
        (error "qualified relation replacement requires WAL"))
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

(defun buf-verify-page (page relfilenode block fork)
  (when page
    (let ((version (page-u16 page +pd-pagesize-version-off+))
          (expected (logior +blcksz+ +pg-page-layout-version+)))
      (unless (= version expected)
        (ereport "XX001"
                 "unsupported page layout version ~a (expected ~a)"
                 version expected)))
    (unless (wal-verify-page page relfilenode block fork)
      (ereport "XX001" "page checksum mismatch in relation ~a block ~a"
               relfilenode block))
    page))

(defun buf-read (relfilenode block &optional (fork :main))
  (let* ((replacement (current-relation-replacement relfilenode fork))
         (page (if replacement (replacement-read-page replacement block)
                   (wal-buffer-read relfilenode block fork))))
    (buf-verify-page page relfilenode block fork)))

;; A handle owns a read-only page version until release, even if WAL retires
;; the dirty reference or storage changes incarnation.  The registry records
;; distinct borrowed vectors and reader counts independently of WAL ownership;
;; it does not keep a dirty reference or scratch store alive.  An owned read
;; can also occupy a handle, but needs no registry entry.
(defstruct buf-page-handle
  page
  borrow-registry)

(defvar *buf-borrow-lock* (sb-thread:make-mutex :name "pg borrowed pages"))
(defvar *buf-borrowed-pages* (make-hash-table :test #'eq))
(defvar *buf-borrow-limit-pages* 512)

(defun buf-borrow-page (handle page)
  (sb-sys:without-interrupts
    (sb-thread:with-mutex (*buf-borrow-lock*)
      (let ((readers (gethash page *buf-borrowed-pages*)))
        (when (or readers (< (hash-table-count *buf-borrowed-pages*)
                             *buf-borrow-limit-pages*))
          (setf (gethash page *buf-borrowed-pages*) (1+ (or readers 0))
                (buf-page-handle-page handle) page
                (buf-page-handle-borrow-registry handle) *buf-borrowed-pages*)
          page)))))

(defun buf-release-page (handle)
  "Release a read handle, including after a WAL incarnation change.
Release is idempotent.  The saved registry, not the current registry, owns
the accounting entry; interrupts and deadlines cannot strand that entry."
  (sb-sys:without-interrupts
    (sb-sys:with-deadline (:seconds nil :override t)
      (let ((registry (buf-page-handle-borrow-registry handle)))
        (when registry
          (sb-thread:with-mutex (*buf-borrow-lock*)
            (let* ((page (buf-page-handle-page handle))
                   (readers (gethash page registry)))
              (if (= readers 1)
                  (remhash page registry)
                  (setf (gethash page registry) (1- readers))))))))
    (setf (buf-page-handle-page handle) nil
          (buf-page-handle-borrow-registry handle) nil))
  (values))

(defun buf-read-clean-page (handle relfilenode block fork)
  (sb-thread:with-mutex (*smgr-lock*)
    (multiple-value-bind (page cached)
        (smgr-read-clean-page-locked relfilenode block fork *wal-storage-incarnation*
                                     #'buf-verify-page *buf-borrow-lock* *buf-borrowed-pages*)
      ;; Register the borrow before eviction can select this entry.
      (values page (and cached (not (buf-borrow-page handle page))) t))))

(defun buf-acquire-read-page (handle relfilenode block &optional (fork :main))
  "Fill an empty, already owned HANDLE and return the page for read-only use.
Install cleanup before acquiring HANDLE and release it on every exit, including
when acquisition or validation fails.  A private replacement shadows the whole
fork.  Otherwise WAL visibility precedes the clean storage cache.  Resident
dirty pages can be borrowed under the WAL lock; scratch and compatibility
entries use owned reads.  When the borrow budget is full, return an owned
copy instead.  Acquisition does not hold a lock across the caller's use."
  (assert (null (buf-page-handle-page handle)))
  (let ((replacement (current-relation-replacement relfilenode fork)))
    (multiple-value-bind (page copy-p validated-p)
        (cond
          (replacement (replacement-read-page replacement block))
          ((not *wal-ready*) (buf-read-clean-page handle relfilenode block fork))
          (t
           (with-wal-page-state-lock
             (let* ((key (wal-page-key relfilenode block fork))
                    (reference (wal-visible-page-reference-under-lock key)))
               (cond
                 ((and (wal-dirty-reference-p reference)
                       (wal-dirty-reference-page reference))
                  (let ((resident (wal-dirty-read-reference key reference)))
                    (if (buf-borrow-page handle resident)
                        resident
                        (values resident t))))
                 ((wal-dirty-reference-p reference)
                  (wal-dirty-read-reference key reference))
                 (reference (values reference t))
                 (t (buf-read-clean-page handle relfilenode block fork)))))))
      (setf page (if copy-p (copy-seq page) page)
            (buf-page-handle-page handle) page)
      (if validated-p page (buf-verify-page page relfilenode block fork)))))

(defmacro with-buf-read-page ((page relfilenode block &optional (fork :main)) &body body)
  "Bind PAGE to a read-only vector for the dynamic extent of BODY."
  (let ((handle (gensym "PAGE-HANDLE-")))
    `(let ((,handle (make-buf-page-handle)))
       (unwind-protect
            (let ((,page (buf-acquire-read-page ,handle ,relfilenode ,block ,fork)))
              ,@body)
         (sb-sys:without-interrupts (buf-release-page ,handle))))))

(defun buf-write (relfilenode block page &optional (fork :main) owned-p)
  "Stage PAGE. OWNED-P gives up the private vector at the call boundary."
  (let ((replacement (current-relation-replacement relfilenode fork)))
    (if replacement (replacement-write-page replacement block page owned-p)
        (wal-buffer-write relfilenode block page fork owned-p))))

(defun buf-extend (relfilenode page &optional (fork :main) owned-p)
  "Append PAGE. OWNED-P gives up the private vector at the call boundary."
  (let ((replacement (current-relation-replacement relfilenode fork)))
    (if replacement
        (let ((block (relation-replacement-nblocks replacement)))
          (replacement-write-page replacement block page owned-p)
          block)
        (wal-buffer-extend relfilenode page fork owned-p))))

(defun buf-nblocks (relfilenode &optional (fork :main))
  (let ((replacement (current-relation-replacement relfilenode fork)))
    (if replacement (relation-replacement-nblocks replacement)
        (wal-buffer-nblocks relfilenode fork))))

(defun buf-fsync (relfilenode &optional (fork :main))
  (smgr-fsync relfilenode fork))
