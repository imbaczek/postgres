;;;; smgr.lisp -- relation file storage: one file per relfilenode.

(in-package #:pg)

(defvar *relation-streams* (make-hash-table :test #'equal))
(defvar *smgr-lock* (sb-thread:make-mutex :name "pg smgr lock"))

;; Clean pages are immutable after validation.  The key names a fork and
;; block within the current storage domain; a change of incarnation, PGDATA,
;; or stream table clears the lookup table.  A retired vector may still be
;; held by a reader, but cannot be found by a later lookup.
;;
;; *SMGR-LOCK* serializes lookup, fill, and physical mutation.  Where needed,
;; acquire the WAL page-state lock before it, and the borrow lock after it.
;; Eviction skips borrowed vectors; if no slot is available, a validated read
;; is returned without admission.  SMGR-READ remains a physical read.
(defvar *smgr-clean-pages* (make-hash-table :test #'equal))
(defvar *smgr-clean-limit-pages* 512)
(defvar *smgr-clean-slots* #())
(defvar *smgr-clean-next-slot* 0)
(defvar *smgr-clean-incarnation* nil)
(defvar *smgr-clean-pgdata* nil)
(defvar *smgr-clean-streams* nil)

(defun relation-file-path (relfilenode &optional (fork :main))
  (pgdata-resolve-path
   (format nil "base/~a~a" relfilenode
           (case fork
             (:main "")
             (:visibility ".vm")
             (otherwise (error "unknown relation fork ~a" fork))))
   *pgdata*))

(defvar *smgr-redo-p* nil)
(defvar *smgr-redo-existing-main-forks* nil)

(defun smgr-open-locked (relfilenode &optional (fork :main) create)
  ;; Defer interrupts across opening and cache registration so the cached
  ;; stream has an owner before a queued interrupt can be delivered.
  (let ((key (list relfilenode fork)))
    (or (gethash key *relation-streams*)
        (sb-sys:without-interrupts
          (setf (gethash key *relation-streams*)
                (open-relation-file (relation-file-path relfilenode fork) :create create))))))

(defun smgr-write-open-locked (relfilenode fork)
  ;; Redo may need to restore a fork first created (or subsequently dropped)
  ;; in the replay range. A main fork referenced at primary startup already
  ;; had to exist before that catalog was published.
  (smgr-open-locked relfilenode fork
                    (or (eq fork :visibility)
                        (and *smgr-redo-p*
                             (not (and *smgr-redo-existing-main-forks*
                                       (gethash relfilenode *smgr-redo-existing-main-forks*)))))))

(defun smgr-create (relfilenode &key redo)
  "Durably create an empty main fork before publishing its catalog identity.
REDO permits an existing file and never truncates it, matching smgrcreate redo."
  (let ((path (relation-file-path relfilenode)))
    (sb-thread:with-mutex (*smgr-lock*)
      (when (and (not redo) (probe-file path))
        (error "relation storage already exists: ~a" path))
      (fsync-stream (smgr-open-locked relfilenode :main t)))
    (fsync-directory (merge-pathnames "base/" *pgdata*)))
  (values))

(defun smgr-nblocks (relfilenode &optional (fork :main))
  (sb-thread:with-mutex (*smgr-lock*)
    (let ((stream (gethash (list relfilenode fork) *relation-streams*))
          (path (relation-file-path relfilenode fork)))
      (cond (stream (relation-nblocks stream))
            ((and (eq fork :visibility) (not (probe-file path))) 0)
            (t (relation-nblocks (smgr-open-locked relfilenode fork)))))))

(defun smgr-read-page (stream block &optional scratch)
  (declare (ftype function call-with-backend-io-wait))
  (call-with-backend-io-wait :data-file-read (lambda () (read-page stream block scratch))))

(defun smgr-read-locked (relfilenode block fork &optional scratch)
  (declare (ftype function wal-check-read-page))
  (let* ((stream (gethash (list relfilenode fork) *relation-streams*))
         (page (cond (stream (smgr-read-page stream block scratch))
                     ((and (eq fork :visibility)
                           (not (probe-file (relation-file-path relfilenode fork))))
                      nil)
                     (t (smgr-read-page (smgr-open-locked relfilenode fork) block scratch)))))
    (multiple-value-prog1 (wal-check-read-page page relfilenode block)
      (when (and page *statement-resource-usage*)
        (incf (statement-resource-usage-relation-reads *statement-resource-usage*))))))

(defun smgr-read (relfilenode block &optional (fork :main))
  (sb-thread:with-mutex (*smgr-lock*)
    (smgr-read-locked relfilenode block fork)))

(defun smgr-clear-clean-pages-locked (capacity)
  (let ((slots (make-array capacity :initial-element nil)))
    (sb-sys:without-interrupts
      (clrhash *smgr-clean-pages*)
      (setf *smgr-clean-slots* slots
            *smgr-clean-next-slot* 0))))

(defun smgr-remove-clean-page-locked (key)
  (let ((slot (gethash key *smgr-clean-pages*)))
    (when slot
      (sb-sys:without-interrupts
        (remhash key *smgr-clean-pages*)
        (setf (aref *smgr-clean-slots* slot) nil)))))

(defun smgr-clean-victim-locked (borrow-lock borrowed-pages)
  (let ((capacity (length *smgr-clean-slots*)))
    (if (< (hash-table-count *smgr-clean-pages*) capacity)
        (loop for offset below capacity
              for slot = (mod (+ *smgr-clean-next-slot* offset) capacity)
              unless (aref *smgr-clean-slots* slot) return slot)
        (sb-thread:with-mutex (borrow-lock)
          (loop for offset below capacity
                for slot = (mod (+ *smgr-clean-next-slot* offset) capacity)
                for entry = (aref *smgr-clean-slots* slot)
                unless (gethash (cdr entry) borrowed-pages) return slot)))))

(defun smgr-read-clean-page-locked (relfilenode block fork incarnation
                                    verify borrow-lock borrowed-pages &optional scratch)
  "Return a validated page and whether it is cached. Caller holds *SMGR-LOCK*.
On a miss, a supplied SCRATCH page belongs to the caller and is never cached."
  (declare (ftype function wal-check-read-page))
  (unless (and (= *smgr-clean-limit-pages* (length *smgr-clean-slots*))
               (eq incarnation *smgr-clean-incarnation*)
               (equal *pgdata* *smgr-clean-pgdata*)
               (eq *relation-streams* *smgr-clean-streams*))
    (sb-sys:without-interrupts
      (smgr-clear-clean-pages-locked *smgr-clean-limit-pages*)
      (setf *smgr-clean-incarnation* incarnation
            *smgr-clean-pgdata* *pgdata*
            *smgr-clean-streams* *relation-streams*)))
  (let* ((key (list relfilenode fork block))
         (slot (gethash key *smgr-clean-pages*)))
    (when slot
      (return-from smgr-read-clean-page-locked
        (let ((page (wal-check-read-page (cdr (aref *smgr-clean-slots* slot))
                                         relfilenode block)))
          (when *statement-resource-usage*
            (incf (statement-resource-usage-clean-cache-hits *statement-resource-usage*)))
          (values page t))))
    (let ((page (funcall verify (smgr-read-locked relfilenode block fork scratch)
                         relfilenode block fork)))
      (when (and page (not scratch))
        (let ((victim (smgr-clean-victim-locked borrow-lock borrowed-pages)))
          (when victim
            (let ((old (aref *smgr-clean-slots* victim))
                  (entry (cons key page)))
              (sb-sys:without-interrupts
                (when old (smgr-remove-clean-page-locked (car old)))
                (setf (gethash key *smgr-clean-pages*) victim
                      (aref *smgr-clean-slots* victim) entry
                      *smgr-clean-next-slot* (mod (1+ victim) *smgr-clean-limit-pages*))))
            (return-from smgr-read-clean-page-locked (values page t)))))
      (values page nil))))

(defun smgr-invalidate-clean-relation-locked (relfilenode &optional fork)
  (maphash (lambda (key page)
             (declare (ignore page))
             (when (and (eql relfilenode (first key))
                        (or (null fork) (eq fork (second key))))
               (smgr-remove-clean-page-locked key)))
           *smgr-clean-pages*))

(declaim (ftype function wal-check-persisted-page))

(defun smgr-write-page-locked (stream relfilenode block page fork)
  (declare (ftype function call-with-backend-io-wait))
  (wal-check-persisted-page stream relfilenode block page fork)
  ;; Invalidate before writing: even a failed partial write must not leave a
  ;; stale page available for a subsequent cache lookup.
  (when (plusp (hash-table-count *smgr-clean-pages*))
    (smgr-remove-clean-page-locked (list relfilenode fork block)))
  (multiple-value-prog1
      (call-with-backend-io-wait :data-file-write (lambda () (write-page stream block page)))
    (when *statement-resource-usage*
      (incf (statement-resource-usage-relation-writes *statement-resource-usage*)))))

(defun smgr-write (relfilenode block page &optional (fork :main))
  (sb-thread:with-mutex (*smgr-lock*)
    (smgr-write-page-locked (smgr-write-open-locked relfilenode fork) relfilenode block page fork))
  (values))

(defun smgr-extend (relfilenode page &optional (fork :main))
  "Append PAGE and return its block number."
  (sb-thread:with-mutex (*smgr-lock*)
    (let* ((stream (smgr-write-open-locked relfilenode fork))
           (block (relation-nblocks stream)))
      (smgr-write-page-locked stream relfilenode block page fork)
      block)))

(defun smgr-reset-fork (relfilenode fork)
  "Reset one fork without reading or rewriting its old pages.
The caller must make the reset WAL durable before calling this for WAL-backed
storage, and is responsible for the eventual fsync. Reopen the cached stream
under the smgr lock so buffered state cannot survive the truncation."
  (sb-thread:with-mutex (*smgr-lock*)
    (let* ((key (list relfilenode fork))
           (stream (smgr-write-open-locked relfilenode fork)))
      (smgr-invalidate-clean-relation-locked relfilenode fork)
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
    (smgr-invalidate-clean-relation-locked relfilenode)
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
  ;; Finish the close traversal and clear the cache before delivering queued
  ;; interrupts.  Individual close errors remain ignored during the traversal.
  (sb-thread:with-mutex (*smgr-lock*)
    (sb-sys:without-interrupts
      (smgr-clear-clean-pages-locked 0)
      (setf *smgr-clean-incarnation* nil
            *smgr-clean-pgdata* nil
            *smgr-clean-streams* nil)
      (maphash (lambda (k v)
                 (declare (ignore k))
                 (ignore-errors (close v)))
               *relation-streams*)
      (clrhash *relation-streams*))))
