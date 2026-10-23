;;;; snapshot.lisp -- per-statement snapshots and MVCC visibility.

(in-package #:pg)

(defstruct (snapshot (:constructor make-snapshot-struct
                         (&key xmin xmax xip curcid)))
  xmin
  xmax
  xip
  curcid)

(defstruct (snapshot-registration
            (:constructor make-snapshot-registration ()))
  snapshot
  wal-retention-lsn
  (releasing-p nil)
  (active-p nil))

(defstruct exported-logical-snapshot
  slot-name token database-oid snapshot registration schema-read-lock-held-p)

(defvar *current-snapshot* nil)
(defvar *exported-logical-snapshot-lock*
  (sb-thread:make-mutex :name "pg exported logical snapshots"))
(defvar *logical-slot-lifecycle-lock*
  (sb-thread:make-mutex :name "pg logical slot lifecycle"))
(defvar *logical-test-after-snapshot-token-lookup-hook* nil)
(defvar *exported-logical-snapshots* (make-hash-table :test #'equal))
(defvar *exported-logical-snapshot-counter* 0)

;;; Active snapshot xmin values bound the oldest tuple version a vacuum may
;;; remove. The count remains separate because XID wraparound waits for every
;;; statement snapshot to drain.
(defvar *snapshot-count-lock* (sb-thread:make-mutex :name "pg snapshot count"))
(defvar *active-snapshots* 0)
(defvar *active-snapshot-set* (make-hash-table :test #'eq))
(defvar *snapshot-cv* (sb-thread:make-waitqueue :name "pg snapshot waitqueue"))
(defvar *snapshot-cv-mutex* (sb-thread:make-mutex :name "pg snapshot cv mutex"))
(defvar *wal-test-after-snapshot-release-mark-hook* nil)

(declaim (ftype function xid-active-p))

(defun register-active-snapshot (snapshot registration)
  "Publish SNAPSHOT's xmin while the caller still holds the XID lock.

REGISTRATION owns both the active count and the snapshot entry before
asynchronous interrupts are re-enabled."
  (unless (snapshot-registration-p registration)
    (error "active snapshots require a snapshot-registration token"))
  (sb-sys:without-interrupts
    (sb-thread:with-mutex (*snapshot-count-lock*)
      (when (or (snapshot-registration-active-p registration)
                (snapshot-registration-releasing-p registration))
        (error "snapshot-registration token is active or being released"))
      (incf *active-snapshots*)
      (setf (gethash snapshot *active-snapshot-set*) t
            (snapshot-registration-snapshot registration) snapshot
            (snapshot-registration-active-p registration) t)))
  snapshot)

(defun oldest-active-snapshot-xmin (&optional exclude-snapshot)
  (sb-thread:with-mutex (*snapshot-count-lock*)
    (let ((oldest nil))
      (maphash (lambda (snapshot present)
                 (when (and present (not (eq snapshot exclude-snapshot)))
                   (let ((xmin (snapshot-xmin snapshot)))
                     (when (or (null oldest) (< xmin oldest))
                       (setf oldest xmin)))))
               *active-snapshot-set*)
      oldest)))

(defun release-snapshot-registration (registration)
  "Release the snapshot, WAL retention pin, and active-count entry once."
  (let ((pinned nil)
        (active nil))
    (sb-sys:without-interrupts
      ;; Boundary capture publishes the pin while holding the snapshot-count
      ;; lock. This first transition makes a concurrent capture either publish
      ;; before us, so we remove its pin, or observe an inactive token and fail.
      (sb-thread:with-mutex (*snapshot-count-lock*)
        (when (and (snapshot-registration-active-p registration)
                   (not (snapshot-registration-releasing-p registration)))
          (setf active t
                pinned (snapshot-registration-wal-retention-lsn registration))
          (when (remhash (snapshot-registration-snapshot registration)
                         *active-snapshot-set*)
            (decf *active-snapshots*))
          (setf (snapshot-registration-snapshot registration) nil
                (snapshot-registration-active-p registration) nil
                (snapshot-registration-releasing-p registration)
                (not (null pinned)))))
      ;; Do not let another capture reuse this token while its old pin is
      ;; waiting to be removed under the WAL lock.
      (when (and active pinned *wal-test-after-snapshot-release-mark-hook*)
        (funcall *wal-test-after-snapshot-release-mark-hook* registration))
      ;; Never take WAL while holding the snapshot-count lock: capture takes
      ;; WAL->XID->snapshot-count. The still-published map entry conservatively
      ;; protects WAL until this serialized removal completes.
      (when pinned
        (sb-thread:with-mutex (*wal-lock*)
          (remhash registration *wal-snapshot-retention-pins*)
          (sb-thread:with-mutex (*snapshot-count-lock*)
            (setf (snapshot-registration-wal-retention-lsn registration) nil
                  (snapshot-registration-releasing-p registration) nil))))
      (when active
        (sb-thread:with-mutex (*snapshot-cv-mutex*)
          (sb-thread:condition-broadcast *snapshot-cv*))))
    (values)))

(defmacro with-statement-snapshot ((snapshot acquire-function) &body body)
  "Acquire a registered statement snapshot inside its cleanup scope.

ACQUIRE-FUNCTION is called with a registration token and must return the
registered snapshot. The token is already protected if acquisition is
interrupted after publication but before the function returns."
  (let ((registration (gensym "SNAPSHOT-REGISTRATION-")))
    `(let ((,registration (make-snapshot-registration)))
       (unwind-protect
            (let ((,snapshot (funcall ,acquire-function ,registration)))
              (declare (ignorable ,snapshot))
              (let ((*current-snapshot* ,snapshot))
                ,@body))
         (release-snapshot-registration ,registration)))))

(defun xid-committed-p (xid)
  (or (member xid (list +bootstrap-xid+ +frozen-xid+) :test #'=)
      (eql (clog-get xid) +clog-committed+)))

(defun xid-aborted-p (xid)
  (and (/= xid +bootstrap-xid+)
       (/= xid +frozen-xid+)
       (or (eql (clog-get xid) +clog-aborted+)
           (and (eql (clog-get xid) +clog-in-progress+)
                (not (xid-active-p xid))))))

(defun xid-in-progress-p (xid)
  (and (= (clog-get xid) +clog-in-progress+)
       (xid-active-p xid)))

(defun heap-tuple-visible-p (xmin xmax cid snapshot my-xid)
  "HeapTupleSatisfiesMVCC for the MVP type set."
  (let ((curcid (snapshot-curcid snapshot)))
    (cond
      ;; Own-xid rule first.
      ((and my-xid (= xmin my-xid))
       (and (< cid curcid)
            (not (= xmax my-xid))))
      ((and my-xid (= xmax my-xid))
       nil)
      ;; Insert visibility.
      ((>= xmin (snapshot-xmax snapshot)) nil)
      ((member xmin (snapshot-xip snapshot) :test #'=) nil)
      ((not (xid-committed-p xmin)) nil)
      ;; Delete visibility.
      ((zerop xmax) t)
      ((>= xmax (snapshot-xmax snapshot)) t)
      ((member xmax (snapshot-xip snapshot) :test #'=) t)
      ((xid-committed-p xmax) nil)
      (t t))))

(defun tuple-prunable-p (xmin xmax oldest-xmin)
  "Return whether tuple XIDs are older than the captured removal horizon."
  (or (and (xid-aborted-p xmin)
           (< xmin oldest-xmin))
      (and (not (zerop xmax))
           (xid-committed-p xmax)
           (< xmax oldest-xmin))))
