;;;; snapshot.lisp -- per-statement snapshots and MVCC visibility.

(in-package #:pg)

(defstruct (snapshot (:constructor make-snapshot-struct
                         (&key xmin xmax xip curcid)))
  xmin
  xmax
  xip
  curcid)

(defvar *current-snapshot* nil)

;;; Active snapshot xmin values bound the oldest tuple version a vacuum may
;;; remove. The count remains separate because XID wraparound waits for every
;;; statement snapshot to drain.
(defvar *snapshot-count-lock* (sb-thread:make-mutex :name "pg snapshot count"))
(defvar *active-snapshots* 0)
(defvar *active-snapshot-set* (make-hash-table :test #'eq))
(defvar *snapshot-cv* (sb-thread:make-waitqueue :name "pg snapshot waitqueue"))
(defvar *snapshot-cv-mutex* (sb-thread:make-mutex :name "pg snapshot cv mutex"))

(declaim (ftype function xid-active-p))

(defun note-snapshot-start ()
  (sb-thread:with-mutex (*snapshot-count-lock*)
    (incf *active-snapshots*)))

(defun register-active-snapshot (snapshot)
  "Publish SNAPSHOT's xmin while the caller still holds the XID lock."
  (sb-thread:with-mutex (*snapshot-count-lock*)
    (setf (gethash snapshot *active-snapshot-set*) t))
  snapshot)

(defun oldest-active-snapshot-xmin ()
  (sb-thread:with-mutex (*snapshot-count-lock*)
    (let ((oldest nil))
      (maphash (lambda (snapshot present)
                 (declare (ignore present))
                 (let ((xmin (snapshot-xmin snapshot)))
                   (when (or (null oldest) (< xmin oldest))
                     (setf oldest xmin))))
               *active-snapshot-set*)
      oldest)))

(defun note-snapshot-end (&optional snapshot)
  (sb-thread:with-mutex (*snapshot-count-lock*)
    (when snapshot
      (remhash snapshot *active-snapshot-set*))
    (when (plusp *active-snapshots*)
      (decf *active-snapshots*)))
  (sb-thread:with-mutex (*snapshot-cv-mutex*)
    (sb-thread:condition-broadcast *snapshot-cv*)))

(defmacro with-active-snapshot ((snapshot) &body body)
  (let ((snapshot-value (gensym "SNAPSHOT-")))
    `(let ((,snapshot-value ,snapshot))
       (unwind-protect (progn ,@body)
         (note-snapshot-end ,snapshot-value)))))

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
