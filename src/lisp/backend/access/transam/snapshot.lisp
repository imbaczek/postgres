;;;; snapshot.lisp -- per-statement snapshots and MVCC visibility.

(in-package #:pg)

(defstruct (snapshot (:constructor make-snapshot-struct
                         (&key xmin xmax xip curcid)))
  xmin
  xmax
  xip
  curcid)

(defvar *current-snapshot* nil)

;;; Number of statements currently scanning under a snapshot. Pruning a
;;; committed-dead tuple is safe only when no other snapshot can still see
;;; it; under READ COMMITTED a statement takes a fresh snapshot, so the
;;; conservative test is "only my snapshot is active".
(defvar *snapshot-count-lock* (sb-thread:make-mutex :name "pg snapshot count"))
(defvar *active-snapshots* 0)
(defvar *snapshot-cv* (sb-thread:make-waitqueue :name "pg snapshot waitqueue"))
(defvar *snapshot-cv-mutex* (sb-thread:make-mutex :name "pg snapshot cv mutex"))

(declaim (ftype function xid-active-p))

(defun note-snapshot-start ()
  (sb-thread:with-mutex (*snapshot-count-lock*)
    (incf *active-snapshots*)))

(defun note-snapshot-end ()
  (sb-thread:with-mutex (*snapshot-count-lock*)
    (decf *active-snapshots*))
  (sb-thread:with-mutex (*snapshot-cv-mutex*)
    (sb-thread:condition-broadcast *snapshot-cv*)))

(defmacro with-active-snapshot (&body body)
  `(progn
     (unwind-protect (progn ,@body)
       (note-snapshot-end))))

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

(defun tuple-prunable-p (xmin xmax)
  "PG's prune predicate, simplified for a no-VACUUM MVP. Never prune a
tuple whose xmax is still in progress (it may abort and resurrect the
tuple), and never prune while another snapshot could still see it."
  (or (xid-aborted-p xmin)
      (and (not (zerop xmax))
           (xid-committed-p xmax)
           (<= *active-snapshots* 1))))
