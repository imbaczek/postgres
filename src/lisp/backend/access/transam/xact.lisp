;;;; xact.lisp -- transaction state, XID allocation, commit log integration.

(in-package #:pg)

(defstruct backend
  stream
  (pid 0)
  (database "postgres")
  (xid nil)
  (state :idle)                 ; :idle | :in-progress | :aborted
  (explicit nil)
  (command-id 0)
  (snapshot nil)
  (dirty-relations '())
  ;; Interrupt state: the equivalents of QueryCancelPending and the
  ;; backend's own thread, used by pg_cancel_backend()/pg_terminate_backend().
  (executing nil)
  (cancel-pending nil)
  (thread nil))

(defvar *backend* nil)

(defun check-for-interrupts ()
  "Port of PostgreSQL's CHECK_FOR_INTERRUPTS().

A pending PG_CANCEL_BACKEND() request raises 57014 here, at a safe point.
PG_TERMINATE_BACKEND() is delivered as a FATAL condition from
INTERRUPT-THREAD, so it needs no flag."
  (when (and *backend* (backend-cancel-pending *backend*))
    (setf (backend-cancel-pending *backend*) nil)
    (ereport "57014" "canceling statement due to user request")))

;;; ---- pg_control: nextXid, fsynced write-ahead of XID allocation ----

(defun pgdata-path (relative)
  (merge-pathnames relative *pgdata*))

(defun control-path ()
  (pgdata-path "global/pg_control"))

(defun load-next-xid ()
  (with-open-file (s (control-path) :direction :input :if-does-not-exist nil)
    (if s
        (or (parse-integer (read-line s nil "") :junk-allowed t)
            +first-normal-xid+)
        +first-normal-xid+)))

(defvar *xid-batch-end* +first-normal-xid+)

(defun save-next-xid ()
  (ensure-directories-exist (control-path))
  (with-open-file (s (control-path)
                     :direction :output
                     :if-exists :supersede
                     :if-does-not-exist :create
                     :element-type 'character)
    (format s "~a~%" *xid-batch-end*)
    (finish-output s)
    (sb-posix:fsync (sb-sys:fd-stream-fd s)))
  (values))

(defvar *xid-lock* (sb-thread:make-mutex :name "pg xid lock"))
(defvar *next-xid* +first-normal-xid+)
(defvar *active-xids* '())

(defvar *xact-cv* (sb-thread:make-waitqueue :name "pg xact waitqueue"))
(defvar *xact-cv-mutex* (sb-thread:make-mutex :name "pg xact cv mutex"))

(defun broadcast-xact ()
  (sb-thread:with-mutex (*xact-cv-mutex*)
    (sb-thread:condition-broadcast *xact-cv*)))

(defun assign-xid ()
  (sb-thread:with-mutex (*xid-lock*)
    (when (>= *next-xid* *xid-batch-end*)
      (setf *xid-batch-end* (+ *next-xid* 1000))
      (save-next-xid))
    (let ((xid *next-xid*))
      (incf *next-xid*)
      (push xid *active-xids*)
      xid)))

(defun release-xid (xid)
  (sb-thread:with-mutex (*xid-lock*)
    (setf *active-xids* (delete xid *active-xids* :test #'=)))
  (broadcast-xact)
  (values))

(defun xid-active-p (xid)
  (sb-thread:with-mutex (*xid-lock*)
    (and (member xid *active-xids* :test #'=) t)))

(defun wait-for-xid (xid)
  "Block until XID ends. Called without the write lock held."
  (loop
    (unless (xid-active-p xid) (return))
    (check-for-interrupts)
    (sb-thread:with-mutex (*xact-cv-mutex*)
      (when (xid-active-p xid)
        (sb-thread:condition-wait *xact-cv* *xact-cv-mutex* :timeout 1))))
  (values))

(defun make-snapshot (curcid)
  (sb-thread:with-mutex (*xid-lock*)
    (let* ((xmax *next-xid*)
           (xip (copy-list *active-xids*))
           (xmin (if xip (reduce #'min xip) xmax)))
      (make-snapshot-struct :xmin xmin :xmax xmax :xip xip :curcid curcid))))

;;; ---- per-backend transaction state ----

(defun get-current-xid ()
  (or (backend-xid *backend*)
      (setf (backend-xid *backend*) (assign-xid))))

(defun mark-dirty (relfilenode)
  (unless (member relfilenode (backend-dirty-relations *backend*) :test #'eql)
    (push relfilenode (backend-dirty-relations *backend*)))
  (values))

(defun statement-start ()
  (let ((b *backend*))
    (when (eq (backend-state b) :aborted)
      (ereport "25P02"
               "current transaction is aborted, commands ignored until end of transaction block"))
    (when (eq (backend-state b) :idle)
      (setf (backend-state b) :in-progress
            (backend-explicit b) nil
            (backend-command-id b) 0))
    (incf (backend-command-id b))
    (let ((snap (make-snapshot (backend-command-id b))))
      (setf (backend-snapshot b) snap)
      snap)))

(defun commit-transaction ()
  (let ((b *backend*))
    (when (backend-xid b)
      ;; Step 1: make heap and index pages durable.
      (dolist (rel (backend-dirty-relations b))
        (smgr-fsync rel))
      ;; Step 2: log the commit.
      (clog-set (backend-xid b) +clog-committed+ :fsync t)
      (release-xid (backend-xid b)))
    (setf (backend-xid b) nil
          (backend-state b) :idle
          (backend-explicit b) nil
          (backend-command-id b) 0
          (backend-snapshot b) nil
          (backend-dirty-relations b) '()))
  (values))

(defun abort-transaction ()
  (let ((b *backend*))
    (when (backend-xid b)
      (clog-set (backend-xid b) +clog-aborted+ :fsync t)
      (release-xid (backend-xid b)))
    (setf (backend-xid b) nil
          (backend-state b) :idle
          (backend-explicit b) nil
          (backend-command-id b) 0
          (backend-snapshot b) nil
          (backend-dirty-relations b) '()))
  (values))

(defun end-backend-session ()
  "Abort any transaction still open when a client disconnects.

A backend that vanishes mid-transaction must not leave its XID in
*active-xids*; otherwise another session waiting on a conflicting unique
key would block forever, and aborted heap changes would never be
reclaimable."
  (when *backend*
    (when (or (backend-xid *backend*)
              (not (eq (backend-state *backend*) :idle)))
      (abort-transaction))))

(defun transaction-status-char ()
  (case (and *backend* (backend-state *backend*))
    (:in-progress #\T)
    (:aborted #\E)
    (t #\I)))

;;; ---- startup ----

(defun xact-init ()
  (ensure-directories-exist (pgdata-path "global/pg_control"))
  (ensure-directories-exist (pgdata-path "base/dummy"))
  (let ((version-file (pgdata-path "PG_VERSION")))
    (unless (probe-file version-file)
      (with-open-file (s version-file
                         :direction :output
                         :if-exists :supersede
                         :if-does-not-exist :create
                         :element-type 'character)
        (format s "16~%")
        (finish-output s)
        (sb-posix:fsync (sb-sys:fd-stream-fd s)))))
  (setf *xid-batch-end* (load-next-xid)
        *next-xid* (load-next-xid))
  (clog-init)
  (clog-recover *next-xid*)
  (values))
