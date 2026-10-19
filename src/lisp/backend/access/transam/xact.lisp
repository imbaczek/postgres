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

;;; Relation access registrations let relation DDL wait for active transactions
;;; that have used a table, while preventing new accesses until it completes.
;;; Registrations live until the XID commits or aborts.
(defvar *relation-lock-mutex* (sb-thread:make-mutex :name "pg relation lock"))
(defvar *relation-lock-cv* (sb-thread:make-waitqueue :name "pg relation waitqueue"))
(defvar *relation-builders* (make-hash-table :test #'eql))
(defvar *relation-reader-blockers* (make-hash-table :test #'eql))
(defvar *relation-build-groups* (make-hash-table :test #'eq))
(defvar *relation-writers* (make-hash-table :test #'eql))
(defvar *relation-readers* (make-hash-table :test #'eql))

(defun broadcast-xact ()
  (sb-thread:with-mutex (*xact-cv-mutex*)
    (sb-thread:condition-broadcast *xact-cv*)))

(defun condition-wait-with-timeout (waitqueue mutex)
  "Wait up to one second, returning with MUTEX held even after a timeout."
  (let ((notified (sb-thread:condition-wait waitqueue mutex :timeout 1)))
    ;; SBCL returns NIL on timeout without reacquiring MUTEX.
    (unless notified
      (sb-thread:grab-mutex mutex))
    notified))

(defun assign-xid ()
  (sb-thread:with-mutex (*xid-lock*)
    (when (>= *next-xid* *xid-batch-end*)
      (setf *xid-batch-end* (+ *next-xid* 1000))
      (save-next-xid))
    (let ((xid *next-xid*))
      (incf *next-xid*)
      (push xid *active-xids*)
      xid)))

(defun relation-build-group-has-user-p (token xid writers-p readers-p)
  (let ((group (gethash token *relation-build-groups*)))
    (and group
         (some (lambda (relfilenode)
                 (or (and writers-p
                          (member xid (gethash relfilenode *relation-writers*)
                                  :test #'=))
                     (and readers-p
                          (member xid (gethash relfilenode *relation-readers*)
                                  :test #'=))))
               (first group)))))

(defun register-relation-writer (relfilenode xid)
  "Register XID as a writer, waiting if an index build owns the relation."
  (sb-thread:with-mutex (*relation-lock-mutex*)
    (loop
        (let* ((writers (gethash relfilenode *relation-writers*))
               (token (gethash relfilenode *relation-builders*))
               (group (gethash token *relation-build-groups*)))
        (when (or (null (gethash relfilenode *relation-builders*))
                  (member xid writers :test #'=)
                    (relation-build-group-has-user-p
                     token xid t (and group (second group))))
          (pushnew xid (gethash relfilenode *relation-writers*) :test #'=)
          (return)))
      (check-for-interrupts)
      (condition-wait-with-timeout *relation-lock-cv* *relation-lock-mutex*)))
  (values))

(defun register-relation-reader (relfilenode xid)
  "Register XID as a reader, waiting if relation DDL excludes readers."
  (sb-thread:with-mutex (*relation-lock-mutex*)
    (loop
        (let* ((readers (gethash relfilenode *relation-readers*))
               (token (gethash relfilenode *relation-reader-blockers*)))
        (when (or (null (gethash relfilenode *relation-reader-blockers*))
                  (member xid readers :test #'=)
                  (member xid (gethash relfilenode *relation-writers*) :test #'=))
          (pushnew xid (gethash relfilenode *relation-readers*) :test #'=)
            (return))
          ;; Multi-relation DDL waits for existing users on every member. Let
          ;; those transactions finish their work on the rest of that group.
          (when (relation-build-group-has-user-p token xid t t)
            (pushnew xid (gethash relfilenode *relation-readers*) :test #'=)
            (return)))
      (check-for-interrupts)
      (condition-wait-with-timeout *relation-lock-cv* *relation-lock-mutex*)))
  (values))

(defun relation-build-user (relfilenodes own-xid wait-readers-p)
  (some (lambda (relfilenode)
          (or (find-if (lambda (xid)
                         (or (null own-xid) (/= xid own-xid)))
                       (gethash relfilenode *relation-writers*))
              (when wait-readers-p
                (find-if (lambda (xid)
                           (or (null own-xid) (/= xid own-xid)))
                         (gethash relfilenode *relation-readers*)))))
        relfilenodes))

(defun acquire-relation-build-locks (relfilenodes &optional own-xid wait-readers-p)
  "Block new users as one group, then wait for all active users to finish."
  (let ((relfilenodes (sort (remove-duplicates (copy-list relfilenodes) :test #'=)
                            #'<))
        (token (gensym "RELATION-BUILD-")))
    (when (null relfilenodes)
      (return-from acquire-relation-build-locks nil))
    (sb-thread:with-mutex (*relation-lock-mutex*)
      (loop while (some (lambda (relfilenode)
                          (gethash relfilenode *relation-builders*))
                        relfilenodes)
            do (check-for-interrupts)
               (condition-wait-with-timeout *relation-lock-cv*
                                            *relation-lock-mutex*))
      (setf (gethash token *relation-build-groups*)
            (list relfilenodes wait-readers-p))
      (dolist (relfilenode relfilenodes)
        (setf (gethash relfilenode *relation-builders*) token)
        (when wait-readers-p
          (setf (gethash relfilenode *relation-reader-blockers*) token))))
    (handler-case
        (progn
          (loop
            (let ((user (sb-thread:with-mutex (*relation-lock-mutex*)
                          (relation-build-user relfilenodes own-xid
                                               wait-readers-p))))
              (unless user (return))
              (wait-for-xid user)))
          token)
      (error (e)
          (release-relation-build-locks relfilenodes token)
        (error e)))))

(defun release-relation-build-locks (relfilenodes token)
  (sb-thread:with-mutex (*relation-lock-mutex*)
    (dolist (relfilenode relfilenodes)
      (when (eq (gethash relfilenode *relation-builders*) token)
        (remhash relfilenode *relation-builders*)
        (when (eq (gethash relfilenode *relation-reader-blockers*) token)
          (remhash relfilenode *relation-reader-blockers*))))
    (when token
      (remhash token *relation-build-groups*))
    (sb-thread:condition-broadcast *relation-lock-cv*))
  (values))

(defun acquire-relation-build-lock (relfilenode &optional own-xid wait-readers-p)
  (acquire-relation-build-locks (list relfilenode) own-xid wait-readers-p))

(defun release-relation-build-lock (relfilenode token)
  (release-relation-build-locks (list relfilenode) token))

(defmacro with-relation-build-lock ((relfilenode &optional wait-readers-p) &body body)
  (let ((rel (gensym "RELATION-"))
        (xid (gensym "OWN-XID-"))
        (token (gensym "BUILD-TOKEN-")))
    `(let* ((,rel ,relfilenode)
              (,xid (and *backend* (backend-xid *backend*)))
              (,token (acquire-relation-build-lock ,rel ,xid ,wait-readers-p)))
       (unwind-protect
            (progn ,@body)
         (release-relation-build-lock ,rel ,token)))))

(defmacro with-relation-build-locks ((relfilenodes &optional wait-readers-p) &body body)
  "Acquire relation build gates as one group and release them on exit."
  (let ((rels (gensym "RELATIONS-"))
        (xid (gensym "OWN-XID-"))
        (token (gensym "BUILD-TOKEN-")))
    `(let* ((,rels (sort (remove-duplicates (copy-list ,relfilenodes) :test #'=)
                         #'<))
            (,xid (and *backend* (backend-xid *backend*)))
            (,token (acquire-relation-build-locks ,rels ,xid ,wait-readers-p)))
       (unwind-protect
            (progn ,@body)
         (release-relation-build-locks ,rels ,token)))))

(defun release-relation-writer-xid (xid)
  "Forget XID's relation accesses atomically with its removal from active XIDs."
  ;; Keep the relation gate closed until the XID is no longer in snapshots.
  ;; No path takes these two locks in the opposite order.
  (sb-thread:with-mutex (*relation-lock-mutex*)
    (sb-thread:with-mutex (*xid-lock*)
      (setf *active-xids* (delete xid *active-xids* :test #'=)))
      (dolist (registrations (list *relation-writers* *relation-readers*))
        (let ((empty-relations '()))
          (maphash (lambda (relfilenode xids)
                     (let ((remaining (delete xid xids :test #'=)))
                       (if remaining
                           (setf (gethash relfilenode registrations) remaining)
                           (push relfilenode empty-relations))))
                   registrations)
          (dolist (relfilenode empty-relations)
            (remhash relfilenode registrations))))
    (sb-thread:condition-broadcast *relation-lock-cv*))
  (values))

(defun release-xid (xid)
  (release-relation-writer-xid xid)
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
        (condition-wait-with-timeout *xact-cv* *xact-cv-mutex*))))
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

(defun refresh-statement-snapshot ()
  "Take a fresh snapshot after DDL has waited for prior table writers."
  (let ((snapshot (make-snapshot (backend-command-id *backend*))))
    (setf (backend-snapshot *backend*) snapshot)
    snapshot))

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

(defun reset-backend-transaction (b)
  (setf (backend-xid b) nil
        (backend-state b) :idle
        (backend-explicit b) nil
        (backend-command-id b) 0
        (backend-snapshot b) nil
        (backend-dirty-relations b) '())
  (values))

(defun commit-transaction ()
  (let ((b *backend*))
    (if (backend-xid b)
        (progn
          ;; Step 1: make heap and index pages durable.
          (dolist (rel (backend-dirty-relations b))
            (smgr-fsync rel))
          ;; Step 2: log the commit.  The CLog write and the reset of the
          ;; backend's xid must not be split by a terminate/FATAL interrupt:
          ;; otherwise END-BACKEND-SESSION could still see the xid and abort an
          ;; already-durable commit.
          (sb-sys:without-interrupts
            (clog-set (backend-xid b) +clog-committed+ :fsync t)
            (release-xid (backend-xid b))
            (reset-backend-transaction b)))
        (reset-backend-transaction b)))
  (values))

(defun abort-transaction ()
  (let ((b *backend*))
    (sb-sys:without-interrupts
      (when (backend-xid b)
        ;; Never downgrade a commit that already reached the CLog.
        (when (= (clog-get (backend-xid b)) +clog-in-progress+)
          (clog-set (backend-xid b) +clog-aborted+ :fsync t))
        (release-xid (backend-xid b)))
      (reset-backend-transaction b)))
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
