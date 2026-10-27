(in-package #:pg)

(defvar *application-name-fatal-backend* nil)
(defvar *application-name-fatal-finish* nil)
(defvar *application-name-fatal-abort* nil)
(defvar *application-name-fatal-xid* nil)
(defvar *application-name-fatal-aborts* 0)

(defun application-name-install-fatal (pid)
  (assert (null *application-name-fatal-finish*))
  (setf *application-name-fatal-backend* (lookup-backend pid)
        *application-name-fatal-finish* (symbol-function 'finish-backend-transaction)
        *application-name-fatal-abort* (symbol-function 'abort-transaction)
        *application-name-fatal-xid* nil
        *application-name-fatal-aborts* 0)
  (assert *application-name-fatal-backend*)
  (let ((finish *application-name-fatal-finish*)
        (abort *application-name-fatal-abort*))
    (setf (symbol-function 'finish-backend-transaction)
          (lambda (backend commit-p)
            (let ((xid (backend-xid backend)))
              (funcall finish backend commit-p)
              (when (and commit-p (eq backend *application-name-fatal-backend*))
                (assert (and xid (> xid 2)))
                (setf *application-name-fatal-xid* xid)
                (sb-thread:interrupt-thread
                 sb-thread:*current-thread*
                 (lambda () (ereport-fatal "57P01" "application-name post-commit test"))))))
          (symbol-function 'abort-transaction)
          (lambda ()
            (when (eq *backend* *application-name-fatal-backend*)
              (incf *application-name-fatal-aborts*))
            (funcall abort))))
  t)

(defun application-name-verify-fatal ()
  (assert *application-name-fatal-xid*)
  (assert (= +clog-committed+ (clog-get *application-name-fatal-xid*)))
  (assert (zerop *application-name-fatal-aborts*))
  (assert (eq :idle (backend-state *application-name-fatal-backend*)))
  (assert (null (backend-settings-transaction-start *application-name-fatal-backend*)))
  (assert (null (lookup-backend (backend-pid *application-name-fatal-backend*))))
  t)

(defun application-name-restore-fatal ()
  (when *application-name-fatal-finish*
    (setf (symbol-function 'finish-backend-transaction) *application-name-fatal-finish*
          (symbol-function 'abort-transaction) *application-name-fatal-abort*
          *application-name-fatal-finish* nil
          *application-name-fatal-abort* nil))
  t)
