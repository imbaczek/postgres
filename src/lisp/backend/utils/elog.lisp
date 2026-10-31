;;;; elog.lisp -- error reporting with SQLSTATE codes.

(in-package #:pg)

;;; Frontend connection that NOTICE/WARNING messages go to.  PostgreSQL keeps
;;; the equivalent in a global error context; admin functions such as
;;; PG_TERMINATE_BACKEND() need to emit warnings without a stream argument.
(defvar *output-stream* nil)

(defmacro with-error-cleanup ((condition cleanup) &body body)
  "Run CLEANUP best-effort and re-signal the original ERROR from BODY."
  `(handler-case
       (progn ,@body)
     (error (,condition)
       (ignore-errors ,cleanup)
       (error ,condition))))

(define-condition pg-error (error)
  ((sqlstate :initarg :sqlstate :reader pg-error-sqlstate :initform "XX000")
   (message  :initarg :message  :reader pg-error-message  :initform "internal error")
   (severity :initarg :severity :reader pg-error-severity :initform "ERROR")
   (detail   :initarg :detail   :reader pg-error-detail   :initform nil)
   (position :initarg :position :reader pg-error-position :initform nil)
   (hint     :initarg :hint     :reader pg-error-hint     :initform nil))
  (:report (lambda (c s)
             (format s "~a: ~a" (pg-error-sqlstate c) (pg-error-message c)))))

(defun fatal-error-p (condition)
  "True if CONDITION is a PG-ERROR that must end the connection."
  (or (and (fboundp 'current-backend-wal-failed-p)
           (funcall 'current-backend-wal-failed-p))
      (and (typep condition 'pg-error)
           (string= (pg-error-severity condition) "FATAL"))))

(defmacro handler-case-nonfatal (form (condition &body handler-body))
  "Handle ERROR from FORM, re-signaling fatal PG errors before HANDLER-BODY."
  `(handler-case ,form
     (error (,condition)
       (if (fatal-error-p ,condition)
           (error ,condition)
           (progn ,@handler-body)))))

(defun make-pg-error (severity sqlstate control-string args)
  "Build, but do not signal, a PG-ERROR.

Trailing keyword arguments :DETAIL, :POSITION, and :HINT set the corresponding
error fields instead of being used as format arguments."
  (let ((detail nil)
        (position nil)
        (hint nil)
        (split (position-if (lambda (a)
                              (and (keywordp a)
                                   (member a '(:detail :position :hint))))
                            args)))
    (when split
      (let ((tail (nthcdr split args)))
        (setf args (subseq args 0 split))
        (loop for (k v) on tail by #'cddr
              do (case k
                   (:detail (setf detail v))
                   (:position (setf position v))
                   (:hint (setf hint v))))))
    (make-condition 'pg-error
                    :sqlstate sqlstate
                    :severity severity
                    :message (if args
                                 (apply #'format nil control-string args)
                                 control-string)
                    :detail detail
                    :position position
                    :hint hint)))

(defun ereport (sqlstate control-string &rest args)
  "Signal PG-ERROR at severity ERROR."
  (error (make-pg-error "ERROR" sqlstate control-string args)))

(defun ereport-fatal (sqlstate control-string &rest args)
  "Signal PG-ERROR at severity FATAL.

FATAL errors are not caught by the statement-level handlers: they unwind to
the connection loop, which sends the error and closes the connection, the way
PostgreSQL's elog(FATAL) terminates a backend."
  (error (make-pg-error "FATAL" sqlstate control-string args)))

(defun elog (sqlstate control-string &rest args)
  (apply #'ereport sqlstate control-string args))
