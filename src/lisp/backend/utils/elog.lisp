;;;; elog.lisp -- error reporting with SQLSTATE codes.

(in-package #:pg)

(define-condition pg-error (error)
  ((sqlstate :initarg :sqlstate :reader pg-error-sqlstate :initform "XX000")
   (message  :initarg :message  :reader pg-error-message  :initform "internal error")
   (detail   :initarg :detail   :reader pg-error-detail   :initform nil)
   (position :initarg :position :reader pg-error-position :initform nil)
   (hint     :initarg :hint     :reader pg-error-hint     :initform nil))
  (:report (lambda (c s)
             (format s "~a: ~a" (pg-error-sqlstate c) (pg-error-message c)))))

(defun ereport (sqlstate control-string &rest args)
  "Signal PG-ERROR, formatting CONTROL-STRING with ARGS.
Trailing keyword arguments :DETAIL, :POSITION, and :HINT set the
corresponding error fields instead of being used as format arguments."
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
    (error 'pg-error
           :sqlstate sqlstate
           :message (if args
                        (apply #'format nil control-string args)
                        control-string)
           :detail detail
           :position position
           :hint hint)))

(defun elog (sqlstate control-string &rest args)
  (apply #'ereport sqlstate control-string args))
