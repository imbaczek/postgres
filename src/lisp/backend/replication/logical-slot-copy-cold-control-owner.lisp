(in-package #:pg)

(defstruct (logical-slot-copy-cold-control-owner
            (:include logical-slot-copy-catalog-owner)
            (:constructor %make-logical-slot-copy-cold-control-owner)
            (:print-function physical-libpq-print-private-object)))

(defstruct (logical-slot-copy-cold-control-attempt
            (:constructor %make-logical-slot-copy-cold-control-attempt)
            (:print-function physical-libpq-print-private-object))
  (owner nil :read-only t) (context nil :read-only t)
  (binding nil :read-only t) (source-check nil :read-only t)
  (operations nil :read-only t) (failure-operation nil :read-only t)
  (mark-started nil :read-only t))

(defvar *logical-slot-copy-cold-control-attempts* nil)

(defun logical-slot-copy-cold-control-revoke (attempt)
  (when (and (logical-slot-copy-cold-control-attempt-p attempt)
             (functionp (logical-slot-copy-cold-control-attempt-failure-operation attempt)))
    (funcall (logical-slot-copy-cold-control-attempt-failure-operation attempt) :revoke attempt)))

(defun logical-slot-copy-cold-control-record (attempt)
  (unless (and (logical-slot-copy-cold-control-attempt-p attempt)
               (eq attempt (cdr (assoc (logical-slot-copy-cold-control-attempt-owner attempt)
                                       *logical-slot-copy-cold-control-attempts* :test #'eq)))
               (not (funcall (logical-slot-copy-cold-control-attempt-failure-operation attempt)
                             :failedp attempt)))
    (error "cold control publication scope ended or failed"))
  (logical-slot-copy-check-input (logical-slot-copy-cold-control-attempt-owner attempt)
                                 (logical-slot-copy-cold-control-attempt-context attempt))
  (funcall (logical-slot-copy-cold-control-attempt-operations attempt) :check-binding attempt)
  attempt)

(defmacro with-logical-slot-copy-cold-control-guard ((attempt) &body body)
  (let ((record (gensym "ATTEMPT-")) (complete (gensym "COMPLETE-")))
    `(let ((,record ,attempt) (,complete nil))
       (unwind-protect
            (multiple-value-prog1
                (progn (logical-slot-copy-cold-control-record ,record) ,@body)
              (setf ,complete t))
         (unless ,complete (logical-slot-copy-cold-control-revoke ,record))))))
