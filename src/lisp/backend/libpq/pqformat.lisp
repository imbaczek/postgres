;;;; pqformat.lisp -- backend message constructors.

(in-package #:pg)

(defvar *logical-ddl-event-order-lock*
  (sb-thread:make-mutex :name "logical DDL event order"))

(defvar *logical-ddl-event-order-active-p* nil)

(defvar *logical-ddl-deferred-responses* nil)

(defmacro with-backend-message ((stream message-type payload) &body body)
  "Build PAYLOAD, append BODY to it, then write MESSAGE-TYPE to STREAM."
  `(let ((,payload (make-payload)))
     ,@body
     (write-message ,stream ,message-type ,payload)))

(defun send-auth-ok (stream)
  (with-backend-message (stream #\R p)
    (payload-push-u32 p 0)))             ; AuthenticationOk

(defun send-parameter-status (stream name value)
  (with-backend-message (stream #\S p)
    (payload-push-cstring p name)
    (payload-push-cstring p value)))

(defun send-backend-key-data (stream pid secret)
  (with-backend-message (stream #\K p)
    (payload-push-u32 p pid)
    (payload-push-u32 p secret)))

(defun send-ready-for-query (stream status)
  (with-backend-message (stream #\Z p)
    (payload-push-byte p (char-code status))))

(defun send-empty-query-response (stream)
  (write-message stream #\I (make-payload)))

(defun send-command-complete (stream tag)
  (if *logical-ddl-event-order-active-p*
      (push (list :command-complete stream tag)
            *logical-ddl-deferred-responses*)
      (with-backend-message (stream #\C p)
        (payload-push-cstring p tag))))

(defun send-error-response (stream condition)
  (with-backend-message (stream #\E p)
    (let ((sqlstate (if (typep condition 'pg-error)
                        (pg-error-sqlstate condition)
                        "XX000"))
          (severity (if (typep condition 'pg-error)
                        (pg-error-severity condition)
                        "ERROR"))
          (message (if (typep condition 'pg-error)
                       (pg-error-message condition)
                       (princ-to-string condition))))
      (flet ((field (code value)
               (payload-push-byte p (char-code code))
               (payload-push-cstring p value)))
        (field #\S severity)
        (field #\V severity)
        (field #\C sqlstate)
        (field #\M message)
        (when (and (typep condition 'pg-error) (pg-error-detail condition))
          (field #\D (pg-error-detail condition)))
        (when (and (typep condition 'pg-error) (pg-error-position condition))
          (field #\P (princ-to-string (pg-error-position condition))))
        (when (and (typep condition 'pg-error) (pg-error-hint condition))
          (field #\H (pg-error-hint condition))))
      (vector-push-extend 0 p))))

(defun send-notice (stream message &optional (severity "NOTICE"))
  (if *logical-ddl-event-order-active-p*
      (push (list :notice stream message severity)
            *logical-ddl-deferred-responses*)
      (with-backend-message (stream #\N p)
        (let ((sqlstate (if (string= severity "NOTICE") "00000" "01000")))
          (flet ((field (code value)
                   (payload-push-byte p (char-code code))
                   (payload-push-cstring p value)))
            (field #\S severity)
            (field #\V severity)
            (field #\C sqlstate)
            (field #\M message))
          (vector-push-extend 0 p)))))

(defun flush-logical-ddl-deferred-responses (responses)
  (dolist (response responses)
    (ecase (first response)
      (:command-complete
       (apply #'send-command-complete (rest response)))
      (:notice
       (apply #'send-notice (rest response)))))
  (values))

(defun call-with-logical-ddl-event-order (function)
  "Serialize schema events and return replies to flush after transaction commit."
  (let ((*logical-ddl-event-order-active-p* t)
        (*logical-ddl-deferred-responses* nil)
        (result nil))
    (unwind-protect
         (setf result
               (sb-thread:with-mutex (*logical-ddl-event-order-lock*)
                 (funcall function)))
      ;; DDL responses force output, so return them only after unlocking.
      (setf *logical-ddl-event-order-active-p* nil))
    (values result (nreverse *logical-ddl-deferred-responses*))))

(defun warning-notice (control-string &rest args)
  "Send a WARNING to the current frontend connection, if there is one.

PostgreSQL reports an unknown PID from PG_CANCEL_BACKEND() /
PG_TERMINATE_BACKEND() with ereport(WARNING) and returns false; this is the
closest equivalent."
  (when *output-stream*
    (send-notice *output-stream*
                 (apply #'format nil control-string args)
                 "WARNING")))

;;; A result column description for RowDescription.
(defstruct result-column
  name
  (table-oid 0)
  (attnum 0)
  (type-oid +textoid+)
  (typlen -1)
  (typmod -1)
  (format 0))

(defun send-row-description (stream columns)
  (with-backend-message (stream #\T p)
    (payload-push-u16 p (length columns))
    (dolist (col columns)
      (payload-push-cstring p (result-column-name col))
      (payload-push-u32 p (result-column-table-oid col))
      (payload-push-u16 p (result-column-attnum col))
      (payload-push-u32 p (result-column-type-oid col))
      (payload-push-u16 p (logand (result-column-typlen col) #xFFFF))
      (payload-push-u32 p (logand (result-column-typmod col) #xFFFFFFFF))
      (payload-push-u16 p (result-column-format col)))))

(defun send-data-row-text (stream values)
  "VALUES is a list of either strings (already text) or NIL for NULL."
  (with-backend-message (stream #\D p)
    (payload-push-u16 p (length values))
    (dolist (v values)
      (if (null v)
          (payload-push-u32 p #xFFFFFFFF)   ; -1 length
          (let ((bytes (if (stringp v) (string->bytes v) v)))
            (payload-push-u32 p (length bytes))
            (payload-push p bytes))))))
