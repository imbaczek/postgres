;;;; pqformat.lisp -- backend message constructors.

(in-package #:pg)

(defun send-auth-ok (stream)
  (let ((p (make-payload)))
    (payload-push-u32 p 0)              ; AuthenticationOk
    (write-message stream #\R p)))

(defun send-parameter-status (stream name value)
  (let ((p (make-payload)))
    (payload-push-cstring p name)
    (payload-push-cstring p value)
    (write-message stream #\S p)))

(defun send-backend-key-data (stream pid secret)
  (let ((p (make-payload)))
    (payload-push-u32 p pid)
    (payload-push-u32 p secret)
    (write-message stream #\K p)))

(defun send-ready-for-query (stream status)
  (let ((p (make-payload)))
    (payload-push-byte p (char-code status))
    (write-message stream #\Z p)))

(defun send-empty-query-response (stream)
  (write-message stream #\I (make-payload)))

(defun send-command-complete (stream tag)
  (let ((p (make-payload)))
    (payload-push-cstring p tag)
    (write-message stream #\C p)))

(defun send-error-response (stream condition)
  (let ((p (make-payload))
        (sqlstate (if (typep condition 'pg-error)
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
    (vector-push-extend 0 p)
    (write-message stream #\E p)))

(defun send-notice (stream message &optional (severity "NOTICE"))
  (let ((p (make-payload))
        (sqlstate (if (string= severity "NOTICE") "00000" "01000")))
    (flet ((field (code value)
             (payload-push-byte p (char-code code))
             (payload-push-cstring p value)))
      (field #\S severity)
      (field #\V severity)
      (field #\C sqlstate)
      (field #\M message))
    (vector-push-extend 0 p)
    (write-message stream #\N p)))

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
  (let ((p (make-payload)))
    (payload-push-u16 p (length columns))
    (dolist (col columns)
      (payload-push-cstring p (result-column-name col))
      (payload-push-u32 p (result-column-table-oid col))
      (payload-push-u16 p (result-column-attnum col))
      (payload-push-u32 p (result-column-type-oid col))
      (payload-push-u16 p (logand (result-column-typlen col) #xFFFF))
      (payload-push-u32 p (logand (result-column-typmod col) #xFFFFFFFF))
      (payload-push-u16 p (result-column-format col)))
    (write-message stream #\T p)))

(defun send-data-row-text (stream values)
  "VALUES is a list of either strings (already text) or NIL for NULL."
  (let ((p (make-payload)))
    (payload-push-u16 p (length values))
    (dolist (v values)
      (if (null v)
          (payload-push-u32 p #xFFFFFFFF)   ; -1 length
          (let ((bytes (if (stringp v) (string->bytes v) v)))
            (payload-push-u32 p (length bytes))
            (payload-push p bytes))))
    (write-message stream #\D p)))
