;;;; support.lisp -- Shared helpers for Lisp-level unit tests.
;;;;
;;;; Run via t/run-unit-tests.sh, which loads the "pg" system first.

(in-package #:pg)

(defvar *test-failures* 0)
(defvar *test-checks* 0)

(defmacro check (form &optional (label nil))
  `(if ,form
       (incf *test-checks*)
       (progn
         (incf *test-failures*)
         (format t "~&FAIL: ~a~%" (or ,label ',form)))))

(defun test-wait-until (predicate &optional (timeout 10.0))
  "Poll PREDICATE for up to TIMEOUT seconds (ten seconds by default)."
  (let ((deadline (+ (get-internal-real-time)
                     (* timeout internal-time-units-per-second))))
    (loop
      (when (funcall predicate)
        (return t))
      (when (>= (get-internal-real-time) deadline)
        (return nil))
      (sleep 0.01))))

(defun test-reset-live-operation-admission ()
  (sb-thread:with-mutex (*live-operation-lock*)
    (setf *live-operation-admission-open* t
          *live-operation-drain-open* nil
          *live-operation-quiescence-owner* nil
          *live-operation-drain-backends* nil
          *live-operation-cleanup-backends* nil
          *live-operation-pending-cancel-requests* nil)
    (clrhash *live-operation-backend-query-tokens*)
    (sb-thread:condition-broadcast *live-operation-cv*))
  (values))

(defun test-read-frontend-message (stream)
  (multiple-value-bind (type-byte length-bytes payload)
      (read-message-frame stream)
    (when type-byte
      (decode-frontend-message type-byte length-bytes payload))))

(defun test-dispatch-query-frame (stream query)
  (let ((payload (frontend-message-payload (test-query-message query))))
    (dispatch-frontend-frame
     stream (char-code #\Q) (u32-to-bytes (+ 4 (length payload))) payload)))
