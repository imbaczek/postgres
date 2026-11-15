(in-package #:pg)

(defstruct (logical-wal-record-provider
            (:constructor %make-logical-wal-record-provider)
            (:print-function (lambda (object stream depth)
                               (declare (ignore object depth))
                               (write-string "#<LOGICAL-WAL-RECORD-PROVIDER>" stream))))
  (reader nil :read-only t) (guard nil :read-only t)
  (thread sb-thread:*current-thread* :read-only t)
  (mutex *wal-lock* :read-only t) (active t) (failed nil) (busy nil))

(defvar *logical-wal-record-providers* nil)

(defun logical-wal-record-provider-check (provider)
  (assert (sb-thread:holding-mutex-p *wal-lock*))
  (unless (and (logical-wal-record-provider-p provider)
               (eq sb-thread:*current-thread* (logical-wal-record-provider-thread provider))
               (eq *wal-lock* (logical-wal-record-provider-mutex provider))
               (logical-wal-record-provider-active provider)
               (not (logical-wal-record-provider-failed provider))
               (member provider *logical-wal-record-providers* :test #'eq))
    (error "logical WAL record provider is outside its source scope"))
  (let ((complete nil))
    (unwind-protect
         (progn
           (unless (funcall (logical-wal-record-provider-guard provider))
             (error "logical WAL record provider lost its source proof"))
           (setf complete t))
      (unless complete (setf (logical-wal-record-provider-failed provider) t))))
  t)

(defun call-with-logical-wal-record-provider (reader guard function)
  (unless (and (functionp reader) (functionp guard) (functionp function))
    (error "invalid logical WAL record provider scope"))
  (let* ((provider (%make-logical-wal-record-provider :reader reader :guard guard))
         (*logical-wal-record-providers* (cons provider *logical-wal-record-providers*)))
    (unwind-protect
         (progn
           (sb-thread:with-mutex (*wal-lock*) (logical-wal-record-provider-check provider))
           (funcall function provider))
      (sb-sys:without-interrupts (setf (logical-wal-record-provider-active provider) nil)))))