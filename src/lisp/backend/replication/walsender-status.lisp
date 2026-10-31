;;; Process-local sender status for replication monitoring.
;;;
;;; The registry follows backend ownership; each stream resets its own progress.
;;; Status is observational and does not grant synchronous commit guarantees.
(in-package #:pg)

(defstruct wal-sender-status
  "Mutable status owned by one replication backend and protected by the sender lock."
  backend index (state "startup") sent-lsn write-lsn flush-lsn replay-lsn reply-time)

(defvar *wal-sender-lock* (sb-thread:make-mutex :name "pg walsender status"))
(defvar *wal-sender-registry* (make-hash-table :test #'eq))

(defun wal-sender-next-index-under-lock ()
  (let ((used (make-hash-table :test #'eql)))
    (maphash (lambda (backend sender)
               (declare (ignore backend))
               (setf (gethash (wal-sender-status-index sender) used) t))
             *wal-sender-registry*)
    (loop for index from 0 unless (gethash index used) return index)))

(defun wal-sender-snapshot-under-lock (sender)
  (let ((backend (wal-sender-status-backend sender)))
    (list :pid (backend-pid backend)
          :application-name (copy-seq (or (backend-application-name backend) ""))
          :state (copy-seq (wal-sender-status-state sender))
          :sent-lsn (wal-sender-status-sent-lsn sender)
          :write-lsn (wal-sender-status-write-lsn sender)
          :flush-lsn (wal-sender-status-flush-lsn sender)
          :replay-lsn (wal-sender-status-replay-lsn sender)
          :reply-time (wal-sender-status-reply-time sender))))

(defun wal-sender-register (backend)
  "Register one status token for a replication backend under the sender lock."
  (unless (and (backend-p backend)
               (member (backend-replication-mode backend) '(:physical :database)))
    (error "walsender status requires a replication backend"))
  (let ((sender (make-wal-sender-status :backend backend)))
    (sb-thread:with-mutex (*wal-sender-lock*)
      (when (gethash backend *wal-sender-registry*)
        (error "backend already owns walsender status"))
      (sb-sys:without-interrupts
        (setf (wal-sender-status-index sender) (wal-sender-next-index-under-lock)
              (gethash backend *wal-sender-registry*) sender)))
    sender))

(defun wal-sender-unregister (backend &optional (expected nil expected-p))
  "Remove the backend's status; an expected token cannot remove a replacement."
  (sb-sys:without-interrupts
    (sb-thread:with-mutex (*wal-sender-lock*)
      (when (or (not expected-p) (eq expected (gethash backend *wal-sender-registry*)))
        (remhash backend *wal-sender-registry*))))
  (values))

(defun wal-sender-for-backend (&optional (backend *backend*))
  (sb-thread:with-mutex (*wal-sender-lock*)
    (gethash backend *wal-sender-registry*)))

(defun wal-sender-current-under-lock-p (sender)
  (and (wal-sender-status-p sender)
       (eq sender (gethash (wal-sender-status-backend sender) *wal-sender-registry*))))

(defun wal-sender-set-state (sender state)
  "Publish a valid state only while SENDER is the backend's current token."
  (unless (member state '("startup" "backup" "catchup" "streaming" "stopping") :test #'equal)
    (error "invalid walsender state"))
  (sb-thread:with-mutex (*wal-sender-lock*)
    (when (wal-sender-current-under-lock-p sender)
      (sb-sys:without-interrupts
        (setf (wal-sender-status-state sender) (copy-seq state)))
      t)))

(defun wal-sender-note-sent (sender sent)
  "Advance the current sender's sent frontier monotonically."
  (unless (typep sent '(unsigned-byte 64)) (error "invalid sent WAL position"))
  (sb-thread:with-mutex (*wal-sender-lock*)
    (when (wal-sender-current-under-lock-p sender)
      (sb-sys:without-interrupts
        (setf (wal-sender-status-sent-lsn sender)
              (max sent (or (wal-sender-status-sent-lsn sender) 0))))
      t)))

(defun wal-sender-note-reply (sender write flush replay reply-time)
  "Publish write, flush, replay and reply time from one received feedback message.
These fields are independent and do not infer apply from flush."
  (unless (and (every (lambda (value) (typep value '(unsigned-byte 64)))
                      (list write flush replay))
               (typep reply-time '(signed-byte 64)))
    (error "invalid walsender feedback"))
  (sb-thread:with-mutex (*wal-sender-lock*)
    (when (wal-sender-current-under-lock-p sender)
      (sb-sys:without-interrupts
        (setf (wal-sender-status-write-lsn sender) write
              (wal-sender-status-flush-lsn sender) flush
              (wal-sender-status-replay-lsn sender) replay
              (wal-sender-status-reply-time sender) reply-time))
      t)))

(defun wal-sender-note-status (sender status)
  (wal-sender-note-reply sender (getf status :write) (getf status :flush)
                         (getf status :apply) (getf status :reply-time 0)))

(defvar *wal-sender* nil)

(defun call-with-wal-sender-stream (function)
  "Capture the backend's registration token for the stream body and reset progress.
Feedback and cleanup use that token, so they cannot affect a replacement registration."
  (let* ((sender (wal-sender-for-backend))
         (*wal-sender* sender))
    (sb-sys:without-interrupts
      (unwind-protect
           (sb-sys:with-local-interrupts
             (sb-thread:with-mutex (*wal-sender-lock*)
               (when (wal-sender-current-under-lock-p sender)
                 (sb-sys:without-interrupts
                   (setf (wal-sender-status-state sender) "catchup"
                         (wal-sender-status-sent-lsn sender) nil
                         (wal-sender-status-write-lsn sender) nil
                         (wal-sender-status-flush-lsn sender) nil
                         (wal-sender-status-replay-lsn sender) nil
                         (wal-sender-status-reply-time sender) nil))))
             (funcall function))
        (wal-sender-set-state sender "startup")))))

(defun wal-sender-snapshots ()
  "Copy registered sender fields under the lock for a stable monitoring result."
  (sb-thread:with-mutex (*wal-sender-lock*)
    (sort (loop for sender being the hash-values of *wal-sender-registry*
                collect (wal-sender-snapshot-under-lock sender))
          #'< :key (lambda (row) (getf row :pid)))))
