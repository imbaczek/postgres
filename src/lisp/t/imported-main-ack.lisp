(in-package #:pg)

(defvar *imported-main-ack-job* nil)
(defvar *imported-main-ack-lock* (sb-thread:make-mutex :name "imported main ACK test"))

(defun imported-main-ack-selected ()
  (logical-slot-continuation-decode
   (read-bounded-regular-file
    (merge-pathnames "pg_logical_continuation/selector" *pgdata*)
    (logical-slot-continuation-prepared-maximum-bytes))))

(defun imported-main-ack-barrier (directory point)
  (let ((reached (merge-pathnames "reached" directory))
        (release (merge-pathnames "release" directory))
        (deadline (+ (get-internal-real-time) (* 30 internal-time-units-per-second))))
    (with-open-file (stream reached :direction :output :if-exists :error)
      (format stream "~s~%" point))
    (loop until (probe-file release)
          do (when (> (get-internal-real-time) deadline)
               (error "imported ACK test barrier timed out"))
             (sleep 0.005))))

(defun imported-main-ack-run (name target directory point)
  ;; Claim the real typed slot and ACK under the worker's live admission.
  ;; Check alpha's new generation, beta's unchanged state and the retained fork.
  (destructuring-bind (slot group generation beta beta-bytes beta-file-bytes beta-facts fork)
      (sb-thread:with-mutex (*wal-lock*)
        (let* ((slot (gethash name *wal-replication-slots*))
               (group (gethash *wal-replication-slots* *logical-slot-continuation-protection-registries*))
               (beta (gethash "beta" *wal-replication-slots*)))
          (assert (and slot group))
          (list slot group (getf (imported-main-ack-selected) :generation) beta
                (and beta (logical-slot-state-encode beta))
                (and beta (read-bounded-regular-file (wal-replication-slot-path "beta") +slot-state-maximum-bytes+))
                (and beta (physical-image-file-facts -100 (namestring (wal-replication-slot-path "beta"))))
                (gethash group *wal-snapshot-retention-pins*))))
    (let ((reached 0)
          (*logical-slot-continuation-writer-test-hook* nil))
      (setf *logical-slot-continuation-writer-test-hook*
            (lambda (current writer)
              (declare (ignore writer))
              (when (eq point current)
                (incf reached)
                (imported-main-ack-barrier directory current))))
      (assert fork)
      (when beta (assert (equalp beta-bytes beta-file-bytes)))
      (call-with-logical-replication-slot
       name
       (lambda (owned)
         (assert (eq slot owned))
         (assert (> target (wal-replication-slot-confirmed-flush-lsn slot)))
         (assert (eq slot (logical-slot-continuation-native-acknowledge
                           name target *wal-logical-slot-owner*)))))
      (assert (= reached 1))
      (sb-thread:with-mutex (*wal-lock*)
        (assert (eq group (gethash *wal-replication-slots* *logical-slot-continuation-protection-registries*)))
        (assert (= fork (gethash group *wal-snapshot-retention-pins*)))
        (assert (logical-slot-continuation-protection-call group :check))
        (assert (not (logical-slot-continuation-protection-call group :uncertain)))
        (assert (= target (wal-replication-slot-confirmed-flush-lsn slot)
                   (wal-replication-slot-restart-lsn slot)))
        (assert (= *wal-insert-timeline* (wal-replication-slot-confirmed-timeline slot)
                   (wal-replication-slot-restart-timeline slot)))
        (assert (= (1+ generation) (getf (imported-main-ack-selected) :generation)))
        (assert (equalp (logical-slot-state-encode slot)
                        (read-bounded-regular-file (wal-replication-slot-path name) +slot-state-maximum-bytes+)))
        (when beta
          (assert (equalp beta-bytes (logical-slot-state-encode beta)))
          (assert (equalp beta-file-bytes
                          (read-bounded-regular-file (wal-replication-slot-path "beta") +slot-state-maximum-bytes+)))
          (assert (equal beta-facts
                         (physical-image-file-facts -100 (namestring (wal-replication-slot-path "beta")))))))
      (list :generation (1+ generation) :confirmed target :fork fork :barriers reached))))

(defun imported-main-ack-start (name target directory point)
  (unless (member point '(:lifetime-acquired :before-intent))
    (error "unsupported imported ACK test boundary"))
  (sb-thread:with-mutex (*imported-main-ack-lock*)
    (when (and *imported-main-ack-job* (eq :running (getf *imported-main-ack-job* :state)))
      (error "imported ACK test worker is still running"))
    (setf *imported-main-ack-job* (list :state :running)))
  (let ((thread
          (sb-thread:make-thread
           (lambda ()
             (let ((result nil) (failure nil))
               (handler-case
                   (setf result (with-live-operation
                                  (imported-main-ack-run name target (pathname directory) point)))
                 (error (condition) (setf failure condition)))
               (sb-thread:with-mutex (*imported-main-ack-lock*)
                 (setf (getf *imported-main-ack-job* :result) result
                       (getf *imported-main-ack-job* :failure) failure
                       (getf *imported-main-ack-job* :state) (if failure :failed :done)))
               (with-open-file (stream (merge-pathnames "done" (pathname directory))
                                       :direction :output :if-exists :error)
                 (write-line "done" stream))))
           :name "imported main ACK test")))
    (sb-thread:with-mutex (*imported-main-ack-lock*)
      (setf (getf *imported-main-ack-job* :thread) thread)))
  :imported-ack-started)

(defun imported-main-ack-join ()
  (let ((thread (sb-thread:with-mutex (*imported-main-ack-lock*)
                  (getf *imported-main-ack-job* :thread))))
    (when thread (sb-thread:join-thread thread)))
  :ack-worker-reaped)

(defun imported-main-ack-result ()
  (let ((thread (sb-thread:with-mutex (*imported-main-ack-lock*)
                  (unless (member (getf *imported-main-ack-job* :state) '(:done :failed))
                    (error "imported ACK test worker has not finished"))
                  (getf *imported-main-ack-job* :thread))))
    (sb-thread:join-thread thread)
    (sb-thread:with-mutex (*imported-main-ack-lock*)
      (when (getf *imported-main-ack-job* :failure)
        (error "imported ACK test failed: ~a" (getf *imported-main-ack-job* :failure)))
      (format nil "IMPORTED-ACK-DONE ~s" (getf *imported-main-ack-job* :result)))))

(defvar *imported-main-ack-sql-functions* nil)

(defvar *imported-main-ack-sql-directory* nil)

(defvar *imported-main-ack-sql-repeat-markers* nil)

(defun imported-main-ack-sql-marker (arguments name)
  (when (and *imported-main-ack-sql-directory*
             (equal (first arguments) "ack_barrier"))
    (with-open-file (stream (merge-pathnames name *imported-main-ack-sql-directory*)
                            :direction :output :if-exists (if *imported-main-ack-sql-repeat-markers* :append :error)
                            :if-does-not-exist :create)
      (write-line name stream))))

(defun imported-main-ack-instrument-sql (directory &key repeat-markers)
  (when *imported-main-ack-sql-functions* (error "ACK SQL instrumentation already installed"))
  (setf *imported-main-ack-sql-directory* (pathname directory)
        *imported-main-ack-sql-repeat-markers* repeat-markers)
  (dolist (entry '((wal-create-physical-replication-slot "creating" "created")
                   (wal-drop-replication-slot "retiring" "retired")))
    (let* ((symbol (first entry)) (before (second entry)) (after (third entry))
                                  (original (symbol-function symbol)))
      (push (cons symbol original) *imported-main-ack-sql-functions*)
      (setf (symbol-function symbol)
            (lambda (&rest arguments)
              (imported-main-ack-sql-marker arguments before)
              (multiple-value-prog1 (apply original arguments)
                (imported-main-ack-sql-marker arguments after))))))
  ;; This marker records query arrival, before dispatch; it does not prove that
  ;; the slot function has entered.  The other markers surround that function.
  (let ((original (symbol-function '%dispatch-frontend-frame)))
    (push (cons '%dispatch-frontend-frame original) *imported-main-ack-sql-functions*)
    (setf (symbol-function '%dispatch-frontend-frame)
          (lambda (stream type-byte length-bytes payload)
            (when (and (= type-byte (char-code #\Q)) (vectorp payload))
              (let ((query (ignore-errors
                            (sb-ext:octets-to-string payload :external-format :utf-8
                                                             :end (position 0 payload)))))
                (when (and query (search "'ack_barrier'" query)
                           (or (search "pg_create_physical" query) (search "pg_drop_replication_slot" query)))
                  (imported-main-ack-sql-marker
                   '("ack_barrier") (if (search "pg_create_physical" query)
                                        "create-query" "drop-query")))))
            (funcall original stream type-byte length-bytes payload))))
  :ack-sql-instrumented)

(defun imported-main-ack-restore-sql ()
  (dolist (entry *imported-main-ack-sql-functions*)
    (setf (symbol-function (car entry)) (cdr entry)))
  (setf *imported-main-ack-sql-functions* nil *imported-main-ack-sql-directory* nil
        *imported-main-ack-sql-repeat-markers* nil)
  :ack-sql-restored)
