(in-package #:pg)

(defun cursor-unit-append-maintenance ()
  (with-wal-critical
    (wal-append-batch-start)
    (let ((end (wal-append-record +wal-record-maintenance+ 0
                                  (make-array 0 :element-type '(unsigned-byte 8)))))
      (wal-flush-core)
      end)))

(defun call-with-owned-cursor-test (function)
  (let ((real-log-reset (symbol-function 'wal-log-clog-reset-under-lock)))
    (call-with-logical-slot-creation-test
     (lambda ()
       (let ((*wal-control-metadata*
               (make-wal-control-metadata (catalog-system-identifier) +db-in-production+))
             (mock-log-reset (symbol-function 'wal-log-clog-reset-under-lock)))
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-log-clog-reset-under-lock) real-log-reset)
                (create-standard-logical-slot "cursor_source" :nothing)
                (let* ((slot (gethash "cursor_source" *wal-replication-slots*))
                       (start (wal-replication-slot-confirmed-flush-lsn slot))
                       (middle (cursor-unit-append-maintenance))
                       (end (cursor-unit-append-maintenance)))
                  (call-with-logical-replication-slot
                   "cursor_source" (lambda (owned) (funcall function owned start middle end)))))
           (setf (symbol-function 'wal-log-clog-reset-under-lock) mock-log-reset)))))))

(rove:deftest logical-wal-owned-cursor-keeps-horizon-across-real-clog-reset
    (call-with-owned-cursor-test
     (lambda (slot start middle end)
       (declare (ignore middle))
       (let ((escaped nil) (before *wal-storage-incarnation*))
         (call-with-logical-slot-wal-cursor
          "cursor_source" start end (wal-replication-slot-confirmed-timeline slot) :prefix-end
          (lambda (cursor)
            (setf escaped cursor)
            (rove:ok (= 1 (hash-table-count *wal-snapshot-retention-pins*)))
            (clog-reset)
            (rove:ok (not (eq before *wal-storage-incarnation*)))
            (rove:ok (> *wal-durable-lsn* end))
            (let ((records (loop for record = (logical-wal-cursor-next cursor)
                                 while record collect record)))
              (rove:ok (equal (list +wal-record-batch-start+ +wal-record-maintenance+
                                    +wal-record-batch-start+ +wal-record-maintenance+)
                              (mapcar #'wal-record-type records)))
              (rove:ok (= end (wal-record-end (car (last records))))))))
         (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*)))
         (rove:ok (rove:signals (logical-wal-cursor-next escaped) 'error))))))

(rove:deftest logical-wal-owned-cursor-rejects-unproven-geometric-starts
    (dolist (failure '(:interior :other-boundary :owner :role))
      (call-with-owned-cursor-test
       (lambda (slot start middle end)
         (declare (ignore slot))
         (let ((entered nil))
           (rove:ok
            (rove:signals
             (call-with-logical-slot-wal-cursor
              "cursor_source" (case failure (:interior (1+ start)) (:other-boundary middle) (t start))
              end (if (eq failure :owner) 2 1) (if (eq failure :role) :record-start :prefix-end)
              (lambda (cursor) (declare (ignore cursor)) (setf entered t)))
             'error))
           (rove:ok (not entered))
           (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*))))))))

(rove:deftest logical-wal-owned-cursor-requires-exact-pending-restart-evidence
    (dolist (failure '(nil :missing :swapped :owner :role))
      (call-with-owned-cursor-test
       (lambda (slot start middle end)
         (declare (ignore start))
         (let ((entered nil))
           (sb-thread:with-mutex (*wal-lock*)
             (setf (wal-replication-slot-restart-lsn slot) middle
                   (wal-replication-slot-restart-timeline slot) 1
                   (wal-replication-slot-confirmed-flush-lsn slot) end)
             (unless (eq failure :missing)
               (setf (gethash 17 (wal-replication-slot-pending-xids slot))
                     (cons (if (eq failure :swapped) (1+ middle) middle) end)))
             (wal-save-replication-slot slot))
           (rove:ok
            (eq (null failure)
                (handler-case
                    (progn
                      (call-with-logical-slot-wal-cursor
                       "cursor_source" middle end (if (eq failure :owner) 2 1)
                       (if (eq failure :role) :prefix-end :record-start)
                       (lambda (cursor)
                         (setf entered t)
                         (rove:ok (= +wal-record-batch-start+
                                     (wal-record-type (logical-wal-cursor-next cursor))))
                         (rove:ok (= +wal-record-maintenance+
                                     (wal-record-type (logical-wal-cursor-next cursor))))
                         (rove:ok (null (logical-wal-cursor-next cursor)))))
                      t)
                  (error () nil))))
           (rove:ok (eq (null failure) entered))
           (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*))))))))

(rove:deftest logical-wal-owned-cursor-fences-source-and-slot-changes
    (dolist (change '(:registry :pin :owner :database :plugin :profile :catalog :birth
                      :timeline :namespace :root :control :horizon :role))
      (call-with-owned-cursor-test
       (lambda (slot start middle end)
         (declare (ignore middle))
         (call-with-logical-slot-wal-cursor
          "cursor_source" start end 1 :prefix-end
          (lambda (cursor)
            (flet ((reject () (rove:ok (rove:signals (logical-wal-cursor-next cursor) 'error))))
              (ecase change
                (:registry
                 (setf (gethash "cursor_source" *wal-replication-slots*)
                       (wal-copy-replication-slot-with-profile slot))
                 (reject))
                (:pin (clrhash *wal-snapshot-retention-pins*) (reject))
                (:owner (setf (wal-replication-slot-active-owner slot) (gensym)) (reject))
                (:database (incf (wal-replication-slot-database-oid slot)) (reject))
                (:plugin (setf (wal-replication-slot-plugin slot) "pg_lisp") (reject))
                (:profile (setf (gethash slot *wal-slot-profiles*) :binary-bytea) (reject))
                (:catalog (let ((*catalog* (copy-catalog *catalog*))) (reject)))
                (:birth (incf (wal-replication-slot-consistent-lsn slot)) (reject))
                (:timeline (let ((*wal-insert-timeline* 2)) (reject)))
                (:namespace (let ((*wal-segment-namespace* :timeline)) (reject)))
                (:root (let ((*pgdata* (uiop:pathname-parent-directory-pathname *pgdata*))) (reject)))
                (:control (let ((*wal-control-metadata* (list :system-id -1))) (reject)))
                (:horizon (let ((*wal-durable-lsn* (1- end))) (reject)))
                (:role (let ((*physical-recovery-state* '(:cursor-test-standby))) (reject)))))))
         (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*)))))))

(rove:deftest logical-wal-owned-cursor-cleans-pins-after-nonlocal-exit
    (call-with-owned-cursor-test
     (lambda (slot start middle end)
       (declare (ignore slot middle))
       (let ((escaped nil))
         (rove:ok
          (eq :aborted
              (catch :cursor-abort
                (call-with-logical-slot-wal-cursor
                 "cursor_source" start end 1 :prefix-end
                 (lambda (cursor) (setf escaped cursor) (throw :cursor-abort :aborted))))))
         (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*)))
         (rove:ok (rove:signals (logical-wal-cursor-next escaped) 'error))))))

(rove:deftest logical-wal-owned-cursor-rejects-mismatched-persisted-control
    (dolist (failure '(:timeline :namespace :horizon :missing :corrupt))
      (call-with-owned-cursor-test
       (lambda (slot start middle end)
         (declare (ignore slot middle))
         (call-with-logical-slot-wal-cursor
          "cursor_source" start end 1 :prefix-end
          (lambda (cursor)
            (let* ((fields (multiple-value-list (wal-read-control-fields)))
                   (bytes (wal-encode-control-image
                           (first fields) (if (eq failure :horizon) (1- end) (second fields))
                           (third fields)
                           :timeline (if (eq failure :timeline) 2 (fourth fields))
                           :checkpoint-timeline (fifth fields)
                           :namespace (if (member failure '(:timeline :namespace))
                                          :timeline (sixth fields))
                           :metadata (seventh fields))))
              (if (eq failure :missing)
                  (delete-file (wal-control-path))
                  (progn
                    (when (eq failure :corrupt)
                      (setf (aref bytes 0) (logxor 1 (aref bytes 0))))
                    (wal-atomic-write-bytes (wal-control-path) bytes)))
              (rove:ok (rove:signals (logical-wal-cursor-next cursor) 'error))
              (rove:ok (= start (logical-wal-cursor-position cursor))))))
         (rove:ok (zerop (hash-table-count *wal-snapshot-retention-pins*)))))))
