(in-package #:pg-physical-tests)

(defun call-with-logical-policy-wal-fixture (function)
  (let ((pg::*wal-level* :replica)
        (pg::*logical-capture-arming* (make-hash-table :test #'eq))
        (pg::*logical-capture-xid-modes* (make-hash-table :test #'eql))
        (pg::*logical-capture-live-slots* 0))
    (call-with-fpw-wal-fixture
     (lambda ()
       (let ((pg::*backend* nil) (pg::*active-xids* nil))
         (funcall function))))))

(deftest primary-logical-policy-real-wal-recovery-preserves-status-through-clog-reset
    (call-with-logical-policy-wal-fixture
     (lambda ()
       (let* ((enabled-end (pg::wal-report-logical-decoding-status t))
              (old pg::*wal-logical-policy*))
         (pg::clog-reset)
         (ok (not (eq old pg::*wal-logical-policy*)))
         (ok (eq pg::*wal-storage-incarnation*
                 (pg::wal-logical-policy-incarnation pg::*wal-logical-policy*)))
         (fpw-reopen-wal)
         (ok (eq :unrecovered pg::*wal-logical-policy*))
         (pg::wal-recover)
         (sb-thread:with-mutex (pg::*wal-lock*)
           (let* ((policy (pg::wal-checked-logical-policy-under-lock))
                  (state (pg::wal-logical-policy-state policy)))
             (ok (pg::wal-logical-status-enabled state))
             (ok pg::*logical-capture-force*)
             (ok (= enabled-end (pg::wal-logical-status-last-enable-lsn state))))
           (pg::wal-note-logical-slot-count-under-lock 0))
         (pg::with-wal-publication-fence (begin)
           (pg::wal-process-pending-logical-disable-under-lock #'begin))
         (let ((disabled-end pg::*wal-durable-lsn*))
           (ok (> disabled-end enabled-end))
           (fpw-reopen-wal)
           (pg::wal-recover)
           (sb-thread:with-mutex (pg::*wal-lock*)
             (ok (equal '(:logical-decoding-enabled nil)
                        (pg::wal-current-logical-decoding-fields)))
             (ok (not pg::*logical-capture-force*))
             (ok (= disabled-end
                    (pg::wal-logical-status-end
                     (pg::wal-logical-policy-state pg::*wal-logical-policy*))))))))))

(deftest primary-logical-policy-checkpoint-freezes-known-booleans
    (call-with-logical-policy-wal-fixture
     (lambda ()
       (dolist (enabled '(t nil t))
         (sb-thread:with-mutex (pg::*wal-lock*)
           (if enabled
               (setf (gethash "policy" pg::*wal-replication-slots*)
                     (pg::make-wal-replication-slot :name "policy" :kind :logical :restart-lsn 0))
               (remhash "policy" pg::*wal-replication-slots*))
           (pg::wal-refresh-logical-capture-slot-mirror-under-wal-lock))
         (pg::wal-report-logical-decoding-status enabled)
         (pg::wal-checkpoint-core)
         (let ((snapshot (copy-list pg::*wal-checkpoint-snapshot*)))
           (ok (eq :logical-decoding-enabled
                   (get-properties snapshot '(:logical-decoding-enabled))))
           (ok (eq enabled (getf snapshot :logical-decoding-enabled)))
           (ok (equal snapshot (third (multiple-value-list (pg::wal-read-control-fields)))))
           (fpw-reopen-wal)
           (ok (eq :unrecovered pg::*wal-logical-policy*))
           (pg::wal-recover)
           (sb-thread:with-mutex (pg::*wal-lock*)
             (ok (equal (list :logical-decoding-enabled enabled)
                        (pg::wal-current-logical-decoding-fields)))
             (ok (eq enabled pg::*logical-capture-force*))
             (ok (= (getf snapshot :end)
                    (pg::wal-logical-status-end
                     (pg::wal-logical-policy-state pg::*wal-logical-policy*))))))))))
