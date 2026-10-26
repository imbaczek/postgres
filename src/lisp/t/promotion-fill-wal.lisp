;;;; Task-owned acceptance padding through the real child WAL insertion path.
(in-package #:pg)

(defun promotion-test-fill-wal ()
  (prevent-recovery-primary-operation "promotion test WAL padding")
  (with-write-lock
    (with-wal-critical
      (let ((limit (+ *wal-end-lsn* +wal-segment-size+)))
        (loop while (< *wal-end-lsn* limit)
              do (wal-append-batch-start) (wal-append-record +wal-record-maintenance+ 0 #()))
        (wal-flush)
        *wal-end-lsn*))))
