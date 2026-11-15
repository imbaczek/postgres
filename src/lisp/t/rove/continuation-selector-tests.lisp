(in-package #:pg)

(rove:deftest continuation-unknown-selection-fence-is-sticky
    (call-with-logical-slot-real-startup-test
     (lambda ()
       (let ((child (merge-pathnames "pg_logical_continuation/" *pgdata*)))
         (rove:ok (not (logical-slot-continuation-gc-fenced-p *pgdata*)))
         (sb-posix:mkdir (namestring child) #o700)
         (rove:ok (logical-slot-continuation-gc-fenced-p *pgdata*))
         (sb-posix:rmdir (namestring child))
         (rove:ok (logical-slot-continuation-gc-fenced-p *pgdata*))))))

(rove:deftest continuation-unknown-selection-stops-wal-removal-before-effects
    (call-with-logical-slot-real-startup-test
     (lambda ()
       (let ((child (merge-pathnames "pg_logical_continuation/" *pgdata*))
             (original (symbol-function 'wal-settle-page-publications-under-lock))
             (called nil))
         (sb-posix:mkdir (namestring child) #o700)
         (unwind-protect
              (progn
                (setf (symbol-function 'wal-settle-page-publications-under-lock)
                      (lambda () (setf called t) (error "unexpected WAL recycling effect")))
                (sb-thread:with-mutex (*wal-lock*)
                  (wal-remove-checkpointed-segments-under-wal-lock *wal-durable-lsn*))
                (rove:ok (not called)))
           (setf (symbol-function 'wal-settle-page-publications-under-lock) original))))))
