;;;; FullXID reservation ends must never become special transaction IDs on restart.
(in-package #:pg-physical-tests)

(deftest xid-reservation-end-skips-special-low-identifiers
    (dolist (epoch '(1 2 4294967295))
      (let ((boundary (* epoch 4294967296)))
        (dotimes (offset 3)
          (let* ((next (+ (- boundary 1000) offset))
                 (end (pg::xid-reservation-end next)))
            (ok (= end (+ boundary 3)))
            (ok (> end next))
            (ok (= 3 (mod end 4294967296)))))))
  (ok (= 18446744073709551616 (pg::xid-reservation-end 18446744073709551615)))
  (dolist (bad '(0 1 2 4294967296 4294967297 4294967298 18446744073709551616))
    (ok (intake-fails-p (lambda () (pg::xid-reservation-end bad))))))

(deftest assigned-reservation-persists-a-normal-restart-counter
    (dotimes (offset 3)
      (with-physical-store-directory (root)
        (let* ((next (+ (- 4294967296 1000) offset))
               (pg::*pgdata* root) (pg::*next-xid* next) (pg::*xid-batch-end* next)
               (pg::*wal-ready* nil) (pg::*active-xids* nil)
               (pg::*physical-recovery-state* nil) (pg::*catalog-oldest-relfrozenxid* nil))
          (pg::wal-ensure-directory-durable (merge-pathnames "global/" root))
          (ok (= next (pg::assign-xid)))
          (ok (= 4294967299 (pg::load-next-xid)))
          (ok (= (1+ next) pg::*next-xid*))))))

(deftest legacy-special-reservation-end-is-corrected-before-startup-allocation
    (dotimes (low 3)
      (with-physical-store-directory (root)
        (let ((pg::*pgdata* root) (pg::*clog-stream* nil)
                                  (pg::*next-xid* 3) (pg::*xid-batch-end* (+ 4294967296 low))
                                  (pg::*xid-wrap-pending* nil) (pg::*xid-wrap-completed-through* nil))
          (pg::wal-ensure-directory-durable (merge-pathnames "global/" root))
          (pg::save-next-xid)
          (unwind-protect
               (progn
                 (pg::xact-init)
                 (ok (= 4294967299 pg::*next-xid*))
                 (ok (= 4294967299 pg::*xid-batch-end*))
                 (ok (= 4294967299 (pg::load-next-xid))))
            (when pg::*clog-stream* (close pg::*clog-stream*)))))))

(cl-spec:defproperty canonical-reservation-end-preserves-normal-full-xids
    ((value (range integer 3 18446744073709551616)))
  (:about pg::canonical-next-full-xid)
  (:trials (:normal 200))
  (let ((end (pg::canonical-next-full-xid value)))
    (and (<= value end (+ value 3) 18446744073709551619)
         (<= end 18446744073709551616)
         (or (= end 18446744073709551616) (>= (mod end 4294967296) 3))
         (= end (pg::canonical-next-full-xid end)))))

(cl-spec:defproperty canonical-special-reservation-end-skips-to-first-normal
    ((epoch (range integer 1 4294967295)) (low (range integer 0 2)))
  (:about pg::canonical-next-full-xid)
  (:trials (:normal 200))
  (let ((boundary (* epoch 4294967296)))
    (= (+ boundary 3) (pg::canonical-next-full-xid (+ boundary low)))))

(deftest generated-canonical-special-reservation-end-boundaries
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'canonical-special-reservation-end-skips-to-first-normal :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest generated-canonical-reservation-end-bounds
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'canonical-reservation-end-preserves-normal-full-xids :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
