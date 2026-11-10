(in-package #:pg-physical-tests)

(defun append-readiness-fpw-change (store enabled)
  (let* ((begin (pg::physical-store-flushed-lsn store))
         (start (+ begin pg::+wal-header-size+)))
    (pg::physical-store-input store begin (sample-physical-record begin))
    (pg::physical-store-input
     store start
     (pg::wal-encode-record pg::+wal-record-full-page-writes-change+ 0 start
                            (pg::wal-encode-full-page-writes-change enabled)))
    (pg::physical-store-flush store)))

(defun append-readiness-fpw-checkpoint (store policy)
  (let* ((begin (pg::physical-store-flushed-lsn store))
         (start (+ begin pg::+wal-header-size+))
         (facts (unless (eq policy :unknown) (list :full-page-writes policy)))
         (snapshot (append
                    (list :version 2 :start start
                          :end (+ start (pg::wal-checkpoint-record-size facts)) :redo start
                          :next-xid 2000 :xid-reserved-through 1999 :next-oid 16384
                          :oldest-active-xid 2000 :time 42 :timeline 2 :prev-timeline 1)
                    facts)))
    (pg::physical-store-input store begin (sample-physical-record begin))
    (pg::physical-store-input
     store start (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                                        (pg::wal-encode-checkpoint-snapshot snapshot)))
    (pg::physical-store-flush store)))

(rove:deftest readiness-fpw-matching-child-checkpoint-keeps-published-state-private
    (dolist (enabled '(nil t))
      (with-restartpoint-fixture (root store)
        (declare (ignore root))
        (selected-replay-test-prepare store)
        (append-readiness-fpw-change store enabled)
        (append-readiness-fpw-checkpoint store enabled)
        (let ((before (copy-tree pg::*physical-recovery-state*))
              (writer pg::*wal-fpw-state*))
          (ng (funcall #'pg::physical-promotion-awaiting-child-initialization-p))
          (ok (equal before pg::*physical-recovery-state*))
          (ok (eq writer pg::*wal-fpw-state*))
          (ok (= 96 pg::*last-replay-lsn*))
          (ok (= 1 (pg::physical-recovery-replay-timeline)))))))

(rove:deftest readiness-fpw-rejects-checkpoint-policy-loss-before-promotion
    (dolist (enabled '(nil t))
      (dolist (policy (list :unknown (not enabled)))
        (with-restartpoint-fixture (root store)
          (declare (ignore root))
          (selected-replay-test-prepare store)
          (append-readiness-fpw-change store enabled)
          (append-readiness-fpw-checkpoint store policy)
          (let ((before (copy-tree pg::*physical-recovery-state*)))
            (ok (signals (funcall
                          #'pg::physical-promotion-awaiting-child-initialization-p) 'error))
            (ok (equal before pg::*physical-recovery-state*))
            (ok (= 96 pg::*last-replay-lsn*)))))))

(rove:deftest readiness-fpw-rejects-a-second-marker-before-the-child-checkpoint
    (dolist (enabled '(nil t))
      (dolist (repeat (list enabled (not enabled)))
        (with-restartpoint-fixture (root store)
          (declare (ignore root))
          (selected-replay-test-prepare store)
          (append-readiness-fpw-change store enabled)
          (append-readiness-fpw-change store repeat)
          (append-readiness-fpw-checkpoint store repeat)
          (let ((before (copy-tree pg::*physical-recovery-state*)))
            (ok (signals (funcall
                          #'pg::physical-promotion-awaiting-child-initialization-p) 'error))
            (ok (equal before pg::*physical-recovery-state*))
            (ok (= 96 pg::*last-replay-lsn*)))))))
