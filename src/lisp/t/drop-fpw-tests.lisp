(in-package #:pg-physical-tests)

(rove:deftest drop-fpw-scan-keeps-matching-child-initialization-private
    (dolist (enabled '(nil t))
      (with-restartpoint-fixture (root store)
        (declare (ignore root))
        (selected-replay-test-prepare store)
        (append-readiness-fpw-change store enabled)
        (append-readiness-fpw-checkpoint store enabled)
        (let ((before (copy-tree pg::*physical-recovery-state*))
              (writer pg::*wal-fpw-state*))
          (multiple-value-bind (removed terminal)
              (funcall
               (lambda () (pg::physical-recovery-proven-drops store "history-source")))
            (ok (zerop (hash-table-count removed)))
            (ok (= terminal (pg::physical-store-flushed-lsn store))))
          (ok (equal before pg::*physical-recovery-state*))
          (ok (eq writer pg::*wal-fpw-state*))
          (ok (= 96 pg::*last-replay-lsn*))))))

(rove:deftest drop-fpw-scan-rejects-child-policy-loss
    (dolist (enabled '(nil t))
      (dolist (policy (list :unknown (not enabled)))
        (with-restartpoint-fixture (root store)
          (declare (ignore root))
          (selected-replay-test-prepare store)
          (append-readiness-fpw-change store enabled)
          (append-readiness-fpw-checkpoint store policy)
          (let ((before (copy-tree pg::*physical-recovery-state*)))
            (ok (signals
                 (funcall
                  (lambda () (pg::physical-recovery-proven-drops store "history-source")))
                 'error))
            (ok (equal before pg::*physical-recovery-state*)))))))

(rove:deftest drop-fpw-scan-rejects-duplicate-child-markers
    (dolist (enabled '(nil t))
      (with-restartpoint-fixture (root store)
        (declare (ignore root))
        (selected-replay-test-prepare store)
        (append-readiness-fpw-change store enabled)
        (append-readiness-fpw-change store enabled)
        (append-readiness-fpw-checkpoint store enabled)
        (let ((before (copy-tree pg::*physical-recovery-state*)))
          (ok (signals
               (funcall
                (lambda () (pg::physical-recovery-proven-drops store "history-source")))
               'error))
          (ok (equal before pg::*physical-recovery-state*))))))

(rove:deftest drop-scan-never-excuses-a-missing-file-before-child-initialization
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (selected-replay-test-prepare store)
      (pg::physical-store-input store 152 (sample-physical-record 152))
      (pg::physical-store-input
       store 184 (pg::wal-encode-record pg::+wal-record-catalog+ 0 184
                                        (history-catalog-payload nil '(900))))
      (pg::physical-store-flush store)
      (let ((before (copy-tree pg::*physical-recovery-state*)))
        (ok (signals (pg::physical-recovery-proven-drops store "history-source") 'error))
        (ok (equal before pg::*physical-recovery-state*))
        (ok (= 96 pg::*last-replay-lsn*)))))
