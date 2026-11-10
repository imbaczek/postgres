(in-package #:pg-physical-tests)

(defun call-with-eor-stage-chain (initialized function)
  (with-restartpoint-fixture (root store)
    (declare (ignore root))
    (selected-replay-test-prepare store)
    (when initialized (append-readiness-fpw-checkpoint store :unknown))
    (let* ((fork (pg::physical-store-flushed-lsn store))
           (raw (pg::wal-encode-end-of-recovery-record fork 42 3 2 1))
           (history (format nil "1~c0/00000060~%2~c0/~8,'0X~%" #\Tab #\Tab fork)))
      (pg::physical-store-select-history store 3 history raw)
      (setf pg::*physical-recovery-state*
            (pg::physical-recovery-attach-selected-store pg::*physical-recovery-state* store))
      (pg::physical-store-input store fork raw)
      (pg::physical-store-flush store)
      (funcall function store (+ fork (length raw)) fork))))

(defun run-private-eor-stage-scan (mode store end)
  (let ((state pg::*physical-recovery-state*))
    (ecase mode
      (:resources (pg::physical-recovery-scan-resource-fact store 32 end nil state))
      (:drops (pg::physical-recovery-proven-drops store "history-source"))
      (:readiness (pg::physical-promotion-awaiting-child-initialization-p))
      (:follow
       (pg::physical-follow-scan-replay-from-seed-core
        store state (list end 3) '(32 1) 50 '(32 1 50 nil nil nil nil nil)
        (lambda (function start)
          (pg::physical-recovery-map-permitted-records store function :start-lsn start))
        nil)))))

(rove:deftest successive-eor-replay-requires-a-child-checkpoint-before-publication
    (let ((pg::*physical-replay-restarting-p* t))
      (dolist (initialized '(nil t))
        (call-with-eor-stage-chain
         initialized
         (lambda (store end fork)
           (declare (ignore store fork))
           (pg::clog-init)
           (unwind-protect
                (if initialized
                    (progn
                      (ok (= end (pg::physical-replay-from 96)))
                      (ok (= 3 (pg::physical-recovery-replay-timeline)))
                      (ok (= 2 (getf (getf pg::*physical-recovery-state* :source-checkpoint) :timeline))))
                    (progn
                      (ok (signals (pg::physical-replay-from 96) 'error))
                      (ok (= 152 pg::*last-replay-lsn*))
                      (ok (= 2 (pg::physical-recovery-replay-timeline)))
                      (ok (= 152 (getf (pg::physical-recovery-read-control pg::*pgdata*) :applied)))))
             (close pg::*clog-stream*)
             (setf pg::*clog-stream* nil)))))))

(rove:deftest private-eor-scans-require-child-initialization-without-changing-published-state
    (dolist (mode '(:resources :drops :readiness :follow))
      (dolist (initialized '(nil t))
        (call-with-eor-stage-chain
         initialized
         (lambda (store end fork)
           (declare (ignore fork))
           (let ((before (copy-tree pg::*physical-recovery-state*))
                 (writer pg::*wal-fpw-state*))
             (if initialized
                 (ok (not (intake-fails-p (lambda () (run-private-eor-stage-scan mode store end)))))
                 (ok (signals (run-private-eor-stage-scan mode store end) 'error)))
             (ok (equal before pg::*physical-recovery-state*))
             (ok (eq writer pg::*wal-fpw-state*))
             (ok (= 96 pg::*last-replay-lsn*))))))))

(rove:deftest selected-eor-checker-allows-an-old-base-without-runtime-state
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let* ((raw (selected-replay-test-prepare store))
             (record (pg::wal-decode-record raw 96))
             (pg::*physical-recovery-state* nil)
             (pg::*wal-recovery-group-active* nil))
        (ok (= 2 (pg::physical-replay-check-selected-eor store record raw 96 1)))
        (ok (null pg::*physical-recovery-state*)))))
