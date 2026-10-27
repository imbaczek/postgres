(in-package #:pg-physical-tests)

(defun selected-replay-test-prepare (store)
  (let* ((eor (pg::wal-encode-end-of-recovery-record 96 42 2 1 1))
         (state pg::*physical-recovery-state*))
    (setf (getf state :phase) 2 (getf state :replay-timeline) 1 (getf state :required-timeline) 1
          (getf state :source-reservation) (list :version 1 :through 96 :bound 999 :complete t)
          (getf state :source-xids)
          (pg::running-xacts-anchor (list :next-xid 50 :running '(3 49) :complete t) 96))
    (pg::physical-store-select-history store 2 (format nil "1~c0/00000060~%" #\Tab) eor)
    (setf pg::*physical-recovery-state* (pg::physical-recovery-attach-selected-store state store))
    (pg::physical-store-input store 96 eor)
    (pg::physical-store-flush store)
    eor))

(defun selected-replay-test-child-checkpoint (start &optional (next 2000))
  (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                         (pg::wal-encode-checkpoint-snapshot
                          (list :version 2 :start start :end (+ start (pg::wal-checkpoint-record-size)) :redo start
                                :next-xid next :xid-reserved-through (1- next) :next-oid 16384 :oldest-active-xid next
                                :time 42 :timeline 2 :prev-timeline 1))))

(deftest selected-eor-replay-publishes-a-child-pair-without-ending-recovery
    (with-restartpoint-fixture (root store)
      (let ((pg::*wal-insert-timeline* 1) (pg::*wal-checkpoint-timeline* 1))
        (pg::clog-init)
        (unwind-protect
             (progn
               (selected-replay-test-prepare store)
               (ok (= 1 (pg::physical-recovery-replay-timeline)))
               (ok (= 152 (pg::physical-replay-from 96)))
               (ok (= 2 (pg::physical-recovery-replay-timeline)))
               (ok (= 1 pg::*wal-insert-timeline* pg::*wal-checkpoint-timeline*))
               (ok (pg::recovery-in-progress-p))
               (ok (= 50 pg::*next-xid*))
               (ok (null (getf pg::*physical-recovery-state* :source-xids)))
               (ng (getf (getf pg::*physical-recovery-state* :source-reservation) :complete))
               (ok (= 0 (pg::clog-get 3)))
               (let ((control (pg::physical-recovery-read-control root)))
                 (ok (= 1 (getf control :timeline)))
                 (ok (= 2 (getf control :applied-timeline)))
                 (ok (= 152 (getf control :applied)))))
          (close pg::*clog-stream*) (setf pg::*clog-stream* nil)))))

(deftest selected-eor-cannot-be-used-to-finish-an-active-batch
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let* ((raw (selected-replay-test-prepare store))
             (record (pg::wal-decode-record raw 96))
             (pg::*wal-recovery-group-active* t))
        (ok (signals (pg::physical-replay-check-selected-eor store record raw 96 1) 'error))
        (ok (= 96 pg::*last-replay-lsn*)))))

(deftest selected-eor-replay-enforces-the-minimum-recovery-timeline
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let* ((raw (selected-replay-test-prepare store))
             (record (pg::wal-decode-record raw 96))
             (pg::*wal-recovery-group-active* nil))
        (setf (getf pg::*physical-recovery-state* :required-applied) 256)
        (ok (signals (pg::physical-replay-check-selected-eor store record raw 96 1) 'error))
        (setf (getf pg::*physical-recovery-state* :required-timeline) 2)
        (ok (= 2 (pg::physical-replay-check-selected-eor store record raw 96 1))))))

(deftest selected-eor-only-replay-rejects-an-allocation-record-before-child-checkpoint
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::clog-init)
      (unwind-protect
           (progn
             (selected-replay-test-prepare store)
             (pg::physical-replay-from 96)
             (let* ((start 152)
                    (record (pg::wal-decode-record
                             (pg::wal-encode-record pg::+wal-record-xid-reservation+ 0 start
                                                    (pg::wal-encode-xid-reservation 1999)) start)))
               (ok (signals (pg::physical-replay-check-child-initialization record 50) 'error)))
             (ok (= 152 pg::*last-replay-lsn*)))
        (close pg::*clog-stream*) (setf pg::*clog-stream* nil))))

(deftest selected-child-checkpoint-restores-allocation-without-inventing-membership
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let ((pg::*physical-replay-restarting-p* t))
        (pg::clog-init)
        (unwind-protect
             (progn
               (selected-replay-test-prepare store)
               (pg::physical-replay-from 96)
               (pg::physical-store-input store 152 (sample-physical-record 152))
               (pg::physical-store-input store 184 (selected-replay-test-child-checkpoint 184))
               (pg::physical-store-flush store)
               (pg::physical-replay-from 152)
               (ok (= 2000 pg::*next-xid*))
               (ok (= 2 (pg::physical-recovery-replay-timeline)))
               (ok (= 2 (getf (getf pg::*physical-recovery-state* :source-checkpoint) :timeline)))
               (ok (getf (getf pg::*physical-recovery-state* :source-reservation) :complete))
               (ok (= 1999 (getf (getf pg::*physical-recovery-state* :source-reservation) :bound)))
               (ok (null (getf pg::*physical-recovery-state* :source-xids))))
          (close pg::*clog-stream*) (setf pg::*clog-stream* nil)))))

(deftest selected-child-generation-restores-a-pair-ahead-of-ordinary-control
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::clog-init)
      (unwind-protect
           (progn
             (selected-replay-test-prepare store)
             (pg::physical-replay-from 96)
             (pg::physical-store-compact store 152 #'pg::physical-restartpoint-publish)
             (setf (getf pg::*physical-recovery-state* :required-applied) 96
                   (getf pg::*physical-recovery-state* :required-timeline) 1
                   (getf pg::*physical-recovery-state* :replay-timeline) 1
                   pg::*last-replay-lsn* 32)
             (ok (= 152 (pg::physical-restartpoint-prepare-storage store)))
             (ok (= 2 (pg::physical-recovery-replay-timeline)))
             (ok (= 152 (getf pg::*physical-recovery-state* :required-applied)))
             (ok (= 2 (getf pg::*physical-recovery-state* :required-timeline)))
             (ng (getf (getf pg::*physical-recovery-state* :source-reservation) :complete))
             (ok (null (getf pg::*physical-recovery-state* :source-xids))))
        (close pg::*clog-stream*) (setf pg::*clog-stream* nil))))

(deftest selected-parent-generation-replays-eor-toward-a-newer-required-pair
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::clog-init)
      (unwind-protect
           (progn
             (pg::physical-store-compact store 96 #'pg::physical-restartpoint-publish)
             (selected-replay-test-prepare store)
             (pg::physical-replay-from 96)
             (setf (getf pg::*physical-recovery-state* :replay-timeline) 1 pg::*last-replay-lsn* 32)
             (ok (= 96 (pg::physical-restartpoint-prepare-storage store)))
             (ok (= 1 (pg::physical-recovery-replay-timeline)))
             (ok (= 152 (getf pg::*physical-recovery-state* :required-applied)))
             (ok (= 2 (getf pg::*physical-recovery-state* :required-timeline)))
             (pg::physical-replay-from 96)
             (ok (= 152 pg::*last-replay-lsn*))
             (ok (= 2 (pg::physical-recovery-replay-timeline))))
        (close pg::*clog-stream*) (setf pg::*clog-stream* nil))))

(deftest selected-recovery-identify-does-not-pair-parent-replay-with-child-receipt
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (selected-replay-test-prepare store)
      (multiple-value-bind (lsn timeline) (pg::physical-recovery-identify-position)
        (ok (= 96 lsn))
        (ok (= 1 timeline)))
      (setf pg::*last-replay-lsn* 152 (getf pg::*physical-recovery-state* :replay-timeline) 2)
      (multiple-value-bind (lsn timeline) (pg::physical-recovery-identify-position)
        (ok (= 152 lsn))
        (ok (= 2 timeline)))))

(deftest selected-promotion-waits-for-durable-child-initialization
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (pg::clog-init)
      (unwind-protect
           (progn
             (selected-replay-test-prepare store)
             (ok (pg::physical-promotion-awaiting-child-initialization-p))
             (ok (= 1 (pg::physical-recovery-replay-timeline)))
             (pg::physical-replay-from 96)
             (ok (pg::physical-promotion-awaiting-child-initialization-p))
             (pg::physical-store-input store 152 (sample-physical-record 152))
             (pg::physical-store-flush store)
             (ok (pg::physical-promotion-awaiting-child-initialization-p))
             (pg::physical-store-input store 184 (selected-replay-test-child-checkpoint 184))
             (pg::physical-store-flush store)
             (ng (pg::physical-promotion-awaiting-child-initialization-p))
             (ok (= 152 pg::*last-replay-lsn*))
             (ok (null (getf pg::*physical-recovery-state* :source-checkpoint))))
        (close pg::*clog-stream*) (setf pg::*clog-stream* nil))))

(deftest selected-promotion-does-not-defer-malformed-child-initialization
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (selected-replay-test-prepare store)
      (pg::physical-store-input
       store 152
       (pg::wal-encode-record pg::+wal-record-xid-reservation+ 0 152
                              (pg::wal-encode-xid-reservation 1999)))
      (pg::physical-store-flush store)
      (ok (signals (pg::physical-promotion-awaiting-child-initialization-p) 'error))
      (ok (= 96 pg::*last-replay-lsn*))
      (ok (= 1 (pg::physical-recovery-replay-timeline)))))

(deftest selected-promotion-readiness-rejects-regressing-child-allocation
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (selected-replay-test-prepare store)
      (pg::physical-store-input store 152 (sample-physical-record 152))
      (pg::physical-store-input store 184 (selected-replay-test-child-checkpoint 184 500))
      (pg::physical-store-flush store)
      (ok (signals (pg::physical-promotion-awaiting-child-initialization-p) 'error))
      (ok (= 96 pg::*last-replay-lsn*))
      (ok (= 1 (pg::physical-recovery-replay-timeline)))))

(deftest selected-receiver-statistics-use-durable-receipt-timeline
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (let* ((runtime (pg::make-physical-follow-runtime))
             (pg::*physical-follow-runtime* runtime)
             (pg::*physical-follow-statistics* (list :runtime runtime)))
        (ok (= 1 (getf (pg::physical-follow-statistics) :received-tli)))
        (selected-replay-test-prepare store)
        (ok (= 1 (pg::physical-recovery-replay-timeline)))
        (ok (= 2 (getf (pg::physical-follow-statistics) :received-tli)))
        (setf pg::*physical-follow-runtime* nil)
        (ok (null (pg::physical-follow-statistics))))))
