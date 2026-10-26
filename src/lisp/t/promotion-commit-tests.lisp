;;;; A child checkpoint bridges pending recovery to restartable primary control.
(in-package #:pg-physical-tests)

(defun sample-promotion-checkpoint-state (fork &key (bound 1002) (next 50) (child 2))
  (list :phase 3 :applied fork :timeline 1 :promotion-child child :next-xid next :state-time 1793030000
        :source-reservation (list :version 1 :through fork :bound bound :complete t)))

(deftest child-checkpoint-plan-preserves-reservation-and-native-rotation
    (dolist (fork (list 216 (- pg::+wal-segment-size+ 208) (- pg::+wal-segment-size+ 207)
                        (- pg::+wal-segment-size+ 56) (- pg::+wal-segment-size+ 55)
                        pg::+wal-segment-size+))
      (let* ((state (sample-promotion-checkpoint-state fork))
             (plan (pg::physical-promotion-checkpoint-plan state 20000 1003))
             (snapshot (getf plan :snapshot)) (frames (getf plan :frames)))
        (ok (= 1003 (getf snapshot :next-xid)))
        (ok (= 1002 (getf snapshot :xid-reserved-through)))
        (ok (= 2 (getf snapshot :timeline))) (ok (= 1 (getf snapshot :prev-timeline)))
        (ok (= 20000 (getf snapshot :next-oid)))
        (ok (= 3 (length frames)))
        (loop with previous = fork
              for (start bytes) in frames
              do (ok (<= previous start))
                 (ok (<= (+ (mod start pg::+wal-segment-size+) (length bytes)) pg::+wal-segment-size+))
                 (ok (= start (pg::wal-record-start (pg::wal-decode-record bytes start))))
                 (setf previous (+ start (length bytes))))
        (ok (equal snapshot (pg::wal-checkpoint-record-snapshot
                             (pg::wal-decode-record (second (third frames)) (first (third frames)))))))))

(deftest child-checkpoint-plan-preserves-exhaustion-and-rejects-overflow
    (let* ((state (sample-promotion-checkpoint-state 216 :bound #xffffffffffffffff
                                                         :next #x10000000000000000))
           (plan (pg::physical-promotion-checkpoint-plan state 20000 #x10000000000000000)))
      (ok (= #x10000000000000000 (getf (getf plan :snapshot) :next-xid))))
  (ok (intake-fails-p
       (lambda () (pg::physical-promotion-checkpoint-plan
                   (sample-promotion-checkpoint-state #xffffffffffffffc0) 20000 1003)))))

(deftest child-control-publication-commits-only-after-checkpoint-and-data-sync
    (with-promotion-fixture (root store)
      (declare (ignore root store))
      (prepare-promotion-child)
      (let ((old (lineage-test-bytes (pg::wal-control-path)))
            (pg::*physical-promotion-test-hook*
              (lambda (point) (when (eq point :after-child-checkpoint-flush) (throw 'staged-child :staged)))))
        (ok (eq :staged (catch 'staged-child
                          (call-with-promotion-exclusion #'pg::physical-promotion-publish-child-control))))
        (ok (equalp old (lineage-test-bytes (pg::wal-control-path))))
        (ok pg::*physical-recovery-failed-p*))
      (let ((pg::*physical-recovery-failed-p* nil))
        (ok (= 424 (call-with-promotion-exclusion #'pg::physical-promotion-publish-child-control)))
        (multiple-value-bind (checkpoint durable snapshot child checkpoint-child namespace) (pg::wal-read-control-fields)
          (ok (= 424 checkpoint durable)) (ok (= 2 child checkpoint-child))
          (ok (eq :timeline namespace)) (ok (= 1003 (getf snapshot :next-xid))))
        (multiple-value-bind (snapshot history) (pg::physical-promotion-check-child-control 2 2)
          (ok (= 1003 (getf snapshot :next-xid))) (ok (= 2 (pg::timeline-at-point 304 history))))
        (ok (equal "57P03" (recovery-state-of #'pg::check-recovery-available))))))

(deftest child-control-publication-uncertainty-never-rolls-back-parent-control
    (dolist (point '(:after-child-control-rename :after-child-control-directory-fsync))
      (with-promotion-fixture (root store)
        (declare (ignore root store))
        (prepare-promotion-child)
        (let ((pg::*physical-promotion-test-hook*
                (lambda (seen) (when (eq point seen) (throw 'uncertain-child :aborted)))))
          (ok (eq :aborted (catch 'uncertain-child
                             (call-with-promotion-exclusion #'pg::physical-promotion-publish-child-control)))))
        (ok pg::*physical-recovery-failed-p*)
        (ok (= 2 (nth-value 3 (pg::wal-read-control-fields))))
        (ok (progn (pg::physical-promotion-check-child-control 2 2) t)))))

(cl-spec:defproperty child-checkpoint-planning-keeps-all-records-in-the-child
    ((fork (range integer 32 33554432)) (bound (range integer 100 100000))
                                        (local (range integer 3 100000)) (child (range integer 2 4294967295)))
  (:about pg::physical-promotion-checkpoint-plan)
  (:trials (:normal 100))
  (let* ((state (sample-promotion-checkpoint-state fork :bound bound :child child))
         (plan (pg::physical-promotion-checkpoint-plan state 20000 local))
         (snapshot (getf plan :snapshot)))
    (and (= child (getf snapshot :timeline)) (= 1 (getf snapshot :prev-timeline))
         (= (pg::canonical-next-full-xid (max 50 (1+ bound) local)) (getf snapshot :next-xid))
         (every (lambda (frame)
                  (destructuring-bind (start bytes) frame
                    (and (<= fork start)
                         (<= (+ (mod start pg::+wal-segment-size+) (length bytes)) pg::+wal-segment-size+))))
                (getf plan :frames)))))

(deftest generated-child-checkpoint-plan
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'child-checkpoint-planning-keeps-all-records-in-the-child :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
