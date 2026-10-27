(in-package #:pg-physical-tests)

(defun fork-follow-history (fork)
  (pg::timeline-check-lineage
   (pg::timeline-parse-history 2 (format nil "1~c~X/~8,'0X~cprobe~%" #\Tab
                                         (ldb (byte 32 32) fork) (ldb (byte 32 0) fork) #\Tab))))

(deftest fork-follow-candidate-keeps-receive-and-replay-frontiers
    (let ((history (fork-follow-history 100)))
      (ok (pg::timeline-follow-candidate-p 1 50 90 history))
      (ok (pg::timeline-follow-candidate-p 1 50 100 history))
      (ok (pg::timeline-follow-candidate-p 1 100 100 history))
      (ng (pg::timeline-follow-candidate-p 1 50 101 history))
      (ng (pg::timeline-follow-candidate-p 1 101 101 history))
      (ng (pg::timeline-follow-candidate-p 2 100 100 history))
      (ng (pg::timeline-follow-candidate-p 3 0 0 history))))

(deftest fork-follow-candidate-checks-ancestor-interval
    (let ((history (pg::timeline-parse-history 5
                                               (format nil "1~c0/0000000A~%3~c0/00000014~%" #\Tab #\Tab))))
      (ok (pg::timeline-follow-candidate-p 3 10 20 history))
      (ng (pg::timeline-follow-candidate-p 3 9 20 history))
      (ng (pg::timeline-follow-candidate-p 3 10 21 history))
      (ng (pg::timeline-follow-candidate-p 2 10 10 history))
      (ok (pg::timeline-follow-candidate-p 1 0 10 history))))

(deftest fork-follow-candidate-validates-input-before-selection
    (let ((history (fork-follow-history 100)))
      (dolist (arguments (list (list 0 0 0 history)
                               (list 4294967296 0 0 history)
                               (list 1 -1 0 history)
                               (list 1 0 18446744073709551616 history)
                               (list 1 20 10 history)
                               (list 1 0 0 nil)))
        (ok (signals (apply #'pg::timeline-follow-candidate-p arguments) 'error)))
      (setf (pg::timeline-history-entry-begin (first history)) 99)
      (ok (signals (pg::timeline-follow-candidate-p 1 0 0 history) 'error))))

(cl-spec:defproperty fork-follow-requires-the-entire-durable-parent-prefix
    ((fork (range integer 0 18446744073709551615))
     (a (range integer 0 18446744073709551615))
     (b (range integer 0 18446744073709551615)))
  (:about pg::timeline-follow-candidate-p)
  (:trials (:normal 200))
  (let ((applied (min a b)) (received (max a b)))
    (eql (<= received fork)
         (pg::timeline-follow-candidate-p 1 applied received (fork-follow-history fork)))))

(deftest generated-fork-follow-frontiers
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'fork-follow-requires-the-entire-durable-parent-prefix :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
