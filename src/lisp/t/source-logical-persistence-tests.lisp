(in-package #:pg-physical-tests)

(defun source-logical-persistence-facts ()
  (let ((history (pg::timeline-parse-history 1 "")))
    (labels ((record (enabled start)
               (pg::make-wal-record :version 8 :type 18 :xid 0 :start start :end (+ start 33)
                                    :payload (pg::wal-encode-logical-decoding-status-change enabled))))
      (let* ((off (pg::physical-logical-fact-next nil (record nil 32) 1 history))
             (on (pg::physical-logical-fact-next off (record t 112) 1 history))
             (later (pg::physical-logical-fact-next on (record nil 192) 1 history)))
        (values off on later)))))

(defun source-logical-persistence-state (source minimum)
  (list :phase 2 :system-id 42 :timeline 1 :manifest-digest 4 :checkpoint 32
        :replay-timeline 1 :source-logical-fact source :required-logical-fact minimum
        :required-applied (getf minimum :through) :required-timeline 1))

(deftest source-logical-progress-preserves-future-required-fact-and-owned-input
    (call-with-fpw-wal-fixture
     (lambda ()
       (multiple-value-bind (off on later) (source-logical-persistence-facts)
         (let* ((input (source-logical-persistence-state off on))
                (before (copy-tree input))
                (published (pg::physical-recovery-write-control input 2 65 3))
                (disk (pg::physical-recovery-read-control pg::*pgdata*)))
           (ok (equal before input))
           (ok (equal off (getf published :source-logical-fact)))
           (ok (not (eq off (getf published :source-logical-fact))))
           (ok (equal on (getf disk :logical-fact)))
           (ok (equal on (getf published :required-logical-fact)))
           (ok (not (eq on (getf published :required-logical-fact))))
           (ok (= 145 (getf disk :applied) (getf published :required-applied)))
           (setf (getf published :source-logical-fact) (copy-list on))
           (let ((reached (pg::physical-recovery-write-control published 2 145 3)))
             (ok (equal on (getf reached :required-logical-fact)))
             (setf (getf reached :source-logical-fact) (copy-list later))
             (let ((advanced (pg::physical-recovery-write-control reached 2 225 3)))
               (ok (equal later (getf advanced :required-logical-fact)))
               (dotimes (attempt 2)
                 (declare (ignorable attempt))
                 (let ((cold (pg::physical-recovery-read-control pg::*pgdata*)))
                   (ok (= 225 (getf cold :applied)))
                   (ok (equal later (getf cold :logical-fact)))
                   (ok (null (getf (getf cold :logical-fact) :enabled))))))))))))

(deftest source-logical-progress-failure-keeps-runtime-input-and-fact
    (dolist (action '(:error :throw))
      (dolist (point '(:before-write :before-fsync :after-fsync :before-rename
                       :after-rename :before-directory-fsync :after-directory-fsync :after-write))
        (call-with-fpw-wal-fixture
         (lambda ()
           (multiple-value-bind (off on) (source-logical-persistence-facts)
             (let* ((pg::*physical-recovery-control-test-hook* nil)
                    (state (pg::physical-recovery-write-control
                            (source-logical-persistence-state off off) 2 65 3))
                    (candidate (copy-tree state)) (attempts 0))
               (setf (getf candidate :source-logical-fact) (copy-list on))
               (let ((before (copy-tree candidate))
                     (pg::*physical-recovery-control-test-hook*
                       (lambda (current ignored)
                         (declare (ignore ignored))
                         (when (eq current point)
                           (incf attempts)
                           (ecase action
                             (:error (error "logical control fault"))
                             (:throw (throw 'logical-control-exit :aborted)))))))
                 (ok (eq :aborted
                         (handler-case
                             (catch 'logical-control-exit
                               (pg::physical-recovery-write-control candidate 2 145 3)
                               :completed)
                           (error () :aborted))))
                 (ok (= 1 attempts))
                 (ok (equal before candidate))
                 (ok (equal off (getf state :source-logical-fact))))
               (let ((cold (pg::physical-recovery-read-control pg::*pgdata*)))
                 (if (member point '(:after-rename :before-directory-fsync :after-directory-fsync :after-write))
                     (progn (ok (= 145 (getf cold :applied))) (ok (equal on (getf cold :logical-fact))))
                     (progn (ok (= 65 (getf cold :applied))) (ok (equal off (getf cold :logical-fact)))))))))))))

(deftest source-logical-save-progress-publishes-copied-facts-with-replay-frontier
    (call-with-fpw-wal-fixture
     (lambda ()
       (multiple-value-bind (off on) (source-logical-persistence-facts)
         (let* ((pg::*physical-recovery-state*
                  (pg::physical-recovery-write-control (source-logical-persistence-state off off) 2 65 3))
                (prior pg::*physical-recovery-state*)
                (policy pg::*wal-logical-policy*) (force pg::*logical-capture-force*))
           (pg::physical-recovery-save-progress 145 3 :logical-fact on)
           (ok (not (eq prior pg::*physical-recovery-state*)))
           (ok (eq policy pg::*wal-logical-policy*))
           (ok (eq force pg::*logical-capture-force*))
           (ok (equal off (getf prior :source-logical-fact)))
           (ok (equal on (getf pg::*physical-recovery-state* :source-logical-fact)))
           (ok (not (eq on (getf pg::*physical-recovery-state* :source-logical-fact))))
           (ok (equal on (getf (pg::physical-recovery-read-control pg::*pgdata*) :logical-fact))))))))
