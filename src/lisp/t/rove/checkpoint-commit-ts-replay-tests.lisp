(in-package #:pg-physical-tests)

(rove:deftest checkpoint-commit-ts-restartpoint-restores-historical-presence
    (dolist (known '(nil t))
      (call-with-resource-restartpoint-fixture
       (lambda (root store)
         (let* ((begin (pg::physical-store-flushed-lsn store))
                (start (+ begin pg::+wal-header-size+))
                (facts (append (pg::wal-copy-control-resource-fields
                                (getf pg::*physical-recovery-state* :source-resource-fact))
                               (when known (list :oldest-commit-ts-xid 0 :newest-commit-ts-xid 0))))
                (end (+ start (pg::wal-checkpoint-record-size facts)))
                (snapshot (append (list :version 2 :start start :end end :redo start
                                        :next-xid pg::*next-xid* :xid-reserved-through 1002
                                        :next-oid nil :time 0 :timeline 1 :prev-timeline 1
                                        :oldest-active-xid 0) facts)))
           (pg::physical-store-input store begin (sample-physical-record begin))
           (pg::physical-store-input
            store start (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                                               (pg::wal-encode-checkpoint-snapshot snapshot)))
           (pg::physical-store-flush store)
           (pg::physical-replay-from begin)
           (rove:ok (= end pg::*last-replay-lsn*))
           (rove:ok (equal snapshot (getf pg::*physical-recovery-state* :source-checkpoint)))
           (pg::physical-store-compact store end #'pg::physical-restartpoint-publish)
           (multiple-value-bind (metadata bytes) (pg::physical-restartpoint-read pg::*physical-recovery-state*)
             (declare (ignore bytes))
             (rove:ok (equal snapshot (getf metadata :source-checkpoint)))
             (when known
               (setf (getf (getf metadata :source-checkpoint) :oldest-commit-ts-xid) 3)
               (rove:ok (zerop (getf (getf pg::*physical-recovery-state* :source-checkpoint)
                                     :oldest-commit-ts-xid)))))
           (dolist (installed '(nil t))
             (let ((pg::*startup-installed-configuration*
                     (when installed
                       (pg::make-startup-configuration :settings (pg::startup-default-settings root)))))
               (dotimes (retry 2)
                 (declare (ignorable retry))
                 (setf (getf pg::*physical-recovery-state* :source-checkpoint) nil)
                 (rove:ok (= end (pg::physical-restartpoint-prepare-storage store)))
                 (rove:ok (equal snapshot (getf pg::*physical-recovery-state* :source-checkpoint)))
                 (rove:ok (eql (when known 0)
                               (getf (getf pg::*physical-recovery-state* :source-checkpoint)
                                     :oldest-commit-ts-xid)))
                 (rove:ok (eql (when known 0)
                               (getf (getf pg::*physical-recovery-state* :source-checkpoint)
                                     :newest-commit-ts-xid)))))))))))
