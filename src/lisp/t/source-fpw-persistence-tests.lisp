(in-package #:pg-physical-tests)

(defun source-fpw-persistence-facts ()
  (let ((history (pg::timeline-parse-history 1 "")))
    (labels ((record (enabled start)
               (pg::make-wal-record :version 8 :type 17 :start start :end (+ start 48) :xid 0
                                    :payload (pg::wal-encode-full-page-writes-change enabled))))
      (let* ((off (pg::physical-fpw-fact-next nil (record nil 32) 1 history))
             (on (pg::physical-fpw-fact-next off (record t 112) 1 history))
             (later (pg::physical-fpw-fact-next on (record nil 192) 1 history)))
        (values off on later)))))

(defun source-fpw-persistence-state (source minimum)
  (list :phase 2 :system-id 42 :timeline 1 :manifest-digest 4 :checkpoint 32
        :replay-timeline 1 :source-fpw-fact source :required-fpw-fact minimum
        :required-applied (getf minimum :through) :required-timeline 1))

(deftest source-fpw-progress-preserves-later-required-fact-and-input-state
    (call-with-fpw-wal-fixture
     (lambda ()
       (multiple-value-bind (off on later) (source-fpw-persistence-facts)
         (let* ((pg::*physical-recovery-control-test-hook* nil)
                (input (source-fpw-persistence-state off on))
                (before (copy-tree input))
                (published (pg::physical-recovery-write-control input 2 80 3))
                (disk (pg::physical-recovery-read-control pg::*pgdata*)))
           (ok (equal before input))
           (ok (equal off (getf published :source-fpw-fact)))
           (ok (equal on (getf published :required-fpw-fact)))
           (ok (not (eq on (getf published :required-fpw-fact))))
           (ok (equal on (getf disk :fpw-fact)))
           (ok (= 160 (getf disk :applied) (getf published :required-applied)))
           (setf (getf published :source-fpw-fact) (copy-list on))
           (let ((reached (pg::physical-recovery-write-control published 2 160 3)))
             (ok (equal on (getf reached :required-fpw-fact)))
             (setf (getf reached :source-fpw-fact) (copy-list later))
             (let ((advanced (pg::physical-recovery-write-control reached 2 240 3)))
               (ok (= 240 (getf advanced :required-applied)))
               (ok (null (getf (getf advanced :required-fpw-fact) :enabled)))
               (dotimes (attempt 2)
                 (declare (ignorable attempt))
                 (let ((cold (pg::physical-recovery-read-control pg::*pgdata*)))
                   (ok (equal later (getf cold :fpw-fact)))
                   (ok (null (getf (getf cold :fpw-fact) :enabled)))
                   (ok (= 192 (getf (getf cold :fpw-fact) :last-disable-start))))))))))))

(deftest source-fpw-recovery-control-largest-image-fits-all-reader-bounds
    (call-with-fpw-wal-fixture
     (lambda ()
       (multiple-value-bind (off on) (source-fpw-persistence-facts)
         (declare (ignore on))
         (let ((bytes (pg::physical-recovery-control-image
                       3 42 1 4 32 80 3 :state-time 0
                       :applied-timeline 1 :history-target 3 :history-digest 7
                       :promotion-child 4 :promotion-generation 9 :promotion-source-namespace :legacy
                       :retained-reference '(:root-id 1 :base-timeline 1 :base 32
                                             :manifest-digest 4 :end 80 :end-digest 7)
                       :resource-fact '(:resource-parameters (100 8 10 0 128)
                                        :resource-lsn 16 :resource-timeline 1)
                       :fpw-fact off)))
           (ok (= 268 (length bytes)))
           (ok (= 285 pg::+physical-recovery-control-maximum-bytes+))
           (ok (= 285 (second (assoc "physical-recovery.control"
                                     (pg::physical-backup-metadata-bounds) :test #'equal))))
           (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path) bytes)
           (let ((control (pg::physical-recovery-read-control pg::*pgdata*)))
             (ok (= 3 (getf control :phase)))
             (ok (equal off (getf control :fpw-fact)))
             (ok (getf control :retained-reference))
             (ok (getf control :resource-fact))))))))

(deftest source-fpw-progress-publication-failures-preserve-runtime-input
    (dolist (point '(:before-write :before-fsync :after-fsync :before-rename
                     :after-rename :before-directory-fsync :after-directory-fsync :after-write))
      (call-with-fpw-wal-fixture
       (lambda ()
         (multiple-value-bind (off on) (source-fpw-persistence-facts)
           (let* ((pg::*physical-recovery-control-test-hook* nil)
                  (state (pg::physical-recovery-write-control
                          (source-fpw-persistence-state off off) 2 80 3))
                  (proposed (copy-tree state)) (attempts 0))
             (setf (getf proposed :source-fpw-fact) (copy-list on))
             (let ((before (copy-tree proposed))
                   (pg::*physical-recovery-control-test-hook*
                     (lambda (stage value)
                       (declare (ignore value))
                       (when (eq stage point) (incf attempts)
                             (error "injected source FPW control publication failure")))))
               (ok (intake-fails-p
                    (lambda () (pg::physical-recovery-write-control proposed 2 160 3))))
               (ok (= 1 attempts))
               (ok (equal before proposed)))
             (let* ((renamed (member point '(:after-rename :before-directory-fsync
                                             :after-directory-fsync :after-write)))
                    (expected (if renamed on off))
                    (disk (pg::physical-recovery-read-control pg::*pgdata*)))
               (ok (= (if renamed 160 80) (getf disk :applied)))
               (ok (equal expected (getf disk :fpw-fact))))))))))
