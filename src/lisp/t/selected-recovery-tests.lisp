(in-package #:pg-physical-tests)

(deftest selected-recovery-control-keeps-base-applied-and-target-identities
    (dolist (applied-timeline '(1 2))
      (let* ((bytes (pg::physical-recovery-control-image
                     2 123 1 456 64 128 100 :state-time 42
                     :applied-timeline applied-timeline :history-target 5 :history-digest 0))
             (control (pg::physical-recovery-decode-control bytes)))
        (ok (= 88 (length bytes)))
        (ok (= 3 (getf control :control-version)))
        (ok (= 1 (getf control :timeline)))
        (ok (= applied-timeline (getf control :applied-timeline)))
        (ok (= 5 (getf control :history-target)))
        (ok (= 0 (getf control :history-digest)))
        (ok (= 128 (getf control :applied))))))

(deftest selected-recovery-control-distinguishes-same-length-ordinary-and-pending
    (let* ((ordinary (pg::physical-recovery-control-image
                      2 123 1 456 64 128 100 :state-time 42
                      :applied-timeline 2 :history-target 2 :history-digest 99))
           (pending (pg::physical-recovery-control-image
                     3 123 1 456 64 128 100 :state-time 42
                     :promotion-child 2 :promotion-generation 7 :promotion-source-namespace :legacy)))
      (ok (= 88 (length ordinary) (length pending)))
      (ok (= 2 (getf (pg::physical-recovery-decode-control ordinary) :phase)))
      (ok (= 3 (getf (pg::physical-recovery-decode-control pending) :phase)))
      (ok (null (getf (pg::physical-recovery-decode-control pending) :history-target)))))

(deftest selected-pending-control-binds-actual-promotion-parent
    (let* ((bytes (pg::physical-recovery-control-image
                   3 123 1 456 64 128 100 :state-time 42
                   :applied-timeline 2 :history-target 2 :history-digest 99
                   :promotion-child 3 :promotion-generation 7 :promotion-source-namespace :legacy))
           (control (pg::physical-recovery-decode-control bytes)))
      (ok (= 104 (length bytes)))
      (ok (= 4 (getf control :control-version)))
      (ok (= 1 (getf control :timeline)))
      (ok (= 2 (getf control :applied-timeline)))
      (ok (= 3 (getf control :promotion-child)))
      (ok (= 7 (getf control :promotion-generation)))))

(deftest selected-pending-control-can-be-read-from-disk
    (with-physical-store-directory (root)
      (pg::wal-atomic-write-bytes
       (pg::physical-recovery-control-path root)
       (pg::physical-recovery-control-image
        3 123 1 456 64 128 100 :applied-timeline 2 :history-target 2 :history-digest 99
                               :promotion-child 3 :promotion-generation 7 :promotion-source-namespace :legacy))
      (ok (= 2 (getf (pg::physical-recovery-read-control root) :applied-timeline)))))

(deftest selected-recovery-control-rejects-contradictory-extensions
    (dolist (options '((:applied-timeline 2)
                       (:history-target 2)
                       (:history-digest 0)
                       (:applied-timeline 2 :history-target 1 :history-digest 99)
                       (:applied-timeline 3 :history-target 2 :history-digest 99)
                       (:applied-timeline 0 :history-target 2 :history-digest 99)
                       (:applied-timeline 1 :history-target 4294967296 :history-digest 99)
                       (:history-target 2 :history-digest 18446744073709551616)))
      (ok (signals (apply #'pg::physical-recovery-control-image 2 123 1 456 64 128 100 options) 'error)))
  (ok (signals (pg::physical-recovery-control-image
                1 123 1 456 64 64 100 :history-target 2 :history-digest 99) 'error))
  (ok (signals (pg::physical-recovery-control-image
                3 123 1 456 64 128 100
                :applied-timeline 2 :history-target 2 :history-digest 99
                :promotion-child 2 :promotion-generation 7 :promotion-source-namespace :legacy) 'error)))

(deftest selected-recovery-control-checks-extension-after-crc
    (let ((bytes (pg::physical-recovery-control-image
                  2 123 1 456 64 128 100
                  :applied-timeline 2 :history-target 2 :history-digest 99)))
      (pg::wal-set-u32 bytes 72 0)
      (pg::wal-set-u32 bytes 84 0)
      (pg::wal-set-u32 bytes 84 (pg::wal-crc32 bytes))
      (ok (signals (pg::physical-recovery-decode-control bytes) 'error))))

(cl-spec:defproperty selected-recovery-control-roundtrips-independent-timelines
    ((base (range integer 1 4294967293))
     (digest (range integer 0 18446744073709551615))
     (on-child boolean))
  (:about pg::physical-recovery-control-image)
  (:trials (:normal 200))
  (let* ((target (1+ base)) (applied-timeline (if on-child target base))
                            (bytes (pg::physical-recovery-control-image
                                    2 99 base 7 64 128 100 :state-time 42
                                    :applied-timeline applied-timeline :history-target target :history-digest digest))
                            (control (pg::physical-recovery-decode-control bytes)))
    (and (= base (getf control :timeline))
         (= applied-timeline (getf control :applied-timeline))
         (= target (getf control :history-target))
         (= digest (getf control :history-digest)))))

(deftest generated-selected-recovery-control
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'selected-recovery-control-roundtrips-independent-timelines :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest selected-recovery-publication-keeps-required-lsn-and-timeline-paired
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (before (list :phase 2 :system-id 123 :timeline 1 :manifest-digest 456
                           :checkpoint 64 :required-applied 256 :required-timeline 2
                           :observed-next-xid 100 :replay-timeline 1 :history-target 2 :history-digest 99))
             (pg::*physical-recovery-state* before)
             (after (pg::physical-recovery-write-control before 2 128 80))
             (control (pg::physical-recovery-read-control root)))
        (ok (= 256 (getf control :applied)))
        (ok (= 2 (getf control :applied-timeline)))
        (ok (= 1 (getf after :replay-timeline)))
        (ok (= 2 (getf after :required-timeline)))
        (ok (= 100 (getf control :next-xid)))
        (ok (= 1 (getf before :replay-timeline)))
        (ok (null (getf before :db-state))))))

(deftest selected-recovery-publication-rejects-equal-lsn-with-another-timeline
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (state (list :phase 2 :system-id 123 :timeline 1 :manifest-digest 456
                          :checkpoint 64 :required-applied 256 :required-timeline 2
                          :observed-next-xid 100 :replay-timeline 1 :history-target 2 :history-digest 99))
             (pg::*physical-recovery-state* state))
        (ok (signals (pg::physical-recovery-write-control state 2 256 100) 'error))
        (ng (probe-file (pg::physical-recovery-control-path root))))))

(deftest selected-recovery-publishes-new-replay-timeline-only-after-control
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (before (list :phase 2 :system-id 123 :timeline 1 :manifest-digest 456
                           :checkpoint 64 :required-applied 64 :required-timeline 1
                           :observed-next-xid 100 :replay-timeline 1 :history-target 2 :history-digest 99))
             (pg::*physical-recovery-state* before))
        (pg::physical-recovery-save-progress 128 100 :replay-timeline 2)
        (ok (= 1 (getf before :replay-timeline)))
        (ok (= 2 (getf pg::*physical-recovery-state* :replay-timeline)))
        (ok (= 2 (getf pg::*physical-recovery-state* :required-timeline)))
        (ok (= 2 (getf (pg::physical-recovery-read-control root) :applied-timeline))))))

(deftest selected-recovery-attaches-descendant-without-changing-replay
    (with-physical-store-directory (root)
      (let* ((store (pg::physical-store-open root 123 1 :create t :start-lsn 64))
             (eor (pg::wal-encode-end-of-recovery-record 64 42 2 1 1))
             (state (list :system-id 123 :timeline 1 :end 64 :required-applied 64
                          :required-timeline 1 :replay-timeline 1)))
        (unwind-protect
             (progn
               (pg::physical-store-select-history store 2 (format nil "1~c0/00000040~% " #\Tab) eor)
               (pg::physical-store-input store 64 eor)
               (pg::physical-store-flush store)
               (let ((attached (pg::physical-recovery-attach-selected-store state store)))
                 (ok (= 1 (getf attached :replay-timeline)))
                 (ok (= 1 (getf attached :timeline)))
                 (ok (= 2 (getf attached :history-target)))
                 (ok (null (getf state :history-target)))
                 (setf (getf attached :required-applied) 120 (getf attached :required-timeline) 2)
                 (ok (pg::physical-recovery-attach-selected-store attached store))
                 (setf (getf attached :required-timeline) 1)
                 (ok (signals (pg::physical-recovery-attach-selected-store attached store) 'error))))
          (pg::physical-store-close store)))))

(deftest selected-recovery-rejects-a-reference-outside-the-durable-chain
    (with-physical-store-directory (root)
      (let ((store (pg::physical-store-open root 123 1 :create t :start-lsn 64)))
        (unwind-protect
             (progn
               (pg::physical-store-select-history store 2 (format nil "1~c0/00000040~%" #\Tab)
                                                  (pg::wal-encode-end-of-recovery-record 64 42 2 1 1))
               (ok (signals (pg::physical-recovery-check-history-reference store 2
                                                                           (logxor 1 (getf (pg::physical-store-selection store) :digest))) 'error))
               (ok (signals (pg::physical-recovery-check-history-reference store 2 nil) 'error)))
          (pg::physical-store-close store)))))

(deftest selected-eor-invalidates-prospective-source-proof-without-changing-snapshots
    (let* ((raw (pg::wal-encode-end-of-recovery-record 64 42 2 1 1))
           (record (pg::wal-decode-record raw 64))
           (reservation (list :version 1 :through 64 :bound 999 :complete t))
           (membership (pg::running-xacts-anchor (list :next-xid 50 :running '(3 49) :complete t) 64))
           (retained (copy-tree membership))
           (after (pg::physical-source-reservation-next reservation record 50)))
      (ok (= 120 (getf after :through)))
      (ok (= 999 (getf after :bound)))
      (ng (getf after :complete))
      (ok (null (pg::physical-source-xids-next membership record 50)))
      (ok (equal membership retained))
      (ok (getf reservation :complete))))
