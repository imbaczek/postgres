;;;; Versioned native recovery state; C DBState semantics, not C disk layout.
(in-package #:pg-physical-tests)

(defun legacy-recovery-control (bytes)
  (let ((legacy (make-array 60 :element-type '(unsigned-byte 8) :initial-element 0)))
    (replace legacy bytes :end1 56 :end2 56)
    (replace legacy #(80 71 82 83 84 65 84 69))
    (pg::wal-set-u32 legacy 56 (pg::wal-crc32 legacy))
    legacy))

(defun sample-resource-fact (&optional (connections 100) (lsn 64))
  (list :resource-parameters (list connections 8 10 0 128) :resource-lsn lsn :resource-timeline 1))

(deftest recovery-control-resources-roundtrip-all-current-layouts
    (dolist (pending '(nil t))
      (dolist (selected '(nil t))
        (dolist (retained (if pending '(nil t) '(nil)))
          (dolist (known '(nil t))
            (let* ((fact (when known (sample-resource-fact)))
                   (arguments (append
                               (when selected '(:applied-timeline 2 :history-target 2 :history-digest 99))
                               (when pending '(:promotion-child 3 :promotion-generation 7
                                               :promotion-source-namespace :legacy))
                               (when retained '(:retained-reference
                                                (:root-id 9 :base-timeline 1 :base 32
                                                 :manifest-digest 456 :end 256 :end-digest 8)))))
                   (bytes (apply #'pg::physical-recovery-control-image
                                 (if pending 3 2) 123 1 456 32 256 1003
                                 :state-time 0 :resource-fact fact arguments))
                   (decoded (pg::physical-recovery-decode-control bytes)))
              (ok (= (+ (if pending 88 72) (if selected 16 0)
                        (if retained 44 0) (if known 48 0)) (length bytes)))
              (ok (equal fact (getf decoded :resource-fact)))
              (when fact
                (setf (first (getf (getf decoded :resource-fact) :resource-parameters)) 101)
                (ok (= 100 (first (getf fact :resource-parameters))))
                (ok (equal fact (getf (pg::physical-recovery-decode-control bytes) :resource-fact))))))))))

(deftest recovery-control-resources-reject-malformed-or-future-facts
    (dolist (change '((:resource-parameters nil) (:resource-parameters (100 8 10 0 9))
                      (:resource-lsn 0) (:resource-lsn 257) (:resource-timeline 0) (:resource-timeline 2)))
      (let ((fact (sample-resource-fact)))
        (setf (getf fact (first change)) (second change))
        (ok (intake-fails-p
             (lambda () (pg::physical-recovery-control-image 2 123 1 456 32 256 1003 :resource-fact fact))))))
  (let ((original (pg::physical-recovery-control-image
                   2 123 1 456 32 256 1003 :resource-fact (sample-resource-fact))))
    (dolist (offset '(68 96 112))
      (let ((bytes (copy-seq original)))
        (setf (aref bytes offset) (logxor 1 (aref bytes offset)))
        (pg::wal-set-u32 bytes 116 0)
        (pg::wal-set-u32 bytes 116 (pg::wal-crc32 bytes))
        (ok (intake-fails-p (lambda () (pg::physical-recovery-decode-control bytes))))))))

(deftest recovery-resource-frontier-keeps-required-facts-while-reconstructing-an-older-seed
    (let* ((old (sample-resource-fact 100 64)) (required (sample-resource-fact 50 192))
                                               (state (list :timeline 1 :replay-timeline 1 :required-applied 256 :required-timeline 1
                                                            :source-resource-fact old :required-resource-fact required)))
      (ok (equal (list 256 1 required)
                 (multiple-value-list (pg::physical-recovery-control-frontier state 128))))
      (ok (intake-fails-p (lambda () (pg::physical-recovery-control-frontier state 256))))
      (setf (getf state :source-resource-fact) (copy-tree required))
      (ok (equal (list 256 1 required)
                 (multiple-value-list (pg::physical-recovery-control-frontier state 256))))
      (let ((decreased (sample-resource-fact 25 320)))
        (setf (getf state :source-resource-fact) decreased)
        (ok (equal (list 384 1 decreased)
                   (multiple-value-list (pg::physical-recovery-control-frontier state 384)))))))

(deftest recovery-control-publishes-required-position-and-fact-in-one-image
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (pg::*physical-recovery-state* nil)
                                 (pg::*physical-recovery-failed-p* nil)
                                 (required (sample-resource-fact 50 192))
                                 (state (list :phase 2 :system-id 123 :timeline 1 :manifest-digest 456 :checkpoint 32
                                              :required-applied 256 :required-timeline 1 :observed-next-xid 1003
                                              :source-resource-fact (sample-resource-fact 100 64)
                                              :required-resource-fact required))
                                 (after (pg::physical-recovery-write-control state 2 128 1003))
                                 (disk (pg::physical-recovery-read-control root)))
        (ok (= 256 (getf disk :applied)))
        (ok (equal required (getf disk :resource-fact) ))
        (ok (equal required (getf after :required-resource-fact)))
        (ok (not (eq required (getf after :required-resource-fact))))
        (let* ((before (pg::recovery-read-bytes (pg::physical-recovery-control-path root)))
               (pg::*physical-recovery-control-test-hook*
                 (lambda (point status)
                   (declare (ignore status))
                   (when (eq point :before-rename) (error "injected resource publication failure")))))
          (setf (getf after :source-resource-fact) (sample-resource-fact 25 320))
          (ok (intake-fails-p (lambda () (pg::physical-recovery-write-control after 2 384 1003))))
          (ok (equal required (getf after :required-resource-fact)))
          (ok (equalp before (pg::recovery-read-bytes (pg::physical-recovery-control-path root))))))))

(cl-spec:defproperty recovery-control-positioned-resource-roundtrip
    ((connections (range integer 1 2000))
     (position (range integer 0 18446744073709551615))
     (pending-bit (range integer 0 1))
     (selected-bit (range integer 0 1)))
  (:about pg::physical-recovery-control-image pg::physical-recovery-decode-control)
  (:trials (:normal 200))
  (let* ((applied (+ 256 (mod position 18446744073709551360)))
         (fact (sample-resource-fact connections (- applied 64)))
         (pending (= pending-bit 1))
         (arguments (append (when (= selected-bit 1)
                              '(:applied-timeline 2 :history-target 2 :history-digest 99))
                            (when pending
                              '(:promotion-child 3 :promotion-generation 7
                                :promotion-source-namespace :legacy))))
         (bytes (apply #'pg::physical-recovery-control-image
                       (if pending 3 2) 123 1 456 32 applied 1003 :resource-fact fact arguments))
         (decoded (pg::physical-recovery-decode-control bytes)))
    (and (= applied (getf decoded :applied)) (equal fact (getf decoded :resource-fact)))))

(deftest generated-recovery-control-positioned-resources-roundtrip
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'recovery-control-positioned-resource-roundtrip :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))
      (ok (= 200 (getf data :trials)))))

(defun physical-resource-test-record (connections start)
  (let ((payload (pg::wal-encode-resource-parameters
                  (pg::make-resource-parameters connections 8 10 0 128))))
    (pg::make-wal-record :version 8 :type 16 :xid 0 :start start :end (+ start 64) :payload payload)))

(deftest resource-replay-transition-replaces-tuples-without-publishing-received-facts
    (let* ((history (pg::timeline-parse-history 1 ""))
           (old (sample-resource-fact 100 64))
           (pg::*physical-recovery-state* (list :source-resource-fact old))
           (increase (pg::physical-resource-fact-next old (physical-resource-test-record 200 128) 1 history))
           (decrease (pg::physical-resource-fact-next increase (physical-resource-test-record 50 256) 1 history)))
      (ok (equal (sample-resource-fact 200 192) increase))
      (ok (equal (sample-resource-fact 50 320) decrease))
      (ok (equal old (getf pg::*physical-recovery-state* :source-resource-fact)))
      (ok (equal old (sample-resource-fact 100 64)))
      (ok (intake-fails-p
           (lambda () (pg::physical-resource-fact-next decrease (physical-resource-test-record 200 128) 1 history))))
      (ok (intake-fails-p
           (lambda () (pg::physical-resource-fact-next old (physical-resource-test-record 200 128) 2 history))))))

(deftest resource-replay-checkpoint-anchor-must-agree-with-the-applied-fact
    (let* ((fact (sample-resource-fact 50 320))
           (snapshot (append '(:version 2 :start 384 :end 552 :redo 384 :next-xid 3
                               :xid-reserved-through 2 :next-oid nil :time 0
                               :timeline 1 :prev-timeline 1 :oldest-active-xid 0)
                             (copy-tree fact)))
           (record (pg::make-wal-record :version 8 :type 4 :xid 0 :start 384 :end 552
                                        :payload (pg::wal-encode-checkpoint-snapshot snapshot)))
           (history (pg::timeline-parse-history 1 "")))
      (ok (equal fact (pg::physical-resource-fact-next nil record 1 history)))
      (ok (equal fact (pg::physical-resource-fact-next fact record 1 history)))
      (ok (intake-fails-p
           (lambda () (pg::physical-resource-fact-next (sample-resource-fact 200 192) record 1 history))))
      (let ((copied (pg::physical-resource-fact-next fact record 1 history)))
        (setf (first (getf copied :resource-parameters)) 51)
        (ok (= 50 (first (getf fact :resource-parameters)))))))

(deftest resource-replay-carries-parent-facts-across-the-selected-child
    (let* ((fact (sample-resource-fact 100 64))
           (history (pg::timeline-parse-history 2 (format nil "1~c0/00000080~cchild~%" #\Tab #\Tab)))
           (record (pg::make-wal-record :version 8 :type 6 :xid 0 :start 184 :end 216
                                        :payload (empty-octets))))
      (ok (equal fact (pg::physical-resource-fact-next fact record 2 history)))
      (ok (null (pg::physical-resource-fact-next nil record 2 history)))))

(deftest recovery-control-v2-roundtrip-and-legacy-read
    (dolist (phase '(1 2))
      (dolist (state '(2 5))
        (let* ((applied (if (= phase 1) 32 96))
               (bytes (pg::physical-recovery-control-image
                       phase 123 1 456 32 applied 1003 :db-state state :state-time 123456))
               (control (pg::physical-recovery-decode-control bytes)))
          (ok (= 72 (length bytes)))
          (ok (= 3 (getf control :control-version)))
          (let ((v2 (copy-seq bytes)))
            (setf (aref v2 7) 50)
            (pg::wal-set-u32 v2 68 0)
            (pg::wal-set-u32 v2 68 (pg::wal-crc32 v2))
            (let ((old (pg::physical-recovery-decode-control v2)))
              (ok (= 2 (getf old :control-version)))
              (ok (= 1003 (getf old :next-xid)))
              (ok (= state (getf old :db-state)))))
          (ok (= phase (getf control :phase)))
          (ok (= state (getf control :db-state)))
          (ok (= 123456 (getf control :state-time)))
          (ok (= applied (getf control :applied)))
          (let ((legacy (pg::physical-recovery-decode-control (legacy-recovery-control bytes))))
            (ok (= 1 (getf legacy :control-version)))
            (ok (null (getf legacy :db-state)))
            (ok (null (getf legacy :state-time)))
            (ok (= applied (getf legacy :applied))))))))

(deftest recovery-control-v3-rejects-ambiguous-exhaustion-flags
    (dolist (mutation '(hidden-value missing-flag unknown-phase-bit))
      (let ((bytes (pg::physical-recovery-control-image
                    2 123 1 456 32 96 18446744073709551616)))
        (ecase mutation
          (hidden-value (pg::wal-set-u64 bytes 48 3))
          (missing-flag (pg::wal-set-u32 bytes 8 2))
          (unknown-phase-bit (pg::wal-set-u32 bytes 8 #xc0000002)))
        (pg::wal-set-u32 bytes 68 0)
        (pg::wal-set-u32 bytes 68 (pg::wal-crc32 bytes))
        (ok (handler-case (progn (pg::physical-recovery-decode-control bytes) nil)
              (error () t))))))

(deftest recovery-control-rejects-invalid-state-and-corruption
    (dolist (arguments '((0 123 1 0 32 32 1003 :db-state 2)
                         (1 123 1 456 32 32 1003 :db-state 6)
                         (2 123 1 456 32 96 1003 :state-time -9223372036854775809)
                         (2 123 1 456 32 96 1003 :state-time 9223372036854775808)))
      (ok (intake-fails-p (lambda () (apply #'pg::physical-recovery-control-image arguments)))))
  (let ((bytes (pg::physical-recovery-control-image 2 123 1 456 32 96 1003 :state-time 1)))
    (loop for offset below (length bytes)
          do (let ((bad (copy-seq bytes)))
               (setf (aref bad offset) (logxor 1 (aref bad offset)))
               (ok (intake-fails-p (lambda () (pg::physical-recovery-decode-control bad))))))
    ;; Even a CRC-valid unsupported primary state is not a recovery control.
    (pg::wal-set-u32 bytes 56 6)
    (pg::wal-set-u32 bytes 68 0)
    (pg::wal-set-u32 bytes 68 (pg::wal-crc32 bytes))
    (ok (intake-fails-p (lambda () (pg::physical-recovery-decode-control bytes))))))

(cl-spec:defproperty recovery-control-roundtrip-preserves-clean-and-active-state
    ((identifier (range integer 0 18446744073709551615))
     (position (range integer 0 18446744073709551615))
     (time (range integer -9223372036854775808 9223372036854775807))
     (state-bit (range integer 0 1)))
  (:about pg::physical-recovery-decode-control)
  (:trials (:normal 200))
  (let* ((applied (+ 32 (mod position 18446744073709551584)))
         (next-xid (+ 3 (mod position 18446744073709551613)))
         (state (if (zerop state-bit) 2 5))
         (bytes (pg::physical-recovery-control-image
                 2 identifier 1 identifier 32 applied next-xid
                 :db-state state :state-time time))
         (decoded (pg::physical-recovery-decode-control bytes)))
    (and (= identifier (getf decoded :system-id) (getf decoded :manifest-digest))
         (= applied (getf decoded :applied))
         (= next-xid (getf decoded :next-xid))
         (= state (getf decoded :db-state))
         (= time (getf decoded :state-time)))))

(deftest generated-recovery-control-roundtrips
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property
                  'recovery-control-roundtrip-preserves-clean-and-active-state :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest recovery-control-state-transitions-never-lower-progress
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (state (list :phase 2 :system-id 123 :timeline 1 :manifest-digest 456
                          :checkpoint 32 :required-applied 256 :observed-next-xid 100))
             (pg::*physical-recovery-state* state))
        (pg::physical-recovery-write-control state 2 256 100 :db-state 2)
        (ok (= 2 (getf (pg::physical-recovery-read-control root) :db-state)))
        (pg::physical-recovery-save-progress 128 80)
        (let ((control (pg::physical-recovery-read-control root)))
          (ok (= 5 (getf control :db-state)))
          (ok (= 256 (getf control :applied)))
          (ok (= 100 (getf control :next-xid))))
        (ok (= 5 (getf pg::*physical-recovery-state* :db-state))))))

(deftest recovery-control-returned-phase-matches-durable-transition
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (before (list :phase 1 :system-id 123 :timeline 1 :manifest-digest 456
                           :checkpoint 32 :required-applied 32 :observed-next-xid 100))
             (after (pg::physical-recovery-write-control before 2 96 100)))
        (ok (= 1 (getf before :phase)))
        (ok (null (getf before :db-state)))
        (ok (= 2 (getf after :phase)))
        (ok (= 5 (getf after :db-state)))
        (ok (= 2 (getf (pg::physical-recovery-read-control root) :phase))))))

(deftest recovery-control-cannot-mark-incomplete-reconstruction-clean
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (state (list :phase 2 :system-id 123 :timeline 1 :manifest-digest 456
                          :checkpoint 32 :required-applied 256 :observed-next-xid 100)))
        (pg::physical-recovery-write-control state 2 96 50)
        (let ((before (pg::recovery-read-bytes (pg::physical-recovery-control-path root))))
          (ok (intake-fails-p
               (lambda () (pg::physical-recovery-write-control state 2 96 50 :db-state 2))))
          (ok (equalp before
                      (pg::recovery-read-bytes (pg::physical-recovery-control-path root)))))
        (ok (= 5 (getf (pg::physical-recovery-read-control root) :db-state))))))

(deftest recovery-control-time-matches-signed-c-pg-time-t
    (dolist (time '(-9223372036854775808 -1 0 1 9223372036854775807))
      (let* ((bytes (pg::physical-recovery-control-image
                     2 123 1 456 32 96 1003 :state-time time))
             (decoded (pg::physical-recovery-decode-control bytes)))
        (ok (= time (getf decoded :state-time)))
        (when (= time -1)
          (ok (every (lambda (byte) (= byte 255)) (subseq bytes 60 68)))))))
