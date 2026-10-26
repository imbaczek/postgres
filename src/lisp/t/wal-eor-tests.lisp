;;;; Native end-of-recovery syntax is separate from timeline publication authority.
(in-package #:pg-physical-tests)

(defun eor-test-frame (&optional (start 32))
  (pg::wal-encode-end-of-recovery-record start -1 5 2 2))

(defun eor-test-restamp (bytes)
  (pg::wal-set-u32 bytes 28 0)
  (pg::wal-set-u32 bytes 28 (pg::wal-crc32 bytes))
  bytes)

(deftest eor-native-fields-match-the-zero-initialized-pinned-c-layout
    (let* ((payload (pg::wal-encode-end-of-recovery-payload -1 5 2 2))
           (expected #(255 255 255 255 255 255 255 255 5 0 0 0 2 0 0 0 2 0 0 0 0 0 0 0)))
      (ok (equalp expected payload))
      (ok (equal '(:end-time -1 :timeline 5 :prev-timeline 2 :wal-level 2)
                 (pg::wal-decode-end-of-recovery-payload payload)))))

(deftest eor-frame-keeps-current-format-without-changing-timeline-or-progress
    (let ((pg::*wal-insert-timeline* 1) (pg::*wal-checkpoint-timeline* 1)
                                        (pg::*last-replay-lsn* 32) (pg::*wal-durable-lsn* 32))
      (let* ((raw (eor-test-frame)) (record (pg::wal-decode-record raw 32)))
        (ok (= pg::+wal-version+ (pg::wal-record-version record)))
        (ok (= pg::+wal-record-end-of-recovery+ (pg::wal-record-type record)))
        (ok (zerop (pg::wal-record-xid record)))
        (ok (= 32 (pg::wal-record-start record)))
        (ok (= 88 (pg::wal-record-end record)))
        (ok (= 5 (getf (pg::wal-end-of-recovery-record-info record) :timeline))))
      (ok (= 1 pg::*wal-insert-timeline* pg::*wal-checkpoint-timeline*))
      (ok (= 32 pg::*last-replay-lsn* pg::*wal-durable-lsn*))))

(deftest eor-is-invalid-in-older-native-record-formats-even-with-valid-crc
    (loop for version from 2 to 6
          do (let ((bytes (eor-test-frame)))
               (pg::wal-set-u16 bytes 4 version)
               (eor-test-restamp bytes)
               (ok (intake-fails-p (lambda () (pg::wal-decode-record bytes 32)))))))

(deftest malformed-eor-cannot-enter-receive-callbacks-even-with-valid-crc
    (dolist (change '((20 1) (40 1) (44 5) (48 3) (52 1)))
      (let ((bytes (eor-test-frame)) (seen 0)
                                     (decoder (pg::make-physical-wal-decoder :start-lsn 32)))
        (pg::wal-set-u32 bytes (first change) (second change))
        (eor-test-restamp bytes)
        (ok (intake-fails-p
             (lambda () (pg::physical-wal-decode-input decoder 32 bytes
                                                       :on-record (lambda (&rest ignored)
                                                                    (declare (ignore ignored))
                                                                    (incf seen))))))
        (ok (zerop seen))))
  (dolist (length '(23 25))
    (let ((bytes (pg::wal-encode-record pg::+wal-record-end-of-recovery+ 0 32
                                        (make-array length :element-type '(unsigned-byte 8)))))
      (ok (intake-fails-p (lambda () (pg::wal-decode-record bytes 32)))))))

(deftest eor-intake-cannot-publish-progress-under-the-old-timeline
    (with-physical-store-directory (root)
      (let* ((store (pg::physical-store-open root 123 1 :create t :start-lsn 32))
             (control (pg::physical-store-path store "receive.control"))
             (before (lineage-test-bytes control)))
        (unwind-protect
             (progn
               (ok (intake-fails-p (lambda () (pg::physical-store-input store 32 (eor-test-frame)))))
               (ok (= 32 (pg::physical-store-written-lsn store) (pg::physical-store-flushed-lsn store)))
               (ok (not (probe-file (pg::physical-store-segment-path store 0))))
               (ok (equalp before (lineage-test-bytes control))))
          (pg::physical-store-close store)))))

(deftest eor-cannot-mutate-primary-or-history-batch-state-before-a-verified-transition
    (let ((record (pg::wal-decode-record (eor-test-frame) 32))
          (pg::*wal-checkpoint-lsn* 0) (pg::*wal-insert-timeline* 1)
          (pg::*wal-recovery-reservation-batch-clean-p* t)
          (pg::*wal-recovery-group-active* nil)
          (pg::*wal-recovery-page-images* nil))
      (ok (intake-fails-p (lambda () (pg::wal-replay-record record))))
      (ok pg::*wal-recovery-reservation-batch-clean-p*)
      (ok (null pg::*wal-recovery-group-active*))
      (ok (null pg::*wal-recovery-page-images*))
      (ok (intake-fails-p
           (lambda () (pg::physical-history-accept-record record "fixture" (make-hash-table)))))
      (ok pg::*wal-recovery-reservation-batch-clean-p*)
      (ok (= 1 pg::*wal-insert-timeline*))))

(deftest eor-cannot-be-appended-through-ordinary-parent-wal
    (call-with-wal-allocation-fixture
     (lambda ()
       (let ((before (lineage-test-bytes (pg::wal-control-path))))
         (ok (intake-fails-p
              (lambda () (pg::wal-append-record pg::+wal-record-end-of-recovery+ 0
                                                (pg::wal-encode-end-of-recovery-payload -1 2 1 1)))))
         (ok (null pg::*wal-poisoned*))
         (ok (zerop pg::*wal-end-lsn*))
         (ok (null (pg::wal-discover-segments)))
         (ok (equalp before (lineage-test-bytes (pg::wal-control-path))))))))

(deftest eor-startup-refuses-uncommitted-tail-without-adopting-or-truncating-it
    (dolist (damage '(nil :bad-level :short-body :bad-length))
      (call-with-wal-allocation-fixture
       (lambda ()
         (let* ((before (lineage-test-bytes (pg::wal-control-path)))
                (raw (eor-test-frame 0)) (path (pg::wal-segment-path 0)))
           (case damage
             (:bad-level (pg::wal-set-u32 raw 48 3) (eor-test-restamp raw))
             (:short-body (setf raw (subseq raw 0 40)))
             (:bad-length (pg::wal-set-u32 raw 8 16) (eor-test-restamp raw)))
           (pg::wal-atomic-write-bytes path raw)
           (ok (intake-fails-p (lambda () (pg::wal-init))))
           (ok (equalp before (lineage-test-bytes (pg::wal-control-path))))
           (ok (equalp raw (lineage-test-bytes path)))
           (ok (zerop pg::*wal-durable-lsn*))
           (ok (= 1 pg::*wal-insert-timeline*)))))))

(deftest eor-cannot-be-crossed-by-slot-acknowledgment-or-rebuild-scans
    (call-with-wal-allocation-fixture
     (lambda ()
       (let* ((raw (eor-test-frame 0))
              (slot (pg::make-wal-replication-slot :name "eor-ack" :database-oid 1
                                                   :restart-lsn 0 :consistent-lsn 0 :confirmed-flush-lsn 0)))
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 0) raw)
         (setf pg::*wal-end-lsn* (length raw) pg::*wal-durable-lsn* (length raw))
         (ok (intake-fails-p (lambda () (pg::wal-slot-terminal-ack-p slot (length raw)))))
         (ok (intake-fails-p (lambda () (pg::wal-slot-rebuild-progress slot))))
         (ok (zerop (pg::wal-replication-slot-confirmed-flush-lsn slot)))
         (ok (zerop (pg::wal-replication-slot-restart-lsn slot)))))))

(deftest eor-cannot-silently-advance-a-logical-slot
    (call-with-wal-allocation-fixture
     (lambda ()
       (let* ((owner (gensym "EOR-READER")) (pg::*wal-logical-slot-owner* owner)
                                            (pg::*wal-logical-slot-read-progress* nil)
                                            (slot (pg::make-wal-replication-slot :name "eor" :database-oid 1 :restart-lsn 0
                                                                                 :consistent-lsn 0 :confirmed-flush-lsn 0 :active-owner owner))
                                            (raw (eor-test-frame 0)))
         (setf (gethash "eor" pg::*wal-replication-slots*) slot)
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 0) raw)
         (setf pg::*wal-end-lsn* (length raw) pg::*wal-durable-lsn* (length raw))
         (ok (intake-fails-p (lambda () (pg::wal-logical-slot-read-transactions "eor"))))
         (ok (null pg::*wal-logical-slot-read-progress*))
         (ok (zerop (pg::wal-replication-slot-confirmed-flush-lsn slot)))
         (ok (zerop (pg::wal-replication-slot-restart-lsn slot)))))))

(deftest eor-cannot-be-exported-or-confirmed-through-an-unverified-source-timeline
    (call-with-wal-allocation-fixture
     (lambda ()
       (let* ((owner (gensym "EOR-SENDER"))
              (pg::*physical-source-pins* (make-hash-table :test #'equal))
              (pg::*physical-source-senders* (make-hash-table :test #'equal))
              (pin (pg::make-physical-source-pin :name "eor" :consistent-lsn 0 :restart-lsn 0))
              (raw (eor-test-frame 0)))
         (setf (gethash "eor" pg::*physical-source-pins*) pin
               (gethash "eor" pg::*physical-source-senders*) owner)
         (pg::wal-atomic-write-bytes (pg::wal-segment-path 0) raw)
         (setf pg::*wal-end-lsn* (length raw) pg::*wal-durable-lsn* (length raw))
         (ok (intake-fails-p (lambda () (pg::physical-source-read-next "eor" owner 0))))
         (ok (null (pg::physical-source-pin-confirmed-lsn pin)))
         (ok (zerop (pg::physical-source-pin-restart-lsn pin)))))))

(deftest eor-transition-checks-history-previous-timeline-and-c-minimum-recovery-rule
    (let* ((record (pg::wal-decode-record (eor-test-frame) 32))
           (history (lineage-test-history 32 5))
           (pg::*wal-insert-timeline* 1) (pg::*last-replay-lsn* 32))
      ;; This record says previous=2, whereas the helper history normally roots at 1.
      (setf (pg::timeline-history-entry-timeline (second history)) 2)
      (ok (= 5 (pg::timeline-check-end-of-recovery record 2 history)))
      (ok (= 5 (pg::timeline-check-end-of-recovery record 5 history)))
      (ok (intake-fails-p (lambda () (pg::timeline-check-end-of-recovery record 1 history))))
      (ok (intake-fails-p
           (lambda () (pg::timeline-check-end-of-recovery record 2 history
                                                          :min-recovery-point 89 :min-recovery-timeline 2))))
      (ok (= 5 (pg::timeline-check-end-of-recovery record 2 history
                                                   :min-recovery-point 88 :min-recovery-timeline 2)))
      (ok (= 5 (pg::timeline-check-end-of-recovery record 2 history
                                                   :min-recovery-point 89 :min-recovery-timeline 5)))
      (ok (intake-fails-p (lambda () (pg::timeline-check-end-of-recovery record 2
                                                                         (lineage-test-history 89 5)))))
      (ok (= 1 pg::*wal-insert-timeline*))
      (ok (= 32 pg::*last-replay-lsn*))))

(deftest eor-transition-allows-zero-length-intermediate-history-and-copied-prefix-inspection
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (history (list (pg::make-timeline-history-entry :timeline 5 :begin 32)
                            (pg::make-timeline-history-entry :timeline 3 :begin 32 :end 32)
                            (pg::make-timeline-history-entry :timeline 2 :begin 0 :end 32)))
             (seen nil) (raw (eor-test-frame)))
        (lineage-test-file 5 0 (sample-physical-record 0) raw)
        (pg::wal-scan-lineage
         (lambda (record owner file)
           (when (= (pg::wal-record-type record) pg::+wal-record-end-of-recovery+)
             (push (list owner file (pg::timeline-check-end-of-recovery record 2 history)) seen)))
         history 0 88)
        (ok (equal '((5 5 5)) seen)))))

(deftest eor-record-position-requires-the-caller-to-choose-a-fitting-child-segment
    (let* ((size pg::+wal-segment-size+) (last-fitting (- size 56)))
      (ok (= size (pg::wal-record-end
                   (pg::wal-decode-record (eor-test-frame last-fitting) last-fitting))))
      (ok (intake-fails-p (lambda () (eor-test-frame (1+ last-fitting)))))
      (ok (= (+ size 56) (pg::wal-record-end
                          (pg::wal-decode-record (eor-test-frame size) size)))))
  (ok (intake-fails-p (lambda () (eor-test-frame (- #xffffffffffffffff 55))))))

(cl-spec:defproperty end-of-recovery-fields-roundtrip
    ((time (range integer -9223372036854775808 9223372036854775807))
     (child (range integer 2 4294967295)) (seed (range integer 0 4294967295))
     (level (range integer 0 2)))
  (:about pg::wal-encode-end-of-recovery-record)
  (:trials (:normal 200))
  (let* ((previous (1+ (mod seed (1- child))))
         (raw (pg::wal-encode-end-of-recovery-record 32 time child previous level))
         (info (pg::wal-end-of-recovery-record-info (pg::wal-decode-record raw 32))))
    (and (= time (getf info :end-time)) (= child (getf info :timeline))
         (= previous (getf info :prev-timeline)) (= level (getf info :wal-level)))))

(deftest generated-end-of-recovery-fields
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'end-of-recovery-fields-roundtrip :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
