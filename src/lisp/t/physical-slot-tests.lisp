(in-package #:pg-physical-tests)

(defun physical-slot-test-object (&key (name "public_physical") restart (timeline (when restart 1))
                                    (system-id 42) temporary)
  (apply (symbol-function (find-symbol "MAKE-WAL-REPLICATION-SLOT" :pg))
         (list :name name :kind :physical :system-id system-id
               :restart-lsn restart :restart-timeline timeline
               :consistent-lsn nil :confirmed-flush-lsn nil :plugin nil
               :temporary-p temporary)))

(deftest physical-slot-retention-checks-identity-lineage-and-interior-bytes
    (let* ((history (pg::timeline-parse-history 2 (format nil "1~c0/00000040~cpromoted~%" #\Tab #\Tab)))
           (slot (physical-slot-test-object :restart 33 :timeline 1))
           (reads 0)
           (reader (lambda (segment cursor)
                     (declare (ignore cursor))
                     (incf reads)
                     (unless (zerop segment) (error "unexpected segment"))
                     (make-array 96 :element-type '(unsigned-byte 8)))))
      (ok (eq slot (pg::physical-slot-check-retained-range slot 42 history 96 reader)))
      (ok (= 1 reads))
      (ok (signals (pg::physical-slot-check-retained-range slot 43 history 96 reader) 'error))
      (ok (signals (pg::physical-slot-check-retained-range slot 42 history 32 reader) 'error))
      (setf (pg::wal-replication-slot-restart-timeline slot) 2)
      (ok (signals (pg::physical-slot-check-retained-range slot 42 history 96 reader) 'error))
      (setf (pg::wal-replication-slot-restart-lsn slot) 64
            (pg::wal-replication-slot-restart-timeline slot) 1)
      (ok (eq slot (pg::physical-slot-check-retained-range slot 42 history 96 reader)))
      (ok (signals
           (pg::physical-slot-check-retained-range
            slot 42 history 96 (lambda (segment cursor)
                                 (declare (ignore segment cursor))
                                 (make-array 95 :element-type '(unsigned-byte 8))))
           'error))))

(deftest physical-slot-range-allows-only-proven-native-rotation-gaps
    (let* ((segment-size pg::+wal-segment-size+)
           (history (pg::timeline-parse-history 1 ""))
           (next (pg::wal-encode-record pg::+wal-record-maintenance+ 0 segment-size #()))
           (prefix (pg::wal-encode-record
                    pg::+wal-record-maintenance+ 0 0
                    (make-array (- segment-size 20 pg::+wal-header-size+)
                                :element-type '(unsigned-byte 8))))
           (slot (physical-slot-test-object :restart (- segment-size 10)))
           (reader (lambda (segment cursor)
                     (declare (ignore cursor))
                     (if (zerop segment) prefix next))))
      (ok (eq slot (pg::physical-slot-check-retained-range
                    slot 42 history (+ segment-size (length next)) reader)))
      (let ((valid prefix))
        (setf prefix (subseq valid 0 (1- (length valid))))
        (ok (signals (pg::physical-slot-check-retained-range
                      slot 42 history (+ segment-size (length next)) reader) 'error))
        (setf prefix (copy-seq valid)
              (aref prefix (1- (length prefix))) 1)
        (ok (signals (pg::physical-slot-check-retained-range
                      slot 42 history (+ segment-size (length next)) reader) 'error)))
      (setf prefix (make-array (- segment-size 32) :element-type '(unsigned-byte 8)))
      (ok (signals (pg::physical-slot-check-retained-range
                    slot 42 history (+ segment-size (length next)) reader) 'error))
      (setf prefix (make-array 0 :element-type '(unsigned-byte 8)))
      (ok (signals (pg::physical-slot-check-retained-range
                    slot 42 history (+ segment-size (length next)) reader) 'error))))

(deftest physical-slot-unreserved-and-lost-state-needs-no-wal-bytes
    (let ((history (pg::timeline-parse-history 1 ""))
          (slot (physical-slot-test-object))
          (reader (lambda (&rest arguments)
                    (declare (ignore arguments))
                    (error "slot must not read WAL"))))
      (ok (eq slot (pg::physical-slot-check-retained-range slot 42 history 96 reader)))
      (ok (signals (pg::physical-slot-check-retained-range slot 43 history 96 reader) 'error))
      (setf (pg::wal-replication-slot-restart-lsn slot) 33
            (pg::wal-replication-slot-restart-timeline slot) 1
            (pg::wal-replication-slot-wal-status slot) :lost)
      (ok (eq slot (pg::physical-slot-check-retained-range slot 42 history 96 reader)))))

(deftest physical-slot-primary-can-retain-an-eligible-parent-file
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (pg::*physical-recovery-state* nil)
             (pg::*physical-recovery-failed-p* nil)
             (pg::*wal-insert-timeline* 2)
             (pg::*wal-segment-namespace* :timeline)
             (segment-size pg::+wal-segment-size+)
             (fork segment-size)
             (pg::*wal-durable-lsn* (+ fork 32))
             (slot (physical-slot-test-object :restart 33 :timeline 1)))
        (pg::wal-ensure-directory-durable (pg::wal-directory))
        (pg::wal-atomic-write-bytes
         (pg::wal-segment-path 0 1 :timeline)
         (make-array segment-size :element-type '(unsigned-byte 8)))
        (pg::wal-atomic-write-bytes
         (pg::wal-segment-path 1 2 :timeline)
         (pg::wal-encode-record pg::+wal-record-maintenance+ 0 fork #()))
        (pg::wal-atomic-write-bytes
         (merge-pathnames "00000002.history" (pg::wal-directory))
         (pg::string->bytes (format nil "1~c~a~cpromoted~%" #\Tab (pg::wal-format-lsn fork) #\Tab)))
        (ok (eq slot (pg::physical-slot-check-primary-wal slot 42))))))

(deftest physical-slot-startup-uses-durable-identity-and-cleans-dead-temporary-slots
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (durable-catalog (pg::make-catalog :cluster-identity "slot-startup"))
             (pg::*catalog* (pg::make-catalog :cluster-identity "stale-live-image"))
             (pg::*physical-recovery-state* nil)
             (pg::*physical-recovery-failed-p* nil)
             (pg::*wal-insert-timeline* 1)
             (pg::*wal-segment-namespace* :legacy)
             (pg::*wal-durable-lsn* 96)
             (pg::*wal-replication-slots* (make-hash-table :test #'equal))
             (system-id (pg::catalog-system-identifier durable-catalog))
             (slot (physical-slot-test-object :system-id system-id))
             (temporary (physical-slot-test-object :name "dead_temporary" :temporary t)))
        (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
        (ensure-directories-exist (pg::catalog-path))
        (let ((pg::*catalog* durable-catalog))
          (pg::write-catalog-image (pg::catalog-image-bytes)))
        (setf (pg::wal-replication-slot-inactive-since slot) 1)
        (pg::wal-save-replication-slot slot)
        (pg::wal-save-replication-slot temporary)
        (pg::wal-load-replication-slots)
        (let ((loaded (gethash "public_physical" pg::*wal-replication-slots*)))
          (ok (pg::wal-physical-slot-p loaded))
          (ok (null (pg::wal-replication-slot-restart-lsn loaded)))
          (ok (> (pg::wal-replication-slot-inactive-since loaded) 1)))
        (ok (null (gethash "dead_temporary" pg::*wal-replication-slots*)))
        (ok (not (probe-file (pg::wal-replication-slot-path "dead_temporary"))))
        (setf (pg::wal-replication-slot-system-id slot) (logxor 1 system-id))
        (pg::wal-save-replication-slot slot)
        (ok (signals (pg::wal-load-replication-slots) 'error))
        (setf (pg::wal-replication-slot-system-id slot) system-id
              (pg::wal-replication-slot-restart-lsn slot) 97
              (pg::wal-replication-slot-restart-timeline slot) 1)
        (pg::wal-save-replication-slot slot)
        (ok (signals (pg::wal-load-replication-slots) 'error))
        (delete-file (pg::catalog-path))
        (ok (signals (pg::wal-load-replication-slots) 'error)))))

(deftest public-physical-slot-file-dispatch-keeps-owner-state-local
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root)
             (slot (physical-slot-test-object :restart 33))
             (path (pg::wal-replication-slot-path "public_physical")))
        (pg::wal-ensure-directory-durable (pg::wal-replication-slot-directory))
        (setf (pg::wal-replication-slot-active-owner slot) (gensym "SOURCE-SESSION"))
        (pg::wal-save-replication-slot slot)
        (let ((loaded (pg::wal-read-replication-slot path)))
          (ok (pg::wal-physical-slot-p loaded))
          (ok (= 33 (pg::wal-replication-slot-restart-lsn loaded)))
          (ok (null (pg::wal-replication-slot-active-owner loaded)))))))

(deftest public-physical-slots-do-not-join-logical-xid-tracking
    (let* ((physical (physical-slot-test-object))
           (logical (pg::make-wal-replication-slot :name "legacy_logical"))
           (pg::*wal-replication-slots* (make-hash-table :test #'equal))
           (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql)))
      (setf (gethash "public_physical" pg::*wal-replication-slots*) physical
            (gethash "legacy_logical" pg::*wal-replication-slots*) logical
            (gethash 70 pg::*wal-xid-first-lsn*) 32)
      (pg::wal-slot-seed-active-xids-under-wal-lock physical '(70))
      (pg::wal-slot-record-xid-batch-under-wal-lock 70 32)
      (pg::wal-slot-record-xid-commit-under-wal-lock 70 72)
      (ok (equal '(32 . 72) (gethash 70 (pg::wal-replication-slot-pending-xids logical))))
      (ok (zerop (hash-table-count (pg::wal-replication-slot-pending-xids physical))))
      (ok (eq physical (pg::wal-slot-rebuild-progress physical)))
      (ok (pg::wal-logical-slot-p logical))))

(cl-spec:defproperty physical-slot-format-keeps-null-and-full-width-restart-state
    ((system-id (range integer 0 18446744073709551615))
     (restart (range integer 1 18446744073709551615))
     (timeline (range integer 1 4294967295))
     (reserve (range integer 0 1))
     (temporary (range integer 0 1)))
  (:about pg::wal-encode-replication-slot)
  (:trials (:normal 200))
  (let* ((slot (physical-slot-test-object :system-id system-id
                                          :restart (and (plusp reserve) restart)
                                          :timeline (and (plusp reserve) timeline)
                                          :temporary (plusp temporary)))
         (bytes (pg::wal-encode-replication-slot slot))
         (decoded (pg::physical-slot-decode bytes "public_physical")))
    (and (equalp bytes (pg::wal-encode-replication-slot decoded))
         (eql (and (plusp reserve) restart) (pg::wal-replication-slot-restart-lsn decoded))
         (null (pg::wal-replication-slot-active-owner decoded))
         (null (pg::wal-replication-slot-active-owner-backend decoded)))))

(deftest generated-physical-slot-durable-state
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'physical-slot-format-keeps-null-and-full-width-restart-state :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest replication-slot-encoder-rejects-an-unknown-kind
    (let ((slot (pg::make-wal-replication-slot
                 :name "unknown_kind" :restart-lsn 32 :consistent-lsn 32)))
      (setf (pg::wal-replication-slot-kind slot) :unknown)
      (ok (signals (pg::wal-encode-replication-slot slot) 'error))))

(deftest physical-slot-format-rejects-logical-state-or-an-incomplete-restart-pair
    (dolist (arguments '((:restart 32 :timeline nil)
                         (:restart nil :timeline 1)))
      (ok (signals
           (pg::wal-encode-replication-slot (apply #'physical-slot-test-object arguments))
           'error)))
  (let ((slot (physical-slot-test-object :restart 32)))
    (setf (pg::wal-replication-slot-confirmed-flush-lsn slot) 40)
    (ok (signals (pg::wal-encode-replication-slot slot) 'error))))

(deftest physical-slot-decoder-checks-name-crc-and-owner-restoration
    (let* ((slot (physical-slot-test-object :restart 33))
           (owner (gensym "LIVE-PHYSICAL-SENDER")))
      (setf (pg::wal-replication-slot-active-owner slot) owner)
      (let* ((bytes (pg::wal-encode-replication-slot slot))
             (copy (copy-seq bytes)))
        (ok (signals (pg::physical-slot-decode bytes "another_slot") 'error))
        (setf (aref copy (1- (length copy))) 0)
        (ok (signals (pg::physical-slot-decode copy "public_physical") 'error))
        (ok (null (pg::wal-replication-slot-active-owner
                   (pg::physical-slot-decode bytes "public_physical")))))))
