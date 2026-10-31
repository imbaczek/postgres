;;;; Reservation WAL is native allocator metadata, not C binary WAL compatibility.
(in-package #:pg-physical-tests)

(deftest wal-xid-reservation-roundtrips-inclusive-high-water-marks
    (dolist (bound '(2 1002 4294967298 18446744073709551615))
      (let ((bytes (pg::wal-encode-xid-reservation bound)))
        (ok (= 8 (length bytes)))
        (ok (= bound (pg::wal-decode-xid-reservation bytes)))))
  (dolist (bad '(0 1 -1 4294967295 4294967296 4294967297 18446744073709551616))
    (ok (intake-fails-p (lambda () (pg::wal-encode-xid-reservation bad)))))
  (dolist (length '(0 7 9))
    (ok (intake-fails-p
         (lambda () (pg::wal-decode-xid-reservation
                     (make-array length :element-type '(unsigned-byte 8) :initial-element 0)))))))

(deftest wal-xid-reservation-record-requires-new-format-and-no-transaction
    (let ((record (pg::make-wal-record :version 7 :type pg::+wal-record-xid-reservation+
                                       :start 32 :end 72 :xid 0
                                       :payload (pg::wal-encode-xid-reservation 1002))))
      (ok (= 1002 (pg::wal-xid-reservation-record-bound record)))
      (setf (pg::wal-record-version record) 6)
      (ok (intake-fails-p (lambda () (pg::wal-xid-reservation-record-bound record))))
      (setf (pg::wal-record-version record) 7 (pg::wal-record-xid record) 3)
      (ok (intake-fails-p (lambda () (pg::wal-xid-reservation-record-bound record))))
      (setf (pg::wal-record-xid record) 0 (pg::wal-record-end record) 73)
      (ok (intake-fails-p (lambda () (pg::wal-xid-reservation-record-bound record))))
      (setf (pg::wal-record-end record) 72 (pg::wal-record-type record) pg::+wal-record-maintenance+)
      (ok (intake-fails-p (lambda () (pg::wal-xid-reservation-record-bound record))))))

(cl-spec:defproperty wal-xid-reservation-preserves-canonical-inclusive-bound
    ((value (range integer 3 18446744073709551616)))
  (:about pg::wal-decode-xid-reservation)
  (:trials (:normal 200))
  (let ((bound (1- (pg::canonical-next-full-xid value))))
    (= bound (pg::wal-decode-xid-reservation (pg::wal-encode-xid-reservation bound)))))

(deftest generated-wal-xid-reservation-bounds
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'wal-xid-reservation-preserves-canonical-inclusive-bound :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest source-reservation-proof-is-separate-from-visibility-and-legacy-unknowns
    (let* ((pg::*next-xid* 1003)
           (base (list :version 1 :through 32 :bound 1002 :complete t))
           (start (pg::make-wal-record :version 7 :type pg::+wal-record-batch-start+
                                       :start 32 :end 64 :xid 0 :payload (empty-octets)))
           (record (pg::make-wal-record :version 7 :type pg::+wal-record-xid-reservation+
                                        :start 64 :end 104 :xid 0
                                        :payload (pg::wal-encode-xid-reservation 2002)))
           (after-start (pg::physical-source-reservation-next base start 1003))
           (after (pg::physical-source-reservation-next after-start record 1003)))
      (ok (= 2002 (getf after :bound)))
      (ok (= 1003 pg::*next-xid*))
      (ok (getf after :complete))
      (ok (= 104 (getf after :through)))
      (setf (pg::wal-record-version start) 6
            (pg::wal-record-start start) 104 (pg::wal-record-end start) 136)
      (let ((unknown (pg::physical-source-reservation-next after start 1003)))
        (ok (not (getf unknown :complete)))
        (ok (= 2002 (getf unknown :bound)))
        (setf (pg::wal-record-start record) 136 (pg::wal-record-end record) 176
              (pg::wal-record-payload record) (pg::wal-encode-xid-reservation 3002))
        (ok (getf (pg::physical-source-reservation-next unknown record 1003) :complete))
        (setf (pg::wal-record-payload record) (pg::wal-encode-xid-reservation 1002))
        (ok (intake-fails-p
             (lambda () (pg::physical-source-reservation-next unknown record 1003)))))
      (setf (pg::wal-record-version start) 7)
      (ok (intake-fails-p
           (lambda () (pg::physical-source-reservation-next after start 3003))))))

(defun call-with-wal-allocation-fixture (function)
  (with-physical-store-directory (root)
    (let* ((pg::*pgdata* root) (pg::*backend* nil) (pg::*catalog* nil)
                               (pg::*catalog-oldest-relfrozenxid* nil) (pg::*physical-recovery-state* nil)
                               (pg::*wal-ready* nil) (pg::*wal-poisoned* nil)
                               (pg::*wal-end-lsn* 0) (pg::*wal-durable-lsn* 0) (pg::*wal-checkpoint-lsn* 0)
                               (pg::*wal-checkpoint-snapshot* nil) (pg::*wal-xid-reserved-through* nil)
                               (pg::*wal-output-stream* nil) (pg::*wal-output-segment* nil)
                               (pg::*wal-new-segments* nil) (pg::*wal-page-cache* (make-hash-table :test #'equal))
                               (pg::*wal-page-cache-xids* (make-hash-table :test #'equal))
                               (pg::*wal-xid-first-lsn* (make-hash-table :test #'eql))
                               (pg::*wal-snapshot-retention-pins* (make-hash-table :test #'eq))
                               (pg::*wal-replication-slots* (make-hash-table :test #'equal))
                               (pg::*clog-stream* nil) (pg::*next-xid* 3) (pg::*xid-batch-end* 3)
                               (pg::*active-xids* nil) (pg::*xid-wrap-pending* nil) (pg::*xid-wrap-completed-through* nil)
                               (pg::*pm-requested-mode* pg::+no-shutdown+) (pg::*accept-stop-requested* nil)
                               (pg::*shutdown-finalizing-p* nil) (pg::*shutdown-finished* nil)
                               (pg::*xid-reservation-test-hook* nil))
      (unwind-protect
           (progn (pg::wal-init) (pg::xact-init) (funcall function))
        (when pg::*wal-output-stream* (close pg::*wal-output-stream*))
        (when pg::*clog-stream* (close pg::*clog-stream*))))))

(defun call-with-resource-wal-fixture (function)
  (let ((pg::*wal-control-metadata* nil) (pg::*wal-insert-timeline* 1)
                                         (pg::*wal-checkpoint-timeline* 1) (pg::*wal-segment-namespace* :legacy)
                                         (pg::*wal-resource-parameters-test-hook* nil))
    (call-with-wal-allocation-fixture
     (lambda ()
       (setf pg::*wal-control-metadata* (pg::make-wal-control-metadata 42 4 0))
       (pg::wal-save-control-fields 0 0)
       (funcall function)))))

(deftest primary-resource-report-flushes-before-publication-and-skips-unchanged-values
    (call-with-resource-wal-fixture
     (lambda ()
       (let* ((parameters (pg::make-resource-parameters 100 8 10 0 128))
              (events nil)
              (pg::*wal-resource-parameters-test-hook*
                (lambda (point end)
                  (let ((metadata (nth-value 6 (pg::wal-read-control-fields))))
                    (push point events)
                    (ok (= 96 end pg::*wal-durable-lsn*))
                    (if (eq point :after-wal-flush)
                        (ok (null (getf metadata :resource-parameters)))
                        (ok (equal '(100 8 10 0 128) (getf metadata :resource-parameters))))))))
         (ok (= 96 (pg::wal-report-resource-parameters parameters)))
         (ok (equal '(:after-control-save :after-wal-flush) events))
         (ok (= 96 (pg::wal-report-resource-parameters parameters)))
         (ok (= 2 (length events)))
         (let ((records nil))
           (pg::wal-scan (lambda (record) (push record records)))
           (ok (equal '(6 16) (mapcar #'pg::wal-record-type (nreverse records)))))
         (ok (= 96 (getf pg::*wal-control-metadata* :resource-lsn)))
         (ok (= 1 (getf pg::*wal-control-metadata* :resource-timeline)))))))

(deftest primary-resource-report-crash-gap-is-repaired-from-flushed-wal
    (dolist (point '(:after-wal-flush :after-control-save))
      (call-with-resource-wal-fixture
       (lambda ()
         (let ((pg::*wal-resource-parameters-test-hook*
                 (lambda (stage end)
                   (declare (ignore end))
                   (when (eq stage point) (error "injected resource publication failure")))))
           (ok (intake-fails-p
                (lambda () (pg::wal-report-resource-parameters
                            (pg::make-resource-parameters 100 8 10 0 128))))))
         (ok pg::*wal-poisoned*)
         (ok (= 96 pg::*wal-durable-lsn*))
         (close pg::*wal-output-stream*) (setf pg::*wal-output-stream* nil)
         (close pg::*clog-stream*) (setf pg::*clog-stream* nil)
         (pg::wal-init) (pg::xact-init) (pg::wal-recover)
         (ok (not pg::*wal-poisoned*))
         (ok (equal '(100 8 10 0 128) (getf pg::*wal-control-metadata* :resource-parameters)))
         (ok (= 96 (getf (nth-value 6 (pg::wal-read-control-fields)) :resource-lsn)))))))

(deftest primary-resource-report-nonlocal-exit-fences-further-writes
    (call-with-resource-wal-fixture
     (lambda ()
       (let ((pg::*wal-resource-parameters-test-hook*
               (lambda (point end)
                 (declare (ignore end))
                 (when (eq point :after-wal-flush) (throw 'abandoned :abandoned)))))
         (ok (eq :abandoned
                 (catch 'abandoned
                   (pg::wal-report-resource-parameters (pg::make-resource-parameters 100 8 10 0 128))))))
       (ok pg::*wal-poisoned*)
       (ok (null (getf pg::*wal-control-metadata* :resource-parameters)))
       (ok (intake-fails-p
            (lambda () (pg::wal-report-resource-parameters (pg::make-resource-parameters 100 8 10 0 128))))))))

(deftest primary-resource-recovery-restores-the-latest-tuple-including-decreases
    (call-with-resource-wal-fixture
     (lambda ()
       (dolist (values '((100 8 10 0 128) (200 16 20 4 256) (50 4 5 0 64)))
         (pg::wal-report-resource-parameters (apply #'pg::make-resource-parameters values)))
       (ok (zerop pg::*wal-checkpoint-lsn*))
       (ok (= 288 (getf pg::*wal-control-metadata* :resource-lsn)))
       (close pg::*wal-output-stream*) (setf pg::*wal-output-stream* nil)
       (close pg::*clog-stream*) (setf pg::*clog-stream* nil)
       (pg::wal-init) (pg::xact-init) (pg::wal-recover)
       (ok (equal '(50 4 5 0 64) (getf pg::*wal-control-metadata* :resource-parameters)))
       (ok (= 288 (pg::wal-report-resource-parameters (pg::make-resource-parameters 50 4 5 0 64))))
       (ok (= 288 pg::*wal-end-lsn*)))))

(deftest primary-resource-publication-readback-failure-keeps-old-memory-and-fences-writes
    (call-with-resource-wal-fixture
     (lambda ()
       (let* ((read-control (symbol-function 'pg::wal-read-control-fields))
              (prior nil)
              (flush-count 0)
              (pg::*wal-resource-parameters-test-hook*
                (lambda (point end)
                  (when (eq point :after-wal-flush)
                    (incf flush-count)
                    (ok (= 96 end pg::*wal-durable-lsn*))
                    (ok (null (getf pg::*wal-control-metadata* :resource-parameters)))
                    (setf prior (copy-tree pg::*wal-control-metadata*))))))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::wal-read-control-fields)
                      (lambda ()
                        (values pg::*wal-checkpoint-lsn* pg::*wal-durable-lsn*
                                pg::*wal-checkpoint-snapshot* pg::*wal-insert-timeline*
                                pg::*wal-checkpoint-timeline* pg::*wal-segment-namespace* prior)))
                (ok (intake-fails-p
                     (lambda () (pg::wal-report-resource-parameters
                                 (pg::make-resource-parameters 100 8 10 0 128)))))
                (ok (= 1 flush-count))
                (ok pg::*wal-poisoned*)
                (ok (equal prior pg::*wal-control-metadata*)))
           (setf (symbol-function 'pg::wal-read-control-fields) read-control)))
       (ok (equal '(100 8 10 0 128)
                  (getf (nth-value 6 (pg::wal-read-control-fields)) :resource-parameters))))))

(deftest primary-resource-report-ordinary-refusals-do-not-start-a-wal-batch
    (call-with-resource-wal-fixture
     (lambda ()
       (ok (intake-fails-p (lambda () (pg::wal-report-resource-parameters nil))))
       (let ((pg::*wal-ready* nil))
         (ok (intake-fails-p
              (lambda () (pg::wal-report-resource-parameters (pg::make-resource-parameters 100 8 10 0 128))))))
       (ok (not pg::*wal-poisoned*))
       (ok (= 0 pg::*wal-end-lsn* pg::*wal-durable-lsn*)))))

(deftest logical-reader-validates-resource-records-without-publishing-applied-facts
    (dolist (mixed '(nil t))
      (call-with-resource-wal-fixture
       (lambda ()
         (let* ((owner (gensym "RESOURCE-READER"))
                (pg::*wal-logical-slot-owner* owner)
                (pg::*wal-logical-slot-read-progress* nil)
                (slot (pg::make-wal-replication-slot
                       :name "resource" :database-oid 1 :restart-lsn 0
                       :consistent-lsn 0 :confirmed-flush-lsn 0 :active-owner owner)))
           (setf (gethash "resource" pg::*wal-replication-slots*) slot)
           (pg::wal-append-batch-start)
           (when mixed
             (pg::wal-append-record pg::+wal-record-pages+ 0 (pg::wal-encode-page-images nil 0)))
           (pg::wal-append-record pg::+wal-record-parameter-change+ 0
                                  (pg::wal-encode-resource-parameters
                                   (pg::make-resource-parameters 100 8 10 0 128)))
           (pg::wal-flush)
           (if mixed
               (ok (intake-fails-p (lambda () (pg::wal-logical-slot-read-transactions "resource"))))
               (multiple-value-bind (transactions progress)
                   (pg::wal-logical-slot-read-transactions "resource")
                 (ok (null transactions))
                 (ok (= progress pg::*wal-durable-lsn*))))
           (ok (null (getf pg::*wal-control-metadata* :resource-parameters))))))))

(deftest resource-resume-boundary-rejects-mixed-malformed-and-unstarted-metadata
    (dolist (mode '(:valid :mixed :malformed :unstarted :invalid-marker))
      (call-with-resource-wal-fixture
       (lambda ()
         (let ((slot (pg::make-wal-replication-slot :name "resource" :database-oid 1 :restart-lsn 0
                                                    :consistent-lsn 0 :confirmed-flush-lsn 0))
               (payload (pg::wal-encode-resource-parameters (pg::make-resource-parameters 100 8 10 0 128))))
           (unless (eq mode :unstarted)
             (if (eq mode :invalid-marker)
                 (pg::wal-append-record pg::+wal-record-batch-start+ 3
                                        (make-array 0 :element-type '(unsigned-byte 8)))
                 (pg::wal-append-batch-start)))
           (when (eq mode :mixed)
             (pg::wal-append-record pg::+wal-record-pages+ 0 (pg::wal-encode-page-images nil 0)))
           (when (eq mode :malformed) (setf (aref payload 28) 1))
           (pg::wal-append-record pg::+wal-record-parameter-change+ 0 payload)
           (pg::wal-flush)
           (if (eq mode :valid)
               (ok (pg::wal-slot-terminal-ack-p slot pg::*wal-durable-lsn*))
               (ok (intake-fails-p
                    (lambda () (pg::wal-slot-terminal-ack-p slot pg::*wal-durable-lsn*)))))
           (ok (null (getf pg::*wal-control-metadata* :resource-parameters))))))))

(deftest resource-checkpoint-captures-the-frozen-primary-fact-and-real-record-size
    (call-with-resource-wal-fixture
     (lambda ()
       (let ((pg::*relation-streams* (make-hash-table :test #'equal))
             (pg::*physical-source-pins* (make-hash-table :test #'equal))
             (pg::*wal-prev-timeline* 1))
         (ensure-directories-exist (merge-pathnames "base/placeholder" pg::*pgdata*))
         (pg::wal-report-resource-parameters (pg::make-resource-parameters 100 8 10 0 128))
         (pg::wal-checkpoint-core)
         (let* ((snapshot (copy-tree pg::*wal-checkpoint-snapshot*))
                (fields (multiple-value-list (pg::wal-read-control-fields)))
                (record (pg::wal-read-checkpoint-record 128 296)))
           (ok (= 128 (getf snapshot :start)))
           (ok (= 296 (getf snapshot :end) pg::*wal-durable-lsn*))
           (ok (= 136 (length (pg::wal-record-payload record))))
           (ok (equal snapshot (pg::wal-checkpoint-record-snapshot record)))
           (ok (equal snapshot (third fields)))
           (ok (= 96 (getf snapshot :resource-lsn)))
           (ok (equal '(100 8 10 0 128) (getf snapshot :resource-parameters)))
           (ok (equal (pg::wal-copy-control-resource-fields snapshot)
                      (pg::physical-recovery-check-checkpoint-resources snapshot (seventh fields))))
           (pg::wal-report-resource-parameters (pg::make-resource-parameters 50 4 5 0 64))
           (ok (equal '(100 8 10 0 128) (getf pg::*wal-checkpoint-snapshot* :resource-parameters)))
           (ok (intake-fails-p
                (lambda ()
                  (pg::physical-recovery-check-checkpoint-resources snapshot pg::*wal-control-metadata*))))
           (pg::wal-checkpoint-core)
           (ok (equal '(50 4 5 0 64) (getf pg::*wal-checkpoint-snapshot* :resource-parameters)))
           (ok (= 392 (getf pg::*wal-checkpoint-snapshot* :resource-lsn))))))))

(deftest resource-checkpoint-placement-plans-the-extension-before-rotation
    (let* ((cursor (- pg::+wal-segment-size+ 160))
           (facts '(:resource-parameters (100 8 10 0 128) :resource-lsn 96 :resource-timeline 1)))
      (ok (= cursor (pg::wal-record-start-after cursor (pg::wal-checkpoint-record-size))))
      (ok (= pg::+wal-segment-size+
             (pg::wal-record-start-after cursor (pg::wal-checkpoint-record-size facts))))))

(deftest resource-checkpoint-base-proof-requires-exact-copy-and-ancestor-origin
    (with-physical-store-directory (root)
      (let ((pg::*pgdata* root))
        (pg::wal-ensure-directory-durable (pg::wal-directory))
        (pg::timeline-write-history 2 1 64 "first fork")
        (pg::timeline-write-history 3 2 256 "second fork")
        (pg::timeline-write-history 4 1 256 "sibling fork")
        (let* ((snapshot '(:version 2 :start 320 :end 488 :redo 320 :next-xid 3 :xid-reserved-through 2
                           :next-oid nil :time 0 :timeline 3 :prev-timeline 2 :oldest-active-xid 0
                           :resource-parameters (100 8 10 0 128) :resource-lsn 128 :resource-timeline 2))
               (metadata (append (pg::make-wal-control-metadata 42 4 0)
                                 (pg::wal-copy-control-resource-fields snapshot))))
          (ok (pg::wal-checkpoint-snapshot-valid-p snapshot))
          (let ((facts (pg::physical-recovery-check-checkpoint-resources snapshot metadata)))
            (ok (equal (getf snapshot :resource-parameters) (getf facts :resource-parameters)))
            (setf (first (getf facts :resource-parameters)) 101)
            (ok (= 100 (first (getf snapshot :resource-parameters)))))
          (dolist (change '((:resource-parameters (50 4 5 0 64)) (:resource-lsn 127) (:resource-timeline 1)))
            (let ((different (copy-tree metadata)))
              (setf (getf different (first change)) (second change))
              (ok (intake-fails-p
                   (lambda () (pg::physical-recovery-check-checkpoint-resources snapshot different))))))
          (let ((sibling (copy-tree snapshot)))
            (setf (getf sibling :resource-timeline) 1)
            (ok (pg::wal-checkpoint-snapshot-valid-p sibling))
            (ok (intake-fails-p
                 (lambda ()
                   (pg::physical-recovery-check-checkpoint-resources
                    sibling (pg::wal-copy-control-resource-fields sibling))))))
          (let ((sibling (copy-tree snapshot)))
            (setf (getf sibling :timeline) 4 (getf sibling :prev-timeline) 1)
            (ok (pg::wal-checkpoint-snapshot-valid-p sibling))
            (ok (intake-fails-p
                 (lambda ()
                   (pg::physical-recovery-check-checkpoint-resources
                    sibling (pg::wal-copy-control-resource-fields sibling))))))
          (dolist (end '(64 65))
            (let ((boundary (copy-tree snapshot)))
              (setf (getf boundary :resource-lsn) end (getf boundary :resource-timeline) 1)
              (if (= end 64)
                  (ok (pg::timeline-check-resource-origin boundary (pg::timeline-read-history 3)))
                  (ok (intake-fails-p
                       (lambda ()
                         (pg::timeline-check-resource-origin boundary (pg::timeline-read-history 3))))))))
          (ok (intake-fails-p
               (lambda () (pg::physical-recovery-check-checkpoint-resources nil metadata))))
          (ok (null (pg::physical-recovery-check-checkpoint-resources nil nil)))))))

(deftest xid-allocation-flushes-reservation-once-before-using-capacity
    (call-with-wal-allocation-fixture
     (lambda ()
       (ok (= 3 (pg::assign-xid)))
       (ok (= 1003 (pg::load-next-xid)))
       (ok (= 1002 pg::*wal-xid-reserved-through*))
       (ok (= 72 pg::*wal-durable-lsn*))
       (let ((records nil))
         (pg::wal-scan (lambda (record) (push record records)))
         (ok (equal '(6 12) (mapcar #'pg::wal-record-type (nreverse records)))))
       (ok (= 4 (pg::assign-xid)))
       (ok (= 72 pg::*wal-durable-lsn*)))))

(deftest xid-reservation-flush-before-control-failure-never-hands-out-an-id
    (dolist (point '(:after-wal-flush :after-control-save))
      (call-with-wal-allocation-fixture
       (lambda ()
         (let ((pg::*xid-reservation-test-hook*
                 (lambda (stage end)
                   (declare (ignore end))
                   (when (eq stage point) (error "injected reservation publication failure")))))
           (ok (intake-fails-p #'pg::assign-xid)))
         (ok pg::*wal-poisoned*)
         (ok (= 3 pg::*next-xid* pg::*xid-batch-end*))
         (ok (= (if (eq point :after-wal-flush) 3 1003) (pg::load-next-xid)))
         (ok (null pg::*active-xids*))
         (ok (intake-fails-p #'pg::assign-xid))
         (close pg::*wal-output-stream*) (setf pg::*wal-output-stream* nil)
         (close pg::*clog-stream*) (setf pg::*clog-stream* nil)
         (pg::wal-init) (pg::xact-init) (pg::wal-recover)
         (ok (= 1003 pg::*next-xid* pg::*xid-batch-end* (pg::load-next-xid)))
         (ok (= 1002 pg::*wal-xid-reserved-through*))))))

(deftest ordinary-wrap-refusal-does-not-poison-wal
    (call-with-wal-allocation-fixture
     (lambda ()
       (setf pg::*next-xid* 4294967299 pg::*xid-batch-end* 4294967299)
       (ok (equal "55000" (recovery-state-of #'pg::assign-xid)))
       (ok (not pg::*wal-poisoned*))
       (ok (zerop pg::*wal-durable-lsn*)))))

(deftest reservation-nonlocal-exit-fences-before-any-id-handoff
    (dolist (point '(:after-wal-flush :after-capacity-publication))
      (call-with-wal-allocation-fixture
       (lambda ()
         (let ((pg::*xid-reservation-test-hook*
                 (lambda (stage end)
                   (declare (ignore end))
                   (when (eq stage point) (throw 'abandoned :abandoned)))))
           (ok (eq :abandoned (catch 'abandoned (pg::assign-xid)))))
         (ok pg::*wal-poisoned*)
         (ok (= 3 pg::*next-xid*))
         (ok (= (if (eq point :after-wal-flush) 3 1003) (pg::load-next-xid)))
         (ok (null pg::*active-xids*))
         (ok (intake-fails-p #'pg::assign-xid))))))

(deftest allocation-waiter-rechecks-poison-after-acquiring-xid-lock
    (call-with-wal-allocation-fixture
     (lambda ()
       (pg::assign-xid)
       (let ((check (symbol-function 'pg::wal-check-healthy)) (first t))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::wal-check-healthy)
                      (lambda ()
                        (funcall check)
                        ;; Model a concurrent failure after this thread's early check.
                        (when first (setf first nil pg::*wal-poisoned* t))))
                (ok (intake-fails-p #'pg::assign-xid))
                (ok (= 4 pg::*next-xid*))
                (ok (equal '(3) pg::*active-xids*)))
           (setf (symbol-function 'pg::wal-check-healthy) check))))))

(deftest reservation-validation-rejects-bad-batch-markers
    (dolist (marker (list (list pg::+wal-record-batch-start+ #(1))
                          (list pg::+wal-record-batch-start+ (empty-octets) 3)))
      (ok (intake-fails-p
           (lambda ()
             (history-drop-proof (list marker (list pg::+wal-record-xid-reservation+
                                                    (pg::wal-encode-xid-reservation 2002)))))))))

(deftest reservation-replay-on-standby-preserves-immutable-control-and-visibility
    (call-with-wal-allocation-fixture
     (lambda ()
       (let ((pg::*physical-recovery-state* (list :phase 2))
             (pg::*next-xid* 50) (pg::*xid-batch-end* 50)
             (before (pg::recovery-read-bytes (pg::control-path))))
         (pg::with-physical-replay-staging ()
           (pg::wal-replay-record
            (pg::make-wal-record :version 7 :type pg::+wal-record-batch-start+
                                 :start 0 :end 32 :xid 0 :payload (empty-octets)))
           (pg::wal-replay-record
            (pg::make-wal-record :version 7 :type pg::+wal-record-xid-reservation+
                                 :start 32 :end 72 :xid 0
                                 :payload (pg::wal-encode-xid-reservation 2002))))
         (ok (= 50 pg::*next-xid* pg::*xid-batch-end*))
         (ok (equalp before (pg::recovery-read-bytes (pg::control-path))))))))

(deftest reservation-history-never-completes-a-page-or-unstarted-batch
    (let ((payload (pg::wal-encode-xid-reservation 2002)))
      (ok (zerop (hash-table-count (history-drop-proof
                                    (history-batch pg::+wal-record-xid-reservation+ payload)))))
      (ok (intake-fails-p
           (lambda () (history-drop-proof (list (list pg::+wal-record-xid-reservation+ payload))))))
      (ok (intake-fails-p
           (lambda ()
             (history-drop-proof
              (list (list pg::+wal-record-batch-start+ (empty-octets))
                    (list pg::+wal-record-pages+ (pg::wal-encode-page-images nil 0))
                    (list pg::+wal-record-xid-reservation+ payload))))))))

(deftest reservation-wal-header-rejects-pre-v7-type-with-valid-crc
    (let ((bytes (pg::wal-encode-record pg::+wal-record-xid-reservation+ 0 0
                                        (pg::wal-encode-xid-reservation 2002))))
      (ok (= 2002 (pg::wal-xid-reservation-record-bound (pg::wal-decode-record bytes 0))))
      (pg::wal-set-u16 bytes 4 6) (pg::wal-set-u32 bytes 28 0)
      (pg::wal-set-u32 bytes 28 (pg::wal-crc32 bytes))
      (ok (intake-fails-p (lambda () (pg::wal-decode-record bytes 0))))))

(deftest version-six-checkpoint-snapshots-remain-readable
    (let* ((snapshot (sample-checkpoint-snapshot :start 0 :end 120 :redo 0))
           (bytes (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 0
                                         (pg::wal-encode-checkpoint-snapshot snapshot))))
      (pg::wal-set-u16 bytes 4 6) (pg::wal-set-u32 bytes 28 0)
      (pg::wal-set-u32 bytes 28 (pg::wal-crc32 bytes))
      (let ((records (pg::physical-wal-decode-input (pg::make-physical-wal-decoder) 0 bytes)))
        (ok (= 1 (length records)))
        (ok (= 6 (pg::wal-record-version (first records))))
        (ok (equal snapshot (pg::wal-checkpoint-record-snapshot (first records)))))))

(deftest reservation-restartpoint-format-preserves-v3-proof-and-legacy-unknowns
    (call-with-restartpoint-fixture
     (lambda (root store)
       (declare (ignore root store))
       (let* ((catalog (pg::recovery-read-bytes (pg::catalog-path)))
              (metadata (pg::physical-restartpoint-metadata 77 catalog))
              (proof (list :version 1 :through 96 :bound 1002 :complete t)))
         (setf (getf metadata :source-reservation) proof)
         (multiple-value-bind (decoded bytes)
             (pg::physical-restartpoint-decode (pg::physical-restartpoint-encode metadata catalog))
           (ok (pg::physical-restartpoint-check-metadata decoded bytes pg::*physical-recovery-state*))
           (ok (equal proof (getf decoded :source-reservation))))
         (dolist (version '(1 2))
           (let ((legacy (copy-list metadata)))
             (setf (getf legacy :version) version)
             (remf legacy :source-reservation)
             (remf legacy :wal-prefix-digest)
             (when (= version 1) (remf legacy :source-checkpoint))
             (multiple-value-bind (decoded bytes)
                 (pg::physical-restartpoint-decode (pg::physical-restartpoint-encode legacy catalog))
               (ok (pg::physical-restartpoint-check-metadata decoded bytes pg::*physical-recovery-state*))
               (ok (null (getf decoded :source-reservation))))))
         (setf (getf proof :through) 97)
         (ok (intake-fails-p
              (lambda () (pg::physical-restartpoint-check-metadata metadata catalog pg::*physical-recovery-state*))))))))

(deftest replay-without-restartpoint-restores-base-visibility-before-historical-reservations
    (call-with-restartpoint-fixture
     (lambda (root store)
       (declare (ignore root))
       (setf (getf pg::*physical-recovery-state* :files) '(("base/900" 32 0))
             (getf pg::*physical-recovery-state* :observed-next-xid) 2000
             (getf pg::*physical-recovery-state* :source-reservation)
             (list :version 1 :through 32 :bound 1002 :complete t)
             pg::*next-xid* 2000 pg::*xid-batch-end* 2000)
       (ok (= 32 (pg::physical-restartpoint-prepare-storage store)))
       (ok (= 50 pg::*next-xid* pg::*xid-batch-end*))
       (ok (= 1003 (pg::load-next-xid)))
       (ok (< pg::*next-xid* (pg::load-next-xid)))
       (ok (= 2000 (getf pg::*physical-recovery-state* :observed-next-xid)))
       (let ((record (pg::wal-decode-record (sample-physical-record 32) 32)))
         (ok (pg::physical-source-reservation-next
              (getf pg::*physical-recovery-state* :source-reservation) record pg::*next-xid*))))))

(deftest logical-reader-advances-through-reservation-only-batches-and-rejects-mixed-ones
    (dolist (mixed '(nil t))
      (call-with-wal-allocation-fixture
       (lambda ()
         (let* ((owner (gensym "READER"))
                (pg::*wal-logical-slot-owner* owner)
                (pg::*wal-logical-slot-read-progress* nil)
                (slot (pg::make-wal-replication-slot
                       :name "allocation" :database-oid 1 :restart-lsn 0
                       :consistent-lsn 0 :confirmed-flush-lsn 0 :active-owner owner)))
           (setf (gethash "allocation" pg::*wal-replication-slots*) slot)
           (pg::wal-append-batch-start)
           (when mixed
             (pg::wal-append-record pg::+wal-record-pages+ 0 (pg::wal-encode-page-images nil 0)))
           (pg::wal-append-record pg::+wal-record-xid-reservation+ 0
                                  (pg::wal-encode-xid-reservation 2002))
           (pg::wal-flush)
           (if mixed
               (ok (intake-fails-p (lambda () (pg::wal-logical-slot-read-transactions "allocation"))))
               (multiple-value-bind (transactions progress)
                   (pg::wal-logical-slot-read-transactions "allocation")
                 (ok (null transactions))
                 (ok (= progress pg::*wal-durable-lsn*)))))))))

(deftest checkpoint-and-recovery-control-preserve-exhausted-next-xid
    (let* ((end 18446744073709551616)
           (snapshot (sample-checkpoint-snapshot :version 2 :next-xid end
                                                 :xid-reserved-through (1- end)
                                                 :oldest-active-xid end))
           (bytes (pg::wal-encode-checkpoint-snapshot snapshot)))
      (ok (= 88 (length bytes)))
      (ok (equal snapshot (pg::wal-decode-checkpoint-snapshot bytes)))
      (ok (= 3 (pg::wal-u32 bytes 84)))
      (ok (zerop (pg::wal-u64 bytes 32)))
      (ok (zerop (pg::wal-u64 bytes 72)))
      (let ((control (pg::physical-recovery-decode-control
                      (pg::physical-recovery-control-image 2 123 1 456 32 96 end))))
        (ok (= 3 (getf control :control-version)))
        (ok (= end (getf control :next-xid)))))
  (let* ((pg::*next-xid* 18446744073709551616)
         (pg::*xid-batch-end* 18446744073709551616)
         (pg::*active-xids* nil) (pg::*catalog* nil)
         (snapshot (pg::wal-capture-checkpoint-snapshot 32)))
    (ok (= 2 (getf snapshot :version)))
    (ok (= pg::*next-xid*
           (getf (pg::wal-decode-checkpoint-snapshot
                  (pg::wal-encode-checkpoint-snapshot snapshot)) :next-xid)))))

(deftest checkpoint-visibility-uses-actual-counter-not-reserved-capacity
    (let* ((actual (- 4294967296 1000))
           (snapshot (sample-checkpoint-snapshot :version 2 :next-xid actual
                                                 :xid-reserved-through 4294967298
                                                 :oldest-active-xid actual))
           (record (pg::make-wal-record :version 7 :type pg::+wal-record-checkpoint+
                                        :start 32 :end 152 :xid 0
                                        :payload (pg::wal-encode-checkpoint-snapshot snapshot))))
      (ok (= actual (pg::physical-replay-observed-next-xid record 3)))
      (ok (< (pg::physical-replay-observed-next-xid record 3)
             (1+ (getf snapshot :xid-reserved-through))))))
