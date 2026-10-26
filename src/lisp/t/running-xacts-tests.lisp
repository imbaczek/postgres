;;;; Source transaction tracking contracts.
(in-package #:pg-physical-tests)

(deftest running-xacts-codec-preserves-actual-next-and-membership
    (dolist (example '((3 nil t) (10 (3 6 9) t)
                       (4294967300 (4294967299) t)
                       (18446744073709551616 (18446744073709551615) t)
                       (10 nil nil)))
      (destructuring-bind (next running complete) example
        (let ((decoded (pg::wal-decode-running-xacts
                        (pg::wal-encode-running-xacts next running complete))))
          (ok (= next (getf decoded :next-xid)))
          (ok (equal running (getf decoded :running)))
          (ok (eq complete (getf decoded :complete)))))))

(deftest running-xacts-codec-rejects-ambiguous-or-unbounded-metadata
    (dolist (example '((2 nil t) (4294967296 nil t)
                       (18446744073709551617 nil t)
                       (10 (3 3) t) (10 (6 3) t) (10 (10) t)
                       (10 (0) t) (10 (3) nil) (10 nil :yes)))
      (ok (intake-fails-p
           (lambda () (apply #'pg::wal-encode-running-xacts example)))))
  (ok (intake-fails-p
       (lambda ()
         (pg::wal-encode-running-xacts 100000
                                       (loop for xid from 3 below (+ 4 pg::+running-xacts-limit+) collect xid) t))))
  (let ((bytes (pg::wal-encode-running-xacts 10 '(3 9) t)))
    (dolist (length '(0 7 23 25 39 41))
      (ok (intake-fails-p
           (lambda ()
             (pg::wal-decode-running-xacts
              (adjust-array (copy-seq bytes) length :initial-element 0))))))
    (dolist (offset '(0 8 20 24 32))
      (let ((bad (copy-seq bytes)))
        (setf (aref bad offset) 255)
        (ok (intake-fails-p (lambda () (pg::wal-decode-running-xacts bad))))))))

(cl-spec:defproperty running-xacts-roundtrip-preserves-source-visibility
    ((value (range integer 3 18446744073709551616)))
  (:about pg::wal-decode-running-xacts)
  (:trials (:normal 200))
  (let* ((next (pg::canonical-next-full-xid value))
         (running (if (> (ldb (byte 32 0) next) 3) (list (1- next)) nil))
         (decoded (pg::wal-decode-running-xacts
                   (pg::wal-encode-running-xacts next running t))))
    (and (= next (getf decoded :next-xid))
         (equal running (getf decoded :running))
         (getf decoded :complete))))

(deftest generated-running-xacts-codec
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'running-xacts-roundtrip-preserves-source-visibility
                                       :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest source-xids-anchor-and-gap-tracking-follow-replay-order
    (let* ((anchor (pg::running-xacts-anchor
                    (list :next-xid 8 :running '(3 7) :complete t) 100))
           (advanced (pg::running-xacts-advance anchor 12 200 :committed-xid 7)))
      (ok (equal '(3 8 9 10 11) (getf advanced :running)))
      (ok (= 12 (getf advanced :next-xid)))
      (ok (= 100 (getf advanced :anchor)))
      (ok (= 200 (getf advanced :through)))
      (ok (getf advanced :complete))
      (ok (equal '(3 7) (getf anchor :running)))
      (ok (= 100 (getf anchor :through)))
      (let ((exact (pg::running-xacts-anchor
                    (list :next-xid 12 :running '(11) :complete t) 250)))
        (ok (equal '(11) (getf exact :running))))
      (ok (null (pg::running-xacts-advance nil 12 200)))
      (ok (intake-fails-p (lambda () (pg::running-xacts-advance anchor 7 200))))
      (ok (intake-fails-p (lambda () (pg::running-xacts-advance anchor 12 99))))))

(deftest source-xids-gap-tracking-skips-special-ids-and-bounds-memory
    (let* ((anchor (pg::running-xacts-anchor
                    (list :next-xid 4294967295 :running nil :complete t) 100))
           (advanced (pg::running-xacts-advance anchor 4294967301 200)))
      (ok (equal '(4294967295 4294967299 4294967300) (getf advanced :running))))
  (let* ((anchor (pg::running-xacts-anchor
                  (list :next-xid 3 :running nil :complete t) 100))
         (overflow (pg::running-xacts-advance anchor 100000 200))
         (unknown (pg::running-xacts-advance overflow 100001 300)))
    (ok (not (getf overflow :complete)))
    (ok (null (getf overflow :running)))
    (ok (= 100000 (getf overflow :next-xid)))
    (ok (not (getf unknown :complete)))
    (ok (getf anchor :complete))
    (ok (= 3 (getf anchor :next-xid)))))

(deftest source-xids-removes-committer-only-at-ordered-terminal
    (let* ((anchor (pg::running-xacts-anchor
                    (list :next-xid 10 :running '(3 9) :complete t) 100))
           (metadata (pg::running-xacts-advance anchor 10 150))
           (terminal (pg::running-xacts-advance metadata 10 200 :committed-xid 3)))
      (ok (equal '(3 9) (getf metadata :running)))
      (ok (equal '(9) (getf terminal :running)))
      (ok (equal '(3 9) (getf anchor :running)))
      (ok (intake-fails-p
           (lambda () (pg::running-xacts-advance anchor 10 200 :committed-xid 10)))))
  (let* ((anchor (pg::running-xacts-anchor
                  (list :next-xid 18446744073709551615 :running nil :complete t) 100))
         (last (pg::running-xacts-advance anchor 18446744073709551616 200)))
    (ok (equal '(18446744073709551615) (getf last :running)))))

(deftest running-xacts-decoder-validates-membership-and-sentinel
    (dolist (members '((3 3) (9 3) (0 9) (3 10)))
      (let ((bytes (pg::wal-encode-running-xacts 10 '(3 9) t)))
        (pg::wal-set-u64 bytes 24 (first members))
        (pg::wal-set-u64 bytes 32 (second members))
        (ok (intake-fails-p (lambda () (pg::wal-decode-running-xacts bytes))))))
  (dolist (flags '(0 1 2 3 4))
    (dolist (raw '(0 2 10 4294967296))
      (let ((bytes (pg::wal-encode-running-xacts 10 nil t)))
        (pg::wal-set-u32 bytes 8 flags)
        (pg::wal-set-u64 bytes 12 raw)
        (if (or (and (member flags '(0 1)) (= raw 10))
                (and (member flags '(2 3)) (zerop raw)))
            (ok (pg::wal-decode-running-xacts bytes))
            (ok (intake-fails-p (lambda () (pg::wal-decode-running-xacts bytes)))))))))

(deftest source-xids-state-rejects-malformed-generation-metadata
    (let ((state (pg::running-xacts-anchor
                  (list :next-xid 10 :running '(3 9) :complete t) 100)))
      (dolist (mutation '((:version 2) (:anchor 101) (:through -1)
                          (:next-xid 9) (:running (9 3)) (:complete :yes)))
        (let ((bad (copy-tree state)))
          (setf (getf bad (first mutation)) (second mutation))
          (ok (intake-fails-p (lambda () (pg::running-xacts-check-state bad))))))
      (ok (intake-fails-p
           (lambda () (pg::running-xacts-check-state (append state '(:version 1))))))
      (ok (intake-fails-p
           (lambda () (pg::running-xacts-check-state (cddr state)))))
      (let ((bad (copy-list state)))
        (setf (first bad) :unexpected)
        (ok (intake-fails-p (lambda () (pg::running-xacts-check-state bad)))))))

(deftest running-xacts-native-record-requires-v7-and-empty-metadata-batch
    (let* ((payload (pg::wal-encode-running-xacts 10 '(3 9) t))
           (bytes (pg::wal-encode-record pg::+wal-record-running-xacts+ 0 32 payload))
           (record (pg::wal-decode-record bytes 32)))
      (ok (equal '(3 9) (getf (pg::wal-running-xacts-record-info record) :running)))
      (ok (zerop (hash-table-count
                  (history-drop-proof (history-batch pg::+wal-record-running-xacts+ payload)))))
      (ok (intake-fails-p
           (lambda () (history-drop-proof (list (list pg::+wal-record-running-xacts+ payload))))))
      (ok (intake-fails-p
           (lambda ()
             (history-drop-proof
              (list (list pg::+wal-record-batch-start+ (empty-octets))
                    (list pg::+wal-record-pages+ (pg::wal-encode-page-images nil 0))
                    (list pg::+wal-record-running-xacts+ payload))))))
      (setf (pg::wal-record-xid record) 3)
      (ok (intake-fails-p (lambda () (pg::wal-running-xacts-record-info record))))
      (pg::wal-set-u16 bytes 4 6)
      (pg::wal-set-u32 bytes 28 0)
      (pg::wal-set-u32 bytes 28 (pg::wal-crc32 bytes))
      (ok (intake-fails-p (lambda () (pg::wal-decode-record bytes 32))))))

(deftest source-commit-records-running-snapshot-before-committing-batch
    (call-with-wal-allocation-fixture
     (lambda ()
       (let ((writer (pg::assign-xid)) (other (pg::assign-xid)) (records nil))
         (pg::wal-commit-transaction writer
                                     (lambda () (pg::clog-set writer 1) (pg::release-xid writer)))
         (pg::wal-scan (lambda (record) (push record records)))
         (setf records (nreverse records))
         (ok (equal '(6 12 6 14 6 2) (mapcar #'pg::wal-record-type records)))
         (let ((snapshot (pg::wal-running-xacts-record-info (fourth records))))
           (ok (= 5 (getf snapshot :next-xid)))
           (ok (equal (list writer other) (getf snapshot :running)))
           (ok (getf snapshot :complete))
           (ok (< (getf snapshot :next-xid) pg::*xid-batch-end*)))
         (ok (not (member writer pg::*active-xids*)))
         (ok (member other pg::*active-xids*))))))

(deftest source-running-snapshot-holds-wal-and-xid-locks-through-insertion
    (call-with-wal-allocation-fixture
     (lambda ()
       (let ((writer (pg::assign-xid))
             (append-record (symbol-function 'pg::wal-append-record))
             (checked 0))
         (unwind-protect
              (progn
                (setf (symbol-function 'pg::wal-append-record)
                      (lambda (type xid payload)
                        (when (or (= type pg::+wal-record-running-xacts+)
                                  (and (= type pg::+wal-record-batch-start+) (zerop checked)))
                          (ok (sb-thread:holding-mutex-p pg::*wal-lock*))
                          (ok (sb-thread:holding-mutex-p pg::*xid-lock*))
                          (incf checked))
                        (funcall append-record type xid payload)))
                (pg::wal-commit-transaction writer
                                            (lambda () (pg::clog-set writer 1) (pg::release-xid writer)))
                (ok (= 2 checked)))
           (setf (symbol-function 'pg::wal-append-record) append-record))))))

(deftest running-xacts-redo-validates-without-changing-local-transaction-state
    (let* ((pg::*next-xid* 50) (pg::*active-xids* '(49))
                               (payload (pg::wal-encode-running-xacts 10 '(3 9) t))
                               (record (pg::wal-decode-record
                                        (pg::wal-encode-record pg::+wal-record-running-xacts+ 0 32 payload) 32))
                               (pg::*wal-checkpoint-lsn* 0))
      (pg::with-physical-replay-staging ()
        (pg::wal-replay-record
         (pg::wal-decode-record
          (pg::wal-encode-record pg::+wal-record-batch-start+ 0 0 (empty-octets)) 0))
        (pg::wal-replay-record record)
        (ok (not pg::*wal-recovery-group-active*)))
      (ok (= 50 pg::*next-xid*))
      (ok (equal '(49) pg::*active-xids*))
      (ok (= 10 (pg::physical-replay-observed-next-xid record 3)))
      (ok (= 50 (pg::physical-replay-observed-next-xid record 50)))))

(deftest logical-reader-advances-through-running-snapshots-without-row-events
    (dolist (mixed '(nil t))
      (call-with-wal-allocation-fixture
       (lambda ()
         (let* ((owner (gensym "RUNNING-READER"))
                (pg::*wal-logical-slot-owner* owner)
                (pg::*wal-logical-slot-read-progress* nil)
                (slot (pg::make-wal-replication-slot
                       :name "running" :database-oid 1 :restart-lsn 0
                       :consistent-lsn 0 :confirmed-flush-lsn 0 :active-owner owner)))
           (setf (gethash "running" pg::*wal-replication-slots*) slot)
           (pg::wal-append-batch-start)
           (when mixed
             (pg::wal-append-record pg::+wal-record-pages+ 0 (pg::wal-encode-page-images nil 0)))
           (pg::wal-append-record pg::+wal-record-running-xacts+ 0
                                  (pg::wal-encode-running-xacts 10 '(3 9) t))
           (pg::wal-flush)
           (if mixed
               (ok (intake-fails-p (lambda () (pg::wal-logical-slot-read-transactions "running"))))
               (multiple-value-bind (transactions progress)
                   (pg::wal-logical-slot-read-transactions "running")
                 (ok (null transactions))
                 (ok (= progress pg::*wal-durable-lsn*)))))))))

(deftest source-running-snapshot-overflow-is-explicit-and-legacy-callers-give-no-proof
    (call-with-wal-allocation-fixture
     (lambda ()
       (let ((pg::*next-xid* 100000)
             (pg::*active-xids* (loop for xid from 3 below (+ 4 pg::+running-xacts-limit+)
                                      collect xid)))
         (sb-thread:with-mutex (pg::*wal-lock*)
           (pg::wal-append-running-xacts-under-lock 3)
           (pg::wal-flush))
         (let ((records nil))
           (pg::wal-scan (lambda (record) (push record records)))
           (let ((info (pg::wal-running-xacts-record-info (first records))))
             (ok (= 100000 (getf info :next-xid)))
             (ok (not (getf info :complete)))
             (ok (null (getf info :running))))))
       (let ((end pg::*wal-end-lsn*) (pg::*active-xids* nil))
         (sb-thread:with-mutex (pg::*wal-lock*)
           (ok (null (pg::wal-append-running-xacts-under-lock 3))))
         (ok (= end pg::*wal-end-lsn*))))))
