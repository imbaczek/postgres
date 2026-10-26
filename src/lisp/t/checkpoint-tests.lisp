;;;; Native checkpoint snapshots preserve source facts, not write authority.
(in-package #:pg-physical-tests)

(defun sample-checkpoint-snapshot (&rest changes)
  (let ((snapshot (list :version 1 :start 32 :end 152 :redo 32
                        :next-xid 42 :xid-reserved-through 1002 :next-oid 16384
                        :time -1 :timeline 1 :prev-timeline 1 :oldest-active-xid 17)))
    (loop for (key value) on changes by #'cddr do (setf (getf snapshot key) value))
    snapshot))

(deftest checkpoint-snapshot-roundtrip-distinguishes-source-counters
    (dolist (oid '(nil 0 16384 18446744073709551615))
      (let* ((snapshot (sample-checkpoint-snapshot :next-oid oid))
             (bytes (pg::wal-encode-checkpoint-snapshot snapshot))
             (decoded (pg::wal-decode-checkpoint-snapshot bytes)))
        (ok (= 88 (length bytes)))
        (loop for (key value) on snapshot by #'cddr
              do (ok (eql value (getf decoded key))))
        (ok (= 42 (getf decoded :next-xid)))
        (ok (= 1002 (getf decoded :xid-reserved-through)))
        (ok (eql oid (getf decoded :next-oid)))))
  (ok (= 42 (getf (pg::wal-decode-checkpoint-snapshot
                   (pg::wal-encode-checkpoint-snapshot
                    (sample-checkpoint-snapshot :oldest-active-xid 42)))
                  :oldest-active-xid))))

(deftest checkpoint-time-uses-signed-little-endian-int64
    (dolist (value '(-9223372036854775808 -1 0 9223372036854775807))
      (let* ((snapshot (sample-checkpoint-snapshot :time value))
             (bytes (pg::wal-encode-checkpoint-snapshot snapshot)))
        (ok (= value (getf (pg::wal-decode-checkpoint-snapshot bytes) :time)))
        (ok (= (ldb (byte 64 0) value) (pg::wal-u64 bytes 56)))))
  (let ((bytes (make-array 8 :element-type '(unsigned-byte 8) :initial-element 0)))
    (dolist (value '(-9223372036854775809 9223372036854775808))
      (ok (intake-fails-p (lambda () (pg::wal-set-i64 bytes 0 value))))))
  (let ((bytes (pg::wal-encode-checkpoint-snapshot
                (sample-checkpoint-snapshot :next-oid nil))))
    ;; Unknown must not hide a nonzero OID behind the presence bit.
    (setf (aref bytes 48) 1)
    (ok (intake-fails-p (lambda () (pg::wal-decode-checkpoint-snapshot bytes))))))

(deftest checkpoint-snapshot-rejects-invalid-schema-and-bounds
    (dolist (changes '((:end 153) (:redo 33) (:next-xid 2) (:xid-reserved-through 40)
                       (:oldest-active-xid 43) (:oldest-active-xid 2) (:timeline 0)
                       (:time 9223372036854775808) (:next-oid -1)
                       (:start 16777100 :end 16777220 :redo 16777100)))
      (ok (intake-fails-p
           (lambda () (pg::wal-encode-checkpoint-snapshot
                       (apply #'sample-checkpoint-snapshot changes))))))
  (let ((bytes (pg::wal-encode-checkpoint-snapshot (sample-checkpoint-snapshot))))
    (dolist (offset '(0 80 84))
      (let ((bad (copy-seq bytes)))
        (setf (aref bad offset) (logxor 2 (aref bad offset)))
        (ok (intake-fails-p (lambda () (pg::wal-decode-checkpoint-snapshot bad))))))
    (ok (intake-fails-p
         (lambda () (pg::wal-decode-checkpoint-snapshot (subseq bytes 0 87)))))))

(deftest checkpoint-record-payload-binds-actual-record-boundaries
    (let* ((snapshot (sample-checkpoint-snapshot))
           (record (pg::make-wal-record :version 6 :type pg::+wal-record-checkpoint+
                                        :xid 0 :start 32 :end 152
                                        :payload (pg::wal-encode-checkpoint-snapshot snapshot))))
      (ok (= 32 (getf (pg::wal-checkpoint-record-snapshot record) :start)))
      (setf (pg::wal-record-start record) 33)
      (ok (intake-fails-p (lambda () (pg::wal-checkpoint-record-snapshot record))))
      (setf (pg::wal-record-start record) 32 (pg::wal-record-version record) 5)
      (ok (intake-fails-p (lambda () (pg::wal-checkpoint-record-snapshot record))))
      (setf (pg::wal-record-payload record) (empty-octets) (pg::wal-record-end record) 64)
      (ok (null (pg::wal-checkpoint-record-snapshot record)))
      (setf (pg::wal-record-version record) 6)
      (ok (intake-fails-p (lambda () (pg::wal-checkpoint-record-snapshot record))))))

(cl-spec:defproperty checkpoint-snapshot-preserves-full-source-facts
    ((position (range integer 0 1000000))
     (xid (range integer 3 18446744073709551615))
     (oid (range integer 0 18446744073709551615))
     (time (range integer -9223372036854775808 9223372036854775807))
     (known-oid (range integer 0 1)))
  (:about pg::wal-decode-checkpoint-snapshot)
  (:trials (:normal 200))
  (let* ((snapshot (sample-checkpoint-snapshot
                    :start position :end (+ position 120) :redo position
                    :next-xid xid :xid-reserved-through (1- xid)
                    :oldest-active-xid 0 :time time
                    :next-oid (unless (zerop known-oid) oid)))
         (decoded (pg::wal-decode-checkpoint-snapshot
                   (pg::wal-encode-checkpoint-snapshot snapshot))))
    (loop for (key value) on snapshot by #'cddr always (eql value (getf decoded key)))))

(deftest generated-checkpoint-snapshot-roundtrips
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'checkpoint-snapshot-preserves-full-source-facts :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))
