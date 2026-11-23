;;;; Native checkpoint snapshots preserve source facts, not write authority.
(in-package #:pg-physical-tests)

(defun sample-checkpoint-snapshot (&rest changes)
  (let ((snapshot (list :version 2 :start 32 :end 152 :redo 32
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
    (dolist (changes '((:end 153) (:redo 31) (:redo 33) (:next-xid 2) (:xid-reserved-through 40)
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
           (record (pg::make-wal-record :version pg::+wal-version+ :type pg::+wal-record-checkpoint+
                                        :xid 0 :start 32 :end 152
                                        :payload (pg::wal-encode-checkpoint-snapshot snapshot))))
      (ok (= 32 (getf (pg::wal-checkpoint-record-snapshot record) :start)))
      (setf (pg::wal-record-start record) 33)
      (ok (intake-fails-p (lambda () (pg::wal-checkpoint-record-snapshot record))))
      (setf (pg::wal-record-start record) 32 (pg::wal-record-version record) 5)
      (ok (intake-fails-p (lambda () (pg::wal-checkpoint-record-snapshot record))))
      (setf (pg::wal-record-payload record) (empty-octets)
            (pg::wal-record-end record) 64
            (pg::wal-record-version record) pg::+wal-version+)
      (ok (intake-fails-p (lambda () (pg::wal-checkpoint-record-snapshot record))))))

(deftest checkpoint-reservation-seed-keeps-source-capacity
    (let* ((manifest (list :version 3 :checkpoint-start 32 :end 152 :next-xid 42))
           (record (pg::make-wal-record
                    :version pg::+wal-version+ :type pg::+wal-record-checkpoint+ :xid 0
                    :start 32 :end 152
                    :payload (pg::wal-encode-checkpoint-snapshot (sample-checkpoint-snapshot))))
           (reservation (pg::physical-recovery-checkpoint-reservation manifest record 1003)))
      (ok (= 152 (getf reservation :through)))
      (ok (= 1002 (getf reservation :bound)))
      (ok (getf reservation :complete))
      (ok (intake-fails-p
           (lambda () (pg::physical-recovery-checkpoint-reservation manifest record 1004))))
      (dolist (key '(:checkpoint-start :end :next-xid))
        (let ((changed (copy-list manifest)))
          (incf (getf changed key))
          (ok (intake-fails-p
               (lambda () (pg::physical-recovery-checkpoint-reservation changed record 1003))))))))

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

(deftest checkpoint-control-copy-is-crc-bound-and-rejects-retired-formats
    (let* ((snapshot (sample-checkpoint-snapshot))
           (text (string-right-trim '(#\Newline)
                                    (pg::bytes->string (pg::wal-encode-control-image 152 300 snapshot)))))
      (multiple-value-bind (checkpoint durable copy insertion checkpoint-timeline namespace)
          (pg::wal-decode-control-line text)
        (declare (ignore insertion checkpoint-timeline))
        (ok (eq :timeline namespace))
        (ok (= checkpoint 152)) (ok (= durable 300))
        (ok (equal copy snapshot)))
      (let ((bad (copy-seq text)))
        (setf (char bad 30) (if (char= (char bad 30) #\0) #\1 #\0))
        (ok (intake-fails-p (lambda () (pg::wal-decode-control-line bad)))))
      (ok (intake-fails-p (lambda () (pg::wal-encode-control-image 153 300 snapshot))))
      (ok (intake-fails-p (lambda () (pg::wal-encode-control-image 152 151 snapshot)))))
  (dolist (prefix (list "PGWAL2 64 300" "PGWAL3 64 300" "PGWAL5 0 0 1 1 -"
                        (format nil "PGWAL4 152 300 ~a"
                                (pg::wal-checkpoint-snapshot-hex (sample-checkpoint-snapshot)))))
    (let ((text (format nil "~a ~8,'0X" prefix (pg::wal-crc32 (pg::string->bytes prefix)))))
      (ok (intake-fails-p (lambda () (pg::wal-decode-control-line text)))))))

(deftest wal-control-keeps-insertion-timeline-separate-from-checkpoint-timeline
    (let* ((snapshot (sample-checkpoint-snapshot))
           (text (string-right-trim '(#\Newline)
                                    (pg::bytes->string
                                     (pg::wal-encode-control-image 152 300 snapshot
                                                                   :timeline 3 :checkpoint-timeline 1)))))
      (multiple-value-bind (checkpoint durable copy insertion checkpoint-timeline)
          (pg::wal-decode-control-line text)
        (ok (= checkpoint 152)) (ok (= durable 300)) (ok (equal copy snapshot))
        (ok (= insertion 3)) (ok (= checkpoint-timeline 1))))
  (ok (intake-fails-p
       (lambda () (pg::wal-encode-control-image 152 300 (sample-checkpoint-snapshot)
                                                :timeline 1 :checkpoint-timeline 2)))))

(deftest wal-startup-rejects-child-control-before-mutating-legacy-storage
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (pg::*wal-ready* nil)
                                 (pg::*wal-insert-timeline* 1) (pg::*wal-checkpoint-timeline* 1)
                                 (control (pg::wal-control-path))
                                 (wal (merge-pathnames "pg_wal/0000000000000000.wal" root))
                                 (bytes (pg::wal-encode-control-image 0 64 nil :timeline 2 :checkpoint-timeline 1 :namespace :legacy))
                                 (tail (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(1 2 3))))
        (pg::wal-atomic-write-bytes control bytes)
        (pg::wal-atomic-write-bytes wal tail)
        (ok (intake-fails-p #'pg::wal-init))
        (ok (not pg::*wal-ready*))
        (ok (= 1 pg::*wal-insert-timeline* pg::*wal-checkpoint-timeline*))
        (ok (not (probe-file (merge-pathnames "base/" root))))
        (ok (equalp bytes (pg::recovery-read-bytes control)))
        (ok (equalp tail (pg::recovery-read-bytes wal))))))

(deftest standby-rejects-child-wal-control-before-publishing-dirty-recovery-state
    (with-physical-store-directory (root)
      (let* ((pg::*pgdata* root) (pg::*physical-recovery-state* nil)
                                 (writes 0) (checked nil)
                                 (pg::*physical-recovery-control-test-hook* (lambda (&rest args) (declare (ignore args)) (incf writes)))
                                 (checker (symbol-function 'pg::physical-recovery-check-streaming-base))
                                 (marker (pg::string->bytes (format nil "PGPHYSICAL-RECOVERY-REQUIRED-1~%"))))
        (pg::wal-atomic-write-bytes (merge-pathnames "recovery.required" root) marker)
        (write-recovery-manifest-text
         root (pg::physical-bootstrap-manifest-text (sample-recovery-manifest)))
        (multiple-value-bind (manifest digest) (pg::recovery-read-manifest root)
          (declare (ignore manifest))
          (let ((before (pg::physical-recovery-control-image 2 123 1 digest 32 96 3 :db-state 2)))
            (pg::wal-atomic-write-bytes (pg::physical-recovery-control-path) before)
            (pg::wal-atomic-write-bytes
             (pg::wal-control-path)
             (pg::wal-encode-control-image 32 32 nil :timeline 2 :checkpoint-timeline 1))
            (unwind-protect
                 (progn
                   ;; Isolate the post-provenance timeline gate; keep real control readers/writer.
                   (setf (symbol-function 'pg::physical-recovery-check-streaming-base)
                         (lambda (&rest args) (declare (ignore args)) (setf checked t)))
                   (ok (handler-case (progn (pg::physical-recovery-prepare) nil)
                         (error (condition)
                           (search "native timeline-keyed WAL is not yet supported"
                                   (princ-to-string condition)))))
                   (ok checked)
                   (ok (zerop writes))
                   (ok (null pg::*physical-recovery-state*))
                   (ok (equalp before (pg::recovery-read-bytes (pg::physical-recovery-control-path)))))
              (setf (symbol-function 'pg::physical-recovery-check-streaming-base) checker)))))))

(cl-spec:defproperty wal-control-preserves-independent-insertion-identity
    ((timeline (range integer 1 4294967295)))
  (:about pg::wal-encode-control-image)
  (:trials (:normal 200))
  (let ((text (string-right-trim '(#\Newline)
                                 (pg::bytes->string
                                  (pg::wal-encode-control-image 152 300 (sample-checkpoint-snapshot)
                                                                :timeline timeline :checkpoint-timeline 1)))))
    (multiple-value-bind (checkpoint durable snapshot insertion checkpoint-timeline)
        (pg::wal-decode-control-line text)
      (and (= checkpoint 152) (= durable 300) (equal snapshot (sample-checkpoint-snapshot))
           (= insertion timeline) (= checkpoint-timeline 1)))))

(deftest generated-independent-wal-control-timelines
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'wal-control-preserves-independent-insertion-identity :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest checkpoint-unknown-fields-survive-literal-metadata-reader
    (let* ((metadata (list :source-checkpoint (sample-checkpoint-snapshot :next-oid nil)
                           :empty nil))
           (text (pg::physical-bootstrap-manifest-text metadata))
           (decoded (pg::recovery-parse-manifest-text text)))
      (ok (equal metadata decoded))
      (ok (null (getf (getf decoded :source-checkpoint) :next-oid)))
      (ok (null (getf decoded :empty)))))

(deftest checkpoint-control-rejects-earlier-redo-even-with-valid-crc
    (let ((bytes (pg::wal-encode-checkpoint-snapshot (sample-checkpoint-snapshot))))
      (pg::wal-set-u64 bytes 24 31)
      (ok (intake-fails-p (lambda () (pg::wal-decode-checkpoint-snapshot bytes))))
      (let* ((hex (with-output-to-string (out)
                    (loop for byte across bytes do (format out "~2,'0X" byte))))
             (prefix (format nil "PGWAL6 152 300 1 1 legacy ~a" hex))
             (line (format nil "~a ~8,'0X" prefix (pg::wal-crc32 (pg::string->bytes prefix)))))
        (ok (intake-fails-p (lambda () (pg::wal-decode-control-line line)))))))

(deftest checkpoint-capture-distinguishes-real-counter-and-reserved-capacity
    (let* ((pg::*next-xid* 42) (pg::*xid-batch-end* 1042)
                               (pg::*active-xids* '(17 31))
                               (pg::*catalog* (pg::make-catalog :next-oid 17001))
                               (snapshot (pg::wal-capture-checkpoint-snapshot 32)))
      (ok (= 42 (getf snapshot :next-xid)))
      (ok (= 1041 (getf snapshot :xid-reserved-through)))
      (ok (= 17001 (getf snapshot :next-oid)))
      (ok (= 17 (getf snapshot :oldest-active-xid)))
      (setf pg::*active-xids* nil)
      (ok (= 42 (getf (pg::wal-capture-checkpoint-snapshot 32) :oldest-active-xid)))))

(deftest checkpoint-failed-publication-keeps-previous-runtime-copy
    (let* ((old (sample-checkpoint-snapshot))
           (candidate (sample-checkpoint-snapshot :start 300 :end 420 :redo 300))
           (pg::*wal-checkpoint-lsn* 152) (pg::*wal-durable-lsn* 500)
           (pg::*wal-checkpoint-snapshot* old)
           (writer (symbol-function 'pg::wal-save-control-fields)))
      (unwind-protect
           (progn
             (setf (symbol-function 'pg::wal-save-control-fields)
                   (lambda (checkpoint durable)
                     (ok (= checkpoint 420)) (ok (= durable 500))
                     (ok (equal candidate pg::*wal-checkpoint-snapshot*))
                     (error "injected checkpoint publication failure")))
             (ok (intake-fails-p (lambda () (pg::wal-save-checkpoint-control 420 candidate))))
             (ok (= 152 pg::*wal-checkpoint-lsn*))
             (ok (equal old pg::*wal-checkpoint-snapshot*)))
        (setf (symbol-function 'pg::wal-save-control-fields) writer))))

(deftest checkpoint-source-pin-binds-full-record-to-control-copy
    (dolist (start (list 32 (- pg::+wal-segment-size+ 120)))
      (with-physical-store-directory (root)
        (let* ((pg::*pgdata* root)
               (end (+ start 120))
               (snapshot (sample-checkpoint-snapshot :start start :end end :redo start))
               (pg::*wal-checkpoint-lsn* end) (pg::*wal-durable-lsn* end)
               (pg::*wal-checkpoint-snapshot* snapshot)
               (pin (pg::make-physical-source-pin :name "snapshot" :restart-lsn start
                                                  :consistent-lsn end)))
          (pg::wal-ensure-directory-durable (pg::wal-directory))
          (with-open-file (stream (pg::wal-segment-path 0) :direction :output
                                                           :element-type '(unsigned-byte 8))
            (file-position stream start)
            (write-sequence (pg::wal-encode-record pg::+wal-record-checkpoint+ 0 start
                                                   (pg::wal-encode-checkpoint-snapshot snapshot)) stream))
          (ok (not (intake-fails-p (lambda () (pg::physical-source-pin-check-wal pin)))))
          (setf pg::*wal-checkpoint-snapshot* (copy-list snapshot)
                (getf pg::*wal-checkpoint-snapshot* :time) 0)
          (ok (intake-fails-p (lambda () (pg::physical-source-pin-check-wal pin))))))))

(deftest checkpoint-manifest-requires-current-version-and-explicit-boundary
    (let* ((current (list :version 3 :system-id 123 :cluster-identity "checkpoint-test"
                          :timeline 1 :checkpoint 152 :checkpoint-start 32 :end 152
                          :next-xid 42 :source-pin "checkpoint_test" :files '(("PG_VERSION" 3 7))))
           (v1 (copy-list current))
           (v2 (copy-list current))
           (missing-start (copy-list current)))
      (setf (getf v1 :version) 1
            (getf v1 :checkpoint) 64
            (getf v1 :end) 64
            (getf v2 :version) 2)
      (remf v1 :checkpoint-start)
      (remf missing-start :checkpoint-start)
      (ok (pg::recovery-check-manifest-schema current))
      (ok (= 32 (pg::physical-manifest-checkpoint-start current)))
      (dolist (invalid (list v1 v2 missing-start))
        (ok (intake-fails-p (lambda () (pg::recovery-check-manifest-schema invalid)))))
      (setf (getf current :checkpoint-start) 33)
      (ok (intake-fails-p (lambda () (pg::recovery-check-manifest-schema current))))))

(deftest native-logical-records-survive-streaming-intake
    (dolist (type (list pg::+wal-record-logical-changes+ pg::+wal-record-logical-catalog-event+))
      (let* ((payload #(1 2 3))
             (bytes (pg::wal-encode-record type 0 0 payload))
             (records (pg::physical-wal-decode-input (pg::make-physical-wal-decoder) 0 bytes)))
        (ok (= 1 (length records)))
        (ok (= pg::+wal-version+ (pg::wal-record-version (first records))))
        (ok (= type (pg::wal-record-type (first records))))
        (ok (equalp payload (pg::wal-record-payload (first records)))))))
