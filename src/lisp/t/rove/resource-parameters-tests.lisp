(in-package #:pg)

(defun resource-parameters-test-error-p (function)
  (handler-case (progn (funcall function) nil) (error () t)))

(rove:deftest resource-parameters-codec-has-an-independent-byte-oracle
    (let* ((parameters (make-resource-parameters 100 8 10 0 128))
           (golden (make-array 32 :element-type '(unsigned-byte 8)
                                  :initial-contents
                                  '(80 71 82 69 83 48 48 49
                                    100 0 0 0 8 0 0 0 10 0 0 0 0 0 0 0 128 0 0 0 0 0 0 0))))
      (rove:ok (equalp golden (wal-encode-resource-parameters parameters)))
      (rove:ok (equal '(100 8 10 0 128)
                      (resource-parameters-values (wal-decode-resource-parameters golden))))
      (let ((bytes (wal-encode-resource-parameters parameters)))
        (setf (aref bytes 8) 0)
        (rove:ok (= 100 (first (resource-parameters-values parameters)))))))

(rove:deftest resource-parameters-do-not-invent-unknown-legacy-facts
    (let ((parameters (make-resource-parameters 100 8 10 0 128)))
      (rove:ok (not (resource-parameters-valid-p nil)))
      (rove:ok (resource-parameters-test-error-p (lambda () (wal-encode-resource-parameters nil))))
      (rove:ok (resource-parameters-test-error-p
                (lambda () (wal-decode-resource-parameters (make-array 0 :element-type '(unsigned-byte 8))))))
      (rove:ok (resource-parameters-test-error-p
                (lambda () (resource-parameters-shortfalls parameters nil))))
      (rove:ok (resource-parameters-test-error-p
                (lambda () (resource-parameters-shortfalls nil parameters))))))

(rove:deftest resource-parameters-validate-all-five-fields
    (dolist (values '((0 8 10 0 128) (262144 8 10 0 128)
                      (100 -1 10 0 128) (100 262144 10 0 128)
                      (100 8 -1 0 128) (100 8 262144 0 128)
                      (100 8 10 -1 128) (100 8 10 262144 128)
                      (100 8 10 0 9) (100 8 10 0 2147483648)
                      (100 8 10 0 128.0) (262141 0 0 0 10)
                      (1 262140 0 0 10) (1 0 262140 0 10)))
      (rove:ok (resource-parameters-test-error-p
                (lambda () (apply #'make-resource-parameters values)))))
  (dolist (values '((1 0 0 0 10) (262140 0 0 262143 2147483647)
                    (1 262139 0 262143 2147483647) (1 0 262139 262143 2147483647)))
    (rove:ok (resource-parameters-valid-p (apply #'make-resource-parameters values)))))

(rove:deftest resource-parameters-decode-rejects-malformed-present-facts
    (let ((golden (wal-encode-resource-parameters (make-resource-parameters 100 8 10 0 128))))
      (dolist (index '(0 1 2 3 4 5 6 7 28 29 30 31))
        (let ((bytes (copy-seq golden)))
          (setf (aref bytes index) (logxor 1 (aref bytes index)))
          (rove:ok (resource-parameters-test-error-p (lambda () (wal-decode-resource-parameters bytes))))))
      (dolist (length '(1 8 28 31 33 64))
        (let ((bytes (make-array length :element-type '(unsigned-byte 8) :initial-element 0)))
          (replace bytes golden)
          (rove:ok (resource-parameters-test-error-p (lambda () (wal-decode-resource-parameters bytes))))))
      (dolist (change '((8 0) (8 262143) (8 262144) (12 262144) (16 262144) (20 262144) (24 9) (24 2147483648)))
        (let ((bytes (copy-seq golden)))
          (wal-set-u32 bytes (first change) (second change))
          (rove:ok (resource-parameters-test-error-p (lambda () (wal-decode-resource-parameters bytes))))))))

(rove:deftest resource-parameters-checks-individual-floors-and-permits-decreases
    (let ((local (make-resource-parameters 100 8 10 0 128)))
      (rove:ok (null (resource-parameters-shortfalls local (make-resource-parameters 50 4 5 0 64))))
      (rove:ok (equal '(("max_connections" 100 101)
                        ("max_worker_processes" 8 9)
                        ("max_wal_senders" 10 11)
                        ("max_prepared_transactions" 0 1)
                        ("max_locks_per_transaction" 128 129))
                      (resource-parameters-shortfalls local (make-resource-parameters 101 9 11 1 129))))
      (rove:ok (equal '(("max_locks_per_transaction" 128 192))
                      (resource-parameters-shortfalls local (make-resource-parameters 100 8 10 0 192))))
      (rove:ok (null (resource-parameters-shortfalls local (make-resource-parameters 100 8 10 0 64))))
      (let ((large (make-resource-parameters 1000 8 10 0 10)))
        (rove:ok (equal '(("max_locks_per_transaction" 10 128))
                        (resource-parameters-shortfalls large local))))))

(rove:deftest resource-lock-capacities-use-the-complete-process-budget
    (let ((parameters (make-resource-parameters 100 8 10 3 64)))
      (rove:ok (equal '(8896 17792) (multiple-value-list (resource-lock-capacities parameters))))
      (rove:ok (equal '(7936 15872)
                      (multiple-value-list
                       (resource-lock-capacities parameters :autovacuum-worker-slots 1)))))
  (rove:ok (resource-parameters-test-error-p
            (lambda () (resource-lock-capacities (make-resource-parameters 262140 0 0 0 10)))))
  (rove:ok (equal '(2621430 5242860)
                  (multiple-value-list
                   (resource-lock-capacities (make-resource-parameters 262125 0 0 0 10)))))
  (rove:ok (equal '(2621430 5242860)
                  (multiple-value-list
                   (resource-lock-capacities (make-resource-parameters 262140 0 0 0 10)
                                             :autovacuum-worker-slots 1))))
  (dolist (slots '(0 262144 1.0))
    (rove:ok (resource-parameters-test-error-p
              (lambda ()
                (resource-lock-capacities (make-resource-parameters 1 0 0 0 10)
                                          :autovacuum-worker-slots slots))))))

(cl-spec:defproperty resource-parameters-roundtrip-and-capacity
    ((connections (range integer 1 2000))
     (workers (range integer 0 128))
     (senders (range integer 0 128))
     (prepared (range integer 0 128))
     (locks (range integer 10 4096)))
  (:about make-resource-parameters wal-encode-resource-parameters wal-decode-resource-parameters resource-lock-capacities)
  (:trials (:normal 200))
  (let* ((parameters (make-resource-parameters connections workers senders prepared locks))
         (decoded (wal-decode-resource-parameters (wal-encode-resource-parameters parameters))))
    (multiple-value-bind (objects owners) (resource-lock-capacities decoded)
      (and (equal (list connections workers senders prepared locks)
                  (resource-parameters-values decoded))
           (= objects (* locks (+ connections workers senders prepared 16 2)))
           (= owners (* 2 objects))))))

(rove:deftest generated-resource-parameters-codec-and-capacity
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'resource-parameters-roundtrip-and-capacity :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 200 (getf data :trials)))))

(rove:deftest resource-control-roundtrip-preserves-known-and-unknown-facts
    (dolist (retained '(nil t))
      (let* ((base (append (make-wal-control-metadata 42 4 0)
                           (when retained
                             (list :retained-reference
                                   '(:root-id 1 :base-timeline 1 :base 32
                                     :manifest-digest 99 :end 64 :end-digest 7)))))
             (parameters (resource-parameters-values (make-resource-parameters 100 8 10 0 128)))
             (known (append base (list :resource-parameters parameters
                                       :resource-lsn 96 :resource-timeline 1))))
        (dolist (metadata (list base known))
          (rove:ok (wal-control-metadata-valid-p metadata))
          (let* ((bytes (wal-encode-control-image 0 96 nil :metadata metadata))
                 (actual (nth-value 6 (wal-decode-control-line (bytes->string bytes)))))
            (rove:ok (equal metadata actual))
            (rove:ok (eq (not (null (getf metadata :resource-parameters)))
                         (not (null (getf actual :resource-parameters))))))))))

(rove:deftest resource-control-rejects-partial-invalid-and-future-facts
    (let* ((base (make-wal-control-metadata 42 4 0))
           (parameters (resource-parameters-values (make-resource-parameters 100 8 10 0 128))))
      (dolist (fields (list (list :resource-parameters parameters)
                            (list :resource-lsn 96 :resource-timeline 1)
                            (list :resource-parameters nil :resource-lsn 96 :resource-timeline 1)
                            (list :resource-parameters parameters :resource-lsn 0 :resource-timeline 1)
                            (list :resource-parameters parameters :resource-lsn 96 :resource-timeline 0)))
        (rove:ok (not (wal-control-metadata-valid-p (append base fields)))))
      (dolist (fields (list (list :resource-parameters parameters :resource-lsn 97 :resource-timeline 1)
                            (list :resource-parameters parameters :resource-lsn 96 :resource-timeline 2)))
        (rove:ok (resource-parameters-test-error-p
                  (lambda () (wal-encode-control-image 0 96 nil :metadata (append base fields))))))))

(rove:deftest resource-control-decodes-only-the-complete-tagged-extension
    (let ((hex (wal-resource-parameters-hex (make-resource-parameters 100 8 10 0 128))))
      (flet ((decode (tail)
               (let ((prefix (format nil "PGWAL6 0 96 1 1 legacy - 42 4 0 ~a" tail)))
                 (wal-decode-control-line
                  (format nil "~a ~8,'0X" prefix (wal-crc32 (string->bytes prefix)))))))
        (rove:ok (equal '(100 8 10 0 128)
                        (getf (nth-value 6 (decode (format nil "R ~a 96 1" hex)))
                              :resource-parameters)))
        (dolist (tail (list (format nil "X ~a 96 1" hex)
                            (format nil "R ~a 97 1" hex)
                            (format nil "R ~a 96 2" hex)
                            (format nil "R ~a 96" hex)
                            (format nil "R ~a 96 1 0" hex)
                            (format nil "R ~a 1_20 1" hex)
                            (format nil "R ~a 96 0_1" hex)
                            (format nil "R ~a 96 1" (subseq hex 2))))
          (rove:ok (resource-parameters-test-error-p (lambda () (decode tail))))))))

(rove:deftest resource-control-refresh-preserves-resource-position
    (let* ((parameters (resource-parameters-values (make-resource-parameters 100 8 10 0 128)))
           (metadata (append (make-wal-control-metadata 42 4 0)
                             (list :resource-parameters parameters :resource-lsn 96 :resource-timeline 1)))
           (refreshed (wal-refresh-control-metadata metadata 6 1)))
      (rove:ok (equal parameters (getf refreshed :resource-parameters)))
      (rove:ok (not (eq parameters (getf refreshed :resource-parameters))))
      (setf (first (getf refreshed :resource-parameters)) 101)
      (rove:ok (= 100 (first parameters)))
      (rove:ok (= 96 (getf refreshed :resource-lsn)))
      (rove:ok (= 1 (getf refreshed :resource-timeline)))
      (rove:ok (= 6 (getf refreshed :db-state)))))

(rove:deftest resource-control-bounded-reader-accepts-the-complete-extended-image
    (let* ((maximum 18446744073709551615)
           (start (* (- (floor maximum +wal-segment-size+) 1) +wal-segment-size+))
           (end (+ start (wal-checkpoint-record-size)))
           (snapshot (list :version 2 :start start :end end :redo start :next-xid 3
                           :xid-reserved-through 2 :next-oid maximum :time -9223372036854775808
                           :timeline 4294967295 :prev-timeline 4294967295 :oldest-active-xid 0))
           (metadata (append (make-wal-control-metadata maximum 4 9223372036854775807)
                             (list :retained-reference
                                   (list :root-id maximum :base-timeline 4294967295 :base (- maximum 64)
                                         :manifest-digest maximum :end maximum :end-digest maximum)
                                   :resource-parameters '(100 8 10 0 128)
                                   :resource-lsn maximum :resource-timeline 4294967295)))
           (bytes (wal-encode-control-image end maximum snapshot
                                            :timeline 4294967295 :namespace :timeline :metadata metadata))
           (control-path (symbol-function 'wal-control-path)))
      (rove:ok (> (length bytes) 513))
      (uiop:with-temporary-file (:stream stream :pathname path :direction :output
                                 :element-type '(unsigned-byte 8))
        (write-sequence bytes stream)
        :close-stream
        (unwind-protect
             (progn
               (setf (symbol-function 'wal-control-path) (lambda () path))
               (let ((actual (multiple-value-list (wal-read-control-fields))))
                 (rove:ok (equal (list end maximum snapshot 4294967295 4294967295 :timeline metadata)
                                 actual))
                 (setf (first (getf (seventh actual) :resource-parameters)) 101)
                 (rove:ok (= 100 (first (getf metadata :resource-parameters))))
                 (rove:ok (= 100 (first (getf (nth-value 6 (wal-read-control-fields)) :resource-parameters))))))
          (setf (symbol-function 'wal-control-path) control-path)))))
