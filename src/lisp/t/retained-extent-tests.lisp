(in-package #:pg-physical-tests)

(defconstant +retained-extent-test-start+ (pg::wal-checkpoint-record-size))

(defconstant +retained-extent-test-fork+ (* 2 +retained-extent-test-start+))

(defconstant +retained-extent-test-end+ (+ +retained-extent-test-fork+ pg::+wal-end-of-recovery-record-size+))

(defun retained-extent-fixture (&optional (seed #xCBF29CE484222325) (manifest 77))
  (let* ((prefix (concatenate '(vector (unsigned-byte 8))
                              (physical-test-checkpoint-record 0) (physical-test-checkpoint-record +retained-extent-test-start+)
                              (pg::wal-encode-end-of-recovery-record +retained-extent-test-fork+ 42 2 1 1)))
         (metadata (list :version 1 :system-id 42 :base-timeline 1 :base 0
                         :manifest-digest manifest :history-target 2 :history-digest 99
                         :history-text (store-history-text 1 +retained-extent-test-fork+)
                         :start +retained-extent-test-start+ :end +retained-extent-test-end+ :start-digest seed
                         :end-digest (pg::physical-store-update-digest seed (subseq prefix +retained-extent-test-start+))
                         :segment 0 :file-timeline 2
                         :prefix-digest (pg::physical-store-update-digest #xCBF29CE484222325 prefix))))
    (values metadata prefix)))

(deftest retained-extent-codec-preserves-provenance-and-bytes
    (multiple-value-bind (metadata prefix) (retained-extent-fixture)
      (multiple-value-bind (loaded bytes)
          (pg::physical-retained-extent-decode (pg::physical-retained-extent-encode metadata prefix))
        (ok (equal metadata loaded))
        (ok (equalp prefix bytes)))))

(deftest retained-extent-rejects-rebound-digests-and-interior-boundaries
    (dolist (key '(:start-digest :end-digest :prefix-digest :start :end :file-timeline))
      (multiple-value-bind (metadata prefix) (retained-extent-fixture)
        (setf (getf metadata key) (if (eq key :file-timeline) 1 (1+ (getf metadata key))))
        (ok (signals (pg::physical-retained-extent-encode metadata prefix) 'error)))))

(deftest retained-extent-rejects-corruption-before-literal-reading
    (multiple-value-bind (metadata prefix) (retained-extent-fixture)
      (let ((image (pg::physical-retained-extent-encode metadata prefix)))
        (setf (aref image (- (length image) 5)) (logxor 1 (aref image (- (length image) 5))))
        (ok (signals (pg::physical-retained-extent-decode image) 'error)))))

(cl-spec:defproperty retained-extent-roundtrips-independent-anchor-and-manifest
    ((seed (range integer 0 18446744073709551615))
     (manifest (range integer 0 18446744073709551615)))
  (:about pg::physical-retained-extent-encode)
  (:trials (:normal 200))
  (multiple-value-bind (metadata prefix) (retained-extent-fixture seed manifest)
    (multiple-value-bind (loaded bytes)
        (pg::physical-retained-extent-decode (pg::physical-retained-extent-encode metadata prefix))
      (and (= seed (getf loaded :start-digest))
           (= manifest (getf loaded :manifest-digest))
           (equal metadata loaded) (equalp prefix bytes)))))

(deftest generated-retained-extent-anchor-roundtrips
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'retained-extent-roundtrips-independent-anchor-and-manifest :seed 42))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(defun retained-extent-chain ()
  (multiple-value-bind (metadata prefix) (retained-extent-fixture)
    (let* ((first (copy-list metadata)) (second (copy-list metadata))
                                        (half (subseq prefix 0 +retained-extent-test-fork+))
                                        (digest (pg::physical-store-update-digest (getf metadata :start-digest) (subseq prefix +retained-extent-test-start+ +retained-extent-test-fork+))))
      (setf (getf first :end) +retained-extent-test-fork+ (getf first :end-digest) digest
            (getf first :prefix-digest) (pg::physical-store-update-digest #xCBF29CE484222325 half)
            (getf first :file-timeline) 1
            (getf second :start) +retained-extent-test-fork+ (getf second :start-digest) digest)
      (values (list (pg::physical-retained-extent-encode first half)
                    (pg::physical-retained-extent-encode second prefix))
              (list :system-id 42 :base-timeline 1 :base 0 :manifest-digest 77)
              (pg::timeline-parse-history 2 (getf metadata :history-text))))))

(deftest retained-extent-plan-covers-only-exact-record-boundaries
    (multiple-value-bind (images identity history) (retained-extent-chain)
      (let ((plan (pg::physical-retained-wal-plan images identity history +retained-extent-test-start+ +retained-extent-test-end+)))
        (ok (= 2 (length plan)))
        (ok (= +retained-extent-test-start+ (getf (first plan) :start)))
        (ok (= +retained-extent-test-end+ (getf (second plan) :end))))
      (ok (= 1 (length (pg::physical-retained-wal-plan images identity history +retained-extent-test-fork+ +retained-extent-test-end+))))
      (ok (signals (pg::physical-retained-wal-plan images identity history (1+ +retained-extent-test-start+) +retained-extent-test-end+) 'error))
      (ok (signals (pg::physical-retained-wal-plan images identity history +retained-extent-test-start+ (1+ +retained-extent-test-fork+)) 'error))))

(deftest retained-extent-plan-rejects-holes-and-foreign-identity
    (multiple-value-bind (images identity history) (retained-extent-chain)
      (ok (signals (pg::physical-retained-wal-plan (rest images) identity history +retained-extent-test-start+ +retained-extent-test-end+) 'error))
      (setf (getf identity :manifest-digest) 78)
      (ok (signals (pg::physical-retained-wal-plan images identity history +retained-extent-test-start+ +retained-extent-test-end+) 'error))))

(deftest retained-extent-plan-rejects-individually-valid-digest-discontinuity
    (multiple-value-bind (images identity history) (retained-extent-chain)
      (multiple-value-bind (metadata bytes) (pg::physical-retained-extent-decode (second images))
        (setf (getf metadata :start-digest) 123
              (getf metadata :end-digest) (pg::physical-store-update-digest 123 (subseq bytes +retained-extent-test-fork+)))
        (setf (second images) (pg::physical-retained-extent-encode metadata bytes)))
      (ok (signals (pg::physical-retained-wal-plan images identity history +retained-extent-test-start+ +retained-extent-test-end+) 'error))))

(deftest retained-extent-rejects-ambiguous-fields-and-trailing-envelope-data
    (multiple-value-bind (metadata bytes) (retained-extent-fixture)
      (ok (signals (pg::physical-retained-extent-encode
                    (append metadata (list :start +retained-extent-test-start+)) bytes) 'error))
      (let ((image (pg::physical-retained-extent-encode metadata bytes)))
        (ok (signals (pg::physical-retained-extent-decode
                      (concatenate '(vector (unsigned-byte 8)) image #(0))) 'error))
        (pg::wal-set-u32 image 8 (1+ pg::+physical-retained-metadata-limit+))
        (ok (signals (pg::physical-retained-extent-decode image) 'error)))))

(deftest retained-extent-plan-rejects-overlap-and-divergent-history
    (multiple-value-bind (images identity history) (retained-extent-chain)
      (declare (ignore history))
      (ok (signals (pg::physical-retained-wal-plan
                    (list (first images) (first images)) identity
                    (pg::timeline-parse-history 2 (store-history-text 1 +retained-extent-test-fork+)) +retained-extent-test-start+ +retained-extent-test-fork+) 'error))
      (ok (signals (pg::physical-retained-wal-plan
                    images identity
                    (pg::timeline-parse-history 2 (store-history-text 1 (+ +retained-extent-test-fork+ 32)))
                    +retained-extent-test-start+ +retained-extent-test-end+) 'error))))

(deftest retained-extent-plan-owns-decoded-bytes
    (multiple-value-bind (images identity history) (retained-extent-chain)
      (let* ((plan (pg::physical-retained-wal-plan images identity history +retained-extent-test-start+ +retained-extent-test-end+))
             (saved (copy-seq (getf (first plan) :bytes))))
        (fill (first images) 0)
        (ok (equalp saved (getf (first plan) :bytes))))))

(deftest retained-extent-requires-eor-at-every-timeline-transition
    (multiple-value-bind (metadata ignored) (retained-extent-fixture)
      (declare (ignore ignored))
      (let ((bytes (concatenate '(vector (unsigned-byte 8))
                                (physical-test-checkpoint-record 0) (physical-test-checkpoint-record +retained-extent-test-start+)
                                (physical-test-checkpoint-record +retained-extent-test-fork+))))
        (setf (getf metadata :end) (+ +retained-extent-test-fork+ +retained-extent-test-start+)
              (getf metadata :end-digest)
              (pg::physical-store-update-digest (getf metadata :start-digest) (subseq bytes +retained-extent-test-start+))
              (getf metadata :prefix-digest)
              (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
        (ok (signals (pg::physical-retained-extent-encode metadata bytes) 'error)))))

(deftest retained-extent-accepts-verified-native-rotation-and-rejects-invented-gap
    (multiple-value-bind (metadata ignored) (retained-extent-fixture)
      (declare (ignore ignored))
      (let* ((segment pg::+wal-segment-size+) (fork (- segment 16))
                                              (bytes (pg::wal-encode-end-of-recovery-record segment 42 2 1 1)))
        (setf (getf metadata :start) fork (getf metadata :end) (+ segment 56)
              (getf metadata :segment) 1
              (getf metadata :history-text) (store-history-text 1 fork)
              (getf metadata :prefix-digest) (pg::physical-store-update-digest #xCBF29CE484222325 bytes)
              (getf metadata :end-digest)
              (pg::physical-store-update-digest (getf metadata :start-digest) bytes))
        (ok (not (signals (pg::physical-retained-extent-encode metadata bytes) 'error)))
        (setf (getf metadata :start) 64)
        (ok (signals (pg::physical-retained-extent-encode metadata bytes) 'error)))))

(deftest retained-extent-plan-checks-aggregate-budget-before-decoding
    (multiple-value-bind (images identity history) (retained-extent-chain)
      (let ((image (first images)))
        (ok (not (signals (pg::physical-retained-wal-check-budget (list image) (length image)) 'error)))
        (ok (signals (pg::physical-retained-wal-check-budget (list image) (1- (length image))) 'error)))
      (ok (handler-case
              (progn
                (pg::physical-retained-wal-plan
                 (make-list 1025 :initial-element (first images)) identity history +retained-extent-test-start+ +retained-extent-test-fork+)
                nil)
            (error (condition) (search "budget" (princ-to-string condition)))))))

(deftest retained-extent-plan-rechecks-old-tip-bytes-against-selected-fork
    (multiple-value-bind (metadata ignored) (retained-extent-fixture)
      (declare (ignore ignored))
      (let* ((bytes (concatenate '(vector (unsigned-byte 8))
                                 (physical-test-checkpoint-record 0) (physical-test-checkpoint-record +retained-extent-test-start+)
                                 (physical-test-checkpoint-record +retained-extent-test-fork+)))
             (identity (list :system-id 42 :base-timeline 1 :base 0 :manifest-digest 77))
             (history (pg::timeline-parse-history 2 (getf metadata :history-text))))
        (setf (getf metadata :history-target) 1 (getf metadata :history-digest) nil
              (getf metadata :history-text) "" (getf metadata :file-timeline) 1
              (getf metadata :end) (+ +retained-extent-test-fork+ +retained-extent-test-start+)
              (getf metadata :end-digest)
              (pg::physical-store-update-digest (getf metadata :start-digest) (subseq bytes +retained-extent-test-start+))
              (getf metadata :prefix-digest)
              (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
        (let ((image (pg::physical-retained-extent-encode metadata bytes)))
          (ok (signals (pg::physical-retained-wal-plan (list image) identity history +retained-extent-test-start+ (+ +retained-extent-test-fork+ +retained-extent-test-start+)) 'error))))))

(deftest retained-extent-plan-rejects-conflicting-auxiliary-prefix-bytes
    (multiple-value-bind (images identity history) (retained-extent-chain)
      (multiple-value-bind (metadata bytes) (pg::physical-retained-extent-decode (second images))
        (setf (aref bytes 0) (logxor 1 (aref bytes 0))
              (getf metadata :prefix-digest)
              (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
        (setf (second images) (pg::physical-retained-extent-encode metadata bytes)))
      (ok (signals (pg::physical-retained-wal-plan images identity history +retained-extent-test-start+ +retained-extent-test-end+) 'error))))

(deftest retained-extent-rejects-carrier-before-its-first-segment
    (let* ((bytes (overlap-test-bytes 184 :native t))
           (raw (physical-test-checkpoint-record 184))
           (data (concatenate '(vector (unsigned-byte 8)) bytes raw))
           (metadata (list :version 1 :system-id 123 :base-timeline 1 :base 184 :manifest-digest 77
                           :history-target 2 :history-digest 99 :history-text (store-history-text 1 pg::+wal-segment-size+)
                           :start 184 :end (+ 184 (length raw)) :start-digest #xCBF29CE484222325
                           :end-digest (pg::physical-store-update-digest #xCBF29CE484222325 raw)
                           :segment 0 :file-timeline 2
                           :prefix-digest (pg::physical-store-update-digest #xCBF29CE484222325 data))))
      (ok (intake-fails-p (lambda () (pg::physical-retained-extent-encode metadata data))))
      (setf (getf metadata :file-timeline) 1)
      (ok (pg::physical-retained-extent-encode metadata data))))
