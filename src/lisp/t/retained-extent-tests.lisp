(in-package #:pg-physical-tests)

(defun retained-extent-fixture (&optional (seed #xCBF29CE484222325) (manifest 77))
  (let* ((prefix (concatenate '(vector (unsigned-byte 8))
                              (legacy-checkpoint-record 0) (legacy-checkpoint-record 32)
                              (pg::wal-encode-end-of-recovery-record 64 42 2 1 1)))
         (metadata (list :version 1 :system-id 42 :base-timeline 1 :base 0
                         :manifest-digest manifest :history-target 2 :history-digest 99
                         :history-text (format nil "1~c0/00000040~cpromotion~%" #\Tab #\Tab)
                         :start 32 :end 120 :start-digest seed
                         :end-digest (pg::physical-store-update-digest seed (subseq prefix 32))
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
                                        (half (subseq prefix 0 64))
                                        (digest (pg::physical-store-update-digest (getf metadata :start-digest) (subseq prefix 32 64))))
      (setf (getf first :end) 64 (getf first :end-digest) digest
            (getf first :prefix-digest) (pg::physical-store-update-digest #xCBF29CE484222325 half)
            (getf first :file-timeline) 1
            (getf second :start) 64 (getf second :start-digest) digest)
      (values (list (pg::physical-retained-extent-encode first half)
                    (pg::physical-retained-extent-encode second prefix))
              (list :system-id 42 :base-timeline 1 :base 0 :manifest-digest 77)
              (pg::timeline-parse-history 2 (getf metadata :history-text))))))

(deftest retained-extent-plan-covers-only-exact-record-boundaries
    (multiple-value-bind (images identity history) (retained-extent-chain)
      (let ((plan (pg::physical-retained-wal-plan images identity history 32 120)))
        (ok (= 2 (length plan)))
        (ok (= 32 (getf (first plan) :start)))
        (ok (= 120 (getf (second plan) :end))))
      (ok (= 1 (length (pg::physical-retained-wal-plan images identity history 64 120))))
      (ok (signals (pg::physical-retained-wal-plan images identity history 48 120) 'error))
      (ok (signals (pg::physical-retained-wal-plan images identity history 32 100) 'error))))

(deftest retained-extent-plan-rejects-holes-and-foreign-identity
    (multiple-value-bind (images identity history) (retained-extent-chain)
      (ok (signals (pg::physical-retained-wal-plan (rest images) identity history 32 120) 'error))
      (setf (getf identity :manifest-digest) 78)
      (ok (signals (pg::physical-retained-wal-plan images identity history 32 120) 'error))))

(deftest retained-extent-plan-rejects-individually-valid-digest-discontinuity
    (multiple-value-bind (images identity history) (retained-extent-chain)
      (multiple-value-bind (metadata bytes) (pg::physical-retained-extent-decode (second images))
        (setf (getf metadata :start-digest) 123
              (getf metadata :end-digest) (pg::physical-store-update-digest 123 (subseq bytes 64)))
        (setf (second images) (pg::physical-retained-extent-encode metadata bytes)))
      (ok (signals (pg::physical-retained-wal-plan images identity history 32 120) 'error))))

(deftest retained-extent-rejects-ambiguous-fields-and-trailing-envelope-data
    (multiple-value-bind (metadata bytes) (retained-extent-fixture)
      (ok (signals (pg::physical-retained-extent-encode
                    (append metadata (list :start 32)) bytes) 'error))
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
                    (pg::timeline-parse-history 2 (store-history-text 1 64)) 32 64) 'error))
      (ok (signals (pg::physical-retained-wal-plan
                    images identity (pg::timeline-parse-history 2 (store-history-text 1 96)) 32 120) 'error))))

(deftest retained-extent-plan-owns-decoded-bytes
    (multiple-value-bind (images identity history) (retained-extent-chain)
      (let* ((plan (pg::physical-retained-wal-plan images identity history 32 120))
             (saved (copy-seq (getf (first plan) :bytes))))
        (fill (first images) 0)
        (ok (equalp saved (getf (first plan) :bytes))))))

(deftest retained-extent-requires-eor-at-every-timeline-transition
    (multiple-value-bind (metadata ignored) (retained-extent-fixture)
      (declare (ignore ignored))
      (let ((bytes (concatenate '(vector (unsigned-byte 8))
                                (legacy-checkpoint-record 0) (legacy-checkpoint-record 32)
                                (legacy-checkpoint-record 64))))
        (setf (getf metadata :end) 96
              (getf metadata :end-digest)
              (pg::physical-store-update-digest (getf metadata :start-digest) (subseq bytes 32))
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
                 (make-list 1025 :initial-element (first images)) identity history 32 64)
                nil)
            (error (condition) (search "budget" (princ-to-string condition)))))))

(deftest retained-extent-plan-rechecks-old-tip-bytes-against-selected-fork
    (multiple-value-bind (metadata ignored) (retained-extent-fixture)
      (declare (ignore ignored))
      (let* ((bytes (concatenate '(vector (unsigned-byte 8))
                                 (legacy-checkpoint-record 0) (legacy-checkpoint-record 32)
                                 (legacy-checkpoint-record 64)))
             (identity (list :system-id 42 :base-timeline 1 :base 0 :manifest-digest 77))
             (history (pg::timeline-parse-history 2 (getf metadata :history-text))))
        (setf (getf metadata :history-target) 1 (getf metadata :history-digest) nil
              (getf metadata :history-text) "" (getf metadata :file-timeline) 1
              (getf metadata :end) 96
              (getf metadata :end-digest)
              (pg::physical-store-update-digest (getf metadata :start-digest) (subseq bytes 32))
              (getf metadata :prefix-digest)
              (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
        (let ((image (pg::physical-retained-extent-encode metadata bytes)))
          (ok (signals (pg::physical-retained-wal-plan (list image) identity history 32 96) 'error))))))

(deftest retained-extent-plan-rejects-conflicting-auxiliary-prefix-bytes
    (multiple-value-bind (images identity history) (retained-extent-chain)
      (multiple-value-bind (metadata bytes) (pg::physical-retained-extent-decode (second images))
        (setf (aref bytes 0) (logxor 1 (aref bytes 0))
              (getf metadata :prefix-digest)
              (pg::physical-store-update-digest #xCBF29CE484222325 bytes))
        (setf (second images) (pg::physical-retained-extent-encode metadata bytes)))
      (ok (signals (pg::physical-retained-wal-plan images identity history 32 120) 'error))))
