(in-package #:pg)

(cl-spec:defgenerator unit-promotion-fpw-arguments ()
  (list (zerop (random 2))))

(cl-spec:defspec-function physical-fpw-encode-promotion-intent
    (:args (enabled (type boolean)))
  (:args-generator unit-promotion-fpw-arguments)
  (:returns (type (simple-array (unsigned-byte 8) (17))))
  (:post (eql enabled (physical-fpw-decode-promotion-intent result))))

(cl-spec:defproperty promotion-fpw-intent-roundtrip ((enabled (member nil t)))
  (:about physical-fpw-encode-promotion-intent physical-fpw-decode-promotion-intent)
  (:trials (:normal 100))
  (eql enabled (physical-fpw-decode-promotion-intent (physical-fpw-encode-promotion-intent enabled))))

(defun unit-promotion-fpw-image (enabled &key (intent-p t) resources source selected retained)
  (apply #'physical-recovery-control-image
         (append
          (list 3 42 1 4 32 80 3 :state-time 0 :promotion-child 4
                                 :promotion-generation 9 :promotion-source-namespace :legacy
                                 :applied-timeline 1 :history-target (when selected 3) :history-digest (when selected 7)
                                 :retained-reference (when retained '(:root-id 1 :base-timeline 1 :base 32
                                                                      :manifest-digest 4 :end 80 :end-digest 7))
                                 :resource-fact (when resources '(:resource-parameters (100 8 10 0 128)
                                                                  :resource-lsn 16 :resource-timeline 1))
                                 :fpw-fact (when source (unit-source-fpw-fact)))
          (when intent-p (list :promotion-full-page-writes enabled)))))

(defun unit-promotion-fpw-refresh-crc (bytes)
  (wal-set-u32 bytes (- (length bytes) 4) 0)
  (wal-set-u32 bytes (- (length bytes) 4) (wal-crc32 bytes))
  bytes)

(rove:deftest promotion-fpw-intent-strict-boolean-and-reserved-bytes
    (dolist (enabled '(nil t))
      (let ((bytes (physical-fpw-encode-promotion-intent enabled)))
        (rove:ok (= 17 (length bytes)))
        (rove:ok (equalp #(80 71 70 80 87 80 73 49) (subseq bytes 0 8)))
        (rove:ok (= (if enabled 1 0) (aref bytes 8)))
        (rove:ok (eql enabled (physical-fpw-decode-promotion-intent bytes)))
        (dolist (index '(0 1 2 3 4 5 6 7 9 10 11 12 13 14 15 16))
          (let ((bad (copy-seq bytes)))
            (setf (aref bad index) (logxor 1 (aref bad index)))
            (rove:ok (unit-commit-ts-error-p (lambda () (physical-fpw-decode-promotion-intent bad))))))
        (dolist (flag '(2 255))
          (let ((bad (copy-seq bytes)))
            (setf (aref bad 8) flag)
            (rove:ok (unit-commit-ts-error-p (lambda () (physical-fpw-decode-promotion-intent bad))))))))
  (dolist (value '(0 1 :off "on"))
    (rove:ok (unit-commit-ts-error-p (lambda () (physical-fpw-encode-promotion-intent value))))))

(rove:deftest promotion-fpw-control-preserves-old-layouts-and-false-presence
    (let ((lengths nil))
      (dolist (resources '(nil t))
        (dolist (source '(nil t))
          (dolist (selected '(nil t))
            (dolist (retained '(nil t))
              (let* ((old (unit-promotion-fpw-image nil :intent-p nil :resources resources :source source
                                                        :selected selected :retained retained))
                     (old-control (physical-recovery-decode-control old)))
                (push (length old) lengths)
                (rove:ok (not (get-properties old-control '(:promotion-full-page-writes))))
                (dolist (enabled '(nil t))
                  (let* ((bytes (unit-promotion-fpw-image enabled :resources resources :source source
                                                                  :selected selected :retained retained))
                         (decoded (physical-recovery-decode-control bytes)))
                    (rove:ok (= (+ 17 (length old)) (length bytes)))
                    (rove:ok (= 1 (mod (length bytes) 4)))
                    (rove:ok (equalp (subseq old 0 (- (length old) 4))
                                     (subseq bytes 0 (- (length old) 4))))
                    (rove:ok (get-properties decoded '(:promotion-full-page-writes)))
                    (rove:ok (eql enabled (getf decoded :promotion-full-page-writes)))
                    (remf decoded :promotion-full-page-writes)
                    (rove:ok (equal old-control decoded)))))))))
      (rove:ok (equal '(88 104 132 136 148 152 160 176 180 196 204 208 220 224 252 268)
                      (sort (remove-duplicates lengths) #'<)))
      (rove:ok (= (+ 285 +physical-logical-interval-fact-size+ +physical-promotion-logical-intent-size+) +physical-recovery-control-maximum-bytes+))))

(rove:deftest promotion-fpw-control-rejects-valid-crc-malformed-intent
    (let* ((bytes (unit-promotion-fpw-image nil :resources t :source t :selected t :retained t))
           (offset (- (length bytes) 21)))
      (dolist (index '(0 1 2 3 4 5 6 7 9 10 11 12 13 14 15 16))
        (let ((bad (copy-seq bytes)))
          (setf (aref bad (+ offset index)) (logxor 1 (aref bad (+ offset index))))
          (unit-promotion-fpw-refresh-crc bad)
          (rove:ok (unit-commit-ts-error-p (lambda () (physical-recovery-decode-control bad))))))
      (dolist (flag '(2 255))
        (let ((bad (copy-seq bytes)))
          (setf (aref bad (+ offset 8)) flag)
          (unit-promotion-fpw-refresh-crc bad)
          (rove:ok (unit-commit-ts-error-p (lambda () (physical-recovery-decode-control bad)))))))
  (let* ((old (physical-recovery-control-image 2 42 1 4 32 80 3 :state-time 0))
         (bytes (make-array (+ 17 (length old)) :element-type '(unsigned-byte 8) :initial-element 0)))
    (replace bytes old :end2 (- (length old) 4))
    (replace bytes (physical-fpw-encode-promotion-intent t) :start1 (- (length old) 4))
    (unit-promotion-fpw-refresh-crc bytes)
    (rove:ok (unit-commit-ts-error-p (lambda () (physical-recovery-decode-control bytes)))))
  (dolist (enabled '(nil t))
    (rove:ok (unit-commit-ts-error-p
              (lambda () (physical-recovery-control-image
                          2 42 1 4 32 80 3 :promotion-full-page-writes enabled)))))
  (rove:ok (unit-commit-ts-error-p (lambda () (unit-promotion-fpw-image 0)))))

(rove:deftest promotion-fpw-intent-generated-contracts
    (dolist (result (list (cl-spec:result-data
                           (cl-spec:check-function 'physical-fpw-encode-promotion-intent :trials 100 :seed 263))
                          (cl-spec:result-data
                           (cl-spec:run-property 'promotion-fpw-intent-roundtrip :seed 264))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))))
