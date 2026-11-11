(in-package #:pg-physical-tests)

(defun source-logical-promotion-fact (enabled start)
  (pg::physical-logical-fact-next
   nil (pg::make-wal-record :version 8 :type 18 :xid 0 :start start :end (+ start 33)
                            :payload (pg::wal-encode-logical-decoding-status-change enabled))
   1 (pg::timeline-parse-history 1 "")))

(cl-spec:defgenerator promotion-logical-intent-arguments ()
  (list (zerop (random 2))))

(cl-spec:defspec-function pg::physical-logical-encode-promotion-intent
    (:args (enabled (type boolean)))
  (:args-generator promotion-logical-intent-arguments)
  (:returns (type (simple-array (unsigned-byte 8) (18))))
  (:post (eql enabled (pg::physical-logical-decode-promotion-intent result))))

(cl-spec:defproperty promotion-logical-intent-roundtrips-known-off-and-on
    ((enabled (member nil t)))
  (:about pg::physical-logical-encode-promotion-intent pg::physical-logical-decode-promotion-intent)
  (:trials (:normal 150))
  (eql enabled (pg::physical-logical-decode-promotion-intent
                (pg::physical-logical-encode-promotion-intent enabled))))

(defun promotion-logical-codec-image (logical &key (fpw :absent) resources fpw-source source selected retained)
  (let ((logical-fact (when source (source-logical-promotion-fact t 32))))
    (when logical-fact (setf (getf logical-fact :through) 152))
    (apply #'pg::physical-recovery-control-image
           (append
            (list 3 42 1 4 32 152 50 :state-time 0 :promotion-child 4
                                     :promotion-generation 9 :promotion-source-namespace :legacy
                                     :history-target (when selected 3) :history-digest (when selected 7)
                                     :retained-reference
                                     (when retained '(:root-id 1 :base-timeline 1 :base 32 :manifest-digest 4 :end 152 :end-digest 7))
                                     :resource-fact
                                     (when resources '(:resource-parameters (100 8 10 0 128) :resource-lsn 16 :resource-timeline 1))
                                     :fpw-fact (when fpw-source
                                                 (pg::physical-fpw-fact-from-state
                                                  (pg::wal-fpw-state-from-checkpoint
                                                   (sample-checkpoint-snapshot :version 2 :full-page-writes nil)
                                                   (make-symbol "CODEC-FPW")) 152 1))
                                     :logical-fact logical-fact)
            (unless (eq fpw :absent) (list :promotion-full-page-writes fpw))
            (unless (eq logical :absent) (list :promotion-logical-decoding-enabled logical))))))

(defun promotion-logical-codec-refresh-crc (bytes)
  (pg::wal-set-u32 bytes (- (length bytes) 4) 0)
  (pg::wal-set-u32 bytes (- (length bytes) 4) (pg::wal-crc32 bytes))
  bytes)

(deftest promotion-logical-intent-generated-contracts
    (dolist (data (list
                   (cl-spec:result-data
                    (cl-spec:check-function 'pg::physical-logical-encode-promotion-intent :trials 150 :seed 7401))
                   (cl-spec:result-data
                    (cl-spec:run-property 'promotion-logical-intent-roundtrips-known-off-and-on
                                          :seed 7402 :profile :normal))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest promotion-logical-intent-rejects-noncanonical-bytes-and-values
    (dolist (enabled '(nil t))
      (let ((bytes (pg::physical-logical-encode-promotion-intent enabled)))
        (ok (= 18 (length bytes)))
        (ok (equalp #(80 71 76 79 71 80 73 49) (subseq bytes 0 8)))
        (ok (eql enabled (pg::physical-logical-decode-promotion-intent bytes)))
        (dotimes (index 18)
          (let ((bad (copy-seq bytes)))
            (setf (aref bad index) (if (= index 8) 2 (logxor 1 (aref bad index))))
            (ok (intake-fails-p (lambda () (pg::physical-logical-decode-promotion-intent bad))))))
        (dolist (bad (list (subseq bytes 0 17)
                           (concatenate '(vector (unsigned-byte 8)) bytes #(0))))
          (ok (intake-fails-p (lambda () (pg::physical-logical-decode-promotion-intent bad)))))))
  (dolist (value '(0 1 :off "on"))
    (ok (intake-fails-p (lambda () (pg::physical-logical-encode-promotion-intent value))))))

(deftest promotion-logical-control-composes-intents-with-every-optional-layout
    (ok (equal
         '(((88 3047467184057977510) (356 10222404067229254730))
           ((105 6362589021702428304) (373 784685876338193293))
           ((105 15851637857956649796) (373 4463505136374351416)))
         (loop for fpw in '(:absent nil t) collect
                                           (loop for full in '(nil t) collect
                                                                      (let ((bytes (promotion-logical-codec-image
                                                                                    :absent :fpw fpw :resources full :fpw-source full :source full
                                                                                    :selected full :retained full)))
                                                                        (list (length bytes)
                                                                              (pg::physical-store-update-digest #xCBF29CE484222325 bytes)))))))
  (dolist (resources '(nil t))
    (dolist (fpw-source '(nil t))
      (dolist (source '(nil t))
        (dolist (selected '(nil t))
          (dolist (retained '(nil t))
            (dolist (fpw '(:absent nil t))
              (let* ((args (list :resources resources :fpw-source fpw-source :source source
                                 :selected selected :retained retained :fpw fpw))
                     (old (apply #'promotion-logical-codec-image :absent args))
                     (old-control (pg::physical-recovery-decode-control old)))
                (ok (not (get-properties old-control '(:promotion-logical-decoding-enabled))))
                (dolist (enabled '(nil t))
                  (let* ((bytes (apply #'promotion-logical-codec-image enabled args))
                         (decoded (pg::physical-recovery-decode-control bytes)))
                    (ok (= (+ 18 (length old)) (length bytes)))
                    (ok (get-properties decoded '(:promotion-logical-decoding-enabled)))
                    (ok (eql enabled (getf decoded :promotion-logical-decoding-enabled)))
                    (remf decoded :promotion-logical-decoding-enabled)
                    (ok (equal old-control decoded)))))))))))
  (ok (= 391 (length (promotion-logical-codec-image
                      nil :fpw nil :resources t :fpw-source t :source t :selected t :retained t)))))

(deftest promotion-logical-control-rejects-malformed-intent-under-valid-crc
    (let* ((bytes (promotion-logical-codec-image
                   nil :fpw t :resources t :fpw-source t :source t :selected t :retained t))
           (offset (- (length bytes) 4 88 18)))
      (dotimes (index 18)
        (let ((bad (copy-seq bytes)))
          (setf (aref bad (+ offset index)) (if (= index 8) 255 (logxor 1 (aref bad (+ offset index)))))
          (promotion-logical-codec-refresh-crc bad)
          (ok (intake-fails-p (lambda () (pg::physical-recovery-decode-control bad)))))))
  (let* ((bytes (promotion-logical-codec-image nil :fpw t :source t))
         (logical-offset (- (length bytes) 4 88 18))
         (fpw-offset (- logical-offset 17))
         (reordered (concatenate '(vector (unsigned-byte 8))
                                 (subseq bytes 0 fpw-offset)
                                 (subseq bytes logical-offset (+ logical-offset 18))
                                 (subseq bytes fpw-offset logical-offset)
                                 (subseq bytes (+ logical-offset 18)))))
    (promotion-logical-codec-refresh-crc reordered)
    (ok (intake-fails-p (lambda () (pg::physical-recovery-decode-control reordered)))))
  (dolist (logical '(nil t))
    (let* ((bytes (pg::physical-recovery-control-image
                   3 42 1 4 32 152 18446744073709551616
                   :state-time 0 :promotion-child 4 :promotion-generation 9
                   :promotion-source-namespace :legacy :promotion-full-page-writes nil
                   :promotion-logical-decoding-enabled logical))
           (decoded (pg::physical-recovery-decode-control bytes)))
      (ok (= 18446744073709551616 (getf decoded :next-xid)))
      (ok (get-properties decoded '(:promotion-logical-decoding-enabled)))
      (ok (eql logical (getf decoded :promotion-logical-decoding-enabled)))))
  (dolist (phase '(0 1 2))
    (dolist (enabled '(nil t))
      (ok (intake-fails-p
           (lambda () (pg::physical-recovery-control-image
                       phase 42 1 0 32 32 50 :promotion-logical-decoding-enabled enabled))))))
  (ok (intake-fails-p (lambda () (promotion-logical-codec-image 1)))))

(deftest source-logical-promotion-backlog-checks-the-published-logical-seed
    (dolist (enabled '(:unknown nil t))
      (let* ((fact (unless (eq enabled :unknown) (source-logical-promotion-fact enabled 32)))
             (pg::*physical-recovery-state* (list :cluster-identity "logical-backlog"
                                                  :source-logical-fact fact))
             (pg::*physical-promotion-preserve-readers-p* t)
             (pg::*last-replay-lsn* 65) (pg::*next-xid* 3)
             (functions '(pg::physical-recovery-flush pg::physical-recovery-reader-conflict-p
                          pg::physical-recovery-replay-core pg::physical-recovery-proven-drops))
             (saved (mapcar #'symbol-function functions))
             (calls 0) (arguments nil))
        (unwind-protect
             (progn
               (setf (symbol-function (first functions)) (lambda () nil)
                     (symbol-function (second functions)) (lambda () nil)
                     (symbol-function (third functions)) (lambda () nil)
                     (symbol-function (fourth functions))
                     (lambda (store identity &rest keys)
                       (declare (ignore store identity))
                       (incf calls) (setf arguments keys)
                       (values nil 65)))
               (ok (pg::physical-promotion-replay-backlog))
               (ok (= 1 calls))
               (ok (equal fact (getf arguments :logical-seed)))
               (when (eq enabled :unknown)
                 (ok (not (get-properties arguments '(:logical-seed))))))
          (loop for name in functions for function in saved
                do (setf (symbol-function name) function))))))

(deftest source-logical-readiness-rejects-a-seed-at-the-wrong-frontier
    (with-restartpoint-fixture (root store)
      (declare (ignore root))
      (selected-replay-test-prepare store)
      (let* ((fact (source-logical-promotion-fact t 32))
             (before nil))
        (setf (getf fact :through) 96
              (getf pg::*physical-recovery-state* :source-logical-fact) fact)
        (ok (pg::physical-promotion-awaiting-child-initialization-p))
        (decf (getf fact :through))
        (setf before (copy-tree pg::*physical-recovery-state*))
        (ok (intake-fails-p #'pg::physical-promotion-awaiting-child-initialization-p))
        (ok (equal before pg::*physical-recovery-state*))
        (ok (= 96 pg::*last-replay-lsn*)))))

(cl-spec:defgenerator source-logical-promotion-arguments ()
  (let* ((fact (source-logical-promotion-fact (zerop (random 2)) (+ 32 (random 1000000))))
         (end (getf fact :through)))
    (list (list :logical-fact fact)
          (list :lsn end :timeline 1 :source-logical-fact fact))))

(cl-spec:defspec-function pg::physical-promotion-frozen-logical-fact
    (:args (control (type list)) (metadata (type list)))
  (:args-generator source-logical-promotion-arguments)
  (:pre (and (pg::physical-logical-fact-valid-p (getf metadata :source-logical-fact))
             (equal (getf control :logical-fact) (getf metadata :source-logical-fact))))
  (:returns (type list))
  (:post (and (equal result (getf metadata :source-logical-fact))
              (not (eq result (getf metadata :source-logical-fact)))
              (not (eq result (getf control :logical-fact))))))

(cl-spec:defproperty frozen-source-logical-fact-preserves-generation-evidence
    ((enabled (member nil t)) (start (range integer 32 1000000)) (saved (member nil t)))
  (:about pg::physical-promotion-frozen-logical-fact)
  (:trials (:normal 150))
  (let* ((fact (source-logical-promotion-fact enabled start))
         (metadata (list :lsn (+ start 33) :timeline 1 :source-logical-fact fact))
         (control (when saved (list :logical-fact fact)))
         (result (pg::physical-promotion-frozen-logical-fact control metadata))
         (bad (copy-tree fact)))
    (decf (getf bad :coverage-start))
    (and (equal result fact) (not (eq result fact))
         (pg::physical-logical-fact-valid-p bad)
         (intake-fails-p
          (lambda () (pg::physical-promotion-frozen-logical-fact (list :logical-fact bad) metadata))))))

(deftest generated-source-logical-promotion-frozen-fact-contract
    (let ((data (cl-spec:result-data
                 (cl-spec:check-function 'pg::physical-promotion-frozen-logical-fact :trials 150 :seed 7201))))
      (ok (eq :passed (getf data :status)) (format nil "~s" data)))
  (let ((data (cl-spec:result-data
               (cl-spec:run-property 'frozen-source-logical-fact-preserves-generation-evidence
                                     :seed 7202 :profile :normal))))
    (ok (eq :passed (getf data :status)) (format nil "~s" data))))

(deftest source-logical-pending-promotion-keeps-frozen-source-on-cold-read
    (dolist (source '(:unknown nil t))
      (dolist (namespace '(:legacy :timeline))
        (call-with-promotion-fixture
         (lambda (root store)
           (pg::wal-atomic-write-bytes (pg::clog-path) (empty-octets))
           (pg::wal-atomic-write-bytes (pg::pgdata-path "PG_VERSION")
                                       (pg::string->bytes (format nil "16~%")))
           (unless (eq source :unknown)
             (let ((end (append-source-logical-test-status store source)))
               (ok (= end (pg::physical-replay-from pg::*last-replay-lsn*)))
               (pg::physical-store-compact store end #'pg::physical-restartpoint-publish)))
           (let* ((expected (copy-tree (getf pg::*physical-recovery-state* :source-logical-fact)))
                  (primary pg::*wal-logical-policy*)
                  (pending (call-with-promotion-exclusion
                            (lambda () (pg::physical-promotion-publish-intent 2))))
                  (disk (pg::physical-recovery-read-control root)))
             (ok (equal expected (getf disk :logical-fact)))
             (ok (equal expected (getf pending :source-logical-fact)))
             (ok (equal expected (getf pending :required-logical-fact)))
             (when expected
               (ok (not (eq (getf pending :source-logical-fact)
                            (getf pending :required-logical-fact)))))
             (dotimes (retry 2)
               (declare (ignorable retry))
               (multiple-value-bind (cold metadata prefix generation) (pg::physical-promotion-read-pending-state)
                 (declare (ignore prefix generation))
                 (ok (equal expected (getf cold :source-logical-fact)))
                 (ok (equal expected (getf cold :required-logical-fact)))
                 (when expected
                   (ok (not (eq (getf cold :source-logical-fact) (getf cold :required-logical-fact))))
                   (ok (not (eq (getf cold :source-logical-fact) (getf metadata :source-logical-fact)))))))
             (ok (eq primary pg::*wal-logical-policy*))))
         :namespace namespace
         :system-id (pg::physical-store-update-digest
                     #xCBF29CE484222325 (pg::string->bytes "history-source"))))))
