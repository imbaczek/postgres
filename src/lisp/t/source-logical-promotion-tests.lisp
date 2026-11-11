(in-package #:pg-physical-tests)

(defun source-logical-promotion-fact (enabled start)
  (pg::physical-logical-fact-next
   nil (pg::make-wal-record :version 8 :type 18 :xid 0 :start start :end (+ start 33)
                            :payload (pg::wal-encode-logical-decoding-status-change enabled))
   1 (pg::timeline-parse-history 1 "")))

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
