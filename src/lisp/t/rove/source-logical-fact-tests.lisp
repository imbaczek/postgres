(in-package #:pg)

(defun unit-source-logical-fact (&key (enabled nil) (start 32))
  (list :enabled enabled :witness-kind :change :witness-start start :witness-end (+ start 33)
        :witness-timeline 1 :coverage-start start :coverage-timeline 1
        :through (+ start 33) :through-timeline 1
        :last-enable-lsn (when enabled (+ start 33)) :last-enable-timeline (when enabled 1)
        :last-disable-lsn (unless enabled (+ start 33)) :last-disable-timeline (unless enabled 1)))

(defun unit-source-logical-checkpoint (enabled start)
  (let ((snapshot (unit-logical-status-snapshot enabled nil nil :unknown)))
    (setf (getf snapshot :start) start (getf snapshot :redo) start
          (getf snapshot :end) (+ start 120)
          (getf snapshot :timeline) 1 (getf snapshot :prev-timeline) 1)
    (make-wal-record :version 8 :type +wal-record-checkpoint+ :xid 0
                     :start start :end (+ start 120)
                     :payload (wal-encode-checkpoint-snapshot snapshot))))

(cl-spec:defgenerator unit-source-logical-arguments ()
  (list (unit-source-logical-fact :enabled (zerop (random 2)) :start (+ 32 (random 1000000)))))

(cl-spec:defspec-function physical-logical-encode-fact
    (:args (fact (type list)))
  (:args-generator unit-source-logical-arguments)
  (:pre (physical-logical-fact-valid-p fact))
  (:returns (type (simple-array (unsigned-byte 8) (88))))
  (:post (equal fact (physical-logical-decode-fact result))))

(cl-spec:defproperty source-logical-fact-roundtrip
    ((enabled (member nil t)) (start (range integer 32 1000000)))
  (:about physical-logical-encode-fact physical-logical-decode-fact)
  (:trials (:normal 150))
  (let ((fact (unit-source-logical-fact :enabled enabled :start start)))
    (equal fact (physical-logical-decode-fact (physical-logical-encode-fact fact)))))

(rove:deftest source-logical-fact-shape-and-owned-copy
    (let* ((fact (unit-source-logical-fact)) (copy (physical-logical-copy-fact fact)))
      (rove:ok (physical-logical-fact-valid-p fact))
      (rove:ok (equal fact copy)) (rove:ok (not (eq fact copy)))
      (setf (getf copy :through) 128)
      (rove:ok (= 65 (getf fact :through)))
      (rove:ok (null (physical-logical-copy-fact nil)))
      (dolist (bad (list nil (append fact '(:extra 1)) (append fact '(:enabled nil))
                         (cddr fact) (cons :enabled t)
                         (let ((cycle (copy-list fact))) (setf (cdr (last cycle)) cycle) cycle)))
        (rove:ok (not (physical-logical-fact-valid-p bad))))
      (dolist (pair '((:enabled 0) (:witness-kind :other) (:witness-end 64)
                      (:coverage-start 33) (:coverage-timeline 2) (:through 64)
                      (:through-timeline 0) (:last-disable-lsn nil) (:last-disable-timeline nil)))
        (let ((bad (copy-list fact)))
          (setf (getf bad (first pair)) (second pair))
          (rove:ok (not (physical-logical-fact-valid-p bad)))))))

(rove:deftest source-logical-fact-codec-contract
    (let* ((data (cl-spec:result-data
                  (cl-spec:check-function 'physical-logical-encode-fact :trials 150 :seed 42)))
           (passed (eq :passed (getf data :status))))
      (rove:ok passed (if passed "Source logical fact Function Spec passed" (format nil "~s" data))))
  (unit-check-property 'source-logical-fact-roundtrip))

(rove:deftest source-logical-fact-codec-rejects-noncanonical-bytes
    (dolist (enabled '(nil t))
      (let ((bytes (physical-logical-encode-fact (unit-source-logical-fact :enabled enabled))))
        (dolist (offset '(0 12 84))
          (let ((bad (copy-seq bytes)))
            (setf (aref bad offset) 1)
            (rove:ok (unit-commit-ts-error-p (lambda () (physical-logical-decode-fact bad))))))
        (let ((bad (copy-seq bytes)))
          (wal-set-u32 bad 8 16)
          (rove:ok (unit-commit-ts-error-p (lambda () (physical-logical-decode-fact bad)))))
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (physical-logical-decode-fact (subseq bytes 0 87)))))))
  (let ((bytes (physical-logical-encode-fact (unit-source-logical-fact :enabled t))))
    (wal-set-u64 bytes 64 65)
    (rove:ok (unit-commit-ts-error-p (lambda () (physical-logical-decode-fact bytes))))))

(rove:deftest source-logical-checkpoint-seed-preserves-unknown-and-off
    (dolist (enabled '(:unknown nil t))
      (let* ((snapshot (wal-checkpoint-record-snapshot
                        (unit-source-logical-checkpoint enabled 512)))
             (fact (physical-logical-fact-from-checkpoint snapshot)))
        (if (eq enabled :unknown)
            (rove:ok (null fact))
            (progn
              (rove:ok (physical-logical-fact-valid-p fact))
              (rove:ok (eq enabled (getf fact :enabled)))
              (rove:ok (= 512 (getf fact :coverage-start)))
              (rove:ok (null (getf fact :last-enable-lsn)))
              (rove:ok (null (getf fact :last-disable-lsn))))))))

(rove:deftest source-logical-fold-retains-both-change-boundaries
    (let* ((history (timeline-parse-history 1 ""))
           (on (physical-logical-fact-next nil (unit-logical-status-record t :start 32) 1 history))
           (off (physical-logical-fact-next on (unit-logical-status-record nil :start 128) 1 history))
           (again (physical-logical-fact-next off (unit-logical-status-record t :start 224) 1 history))
           (repeat (physical-logical-fact-next again (unit-logical-status-record t :start 320) 1 history))
           (checkpoint (physical-logical-fact-next repeat (unit-source-logical-checkpoint t 512) 1 history)))
      (rove:ok (= 65 (getf on :last-enable-lsn)))
      (rove:ok (= 161 (getf checkpoint :last-disable-lsn)))
      (rove:ok (= 353 (getf checkpoint :last-enable-lsn)))
      (rove:ok (= 32 (getf checkpoint :coverage-start)))
      (rove:ok (= 632 (getf checkpoint :through)))
      (rove:ok (eq :checkpoint (getf checkpoint :witness-kind)))
      (rove:ok (= 257 (getf again :last-enable-lsn)))
      (rove:ok (null (getf on :last-disable-lsn)))
      (rove:ok (equal checkpoint (physical-logical-decode-fact
                                  (physical-logical-encode-fact checkpoint))))))

(rove:deftest source-logical-fold-rejects-contradiction-and-regression
    (let* ((history (timeline-parse-history 1 ""))
           (on (physical-logical-fact-next nil (unit-logical-status-record t :start 32) 1 history)))
      (dolist (record (list (unit-source-logical-checkpoint nil 512)
                            (unit-logical-status-record nil :start 16)
                            (make-wal-record :version 8 :type +wal-record-batch-start+
                                             :xid 0 :start 128 :end 160 :payload #())))
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (physical-logical-fact-next on record 1 history)))))
      (rove:ok (= 65 (getf on :through)))))

(rove:deftest source-logical-fact-rejects-wrong-history-and-crossed-marker
    (let ((history (timeline-parse-history 3 (format nil "1 0/A0 ancestor~%")))
          (fact (unit-source-logical-fact :enabled t)))
      (setf (getf fact :through) 160)
      (rove:ok (equal fact (physical-logical-check-fact fact history 160 1)))
      (rove:ok (unit-commit-ts-error-p (lambda () (physical-logical-check-fact fact history 160 3))))
      (let ((bad (unit-source-logical-fact :start 136)))
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (physical-logical-check-fact bad history 169 3)))))))

(rove:deftest source-logical-shared-policy-preflight-rejects-crossed-segments
    (let ((history (timeline-parse-history 1 "")))
      (dolist (case (list (list #'physical-fpw-fact-next #'unit-fpw-change-record 48)
                          (list #'physical-logical-fact-next
                                (lambda (enabled &key start)
                                  (unit-logical-status-record enabled :start start)) 33)))
        (destructuring-bind (fold record size) case
          (rove:ok (funcall fold nil (funcall record t :start (- +wal-segment-size+ size)) 1 history))
          (rove:ok (unit-commit-ts-error-p
                    (lambda () (funcall fold nil (funcall record t :start (- +wal-segment-size+ 16))
                                        1 history))))))))

(rove:deftest source-logical-required-frontier-preserves-and-compares
    (let* ((history (timeline-parse-history 1 ""))
           (minimum (physical-logical-fact-next nil (unit-logical-status-record t :start 32) 1 history))
           (copy (physical-logical-required-fact nil minimum 0 1 65 1))
           (off (physical-logical-fact-next minimum (unit-logical-status-record nil :start 128) 1 history)))
      (rove:ok (equal minimum copy)) (rove:ok (not (eq minimum copy)))
      (rove:ok (equal minimum (physical-logical-required-fact minimum minimum 65 1 65 1)))
      (rove:ok (equal off (physical-logical-required-fact off minimum 161 1 65 1)))
      (let ((lost (copy-list minimum)))
        (setf (getf lost :coverage-start) 33)
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (physical-logical-required-fact lost minimum 65 1 65 1)))))))

(rove:deftest source-logical-required-frontier-rejects-erased-transitions
    (let* ((history (timeline-parse-history 1 ""))
           (off (physical-logical-fact-next nil (unit-logical-status-record nil :start 32) 1 history))
           (on (physical-logical-fact-next off (unit-logical-status-record t :start 128) 1 history))
           (later (physical-logical-fact-next on (unit-source-logical-checkpoint t 512) 1 history))
           (erased (copy-list later)))
      (setf (getf erased :last-disable-lsn) nil (getf erased :last-disable-timeline) nil)
      (rove:ok (physical-logical-fact-valid-p erased))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-logical-required-fact erased on 632 1 161 1))))
      (rove:ok (equal later (physical-logical-required-fact later on 632 1 161 1)))))

(rove:deftest source-logical-required-frontier-does-not-invent-checkpoint-continuity
    (let* ((history (timeline-parse-history 1 ""))
           (on (physical-logical-fact-next nil (unit-logical-status-record t :start 32) 1 history))
           (record (unit-source-logical-checkpoint t 512))
           (strong (physical-logical-fact-next on record 1 history))
           (minimum (physical-logical-fact-from-checkpoint (wal-checkpoint-record-snapshot record))))
      (rove:ok (null (getf minimum :last-enable-lsn)))
      (rove:ok (equal strong (physical-logical-required-fact strong minimum 632 1 632 1)))
      (rove:ok (= 65 (getf strong :last-enable-lsn)))))
