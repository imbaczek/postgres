(in-package #:pg)

(defun unit-fpw-change-record (enabled &key (start 216) (version 8) (xid 0) (type 17))
  (make-wal-record :version version :type type :xid xid :start start :end (+ start 48)
                   :payload (wal-encode-full-page-writes-change enabled)))

(defun unit-fpw-checkpoint-record (policy start)
  (let* ((snapshot (append (unit-commit-ts-snapshot :start start)
                           (unless (eq policy :unknown) (list :full-page-writes policy))))
         (payload (wal-encode-checkpoint-snapshot snapshot)))
    (make-wal-record :version 8 :type +wal-record-checkpoint+ :xid 0 :start start
                     :end (getf snapshot :end) :payload payload)))

(cl-spec:defgenerator unit-fpw-change-arguments ()
  (list (zerop (random 2))))

(cl-spec:defspec-function wal-encode-full-page-writes-change
    (:args (enabled (type boolean)))
  (:args-generator unit-fpw-change-arguments)
  (:returns (type (simple-array (unsigned-byte 8) (16))))
  (:post (eq enabled (wal-decode-full-page-writes-change result))))

(cl-spec:defproperty full-page-writes-change-roundtrip
    ((enabled (member nil t)) (start (range integer 256 1000000)))
  (:about wal-encode-full-page-writes-change wal-decode-full-page-writes-change
          wal-full-page-writes-change-record-info)
  (:trials (:normal 100))
  (eq enabled (wal-full-page-writes-change-record-info
               (unit-fpw-change-record enabled :start start))))

(rove:deftest full-page-writes-change-codec-has-independent-bytes
    (dolist (enabled '(nil t))
      (let ((expected (make-array 16 :element-type '(unsigned-byte 8) :initial-element 0)))
        (replace expected #(80 71 70 80 87 48 48 49))
        (setf (aref expected 8) (if enabled 1 0))
        (rove:ok (equalp expected (wal-encode-full-page-writes-change enabled)))
        (rove:ok (eq enabled (wal-decode-full-page-writes-change expected)))
        (dolist (offset '(0 8 9 15))
          (let ((bad (copy-seq expected)))
            (setf (aref bad offset) 2)
            (rove:ok (unit-commit-ts-error-p
                      (lambda () (wal-decode-full-page-writes-change bad))))))
        (dolist (bad (list (subseq expected 0 15)
                           (concatenate '(vector (unsigned-byte 8)) expected #(0))))
          (rove:ok (unit-commit-ts-error-p
                    (lambda () (wal-decode-full-page-writes-change bad)))))))
  (dolist (bad '(0 1 :on :off "on"))
    (rove:ok (unit-commit-ts-error-p
              (lambda () (wal-encode-full-page-writes-change bad))))))

(rove:deftest full-page-writes-change-requires-a-clean-supported-terminal
    (rove:ok (wal-valid-type-p 17 8))
  (rove:ok (not (wal-valid-type-p 17 7)))
  (dolist (enabled '(nil t))
    (let ((record (unit-fpw-change-record enabled)))
      (rove:ok (eq enabled (wal-full-page-writes-change-record-info record)))
      (rove:ok (eq enabled (wal-validate-empty-metadata-terminal record t)))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (wal-validate-empty-metadata-terminal record nil)))))
    (dolist (record (list (unit-fpw-change-record enabled :version 7)
                          (unit-fpw-change-record enabled :type 16)
                          (unit-fpw-change-record enabled :xid 3)
                          (unit-fpw-change-record enabled :start (- +wal-segment-size+ 16))))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (wal-full-page-writes-change-record-info record)))))))

(rove:deftest full-page-writes-state-fold-retains-witness-and-disable-history
    (let* ((incarnation (gensym "FPW-STORE-"))
           (unknown (wal-fpw-state-from-checkpoint
                     (unit-fpw-snapshot :unknown nil nil) incarnation))
           (checkpoint (unit-fpw-snapshot t nil nil))
           (seed (wal-fpw-state-from-checkpoint checkpoint incarnation))
           (off-record (unit-fpw-change-record nil))
           (off (wal-fpw-state-next seed off-record 3 incarnation))
           (on-record (unit-fpw-change-record t :start 296))
           (on (wal-fpw-state-next off on-record 3 incarnation)))
      (rove:ok (null unknown))
      (rove:ok (eq :checkpoint (wal-fpw-state-witness-kind seed)))
      (rove:ok (null (wal-fpw-state-last-disable-start seed)))
      (rove:ok (eq nil (wal-fpw-state-enabled off)))
      (rove:ok (= 216 (wal-fpw-state-last-disable-start off)))
      (rove:ok (eq t (wal-fpw-state-enabled on)))
      (rove:ok (= 216 (wal-fpw-state-last-disable-start on)))
      (rove:ok (= 344 (wal-fpw-state-end on)))
      (rove:ok (= 64 (wal-fpw-state-coverage-start on)))
      (rove:ok (eq on (wal-fpw-state-next on on-record 3 incarnation)))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (wal-fpw-state-next on on-record 3 (gensym "OTHER-STORE-")))))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (wal-fpw-state-next on off-record 3 incarnation))))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (wal-fpw-state-next on (unit-fpw-checkpoint-record nil 512) 3 incarnation))))
      (let ((later (wal-fpw-state-next on (unit-fpw-checkpoint-record t 512) 3 incarnation)))
        (rove:ok (eq :checkpoint (wal-fpw-state-witness-kind later)))
        (rove:ok (= 216 (wal-fpw-state-last-disable-start later))))
      (rove:ok (eq on (wal-fpw-state-next on (unit-fpw-checkpoint-record :unknown 512) 3 incarnation)))))

(rove:deftest full-page-writes-change-generated-contracts
    (dolist (result
             (list (cl-spec:result-data
                    (cl-spec:check-function 'wal-encode-full-page-writes-change :trials 100 :seed 251))
                   (cl-spec:result-data
                    (cl-spec:run-property 'full-page-writes-change-roundtrip :seed 252))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))))

(rove:deftest full-page-writes-physical-consumers-reject-malformed-policy-before-credit
    (let ((*wal-recovery-group-active* :unchanged)
          (record (unit-fpw-change-record nil)))
      (setf (aref (wal-record-payload record) 9) 1)
      (rove:ok (rove:signals (physical-history-accept-record record "source" nil) 'error))
      (rove:ok (eq :unchanged *wal-recovery-group-active*))
      (rove:ok (rove:signals (physical-replay-check-child-initialization record 3) 'error))
      (rove:ok (eq :unchanged *wal-recovery-group-active*))))
