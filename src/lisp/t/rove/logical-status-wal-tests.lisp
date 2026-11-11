(in-package #:pg)

(defun unit-logical-status-record (enabled &key (start 216) (version 8) (xid 0) (type 18))
  (make-wal-record :version version :type type :xid xid :start start :end (+ start 33)
                   :payload (wal-encode-logical-decoding-status-change enabled)))

(cl-spec:defgenerator unit-logical-status-change-arguments ()
  (list (zerop (random 2))))

(cl-spec:defspec-function wal-encode-logical-decoding-status-change
    (:args (enabled (type boolean)))
  (:args-generator unit-logical-status-change-arguments)
  (:returns (type (simple-array (unsigned-byte 8) (1))))
  (:post (eq enabled (wal-decode-logical-decoding-status-change result))))

(cl-spec:defproperty logical-status-change-roundtrip
    ((enabled (member nil t)) (start (range integer 256 1000000)))
  (:about wal-encode-logical-decoding-status-change
          wal-decode-logical-decoding-status-change wal-logical-decoding-status-record-info)
  (:trials (:normal 150))
  (eq enabled (wal-logical-decoding-status-record-info
               (unit-logical-status-record enabled :start start))))

(rove:deftest logical-status-change-codec-and-envelope
    (dolist (enabled '(nil t))
      (let* ((bytes (make-array 1 :element-type '(unsigned-byte 8)
                                  :initial-element (if enabled 1 0)))
             (record (unit-logical-status-record enabled))
             (wire (wal-encode-record 18 0 216 bytes)))
        (rove:ok (equalp bytes (wal-encode-logical-decoding-status-change enabled)))
        (rove:ok (eq enabled (wal-decode-logical-decoding-status-change bytes)))
        (rove:ok (eq enabled (wal-logical-decoding-status-record-info record)))
        (rove:ok (eq enabled (wal-logical-decoding-status-record-info
                              (wal-decode-record wire 216))))
        (rove:ok (eq enabled (wal-validate-empty-metadata-terminal record t)))
        (rove:ok (wal-terminal-record-type-p 18))
        (rove:ok (wal-empty-metadata-terminal-type-p 18))
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (wal-validate-empty-metadata-terminal record nil)))))))

(rove:deftest logical-status-change-rejects-malformed-terminals
    (rove:ok (wal-valid-type-p 18 8))
  (dolist (version '(2 3 4 5 6 7))
    (rove:ok (not (wal-valid-type-p 18 version))))
  (dolist (enabled '(nil t))
    (dolist (record (list (unit-logical-status-record enabled :version 7)
                          (unit-logical-status-record enabled :xid 3)
                          (unit-logical-status-record enabled :type 17)
                          (unit-logical-status-record enabled :start (- +wal-segment-size+ 16))))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (wal-logical-decoding-status-record-info record)))))
    (let ((record (unit-logical-status-record enabled)))
      (incf (wal-record-end record))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (wal-logical-decoding-status-record-info record))))))
  (dolist (bad '(0 1 :on :off "on"))
    (rove:ok (unit-commit-ts-error-p
              (lambda () (wal-encode-logical-decoding-status-change bad)))))
  (dolist (bytes (list (make-array 0 :element-type '(unsigned-byte 8))
                       (make-array 2 :element-type '(unsigned-byte 8) :initial-element 0)
                       (make-array 1 :element-type '(unsigned-byte 8) :initial-element 2)))
    (rove:ok (unit-commit-ts-error-p
              (lambda () (wal-decode-logical-decoding-status-change bytes))))))

(rove:deftest logical-status-change-physical-consumers-check-before-mutating
    (let ((*wal-recovery-group-active* :unchanged)
          (record (unit-logical-status-record nil)))
      (setf (aref (wal-record-payload record) 0) 2)
      (rove:ok (rove:signals (physical-history-accept-record record "source" nil) 'error))
      (rove:ok (eq :unchanged *wal-recovery-group-active*))
      (rove:ok (rove:signals (physical-replay-check-child-initialization record 3) 'error))
      (rove:ok (eq :unchanged *wal-recovery-group-active*))))

(rove:deftest logical-status-history-fold-preserves-enable-boundaries
    (let* ((checkpoint (unit-logical-status-snapshot t nil nil :unknown))
           (seed (wal-logical-status-from-checkpoint checkpoint))
           (off (wal-logical-status-next seed (unit-logical-status-record nil) 3))
           (on (wal-logical-status-next off (unit-logical-status-record t :start 300) 3))
           (on-again (wal-logical-status-next on (unit-logical-status-record t :start 400) 3)))
      (rove:ok (null (wal-logical-status-from-checkpoint
                      (unit-logical-status-snapshot :unknown nil nil :unknown))))
      (rove:ok (wal-logical-status-enabled seed))
      (rove:ok (null (wal-logical-status-last-enable-lsn seed)))
      (rove:ok (null (wal-logical-status-enabled off)))
      (rove:ok (wal-logical-status-enabled on))
      (rove:ok (= 333 (wal-logical-status-last-enable-lsn on)))
      (rove:ok (= 433 (wal-logical-status-last-enable-lsn on-again)))
      (rove:ok (= 333 (wal-logical-status-last-enable-lsn on)))
      (rove:ok (wal-logical-status-enabled seed))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (wal-logical-status-next on-again (unit-logical-status-record nil) 3))))))

(rove:deftest logical-status-history-checkpoints-agree-with-ordered-status
    (let* ((on (wal-logical-status-next nil (unit-logical-status-record t) 3))
           (snapshot (unit-logical-status-snapshot t nil nil :unknown))
           (unknown (unit-logical-status-snapshot :unknown nil nil :unknown)))
      (dolist (s (list snapshot unknown))
        (setf (getf s :start) 512 (getf s :redo) 512 (getf s :end) 632))
      (labels ((record (s)
                 (make-wal-record :version 8 :type +wal-record-checkpoint+ :xid 0
                                  :start 512 :end 632 :payload (wal-encode-checkpoint-snapshot s))))
        (let ((after (wal-logical-status-next on (record snapshot) 3)))
          (rove:ok (= 249 (wal-logical-status-last-enable-lsn after)))
          (rove:ok (= 632 (wal-logical-status-end after)))
          (rove:ok (eq on (wal-logical-status-next on (record unknown) 3))))
        (setf (getf snapshot :logical-decoding-enabled) nil)
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (wal-logical-status-next on (record snapshot) 3))))
        (setf (getf snapshot :logical-decoding-enabled) t)
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (wal-logical-status-next on (record snapshot) 2)))))))

(rove:deftest logical-status-change-shares-complete-batch-consumer-validation
    (dolist (enabled '(nil t))
      (let* ((batch (make-wal-record
                     :version 8 :type +wal-record-batch-start+ :xid 0 :start 0 :end 32
                     :payload (make-array 0 :element-type '(unsigned-byte 8))))
             (terminal (unit-logical-status-record enabled :start 32))
             (pages (make-wal-record
                     :version 8 :type +wal-record-pages+ :xid 0 :start 32 :end 68
                     :payload (make-array 4 :element-type '(unsigned-byte 8) :initial-element 0))))
        (dolist (consumer (list #'wal-replay-record
                                (lambda (record) (physical-history-accept-record record "source" nil))))
          (let ((*wal-checkpoint-lsn* 0) (*wal-recovery-page-spool-enabled* nil)
                                         (*physical-history-reset-forks* nil))
            (with-wal-replay-staging ()
              (rove:ok (unit-commit-ts-error-p (lambda () (funcall consumer terminal)))))
            (with-wal-replay-staging ()
              (funcall consumer batch)
              (rove:ok *wal-recovery-group-active*)
              (funcall consumer terminal)
              (rove:ok (null *wal-recovery-group-active*))
              (rove:ok (null *wal-recovery-reservation-batch-clean-p*)))
            (with-wal-replay-staging ()
              (funcall consumer batch)
              (funcall consumer pages)
              (rove:ok (null *wal-recovery-reservation-batch-clean-p*))
              (rove:ok (unit-commit-ts-error-p
                        (lambda () (funcall consumer
                                            (unit-logical-status-record enabled :start 68)))))
              (rove:ok *wal-recovery-group-active*)))))))

(rove:deftest logical-status-recovery-validates-checkpoint-before-redo
    (let* ((*wal-checkpoint-snapshot* (unit-logical-status-snapshot t nil nil :unknown))
           (*wal-checkpoint-lsn* (getf *wal-checkpoint-snapshot* :end))
           (*wal-insert-timeline* 3)
           (bad (unit-logical-status-snapshot nil nil nil :unknown))
           (applied 0) (synced 0) (installed 0)
           (names '(wal-scan wal-replay-record clog-fsync wal-fpw-install-recovered-state))
           (originals (mapcar #'symbol-function names)))
      (setf (getf bad :start) 512 (getf bad :redo) 512 (getf bad :end) 632)
      (let ((record (make-wal-record
                     :version 8 :type +wal-record-checkpoint+ :xid 0 :start 512 :end 632
                     :payload (wal-encode-checkpoint-snapshot bad))))
        (unwind-protect
             (progn
               (setf (symbol-function 'wal-scan)
                     (lambda (function &rest options)
                       (declare (ignore options))
                       (funcall function record))
                     (symbol-function 'wal-replay-record)
                     (lambda (record) (declare (ignore record)) (incf applied))
                     (symbol-function 'clog-fsync) (lambda () (incf synced))
                     (symbol-function 'wal-fpw-install-recovered-state)
                     (lambda (&rest args) (declare (ignore args)) (incf installed)))
               (rove:ok (unit-commit-ts-error-p #'wal-recover))
               (rove:ok (zerop applied))
               (rove:ok (zerop synced))
               (rove:ok (zerop installed)))
          (loop for name in names for original in originals
                do (setf (symbol-function name) original))))))

(rove:deftest logical-status-change-generated-contracts
    (dolist (result
             (list (cl-spec:result-data
                    (cl-spec:check-function 'wal-encode-logical-decoding-status-change
                                            :trials 150 :seed 252))
                   (cl-spec:result-data
                    (cl-spec:run-property 'logical-status-change-roundtrip :seed 253))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))))
