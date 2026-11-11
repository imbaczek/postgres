(in-package #:pg)

(defun unit-logical-status-snapshot (status resources commit-ts fpw)
  (append (unit-fpw-snapshot fpw resources commit-ts)
          (unless (eq status :unknown) (list :logical-decoding-enabled status))))

(cl-spec:defgenerator unit-logical-status-fact-arguments ()
  (let* ((status (nth (random 3) '(:unknown nil t)))
         (known-interval (and (eq status t) (zerop (random 2)))))
    (list (append (list :unrelated 7)
                  (unless (eq status :unknown)
                    (list :logical-decoding-enabled status))
                  (when known-interval
                    (list :logical-on-start-lsn (+ 33 (random 100000))
                          :logical-on-start-timeline (1+ (random 4))))))))

(cl-spec:defspec-function wal-copy-checkpoint-logical-decoding-fields
    (:args (facts (type list)))
  (:args-generator unit-logical-status-fact-arguments)
  (:returns (type list))
  (:post (if (get-properties facts '(:logical-decoding-enabled))
             (and (equal result
                         (append (list :logical-decoding-enabled (getf facts :logical-decoding-enabled))
                                 (when (getf facts :logical-on-start-lsn)
                                   (list :logical-on-start-lsn (getf facts :logical-on-start-lsn)
                                         :logical-on-start-timeline (getf facts :logical-on-start-timeline)))))
                  (not (eq result facts)))
             (null result))))

(cl-spec:defproperty checkpoint-logical-status-roundtrip
    ((status (member :unknown nil t)) (resources (member nil t))
                                      (commit-ts (member nil t)) (fpw (member :unknown nil t)))
  (:about wal-encode-checkpoint-snapshot wal-decode-checkpoint-snapshot
          wal-copy-checkpoint-logical-decoding-fields)
  (:trials (:normal 150))
  (let* ((snapshot (unit-logical-status-snapshot status resources commit-ts fpw))
         (bytes (wal-encode-checkpoint-snapshot snapshot))
         (decoded (wal-decode-checkpoint-snapshot bytes)))
    (and (equal snapshot decoded)
         (= (length bytes) (+ (if resources 136 88) (if commit-ts 8 0)))
         (equalp bytes (wal-encode-checkpoint-snapshot decoded)))))

(rove:deftest checkpoint-logical-status-preserves-history-and-control-copy
    (dolist (status '(:unknown nil t))
      (dolist (resources '(nil t))
        (dolist (commit-ts '(nil t))
          (dolist (fpw '(:unknown nil t))
            (let* ((snapshot (unit-logical-status-snapshot status resources commit-ts fpw))
                   (bytes (wal-encode-checkpoint-snapshot snapshot))
                   (expected (case status (:unknown 0) ((nil) 32) (otherwise 96)))
                   (metadata (append (make-wal-control-metadata 42 4 0)
                                     (wal-copy-control-resource-fields snapshot)))
                   (control (wal-encode-control-image
                             (getf snapshot :end) (getf snapshot :end) snapshot
                             :timeline 3 :checkpoint-timeline 3 :namespace :timeline
                             :metadata metadata)))
              (rove:ok (wal-checkpoint-snapshot-valid-p snapshot))
              (rove:ok (= expected (logand 96 (wal-u32 bytes 84))))
              (rove:ok (equal snapshot (wal-decode-checkpoint-snapshot bytes)))
              (rove:ok (equal snapshot
                              (third (multiple-value-list
                                      (wal-decode-control-line (bytes->string control))))))
              (let ((copy (wal-copy-checkpoint-logical-decoding-fields snapshot)))
                (rove:ok (equal copy (unless (eq status :unknown)
                                       (list :logical-decoding-enabled status))))
                (when copy
                  (setf (getf copy :logical-decoding-enabled) (not status))
                  (rove:ok (eq status (getf snapshot :logical-decoding-enabled)))))))))))

(rove:deftest checkpoint-logical-status-rejects-malformed-facts-and-flags
    (dolist (facts '((:logical-decoding-enabled 0)
                     (:logical-decoding-enabled :unknown)
                     (:logical-decoding-enabled t :logical-decoding-enabled nil)
                     (:logical-decoding-enabled nil :logical-decoding-enabled nil)))
      (let ((snapshot (append (unit-commit-ts-snapshot) facts)))
        (rove:ok (not (wal-checkpoint-logical-decoding-fields-valid-p facts)))
        (rove:ok (not (wal-checkpoint-snapshot-valid-p snapshot)))
        (dolist (function (list (lambda () (wal-copy-checkpoint-logical-decoding-fields facts))
                                (lambda () (wal-checkpoint-record-size snapshot))
                                (lambda () (wal-encode-checkpoint-snapshot snapshot))))
          (rove:ok (unit-commit-ts-error-p function)))))
  (dolist (facts '((:logical-decoding-enabled) (:unrelated)))
    (rove:ok (not (wal-checkpoint-logical-decoding-fields-valid-p facts))))
  (dolist (flags '(64 65 80 128 255))
    (let ((bytes (wal-encode-checkpoint-snapshot (unit-commit-ts-snapshot))))
      (wal-set-u32 bytes 84 flags)
      (rove:ok (unit-commit-ts-error-p (lambda () (wal-decode-checkpoint-snapshot bytes))))))
  (dolist (status '(nil t))
    (let ((snapshot (unit-logical-status-snapshot status nil nil :unknown)))
      (setf (getf snapshot :version) 1)
      (rove:ok (not (wal-checkpoint-snapshot-valid-p snapshot))))
    (let* ((snapshot (unit-logical-status-snapshot status nil nil :unknown))
           (bytes (wal-encode-checkpoint-snapshot snapshot))
           (record (make-wal-record :version 7 :type +wal-record-checkpoint+
                                    :xid 0 :start 64 :end (getf snapshot :end) :payload bytes)))
      (rove:ok (unit-commit-ts-error-p (lambda () (wal-checkpoint-record-snapshot record))))
      (setf (wal-record-version record) 8)
      (rove:ok (equal snapshot (wal-checkpoint-record-snapshot record)))
      (setf (aref bytes 7) (char-code #\1))
      (rove:ok (unit-commit-ts-error-p (lambda () (wal-decode-checkpoint-snapshot bytes)))))))

(rove:deftest checkpoint-logical-status-capture-keeps-caller-facts
    (dolist (status '(:unknown nil t))
      (let* ((*xid-lock* (sb-thread:make-mutex :name "logical snapshot XID"))
             (*write-lock* (sb-thread:make-mutex :name "logical snapshot writer"))
             (*wal-lock* (sb-thread:make-mutex :name "logical snapshot WAL"))
             (*next-xid* 3) (*xid-batch-end* 3) (*active-xids* nil) (*catalog* nil)
             (*wal-insert-timeline* 1) (*wal-prev-timeline* 1)
             (facts (unless (eq status :unknown) (list :logical-decoding-enabled status))))
        (sb-thread:with-mutex (*write-lock*)
          (sb-thread:with-mutex (*wal-lock*)
            (let ((snapshot (wal-capture-checkpoint-snapshot 64 facts)))
              (rove:ok (equal facts (wal-copy-checkpoint-logical-decoding-fields snapshot)))
              (when facts
                (setf (getf facts :logical-decoding-enabled) (not status))
                (rove:ok (eq status (getf snapshot :logical-decoding-enabled))))))))))

(rove:deftest checkpoint-logical-status-generated-contracts
    (dolist (result
             (list (cl-spec:result-data
                    (cl-spec:check-function 'wal-copy-checkpoint-logical-decoding-fields
                                            :trials 150 :seed 250))
                   (cl-spec:result-data
                    (cl-spec:run-property 'checkpoint-logical-status-roundtrip :seed 251))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))))
