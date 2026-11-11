(in-package #:pg)

(defun unit-logical-status-record (enabled &key (start 216) (version 8) (xid 0) (type 18))
  (make-wal-record :version version :type type :xid xid :start start :end (+ start 33)
                   :payload (wal-encode-logical-decoding-status-change enabled)))

(defun unit-logical-on-interval (state)
  (list (funcall (symbol-function 'wal-logical-status-on-start-lsn) state)
        (funcall (symbol-function 'wal-logical-status-on-start-timeline) state)))

(cl-spec:defproperty logical-on-interval-survives-redundant-enable
    ((repeats (range integer 0 12)))
  (:about wal-logical-status-next)
  (:trials (:normal 100))
  (let* ((first (wal-logical-status-next nil (unit-logical-status-record t :start 128) 3))
         (current first))
    (dotimes (index repeats)
      (setf current (wal-logical-status-next
                     current (unit-logical-status-record t :start (+ 1000 (* 64 index))) 3)))
    (and (equal '(161 3) (unit-logical-on-interval first))
         (equal '(161 3) (unit-logical-on-interval current)))))

(rove:deftest logical-on-interval-distinguishes-lifetimes
    (let* ((first (wal-logical-status-next nil (unit-logical-status-record t :start 128) 3))
           (again (wal-logical-status-next first (unit-logical-status-record t :start 256) 3))
           (off (wal-logical-status-next again (unit-logical-status-record nil :start 384) 3))
           (next (wal-logical-status-next off (unit-logical-status-record t :start 512) 3))
           (unknown (wal-logical-status-from-checkpoint
                     (unit-logical-status-snapshot t nil nil :unknown))))
      (rove:ok (equal '(161 3) (unit-logical-on-interval first)))
      (rove:ok (equal '(161 3) (unit-logical-on-interval again)))
      (rove:ok (= 289 (wal-logical-status-last-enable-lsn again)))
      (rove:ok (equal '(nil nil) (unit-logical-on-interval off)))
      (rove:ok (equal '(545 3) (unit-logical-on-interval next)))
      (rove:ok (equal '(161 3) (unit-logical-on-interval first)))
      (rove:ok (equal '(nil nil) (unit-logical-on-interval unknown)))
      (rove:ok (equal '(nil nil)
                      (unit-logical-on-interval
                       (wal-logical-status-next unknown (unit-logical-status-record t :start 512) 3))))))

(rove:deftest logical-on-interval-generated-redundant-enables
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-on-interval-survives-redundant-enable :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 100 (getf data :trials)))))

(rove:deftest logical-on-interval-checkpoint-roundtrip
    (dolist (resources '(nil t))
      (dolist (commit-ts '(nil t))
        (let* ((snapshot (append (unit-logical-status-snapshot t resources commit-ts :unknown)
                                 (list :logical-on-start-lsn 65 :logical-on-start-timeline 3))))
          (incf (getf snapshot :start) 32)
          (incf (getf snapshot :redo) 32)
          (incf (getf snapshot :end) 48)
          (let* ((bytes (wal-encode-checkpoint-snapshot snapshot))
                 (decoded (wal-decode-checkpoint-snapshot bytes))
                 (seed (wal-logical-status-from-checkpoint decoded))
                 (control (wal-encode-control-image
                           (getf snapshot :end) (getf snapshot :end) snapshot
                           :timeline 3 :checkpoint-timeline 3 :namespace :timeline
                           :metadata (append (make-wal-control-metadata 42 4 0)
                                             (wal-copy-control-resource-fields snapshot)))))
            (rove:ok (= (length bytes) (+ (if resources 136 88) (if commit-ts 8 0) 16)))
            (rove:ok (logbitp 7 (wal-u32 bytes 84)))
            (rove:ok (equal snapshot decoded))
            (rove:ok (equal '(65 3) (unit-logical-on-interval seed)))
            (rove:ok (equal snapshot (third (multiple-value-list (wal-decode-control-line
                                                                  (string-right-trim '(#\Newline)
                                                                                     (bytes->string control)))))))
            (let ((copy (copy-seq bytes)))
              (setf (aref copy (1- (length copy))) 1)
              (rove:ok (unit-commit-ts-error-p (lambda () (wal-decode-checkpoint-snapshot copy)))))
            (let ((copy (copy-seq bytes)))
              (wal-set-u32 copy 84 (logandc2 (wal-u32 copy 84) 128))
              (rove:ok (unit-commit-ts-error-p (lambda () (wal-decode-checkpoint-snapshot copy))))))))))

(rove:deftest logical-on-interval-checkpoints-reject-invalid-pairs
    (dolist (fields '((:logical-on-start-lsn 65)
                      (:logical-on-start-timeline 3)
                      (:logical-decoding-enabled nil :logical-on-start-lsn 65 :logical-on-start-timeline 3)
                      (:logical-decoding-enabled t :logical-on-start-lsn nil :logical-on-start-timeline nil)
                      (:logical-decoding-enabled t :logical-on-start-lsn 65 :logical-on-start-timeline 3
                       :logical-on-start-lsn 65)))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (wal-copy-checkpoint-logical-decoding-fields fields)))))
  (dolist (pair '((0 3) (18446744073709551616 3) (65 0) (65 4294967296) (100000 3) (65 4)))
    (let ((snapshot (append (unit-logical-status-snapshot t nil nil :unknown)
                            (list :logical-on-start-lsn (first pair)
                                  :logical-on-start-timeline (second pair)))))
      (incf (getf snapshot :end) 16)
      (rove:ok (not (wal-checkpoint-snapshot-valid-p snapshot))))))

(rove:deftest logical-on-interval-checkpoint-fold-keeps-proven-history
    (let* ((on (wal-logical-status-next nil (unit-logical-status-record t :start 128) 3))
           (unknown (wal-logical-status-from-checkpoint
                     (unit-logical-status-snapshot t nil nil :unknown))))
      (labels ((checkpoint (interval)
                 (let ((snapshot (unit-logical-status-snapshot t nil nil :unknown)))
                   (setf (getf snapshot :start) 512 (getf snapshot :redo) 512
                         (getf snapshot :end) (if interval 648 632))
                   (when interval
                     (setf snapshot (append snapshot
                                            (list :logical-on-start-lsn interval
                                                  :logical-on-start-timeline 3))))
                   (make-wal-record :version 8 :type +wal-record-checkpoint+ :xid 0
                                    :start 512 :end (getf snapshot :end)
                                    :payload (wal-encode-checkpoint-snapshot snapshot)))))
        (rove:ok (equal '(161 3)
                        (unit-logical-on-interval
                         (wal-logical-status-next on (checkpoint nil) 3))))
        (rove:ok (equal '(161 3)
                        (unit-logical-on-interval
                         (wal-logical-status-next on (checkpoint 161) 3))))
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (wal-logical-status-next on (checkpoint 65) 3))))
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (wal-logical-status-next unknown (checkpoint 300) 3))))
        (rove:ok (equal '(161 3)
                        (unit-logical-on-interval
                         (wal-logical-status-next
                          (%make-wal-logical-status t 128 161 3 161 3)
                          (checkpoint 161) 3))))
        (rove:ok (unit-commit-ts-error-p
                  (lambda ()
                    (wal-logical-status-next
                     (%make-wal-logical-status t 128 161 3 161 3)
                     (checkpoint 200) 3))))
        (rove:ok (unit-commit-ts-error-p
                  (lambda ()
                    (wal-logical-status-next
                     (%make-wal-logical-status t 256 376 3 161 3)
                     (checkpoint 200) 3))))
        (rove:ok (equal '(33 3)
                        (unit-logical-on-interval
                         (wal-logical-status-next unknown (checkpoint 33) 3)))))))

(rove:deftest logical-on-interval-physical-state-roundtrip
    (let* ((record (unit-logical-status-record t :start 32))
           (state (wal-logical-status-next nil record 1))
           (fact (physical-logical-fact-from-state state :change 32 1 65 1 nil nil))
           (bytes (physical-logical-encode-fact fact))
           (history (timeline-check-lineage (timeline-parse-history 1 "")))
           (control (physical-recovery-control-image
                     2 42 1 77 32 65 3 :logical-fact fact)))
      (rove:ok (eql 65 (getf fact :logical-on-start-lsn)))
      (rove:ok (eql 1 (getf fact :logical-on-start-timeline)))
      (rove:ok (= 104 (length bytes)))
      (rove:ok (= 16 (wal-u32 bytes 100)))
      (rove:ok (equal fact (physical-logical-decode-fact bytes)))
      (rove:ok (equal '(65 1)
                      (unit-logical-on-interval (physical-logical-state-from-fact fact))))
      (rove:ok (equal fact (physical-logical-check-fact fact history 65 1)))
      (rove:ok (equal fact (getf (physical-recovery-decode-control control) :logical-fact)))
      (let ((anchor (physical-logical-encode-base-anchor fact)))
        (rove:ok (= 208 (length anchor)))
        (rove:ok (equal fact (physical-logical-decode-base-anchor anchor))))))

(cl-spec:defproperty logical-on-interval-physical-codec-preserves-full-width
    ((end (range integer 65 18446744073709551615))
     (timeline (range integer 1 4294967295)))
  (:about physical-logical-encode-fact physical-logical-decode-fact)
  (:trials (:normal 100))
  (let* ((start (- end 33))
         (state (%make-wal-logical-status t start end timeline end timeline end timeline))
         (fact (physical-logical-fact-from-state state :change start timeline end timeline nil nil))
         (bytes (physical-logical-encode-fact fact)))
    (and (= 104 (length bytes))
         (equal fact (physical-logical-decode-fact bytes))
         (equal (list end timeline)
                (unit-logical-on-interval
                 (physical-logical-state-from-fact (physical-logical-decode-fact bytes)))))))

(rove:deftest logical-on-interval-physical-generated-codec
    (let ((data (cl-spec:result-data
                 (cl-spec:run-property 'logical-on-interval-physical-codec-preserves-full-width :seed 42))))
      (rove:ok (eq :passed (getf data :status)) (format nil "~s" data))
      (rove:ok (= 100 (getf data :trials)))))

(rove:deftest logical-on-interval-physical-codec-rejects-false-knowledge
    (let* ((state (wal-logical-status-next nil (unit-logical-status-record t :start 32) 1))
           (fact (physical-logical-fact-from-state state :change 32 1 65 1 nil nil))
           (bytes (physical-logical-encode-fact fact))
           (old (physical-logical-fact-from-state
                 (%make-wal-logical-status t 32 65 1 65 1) :change 32 1 65 1 nil nil))
           (old-bytes (physical-logical-encode-fact old)))
      (rove:ok (= 88 (length old-bytes)))
      (rove:ok (equal old (physical-logical-decode-fact old-bytes)))
      (rove:ok (= 176 (length (physical-logical-encode-base-anchor old))))
      (dolist (mutation
               (list (lambda (copy) (wal-set-u32 copy 8 (logandc2 (wal-u32 copy 8) 16)))
                     (lambda (copy) (wal-set-u32 copy 8 (logior (wal-u32 copy 8) 64)))
                     (lambda (copy) (wal-set-u32 copy 8 (logandc2 (wal-u32 copy 8) 1)))
                     (lambda (copy) (wal-set-u32 copy 12 1))
                     (lambda (copy) (wal-set-u32 copy 84 1))
                     (lambda (copy) (wal-set-u64 copy 88 0))
                     (lambda (copy) (wal-set-u32 copy 96 0))
                     (lambda (copy) (wal-set-u32 copy 100 0))))
        (let ((copy (copy-seq bytes)))
          (funcall mutation copy)
          (rove:ok (unit-commit-ts-error-p (lambda () (physical-logical-decode-fact copy))))))
      (wal-set-u32 old-bytes 8 (logior (wal-u32 old-bytes 8) 16))
      (rove:ok (unit-commit-ts-error-p (lambda () (physical-logical-decode-fact old-bytes))))
      (let* ((control (physical-recovery-control-image 2 42 1 77 32 65 3 :logical-fact fact))
             (crc-offset (- (length control) 4)))
        (wal-set-u32 control (- (length control) 8) 0)
        (wal-set-u32 control crc-offset 0)
        (wal-set-u32 control crc-offset (wal-crc32 control))
        (rove:ok (unit-commit-ts-error-p (lambda () (physical-recovery-decode-control control)))))))

(rove:deftest logical-on-interval-required-prefix-cannot-invent-or-erase-proof
    (let* ((history (timeline-check-lineage (timeline-parse-history 1 "")))
           (first (wal-logical-status-next nil (unit-logical-status-record t :start 32) 1))
           (minimum (physical-logical-fact-from-state first :change 32 1 65 1 nil nil))
           (same (physical-logical-fact-next minimum (unit-logical-status-record t :start 128) 1 history))
           (off (physical-logical-fact-next minimum (unit-logical-status-record nil :start 128) 1 history))
           (next (physical-logical-fact-next off (unit-logical-status-record t :start 256) 1 history))
           (unknown (physical-logical-fact-from-state
                     (%make-wal-logical-status t 32 65 1 65 1) :change 32 1 65 1 nil nil))
           (lost (physical-logical-fact-from-state
                  (%make-wal-logical-status t 256 289 1 289 1) :change 32 1 289 1 nil nil))
           (invented (physical-logical-fact-from-state
                      (wal-logical-status-next nil (unit-logical-status-record t :start 256) 1)
                      :change 32 1 289 1 nil nil)))
      (rove:ok (eql 65 (getf (physical-logical-required-fact same minimum 161 1 65 1)
                             :logical-on-start-lsn)))
      (rove:ok (null (getf (physical-logical-required-fact off minimum 161 1 65 1)
                           :logical-on-start-lsn)))
      (rove:ok (eql 289 (getf (physical-logical-required-fact next minimum 289 1 65 1)
                              :logical-on-start-lsn)))
      (rove:ok (eql 65 (getf (physical-logical-required-fact same unknown 161 1 65 1)
                             :logical-on-start-lsn)))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-logical-required-fact lost minimum 289 1 65 1))))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-logical-required-fact invented minimum 289 1 65 1))))
      (rove:ok (unit-commit-ts-error-p
                (lambda () (physical-logical-required-fact invented unknown 289 1 65 1))))))

(rove:deftest logical-on-interval-verified-checkpoint-refines-legacy-coverage
    (let* ((history (timeline-check-lineage (timeline-parse-history 1 "")))
           (old (unit-source-logical-checkpoint t 512))
           (minimum (physical-logical-fact-from-checkpoint
                     (wal-checkpoint-record-snapshot old)))
           (later (wal-checkpoint-record-snapshot (unit-source-logical-checkpoint t 1024))))
      (setf (getf later :end) 1160)
      (labels ((record (interval)
                 (let ((snapshot (append later
                                         (list :logical-on-start-lsn interval
                                               :logical-on-start-timeline 1))))
                   (make-wal-record :version 8 :type +wal-record-checkpoint+ :xid 0
                                    :start 1024 :end 1160
                                    :payload (wal-encode-checkpoint-snapshot snapshot)))))
        (let* ((source (physical-logical-fact-next minimum (record 65) 1 history))
               (reconciled (physical-logical-required-fact source minimum 1160 1 632 1)))
          (rove:ok (= 512 (getf source :coverage-start)))
          (rove:ok (= 65 (getf reconciled :logical-on-start-lsn)))
          (rove:ok (null (getf reconciled :last-enable-lsn)))
          (rove:ok (not (eq source reconciled))))
        (rove:ok (unit-commit-ts-error-p
                  (lambda () (physical-logical-fact-next minimum (record 600) 1 history))))
        (let ((source (physical-logical-fact-next
                       minimum (unit-source-logical-checkpoint t 1024) 1 history)))
          (rove:ok (null (getf (physical-logical-required-fact source minimum 1144 1 632 1)
                               :logical-on-start-lsn)))))))

(rove:deftest logical-on-interval-base-and-replay-seeds-preserve-anchor-knowledge
    (let* ((snapshot (unit-logical-status-snapshot t nil nil :unknown))
           (history (timeline-check-lineage (timeline-parse-history 1 ""))))
      (setf (getf snapshot :start) 512 (getf snapshot :redo) 512 (getf snapshot :end) 632
            (getf snapshot :timeline) 1 (getf snapshot :prev-timeline) 1)
      (let* ((state (%make-wal-logical-status t 512 632 1 65 1 65 1))
             (anchor (physical-logical-fact-from-state state :checkpoint 32 1 632 1 nil nil))
             (manifest (list :end 632 :timeline 1
                             :logical-base-anchor (physical-logical-encode-base-anchor anchor)))
             (base (physical-logical-base-fact manifest snapshot history)))
        (rove:ok (eql 65 (getf base :logical-on-start-lsn)))
        (rove:ok (eql 512 (getf base :coverage-start)))
        (rove:ok (equal anchor (physical-recovery-scan-checkpoint-logical snapshot anchor 632 1)))
        (let ((other (copy-list base)))
          (setf (getf other :logical-on-start-lsn) 66)
          (rove:ok (physical-logical-fact-valid-p other))
          (rove:ok (unit-commit-ts-error-p
                    (lambda () (physical-logical-merge-checkpoint-interval base other)))))))
  (let* ((history (timeline-check-lineage
                   (timeline-parse-history 2 (format nil "1~c0/00000080~cfork~%" #\Tab #\Tab))))
         (bad (physical-logical-fact-from-state
               (%make-wal-logical-status t 256 289 2 289 2 65 2)
               :change 256 2 289 2 nil nil)))
    (rove:ok (physical-logical-fact-valid-p bad))
    (rove:ok (unit-commit-ts-error-p
              (lambda () (physical-logical-check-fact bad history 289 2))))
    (setf (getf bad :logical-on-start-timeline) 1)
    (rove:ok (equal bad (physical-logical-check-fact bad history 289 2)))))

(rove:deftest logical-on-interval-standby-backup-keeps-known-anchor
    (let* ((snapshot (unit-logical-status-snapshot t nil nil :unknown))
           (history (timeline-check-lineage (timeline-parse-history 1 "")))
           (anchor (physical-logical-fact-from-state
                    (%make-wal-logical-status t 512 632 1 65 1 65 1)
                    :checkpoint 32 1 632 1 nil nil))
           (*physical-recovery-state* (list :source-logical-fact anchor))
           (*physical-receive-store* nil)
           (original (symbol-function 'physical-recovery-resource-history)))
      (setf (getf snapshot :start) 512 (getf snapshot :redo) 512 (getf snapshot :end) 632
            (getf snapshot :timeline) 1 (getf snapshot :prev-timeline) 1)
      (unwind-protect
           (progn
             (setf (symbol-function 'physical-recovery-resource-history)
                   (lambda (store state)
                     (rove:ok (null store))
                     (rove:ok (eq state *physical-recovery-state*))
                     history))
             (multiple-value-bind (fact text) (physical-standby-backup-logical-facts snapshot)
               (rove:ok (eql 65 (getf fact :logical-on-start-lsn)))
               (rove:ok text)
               (rove:ok (= 208 (length text)))
               (rove:ok (equal anchor (physical-logical-decode-base-anchor text)))))
        (setf (symbol-function 'physical-recovery-resource-history) original))))

(rove:deftest logical-on-interval-promotion-prefix-preserves-or-starts-interval
    (let* ((first (wal-logical-status-next nil (unit-logical-status-record t :start 32) 1))
           (fact (physical-logical-fact-from-state first :change 32 1 65 1 nil nil))
           (state (list :applied 65 :timeline 1 :replay-timeline 1 :promotion-child 2
                        :source-logical-fact fact :promotion-logical-decoding-enabled t)))
      (multiple-value-bind (frames cursor fields) (physical-promotion-logical-prefix state 128)
        (rove:ok (null frames))
        (rove:ok (= 128 cursor))
        (rove:ok (equal '(:logical-decoding-enabled t :logical-on-start-lsn 65 :logical-on-start-timeline 1)
                        fields)))
      (setf (getf state :source-logical-fact)
            (physical-logical-fact-from-state
             (%make-wal-logical-status t 32 65 1 65 1) :change 32 1 65 1 nil nil))
      (multiple-value-bind (frames cursor fields) (physical-promotion-logical-prefix state 128)
        (declare (ignore cursor))
        (rove:ok (null frames))
        (rove:ok (equal '(:logical-decoding-enabled t) fields)))
      (let* ((off (wal-logical-status-next first (unit-logical-status-record nil :start 128) 1))
             (off-fact (physical-logical-fact-from-state off :change 32 1 161 1 161 1)))
        (setf (getf state :source-logical-fact) off-fact (getf state :applied) 161)
        (multiple-value-bind (frames cursor fields) (physical-promotion-logical-prefix state 256)
          (let ((marker (wal-decode-record (second (second frames)) (first (second frames)))))
            (rove:ok (wal-logical-decoding-status-record-info marker))
            (rove:ok (= cursor (wal-record-end marker)))
            (rove:ok (= cursor (getf fields :logical-on-start-lsn)))
            (rove:ok (= 2 (getf fields :logical-on-start-timeline)))
            (rove:ok (equal (list cursor 2)
                            (unit-logical-on-interval (wal-logical-status-next off marker 2))))
            (rove:ok (equalp (list frames cursor fields)
                             (multiple-value-list (physical-promotion-logical-prefix state 256)))))))))

(rove:deftest logical-on-interval-clog-rebind-preserves-durable-identity
    (let* ((old (gensym "OLD-LOGICAL-STORE-")) (new (gensym "NEW-LOGICAL-STORE-"))
                                               (state (wal-logical-status-next nil (unit-logical-status-record t :start 32) 1))
                                               (policy (%make-wal-logical-policy old state nil))
                                               (replacement (wal-logical-policy-rebind-incarnation policy old new)))
      (rove:ok (wal-logical-policy-rebinding-valid-p policy replacement old new))
      (rove:ok (eq state (wal-logical-policy-state replacement)))
      (rove:ok (equal '(65 1) (unit-logical-on-interval (wal-logical-policy-state replacement))))
      (rove:ok (eq old (wal-logical-policy-incarnation policy)))
      (rove:ok (eq new (wal-logical-policy-incarnation replacement)))))

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

(rove:deftest logical-on-interval-primary-recovery-checks-tail-checkpoint-owner
    (let* ((*wal-insert-timeline* 2)
           (*wal-checkpoint-snapshot* (wal-checkpoint-record-snapshot
                                       (unit-source-logical-checkpoint t 512)))
           (*wal-checkpoint-lsn* 632)
           (history (timeline-parse-history 2 (format nil "1~C0/80~Cfork~%" #\Tab #\Tab)))
           (names '(wal-scan wal-replay-record timeline-read-history))
           (originals (mapcar #'symbol-function names)))
      (setf (getf *wal-checkpoint-snapshot* :timeline) 2)
      (unwind-protect
           (dolist (owner '(nil 1 2))
             (let* ((snapshot (wal-checkpoint-record-snapshot
                               (unit-source-logical-checkpoint t 1024)))
                    (applied 0)
                    (*wal-checkpoint-snapshot* (and owner *wal-checkpoint-snapshot*)))
               (setf (getf snapshot :timeline) 2 (getf snapshot :end) 1160
                     snapshot (if owner
                                  (append snapshot (list :logical-on-start-lsn 65
                                                         :logical-on-start-timeline owner))
                                  snapshot))
               (unless owner (remf snapshot :logical-decoding-enabled)
                       (setf (getf snapshot :end) 1144))
               (let ((record (make-wal-record
                              :version 8 :type +wal-record-checkpoint+ :xid 0
                              :start 1024 :end (getf snapshot :end) :payload (wal-encode-checkpoint-snapshot snapshot))))
                 (setf (symbol-function 'timeline-read-history)
                       (lambda (&rest args) (declare (ignore args)) history)
                       (symbol-function 'wal-scan)
                       (lambda (function &rest args) (declare (ignore args)) (funcall function record))
                       (symbol-function 'wal-replay-record)
                       (lambda (record)
                         (declare (ignore record))
                         (incf applied)
                         (error "stop test before publication")))
                 (rove:ok (unit-commit-ts-error-p #'wal-recover))
                 (rove:ok (= (if (eql owner 2) 0 1) applied)))))
        (loop for name in names for original in originals
              do (setf (symbol-function name) original)))))

(rove:deftest logical-status-change-generated-contracts
    (dolist (result
             (list (cl-spec:result-data
                    (cl-spec:check-function 'wal-encode-logical-decoding-status-change
                                            :trials 150 :seed 252))
                   (cl-spec:result-data
                    (cl-spec:run-property 'logical-status-change-roundtrip :seed 253))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))))
