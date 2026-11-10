(in-package #:pg)

(defun unit-commit-ts-snapshot (&key resources known (start 64))
  (append
   (list :version 2 :start start
         :end (+ start 32 (if resources 136 88) (if known 8 0))
         :redo start :next-xid 3 :xid-reserved-through 2 :next-oid nil
         :time 0 :timeline 3 :prev-timeline 2 :oldest-active-xid 0)
   (when resources
     (list :resource-parameters (list 100 8 10 0 128) :resource-lsn 32 :resource-timeline 2))
   (when known (list :oldest-commit-ts-xid 0 :newest-commit-ts-xid 0))))

(defun unit-commit-ts-error-p (function)
  (handler-case (progn (funcall function) nil)
    (error () t)))

(cl-spec:defgenerator unit-commit-ts-fact-arguments ()
  (list (append (when (zerop (random 2))
                  (list :oldest-commit-ts-xid 0 :newest-commit-ts-xid 0))
                (list :unrelated (random 1000)))))

(cl-spec:defspec-function wal-copy-checkpoint-commit-ts-fields
    (:args (facts (type list)))
  (:args-generator unit-commit-ts-fact-arguments)
  (:returns (type list))
  (:post (if (get-properties facts '(:oldest-commit-ts-xid))
             (and (equal result '(:oldest-commit-ts-xid 0 :newest-commit-ts-xid 0))
                  (not (eq result facts)))
             (null result))))

(cl-spec:defproperty checkpoint-commit-ts-roundtrip
    ((start (range integer 64 1000000)) (resources (member nil t)) (known (member nil t)))
  (:about wal-encode-checkpoint-snapshot wal-decode-checkpoint-snapshot
          wal-copy-checkpoint-commit-ts-fields)
  (:trials (:normal 100))
  (let* ((snapshot (unit-commit-ts-snapshot :start start :resources resources :known known))
         (bytes (wal-encode-checkpoint-snapshot snapshot))
         (decoded (wal-decode-checkpoint-snapshot bytes)))
    (and (equal snapshot decoded)
         (= (length bytes) (+ (if resources 136 88) (if known 8 0)))
         (equalp bytes (wal-encode-checkpoint-snapshot decoded)))))

(rove:deftest checkpoint-commit-ts-preserves-known-zero-and-historical-absence
    (dolist (resources '(nil t))
      (dolist (known '(nil t))
        (let* ((snapshot (unit-commit-ts-snapshot :resources resources :known known))
               (bytes (wal-encode-checkpoint-snapshot snapshot))
               (size (+ 32 (if resources 136 88) (if known 8 0))))
          (rove:ok (wal-checkpoint-snapshot-valid-p snapshot))
          (rove:ok (= size (wal-checkpoint-record-size snapshot)))
          (rove:ok (wal-checkpoint-record-length-p size))
          (rove:ok (= (if known 4 0) (wal-u32 bytes 84)))
          (rove:ok (equal snapshot (wal-decode-checkpoint-snapshot bytes)))
          (rove:ok (equal snapshot
                          (wal-checkpoint-snapshot-from-hex (wal-checkpoint-snapshot-hex snapshot))))
          (when resources
            (rove:ok (equal '(:resource-parameters (100 8 10 0 128)
                              :resource-lsn 32 :resource-timeline 2)
                            (wal-decode-resource-fact (subseq bytes 88 136)))))
          (when known
            (rove:ok (zerop (wal-u32 bytes (- (length bytes) 8))))
            (rove:ok (zerop (wal-u32 bytes (- (length bytes) 4)))))))))

(rove:deftest checkpoint-commit-ts-rejects-partial-and-unsupported-ranges
    (dolist (facts '((:oldest-commit-ts-xid 0) (:newest-commit-ts-xid 0)
                     (:oldest-commit-ts-xid nil :newest-commit-ts-xid nil)
                     (:oldest-commit-ts-xid 0 :newest-commit-ts-xid 3)
                     (:oldest-commit-ts-xid 3 :newest-commit-ts-xid 0)
                     (:oldest-commit-ts-xid -1 :newest-commit-ts-xid 0)
                     (:oldest-commit-ts-xid 0 :newest-commit-ts-xid 4294967296)
                     (:oldest-commit-ts-xid 0 :newest-commit-ts-xid 0 :oldest-commit-ts-xid 3)
                     (:oldest-commit-ts-xid 0 :newest-commit-ts-xid 0 :newest-commit-ts-xid 0)))
      (let ((snapshot (append (unit-commit-ts-snapshot) (copy-list facts))))
        (rove:ok (not (wal-checkpoint-commit-ts-fields-valid-p facts)))
        (rove:ok (not (wal-checkpoint-snapshot-valid-p snapshot)))
        (rove:ok (unit-commit-ts-error-p (lambda () (wal-copy-checkpoint-commit-ts-fields facts))))
        (rove:ok (unit-commit-ts-error-p (lambda () (wal-encode-checkpoint-snapshot snapshot))))))
  (let* ((facts (list :oldest-commit-ts-xid 0 :newest-commit-ts-xid 0))
         (copy (wal-copy-checkpoint-commit-ts-fields facts)))
    (setf (getf copy :oldest-commit-ts-xid) 9)
    (rove:ok (zerop (getf facts :oldest-commit-ts-xid)))))

(rove:deftest checkpoint-commit-ts-rejects-flags-lengths-and-hidden-values
    (dolist (resources '(nil t))
      (dolist (known '(nil t))
        (let* ((snapshot (unit-commit-ts-snapshot :resources resources :known known))
               (bytes (wal-encode-checkpoint-snapshot snapshot)))
          (let ((bad (copy-seq bytes)))
            (wal-set-u32 bad 84 (logxor 4 (wal-u32 bad 84)))
            (rove:ok (unit-commit-ts-error-p (lambda () (wal-decode-checkpoint-snapshot bad)))))
          (let ((bad (copy-seq bytes)))
            (wal-set-u32 bad 84 (logior 32 (wal-u32 bad 84)))
            (rove:ok (unit-commit-ts-error-p (lambda () (wal-decode-checkpoint-snapshot bad)))))
          (dolist (bad (list (subseq bytes 0 (1- (length bytes)))
                             (concatenate '(vector (unsigned-byte 8)) bytes #(0))))
            (rove:ok (unit-commit-ts-error-p (lambda () (wal-decode-checkpoint-snapshot bad)))))
          (when known
            (let ((old-format (copy-seq bytes)))
              (setf (aref old-format 7) (char-code #\1))
              (rove:ok (unit-commit-ts-error-p
                        (lambda () (wal-decode-checkpoint-snapshot old-format)))))
            (dolist (offset (list (- (length bytes) 8) (- (length bytes) 4)))
              (let ((bad (copy-seq bytes)))
                (wal-set-u32 bad offset 3)
                (rove:ok (unit-commit-ts-error-p (lambda () (wal-decode-checkpoint-snapshot bad)))))))))))

(rove:deftest checkpoint-commit-ts-requires-current-wal-and-keeps-legacy-records
    (dolist (known '(nil t))
      (let* ((snapshot (unit-commit-ts-snapshot :known known))
             (payload (wal-encode-checkpoint-snapshot snapshot)))
        (dolist (version '(7 8))
          (let ((record (make-wal-record :version version :type +wal-record-checkpoint+
                                         :xid 0 :start 64 :end (getf snapshot :end) :payload payload)))
            (if (and known (= version 7))
                (rove:ok (unit-commit-ts-error-p (lambda () (wal-checkpoint-record-snapshot record))))
                (rove:ok (equal snapshot (wal-checkpoint-record-snapshot record))))))))
  (let* ((snapshot (unit-commit-ts-snapshot))
         (bytes (wal-encode-checkpoint-snapshot snapshot)))
    (setf (aref bytes 7) (char-code #\1))
    (rove:ok (= 1 (getf (wal-decode-checkpoint-snapshot bytes) :version)))
    (rove:ok (null (getf (wal-decode-checkpoint-snapshot bytes) :oldest-commit-ts-xid)))))

(rove:deftest checkpoint-commit-ts-keeps-exhausted-xids-and-resource-boundaries
    (dolist (resources '(nil t))
      (dolist (next '(3 18446744073709551616))
        (let ((snapshot (unit-commit-ts-snapshot :resources resources :known t)))
          (setf (getf snapshot :next-xid) next
                (getf snapshot :xid-reserved-through) (1- next)
                (getf snapshot :oldest-active-xid) next
                (getf snapshot :next-oid) #xffffffffffffffff)
          (let ((bytes (wal-encode-checkpoint-snapshot snapshot)))
            (rove:ok (= (if (= next 3) 4 7) (wal-u32 bytes 84)))
            (rove:ok (equal snapshot (wal-decode-checkpoint-snapshot bytes))))))))

(rove:deftest checkpoint-commit-ts-control-copy-is-crc-bound
    (dolist (resources '(nil t))
      (dolist (known '(nil t))
        (let* ((snapshot (unit-commit-ts-snapshot :resources resources :known known))
               (metadata (append (make-wal-control-metadata 42 4 0)
                                 (wal-copy-control-resource-fields snapshot)))
               (bytes (wal-encode-control-image
                       (getf snapshot :end) (getf snapshot :end) snapshot
                       :timeline 3 :checkpoint-timeline 3 :namespace :timeline :metadata metadata))
               (decoded (multiple-value-list (wal-decode-control-line (bytes->string bytes)))))
          (rove:ok (equal snapshot (third decoded)))
          (rove:ok (equal metadata (seventh decoded)))
          (when known
            (let* ((hex (wal-checkpoint-snapshot-hex snapshot))
                   (start (search hex (bytes->string bytes)))
                   (bad (copy-seq bytes)))
              (rove:ok start)
              (setf (aref bad (+ start (1- (length hex)))) (char-code #\1))
              (rove:ok (unit-commit-ts-error-p
                        (lambda () (wal-decode-control-line (bytes->string bad)))))))))))

(rove:deftest checkpoint-commit-ts-generated-copy-and-roundtrip-contracts
    (dolist (result
             (list (cl-spec:result-data
                    (cl-spec:check-function 'wal-copy-checkpoint-commit-ts-fields :trials 100 :seed 245))
                   (cl-spec:result-data
                    (cl-spec:run-property 'checkpoint-commit-ts-roundtrip :seed 246))))
      (rove:ok (eq :passed (getf result :status)) (format nil "~s" result))))
